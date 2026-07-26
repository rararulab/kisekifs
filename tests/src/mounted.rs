use std::{
    env,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt, symlink},
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    sync::{Arc, Barrier, mpsc},
    thread,
    time::{Duration, Instant, UNIX_EPOCH},
};

use rustix::fs::FallocateFlags;
use tempfile::{Builder, TempDir};

const READY_TIMEOUT: Duration = Duration::from_secs(15);
const STOP_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(25);
const BLOCK_SIZE: usize = 4 << 20;
const CHUNK_SIZE: u64 = 64 << 20;

struct TestEnv {
    root:    TempDir,
    meta:    PathBuf,
    objects: PathBuf,
    stage:   PathBuf,
    mount:   PathBuf,
    log:     PathBuf,
}

impl TestEnv {
    fn new(case: &str) -> Self {
        require_fuse();
        let root = if let Some(base) = env::var_os("KISEKI_MOUNT_TEST_ROOT") {
            let base = PathBuf::from(base);
            fs::create_dir_all(&base).expect("create mounted-test base");
            Builder::new()
                .prefix(&format!("{case}-"))
                .tempdir_in(base)
                .expect("create mounted-test root")
        } else {
            Builder::new()
                .prefix(&format!("kisekifs-{case}-"))
                .tempdir()
                .expect("create mounted-test root")
        };
        let meta = root.path().join("meta");
        let objects = root.path().join("objects");
        let stage = root.path().join("stage");
        let mount = root.path().join("mount");
        let log = root.path().join("mount.log");
        for path in [&meta, &objects, &stage, &mount] {
            fs::create_dir(path)
                .unwrap_or_else(|error| panic!("create isolated path {}: {error}", path.display()));
        }

        let env = Self {
            root,
            meta,
            objects,
            stage,
            mount,
            log,
        };
        env.assert_owned(&env.mount);
        env.format_volume();
        env
    }

    fn root(&self) -> &Path { self.root.path() }

    fn meta_dsn(&self) -> String { format!("rocksdb://:{}", self.meta.display()) }

    fn object_dsn(&self) -> String { format!("file://{}", self.objects.display()) }

    fn assert_owned(&self, path: &Path) {
        let root = self.root().canonicalize().expect("canonical test root");
        let target = path.canonicalize().expect("canonical owned path");
        assert!(
            target.starts_with(&root),
            "refusing to operate on foreign path {} outside {}",
            target.display(),
            root.display()
        );
    }

    fn format_volume(&self) {
        let output = Command::new(kiseki_binary())
            .args(["format", "mounted-test", "--meta-dsn", &self.meta_dsn()])
            .output()
            .expect("run kiseki format");
        assert_success("format volume", &output);
    }

    fn mount(&self, read_only: bool) -> MountGuard {
        MountGuard::spawn(self, read_only, &self.object_dsn())
    }

    fn invalid_mount_output(&self) -> Output {
        Command::new(kiseki_binary())
            .args([
                "mount",
                "--foreground",
                "--no-log",
                "--auto-unmount",
                "false",
                "--allow-other",
                "false",
                "--meta-dsn",
                &self.meta_dsn(),
                "--object-storage",
                "file://relative/path",
                "--stage-cache-dir",
            ])
            .arg(&self.stage)
            .arg(&self.mount)
            .env("KISEKI_DISABLE_DISK_POOL", "1")
            .output()
            .expect("run invalid mount")
    }
}

struct MountGuard {
    child:       Option<Child>,
    mount_point: PathBuf,
    owned_root:  PathBuf,
    log_path:    PathBuf,
}

impl MountGuard {
    fn spawn(env: &TestEnv, read_only: bool, object_dsn: &str) -> Self {
        env.assert_owned(&env.mount);
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&env.log)
            .expect("open mount log");
        let stderr = log.try_clone().expect("clone mount log");
        let mut command = Command::new(kiseki_binary());
        command
            .args([
                "mount",
                "--foreground",
                "--no-log",
                "--auto-unmount",
                "false",
                "--allow-other",
                "false",
                "--meta-dsn",
                &env.meta_dsn(),
                "--object-storage",
                object_dsn,
                "--stage-cache-dir",
            ])
            .arg(&env.stage)
            .arg(&env.mount)
            .env("KISEKI_DISABLE_DISK_POOL", "1")
            .env_remove("PYROSCOPE_SERVER_URL")
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(stderr));
        if read_only {
            command.arg("--read-only");
        }
        let child = command.spawn().expect("spawn kiseki mount");
        let mut guard = Self {
            child:       Some(child),
            mount_point: env.mount.clone(),
            owned_root:  env.root().to_path_buf(),
            log_path:    env.log.clone(),
        };
        if let Err(error) = guard.wait_ready() {
            let logs = guard.log_tail();
            let _ = guard.cleanup();
            panic!("mount did not become ready: {error}\n--- mount log ---\n{logs}");
        }
        guard
    }

    fn pid(&self) -> u32 { self.child.as_ref().expect("mount child is present").id() }

    fn wait_ready(&mut self) -> Result<(), String> {
        let deadline = Instant::now() + READY_TIMEOUT;
        loop {
            if is_mounted(&self.mount_point) && fs::read_dir(&self.mount_point).is_ok() {
                return Ok(());
            }
            if let Some(status) = self
                .child
                .as_mut()
                .expect("mount child is present")
                .try_wait()
                .map_err(|error| format!("check mount child: {error}"))?
            {
                return Err(format!("mount child exited early with {status}"));
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "timed out; mounted={}, mountinfo={}",
                    is_mounted(&self.mount_point),
                    mountinfo_entry(&self.mount_point).unwrap_or_else(|| "<missing>".to_string())
                ));
            }
            thread::sleep(POLL_INTERVAL);
        }
    }

    fn shutdown(mut self) {
        if let Err(error) = self.cleanup() {
            let logs = self.log_tail();
            panic!("clean unmount failed: {error}\n--- mount log ---\n{logs}");
        }
    }

    fn kill(mut self) {
        let pid = self.pid();
        let child = self.child.as_mut().expect("mount child is present");
        child.kill().expect("SIGKILL mount child");
        child.wait().expect("reap killed mount child");
        self.child = None;
        self.assert_owned_mount()
            .expect("crash cleanup must target the owned mount");
        // AutoUnmount is disabled because fuser otherwise implies allow_other.
        // A dead daemon can therefore leave a disconnected kernel mount that
        // the harness must detach before testing recovery.
        if is_mounted(&self.mount_point) {
            fusermount(&self.mount_point, true).expect("detach killed FUSE mount");
        }
        wait_until(STOP_TIMEOUT, || !is_mounted(&self.mount_point)).unwrap_or_else(|error| {
            panic!("mount remained after detaching killed process {pid}: {error}");
        });
        assert!(
            !process_exists(pid),
            "killed mount child {pid} still exists"
        );
    }

    fn cleanup(&mut self) -> Result<(), String> {
        self.assert_owned_mount()?;
        if is_mounted(&self.mount_point) {
            if fusermount(&self.mount_point, false).is_err() {
                fusermount(&self.mount_point, true)?;
            }
            if wait_until(STOP_TIMEOUT, || !is_mounted(&self.mount_point)).is_err() {
                fusermount(&self.mount_point, true)?;
                wait_until(STOP_TIMEOUT, || !is_mounted(&self.mount_point))?;
            }
        }

        if let Some(mut child) = self.child.take() {
            let deadline = Instant::now() + STOP_TIMEOUT;
            loop {
                if child
                    .try_wait()
                    .map_err(|error| format!("check mount child during cleanup: {error}"))?
                    .is_some()
                {
                    break;
                }
                if Instant::now() >= deadline {
                    child
                        .kill()
                        .map_err(|error| format!("kill mount child: {error}"))?;
                    child
                        .wait()
                        .map_err(|error| format!("reap mount child: {error}"))?;
                    break;
                }
                thread::sleep(POLL_INTERVAL);
            }
        }
        if is_mounted(&self.mount_point) {
            fusermount(&self.mount_point, true)?;
            wait_until(STOP_TIMEOUT, || !is_mounted(&self.mount_point))?;
        }
        if is_mounted(&self.mount_point) {
            return Err(format!(
                "mount remains: {}",
                mountinfo_entry(&self.mount_point).unwrap_or_default()
            ));
        }
        Ok(())
    }

    fn assert_owned_mount(&self) -> Result<(), String> {
        let root = self
            .owned_root
            .canonicalize()
            .map_err(|error| format!("canonical test root: {error}"))?;
        let mount = self
            .mount_point
            .canonicalize()
            .map_err(|error| format!("canonical mount point: {error}"))?;
        if !mount.starts_with(&root) {
            return Err(format!(
                "refusing to clean foreign mount {} outside {}",
                mount.display(),
                root.display()
            ));
        }
        Ok(())
    }

    fn log_tail(&self) -> String {
        let bytes = fs::read(&self.log_path).unwrap_or_default();
        let start = bytes.len().saturating_sub(64 << 10);
        String::from_utf8_lossy(&bytes[start..]).into_owned()
    }
}

impl Drop for MountGuard {
    fn drop(&mut self) {
        if let Err(error) = self.cleanup() {
            eprintln!("mounted-test cleanup failed: {error}\n{}", self.log_tail());
        }
    }
}

fn kiseki_binary() -> PathBuf {
    let path = env::var_os("KISEKI_BIN").expect("KISEKI_BIN must point to the built binary");
    PathBuf::from(path)
        .canonicalize()
        .expect("canonical KISEKI_BIN")
}

fn require_fuse() {
    assert!(
        fs::metadata("/dev/fuse").is_ok_and(|metadata| metadata.file_type().is_char_device()),
        "/dev/fuse is unavailable"
    );
    let status = Command::new("sh")
        .args(["-c", "command -v fusermount3 >/dev/null"])
        .status()
        .expect("probe fusermount3");
    assert!(status.success(), "fusermount3 is unavailable");
}

fn assert_success(action: &str, output: &Output) {
    assert!(
        output.status.success(),
        "{action} failed with {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn wait_until(mut remaining: Duration, mut predicate: impl FnMut() -> bool) -> Result<(), String> {
    while !predicate() {
        if remaining.is_zero() {
            return Err("deadline expired".to_string());
        }
        let sleep_for = remaining.min(POLL_INTERVAL);
        thread::sleep(sleep_for);
        remaining = remaining.saturating_sub(sleep_for);
    }
    Ok(())
}

fn mountinfo_entry(mount_point: &Path) -> Option<String> {
    let expected = mount_point.to_string_lossy();
    fs::read_to_string("/proc/self/mountinfo")
        .ok()?
        .lines()
        .find(|line| {
            line.split_whitespace()
                .nth(4)
                .is_some_and(|path| decode_mountinfo_path(path) == expected)
        })
        .map(str::to_string)
}

fn is_mounted(mount_point: &Path) -> bool { mountinfo_entry(mount_point).is_some() }

fn decode_mountinfo_path(path: &str) -> String {
    path.replace("\\040", " ")
        .replace("\\011", "\t")
        .replace("\\012", "\n")
        .replace("\\134", "\\")
}

fn fusermount(mount_point: &Path, lazy: bool) -> Result<(), String> {
    let mut command = Command::new("fusermount3");
    command.arg("-u");
    if lazy {
        command.arg("-z");
    }
    let output = command
        .arg(mount_point)
        .output()
        .map_err(|error| format!("start fusermount3: {error}"))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "fusermount3 failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

fn process_exists(pid: u32) -> bool { Path::new(&format!("/proc/{pid}")).exists() }

fn has_regular_file(root: &Path) -> bool {
    let Ok(entries) = fs::read_dir(root) else {
        return false;
    };
    entries.flatten().any(|entry| {
        entry.file_type().is_ok_and(|kind| kind.is_file())
            || entry
                .file_type()
                .is_ok_and(|kind| kind.is_dir() && has_regular_file(&entry.path()))
    })
}

fn assert_errno<T>(result: std::io::Result<T>, errno: i32, action: &str) {
    let error = result
        .err()
        .unwrap_or_else(|| panic!("{action} unexpectedly succeeded"));
    assert_eq!(
        error.raw_os_error(),
        Some(errno),
        "{action} returned {error:?}"
    );
}

fn pattern(len: usize, salt: u8) -> Vec<u8> {
    (0..len)
        .map(|index| (index as u8).wrapping_mul(31).wrapping_add(salt))
        .collect()
}

mod smoke {
    use super::*;

    #[test]
    #[ignore = "requires Linux /dev/fuse"]
    fn mount_create_unmount() {
        let env = TestEnv::new("smoke");
        let mount = env.mount(false);
        let pid = mount.pid();
        let path = env.mount.join("smoke.txt");
        fs::write(&path, b"mounted bytes").expect("write through mount");
        assert_eq!(fs::read(&path).unwrap(), b"mounted bytes");

        mount.shutdown();

        assert!(!is_mounted(&env.mount));
        assert!(!process_exists(pid));
    }
}

mod semantics {
    use super::*;

    #[test]
    #[ignore = "requires Linux /dev/fuse"]
    fn namespace_and_metadata() {
        let env = TestEnv::new("namespace");
        let mount = env.mount(false);
        let left = env.mount.join("left");
        let right = env.mount.join("right");
        fs::create_dir(&left).unwrap();
        fs::create_dir(&right).unwrap();
        let original = left.join("original.txt");
        fs::write(&original, b"namespace").unwrap();
        let renamed = right.join("renamed.txt");
        fs::rename(&original, &renamed).unwrap();
        let hard = right.join("hard.txt");
        fs::hard_link(&renamed, &hard).unwrap();
        let symbolic = right.join("symbolic.txt");
        symlink("renamed.txt", &symbolic).unwrap();
        assert_eq!(fs::read_link(&symbolic).unwrap(), Path::new("renamed.txt"));
        assert_eq!(fs::read(&hard).unwrap(), b"namespace");

        fs::set_permissions(&renamed, fs::Permissions::from_mode(0o640)).unwrap();
        let metadata = fs::metadata(&renamed).unwrap();
        assert_eq!(metadata.permissions().mode() & 0o777, 0o640);
        wait_until(Duration::from_secs(2), || {
            fs::metadata(&renamed).is_ok_and(|metadata| metadata.nlink() == 2)
                && fs::metadata(&hard).is_ok_and(|metadata| metadata.nlink() == 2)
        })
        .expect("hard-link count did not refresh after the attribute-cache TTL");

        let timestamp = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let file = OpenOptions::new().write(true).open(&renamed).unwrap();
        file.set_times(
            fs::FileTimes::new()
                .set_accessed(timestamp)
                .set_modified(timestamp),
        )
        .unwrap();
        drop(file);
        let metadata = fs::metadata(&renamed).unwrap();
        assert_eq!(metadata.modified().unwrap(), timestamp);

        let names = fs::read_dir(&right)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<std::collections::HashSet<_>>();
        for name in ["renamed.txt", "hard.txt", "symbolic.txt"] {
            assert!(names.contains(Path::new(name).as_os_str()));
        }

        let stat = rustix::fs::statvfs(&env.mount).unwrap();
        assert!(stat.f_bsize > 0);
        assert!(stat.f_blocks >= stat.f_bfree);
        assert!(stat.f_files >= stat.f_ffree);

        fs::remove_file(&symbolic).unwrap();
        fs::remove_file(&hard).unwrap();
        fs::remove_file(&renamed).unwrap();
        fs::remove_dir(&left).unwrap();
        fs::remove_dir(&right).unwrap();
        mount.shutdown();
    }

    #[test]
    #[ignore = "requires Linux /dev/fuse"]
    fn io_boundaries() {
        let env = TestEnv::new("io");
        let mount = env.mount(false);

        let empty = env.mount.join("empty");
        File::create(&empty).unwrap();
        assert!(fs::read(&empty).unwrap().is_empty());

        let eof = env.mount.join("eof");
        fs::write(&eof, b"hello").unwrap();
        let mut file = File::open(&eof).unwrap();
        file.seek(SeekFrom::Start(3)).unwrap();
        let mut tail = Vec::new();
        file.read_to_end(&mut tail).unwrap();
        assert_eq!(tail, b"lo");
        file.seek(SeekFrom::Start(5)).unwrap();
        assert_eq!(file.read(&mut [0; 8]).unwrap(), 0);
        file.seek(SeekFrom::Start(99)).unwrap();
        assert_eq!(file.read(&mut [0; 8]).unwrap(), 0);
        drop(file);

        let sparse = env.mount.join("sparse");
        let mut file = File::create(&sparse).unwrap();
        file.seek(SeekFrom::Start(8192)).unwrap();
        file.write_all(b"tail").unwrap();
        file.sync_all().unwrap();
        drop(file);
        let bytes = fs::read(&sparse).unwrap();
        assert_eq!(bytes.len(), 8196);
        assert!(bytes[..8192].iter().all(|byte| *byte == 0));
        assert_eq!(&bytes[8192..], b"tail");
        let file = OpenOptions::new().write(true).open(&sparse).unwrap();
        file.set_len(3).unwrap();
        file.set_len(10).unwrap();
        drop(file);
        assert_eq!(fs::read(&sparse).unwrap(), [0; 10]);

        let multiblock = env.mount.join("multiblock");
        let mut expected = pattern(BLOCK_SIZE + 37, 7);
        fs::write(&multiblock, &expected).unwrap();
        assert_eq!(fs::read(&multiblock).unwrap(), expected);
        let mut file = OpenOptions::new().write(true).open(&multiblock).unwrap();
        file.seek(SeekFrom::Start((BLOCK_SIZE - 9) as u64)).unwrap();
        file.write_all(b"ordered-overwrite").unwrap();
        file.sync_all().unwrap();
        drop(file);
        expected[BLOCK_SIZE - 9..BLOCK_SIZE + 8].copy_from_slice(b"ordered-overwrite");
        let bytes = fs::read(&multiblock).unwrap();
        assert_eq!(bytes, expected);

        let chunks = env.mount.join("chunks");
        let boundary = pattern(32, 19);
        let mut file = File::create(&chunks).unwrap();
        file.seek(SeekFrom::Start(CHUNK_SIZE - 7)).unwrap();
        file.write_all(&boundary).unwrap();
        file.sync_all().unwrap();
        drop(file);
        let mut file = File::open(&chunks).unwrap();
        file.seek(SeekFrom::Start(CHUNK_SIZE - 7)).unwrap();
        let mut actual = [0; 32];
        file.read_exact(&mut actual).unwrap();
        assert_eq!(actual.as_slice(), boundary);
        drop(file);
        mount.shutdown();
    }

    #[test]
    #[ignore = "requires Linux /dev/fuse"]
    fn descriptor_lifecycle() {
        let env = TestEnv::new("descriptors");
        let mount = env.mount(false);
        let path = env.mount.join("descriptors");
        fs::write(&path, b"0123456789").unwrap();
        let mut first = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let mut second = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        first.seek(SeekFrom::Start(0)).unwrap();
        first.write_all(b"abc").unwrap();
        first.sync_all().unwrap();
        first.sync_all().unwrap();
        second.seek(SeekFrom::Start(7)).unwrap();
        second.write_all(b"XYZ").unwrap();
        second.sync_data().unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"abc3456XYZ");

        fs::rename(&path, env.mount.join("renamed-open")).unwrap();
        first.seek(SeekFrom::Start(3)).unwrap();
        first.write_all(b"R").unwrap();
        first.sync_all().unwrap();
        drop(first);
        drop(second);
        assert_eq!(
            fs::read(env.mount.join("renamed-open")).unwrap(),
            b"abcR456XYZ"
        );

        let unlinked = env.mount.join("unlinked-open");
        fs::write(&unlinked, b"still-readable").unwrap();
        let mut open = File::open(&unlinked).unwrap();
        fs::remove_file(&unlinked).unwrap();
        let mut actual = Vec::new();
        open.read_to_end(&mut actual).unwrap();
        assert_eq!(actual, b"still-readable");
        drop(open);
        mount.shutdown();
    }
}

mod concurrency {
    use super::*;

    #[test]
    #[ignore = "requires Linux /dev/fuse"]
    fn ordered_and_disjoint_writes() {
        let env = TestEnv::new("concurrency");
        let mount = env.mount(false);
        for iteration in 0..20u8 {
            let path = env.mount.join(format!("parallel-{iteration}"));
            let file = File::create(&path).unwrap();
            file.set_len(128 << 10).unwrap();
            drop(file);
            let barrier = Arc::new(Barrier::new(3));
            let mut workers = Vec::new();
            for (offset, value) in [(0, iteration), (64 << 10, iteration.wrapping_add(1))] {
                let path = path.clone();
                let barrier = barrier.clone();
                workers.push(thread::spawn(move || {
                    let mut file = OpenOptions::new().write(true).open(path).unwrap();
                    barrier.wait();
                    file.seek(SeekFrom::Start(offset)).unwrap();
                    file.write_all(&vec![value; 64 << 10]).unwrap();
                    file.sync_all().unwrap();
                }));
            }
            barrier.wait();
            for worker in workers {
                worker.join().unwrap();
            }
            let bytes = fs::read(&path).unwrap();
            assert!(bytes[..64 << 10].iter().all(|byte| *byte == iteration));
            assert!(
                bytes[64 << 10..]
                    .iter()
                    .all(|byte| *byte == iteration.wrapping_add(1))
            );

            let (first_done_tx, first_done_rx) = mpsc::channel();
            let ordered_path = path.clone();
            let first = thread::spawn(move || {
                let mut file = OpenOptions::new().write(true).open(&ordered_path).unwrap();
                file.seek(SeekFrom::Start(1024)).unwrap();
                file.write_all(&vec![b'A'; 4096]).unwrap();
                file.sync_all().unwrap();
                first_done_tx.send(()).unwrap();
            });
            first_done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            let mut file = OpenOptions::new().write(true).open(&path).unwrap();
            file.seek(SeekFrom::Start(2048)).unwrap();
            file.write_all(&vec![b'B'; 1024]).unwrap();
            file.sync_all().unwrap();
            drop(file);
            first.join().unwrap();
            let bytes = fs::read(&path).unwrap();
            assert!(bytes[1024..2048].iter().all(|byte| *byte == b'A'));
            assert!(bytes[2048..3072].iter().all(|byte| *byte == b'B'));
            assert!(bytes[3072..5120].iter().all(|byte| *byte == b'A'));
        }
        mount.shutdown();
    }
}

mod lifecycle {
    use super::*;

    #[test]
    #[ignore = "requires Linux /dev/fuse"]
    fn clean_remount_and_read_only() {
        let env = TestEnv::new("remount");
        let path = env.mount.join("persistent");
        let mount = env.mount(false);
        let mut file = File::create(&path).unwrap();
        file.seek(SeekFrom::Start(4096)).unwrap();
        file.write_all(b"persistent").unwrap();
        file.sync_all().unwrap();
        drop(file);
        fs::create_dir(env.mount.join("directory")).unwrap();
        mount.shutdown();

        let mount = env.mount(false);
        let bytes = fs::read(&path).unwrap();
        assert!(bytes[..4096].iter().all(|byte| *byte == 0));
        assert_eq!(&bytes[4096..], b"persistent");
        assert!(env.mount.join("directory").is_dir());
        mount.shutdown();

        let mount = env.mount(true);
        assert_eq!(&fs::read(&path).unwrap()[4096..], b"persistent");
        assert_errno(
            fs::write(env.mount.join("new"), b"x"),
            libc::EROFS,
            "create",
        );
        assert_errno(
            OpenOptions::new().write(true).open(&path),
            libc::EROFS,
            "open for write",
        );
        assert_errno(fs::remove_file(&path), libc::EROFS, "unlink");
        mount.shutdown();

        let output = env.invalid_mount_output();
        assert!(!output.status.success(), "invalid object storage mounted");
        assert!(!is_mounted(&env.mount));
    }

    #[test]
    #[ignore = "requires Linux /dev/fuse"]
    fn crash_after_fsync() {
        let env = TestEnv::new("crash-fsync");
        let expected = pattern((1 << 20) + 37, 41);
        let path = env.mount.join("remote-durable");
        let mount = env.mount(false);
        let mut file = File::create(&path).unwrap();
        file.write_all(&expected).unwrap();
        file.sync_all().unwrap();
        mount.kill();
        drop(file);
        assert!(has_regular_file(&env.objects));

        env.assert_owned(&env.stage);
        fs::remove_dir_all(&env.stage).unwrap();
        fs::create_dir(&env.stage).unwrap();

        let mount = env.mount(false);
        assert_eq!(fs::read(&path).unwrap(), expected);
        mount.shutdown();
    }

    #[test]
    #[ignore = "requires Linux /dev/fuse"]
    fn crash_after_local_flush() {
        let env = TestEnv::new("crash-flush");
        let expected = pattern((1 << 20) + 19, 73);
        let path = env.mount.join("local-durable");
        let mount = env.mount(false);
        env.assert_owned(&env.objects);
        fs::remove_dir(&env.objects).unwrap();
        fs::write(&env.objects, b"block remote object writes").unwrap();

        let mut file = File::create(&path).unwrap();
        file.write_all(&expected).unwrap();
        file.flush().unwrap();
        drop(file);
        wait_until(READY_TIMEOUT, || has_regular_file(&env.stage))
            .expect("local stage did not become durable");
        mount.kill();

        fs::remove_file(&env.objects).unwrap();
        fs::create_dir(&env.objects).unwrap();
        let mount = env.mount(false);
        assert_eq!(fs::read(&path).unwrap(), expected);
        mount.shutdown();
    }
}

mod unsupported {
    use super::*;

    #[test]
    #[ignore = "requires Linux /dev/fuse"]
    fn stable_errno_keeps_mount_alive() {
        let env = TestEnv::new("unsupported");
        let mount = env.mount(false);
        let path = env.mount.join("fallocate");
        let file = File::create(&path).unwrap();
        let error = rustix::fs::fallocate(&file, FallocateFlags::empty(), 0, 4096).unwrap_err();
        assert_eq!(error.raw_os_error(), libc::EOPNOTSUPP);
        drop(file);

        fs::write(&path, b"mount remains alive").unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"mount remains alive");
        mount.shutdown();
    }
}
