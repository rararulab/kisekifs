// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    convert::Infallible,
    fmt::{Debug, Formatter},
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use clap::{ArgAction, Args};
use fs4::FileExt;
use fuser::MountOption;
use kiseki_common::KISEKI;
use kiseki_fuse::{FuseConfig, null};
use kiseki_meta::MetaConfig;
use kiseki_utils::{
    logger::LoggingOptions, object_storage::ObjectStorageConfig, readable_size::ReadableSize,
};
use kiseki_vfs::{Config as VFSConfig, KisekiVFS, LifecycleState, ShutdownPolicy};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Whatever, whatever};
use tracing::info;

use crate::build_info;

const MOUNT_OPTIONS_HEADER: &str = "Mount options";
const LOGGING_OPTIONS_HEADER: &str = "Logging options";
const META_OPTIONS_HEADER: &str = "Meta options";
const STORAGE_OPTIONS_HEADER: &str = "Object storage options";
const CACHE_OPTIONS_HEADER: &str = "Cache options";
const MOUNT_READY_TIMEOUT: Duration = Duration::from_secs(30);

fn parse_positive_usize(value: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|error| format!("expected a positive integer: {error}"))?;
    if parsed == 0 {
        return Err("value must be greater than zero".to_string());
    }
    Ok(parsed)
}

#[derive(Clone)]
pub struct ObjectStorageDsn(String);

impl Debug for ObjectStorageDsn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("ObjectStorageDsn(<redacted>)")
    }
}

impl FromStr for ObjectStorageDsn {
    type Err = Infallible;

    fn from_str(dsn: &str) -> Result<Self, Self::Err> { Ok(Self(dsn.to_string())) }
}

#[derive(Debug, Deserialize, Serialize)]
struct ReadyFilePayload {
    version:     String,
    pid:         u32,
    token:       String,
    mount_point: String,
    volume:      String,
    state:       String,
}

struct ReadyFileGuard {
    path:     PathBuf,
    pid:      u32,
    token:    String,
    identity: ReadyFileIdentity,
    _lease:   File,
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ReadyFileIdentity {
    device: u64,
    inode:  u64,
}

#[cfg(not(unix))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ReadyFileIdentity {
    length:   u64,
    modified: Option<SystemTime>,
}

impl ReadyFileGuard {
    fn create(path: PathBuf, mount_point: &Path, volume: &str) -> Result<Self, Whatever> {
        if !path.is_absolute() {
            whatever!("ready file path must be absolute");
        }
        let parent = path
            .parent()
            .with_whatever_context(|| "ready file has no parent directory".to_string())?;
        if !parent.is_dir() {
            whatever!("ready file parent {} is not a directory", parent.display());
        }

        let pid = std::process::id();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let token = format!("{pid}-{timestamp}");
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .with_whatever_context(|| "ready file name must be valid UTF-8".to_string())?;
        let lease_path = parent.join(format!(".{file_name}.lock"));
        let lease = acquire_ready_file_lease(&lease_path).with_whatever_context(|error| {
            format!(
                "failed to acquire ready file lease {}: {error}",
                lease_path.display()
            )
        })?;
        let temp_path = parent.join(format!(".{file_name}.tmp-{token}"));
        let payload = ReadyFilePayload {
            version: build_info::PKG_VERSION.to_string(),
            pid,
            token: token.clone(),
            mount_point: mount_point.display().to_string(),
            volume: volume.to_string(),
            state: "ready".to_string(),
        };
        let bytes = serde_json::to_vec(&payload)
            .with_whatever_context(|error| format!("failed to serialize ready file: {error}"))?;

        let publish_result = (|| -> std::io::Result<()> {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temp_path)?;
            file.write_all(&bytes)?;
            file.write_all(b"\n")?;
            file.sync_all()?;
            match std::fs::hard_link(&temp_path, &path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    reclaim_stale_ready_file(&path)?;
                    std::fs::hard_link(&temp_path, &path)?;
                }
                Err(error) => return Err(error),
            }
            Ok(())
        })();
        let _ = std::fs::remove_file(&temp_path);
        publish_result.with_whatever_context(|error| {
            format!(
                "failed to atomically publish ready file {}: {error}",
                path.display()
            )
        })?;

        let identity = ready_file_identity(&path).with_whatever_context(|error| {
            format!(
                "failed to identify published ready file {}: {error}",
                path.display()
            )
        })?;

        Ok(Self {
            path,
            pid,
            token,
            identity,
            _lease: lease,
        })
    }

    fn remove_if_owned(&self) { self.remove_if_owned_after_retirement(|| {}); }

    fn remove_if_owned_after_retirement(&self, after_retirement: impl FnOnce()) {
        let Ok(retired) = retire_ready_file(&self.path) else {
            return;
        };
        after_retirement();
        let owned = ready_file_identity(&retired).ok() == Some(self.identity)
            && std::fs::read(&retired)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<ReadyFilePayload>(&bytes).ok())
                .is_some_and(|payload| payload.pid == self.pid && payload.token == self.token);
        if owned {
            let _ = std::fs::remove_file(&retired);
        } else if let Err(error) = restore_retired_ready_file(&retired, &self.path) {
            tracing::warn!(
                retired = %retired.display(),
                path = %self.path.display(),
                %error,
                "preserved a concurrently replaced ready record under its retirement name"
            );
        }
    }
}

fn retire_ready_file(path: &Path) -> std::io::Result<PathBuf> {
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "ready path has no parent")
    })?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "ready file name must be valid UTF-8",
            )
        })?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let retired = parent.join(format!(".{name}.retired-{}-{nonce}", std::process::id()));
    rename_ready_file_no_replace(path, &retired)?;
    Ok(retired)
}

fn restore_retired_ready_file(retired: &Path, path: &Path) -> std::io::Result<()> {
    rename_ready_file_no_replace(retired, path)
}

#[cfg(target_os = "linux")]
fn rename_ready_file_no_replace(source: &Path, destination: &Path) -> std::io::Result<()> {
    rustix::fs::renameat_with(
        rustix::fs::CWD,
        source,
        rustix::fs::CWD,
        destination,
        rustix::fs::RenameFlags::NOREPLACE,
    )
    .map_err(|error| std::io::Error::from_raw_os_error(error.raw_os_error()))
}

#[cfg(not(target_os = "linux"))]
fn rename_ready_file_no_replace(source: &Path, destination: &Path) -> std::io::Result<()> {
    if destination.exists() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "ready destination already exists",
        ));
    }
    std::fs::rename(source, destination)
}

fn acquire_ready_file_lease(path: &Path) -> std::io::Result<File> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            #[cfg(unix)]
            let unshared = {
                use std::os::unix::fs::MetadataExt;
                metadata.nlink() == 1
            };
            #[cfg(not(unix))]
            let unshared = true;
            if !metadata.is_file() || metadata.file_type().is_symlink() || !unshared {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "ready file lease must be an unshared regular file, not a symlink",
                ));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let lease = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    FileExt::try_lock_exclusive(&lease)?;
    Ok(lease)
}

#[cfg(unix)]
fn ready_file_identity(path: &Path) -> std::io::Result<ReadyFileIdentity> {
    use std::os::unix::fs::MetadataExt;

    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "ready path is not a regular file",
        ));
    }
    Ok(ReadyFileIdentity {
        device: metadata.dev(),
        inode:  metadata.ino(),
    })
}

#[cfg(not(unix))]
fn ready_file_identity(path: &Path) -> std::io::Result<ReadyFileIdentity> {
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "ready path is not a regular file",
        ));
    }
    Ok(ReadyFileIdentity {
        length:   metadata.len(),
        modified: metadata.modified().ok(),
    })
}

impl Drop for ReadyFileGuard {
    fn drop(&mut self) { self.remove_if_owned() }
}

fn reclaim_stale_ready_file(path: &Path) -> std::io::Result<()> {
    reclaim_stale_ready_file_after_retirement(path, || {})
}

fn reclaim_stale_ready_file_after_retirement(
    path: &Path,
    after_retirement: impl FnOnce(),
) -> std::io::Result<()> {
    let retired = retire_ready_file(path)?;
    after_retirement();
    // Inspect the file type without following links before any blocking read.
    // Unknown entries (symlink, FIFO, directory, device) are restored and
    // rejected rather than interpreted as KisekiFS ownership records.
    let reclaimable = ready_file_identity(&retired).is_ok()
        && std::fs::read(&retired)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<ReadyFilePayload>(&bytes).ok())
            .is_some_and(|payload| {
                payload.pid != 0
                    && !payload.token.is_empty()
                    && !payload.mount_point.is_empty()
                    && !payload.volume.is_empty()
                    && payload.state == "ready"
                    && ready_file_owner_is_dead(payload.pid)
            });
    if reclaimable {
        return std::fs::remove_file(retired);
    }

    let restore_error = restore_retired_ready_file(&retired, path).err();
    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        restore_error.map_or_else(
            || "ready file owner is live, malformed, or unverifiable".to_string(),
            |error| {
                format!(
                    "ready file owner is live, malformed, or unverifiable; the captured record \
                     was preserved at {} because its public path is occupied: {error}",
                    retired.display()
                )
            },
        ),
    ))
}

#[cfg(target_os = "linux")]
fn ready_file_owner_is_dead(pid: u32) -> bool {
    match std::fs::metadata(format!("/proc/{pid}")) {
        Ok(_) => false,
        Err(error) => error.kind() == std::io::ErrorKind::NotFound,
    }
}

#[cfg(not(target_os = "linux"))]
fn ready_file_owner_is_dead(_pid: u32) -> bool { false }

#[derive(Debug, Clone, Args)]
#[command(flatten_help = true)]
#[command(long_about = r"

Mount the target volume at the mount point.
Examples:

# Mount in foreground
kiseki mount -f /tmp/kiseki
")]
pub struct MountArgs {
    #[arg(
        help = "Directory to mount the fs at",
        value_name = "MOUNT_POINT",
        default_value = "/tmp/kiseki"
    )]
    pub mount_point: PathBuf,

    #[arg(
    long,
    help = "Mount file system in read-only mode",
    help_heading = MOUNT_OPTIONS_HEADER
    )]
    pub read_only: bool,

    #[arg(
    long,
    help = "Automatically unmount on exit",
    help_heading = MOUNT_OPTIONS_HEADER,
    default_value = "true",
    action = ArgAction::Set,
    )]
    pub auto_unmount: bool,

    #[arg(long, help = "Allow root user to access file system", help_heading = MOUNT_OPTIONS_HEADER)]
    pub allow_root: bool,

    #[arg(
    long,
    help = "Allow other users, including root, to access file system",
    help_heading = MOUNT_OPTIONS_HEADER,
    conflicts_with = "allow_root",
    default_value_t = true,
    action = ArgAction::Set,
    )]
    pub allow_other: bool,

    #[arg(
    long,
    help = "Number of threads to use for tokio async runtime",
    help_heading = MOUNT_OPTIONS_HEADER,
    default_value = "10",
    value_parser = parse_positive_usize,
    )]
    pub async_work_threads: usize,

    #[arg(
    long,
    help = "FUSE backend: 'rara' (fuse-backend-rs, default) or 'fuser' (legacy)",
    help_heading = MOUNT_OPTIONS_HEADER,
    default_value = "rara",
    )]
    pub fuse_backend: String,

    #[clap(
    long,
    help = "Write log files to a directory [default: logs written to syslog]",
    help_heading = LOGGING_OPTIONS_HEADER,
    value_name = "DIRECTORY",
    default_value = "/tmp/kiseki.log"
    )]
    pub log_directory: String,

    #[clap(
    short,
    long,
    help = "Log level",
    help_heading = LOGGING_OPTIONS_HEADER,
    value_name = "LEVEL",
    default_value = "info"
    )]
    pub level: Option<String>,

    #[clap(
    long,
    help = "Enable OTLP tracing",
    help_heading = LOGGING_OPTIONS_HEADER,
    default_value = "true"
    )]
    pub enable_otlp_tracing: bool,

    #[clap(
    long,
    help = "Specify the OTLP endpoint",
    help_heading = LOGGING_OPTIONS_HEADER,
    value_name = "URL",
    default_value = "localhost:4317",
    )]
    pub otlp_endpoint: Option<String>,

    #[clap(
    long,
    help = "Specify the tracing sample ratio",
    help_heading = LOGGING_OPTIONS_HEADER,
    default_value = "0.5",
    value_name = "RATIO",
    )]
    pub tracing_sample_ratio: Option<f64>,

    #[clap(
    long,
    help = "Append stdout to log files",
    help_heading = LOGGING_OPTIONS_HEADER,
    default_value = "true",
    )]
    pub append_stdout: bool,

    #[clap(
    long,
    help = "Disable all logging. You will still see stdout messages.",
    help_heading = LOGGING_OPTIONS_HEADER,
    conflicts_with_all(["log_directory", "level", "enable_otlp_tracing"])
    )]
    pub no_log: bool,

    #[clap(
        short,
        long,
        help = "Run as foreground process",
        default_value = "true"
    )]
    pub foreground: bool,

    #[arg(
    long,
    help = "Specify the address of the meta store",
    help_heading = META_OPTIONS_HEADER,
    default_value = kiseki_common::KISEKI_DEBUG_META_ADDR,
    )]
    pub meta_dsn: String,

    #[arg(
        long,
        value_name = "DSN",
        help = "Object store: file:///absolute/path or s3://bucket[/prefix]",
        long_help = "Object store DSN. Use file:///absolute/path for local storage or \
                     s3://bucket[/prefix] for S3. S3 credentials are loaded from the standard \
                     AWS environment or identity chain; credentials in the DSN are rejected.",
        help_heading = STORAGE_OPTIONS_HEADER,
        required = true,
    )]
    pub object_storage: ObjectStorageDsn,

    #[arg(
        long,
        value_name = "DIRECTORY",
        help = "Mount-specific root for page, read, and restart-recoverable stage caches",
        help_heading = CACHE_OPTIONS_HEADER,
        required = true,
    )]
    pub cache_dir: PathBuf,

    #[arg(
        long,
        value_name = "SIZE",
        help = "In-memory page pool capacity",
        help_heading = CACHE_OPTIONS_HEADER,
        default_value = "300MiB",
    )]
    pub memory_page_capacity: ReadableSize,

    #[arg(
        long,
        value_name = "SIZE",
        help = "Optional disk-backed page spill capacity",
        help_heading = CACHE_OPTIONS_HEADER,
    )]
    pub disk_page_capacity: Option<ReadableSize>,

    #[arg(
        long,
        value_name = "SIZE",
        help = "Maximum restart-recoverable stage cache size",
        help_heading = CACHE_OPTIONS_HEADER,
        default_value = "10GiB",
    )]
    pub stage_cache_capacity: ReadableSize,

    #[arg(
        long,
        value_name = "DURATION",
        help = "Time before a staged block is scheduled for remote migration",
        help_heading = CACHE_OPTIONS_HEADER,
        default_value = "24h",
        value_parser = humantime::parse_duration,
    )]
    pub stage_cache_ttl: Duration,

    #[arg(
        long,
        value_name = "SIZE",
        help = "In-memory read cache capacity",
        help_heading = CACHE_OPTIONS_HEADER,
        default_value = "1GiB",
    )]
    pub memory_read_cache_capacity: ReadableSize,

    #[arg(
        long,
        value_name = "DURATION",
        help = "Maximum graceful shutdown duration",
        help_heading = MOUNT_OPTIONS_HEADER,
        default_value = "30s",
        value_parser = humantime::parse_duration,
    )]
    pub shutdown_deadline: Duration,

    #[arg(
        long,
        value_name = "POLICY",
        help = "Shutdown durability boundary: local or remote",
        help_heading = MOUNT_OPTIONS_HEADER,
        default_value = "local",
    )]
    pub shutdown_policy: ShutdownPolicy,

    #[arg(
        long,
        value_name = "PATH",
        help = "Atomically publish mount readiness to this file",
        help_heading = MOUNT_OPTIONS_HEADER,
    )]
    pub ready_file: Option<PathBuf>,
}

impl MountArgs {
    fn fuse_config(&self) -> FuseConfig {
        let mut options = vec![
            MountOption::DefaultPermissions,
            MountOption::FSName(KISEKI.to_string()),
            MountOption::NoAtime,
        ];
        if self.read_only {
            options.push(MountOption::RO);
        }
        if self.auto_unmount {
            options.push(MountOption::AutoUnmount);
        }
        if self.allow_root {
            options.push(MountOption::AllowRoot);
        }
        if self.allow_other {
            options.push(MountOption::AllowOther);
        }
        FuseConfig {
            mount_point:        self.mount_point.clone(),
            mount_options:      options,
            async_work_threads: self.async_work_threads,
        }
    }

    fn meta_config(&self) -> Result<MetaConfig, Whatever> {
        let mut mc = MetaConfig::default();
        mc.with_dsn(&self.meta_dsn);
        Ok(mc)
    }

    fn load_logging_opts(&self) -> Option<LoggingOptions> {
        if self.no_log {
            return None;
        }
        let opts = LoggingOptions {
            dir:                  self.log_directory.clone(),
            level:                self.level.clone(),
            enable_otlp_tracing:  self.enable_otlp_tracing,
            otlp_endpoint:        self.otlp_endpoint.clone(),
            tracing_sample_ratio: self.tracing_sample_ratio,
            append_stdout:        self.append_stdout,
            tokio_console_addr:   Some(
                kiseki_utils::logger::DEFAULT_TOKIO_CONSOLE_ADDR.to_string(),
            ),
        };
        Some(opts)
    }

    fn vfs_config(&self) -> Result<VFSConfig, Whatever> {
        let object_storage = self
            .object_storage
            .0
            .parse::<ObjectStorageConfig>()
            .with_whatever_context(|error| {
                format!("invalid object storage configuration: {error}")
            })?;
        if matches!(object_storage, ObjectStorageConfig::Memory) {
            whatever!("memory object storage is available only in tests");
        }
        let config = VFSConfig {
            object_storage,
            cache_dir: self.cache_dir.clone(),
            memory_page_capacity: self.memory_page_capacity,
            disk_page_capacity: self.disk_page_capacity,
            stage_cache_capacity: self.stage_cache_capacity,
            stage_cache_ttl: self.stage_cache_ttl,
            memory_read_cache_capacity: self.memory_read_cache_capacity,
            shutdown_deadline: self.shutdown_deadline,
            shutdown_policy: self.shutdown_policy,
            ..VFSConfig::default()
        };
        config
            .validate_mount_paths(&self.mount_point, self.ready_file.as_deref())
            .with_whatever_context(|error| format!("invalid mount cache configuration: {error}"))?;
        Ok(config)
    }

    pub fn run(self) -> Result<(), Whatever> {
        // the `setup_panic!` expansion still uses the deprecated
        // `std::panic::PanicInfo` alias; nothing to fix on our side.
        #[allow(deprecated)]
        {
            human_panic::setup_panic!();
        }
        kiseki_utils::panic_hook::set_panic_hook();

        if self.foreground {
            let logging_guard = self.load_logging_opts().map(|opts| {
                kiseki_utils::logger::init_global_logging_without_runtime("kiseki-fuse", &opts)
            });

            let pyroscope_guard = kiseki_utils::pyroscope_init::init_pyroscope()?;

            mount(self)?;

            if let Some(agent_running) = pyroscope_guard {
                // Stop Agent
                let agent_ready = agent_running
                    .stop()
                    .with_whatever_context(|e| format!("failed to stop pyroscope agent {e} "))?;

                // Shutdown the Agent
                agent_ready.shutdown();
            }
            if let Some(logging_guard) = logging_guard {
                logging_guard.shutdown(Duration::from_secs(2));
            }
        }
        Ok(())
    }
}

pub fn print_versions() {
    // Report app version as gauge.
    // APP_VERSION
    //     .with_label_values(&[short_version(), full_version()])
    //     .inc();

    // Report the build version without dumping process arguments.
    println!(
        "PKG_VERSION: {}, FULL_VERSION: {}",
        build_info::PKG_VERSION,
        build_info::FULL_VERSION,
    );
}

fn mount(args: MountArgs) -> Result<(), Whatever> {
    info!("try to mount kiseki on {:?}", &args.mount_point);
    print_versions();

    // Signals are process-scoped, so install the handler before storage
    // probing or recovery begins. Startup polls the same latch as the mounted
    // session and can be cancelled without publishing readiness.
    let shutdown_requested = Arc::new(AtomicBool::new(false));
    let signal_latch = shutdown_requested.clone();
    ctrlc::set_handler(move || {
        signal_latch.store(true, Ordering::Release);
    })
    .with_whatever_context(|error| format!("failed to install signal handler: {error}"))?;

    let fuse_config = args.fuse_config();
    let meta_config = args.meta_config()?;
    let vfs_config = args.vfs_config()?;
    let ready_file_path = args.ready_file.clone();

    validate_mount_point(&args.mount_point)?;

    if shutdown_requested.load(Ordering::Acquire) {
        return Ok(());
    }

    let meta = kiseki_meta::open(meta_config)
        .with_whatever_context(|e| format!("failed to open meta, {e:?}"))?;

    if args.fuse_backend != "fuser" {
        // Default: fuse-backend-rs. The VFS keeps its own long-lived
        // multi-threaded runtime; the fusedev worker threads bridge into it via
        // block_on.
        let threads = args.async_work_threads.max(1);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads)
            .thread_name("kiseki-vfs")
            .enable_all()
            .build()
            .with_whatever_context(|error| format!("failed to build vfs runtime: {error}"))?;
        let vfs = runtime
            .block_on(KisekiVFS::new_checked(vfs_config, meta))
            .with_whatever_context(|e| format!("failed to create file system, {e:?}"))?;
        let vfs = std::sync::Arc::new(vfs);
        runtime
            .block_on(vfs.init(&kiseki_meta::context::FuseContext::background()))
            .with_whatever_context(|e| format!("failed to initialize file system, {e:?}"))?;
        kiseki_fuse::fbr::mount_and_serve(
            vfs,
            runtime.handle().clone(),
            &args.mount_point,
            KISEKI,
            args.allow_other,
            args.read_only,
            threads,
        )
        .with_whatever_context(|e| {
            format!("failed to mount kiseki on {}; {e}", args.mount_point.display())
        })?;
        drop(runtime);
        return Ok(());
    }

    let fuse_runtime = kiseki_fuse::KisekiFuse::build_runtime(&fuse_config)?;
    let shutdown_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .with_whatever_context(|error| format!("failed to build shutdown runtime: {error}"))?;
    let startup_signal = shutdown_requested.clone();
    let file_system = match fuse_runtime.block_on(async {
        tokio::select! {
            result = KisekiVFS::new_checked(vfs_config, meta) => Some(result),
            () = wait_for_shutdown_request(startup_signal) => None,
        }
    }) {
        Some(result) => Arc::new(
            result.with_whatever_context(|e| format!("failed to create file system, {e:?}"))?,
        ),
        None => return Ok(()),
    };

    // Build all remaining process-level shutdown primitives before the FUSE
    // session owns a live kernel mount. The FUSE runtime above is deliberately
    // retained: recovery workers were spawned onto it during VFS startup.
    if shutdown_requested.load(Ordering::Acquire) {
        let result =
            fuse_runtime.block_on(file_system.shutdown(file_system.config.shutdown_deadline));
        result.with_whatever_context(|error| {
            format!("mount startup cancellation failed to drain cleanly: {error}")
        })?;
        return Ok(());
    }

    let fs =
        kiseki_fuse::KisekiFuse::create(fuse_config.clone(), file_system.clone(), fuse_runtime);
    let mut session = match fuser::Session::new(fs, &args.mount_point, &fuse_config.mount_options) {
        Ok(session) => session,
        Err(error) => {
            let shutdown = shutdown_runtime
                .block_on(file_system.shutdown(file_system.config.shutdown_deadline));
            if let Err(shutdown_error) = shutdown {
                whatever!(
                    "failed to mount kiseki on {}; {error}; cleanup failed: {shutdown_error}",
                    args.mount_point.display()
                );
            }
            whatever!(
                "failed to mount kiseki on {}; {error}",
                args.mount_point.display()
            );
        }
    };
    let mut unmounter = session.unmount_callable();
    let session_guard = match thread::Builder::new()
        .name("kiseki-fuse-session".to_string())
        .spawn(move || session.run())
    {
        Ok(session_guard) => session_guard,
        Err(error) => {
            let shutdown = shutdown_runtime
                .block_on(file_system.shutdown(file_system.config.shutdown_deadline));
            let suffix = shutdown
                .err()
                .map(|error| format!("; cleanup failed: {error}"))
                .unwrap_or_default();
            whatever!("failed to start FUSE session thread: {error}{suffix}");
        }
    };

    let startup_deadline = Instant::now() + MOUNT_READY_TIMEOUT;
    let mut received_signal = false;
    let mut startup_error = None;
    loop {
        if file_system.lifecycle_state() == LifecycleState::Ready
            && std::fs::metadata(&args.mount_point).is_ok()
        {
            break;
        }
        if file_system.lifecycle_state() == LifecycleState::Failed {
            startup_error = Some("filesystem initialization failed before readiness".to_string());
            break;
        }
        if session_guard.is_finished() {
            startup_error = Some("FUSE session exited before readiness".to_string());
            break;
        }
        if shutdown_requested.load(Ordering::Acquire) {
            received_signal = true;
            break;
        }
        thread::sleep(Duration::from_millis(25));
        if Instant::now() >= startup_deadline {
            startup_error = Some("timed out waiting for truthful mount readiness".to_string());
            break;
        }
    }

    let ready_file = if !received_signal && startup_error.is_none() {
        match ready_file_path {
            Some(path) => {
                match ReadyFileGuard::create(path, &args.mount_point, file_system.volume_name()) {
                    Ok(guard) => Some(guard),
                    Err(error) => {
                        startup_error = Some(error.to_string());
                        None
                    }
                }
            }
            None => None,
        }
    } else {
        None
    };

    if !received_signal && startup_error.is_none() {
        loop {
            if session_guard.is_finished() {
                break;
            }
            if shutdown_requested.load(Ordering::Acquire) {
                received_signal = true;
                break;
            }
            thread::sleep(Duration::from_millis(25));
        }
    }

    let termination_deadline = Instant::now() + file_system.config.shutdown_deadline;
    drop(ready_file);
    let should_unmount = received_signal || startup_error.is_some();
    let initiated_shutdown = should_unmount.then(|| {
        shutdown_runtime.block_on(file_system.shutdown(file_system.config.shutdown_deadline))
    });
    let unmount_error = should_unmount.then(|| unmounter.unmount().err()).flatten();

    let session_outcome = join_session_until(session_guard, termination_deadline);
    let detach_error = if should_unmount && session_outcome.timed_out {
        detach_mount_after_timeout(&args.mount_point)
    } else {
        None
    };
    let shutdown_result = initiated_shutdown.unwrap_or_else(|| {
        shutdown_runtime.block_on(file_system.shutdown(file_system.config.shutdown_deadline))
    });

    let mut terminal_errors = Vec::new();
    if let Some(error) = startup_error {
        terminal_errors.push(format!("mount startup failed: {error}"));
    }
    if let Some(error) = unmount_error {
        terminal_errors.push(format!("failed to unmount during shutdown: {error}"));
    }
    if let Some(error) = session_outcome.error {
        terminal_errors.push(error);
    }
    if let Some(error) = detach_error {
        terminal_errors.push(error);
    }
    if let Err(error) = shutdown_result {
        terminal_errors.push(format!("mount shutdown failed: {error}"));
    }
    if !terminal_errors.is_empty() {
        whatever!(
            "mount terminated with errors: {}",
            terminal_errors.join("; ")
        );
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn detach_mount_after_timeout(mount_point: &Path) -> Option<String> {
    match rustix::mount::unmount(mount_point, rustix::mount::UnmountFlags::DETACH) {
        Ok(()) | Err(rustix::io::Errno::INVAL | rustix::io::Errno::NOENT) => None,
        Err(error) => Some(format!(
            "failed to detach FUSE mount after session timeout: {error}"
        )),
    }
}

#[cfg(not(target_os = "linux"))]
fn detach_mount_after_timeout(_mount_point: &Path) -> Option<String> { None }

struct SessionJoinOutcome {
    error:     Option<String>,
    timed_out: bool,
}

async fn wait_for_shutdown_request(requested: Arc<AtomicBool>) {
    while !requested.load(Ordering::Acquire) {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn join_session_until(
    session: thread::JoinHandle<std::io::Result<()>>,
    deadline: Instant,
) -> SessionJoinOutcome {
    while !session.is_finished() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    if !session.is_finished() {
        return SessionJoinOutcome {
            error:     Some("FUSE session did not exit before the shutdown deadline".to_string()),
            timed_out: true,
        };
    }
    let error = match session.join() {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(format!("FUSE session failed: {error}")),
        Err(_) => Some("FUSE session thread panicked".to_string()),
    };
    SessionJoinOutcome {
        error,
        timed_out: false,
    }
}

fn validate_mount_point(path: impl AsRef<Path>) -> Result<(), Whatever> {
    let mount_point = path.as_ref();
    if !mount_point.exists() {
        whatever!("mount point {} does not exist", mount_point.display());
    }

    if !mount_point.is_dir() {
        whatever!("mount point {} is not a directory", mount_point.display());
    }

    #[cfg(target_os = "linux")]
    {
        use procfs::process::Process;

        // This is a best-effort validation, so don't fail if we can't read
        // /proc/self/mountinfo for some reason.
        let mounts = match Process::myself().and_then(|me| me.mountinfo()) {
            Ok(mounts) => mounts,
            Err(e) => {
                tracing::debug!(
                    "failed to read mountinfo, not checking for existing mounts: {e:?}"
                );
                return Ok(());
            }
        };

        if mounts
            .into_iter()
            .any(|mount| mount.mount_point == path.as_ref())
        {
            whatever!("mount point {} is already mounted", path.as_ref().display());
        }
    }

    null::mount_check(path)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[derive(Parser)]
    struct TestCli {
        #[command(flatten)]
        mount: MountArgs,
    }

    #[test]
    fn session_join_outcome_distinguishes_timeout_from_completed_failure() {
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let timed_out = join_session_until(
            thread::spawn(move || {
                release_rx.recv().unwrap();
                Ok(())
            }),
            Instant::now(),
        );
        assert!(timed_out.timed_out);
        assert!(timed_out.error.is_some());
        release_tx.send(()).unwrap();

        let failed = join_session_until(
            thread::spawn(|| Err(std::io::Error::other("injected session failure"))),
            Instant::now() + Duration::from_secs(1),
        );
        assert!(!failed.timed_out);
        assert_eq!(
            failed.error.as_deref(),
            Some("FUSE session failed: injected session failure")
        );

        let panicked = join_session_until(
            thread::spawn(|| -> std::io::Result<()> { panic!("injected session panic") }),
            Instant::now() + Duration::from_secs(1),
        );
        assert!(!panicked.timed_out);
        assert_eq!(
            panicked.error.as_deref(),
            Some("FUSE session thread panicked")
        );
    }

    #[test]
    fn object_storage_cli_value_reaches_vfs_config() {
        let cli = TestCli::try_parse_from([
            "test",
            "--object-storage",
            "s3://volume-bucket/tenant/volume?region=test-region",
            "--cache-dir",
            "/tmp/kiseki-cli-test-cache",
            "/tmp/kiseki",
        ])
        .expect("parse mount arguments");

        assert_eq!(
            cli.mount.vfs_config().unwrap().object_storage,
            ObjectStorageConfig::S3 {
                bucket:     "volume-bucket".to_string(),
                prefix:     Some("tenant/volume".to_string()),
                region:     Some("test-region".to_string()),
                endpoint:   None,
                allow_http: false,
            }
        );
    }

    #[test]
    fn object_storage_is_required_and_invalid_dsns_fail_before_mounting() {
        assert!(TestCli::try_parse_from(["test", "/tmp/kiseki"]).is_err());
        let invalid = TestCli::try_parse_from([
            "test",
            "--object-storage",
            "file://relative/path",
            "--cache-dir",
            "/tmp/kiseki-cli-test-cache",
            "/tmp/kiseki",
        ])
        .unwrap();
        assert!(invalid.mount.vfs_config().is_err());

        let memory = TestCli::try_parse_from([
            "test",
            "--object-storage",
            "memory://",
            "--cache-dir",
            "/tmp/kiseki-cli-test-cache",
            "/tmp/kiseki",
        ])
        .unwrap();
        assert!(memory.mount.vfs_config().is_err());
    }

    #[test]
    fn mount_argument_debug_never_contains_storage_dsn_values() {
        let secret_marker = "do-not-echo-this-value";
        let dsn = format!("s3://user:{secret_marker}@volume-bucket/prefix");
        let cli = TestCli::try_parse_from([
            "test",
            "--object-storage",
            &dsn,
            "--cache-dir",
            "/tmp/kiseki-cli-test-cache",
            "/tmp/kiseki",
        ])
        .unwrap();

        assert!(!format!("{:?}", cli.mount).contains(secret_marker));
        let error = cli.mount.vfs_config().unwrap_err();
        assert!(!error.to_string().contains(secret_marker));
    }

    #[test]
    fn mounted_tests_can_disable_allow_other_and_isolate_the_stage_cache() {
        let cli = TestCli::try_parse_from([
            "test",
            "--auto-unmount",
            "false",
            "--object-storage",
            "file:///tmp/kiseki-objects",
            "--allow-other",
            "false",
            "--cache-dir",
            "/tmp/kiseki-cache-isolated",
            "/tmp/kiseki",
        ])
        .expect("parse isolated mount arguments");

        assert!(!cli.mount.auto_unmount);
        assert!(!cli.mount.allow_other);
        assert_eq!(
            cli.mount.vfs_config().unwrap().cache_dir,
            PathBuf::from("/tmp/kiseki-cache-isolated")
        );
    }

    #[test]
    fn cache_limits_are_readable_and_rejected_before_mounting_when_invalid() {
        let cli = TestCli::try_parse_from([
            "test",
            "--object-storage",
            "file:///tmp/kiseki-objects",
            "--cache-dir",
            "/tmp/kiseki-resource-test-cache",
            "--memory-page-capacity",
            "8MiB",
            "--disk-page-capacity",
            "16MiB",
            "--stage-cache-capacity",
            "32MiB",
            "--stage-cache-ttl",
            "5m",
            "--memory-read-cache-capacity",
            "4MiB",
            "--shutdown-deadline",
            "7s",
            "/tmp/kiseki",
        ])
        .unwrap();
        let config = cli.mount.vfs_config().unwrap();
        assert_eq!(config.memory_page_capacity, ReadableSize::mb(8));
        assert_eq!(config.disk_page_capacity, Some(ReadableSize::mb(16)));
        assert_eq!(config.stage_cache_capacity, ReadableSize::mb(32));
        assert_eq!(config.stage_cache_ttl, Duration::from_secs(300));
        assert_eq!(config.shutdown_deadline, Duration::from_secs(7));

        let invalid = TestCli::try_parse_from([
            "test",
            "--object-storage",
            "file:///tmp/kiseki-objects",
            "--cache-dir",
            "/tmp/kiseki-resource-test-cache",
            "--memory-page-capacity",
            "1B",
            "/tmp/kiseki",
        ])
        .unwrap();
        assert!(invalid.mount.vfs_config().is_err());

        let overlapping = TestCli::try_parse_from([
            "test",
            "--object-storage",
            "file:///tmp/kiseki-objects",
            "--cache-dir",
            "/tmp/kiseki/cache",
            "/tmp/kiseki",
        ])
        .unwrap();
        assert!(overlapping.mount.vfs_config().is_err());

        assert!(
            TestCli::try_parse_from([
                "test",
                "--object-storage",
                "file:///tmp/kiseki-objects",
                "--cache-dir",
                "/tmp/kiseki-resource-test-cache",
                "--async-work-threads",
                "0",
                "/tmp/kiseki",
            ])
            .is_err()
        );
    }

    #[test]
    fn ready_file_cleanup_removes_only_the_publishers_token() {
        let tempdir = tempfile::tempdir().unwrap();
        let mount_point = tempdir.path().join("mount");
        std::fs::create_dir(&mount_point).unwrap();
        let ready_path = tempdir.path().join("ready.json");

        let guard = ReadyFileGuard::create(ready_path.clone(), &mount_point, "volume").unwrap();
        let payload: ReadyFilePayload =
            serde_json::from_slice(&std::fs::read(&ready_path).unwrap()).unwrap();
        assert_eq!(payload.pid, std::process::id());
        assert_eq!(payload.state, "ready");
        drop(guard);
        assert!(!ready_path.exists());

        let guard = ReadyFileGuard::create(ready_path.clone(), &mount_point, "volume").unwrap();
        std::fs::write(&ready_path, b"foreign replacement").unwrap();
        drop(guard);
        assert_eq!(std::fs::read(&ready_path).unwrap(), b"foreign replacement");
    }

    #[test]
    fn ready_file_cleanup_never_unlinks_a_replacement_published_during_retirement() {
        let tempdir = tempfile::tempdir().unwrap();
        let mount_point = tempdir.path().join("mount");
        std::fs::create_dir(&mount_point).unwrap();
        let ready_path = tempdir.path().join("ready.json");
        let guard = ReadyFileGuard::create(ready_path.clone(), &mount_point, "volume").unwrap();

        guard.remove_if_owned_after_retirement(|| {
            std::fs::write(&ready_path, b"concurrent replacement").unwrap();
        });
        assert_eq!(
            std::fs::read(&ready_path).unwrap(),
            b"concurrent replacement"
        );
        drop(guard);
        assert_eq!(
            std::fs::read(&ready_path).unwrap(),
            b"concurrent replacement"
        );
    }

    #[cfg(unix)]
    #[test]
    fn ready_file_cleanup_restores_an_unknown_symlink_without_following_it() {
        use std::os::unix::fs::symlink;

        let tempdir = tempfile::tempdir().unwrap();
        let mount_point = tempdir.path().join("mount");
        std::fs::create_dir(&mount_point).unwrap();
        let ready_path = tempdir.path().join("ready.json");
        let target_path = tempdir.path().join("foreign-target");
        std::fs::write(&target_path, b"foreign").unwrap();
        let guard = ReadyFileGuard::create(ready_path.clone(), &mount_point, "volume").unwrap();
        std::fs::remove_file(&ready_path).unwrap();
        symlink(&target_path, &ready_path).unwrap();

        drop(guard);
        assert_eq!(std::fs::read_link(&ready_path).unwrap(), target_path);
        assert_eq!(std::fs::read(&target_path).unwrap(), b"foreign");
    }

    #[test]
    fn ready_file_lease_prevents_a_second_publisher_even_if_record_is_removed() {
        let tempdir = tempfile::tempdir().unwrap();
        let mount_point = tempdir.path().join("mount");
        std::fs::create_dir(&mount_point).unwrap();
        let ready_path = tempdir.path().join("ready.json");

        let guard = ReadyFileGuard::create(ready_path.clone(), &mount_point, "volume").unwrap();
        std::fs::remove_file(&ready_path).unwrap();
        assert!(ReadyFileGuard::create(ready_path.clone(), &mount_point, "volume").is_err());
        drop(guard);
        let replacement =
            ReadyFileGuard::create(ready_path.clone(), &mount_point, "volume").unwrap();
        drop(replacement);
        assert!(!ready_path.exists());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn ready_file_reclaims_only_a_well_formed_dead_process_record() {
        let tempdir = tempfile::tempdir().unwrap();
        let mount_point = tempdir.path().join("mount");
        std::fs::create_dir(&mount_point).unwrap();
        let ready_path = tempdir.path().join("ready.json");
        let stale = ReadyFilePayload {
            version:     "stale".to_string(),
            pid:         u32::MAX,
            token:       "dead-owner".to_string(),
            mount_point: mount_point.display().to_string(),
            volume:      "stale-volume".to_string(),
            state:       "ready".to_string(),
        };
        std::fs::write(&ready_path, serde_json::to_vec(&stale).unwrap()).unwrap();

        let guard = ReadyFileGuard::create(ready_path.clone(), &mount_point, "volume").unwrap();
        let current: ReadyFilePayload =
            serde_json::from_slice(&std::fs::read(&ready_path).unwrap()).unwrap();
        assert_eq!(current.pid, std::process::id());
        assert_ne!(current.token, stale.token);
        drop(guard);
        assert!(!ready_path.exists());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn stale_reclaim_never_unlinks_a_concurrent_replacement() {
        let tempdir = tempfile::tempdir().unwrap();
        let mount_point = tempdir.path().join("mount");
        std::fs::create_dir(&mount_point).unwrap();
        let ready_path = tempdir.path().join("ready.json");
        let stale = ReadyFilePayload {
            version:     "stale".to_string(),
            pid:         u32::MAX,
            token:       "dead-owner".to_string(),
            mount_point: mount_point.display().to_string(),
            volume:      "stale-volume".to_string(),
            state:       "ready".to_string(),
        };
        std::fs::write(&ready_path, serde_json::to_vec(&stale).unwrap()).unwrap();

        reclaim_stale_ready_file_after_retirement(&ready_path, || {
            std::fs::write(&ready_path, b"concurrent replacement").unwrap();
        })
        .unwrap();
        assert_eq!(
            std::fs::read(&ready_path).unwrap(),
            b"concurrent replacement"
        );
    }

    #[test]
    fn ready_file_preserves_live_and_unknown_existing_owners() {
        let tempdir = tempfile::tempdir().unwrap();
        let mount_point = tempdir.path().join("mount");
        std::fs::create_dir(&mount_point).unwrap();
        let ready_path = tempdir.path().join("ready.json");
        let live = ReadyFilePayload {
            version:     build_info::PKG_VERSION.to_string(),
            pid:         std::process::id(),
            token:       "live-owner".to_string(),
            mount_point: mount_point.display().to_string(),
            volume:      "live-volume".to_string(),
            state:       "ready".to_string(),
        };
        let live_bytes = serde_json::to_vec(&live).unwrap();
        std::fs::write(&ready_path, &live_bytes).unwrap();

        assert!(ReadyFileGuard::create(ready_path.clone(), &mount_point, "volume").is_err());
        assert_eq!(std::fs::read(&ready_path).unwrap(), live_bytes);

        let unknown = b"foreign replacement";
        std::fs::write(&ready_path, unknown).unwrap();
        assert!(ReadyFileGuard::create(ready_path.clone(), &mount_point, "volume").is_err());
        assert_eq!(std::fs::read(&ready_path).unwrap(), unknown);
    }

    #[cfg(unix)]
    #[test]
    fn stale_reclaim_preserves_a_symlink_even_when_its_target_looks_reclaimable() {
        use std::os::unix::fs::symlink;

        let tempdir = tempfile::tempdir().unwrap();
        let mount_point = tempdir.path().join("mount");
        std::fs::create_dir(&mount_point).unwrap();
        let ready_path = tempdir.path().join("ready.json");
        let target_path = tempdir.path().join("foreign-target.json");
        let stale = ReadyFilePayload {
            version:     "stale".to_string(),
            pid:         u32::MAX,
            token:       "dead-owner".to_string(),
            mount_point: mount_point.display().to_string(),
            volume:      "stale-volume".to_string(),
            state:       "ready".to_string(),
        };
        let target_bytes = serde_json::to_vec(&stale).unwrap();
        std::fs::write(&target_path, &target_bytes).unwrap();
        symlink(&target_path, &ready_path).unwrap();

        assert!(ReadyFileGuard::create(ready_path.clone(), &mount_point, "volume").is_err());
        assert_eq!(std::fs::read_link(&ready_path).unwrap(), target_path);
        assert_eq!(std::fs::read(&target_path).unwrap(), target_bytes);
    }
}
