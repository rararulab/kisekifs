// Copyright 2024 kisekifs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

//! fuse-backend-rs implementation of the KisekiFS FUSE layer.
//!
//! Async-native replacement for the `fuser`-based layer in `lib.rs` (issue
//! #71); the two coexist during migration so behaviour can be compared.
//!
//! ## Runtime model
//!
//! fuse-backend-rs' async server drives requests on a `tokio-uring` (io_uring)
//! runtime, one per worker thread. The VFS keeps its own multi-threaded Tokio
//! runtime. Fuse methods dispatch VFS futures onto the VFS runtime via a
//! [`Handle`]:
//!
//! * async (hot-path) methods `spawn` on the VFS runtime and `.await` the join
//!   handle — no `block_on` on the io_uring worker;
//! * the remaining ops are dispatched by fuse-backend-rs to the *synchronous*
//!   `FileSystem` methods (there is no `async_readdir`/`async_mkdir`); those
//!   run inline on the io_uring thread, so they offload to the VFS runtime and
//!   block only the current worker via [`futures::executor::block_on`].

use std::{
    ffi::CStr,
    future::Future,
    io,
    path::Path,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use fuse_backend_rs::{
    abi::fuse_abi::{CreateIn, stat64, statvfs64},
    api::filesystem::{
        AsyncFileSystem, AsyncZeroCopyReader, AsyncZeroCopyWriter, Context, DirEntry, Entry,
        FileSystem, FsOptions, OpenOptions, SetattrValid,
    },
};
use fuser::TimeOrNow;
use kiseki_common::{BLOCK_SIZE, MAX_NAME_LENGTH};
use kiseki_meta::context::FuseContext;
use kiseki_types::{
    FileType, ToErrno,
    attr::{InodeAttr, SetAttrFlags},
    entry::{Entry as KisekiEntry, FullEntry},
    ino::Ino,
};
use kiseki_vfs::KisekiVFS;
use tokio::runtime::Handle;

// ---------------------------------------------------------------------------
// conversions
// ---------------------------------------------------------------------------

/// Zero-initialise a POD C struct. Only for `#[repr(C)]` libc types whose
/// all-zero bit pattern is valid (`stat64`, `statvfs64`).
#[allow(unsafe_code)]
const fn zeroed_pod<T>() -> T {
    // SAFETY: callers restrict `T` to POD C structs where all-zero is valid.
    unsafe { std::mem::zeroed() }
}

fn to_io<E: ToErrno>(errno: E) -> io::Error { io::Error::from_raw_os_error(errno.to_errno()) }

fn join_io(_e: tokio::task::JoinError) -> io::Error { io::Error::from_raw_os_error(libc::EIO) }

fn errno_io(errno: i32) -> io::Error { io::Error::from_raw_os_error(errno) }

const fn file_type_bits(kind: FileType) -> u32 {
    match kind {
        FileType::NamedPipe => libc::S_IFIFO,
        FileType::CharDevice => libc::S_IFCHR,
        FileType::BlockDevice => libc::S_IFBLK,
        FileType::Directory => libc::S_IFDIR,
        FileType::RegularFile => libc::S_IFREG,
        FileType::Symlink => libc::S_IFLNK,
        FileType::Socket => libc::S_IFSOCK,
    }
}

const fn dir_entry_type(kind: FileType) -> u32 {
    let dt = match kind {
        FileType::NamedPipe => libc::DT_FIFO,
        FileType::CharDevice => libc::DT_CHR,
        FileType::Directory => libc::DT_DIR,
        FileType::BlockDevice => libc::DT_BLK,
        FileType::RegularFile => libc::DT_REG,
        FileType::Symlink => libc::DT_LNK,
        FileType::Socket => libc::DT_SOCK,
    };
    dt as u32
}

fn system_time_parts(t: SystemTime) -> (i64, i64) {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => (d.as_secs() as i64, d.subsec_nanos() as i64),
        Err(_) => (0, 0),
    }
}

fn system_time_from(secs: i64, nsecs: i64) -> SystemTime {
    UNIX_EPOCH + Duration::new(secs.max(0) as u64, nsecs.clamp(0, 999_999_999) as u32)
}

/// Convert a KisekiFS [`InodeAttr`] into a `libc::stat64`.
pub(crate) fn attr_to_stat64(attr: &InodeAttr, ino: u64) -> stat64 {
    let mut st: stat64 = zeroed_pod();
    st.st_ino = ino;
    st.st_mode = file_type_bits(attr.kind) | (attr.mode & 0o7777);
    st.st_nlink = attr.nlink as libc::nlink_t;
    st.st_uid = attr.uid;
    st.st_gid = attr.gid;
    st.st_rdev = attr.rdev as libc::dev_t;
    st.st_blksize = 0x10000;
    match attr.kind {
        FileType::Directory | FileType::Symlink | FileType::RegularFile => {
            st.st_size = attr.length as libc::off64_t;
            st.st_blocks = attr.length.div_ceil(512) as libc::blkcnt64_t;
        }
        _ => {}
    }
    let (atime, atime_n) = system_time_parts(attr.atime);
    let (mtime, mtime_n) = system_time_parts(attr.mtime);
    let (ctime, ctime_n) = system_time_parts(attr.ctime);
    st.st_atime = atime;
    st.st_atime_nsec = atime_n;
    st.st_mtime = mtime;
    st.st_mtime_nsec = mtime_n;
    st.st_ctime = ctime;
    st.st_ctime_nsec = ctime_n;
    st
}

// ---------------------------------------------------------------------------
// backend
// ---------------------------------------------------------------------------

/// fuse-backend-rs filesystem backed by the KisekiFS VFS.
pub struct KisekiFsBackend {
    vfs:    Arc<KisekiVFS>,
    /// Handle to the VFS' own multi-threaded Tokio runtime.
    vfs_rt: Handle,
}

impl KisekiFsBackend {
    pub const fn new(vfs: Arc<KisekiVFS>, vfs_rt: Handle) -> Self { Self { vfs, vfs_rt } }

    fn ctx(&self, ctx: &Context) -> FuseContext {
        FuseContext::from_uid_gid_pid(ctx.uid, ctx.gid, ctx.pid as u32)
    }

    fn build_entry(&self, e: &FullEntry) -> Entry {
        let ino: u64 = e.inode.into();
        let ttl = *self.vfs.get_entry_ttl(e.attr.kind);
        Entry {
            inode:         ino,
            generation:    1,
            attr:          attr_to_stat64(&e.attr, ino),
            attr_flags:    0,
            attr_timeout:  ttl,
            entry_timeout: ttl,
        }
    }

    /// Await a VFS future from an async (io_uring) context by offloading it to
    /// the VFS runtime. The inner error is reduced to an errno inside the task
    /// so the VFS error type does not need to cross the runtime boundary.
    async fn run<F, T>(&self, fut: F) -> io::Result<T>
    where
        F: Future<Output = std::result::Result<T, i32>> + Send + 'static,
        T: Send + 'static,
    {
        self.vfs_rt
            .spawn(fut)
            .await
            .map_err(join_io)?
            .map_err(errno_io)
    }

    /// Block on a VFS future from a synchronous (io_uring worker) context.
    /// Offloads to the VFS runtime and parks only the current worker.
    fn block<F, T>(&self, fut: F) -> io::Result<T>
    where
        F: Future<Output = std::result::Result<T, i32>> + Send + 'static,
        T: Send + 'static,
    {
        let jh = self.vfs_rt.spawn(fut);
        futures::executor::block_on(jh)
            .map_err(join_io)?
            .map_err(errno_io)
    }
}

// ---------------------------------------------------------------------------
// synchronous FileSystem: base + ops the async server dispatches synchronously
// ---------------------------------------------------------------------------

impl FileSystem for KisekiFsBackend {
    type Handle = u64;
    type Inode = u64;

    fn init(&self, _capable: FsOptions) -> io::Result<FsOptions> { Ok(FsOptions::empty()) }

    fn destroy(&self) {}

    fn readlink(&self, ctx: &Context, inode: Self::Inode) -> io::Result<Vec<u8>> {
        let ctx = Arc::new(self.ctx(ctx));
        let vfs = self.vfs.clone();
        let target = self.block(async move {
            vfs.readlink(ctx, Ino::from(inode))
                .await
                .map_err(|e| e.to_errno())
        })?;
        Ok(target.to_vec())
    }

    fn symlink(
        &self,
        ctx: &Context,
        linkname: &CStr,
        parent: Self::Inode,
        name: &CStr,
    ) -> io::Result<Entry> {
        let target = linkname
            .to_str()
            .map_err(|_| errno_io(libc::EINVAL))?
            .to_owned();
        let name = name
            .to_str()
            .map_err(|_| errno_io(libc::EINVAL))?
            .to_owned();
        let ctx = Arc::new(self.ctx(ctx));
        let vfs = self.vfs.clone();
        let fe = self.block(async move {
            vfs.symlink(ctx, Ino::from(parent), &name, Path::new(&target))
                .await
                .map_err(|e| e.to_errno())
        })?;
        Ok(self.build_entry(&fe))
    }

    fn mknod(
        &self,
        ctx: &Context,
        parent: Self::Inode,
        name: &CStr,
        mode: u32,
        rdev: u32,
        umask: u32,
    ) -> io::Result<Entry> {
        let name = name
            .to_str()
            .map_err(|_| errno_io(libc::EINVAL))?
            .to_owned();
        let ctx = Arc::new(self.ctx(ctx));
        let vfs = self.vfs.clone();
        let fe = self.block(async move {
            vfs.mknod(ctx, Ino::from(parent), name, mode, umask, rdev)
                .await
                .map_err(|e| e.to_errno())
        })?;
        Ok(self.build_entry(&fe))
    }

    fn mkdir(
        &self,
        ctx: &Context,
        parent: Self::Inode,
        name: &CStr,
        mode: u32,
        umask: u32,
    ) -> io::Result<Entry> {
        let name = name
            .to_str()
            .map_err(|_| errno_io(libc::EINVAL))?
            .to_owned();
        let ctx = Arc::new(self.ctx(ctx));
        let vfs = self.vfs.clone();
        let fe = self.block(async move {
            vfs.mkdir(ctx, Ino::from(parent), &name, mode, umask)
                .await
                .map_err(|e| e.to_errno())
        })?;
        Ok(self.build_entry(&fe))
    }

    fn unlink(&self, ctx: &Context, parent: Self::Inode, name: &CStr) -> io::Result<()> {
        let name = name
            .to_str()
            .map_err(|_| errno_io(libc::EINVAL))?
            .to_owned();
        let ctx = Arc::new(self.ctx(ctx));
        let vfs = self.vfs.clone();
        self.block(async move {
            vfs.unlink(ctx, Ino::from(parent), &name)
                .await
                .map_err(|e| e.to_errno())
        })
    }

    fn rmdir(&self, ctx: &Context, parent: Self::Inode, name: &CStr) -> io::Result<()> {
        let name = name
            .to_str()
            .map_err(|_| errno_io(libc::EINVAL))?
            .to_owned();
        let ctx = Arc::new(self.ctx(ctx));
        let vfs = self.vfs.clone();
        self.block(async move {
            vfs.rmdir(ctx, Ino::from(parent), &name)
                .await
                .map_err(|e| e.to_errno())
        })
    }

    fn rename(
        &self,
        ctx: &Context,
        olddir: Self::Inode,
        oldname: &CStr,
        newdir: Self::Inode,
        newname: &CStr,
        flags: u32,
    ) -> io::Result<()> {
        let oldname = oldname
            .to_str()
            .map_err(|_| errno_io(libc::EINVAL))?
            .to_owned();
        let newname = newname
            .to_str()
            .map_err(|_| errno_io(libc::EINVAL))?
            .to_owned();
        let ctx = Arc::new(self.ctx(ctx));
        let vfs = self.vfs.clone();
        self.block(async move {
            vfs.rename(
                ctx,
                Ino::from(olddir),
                &oldname,
                Ino::from(newdir),
                &newname,
                flags,
            )
            .await
            .map_err(|e| e.to_errno())
        })
    }

    fn link(
        &self,
        ctx: &Context,
        inode: Self::Inode,
        newparent: Self::Inode,
        newname: &CStr,
    ) -> io::Result<Entry> {
        let newname = newname
            .to_str()
            .map_err(|_| errno_io(libc::EINVAL))?
            .to_owned();
        let ctx = Arc::new(self.ctx(ctx));
        let vfs = self.vfs.clone();
        let fe = self.block(async move {
            vfs.link(ctx, Ino::from(inode), Ino::from(newparent), &newname)
                .await
                .map_err(|e| e.to_errno())
        })?;
        Ok(self.build_entry(&fe))
    }

    fn opendir(
        &self,
        ctx: &Context,
        inode: Self::Inode,
        flags: u32,
    ) -> io::Result<(Option<Self::Handle>, OpenOptions)> {
        let cctx = self.ctx(ctx);
        let vfs = self.vfs.clone();
        let fh = self.block(async move {
            vfs.open_dir(&cctx, Ino::from(inode), flags as i32)
                .await
                .map_err(|e| e.to_errno())
        })?;
        Ok((Some(fh), OpenOptions::empty()))
    }

    fn readdir(
        &self,
        ctx: &Context,
        inode: Self::Inode,
        handle: Self::Handle,
        _size: u32,
        offset: u64,
        add_entry: &mut dyn FnMut(DirEntry) -> io::Result<usize>,
    ) -> io::Result<()> {
        let cctx = self.ctx(ctx);
        let vfs = self.vfs.clone();
        let entries = self.block(async move {
            vfs.read_dir(&cctx, Ino::from(inode), handle, offset as i64, false)
                .await
                .map_err(|e| e.to_errno())
        })?;
        for (next, e) in (offset + 1..).zip(entries.iter()) {
            let de = DirEntry {
                ino:    e.get_inode().0,
                offset: next,
                type_:  dir_entry_type(e.get_file_type()),
                name:   e.get_name().as_bytes(),
            };
            if add_entry(de)? == 0 {
                break;
            }
        }
        Ok(())
    }

    fn readdirplus(
        &self,
        ctx: &Context,
        inode: Self::Inode,
        handle: Self::Handle,
        _size: u32,
        offset: u64,
        add_entry: &mut dyn FnMut(DirEntry, Entry) -> io::Result<usize>,
    ) -> io::Result<()> {
        let cctx = self.ctx(ctx);
        let vfs = self.vfs.clone();
        let entries = self.block(async move {
            vfs.read_dir(&cctx, Ino::from(inode), handle, offset as i64, true)
                .await
                .map_err(|e| e.to_errno())
        })?;
        for (next, e) in (offset + 1..).zip(entries.iter()) {
            let KisekiEntry::Full(fe) = e else { continue };
            let de = DirEntry {
                ino:    fe.inode.0,
                offset: next,
                type_:  dir_entry_type(fe.attr.kind),
                name:   fe.name.as_bytes(),
            };
            if add_entry(de, self.build_entry(fe))? == 0 {
                break;
            }
        }
        Ok(())
    }

    fn releasedir(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        _flags: u32,
        handle: Self::Handle,
    ) -> io::Result<()> {
        let vfs = self.vfs.clone();
        self.block(async move {
            vfs.release_dir(Ino::from(inode), handle)
                .await
                .map_err(|e| e.to_errno())
        })
    }

    fn flush(
        &self,
        ctx: &Context,
        inode: Self::Inode,
        handle: Self::Handle,
        lock_owner: u64,
    ) -> io::Result<()> {
        let ctx = Arc::new(self.ctx(ctx));
        let vfs = self.vfs.clone();
        self.block(async move {
            vfs.flush(ctx, Ino::from(inode), handle, lock_owner)
                .await
                .map_err(|e| e.to_errno())
        })
    }

    fn release(
        &self,
        ctx: &Context,
        inode: Self::Inode,
        _flags: u32,
        handle: Self::Handle,
        _flush: bool,
        _flock_release: bool,
        _lock_owner: Option<u64>,
    ) -> io::Result<()> {
        let ctx = Arc::new(self.ctx(ctx));
        let vfs = self.vfs.clone();
        // vfs.release returns a JoinHandle for the background finalisation; we do
        // not need to await it here (matches the fuser layer).
        self.block(async move {
            vfs.release(ctx, Ino::from(inode), handle)
                .await
                .map(|_| ())
                .map_err(|e| e.to_errno())
        })
    }

    fn statfs(&self, ctx: &Context, inode: Self::Inode) -> io::Result<statvfs64> {
        let ctx = Arc::new(self.ctx(ctx));
        let state = self.vfs.stat_fs(ctx, Ino::from(inode)).map_err(to_io)?;

        let total_blocks = (state.total_size / BLOCK_SIZE as u64).max(1);
        let used_blocks = state.used_size / BLOCK_SIZE as u64;
        let avail_blocks = total_blocks.saturating_sub(used_blocks);

        let mut st: statvfs64 = zeroed_pod();
        st.f_bsize = BLOCK_SIZE as u64;
        st.f_frsize = BLOCK_SIZE as u64;
        st.f_blocks = total_blocks;
        st.f_bfree = avail_blocks;
        st.f_bavail = avail_blocks;
        st.f_files = u64::MAX;
        st.f_ffree = u64::MAX - state.file_count;
        st.f_favail = u64::MAX - state.file_count;
        st.f_namemax = MAX_NAME_LENGTH as u64;
        Ok(st)
    }
}

// ---------------------------------------------------------------------------
// asynchronous hot path
// ---------------------------------------------------------------------------

#[async_trait]
impl AsyncFileSystem for KisekiFsBackend {
    async fn async_lookup(
        &self,
        ctx: &Context,
        parent: Self::Inode,
        name: &CStr,
    ) -> io::Result<Entry> {
        let name = name
            .to_str()
            .map_err(|_| errno_io(libc::EINVAL))?
            .to_owned();
        let cctx = Arc::new(self.ctx(ctx));
        let vfs = self.vfs.clone();
        let fe = self
            .run(async move {
                vfs.lookup(cctx, Ino::from(parent), &name)
                    .await
                    .map_err(|e| e.to_errno())
            })
            .await?;
        Ok(self.build_entry(&fe))
    }

    async fn async_getattr(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        _handle: Option<Self::Handle>,
    ) -> io::Result<(stat64, Duration)> {
        let vfs = self.vfs.clone();
        let attr = self
            .run(async move {
                vfs.get_attr(Ino::from(inode))
                    .await
                    .map_err(|e| e.to_errno())
            })
            .await?;
        let ttl = *self.vfs.get_entry_ttl(attr.kind);
        Ok((attr_to_stat64(&attr, inode), ttl))
    }

    async fn async_setattr(
        &self,
        ctx: &Context,
        inode: Self::Inode,
        attr: stat64,
        fh: Option<Self::Handle>,
        valid: SetattrValid,
    ) -> io::Result<(stat64, Duration)> {
        let mut flags = SetAttrFlags::empty();
        let mut mode = None;
        let mut uid = None;
        let mut gid = None;
        let mut size = None;
        if valid.contains(SetattrValid::MODE) {
            flags |= SetAttrFlags::MODE;
            mode = Some(attr.st_mode & 0o7777);
        }
        if valid.contains(SetattrValid::UID) {
            flags |= SetAttrFlags::UID;
            uid = Some(attr.st_uid);
        }
        if valid.contains(SetattrValid::GID) {
            flags |= SetAttrFlags::GID;
            gid = Some(attr.st_gid);
        }
        if valid.contains(SetattrValid::SIZE) {
            flags |= SetAttrFlags::SIZE;
            size = Some(attr.st_size as u64);
        }
        let atime = if valid.contains(SetattrValid::ATIME_NOW) {
            flags |= SetAttrFlags::ATIME_NOW;
            Some(TimeOrNow::Now)
        } else if valid.contains(SetattrValid::ATIME) {
            flags |= SetAttrFlags::ATIME;
            Some(TimeOrNow::SpecificTime(system_time_from(
                attr.st_atime,
                attr.st_atime_nsec,
            )))
        } else {
            None
        };
        let mtime = if valid.contains(SetattrValid::MTIME_NOW) {
            flags |= SetAttrFlags::MTIME_NOW;
            Some(TimeOrNow::Now)
        } else if valid.contains(SetattrValid::MTIME) {
            flags |= SetAttrFlags::MTIME;
            Some(TimeOrNow::SpecificTime(system_time_from(
                attr.st_mtime,
                attr.st_mtime_nsec,
            )))
        } else {
            None
        };

        let bits = flags.bits();
        let cctx = Arc::new(self.ctx(ctx));
        let vfs = self.vfs.clone();
        let new = self
            .run(async move {
                vfs.set_attr(
                    cctx,
                    Ino::from(inode),
                    bits,
                    atime,
                    mtime,
                    mode,
                    uid,
                    gid,
                    size,
                    fh,
                    None,
                )
                .await
                .map_err(|e| e.to_errno())
            })
            .await?;
        let ttl = *self.vfs.get_entry_ttl(new.kind);
        Ok((attr_to_stat64(&new, inode), ttl))
    }

    async fn async_open(
        &self,
        ctx: &Context,
        inode: Self::Inode,
        flags: u32,
        _fuse_flags: u32,
    ) -> io::Result<(Option<Self::Handle>, OpenOptions)> {
        let cctx = self.ctx(ctx);
        let vfs = self.vfs.clone();
        let opened = self
            .run(async move {
                vfs.open(&cctx, Ino::from(inode), flags as i32)
                    .await
                    .map_err(|e| e.to_errno())
            })
            .await?;
        Ok((Some(opened.fh), OpenOptions::empty()))
    }

    async fn async_create(
        &self,
        ctx: &Context,
        parent: Self::Inode,
        name: &CStr,
        args: CreateIn,
    ) -> io::Result<(Entry, Option<Self::Handle>, OpenOptions)> {
        let name = name
            .to_str()
            .map_err(|_| errno_io(libc::EINVAL))?
            .to_owned();
        let cctx = Arc::new(self.ctx(ctx));
        let vfs = self.vfs.clone();
        let (fe, fh) = self
            .run(async move {
                vfs.create(
                    cctx,
                    Ino::from(parent),
                    &name,
                    args.mode,
                    args.umask,
                    args.flags as i32,
                )
                .await
                .map_err(|e| e.to_errno())
            })
            .await?;
        Ok((self.build_entry(&fe), Some(fh), OpenOptions::empty()))
    }

    async fn async_read(
        &self,
        ctx: &Context,
        inode: Self::Inode,
        handle: Self::Handle,
        w: &mut (dyn AsyncZeroCopyWriter + Send),
        size: u32,
        offset: u64,
        lock_owner: Option<u64>,
        flags: u32,
    ) -> io::Result<usize> {
        let cctx = Arc::new(self.ctx(ctx));
        let vfs = self.vfs.clone();
        let data = self
            .run(async move {
                vfs.read(
                    cctx,
                    Ino::from(inode),
                    handle,
                    offset as i64,
                    size,
                    flags as i32,
                    lock_owner,
                )
                .await
                .map_err(|e| e.to_errno())
            })
            .await?;
        w.write_all(&data)?;
        Ok(data.len())
    }

    async fn async_write(
        &self,
        ctx: &Context,
        inode: Self::Inode,
        handle: Self::Handle,
        r: &mut (dyn AsyncZeroCopyReader + Send),
        size: u32,
        offset: u64,
        lock_owner: Option<u64>,
        _delayed_write: bool,
        flags: u32,
        _fuse_flags: u32,
    ) -> io::Result<usize> {
        let mut buf = vec![0u8; size as usize];
        r.read_exact(&mut buf)?;
        let cctx = Arc::new(self.ctx(ctx));
        let vfs = self.vfs.clone();
        let written = self
            .run(async move {
                vfs.write(
                    cctx,
                    Ino::from(inode),
                    handle,
                    offset as i64,
                    &buf,
                    0,
                    flags as i32,
                    lock_owner,
                )
                .await
                .map_err(|e| e.to_errno())
            })
            .await?;
        Ok(written as usize)
    }

    async fn async_fsync(
        &self,
        ctx: &Context,
        inode: Self::Inode,
        datasync: bool,
        handle: Self::Handle,
    ) -> io::Result<()> {
        let cctx = Arc::new(self.ctx(ctx));
        let vfs = self.vfs.clone();
        self.run(async move {
            vfs.fsync(cctx, Ino::from(inode), handle, datasync)
                .await
                .map_err(|e| e.to_errno())
        })
        .await
    }

    async fn async_fallocate(
        &self,
        ctx: &Context,
        inode: Self::Inode,
        handle: Self::Handle,
        mode: u32,
        offset: u64,
        length: u64,
    ) -> io::Result<()> {
        let cctx = Arc::new(self.ctx(ctx));
        let vfs = self.vfs.clone();
        self.run(async move {
            vfs.fallocate(
                cctx,
                Ino::from(inode),
                handle,
                offset as i64,
                length as i64,
                mode as i32,
            )
            .await
            .map_err(|e| e.to_errno())
        })
        .await
    }

    async fn async_fsyncdir(
        &self,
        ctx: &Context,
        inode: Self::Inode,
        datasync: bool,
        handle: Self::Handle,
    ) -> io::Result<()> {
        // Directory metadata is published synchronously; treat like fsync.
        let cctx = Arc::new(self.ctx(ctx));
        let vfs = self.vfs.clone();
        self.run(async move {
            vfs.fsync(cctx, Ino::from(inode), handle, datasync)
                .await
                .map_err(|e| e.to_errno())
        })
        .await
    }
}
