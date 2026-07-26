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
//! This is the async-native replacement for the `fuser`-based layer in
//! `lib.rs` (issue #71). The two coexist during migration so behaviour can be
//! compared side by side.
//!
//! ## Runtime model
//!
//! fuse-backend-rs' async server drives requests on a `tokio-uring`
//! (io_uring) runtime, one per worker thread. The VFS, however, keeps its own
//! multi-threaded Tokio runtime. So the fuse methods here dispatch VFS futures
//! onto the VFS runtime via a [`Handle`]:
//!
//! * async (hot-path) methods `spawn` on the VFS runtime and `.await` the join
//!   handle — no `block_on`, so nothing blocks the io_uring worker;
//! * the remaining ops are dispatched by fuse-backend-rs to the *synchronous*
//!   `FileSystem` methods (there is no `async_readdir`/`async_mkdir` in the
//!   trait); those run inline on the io_uring thread, so they offload to the
//!   VFS runtime and block only the current worker via [`futures::executor`].

use std::{
    ffi::CStr,
    io,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use fuse_backend_rs::api::filesystem::{
    AsyncFileSystem, AsyncZeroCopyReader, AsyncZeroCopyWriter, Context, Entry, FileSystem,
    FsOptions, OpenOptions, SetattrValid,
};
use kiseki_meta::context::FuseContext;
use kiseki_types::{FileType, ToErrno, attr::InodeAttr, entry::FullEntry, ino::Ino};
use kiseki_vfs::KisekiVFS;
use tokio::runtime::Handle;

/// Map any KisekiFS error carrying an errno into an `io::Error`.
fn to_errno_io<E: ToErrno>(e: E) -> io::Error { io::Error::from_raw_os_error(e.to_errno()) }

/// A join failure (VFS task panicked / was cancelled) surfaces as EIO.
fn join_io(_e: tokio::task::JoinError) -> io::Error { io::Error::from_raw_os_error(libc::EIO) }

fn file_type_bits(kind: FileType) -> u32 {
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

fn system_time_parts(t: SystemTime) -> (i64, i64) {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => (d.as_secs() as i64, d.subsec_nanos() as i64),
        Err(_) => (0, 0),
    }
}

/// Convert a KisekiFS [`InodeAttr`] into a `libc::stat64` (the attribute type
/// fuse-backend-rs uses in `Entry` and `getattr`).
// `stat64` is a `repr(C)` plain-old-data struct with private padding fields, so
// there is no safe constructor; zero-initialising then filling the meaningful
// fields is the idiomatic approach (fuse-backend-rs does the same internally).
#[allow(unsafe_code)]
pub(crate) fn attr_to_stat64(attr: &InodeAttr, ino: u64) -> libc::stat64 {
    // SAFETY: all-zero is a valid bit pattern for this POD C struct.
    let mut st: libc::stat64 = unsafe { std::mem::zeroed() };
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

/// fuse-backend-rs filesystem backed by the KisekiFS VFS.
pub struct KisekiFsBackend {
    vfs:    Arc<KisekiVFS>,
    /// Handle to the VFS' own multi-threaded Tokio runtime.
    vfs_rt: Handle,
}

impl KisekiFsBackend {
    pub fn new(vfs: Arc<KisekiVFS>, vfs_rt: Handle) -> Self { Self { vfs, vfs_rt } }

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

    fn ctx(&self, ctx: &Context) -> Arc<FuseContext> {
        Arc::new(FuseContext::from_uid_gid_pid(ctx.uid, ctx.gid, ctx.pid as u32))
    }
}

impl FileSystem for KisekiFsBackend {
    type Inode = u64;
    type Handle = u64;

    fn init(&self, _capable: FsOptions) -> io::Result<FsOptions> { Ok(FsOptions::empty()) }

    fn destroy(&self) {}
}

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
            .map_err(|_| io::Error::from_raw_os_error(libc::EINVAL))?
            .to_owned();
        let fctx = self.ctx(ctx);
        let vfs = self.vfs.clone();
        let full = self
            .vfs_rt
            .spawn(async move { vfs.lookup(fctx, Ino::from(parent), &name).await })
            .await
            .map_err(join_io)?
            .map_err(to_errno_io)?;
        Ok(self.build_entry(&full))
    }

    async fn async_getattr(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        _handle: Option<Self::Handle>,
    ) -> io::Result<(libc::stat64, Duration)> {
        let vfs = self.vfs.clone();
        let attr = self
            .vfs_rt
            .spawn(async move { vfs.get_attr(Ino::from(inode)).await })
            .await
            .map_err(join_io)?
            .map_err(to_errno_io)?;
        let ttl = *self.vfs.get_entry_ttl(attr.kind);
        Ok((attr_to_stat64(&attr, inode), ttl))
    }

    // --- Remaining hot-path methods: filled in on subsequent iterations. ---

    async fn async_setattr(
        &self,
        _ctx: &Context,
        _inode: Self::Inode,
        _attr: libc::stat64,
        _handle: Option<Self::Handle>,
        _valid: SetattrValid,
    ) -> io::Result<(libc::stat64, Duration)> {
        Err(io::Error::from_raw_os_error(libc::ENOSYS))
    }

    async fn async_open(
        &self,
        _ctx: &Context,
        _inode: Self::Inode,
        _flags: u32,
        _fuse_flags: u32,
    ) -> io::Result<(Option<Self::Handle>, OpenOptions)> {
        Err(io::Error::from_raw_os_error(libc::ENOSYS))
    }

    async fn async_create(
        &self,
        _ctx: &Context,
        _parent: Self::Inode,
        _name: &CStr,
        _args: fuse_backend_rs::abi::fuse_abi::CreateIn,
    ) -> io::Result<(Entry, Option<Self::Handle>, OpenOptions)> {
        Err(io::Error::from_raw_os_error(libc::ENOSYS))
    }

    async fn async_read(
        &self,
        _ctx: &Context,
        _inode: Self::Inode,
        _handle: Self::Handle,
        _w: &mut (dyn AsyncZeroCopyWriter + Send),
        _size: u32,
        _offset: u64,
        _lock_owner: Option<u64>,
        _flags: u32,
    ) -> io::Result<usize> {
        Err(io::Error::from_raw_os_error(libc::ENOSYS))
    }

    async fn async_write(
        &self,
        _ctx: &Context,
        _inode: Self::Inode,
        _handle: Self::Handle,
        _r: &mut (dyn AsyncZeroCopyReader + Send),
        _size: u32,
        _offset: u64,
        _lock_owner: Option<u64>,
        _delayed_write: bool,
        _flags: u32,
        _fuse_flags: u32,
    ) -> io::Result<usize> {
        Err(io::Error::from_raw_os_error(libc::ENOSYS))
    }

    async fn async_fsync(
        &self,
        _ctx: &Context,
        _inode: Self::Inode,
        _datasync: bool,
        _handle: Self::Handle,
    ) -> io::Result<()> {
        Err(io::Error::from_raw_os_error(libc::ENOSYS))
    }

    async fn async_fallocate(
        &self,
        _ctx: &Context,
        _inode: Self::Inode,
        _handle: Self::Handle,
        _mode: u32,
        _offset: u64,
        _length: u64,
    ) -> io::Result<()> {
        Err(io::Error::from_raw_os_error(libc::ENOSYS))
    }

    async fn async_fsyncdir(
        &self,
        _ctx: &Context,
        _inode: Self::Inode,
        _datasync: bool,
        _handle: Self::Handle,
    ) -> io::Result<()> {
        Err(io::Error::from_raw_os_error(libc::ENOSYS))
    }
}
