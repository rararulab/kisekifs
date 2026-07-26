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
//! Replacement for the `fuser`-based layer in `lib.rs` (issue #71); the two
//! coexist during migration so behaviour can be compared side by side.
//!
//! ## Runtime model
//!
//! We use fuse-backend-rs' production fusedev path: a [`FuseSession`] plus a
//! pool of worker threads each looping on `FuseChannel::get_request` +
//! `Server::handle_message`. That path dispatches to the *synchronous*
//! `FileSystem` trait. The KisekiFS VFS is async and keeps its own
//! multi-threaded Tokio runtime, so each `FileSystem` method bridges via
//! [`Handle::block_on`]. This does not hit the "block_on inside an async
//! runtime" panic: the fusedev worker threads are plain OS threads with no
//! runtime entered, and with `N` workers there are `N` concurrent in-flight
//! requests — the same model nydus / virtiofsd ship.
//!
//! (fuse-backend-rs 0.14's async fusedev task `FuseDevTask` is gated behind a
//! non-existent `async_io` feature and is dead code, so the async server has no
//! usable driver; the sync worker-pool path above is the production one.)

use std::{ffi::CStr, io, path::Path, sync::Arc, time::Duration};

use fuse_backend_rs::{
    abi::fuse_abi::{CreateIn, stat64, statvfs64},
    api::filesystem::{
        Context, DirEntry, Entry, FileSystem, FsOptions, OpenOptions, SetattrValid, ZeroCopyReader,
        ZeroCopyWriter,
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

fn to_io<E: ToErrno>(e: E) -> io::Error { io::Error::from_raw_os_error(e.to_errno()) }

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

fn system_time_parts(t: std::time::SystemTime) -> (i64, i64) {
    match t.duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => (d.as_secs() as i64, d.subsec_nanos() as i64),
        Err(_) => (0, 0),
    }
}

fn system_time_from(secs: i64, nsecs: i64) -> std::time::SystemTime {
    std::time::UNIX_EPOCH + Duration::new(secs.max(0) as u64, nsecs.clamp(0, 999_999_999) as u32)
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

    fn ctx(&self, ctx: &Context) -> Arc<FuseContext> {
        Arc::new(FuseContext::from_uid_gid_pid(
            ctx.uid,
            ctx.gid,
            ctx.pid as u32,
        ))
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

    fn cstr(name: &CStr) -> io::Result<&str> { name.to_str().map_err(|_| errno_io(libc::EINVAL)) }
}

impl FileSystem for KisekiFsBackend {
    type Handle = u64;
    type Inode = u64;

    fn init(&self, _capable: FsOptions) -> io::Result<FsOptions> { Ok(FsOptions::empty()) }

    fn destroy(&self) {}

    fn lookup(&self, ctx: &Context, parent: Self::Inode, name: &CStr) -> io::Result<Entry> {
        let name = Self::cstr(name)?;
        let ctx = self.ctx(ctx);
        let fe = self
            .vfs_rt
            .block_on(self.vfs.lookup(ctx, Ino::from(parent), name))
            .map_err(to_io)?;
        Ok(self.build_entry(&fe))
    }

    fn getattr(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        _handle: Option<Self::Handle>,
    ) -> io::Result<(stat64, Duration)> {
        let attr = self
            .vfs_rt
            .block_on(self.vfs.get_attr(Ino::from(inode)))
            .map_err(to_io)?;
        let ttl = *self.vfs.get_entry_ttl(attr.kind);
        Ok((attr_to_stat64(&attr, inode), ttl))
    }

    fn setattr(
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

        let ctx = self.ctx(ctx);
        let new = self
            .vfs_rt
            .block_on(self.vfs.set_attr(
                ctx,
                Ino::from(inode),
                flags.bits(),
                atime,
                mtime,
                mode,
                uid,
                gid,
                size,
                fh,
                None,
            ))
            .map_err(to_io)?;
        let ttl = *self.vfs.get_entry_ttl(new.kind);
        Ok((attr_to_stat64(&new, inode), ttl))
    }

    fn readlink(&self, ctx: &Context, inode: Self::Inode) -> io::Result<Vec<u8>> {
        let ctx = self.ctx(ctx);
        let target = self
            .vfs_rt
            .block_on(self.vfs.readlink(ctx, Ino::from(inode)))
            .map_err(to_io)?;
        Ok(target.to_vec())
    }

    fn symlink(
        &self,
        ctx: &Context,
        linkname: &CStr,
        parent: Self::Inode,
        name: &CStr,
    ) -> io::Result<Entry> {
        let target = Self::cstr(linkname)?;
        let name = Self::cstr(name)?;
        let ctx = self.ctx(ctx);
        let fe = self
            .vfs_rt
            .block_on(
                self.vfs
                    .symlink(ctx, Ino::from(parent), name, Path::new(target)),
            )
            .map_err(to_io)?;
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
        let name = Self::cstr(name)?.to_owned();
        let ctx = self.ctx(ctx);
        let fe = self
            .vfs_rt
            .block_on(
                self.vfs
                    .mknod(ctx, Ino::from(parent), name, mode, umask, rdev),
            )
            .map_err(to_io)?;
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
        let name = Self::cstr(name)?;
        let ctx = self.ctx(ctx);
        let fe = self
            .vfs_rt
            .block_on(self.vfs.mkdir(ctx, Ino::from(parent), name, mode, umask))
            .map_err(to_io)?;
        Ok(self.build_entry(&fe))
    }

    fn unlink(&self, ctx: &Context, parent: Self::Inode, name: &CStr) -> io::Result<()> {
        let name = Self::cstr(name)?;
        let ctx = self.ctx(ctx);
        self.vfs_rt
            .block_on(self.vfs.unlink(ctx, Ino::from(parent), name))
            .map_err(to_io)
    }

    fn rmdir(&self, ctx: &Context, parent: Self::Inode, name: &CStr) -> io::Result<()> {
        let name = Self::cstr(name)?;
        let ctx = self.ctx(ctx);
        self.vfs_rt
            .block_on(self.vfs.rmdir(ctx, Ino::from(parent), name))
            .map_err(to_io)
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
        let oldname = Self::cstr(oldname)?;
        let newname = Self::cstr(newname)?;
        let ctx = self.ctx(ctx);
        self.vfs_rt
            .block_on(self.vfs.rename(
                ctx,
                Ino::from(olddir),
                oldname,
                Ino::from(newdir),
                newname,
                flags,
            ))
            .map_err(to_io)
    }

    fn link(
        &self,
        ctx: &Context,
        inode: Self::Inode,
        newparent: Self::Inode,
        newname: &CStr,
    ) -> io::Result<Entry> {
        let newname = Self::cstr(newname)?;
        let ctx = self.ctx(ctx);
        let fe = self
            .vfs_rt
            .block_on(
                self.vfs
                    .link(ctx, Ino::from(inode), Ino::from(newparent), newname),
            )
            .map_err(to_io)?;
        Ok(self.build_entry(&fe))
    }

    fn open(
        &self,
        ctx: &Context,
        inode: Self::Inode,
        flags: u32,
        _fuse_flags: u32,
    ) -> io::Result<(Option<Self::Handle>, OpenOptions, Option<u32>)> {
        let cctx = FuseContext::from_uid_gid_pid(ctx.uid, ctx.gid, ctx.pid as u32);
        let opened = self
            .vfs_rt
            .block_on(self.vfs.open(&cctx, Ino::from(inode), flags as i32))
            .map_err(to_io)?;
        Ok((Some(opened.fh), OpenOptions::empty(), None))
    }

    fn create(
        &self,
        ctx: &Context,
        parent: Self::Inode,
        name: &CStr,
        args: CreateIn,
    ) -> io::Result<(Entry, Option<Self::Handle>, OpenOptions, Option<u32>)> {
        let name = Self::cstr(name)?;
        let ctx = self.ctx(ctx);
        let (fe, fh) = self
            .vfs_rt
            .block_on(self.vfs.create(
                ctx,
                Ino::from(parent),
                name,
                args.mode,
                args.umask,
                args.flags as i32,
            ))
            .map_err(to_io)?;
        Ok((self.build_entry(&fe), Some(fh), OpenOptions::empty(), None))
    }

    #[allow(clippy::too_many_arguments)]
    fn read(
        &self,
        ctx: &Context,
        inode: Self::Inode,
        handle: Self::Handle,
        w: &mut dyn ZeroCopyWriter,
        size: u32,
        offset: u64,
        lock_owner: Option<u64>,
        flags: u32,
    ) -> io::Result<usize> {
        let ctx = self.ctx(ctx);
        let data = self
            .vfs_rt
            .block_on(self.vfs.read(
                ctx,
                Ino::from(inode),
                handle,
                offset as i64,
                size,
                flags as i32,
                lock_owner,
            ))
            .map_err(to_io)?;
        w.write_all(&data)?;
        Ok(data.len())
    }

    #[allow(clippy::too_many_arguments)]
    fn write(
        &self,
        ctx: &Context,
        inode: Self::Inode,
        handle: Self::Handle,
        r: &mut dyn ZeroCopyReader,
        size: u32,
        offset: u64,
        lock_owner: Option<u64>,
        _delayed_write: bool,
        flags: u32,
        _fuse_flags: u32,
    ) -> io::Result<usize> {
        let mut buf = vec![0u8; size as usize];
        r.read_exact(&mut buf)?;
        let ctx = self.ctx(ctx);
        let written = self
            .vfs_rt
            .block_on(self.vfs.write(
                ctx,
                Ino::from(inode),
                handle,
                offset as i64,
                &buf,
                0,
                flags as i32,
                lock_owner,
            ))
            .map_err(to_io)?;
        Ok(written as usize)
    }

    fn flush(
        &self,
        ctx: &Context,
        inode: Self::Inode,
        handle: Self::Handle,
        lock_owner: u64,
    ) -> io::Result<()> {
        let ctx = self.ctx(ctx);
        self.vfs_rt
            .block_on(self.vfs.flush(ctx, Ino::from(inode), handle, lock_owner))
            .map_err(to_io)
    }

    fn fsync(
        &self,
        ctx: &Context,
        inode: Self::Inode,
        datasync: bool,
        handle: Self::Handle,
    ) -> io::Result<()> {
        let ctx = self.ctx(ctx);
        self.vfs_rt
            .block_on(self.vfs.fsync(ctx, Ino::from(inode), handle, datasync))
            .map_err(to_io)
    }

    fn fallocate(
        &self,
        ctx: &Context,
        inode: Self::Inode,
        handle: Self::Handle,
        mode: u32,
        offset: u64,
        length: u64,
    ) -> io::Result<()> {
        let ctx = self.ctx(ctx);
        self.vfs_rt
            .block_on(self.vfs.fallocate(
                ctx,
                Ino::from(inode),
                handle,
                offset as i64,
                length as i64,
                mode as i32,
            ))
            .map_err(to_io)
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
        let ctx = self.ctx(ctx);
        // The returned JoinHandle drives background finalisation; not awaited
        // here (matches the fuser layer).
        self.vfs_rt
            .block_on(self.vfs.release(ctx, Ino::from(inode), handle))
            .map(|_| ())
            .map_err(to_io)
    }

    fn opendir(
        &self,
        ctx: &Context,
        inode: Self::Inode,
        flags: u32,
    ) -> io::Result<(Option<Self::Handle>, OpenOptions)> {
        let cctx = FuseContext::from_uid_gid_pid(ctx.uid, ctx.gid, ctx.pid as u32);
        let fh = self
            .vfs_rt
            .block_on(self.vfs.open_dir(&cctx, Ino::from(inode), flags as i32))
            .map_err(to_io)?;
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
        let cctx = FuseContext::from_uid_gid_pid(ctx.uid, ctx.gid, ctx.pid as u32);
        let entries = self
            .vfs_rt
            .block_on(
                self.vfs
                    .read_dir(&cctx, Ino::from(inode), handle, offset as i64, false),
            )
            .map_err(to_io)?;
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
        let cctx = FuseContext::from_uid_gid_pid(ctx.uid, ctx.gid, ctx.pid as u32);
        let entries = self
            .vfs_rt
            .block_on(
                self.vfs
                    .read_dir(&cctx, Ino::from(inode), handle, offset as i64, true),
            )
            .map_err(to_io)?;
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
        self.vfs_rt
            .block_on(self.vfs.release_dir(Ino::from(inode), handle))
            .map_err(to_io)
    }

    fn statfs(&self, ctx: &Context, inode: Self::Inode) -> io::Result<statvfs64> {
        let ctx = self.ctx(ctx);
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
