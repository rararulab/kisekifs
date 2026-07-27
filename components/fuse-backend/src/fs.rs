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

//! The [`FileSystem`] implementation backed by the KisekiFS VFS.

use std::{ffi::CStr, future::Future, io, path::Path, sync::Arc, time::Duration};

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
    ToErrno,
    attr::SetAttrFlags,
    entry::{Entry as KisekiEntry, FullEntry},
    ino::Ino,
};
use kiseki_vfs::{KisekiVFS, OperationGuard};
use tokio::runtime::Handle;

use crate::convert::{attr_to_stat64, dir_entry_type, errno_io, system_time_from, to_io};

/// fuse-backend-rs filesystem backed by the KisekiFS VFS.
///
/// The VFS is async and keeps its own multi-threaded Tokio runtime; each op
/// bridges to it via [`Handle::block_on`]. The fusedev workers are plain OS
/// threads with no runtime entered, so `block_on` does not panic, and N workers
/// give N concurrent in-flight requests.
///
/// Every op takes a VFS [`OperationGuard`] first (via
/// [`KisekiVFS::begin_operation`]), so graceful shutdown drains in-flight ops
/// and rejects new ones with `EIO` — matching the fuser layer's
/// `begin_operation!` gate.
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

    /// Take an operation guard so shutdown can drain / reject in flight.
    fn guard(&self) -> io::Result<OperationGuard> {
        self.vfs
            .begin_operation()
            .ok_or_else(|| errno_io(libc::EIO))
    }

    /// Guard the op, run the VFS future to completion on the VFS runtime, and
    /// map the VFS error to an `io::Error` with its errno preserved.
    fn block<F, T, E>(&self, fut: F) -> io::Result<T>
    where
        F: Future<Output = std::result::Result<T, E>>,
        E: ToErrno,
    {
        let _guard = self.guard()?;
        self.vfs_rt.block_on(fut).map_err(to_io)
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
        let fe = self.block(self.vfs.lookup(ctx, Ino::from(parent), name))?;
        Ok(self.build_entry(&fe))
    }

    fn getattr(
        &self,
        _ctx: &Context,
        inode: Self::Inode,
        _handle: Option<Self::Handle>,
    ) -> io::Result<(stat64, Duration)> {
        let attr = self.block(self.vfs.get_attr(Ino::from(inode)))?;
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
        let new = self.block(self.vfs.set_attr(
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
        ))?;
        let ttl = *self.vfs.get_entry_ttl(new.kind);
        Ok((attr_to_stat64(&new, inode), ttl))
    }

    fn readlink(&self, ctx: &Context, inode: Self::Inode) -> io::Result<Vec<u8>> {
        let ctx = self.ctx(ctx);
        let target = self.block(self.vfs.readlink(ctx, Ino::from(inode)))?;
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
        let fe = self.block(
            self.vfs
                .symlink(ctx, Ino::from(parent), name, Path::new(target)),
        )?;
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
        let fe = self.block(
            self.vfs
                .mknod(ctx, Ino::from(parent), name, mode, umask, rdev),
        )?;
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
        let fe = self.block(self.vfs.mkdir(ctx, Ino::from(parent), name, mode, umask))?;
        Ok(self.build_entry(&fe))
    }

    fn unlink(&self, ctx: &Context, parent: Self::Inode, name: &CStr) -> io::Result<()> {
        let name = Self::cstr(name)?;
        let ctx = self.ctx(ctx);
        self.block(self.vfs.unlink(ctx, Ino::from(parent), name))
    }

    fn rmdir(&self, ctx: &Context, parent: Self::Inode, name: &CStr) -> io::Result<()> {
        let name = Self::cstr(name)?;
        let ctx = self.ctx(ctx);
        self.block(self.vfs.rmdir(ctx, Ino::from(parent), name))
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
        self.block(self.vfs.rename(
            ctx,
            Ino::from(olddir),
            oldname,
            Ino::from(newdir),
            newname,
            flags,
        ))
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
        let fe = self.block(
            self.vfs
                .link(ctx, Ino::from(inode), Ino::from(newparent), newname),
        )?;
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
        let opened = self.block(self.vfs.open(&cctx, Ino::from(inode), flags as i32))?;
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
        let (fe, fh) = self.block(self.vfs.create(
            ctx,
            Ino::from(parent),
            name,
            args.mode,
            args.umask,
            args.flags as i32,
        ))?;
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
        let data = self.block(self.vfs.read(
            ctx,
            Ino::from(inode),
            handle,
            offset as i64,
            size,
            flags as i32,
            lock_owner,
        ))?;
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
        let written = self.block(self.vfs.write(
            ctx,
            Ino::from(inode),
            handle,
            offset as i64,
            &buf,
            0,
            flags as i32,
            lock_owner,
        ))?;
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
        self.block(self.vfs.flush(ctx, Ino::from(inode), handle, lock_owner))
    }

    fn fsync(
        &self,
        ctx: &Context,
        inode: Self::Inode,
        datasync: bool,
        handle: Self::Handle,
    ) -> io::Result<()> {
        let ctx = self.ctx(ctx);
        self.block(self.vfs.fsync(ctx, Ino::from(inode), handle, datasync))
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
        self.block(self.vfs.fallocate(
            ctx,
            Ino::from(inode),
            handle,
            offset as i64,
            length as i64,
            mode as i32,
        ))
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
        self.block(self.vfs.release(ctx, Ino::from(inode), handle))
            .map(|_| ())
    }

    fn opendir(
        &self,
        ctx: &Context,
        inode: Self::Inode,
        flags: u32,
    ) -> io::Result<(Option<Self::Handle>, OpenOptions)> {
        let cctx = FuseContext::from_uid_gid_pid(ctx.uid, ctx.gid, ctx.pid as u32);
        let fh = self.block(self.vfs.open_dir(&cctx, Ino::from(inode), flags as i32))?;
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
        let entries =
            self.block(
                self.vfs
                    .read_dir(&cctx, Ino::from(inode), handle, offset as i64, false),
            )?;
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
        let entries =
            self.block(
                self.vfs
                    .read_dir(&cctx, Ino::from(inode), handle, offset as i64, true),
            )?;
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
        self.block(self.vfs.release_dir(Ino::from(inode), handle))
    }

    fn statfs(&self, ctx: &Context, inode: Self::Inode) -> io::Result<statvfs64> {
        let _guard = self.guard()?;
        let ctx = self.ctx(ctx);
        let state = self.vfs.stat_fs(ctx, Ino::from(inode)).map_err(to_io)?;

        let total_blocks = (state.total_size / BLOCK_SIZE as u64).max(1);
        let used_blocks = state.used_size / BLOCK_SIZE as u64;
        let avail_blocks = total_blocks.saturating_sub(used_blocks);

        let mut st: statvfs64 = crate::convert::zeroed_pod();
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
