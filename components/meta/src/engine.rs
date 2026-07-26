// Copyright 2024 kisekifs
//
// JuiceFS, Copyright 2020 Juicedata, Inc.
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
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    collections::{HashMap, HashSet},
    fmt::{Display, Formatter},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

use bitflags::bitflags;
use bytes::Bytes;
use kiseki_common::{ChunkIndex, DOT, DOT_DOT, MODE_MASK_R, MODE_MASK_W, MODE_MASK_X};
use kiseki_types::{
    FileType,
    attr::{InodeAttr, SetAttrFlags},
    entry::{Entry, FullEntry},
    ino::{Ino, ROOT_INO},
    setting::Format,
    slice::{Slice, SliceID, Slices},
    stat::FSStat,
};
use scopeguard::defer;
use snafu::{ResultExt, ensure};
use tokio::{
    sync::{RwLock, Semaphore},
    time::Instant,
};
use tracing::{debug, instrument, trace, warn};

use crate::{
    backend::{BackendRef, key::Counter, open_backend},
    config::MetaConfig,
    context::FuseContext,
    err::{
        AlreadyInitializedSnafu, Error, Error::LibcError, IncompatibleFormatSnafu, LibcSnafu,
        Result, TokioJoinSnafu,
    },
    id_table::IdTable,
    open_files::{InvalidReq, OpenFiles, OpenFilesRef},
};

pub type MetaEngineRef = Arc<MetaEngine>;

pub fn open(config: MetaConfig) -> Result<MetaEngineRef> {
    let backend = open_backend(&config.dsn, config.skip_dir_mtime)?;
    let format = backend.load_format()?;
    let open_files = Arc::new(OpenFiles::new(config.open_cache, config.open_cache_limit));

    let me = MetaEngine {
        config,
        format,
        root: ROOT_INO,
        session_id: 0,
        open_files,
        symlinks: RwLock::default(),
        removed_files: RwLock::default(),
        // Limit the number of incoming requests being handled at the same time
        delete_semaphore: Arc::new(Semaphore::const_new(100)),
        dir_parents: RwLock::default(),
        fs_stat_used_size: AtomicU64::default(),
        fs_stat_file_count: AtomicU64::default(),
        free_inodes: IdTable::new(backend.clone(), Counter::NextInode),
        free_slices: IdTable::new(backend.clone(), Counter::NextSlice),
        backend,
    };

    debug!("open meta engine: {}", me);

    Ok(Arc::new(me))
}

// update_format is used to change the file system's setting.
pub fn update_format(dsn: &str, format: Format, force: bool) -> Result<()> {
    let backend = open_backend(dsn, Duration::from_millis(100))?;

    match backend.load_format() {
        Ok(stored) => {
            ensure!(force, AlreadyInitializedSnafu);
            if let Some(mismatch) = stored.layout_mismatch(&format) {
                return IncompatibleFormatSnafu {
                    field:     mismatch.field,
                    stored:    mismatch.stored,
                    requested: mismatch.requested,
                }
                .fail();
            }

            debug!("updating mutable fields on existing format");
            backend.set_format(&stored.merge_mutable_from(format))?;
            Ok(())
        }
        Err(e) => {
            if !matches!(e, Error::UninitializedEngine { .. }) {
                debug!("failed to load format: {:?}", e);
                return Err(e);
            }

            debug!("initializing format, root inode, and counters");
            let mut root = InodeAttr::default()
                .set_kind(FileType::Directory)
                .set_nlink(2)
                .set_length(4 << 10)
                .set_parent(ROOT_INO)
                .to_owned();
            root.set_mode(0o777);

            backend.initialize_volume(&format, &root)
        }
    }
}

pub struct MetaEngine {
    // config represents the configuration of the meta-engine,
    // like the underlying database engine, etc.
    config: MetaConfig,
    // format represents the config of the file system.
    format: Format,
    // The root inode of the file system.
    // TODO: review me. JuiceFS use it for enabling chroot.
    root:   Ino,

    // TODO: implement me
    session_id: u64,

    // track the open files, since we cannot remove the associated
    // info of the file when it is being opened.
    open_files:       OpenFilesRef,
    // a cache for symlink content
    symlinks:         RwLock<HashMap<Ino, Bytes>>,
    // track those inodes that didn't be removed totally since
    // someone has opened it.
    //
    // check this set when closing the inode, when we actually close
    // the inode, we should call the actual delete operation.
    removed_files:    RwLock<HashSet<Ino>>,
    // when do actual delete operation, try to acquire the permit first.
    delete_semaphore: Arc<Semaphore>,

    // directory inode -> parent inode
    dir_parents:        RwLock<HashMap<Ino, Ino>>,
    // stats
    fs_stat_used_size:  AtomicU64,
    fs_stat_file_count: AtomicU64,

    // id tables
    free_inodes: IdTable,
    free_slices: IdTable,

    // Backend for the meta engine
    backend: BackendRef,
}

impl Display for MetaEngine {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // TODO
        write!(f, "MetaEngine: {}", self.format.name)
    }
}

impl MetaEngine {
    async fn add_dir2parent_mapping(&self, inode: Ino, parent: Ino) {
        let read_guard = self.dir_parents.read().await;
        if !read_guard.contains_key(&inode) {
            drop(read_guard);
            let mut write_guard = self.dir_parents.write().await;
            write_guard.entry(inode).or_insert(parent);
        }
    }

    async fn del_dir2parents_mapping(&self, inode: Ino) {
        let mut write_guard = self.dir_parents.write().await;
        write_guard.remove(&inode);
    }
}

impl MetaEngine {
    pub const fn get_format(&self) -> &Format { &self.format }

    #[instrument(skip(self))]
    pub async fn next_slice_id(&self) -> Result<SliceID> { self.free_slices.next().await }

    /// [`Self::stat_fs`] returns summary statistics of a volume.
    ///
    /// TODO: support chroot ?
    pub fn stat_fs(&self, _ctx: Arc<FuseContext>, _inode: Ino) -> Result<FSStat> {
        let total_used_file_count = self.fs_stat_file_count.load(Ordering::Acquire);
        let total_used_size = self.fs_stat_used_size.load(Ordering::Acquire);
        Ok(FSStat {
            total_size: self.format.max_capacity.unwrap_or(usize::MAX) as u64,
            used_size:  total_used_size,
            file_count: total_used_file_count,
        })
    }

    // Verifies if the requested access mode (mmask) is permitted for the given user
    // or group based on the file's actual permissions (mode). Ensures access
    // control based on file permissions.
    pub fn check_root(&self, inode: Ino) -> Ino {
        if inode.is_zero() {
            ROOT_INO // force using Root inode
        } else if inode == ROOT_INO {
            self.root
        } else {
            inode
        }
    }

    /// Lookup returns the inode and attributes for the given entry in a
    /// directory.
    pub async fn lookup(
        &self,
        ctx: Arc<FuseContext>,
        parent: Ino,
        name: &str,
        check_perm: bool,
    ) -> Result<(Ino, InodeAttr)> {
        trace!(parent=?parent, ?name, "lookup");
        let _timer = crate::metrics_start_timer!(lookup_duration_ms);
        let parent = self.check_root(parent);

        if check_perm {
            let parent_attr = self.get_attr(parent).await?;
            ctx.check_access(&parent_attr, MODE_MASK_X)?;
        }
        let mut name = name;
        if name == DOT_DOT {
            if parent == self.root {
                // If parent is already the root directory,
                // sets name to "." (current directory).
                name = DOT;
            } else {
                // Otherwise, retrieves attributes of parent.
                // Checks if parent is a directory using attr.Typ != TypeDirectory.
                // Returns syscall.ENOTDIR if not.
                let parent_attr = self.get_attr(parent).await?;
                ensure!(
                    parent_attr.get_filetype() == FileType::Directory,
                    LibcSnafu {
                        errno: libc::ENOTDIR,
                    }
                );
                let attr = self.get_attr(parent_attr.parent).await?;
                crate::metrics_counter!(lookups_total);
                return Ok((parent_attr.parent, attr));
            }
        }
        if name == DOT {
            let attr = self.get_attr(parent).await?;
            crate::metrics_counter!(lookups_total);
            return Ok((parent, attr));
        }
        let (inode, attr) = self.do_lookup(parent, name)?;

        if attr.kind == FileType::Directory {
            self.add_dir2parent_mapping(inode, parent).await;
        }

        crate::metrics_counter!(lookups_total);
        Ok((inode, attr))
    }

    fn do_lookup(&self, parent: Ino, name: &str) -> Result<(Ino, InodeAttr)> {
        let entry_info = self.backend.get_dentry(parent, name)?;
        let inode = entry_info.inode;
        let attr = self.backend.get_attr(inode)?;
        Ok((inode, attr))
    }

    pub async fn get_attr(&self, inode: Ino) -> Result<InodeAttr> {
        let inode = self.check_root(inode);
        // check cache
        if !self.config.open_cache.is_zero()
            && let Some(attr) = self.open_files.load_attr(inode, false).await
        {
            crate::metrics_cache!(HIT);
            return Ok(attr);
        }

        crate::metrics_cache!(MISS);
        // TODO: add timeout here
        let mut attr = self.backend.get_attr(inode)?;

        // update cache
        self.open_files.refresh_attr(inode, &mut attr).await;
        if attr.is_filetype(FileType::Directory) && !inode.is_root() {
            self.add_dir2parent_mapping(inode, attr.parent).await;
        }
        Ok(attr)
    }

    // Readdir returns all entries for given directory, which include attributes if
    // plus is true.
    pub async fn read_dir(&self, ctx: &FuseContext, inode: Ino, plus: bool) -> Result<Vec<Entry>> {
        debug!(dir=?inode, "readdir in plus?, {plus}");
        let inode = self.check_root(inode);
        let attr = self.get_attr(inode).await?;
        let mmask = if plus {
            MODE_MASK_R | MODE_MASK_X
        } else {
            MODE_MASK_X
        };

        ctx.check_access(&attr, mmask)?;

        // let mut basic_entries = Entry::new_basic_entry_pair(inode, attr.parent);
        let mut basic_entries = vec![];
        self.do_read_dir(inode, plus, &mut basic_entries, -1)
            .await?;

        debug!("find entries: {:?}", &basic_entries);

        Ok(basic_entries)
    }

    async fn do_read_dir(
        &self,
        inode: Ino,
        plus: bool,
        basic_entries: &mut Vec<Entry>,
        _limit: i64,
    ) -> Result<()> {
        let backend = self.backend.clone();
        let entries = tokio::task::spawn_blocking(move || backend.list_dentry(inode, _limit))
            .await
            .context(TokioJoinSnafu)??;
        // let entries = self.backend.list_entry_info(inode, _limit)?;
        for de in entries {
            let entry = if plus {
                let attr = self.backend.get_attr(de.inode)?;
                Entry::Full(FullEntry {
                    inode,
                    name: de.name.clone(),
                    attr,
                })
            } else {
                Entry::DEntry(de)
            };
            basic_entries.push(entry);
        }
        Ok(())
    }

    // Change root to a directory specified by sub_dir.
    // pub async fn chroot<P: AsRef<Path>>(&self, ctx: &FuseContext, sub_dir: P) ->
    // Result<()> {     let sub_dir = sub_dir.as_ref();
    //     for c in sub_dir.components() {
    //         let name = match c {
    //             Component::Normal(name) => {
    //                 name.to_str().expect("invalid path component { sub_dir}")
    //             }
    //             _ => unreachable!("invalid path component: {:?}", c),
    //         };
    //         let (inode, attr) = match self.lookup(ctx, self.root, name,
    // true).await {             Ok(r) => r,
    //             Err(e) => {
    //                 if e.to_errno() == libc::ENOENT {
    //                     let (inode, attr) = self.mkdir(ctx, self.root, name,
    // 0o777, 0).await?;                     (inode, attr)
    //                 } else {
    //                     return Err(e);
    //                 }
    //             }
    //         };
    //         ensure!(
    //             attr.get_filetype() == FileType::Directory,
    //             LibcSnafu {
    //                 errno: libc::ENOTDIR,
    //             }
    //         );
    //     }
    //     Ok(())
    // }

    // Mkdir creates a sub-directory with given name and mode.
    pub async fn mkdir(
        &self,
        ctx: Arc<FuseContext>,
        parent: Ino,
        name: &str,
        mode: u32,
        umask: u32,
    ) -> Result<(Ino, InodeAttr)> {
        match self
            .mknod(
                ctx,
                parent,
                name,
                FileType::Directory,
                mode,
                umask,
                0,
                String::new(),
            )
            .await
        {
            Ok(r) => {
                crate::metrics_business_op!(MKDIR, crate::metrics::labels::TYPE_DIRECTORY);
                crate::metrics_fs_file_count_change!(1);
                self.add_dir2parent_mapping(r.0, parent).await;
                Ok(r)
            }
            Err(e) => Err(e),
        }
    }

    /// [`Self::rmdir`] removes an empty subdirectory.
    pub async fn rmdir(&self, ctx: Arc<FuseContext>, parent: Ino, name: &str) -> Result<()> {
        let parent = self.check_root(parent);
        let (dentry, _) = self
            .backend
            .do_rmdir(ctx, parent, name, self.config.skip_dir_mtime)?;

        crate::metrics_business_op!(RMDIR, crate::metrics::labels::TYPE_DIRECTORY);
        crate::metrics_fs_file_count_change!(-1);
        crate::metrics_fs_size_change!(-4096);

        self.fs_stat_file_count.fetch_sub(1, Ordering::AcqRel);
        self.fs_stat_used_size.fetch_sub(4096, Ordering::AcqRel);
        self.del_dir2parents_mapping(dentry.inode).await;
        Ok(())
    }

    // Mknod creates a node in a directory with given name, type and permissions.
    #[instrument(skip(self, ctx), fields(parent = ? parent, name = ? name, typ = ? typ, mode = ? mode, cumask = ? umask, rdev = ? rdev, path = ? path))]
    #[allow(clippy::too_many_arguments)]
    pub async fn mknod(
        &self,
        ctx: Arc<FuseContext>,
        parent: Ino,
        name: &str,
        typ: FileType,
        mode: u32,
        umask: u32,
        rdev: u32,
        path: String,
    ) -> Result<(Ino, InodeAttr)> {
        ensure!(!self.config.read_only, LibcSnafu { errno: libc::EROFS });
        ensure!(
            !name.is_empty(),
            LibcSnafu {
                errno: libc::ENOENT,
            }
        );

        let parent = self.check_root(parent);

        let new_inode = Ino::from(self.free_inodes.next().await?);
        debug!("new inode: {}", new_inode);

        let mut attr = InodeAttr::default()
            .set_mode(mode & !umask) // erase umask
            .set_kind(typ)
            .set_gid(ctx.gid)
            .set_uid(ctx.uid)
            .set_parent(parent)
            .to_owned();
        if matches!(typ, FileType::Directory) {
            attr.set_nlink(2).set_length(4 << 10);
        } else {
            attr.set_nlink(1);
            if matches!(typ, FileType::Symlink) {
                attr.set_length(path.len() as u64);
            } else {
                attr.set_length(0).set_rdev(rdev);
            }
        };

        let r = self
            .backend
            .do_mknod(ctx, new_inode, attr, parent, name, typ, path)?;

        // Record business metrics for node creation
        let inode_type = match typ {
            FileType::RegularFile => crate::metrics::labels::TYPE_REGULAR_FILE,
            FileType::Directory => crate::metrics::labels::TYPE_DIRECTORY,
            FileType::Symlink => crate::metrics::labels::TYPE_SYMLINK,
            _ => crate::metrics::labels::TYPE_OTHER,
        };
        crate::metrics_counter_with_labels!(inodes_created_total,
            crate::metrics::labels::TYPE => inode_type);
        crate::metrics_fs_file_count_change!(1);
        crate::metrics_fs_size_change!(4096);

        self.fs_stat_file_count.fetch_add(1, Ordering::AcqRel);
        self.fs_stat_used_size.fetch_add(4096, Ordering::Acquire);

        Ok(r)
    }

    pub async fn create(
        &self,
        ctx: Arc<FuseContext>,
        parent: Ino,
        name: &str,
        mode: u32,
        umask: u32,
        flags: i32,
    ) -> Result<(Ino, InodeAttr)> {
        debug!(
            "create with parent {:?}, name {:?}, mode {:?}, umask {:?}, flags {:?}",
            parent, name, mode, umask, flags
        );
        let mut x = match self
            .mknod(
                ctx,
                parent,
                name,
                FileType::RegularFile,
                mode,
                umask,
                0,
                String::new(),
            )
            .await
        {
            Ok(r) => {
                crate::metrics_business_op!(CREATE, crate::metrics::labels::TYPE_REGULAR_FILE);
                r
            }
            Err(e) if matches!(e, LibcError{errno, ..} if errno == libc::EEXIST) => {
                warn!("create failed: {:?}", e);
                self.do_lookup(parent, name)?
            }
            Err(e) => return Err(e),
        };

        self.open_files.open(x.0, &mut x.1).await;
        Ok(x)
    }

    pub async fn check_set_attr(
        &self,
        ctx: &FuseContext,
        ino: Ino,
        flags: SetAttrFlags,
        new_attr: &mut InodeAttr,
    ) -> Result<()> {
        let inode = self.check_root(ino);
        let cur_attr = self.get_attr(inode).await?;
        self.merge_attr(ctx, flags, inode, &cur_attr, new_attr, SystemTime::now())?;

        // TODO: implement me
        Ok(())
    }

    // SetAttr updates the attributes for given node.
    pub async fn set_attr(
        &self,
        ctx: &FuseContext,
        flags: SetAttrFlags,
        ino: Ino,
        new_attr: &mut InodeAttr,
    ) -> Result<()> {
        let inode = self.check_root(ino);

        let cur_attr = self.backend.get_attr(inode)?;
        let now = SystemTime::now();
        let mut dirty_attr = self.merge_attr(ctx, flags, ino, &cur_attr, new_attr, now)?;
        dirty_attr.ctime = now;
        self.backend.set_attr(inode, &dirty_attr)?;
        Ok(())
    }

    fn merge_attr(
        &self,
        ctx: &FuseContext,
        flags: SetAttrFlags,
        _inode: Ino,
        cur: &InodeAttr,
        new_attr: &mut InodeAttr,
        now: SystemTime,
    ) -> Result<InodeAttr> {
        let mut dirty_attr = cur.clone();
        if flags.contains(SetAttrFlags::MODE)
            && flags.contains(SetAttrFlags::UID | SetAttrFlags::GID)
        {
            // This operation isolates the file permission bits representing the user's file
            // type (setuid, setgid, or sticky bit).
            dirty_attr.mode |= cur.mode & 0o6000;
        }
        let mut changed = false;
        if cur.mode & 0o6000 != 0 && flags.contains(SetAttrFlags::UID | SetAttrFlags::GID) {
            #[cfg(target_os = "linux")]
            {
                if !dirty_attr.is_dir() {
                    if ctx.uid != 0 || (dirty_attr.mode >> 3) & 1 != 0 {
                        // clear SUID and SGID
                        dirty_attr.mode &= 0o1777; // WHY ?
                        new_attr.mode &= 0o1777;
                    } else {
                        // keep SGID if the file is non-group-executable
                        dirty_attr.mode &= 0o3777;
                        new_attr.mode &= 0o3777;
                    }
                }
            }
            changed = true;
        }
        if flags.contains(SetAttrFlags::GID) {
            ensure!(
                ctx.uid == 0 || ctx.uid == cur.uid,
                LibcSnafu { errno: libc::EPERM }
            );
            // if ctx.uid != 0 && ctx.uid != cur.uid {
            //     return Err(MetaError::ErrLibc { kind: libc::EPERM });
            // }
            if cur.gid != new_attr.gid {
                if ctx.check_permission && cur.uid != 0 && !ctx.contains_gid(new_attr.gid) {
                    LibcSnafu { errno: libc::EPERM }.fail()?;
                }
                dirty_attr.gid = new_attr.gid;
                changed = true;
            }
        }
        if flags.contains(SetAttrFlags::UID) && cur.uid != new_attr.uid {
            ensure!(ctx.uid == 0, LibcSnafu { errno: libc::EPERM });
            // if ctx.uid != 0 {
            //     return Err(MetaError::ErrLibc { kind: libc::EPERM });
            // }
            dirty_attr.uid = new_attr.uid;
            changed = true;
        }
        if flags.contains(SetAttrFlags::MODE) {
            if ctx.uid != 0 && new_attr.mode & 0o2000 != 0 && ctx.uid != cur.gid {
                new_attr.mode &= 0o5777;
            }
            if cur.mode != new_attr.mode {
                if ctx.uid != 0
                    && ctx.uid != cur.uid
                    && (cur.mode & 0o1777 != new_attr.mode & 0o1777
                        || new_attr.mode & 0o2000 > cur.mode & 0o2000
                        || new_attr.mode & 0o4000 > cur.mode & 0o4000)
                {
                    LibcSnafu { errno: libc::EPERM }.fail()?;
                }

                dirty_attr.mode = new_attr.mode;
                changed = true;
            }
        }
        if flags.contains(SetAttrFlags::ATIME_NOW) {
            if ctx.check_access(cur, MODE_MASK_W).is_err() {
                ensure!(ctx.uid == cur.uid, LibcSnafu { errno: libc::EPERM });
            }
            dirty_attr.atime = now;
            changed = true;
        } else if flags.contains(SetAttrFlags::ATIME) {
            ensure!(
                ctx.uid == cur.uid,
                LibcSnafu {
                    errno: libc::EACCES,
                }
            );
            if ctx.check_access(cur, MODE_MASK_W).is_err() {
                ensure!(
                    ctx.uid == cur.uid,
                    LibcSnafu {
                        errno: libc::EACCES,
                    }
                );
            }
            dirty_attr.atime = new_attr.atime;
            changed = true;
        }
        if flags.contains(SetAttrFlags::MTIME_NOW) {
            if ctx.check_access(cur, MODE_MASK_W).is_err() {
                ensure!(
                    ctx.uid == cur.uid,
                    LibcSnafu {
                        errno: libc::EACCES,
                    }
                );
            }
            dirty_attr.mtime = now;
            changed = true;
        } else if flags.contains(SetAttrFlags::MTIME) {
            ensure!(
                ctx.uid == cur.uid,
                LibcSnafu {
                    errno: libc::EACCES,
                }
            );
            if ctx.check_access(cur, MODE_MASK_W).is_err() && ctx.uid != cur.uid {
                LibcSnafu {
                    errno: libc::EACCES,
                }
                .fail()?;
            }
            dirty_attr.mtime = new_attr.mtime;
            changed = true;
        }
        if flags.contains(SetAttrFlags::FLAG) && dirty_attr.flags != new_attr.flags {
            dirty_attr.flags = new_attr.flags;
            changed = true;
        }
        if !changed {
            dirty_attr = cur.clone();
        }
        Ok(dirty_attr)
    }

    // Open checks permission on a node and track it as open.
    pub async fn open_inode(&self, ctx: &FuseContext, inode: Ino, flags: i32) -> Result<InodeAttr> {
        if self.config.read_only
            && flags & (libc::O_WRONLY | libc::O_RDWR | libc::O_TRUNC | libc::O_APPEND) != 0
        {
            LibcSnafu { errno: libc::EROFS }.fail()?;
        }

        defer!(self.refresh_atime(ctx, inode));

        if !self.config.open_cache.is_zero()
            && let Some(attr) = self.open_files.load_attr(inode, true).await
        {
            return Ok(attr);
        }

        let mut attr = self.backend.get_attr(inode)?;
        let mask = match flags & (libc::O_RDONLY | libc::O_WRONLY | libc::O_RDWR) {
            libc::O_RDONLY => MODE_MASK_R,
            libc::O_WRONLY => MODE_MASK_W,
            libc::O_RDWR => MODE_MASK_W | MODE_MASK_R,
            _ => LibcSnafu {
                errno: libc::EINVAL,
            }
            .fail()?,
        };

        ctx.check_access(&attr, mask)?;

        let Some(attr_flags) = u8::try_from(attr.flags)
            .ok()
            .and_then(kiseki_types::attr::Flags::from_bits)
        else {
            return LibcSnafu { errno: libc::EIO }.fail();
        };
        if (attr_flags.contains(kiseki_types::attr::Flags::IMMUTABLE))
            && flags & (libc::O_WRONLY | libc::O_RDWR) != 0
        {
            LibcSnafu { errno: libc::EPERM }.fail()?;
        }
        if attr_flags.contains(kiseki_types::attr::Flags::APPEND) {
            if flags & (libc::O_WRONLY | libc::O_RDWR) != 0 && flags & libc::O_APPEND == 0 {
                LibcSnafu { errno: libc::EPERM }.fail()?;
            }
            if flags & libc::O_TRUNC != 0 {
                LibcSnafu { errno: libc::EPERM }.fail()?;
            }
        }
        self.open_files.open(inode, &mut attr).await;
        Ok(attr)
    }

    const fn refresh_atime(&self, _ctx: &FuseContext, _inode: Ino) {}

    // Write put a slice of data on top of the given chunk.
    #[instrument(skip(self, mtime), fields(inode = ? inode, chunk_idx = ? chunk_idx, chunk_pos = ? chunk_pos, slice = ? slice))]
    pub async fn write_slice(
        &self,
        inode: Ino,
        chunk_idx: usize,
        chunk_pos: usize,
        slice: Slice, // FIXME: introduce another slice type.
        mtime: Instant,
    ) -> Result<()> {
        assert!(
            matches!(slice, Slice::Owned { .. }),
            "slice should be owned for writing slice"
        );
        debug!(
            "write_slice: with inode {:?}, chunk_idx {:?}, off {:?}, slice_id {:?}, mtime {:?}",
            inode, chunk_idx, chunk_pos, slice, mtime
        );

        ensure!(
            slice.get_chunk_pos() == chunk_pos,
            LibcSnafu {
                errno: libc::EINVAL,
            }
        );
        let commit = self.backend.commit_slice(inode, chunk_idx, &slice)?;
        if !commit.inserted {
            debug!("{inode} ignored duplicate slice {slice:?} at chunk {chunk_idx}");
        }

        if commit.slice_count > 350 || commit.slice_count % 100 == 99 {
            // start a background task to compact these slices
            // TODO: we need to do compaction
        }

        // update the used size
        if commit.grew_by > 0 {
            self.fs_stat_used_size
                .fetch_add(commit.grew_by, Ordering::AcqRel);
        }

        // TODO: update the cache
        self.open_files
            .invalid(inode, InvalidReq::OneChunk(chunk_idx))
            .await;

        Ok(())
    }

    /// [MetaEngine::set_lk] sets a file range lock on given file.
    #[allow(clippy::too_many_arguments)]
    pub async fn set_lk(
        &self,
        _ctx: Arc<FuseContext>,
        inode: Ino,
        owner: u64,
        block: bool,
        ltype: libc::c_int,
        start: u64,
        end: u64,
    ) -> Result<()> {
        debug!(
            "set_lk with inode {:?}, owner {:?}, block {:?}, ltype {:?}, start {:?}, end {:?}",
            inode, owner, block, ltype, start, end
        );
        Ok(())
    }

    /// [MetaEngine::read_slice] returns the rangemap of slices on the given
    /// chunk.
    pub async fn read_slice(
        &self,
        inode: Ino,
        chunk_index: ChunkIndex,
    ) -> Result<Option<Arc<Slices>>> {
        debug!(
            "read_slice with inode {:?}, chunk_index {:?}",
            inode, chunk_index
        );

        if let Some(slices) = self.open_files.load_slices(inode, chunk_index).await {
            debug!(
                "read ino {} chunk {} slice info from cache, get count {}",
                inode,
                chunk_index,
                slices.len(),
            );
            return Ok(Some(slices));
        }

        let slices = self.backend.get_chunk_slices(inode, chunk_index)?;

        // fixme
        let slices = Arc::new(slices);
        // TODO: build cache.
        self.open_files
            .refresh_slices(inode, chunk_index, slices.clone())
            .await;

        Ok(Some(slices))
    }

    // Fallocate preallocate given space for given file.
    pub async fn fallocate(
        &self,
        _inode: Ino,
        _offset: usize,
        _length: usize,
        mode: u8,
    ) -> Result<()> {
        validate_fallocate_mode(mode)
    }

    pub async fn flock(
        &self,
        _ctx: Arc<FuseContext>,
        inode: Ino,
        owner: u64,
        ltype: libc::c_int,
    ) -> Result<()> {
        debug!(
            "TO IMPLEMENT: flock with inode {:?}, owner {:?}, ltype {:?}",
            inode, owner, ltype
        );
        Ok(())
    }

    /// [`Self::close`] closes a file by decreasing the reference count of the
    /// open file; if the reference count is zero, then we can remove the
    /// file from the cache.
    pub async fn close(&self, inode: Ino) -> Result<()> {
        if self.open_files.close(inode).await {
            let mut write_guard = self.removed_files.write().await;
            if write_guard.remove(&inode) {
                // TODO: doDeleteSustainedInode
            }
        }
        Ok(())
    }

    /// [`Self::truncate`] changes the length of the given file.
    ///
    /// When the `set_attr` operation carries an `FH`, the permission
    /// check.
    pub async fn truncate(
        &self,
        ctx: Arc<FuseContext>,
        inode: Ino,
        size: u64,
        skip_perm_check: bool,
    ) -> Result<InodeAttr> {
        let attr = if let Some(of) = self.open_files.load(&inode).await {
            let guard = of.read_guard().await;
            if guard.attr.length == size {
                return Ok(guard.attr.clone());
            }
            let attr = self
                .backend
                .do_truncate(ctx, inode, size, skip_perm_check)?;
            drop(guard); // explicitly drop the guard for keeping holding the lock
            attr
        } else {
            self.backend
                .do_truncate(ctx, inode, size, skip_perm_check)?
        };
        self.open_files.invalid(inode, InvalidReq::All).await;
        Ok(attr)
    }
}

// Link
impl MetaEngine {
    pub async fn link(
        &self,
        ctx: Arc<FuseContext>,
        inode: Ino,
        new_parent: Ino,
        new_name: &str,
    ) -> Result<InodeAttr> {
        let current_attr = self.get_attr(inode).await?;
        ensure!(!current_attr.is_dir(), LibcSnafu { errno: libc::EPERM });

        let new_attr = self.backend.do_link(ctx, inode, new_parent, new_name)?;

        crate::metrics_business_op!(HARDLINK, crate::metrics::labels::OP_HARDLINK_CREATE);
        self.open_files.invalid(inode, InvalidReq::OnlyAttr).await;

        Ok(new_attr)
    }

    pub async fn unlink(&self, ctx: Arc<FuseContext>, parent: Ino, name: &str) -> Result<()> {
        let open_files = self.open_files.clone();
        let unlink_result = self
            .backend
            .do_unlink(ctx, parent, name.to_string(), self.session_id, open_files)
            .await?;

        // Record business metrics for unlink
        crate::metrics_counter_with_labels!(file_ops_total,
            crate::metrics::labels::OPERATION => crate::metrics::labels::OP_UNLINK);
        if unlink_result.freed_inode > 0 {
            crate::metrics_counter_with_labels!(inodes_deleted_total,
                crate::metrics::labels::TYPE => crate::metrics::labels::TYPE_REGULAR_FILE);
        }
        crate::metrics_fs_size_change!(-(unlink_result.freed_space as i64));
        crate::metrics_fs_file_count_change!(-(unlink_result.freed_inode as i64));

        self.fs_stat_used_size
            .fetch_sub(unlink_result.freed_space, Ordering::AcqRel);
        self.fs_stat_file_count
            .fetch_sub(unlink_result.freed_inode, Ordering::AcqRel);
        self.open_files
            .invalid(unlink_result.inode, InvalidReq::OnlyAttr)
            .await;
        if unlink_result.removed.is_some() {
            self.delete_file(unlink_result.is_opened, unlink_result.inode)
                .await;
        }
        Ok(())
    }

    // creates a symlink in a directory with given name.
    pub async fn symlink(
        &self,
        ctx: Arc<FuseContext>,
        parent: Ino,
        link_name: &str,
        target: &Path,
    ) -> Result<(Ino, InodeAttr)> {
        // mode of symlink is ignored in POSIX.
        let (inode, attr) = self
            .mknod(
                ctx,
                parent,
                link_name,
                FileType::Symlink,
                0o777,
                0,
                0,
                target.to_string_lossy().to_string(),
            )
            .await?;

        crate::metrics_business_op!(SYMLINK_CREATE);
        Ok((inode, attr))
    }

    pub async fn readlink(&self, _ctx: Arc<FuseContext>, inode: Ino) -> Result<Bytes> {
        let read_guard = self.symlinks.read().await;
        if let Some(target) = read_guard.get(&inode) {
            return Ok(target.clone());
        }
        drop(read_guard);
        let t = self.backend.do_readlink(inode)?;
        self.symlinks.write().await.insert(inode, t.clone());
        Ok(t)
    }
}

// Delete Helper
impl MetaEngine {
    // delete_file may be unable to delete the file directly since the file may be
    // still in using.
    //
    // If the file is opened, then we just add the file to the removed_files set.
    // When close the file, we should check the remove set, when we actually close
    // the file, we can do the actual remove.
    async fn delete_file(&self, opened: bool, inode: Ino) {
        if opened {
            let mut write_guard = self.removed_files.write().await;
            write_guard.insert(inode);
            drop(write_guard);
        } else {
            // spawn a task to delete the file
            let sem = self.delete_semaphore.clone();
            let backend = self.backend.clone();
            // spawn task here since we use semaphore to limit the concurrent
            tokio::spawn(async move {
                // Safety: the semaphore's lifetime is binding to the MetaEngine.
                let Ok(_permit) = sem.acquire().await else {
                    return;
                };
                backend.do_delete_chunks(inode);
            });
        }
    }
}

// Rename
impl MetaEngine {
    // move an entry from a source directory to another with given name.
    // The targeted entry will be overwrited if it's a file or empty directory.
    pub async fn rename(
        &self,
        ctx: Arc<FuseContext>,
        old_parent: Ino,
        old_name: &str,
        new_parent: Ino,
        new_name: &str,
        flags: u32,
    ) -> Result<()> {
        let rename_flags = validate_rename_flags(flags)?;

        let open_files = self.open_files.clone();
        let rename_result = self
            .backend
            .do_rename(
                ctx,
                self.session_id,
                old_parent,
                old_name,
                new_parent,
                new_name,
                rename_flags,
                open_files,
            )
            .await?;

        // Record business metrics for rename - we'll determine the type from backend
        // For now, record as generic operation
        crate::metrics_histogram_with_labels!(fs_operation_duration_ms, 0.0,
            crate::metrics::labels::OPERATION => crate::metrics::labels::OP_RENAME);

        if let Some((inode, opened)) = rename_result.need_delete {
            self.delete_file(opened, inode).await;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FallocateMode(pub u8);

bitflags! {
    impl FallocateMode: u8 {
        const KEEP_SIZE = 0x01;
        const PUNCH_HOLE = 0x02;
        const NO_HIDE_STALE = 0x04;
        const COLLAPSE_RANGE = 0x08;
        const ZERO_RANGE = 0x10;
        const INSERT_RANGE = 0x20;
    }
}

fn validate_fallocate_mode(mode: u8) -> Result<()> {
    let Some(mode) = FallocateMode::from_bits(mode) else {
        return LibcSnafu {
            errno: libc::EINVAL,
        }
        .fail();
    };
    if mode.contains(FallocateMode::COLLAPSE_RANGE) && mode != FallocateMode::COLLAPSE_RANGE {
        return LibcSnafu {
            errno: libc::EINVAL,
        }
        .fail();
    }
    if mode.contains(FallocateMode::INSERT_RANGE) && mode != FallocateMode::INSERT_RANGE {
        return LibcSnafu {
            errno: libc::EINVAL,
        }
        .fail();
    }
    if mode.contains(FallocateMode::PUNCH_HOLE) && !mode.contains(FallocateMode::KEEP_SIZE) {
        return LibcSnafu {
            errno: libc::EINVAL,
        }
        .fail();
    }

    LibcSnafu {
        errno: libc::ENOTSUP,
    }
    .fail()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RenameFlags(pub u32);

bitflags! {
    impl RenameFlags: u32 {
        const ZERO = 0;
        const NOREPLACE = 1;
        const EXCHANGE = 2;
        const WHITEOUT = 4;
    }
}

fn validate_rename_flags(flags: u32) -> Result<RenameFlags> {
    let Some(flags) = RenameFlags::from_bits(flags) else {
        return LibcSnafu {
            errno: libc::EINVAL,
        }
        .fail();
    };
    if flags.contains(RenameFlags::NOREPLACE) && flags.contains(RenameFlags::EXCHANGE) {
        return LibcSnafu {
            errno: libc::EINVAL,
        }
        .fail();
    }
    if flags.contains(RenameFlags::WHITEOUT) {
        return LibcSnafu {
            errno: libc::ENOTSUP,
        }
        .fail();
    }
    Ok(flags)
}

#[cfg(test)]
mod boundary_tests {
    use kiseki_types::ToErrno;

    use super::{RenameFlags, validate_fallocate_mode, validate_rename_flags};

    #[test]
    fn invalid_fallocate_flags_are_rejected_without_panicking() {
        assert_eq!(
            validate_fallocate_mode(0x80).unwrap_err().to_errno(),
            libc::EINVAL
        );
        assert_eq!(
            validate_fallocate_mode(0x02).unwrap_err().to_errno(),
            libc::EINVAL
        );
    }

    #[test]
    fn recognized_fallocate_modes_are_honestly_unsupported() {
        assert_eq!(
            validate_fallocate_mode(0).unwrap_err().to_errno(),
            libc::ENOTSUP
        );
        assert_eq!(
            validate_fallocate_mode(0x03).unwrap_err().to_errno(),
            libc::ENOTSUP
        );
    }

    #[test]
    fn rename_flag_validation_distinguishes_invalid_and_unsupported() {
        assert_eq!(
            validate_rename_flags(u32::MAX).unwrap_err().to_errno(),
            libc::EINVAL
        );
        assert_eq!(
            validate_rename_flags((RenameFlags::NOREPLACE | RenameFlags::EXCHANGE).bits())
                .unwrap_err()
                .to_errno(),
            libc::EINVAL
        );
        assert_eq!(
            validate_rename_flags(RenameFlags::WHITEOUT.bits())
                .unwrap_err()
                .to_errno(),
            libc::ENOTSUP
        );
        assert_eq!(validate_rename_flags(0).unwrap(), RenameFlags::ZERO);
    }
}

#[cfg(all(test, feature = "meta-rocksdb"))]
mod format_tests {
    use std::time::Duration;

    use kiseki_types::{ino::ROOT_INO, setting::Format};
    use tempfile::TempDir;

    use super::update_format;
    use crate::{
        Error,
        backend::{BackendRef, key::Counter, open_backend},
    };

    fn test_store() -> (TempDir, String) {
        let tempdir = tempfile::tempdir().expect("create metadata test directory");
        let dsn = format!("rocksdb://:{}", tempdir.path().display());
        (tempdir, dsn)
    }

    fn open_test_backend(dsn: &str) -> BackendRef {
        open_backend(dsn, Duration::from_millis(100)).expect("open metadata test backend")
    }

    #[test]
    fn initializes_format_root_and_inode_counter() {
        let (_tempdir, dsn) = test_store();
        let format = Format {
            name: "new-volume".to_string(),
            max_capacity: Some(1024),
            max_inodes: Some(128),
            ..Format::default()
        };

        update_format(&dsn, format.clone(), false).expect("initialize volume");

        let backend = open_test_backend(&dsn);
        let stored = backend.load_format().expect("load initialized format");
        assert_eq!(stored.name, format.name);
        assert_eq!(stored.max_capacity, format.max_capacity);
        assert_eq!(stored.max_inodes, format.max_inodes);
        let root = backend.get_attr(ROOT_INO).expect("load initialized root");
        assert!(root.is_dir());
        assert_eq!(root.mode, 0o777);
        assert_eq!(
            backend
                .load_count(Counter::NextInode)
                .expect("load initialized inode counter"),
            2
        );
    }

    #[test]
    fn rejects_existing_volume_without_force_and_preserves_format() {
        let (_tempdir, dsn) = test_store();
        let initial = Format {
            name: "initial-volume".to_string(),
            max_capacity: Some(1024),
            max_inodes: Some(128),
            ..Format::default()
        };
        update_format(&dsn, initial.clone(), false).expect("initialize volume");

        let requested = Format {
            name: "replacement-volume".to_string(),
            max_capacity: Some(2048),
            max_inodes: Some(256),
            ..initial
        };
        let result = update_format(&dsn, requested, false);

        assert!(
            matches!(result, Err(Error::AlreadyInitialized { .. })),
            "existing volume must return AlreadyInitialized"
        );
        let stored = open_test_backend(&dsn)
            .load_format()
            .expect("reload original format");
        assert_eq!(stored.name, initial.name);
        assert_eq!(stored.max_capacity, initial.max_capacity);
        assert_eq!(stored.max_inodes, initial.max_inodes);
    }

    #[test]
    fn force_updates_only_mutable_format_fields() {
        let (_tempdir, dsn) = test_store();
        let initial = Format {
            name: "initial-volume".to_string(),
            max_capacity: Some(1024),
            max_inodes: Some(128),
            ..Format::default()
        };
        update_format(&dsn, initial.clone(), false).expect("initialize volume");

        let requested = Format {
            name: "renamed-volume".to_string(),
            max_capacity: Some(2048),
            max_inodes: None,
            ..initial
        };
        update_format(&dsn, requested.clone(), true).expect("force mutable update");

        let stored = open_test_backend(&dsn)
            .load_format()
            .expect("load updated format");
        assert_eq!(stored.name, requested.name);
        assert_eq!(stored.max_capacity, requested.max_capacity);
        assert_eq!(stored.max_inodes, requested.max_inodes);
        assert_eq!(stored.chunk_size, initial.chunk_size);
        assert_eq!(stored.block_size, initial.block_size);
        assert_eq!(stored.page_size, initial.page_size);
    }

    #[test]
    fn rejects_forced_layout_change_and_preserves_format() {
        let (_tempdir, dsn) = test_store();
        let initial = Format {
            name: "initial-volume".to_string(),
            ..Format::default()
        };
        update_format(&dsn, initial.clone(), false).expect("initialize volume");

        let requested = Format {
            name: "renamed-volume".to_string(),
            block_size: initial.block_size * 2,
            ..initial
        };
        let result = update_format(&dsn, requested, true);

        assert!(
            matches!(
                result,
                Err(Error::IncompatibleFormat {
                    field: kiseki_types::setting::FormatLayoutField::BlockSize,
                    ..
                })
            ),
            "force must return IncompatibleFormat for a layout change"
        );
        let stored = open_test_backend(&dsn)
            .load_format()
            .expect("reload original format");
        assert_eq!(stored.name, initial.name);
        assert_eq!(stored.chunk_size, initial.chunk_size);
        assert_eq!(stored.block_size, initial.block_size);
        assert_eq!(stored.page_size, initial.page_size);
    }
}
