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

use std::{sync::Arc, time::SystemTime};

use fuser::TimeOrNow;
use kiseki_common::DOT;
use kiseki_meta::context::FuseContext;
use kiseki_types::{
    attr::{InodeAttr, SetAttrFlags},
    entry::FullEntry,
    ino::{Ino, ROOT_INO},
    internal_nodes::CONTROL_INODE_NAME,
};
use libc::{EINVAL, EPERM};
use snafu::ResultExt;
use tracing::{debug, info, trace};

use super::KisekiVFS;
use crate::err::{LibcSnafu, MetaSnafu, Result};

impl KisekiVFS {
    /// Look up a directory entry by name.
    ///
    /// This function searches for a file or directory with the given name
    /// within the specified parent directory. It returns the inode number
    /// and attributes of the found entry. This operation is equivalent to
    /// the lookup operation in FUSE and corresponds to path resolution in
    /// traditional filesystems.
    ///
    /// # Arguments
    /// * `ctx`: `Arc<FuseContext>` - The FUSE context containing request
    ///   metadata such as uid, gid, and process information for permission
    ///   checking
    /// * `parent`: [Ino] - The inode number of the parent directory to search
    ///   in
    /// * `name`: [&str] - The name of the file or directory to look up
    ///
    /// # Returns
    /// * `Result<FullEntry>` - Returns a `FullEntry` containing:
    ///   - `inode`: The inode number of the found entry
    ///   - `name`: The name of the entry (same as input)
    ///   - `attr`: Complete inode attributes including type, permissions,
    ///     timestamps, etc.
    ///
    /// # Errors
    /// This function may return errors if:
    /// - The parent inode does not exist (`ENOENT`)
    /// - The parent is not a directory (`ENOTDIR`)
    /// - The requested name does not exist in the parent directory (`ENOENT`)
    /// - Permission denied to search the parent directory (`EACCES`)
    /// - The name is too long (`ENAMETOOLONG`)
    ///
    /// # Special Cases
    /// - Handles internal nodes (control and config inodes) specially
    /// - Supports "." (current directory) lookups for special inodes
    /// - Internal nodes are synthetic and don't exist in the metadata store
    ///
    /// # Examples
    /// ```ignore
    /// use kiseki_types::ino::ROOT_INO;
    ///
    /// // Look up a file in the root directory
    /// let entry = vfs.lookup(ctx.clone(), ROOT_INO, "myfile.txt").await?;
    /// println!("Found inode: {}, type: {:?}", entry.inode, entry.attr.kind);
    /// ```
    ///
    /// # References
    /// * [FUSE lookup](https://libfuse.github.io/doxygen/structfuse__operations.html#a37c513e31e4fc7a12c2da39b7e2490e8)
    pub async fn lookup(
        &self,
        ctx: Arc<FuseContext>,
        parent: Ino,
        name: &str,
    ) -> Result<FullEntry> {
        trace!("fs:lookup with parent {parent:?} name {name:?}");
        // TODO: handle the special case
        if (parent == ROOT_INO || name.eq(CONTROL_INODE_NAME))
            && let Some(n) = self.internal_nodes.get_internal_node_by_name(name)
        {
            return Ok(n.0.clone());
        }
        if parent.is_special()
            && name == DOT
            && let Some(n) = self.internal_nodes.get_internal_node(parent)
        {
            return Ok(n.0.clone());
        }
        let (inode, attr) = self.meta.lookup(ctx, parent, name, true).await?;
        Ok(FullEntry {
            inode,
            name: name.to_string(),
            attr,
        })
    }

    /// Get inode attributes.
    ///
    /// This function retrieves the complete attributes of an inode, including
    /// file type, permissions, size, timestamps, ownership information, and
    /// other metadata. This corresponds to the stat(2) system call and the
    /// getattr operation in FUSE.
    ///
    /// # Arguments
    /// * `inode`: [Ino] - The inode number to retrieve attributes for
    ///
    /// # Returns
    /// * `Result<InodeAttr>` - Returns inode attributes including:
    ///   - File type (regular file, directory, symlink, etc.)
    ///   - Access permissions (mode)
    ///   - File size in bytes
    ///   - Number of hard links
    ///   - Owner user ID (uid) and group ID (gid)
    ///   - Access, modification, and change timestamps
    ///   - Block count and block size
    ///   - Device numbers for special files
    ///
    /// # Errors
    /// This function may return errors if:
    /// - The inode does not exist (`ENOENT`)
    /// - The inode is corrupted or invalid (`EIO`)
    /// - Permission denied to access inode attributes (`EACCES`)
    /// - The metadata engine fails to retrieve the attributes
    ///
    /// # Special Cases
    /// - For special internal nodes (control and config inodes), returns
    ///   synthetic attributes without querying the metadata engine
    /// - Internal nodes have predefined attributes and behavior
    ///
    /// # Examples
    /// ```ignore
    /// use kiseki_types::ino::ROOT_INO;
    ///
    /// let attr = vfs.get_attr(ROOT_INO).await?;
    /// println!("Root directory size: {} bytes", attr.length);
    /// println!("Permissions: {:o}", attr.mode);
    /// ```
    ///
    /// # References
    /// * [stat(2)](https://man7.org/linux/man-pages/man2/stat.2.html)
    /// * [FUSE getattr](https://libfuse.github.io/doxygen/structfuse__operations.html#a1c4a3c6b982ba57d5d96acf49b1a6c8b)
    pub async fn get_attr(&self, inode: Ino) -> Result<InodeAttr> {
        debug!("vfs:get_attr with inode {inode:?}");
        if inode.is_special()
            && let Some(n) = self.internal_nodes.get_internal_node(inode)
        {
            return Ok(n.get_attr());
        }
        let attr = self.meta.get_attr(inode).await?;
        debug!("vfs:get_attr with inode {inode:?} attr {attr:?}");
        Ok(attr)
    }

    /// Set inode attributes.
    ///
    /// This function modifies various attributes of an inode, such as
    /// permissions, size, timestamps, and ownership. Only the attributes
    /// specified by the flags parameter will be modified. This corresponds
    /// to operations like chmod, chown, truncate, and utime system calls,
    /// and the setattr operation in FUSE.
    ///
    /// # Arguments
    /// * `ctx`: `Arc<FuseContext>` - The FUSE context containing request
    ///   metadata such as uid, gid for permission checking
    /// * `ino`: [Ino] - The inode number to modify attributes for
    /// * `flags`: [u32] - Bitmask specifying which attributes to modify
    ///   (SetAttrFlags)
    /// * `atime`: `Option<TimeOrNow>` - New access time, if ATIME flag is set
    /// * `mtime`: `Option<TimeOrNow>` - New modification time, if MTIME flag is
    ///   set
    /// * `mode`: `Option<u32>` - New file permissions, if MODE flag is set
    /// * `uid`: `Option<u32>` - New owner user ID, if UID flag is set
    /// * `gid`: `Option<u32>` - New owner group ID, if GID flag is set
    /// * `size`: `Option<u64>` - New file size for truncation, if SIZE flag is
    ///   set
    /// * `fh`: `Option<u64>` - Optional file handle for size operations
    ///
    /// # Returns
    /// * `Result<InodeAttr>` - Returns the updated inode attributes after
    ///   modification
    ///
    /// # Errors
    /// This function may return errors if:
    /// - The inode does not exist (`ENOENT`)
    /// - Permission denied for the requested operation (`EPERM`, `EACCES`)
    /// - Invalid parameters (`EINVAL`)
    /// - File too large when setting size (`EFBIG`)
    /// - Cannot truncate due to open file handles (`EACCES`)
    /// - Trying to truncate a directory (`EBADF`)
    ///
    /// # Special Behavior
    /// - For special internal nodes, returns synthetic attributes without
    ///   modification
    /// - Size changes trigger file truncation through the data manager
    /// - Timestamp changes may update file writer metadata
    /// - All file handles must be locked during truncation operations
    /// - Permission checks are performed when ctx.check_permission is true
    ///
    /// # Examples
    /// ```ignore
    /// use fuser::TimeOrNow;
    /// use kiseki_types::attr::SetAttrFlags;
    ///
    /// // Change file permissions
    /// let new_attr = vfs
    ///     .set_attr(
    ///         ctx.clone(),
    ///         inode,
    ///         SetAttrFlags::MODE.bits(),
    ///         None,
    ///         None,
    ///         None,
    ///         Some(0o644), // new permissions
    ///         None,
    ///         None,
    ///         None,
    ///         None,
    ///     )
    ///     .await?;
    ///
    /// // Truncate file to 1024 bytes
    /// let new_attr = vfs
    ///     .set_attr(
    ///         ctx.clone(),
    ///         inode,
    ///         SetAttrFlags::SIZE.bits(),
    ///         None,
    ///         None,
    ///         None,
    ///         None,
    ///         None,
    ///         Some(1024), // new size
    ///         None,
    ///         None,
    ///     )
    ///     .await?;
    /// ```
    ///
    /// # References
    /// * [chmod(2)](https://man7.org/linux/man-pages/man2/chmod.2.html)
    /// * [chown(2)](https://man7.org/linux/man-pages/man2/chown.2.html)
    /// * [truncate(2)](https://man7.org/linux/man-pages/man2/truncate.2.html)
    /// * [utimes(2)](https://man7.org/linux/man-pages/man2/utime.2.html)
    /// * [FUSE setattr](https://libfuse.github.io/doxygen/structfuse__operations.html#a603adb23c9e46b4b8c1b956cc0de99c9)
    #[allow(clippy::too_many_arguments)]
    pub async fn set_attr(
        &self,
        ctx: Arc<FuseContext>,
        ino: Ino,
        flags: u32,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        fh: Option<u64>,
        file_flags: Option<u32>,
    ) -> Result<InodeAttr> {
        info!(
            "fs:setattr with ino {:?} flags {:?} atime {:?} mtime {:?}",
            ino, flags, atime, mtime
        );

        if ino.is_special() {
            return if let Some(n) = self.internal_nodes.get_internal_node(ino) {
                Ok(n.get_attr())
            } else {
                return LibcSnafu { errno: EPERM }.fail()?;
            };
        }

        let mut new_attr = InodeAttr::default();
        let Some(flags) = SetAttrFlags::from_bits(flags) else {
            return LibcSnafu { errno: EINVAL }.fail();
        };
        if flags.contains(SetAttrFlags::SIZE) {
            if let Some(size) = size {
                new_attr = self.truncate(ctx.clone(), ino, size, fh).await?;
            } else {
                return LibcSnafu { errno: EINVAL }.fail()?;
            }
        }
        if flags.contains(SetAttrFlags::MODE) {
            if let Some(mode) = mode {
                new_attr.mode = mode & 0o7777;
            } else {
                return LibcSnafu { errno: EINVAL }.fail()?;
            }
        }
        if flags.contains(SetAttrFlags::UID) {
            if let Some(uid) = uid {
                new_attr.uid = uid;
            } else {
                return LibcSnafu { errno: EINVAL }.fail()?;
            }
        }
        if flags.contains(SetAttrFlags::GID) {
            if let Some(gid) = gid {
                new_attr.gid = gid;
            } else {
                return LibcSnafu { errno: EINVAL }.fail()?;
            }
        }
        if flags.contains(SetAttrFlags::FLAG) {
            if let Some(file_flags) = file_flags {
                new_attr.flags = file_flags;
            } else {
                return LibcSnafu { errno: EINVAL }.fail()?;
            }
        }
        let mut needs_time_permission_check = false;
        if flags.contains(SetAttrFlags::ATIME) {
            if let Some(atime) = atime {
                new_attr.atime = match atime {
                    TimeOrNow::SpecificTime(st) => st,
                    TimeOrNow::Now => SystemTime::now(),
                };
                needs_time_permission_check = true;
            } else {
                return LibcSnafu { errno: EINVAL }.fail()?;
            }
        }
        if flags.contains(SetAttrFlags::ATIME_NOW) {
            new_attr.atime = SystemTime::now();
            needs_time_permission_check = true;
        }
        let mut data_mtime = None;
        if flags.contains(SetAttrFlags::MTIME) {
            if let Some(mtime) = mtime {
                new_attr.mtime = match mtime {
                    TimeOrNow::SpecificTime(st) => st,
                    TimeOrNow::Now => SystemTime::now(),
                };
                data_mtime = Some(new_attr.mtime);
                needs_time_permission_check = true;
            } else {
                return LibcSnafu { errno: EINVAL }.fail()?;
            }
        }
        if flags.contains(SetAttrFlags::MTIME_NOW) {
            let now = SystemTime::now();
            new_attr.mtime = now;
            data_mtime = Some(now);
            needs_time_permission_check = true;
        }
        if needs_time_permission_check && ctx.check_permission {
            self.meta
                .check_set_attr(&ctx, ino, flags, &mut new_attr)
                .await
                .context(MetaSnafu)?;
        }
        if let Some(mtime) = data_mtime {
            self.data_manager.update_mtime(ino, mtime)?;
        }

        self.meta
            .set_attr(&ctx, flags, ino, &mut new_attr)
            .await
            .context(MetaSnafu)?;

        self.try_update_file_reader_length(ino, &mut new_attr).await;

        // TODO: invalid open_file cache

        Ok(new_attr)
    }
}
