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

use std::{path::Path, sync::Arc};

use bytes::Bytes;
use kiseki_common::{DOT, DOT_DOT, FH, MAX_NAME_LENGTH, MAX_SYMLINK_LEN, MODE_MASK_R, MODE_MASK_W};
use kiseki_meta::context::FuseContext;
use kiseki_types::{
    entry::{Entry, FullEntry},
    ino::Ino,
};
use libc::{EBADF, EINVAL, mode_t};
use snafu::{OptionExt, ResultExt, ensure};
use tokio::time::Instant;
use tracing::{debug, trace};

use super::{KisekiVFS, get_file_type};
use crate::err::{LibcSnafu, MetaSnafu, Result};

// Dir
impl KisekiVFS {
    pub async fn open_dir<I: Into<Ino>>(
        &self,
        ctx: &FuseContext,
        inode: I,
        flags: i32,
    ) -> Result<FH> {
        let inode = inode.into();
        trace!("vfs:open_dir with {inode:?}, flags: {flags:o}");

        // 检查inode是否为目录类型
        let attr = self.meta.get_attr(inode).await?;
        if !attr.is_dir() {
            return LibcSnafu {
                errno: libc::ENOTDIR,
            }
            .fail();
        }

        if ctx.check_permission {
            let mmask =
                match flags as libc::c_int & (libc::O_RDONLY | libc::O_WRONLY | libc::O_RDWR) {
                    libc::O_RDONLY => MODE_MASK_R,
                    libc::O_WRONLY => MODE_MASK_W,
                    libc::O_RDWR => MODE_MASK_R | MODE_MASK_W,
                    _ => 0, // do nothing, // Handle unexpected flags
                };
            ctx.check_access(&attr, mmask)?;
        }
        Ok(self.handle_table.new_dir_handle(inode).await)
    }

    // The directory handle's write lock is deliberately held across
    // `meta.read_dir` to atomically populate its cached children; releasing it
    // earlier would reintroduce the TOCTOU the double-checked lock avoids.
    #[allow(clippy::significant_drop_tightening)]
    pub async fn read_dir<I: Into<Ino>>(
        &self,
        ctx: &FuseContext,
        inode: I,
        fh: u64,
        offset: i64,
        plus: bool,
    ) -> Result<Vec<Entry>> {
        let inode = inode.into();
        debug!(
            "fs:readdir with ino {:?} fh {:?} offset {:?}",
            inode, fh, offset
        );

        let h = self
            .handle_table
            .find_handle(inode, fh)
            .await
            .context(LibcSnafu { errno: EBADF })?;
        let h = h.as_dir_handle().context(LibcSnafu { errno: EBADF })?;

        let read_guard = h.inner.read().await;
        if read_guard.children.is_empty() || offset == 0 {
            drop(read_guard);
            let mut write_guard = h.inner.write().await;
            // FIXME
            write_guard.read_at = Some(Instant::now());
            write_guard.children = self
                .meta
                .read_dir(ctx, inode, plus)
                .await
                .context(MetaSnafu)?;
            if (offset as usize) < write_guard.children.len() {
                let result = write_guard.children[offset as usize..].to_vec();
                return Ok(result);
            } else {
                return Ok(vec![]);
            }
        }
        if (offset as usize) < read_guard.children.len() {
            let result = read_guard.children[offset as usize..].to_vec();
            return Ok(result);
        }
        Ok(Vec::new())
    }

    /// Create a new directory.
    ///
    /// This function creates a new directory with the specified name in the
    /// given parent directory. It corresponds to the mkdir(2) system call
    /// and the mkdir operation in FUSE.
    ///
    /// # Arguments
    /// * `ctx`: `Arc<FuseContext>` - The FUSE context containing request
    ///   metadata
    /// * `parent`: [Ino] - The inode number of the parent directory
    /// * `name`: [&str] - The name of the new directory to create
    /// * `mode`: [u32] - The file mode (permissions) for the new directory
    /// * `umask`: [u32] - The umask to apply when setting permissions
    ///
    /// # Returns
    /// * `Result<FullEntry>` - Returns a `FullEntry` containing the new
    ///   directory's inode number, name, and attributes
    ///
    /// # Errors
    /// This function may return errors if:
    /// - A file or directory with the same name already exists (`EEXIST`)
    /// - The parent directory does not exist (`ENOENT`)
    /// - Permission denied to create in parent directory (`EACCES`)
    /// - The name is too long (`ENAMETOOLONG`)
    /// - The parent is not a directory (`ENOTDIR`)
    /// - No space left on device (`ENOSPC`)
    ///
    /// # Special Cases
    /// - Cannot create directories with internal node names in root directory
    /// - Name length is limited by MAX_NAME_LENGTH
    /// - Permissions are modified by umask before setting
    ///
    /// # Examples
    /// ```ignore
    /// use kiseki_types::ino::ROOT_INO;
    ///
    /// let new_dir = vfs
    ///     .mkdir(ctx.clone(), ROOT_INO, "my_directory", 0o755, 0)
    ///     .await?;
    /// println!("Created directory with inode: {}", new_dir.inode);
    /// ```
    ///
    /// # References
    /// * [mkdir(2)](https://man7.org/linux/man-pages/man2/mkdir.2.html)
    /// * [FUSE mkdir](https://libfuse.github.io/doxygen/structfuse__operations.html#a13a08af5ce7b5c4a5b1a5ad9ea1c4d8f)
    pub async fn mkdir(
        &self,
        ctx: Arc<FuseContext>,
        parent: Ino,
        name: &str,
        mode: u32,
        umask: u32,
    ) -> Result<FullEntry> {
        debug!("fs:mkdir with parent {parent:?} name {name:?}");

        // 验证文件名是否有效（拒绝空字符串、"." 和 ".."）
        Self::validate_filename(name)?;

        if parent.is_root() && self.internal_nodes.contains_name(name) {
            return LibcSnafu {
                errno: libc::EEXIST,
            }
            .fail()?;
        }
        if name.len() > MAX_NAME_LENGTH {
            return LibcSnafu {
                errno: libc::ENAMETOOLONG,
            }
            .fail()?;
        };

        let (ino, attr) = self
            .meta
            .mkdir(ctx, parent, name, mode, umask)
            .await
            .context(MetaSnafu)?;
        Ok(FullEntry::new(ino, name, attr))
    }

    pub async fn rmdir(&self, ctx: Arc<FuseContext>, parent: Ino, name: &str) -> Result<()> {
        debug!("fs:rmdir with parent {parent:?} name {name:?}");
        ensure!(name != DOT, LibcSnafu { errno: EINVAL });
        ensure!(name != DOT_DOT, LibcSnafu { errno: EINVAL });
        ensure!(
            name.len() < MAX_NAME_LENGTH,
            LibcSnafu {
                errno: libc::ENAMETOOLONG,
            }
        );
        self.meta
            .rmdir(ctx, parent, name)
            .await
            .context(MetaSnafu)?;

        Ok(())
    }

    pub async fn release_dir(&self, inode: Ino, fh: FH) -> Result<()> {
        if let Some(dh) = self.handle_table.find_handle(inode, fh).await {
            let _ = dh.as_dir_handle().context(LibcSnafu { errno: EBADF })?;
            self.handle_table.release_file_handle(inode, fh).await;
        }
        Ok(())
    }
}

// File
impl KisekiVFS {
    pub async fn mknod(
        &self,
        ctx: Arc<FuseContext>,
        parent: Ino,
        name: String,
        mode: u32,
        umask: u32,
        rdev: u32,
    ) -> Result<FullEntry> {
        if parent.is_root() && self.internal_nodes.contains_name(&name) {
            return LibcSnafu {
                errno: libc::EEXIST,
            }
            .fail()?;
        }
        if name.len() > MAX_NAME_LENGTH {
            return LibcSnafu {
                errno: libc::ENAMETOOLONG,
            }
            .fail()?;
        }
        let file_type = get_file_type(mode as mode_t)?;

        let (ino, attr) = self
            .meta
            .mknod(
                ctx,
                parent,
                &name,
                file_type,
                mode & 0o7777,
                umask,
                rdev,
                String::new(),
            )
            .await
            .context(MetaSnafu)?;
        Ok(FullEntry::new(ino, &name, attr))
    }

    /// Create and open a new file.
    ///
    /// This function creates a new regular file with the specified name in the
    /// given parent directory and immediately opens it for I/O operations.
    /// This is an atomic operation that combines file creation and opening,
    /// corresponding to the open(2) system call with O_CREAT flag and the
    /// create operation in FUSE.
    ///
    /// # Arguments
    /// * `ctx`: `Arc<FuseContext>` - The FUSE context containing request
    ///   metadata
    /// * `parent`: [Ino] - The inode number of the parent directory
    /// * `name`: [&str] - The name of the new file to create
    /// * `mode`: [u32] - The file mode (permissions) for the new file
    /// * `umask`: [u32] - The umask to apply when setting permissions
    /// * `flags`: [libc::c_int] - Open flags for the created file (O_RDWR,
    ///   O_WRONLY, etc.)
    ///
    /// # Returns
    /// * [Result<(FullEntry, FH)>] - Returns a tuple containing:
    ///   - `FullEntry`: The newly created file's inode, name, and attributes
    ///   - `FH`: File handle for immediate I/O operations
    ///
    /// # Errors
    /// This function may return errors if:
    /// - A file or directory with the same name already exists (`EEXIST`)
    /// - The parent directory does not exist (`ENOENT`)
    /// - Permission denied to create in parent directory (`EACCES`)
    /// - The name is too long (`ENAMETOOLONG`)
    /// - The parent is not a directory (`ENOTDIR`)
    /// - No space left on device (`ENOSPC`)
    /// - Too many files are open (`EMFILE`, `ENFILE`)
    ///
    /// # Special Cases
    /// - Cannot create files with internal node names in root directory
    /// - Name length is limited by MAX_NAME_LENGTH
    /// - File is immediately opened with the specified flags
    /// - Updates file reader length before returning the handle
    ///
    /// # Examples
    /// ```ignore
    /// use kiseki_types::ino::ROOT_INO;
    /// use libc::O_RDWR;
    ///
    /// let (entry, fh) = vfs
    ///     .create(ctx.clone(), ROOT_INO, "newfile.txt", 0o644, 0, O_RDWR)
    ///     .await?;
    /// println!("Created file {} with handle {}", entry.name, fh);
    /// ```
    ///
    /// # References
    /// * [open(2)](https://man7.org/linux/man-pages/man2/open.2.html)
    /// * [FUSE create](https://libfuse.github.io/doxygen/structfuse__operations.html#a13a08af5ce7b5c4a5b1a5ad9ea1c4d8f)
    pub async fn create(
        &self,
        ctx: Arc<FuseContext>,
        parent: Ino,
        name: &str,
        mode: u32,
        umask: u32,
        flags: libc::c_int,
    ) -> Result<(FullEntry, FH)> {
        debug!("fs:create with parent {parent:?} name {name:?}");

        // 验证文件名是否有效（拒绝空字符串、"." 和 ".."）
        Self::validate_filename(name)?;

        if parent.is_root() && self.internal_nodes.contains_name(name) {
            return LibcSnafu {
                errno: libc::EEXIST,
            }
            .fail()?;
        }
        if name.len() > MAX_NAME_LENGTH {
            return LibcSnafu {
                errno: libc::ENAMETOOLONG,
            }
            .fail()?;
        };

        let (inode, attr) = self
            .meta
            .create(ctx, parent, name, mode & 0o7777, umask, flags)
            .await
            .context(MetaSnafu)?;

        let mut e = FullEntry::new(inode, name, attr);
        self.try_update_file_reader_length(inode, &mut e.attr).await;
        let fh = self
            .handle_table
            .new_file_handle(inode, e.attr.length, flags)
            .await?;
        Ok((e, fh))
    }
}

// Link
impl KisekiVFS {
    pub async fn link(
        &self,
        ctx: Arc<FuseContext>,
        inode: Ino,
        new_parent: Ino,
        new_name: &str,
    ) -> Result<FullEntry> {
        ensure!(
            new_name.len() < MAX_NAME_LENGTH,
            LibcSnafu {
                errno: libc::ENAMETOOLONG,
            }
        );
        ensure!(
            !new_name.is_empty(),
            LibcSnafu {
                errno: libc::ENOENT,
            }
        );

        let attr = self.meta.link(ctx, inode, new_parent, new_name).await?;
        let e = FullEntry::new(inode, new_name, attr);
        Ok(e)
    }

    /// [KisekiVFS::unlink] deletes a name and possibly the file it refers to.
    ///
    /// Deletes a name from the filesystem. <br>
    ///
    /// If that name was the last link to a file and no processes have the<br>
    /// file open, the file is deleted and the space it was using is<br>
    /// made available for reuse.
    ///
    /// # Arguments
    /// * `ctx`: [FuseContext] - the fuse context, including some additional
    ///   information like uid, gid, etc.
    ///
    /// # Examples
    ///
    /// # References
    /// * [unlink(2)](https://man7.org/linux/man-pages/man2/unlink.2.html)
    pub async fn unlink(&self, ctx: Arc<FuseContext>, parent: Ino, name: &str) -> Result<()> {
        ensure!(
            name.len() < MAX_NAME_LENGTH,
            LibcSnafu {
                errno: libc::ENAMETOOLONG,
            }
        );
        ensure!(
            !name.is_empty(),
            LibcSnafu {
                errno: libc::ENOENT,
            }
        );

        self.meta.unlink(ctx, parent, name).await?;
        Ok(())
    }

    /// [KisekiVFS::symlink] makes a new name for a file.
    ///
    /// Symbolic links are interpreted at run time as if the contents of
    /// the link had been substituted into the path being followed to
    /// find a file or directory.
    ///
    /// A symbolic link (also known as a soft link) may point to an existing
    /// file or to a nonexistent one; the latter case is known as a dangling
    /// link.
    ///
    /// # Arguments
    /// * `ctx`: [FuseContext] - the fuse context, including some additional
    ///   information like uid, gid, etc.
    /// * `parent`: [Ino] - the inode number of the parent directory where the
    ///   symbolic link will be created.
    /// * `link_name`: [&str] - the name of the symbolic link.
    /// * `target`: `&Path` - the path to the file or directory that the
    ///   symbolic link
    ///
    /// # Returns
    /// * [FullEntry] - the inode number and the attribute of the symbolic link.
    ///
    /// # Examples
    /// For example, let's say you have a file named **foo.txt** in the current
    /// directory, and you want to create a symbolic link named **foo_link**
    /// that points to **foo.txt**. You would use the following command:
    /// ```shell
    /// ln -s foo.txt foo_link
    /// ```
    ///
    /// # References
    /// * [symlink(2)](https://man7.org/linux/man-pages/man2/symlink.2.html)
    pub async fn symlink(
        &self,
        ctx: Arc<FuseContext>,
        parent: Ino,
        link_name: &str,
        target: &Path,
    ) -> Result<FullEntry> {
        ensure!(
            link_name.len() < MAX_NAME_LENGTH,
            LibcSnafu {
                errno: libc::ENAMETOOLONG,
            }
        );
        ensure!(
            target.to_string_lossy().len() < MAX_SYMLINK_LEN,
            LibcSnafu { errno: EINVAL }
        );

        let (inode, attr) = self.meta.symlink(ctx, parent, link_name, target).await?;
        Ok(FullEntry::new(inode, link_name, attr))
    }

    /// [KisekiVFS::readlink] reads value of a symbolic link.
    ///
    /// # Arguments
    /// * `ctx`: [FuseContext] - the fuse context, including some additional
    ///   information like uid, gid, etc.
    /// * `inode`: [Ino] - inode number of the symbolic link for which you want
    ///   to read the target.
    ///
    /// # Returns
    /// * `TargetPath`: [Bytes] - the target of the symbolic link.
    ///
    /// # References
    /// * [readlink(2)](https://man7.org/linux/man-pages/man2/readlink.2.html)
    pub async fn readlink(&self, ctx: Arc<FuseContext>, inode: Ino) -> Result<Bytes> {
        let target = self.meta.readlink(ctx, inode).await?;
        Ok(target)
    }
}

// Rename
impl KisekiVFS {
    /// Rename or move a file or directory.
    ///
    /// This function renames a file or directory from one name to another,
    /// potentially moving it between directories. This corresponds to the
    /// rename(2) system call and the rename operation in FUSE.
    ///
    /// # Arguments
    /// * `ctx`: `Arc<FuseContext>` - The FUSE context containing request
    ///   metadata
    /// * `parent`: [Ino] - The inode number of the current parent directory
    /// * `name`: [&str] - The current name of the file or directory
    /// * `new_parent`: [Ino] - The inode number of the target parent directory
    /// * `new_name`: [&str] - The new name for the file or directory
    /// * `flags`: [u32] - Rename flags (RENAME_EXCHANGE, RENAME_NOREPLACE,
    ///   etc.)
    ///
    /// # Returns
    /// * [Result<()>] - Returns `Ok(())` on successful rename
    ///
    /// # Errors
    /// This function may return errors if:
    /// - Either the old or new name is empty (`ENOENT`)
    /// - Either name is too long (`ENAMETOOLONG`)
    /// - The source file or directory does not exist (`ENOENT`)
    /// - Permission denied for the operation (`EACCES`)
    /// - The target already exists and cannot be replaced (`EEXIST`)
    /// - The source and target are on different filesystems (`EXDEV`)
    /// - The operation would create a directory loop (`EINVAL`)
    /// - No space left on device (`ENOSPC`)
    ///
    /// # Behavior
    /// - Can move files between directories within the same filesystem
    /// - May replace existing files depending on flags and file types
    /// - Atomic operation - either succeeds completely or fails without changes
    /// - Updates directory modification times
    /// - Handles both file and directory renaming
    ///
    /// # Examples
    /// ```ignore
    /// use kiseki_types::ino::ROOT_INO;
    ///
    /// // Rename a file in the same directory
    /// vfs.rename(
    ///     ctx.clone(),
    ///     ROOT_INO,
    ///     "oldname.txt",
    ///     ROOT_INO,
    ///     "newname.txt",
    ///     0,
    /// )
    /// .await?;
    ///
    /// // Move a file to a different directory
    /// vfs.rename(ctx.clone(), src_dir, "file.txt", dst_dir, "file.txt", 0)
    ///     .await?;
    /// ```
    ///
    /// # References
    /// * [rename(2)](https://man7.org/linux/man-pages/man2/rename.2.html)
    /// * [FUSE rename](https://libfuse.github.io/doxygen/structfuse__operations.html#a13a08af5ce7b5c4a5b1a5ad9ea1c4d8f)
    pub async fn rename(
        &self,
        ctx: Arc<FuseContext>,
        parent: Ino,
        name: &str,
        new_parent: Ino,
        new_name: &str,
        flags: u32,
    ) -> Result<()> {
        ensure!(
            !name.is_empty() && !new_name.is_empty(),
            LibcSnafu {
                errno: libc::ENOENT,
            }
        );
        ensure!(
            name.len() < MAX_NAME_LENGTH && new_name.len() < MAX_NAME_LENGTH,
            LibcSnafu {
                errno: libc::ENAMETOOLONG,
            }
        );

        self.meta
            .rename(ctx, parent, name, new_parent, new_name, flags)
            .await
            .context(MetaSnafu)?;
        Ok(())
    }
}
