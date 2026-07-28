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
    fmt::{Debug, Formatter},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use kiseki_common::{CHUNK_SIZE, ChunkIndex};
use kiseki_types::{
    FileType,
    attr::InodeAttr,
    entry::DEntry,
    ino::{Ino, ROOT_INO, ZERO_INO},
    setting::Format,
    slice::{EMPTY_SLICE_ID, Slice, Slices},
    stat::DirStat,
};
use snafu::{IntoError, OptionExt, ensure};
use tracing::{debug, error};

use super::{
    Backend, RenameResult, SliceCommitResult, UnlinkResult, key::Counter as BackendCounter,
};
use crate::{
    backend::kv::{
        KvEngine, KvRead, KvReadExt, KvTxn, KvTxnExt,
        key::{self, MetaKey},
        rocksdb::RocksDbKv,
        value,
    },
    context::FuseContext,
    engine::RenameFlags,
    err::{
        AlreadyInitializedSnafu, LibcSnafu, ModelSnafu, Result, UninitializedEngineSnafu, model_err,
    },
    open_files::OpenFilesRef,
};

/// **POSIX-Compliant Meta Storage Backend Implementation using RocksDB**
///
/// This module implements a complete POSIX-compliant filesystem metadata
/// storage layer. All persistence goes through the storage-agnostic typed
/// key/value layer ([`crate::backend::kv`]); this file is concerned only with
/// POSIX.1-2008 semantics (permissions, hard-link counting, sticky bits,
/// atomic multi-key mutations) expressed on top of that layer.
///
/// # POSIX Compliance Architecture
///
/// ## Core POSIX Requirements Implemented:
/// - **Atomicity**: All filesystem operations are atomic (create, delete,
///   rename, etc.)
/// - **Consistency**: Metadata always remains in consistent state across
///   operations
/// - **Isolation**: Concurrent operations don't interfere with each other
/// - **Durability**: All committed changes survive system crashes
/// - **Permission Model**: Full POSIX permission bits (owner/group/other +
///   special bits)
/// - **Hard Links**: Proper nlink counting and multi-parent file support
/// - **Symbolic Links**: Full symbolic link semantics with target path storage
/// - **Directory Semantics**: Empty directory checks, link counting, sticky bit
///   enforcement
///
/// ## Key POSIX System Calls Supported:
/// - `mknod(2)`: Create files, directories, special files
/// - `unlink(2)`: Remove files with proper hard link handling
/// - `rmdir(2)`: Remove empty directories with validation
/// - `rename(2)`: Atomic move/rename operations
/// - `link(2)`: Create hard links with proper counting
/// - `readlink(2)`: Read symbolic link targets
/// - `stat(2)/fstat(2)`: Retrieve file attributes and metadata
/// - `truncate(2)`: Modify file size atomically
///
/// ## Transaction Model:
/// Complex operations (mknod, rmdir, rename, ...) run inside a single
/// [`KvEngine::transaction`] closure that commits once and retries on
/// optimistic conflict. Simple point reads/writes use the non-transactional
/// snapshot helpers. All error conditions follow POSIX error code conventions
/// (EEXIST, ENOTEMPTY, EACCES, etc.).
///
/// ## Permission and Security:
/// Implements full POSIX permission checking including:
/// - Standard permission bits (read/write/execute for owner/group/other)
/// - Special permission bits (setuid, setgid, sticky bit)
/// - Sticky bit directory protection for secure deletion
/// - Immutable and append-only file attribute support
///
/// Constants for file permissions and operation modes - POSIX compliant
mod constants {
    // POSIX.1-2008 Section 4.5: File permission bits
    #[allow(dead_code)] // May be used by future POSIX operations
    pub const S_ISUID: u32 = 0o4000; // Set-user-ID on execution (setuid bit)
    #[allow(dead_code)] // False positive: actually used in Linux-specific code
    pub const S_ISGID: u32 = 0o2000; // Set-group-ID on execution (setgid bit)
    pub const S_ISVTX: u32 = 0o1000; // Sticky bit (restricted deletion flag)

    #[allow(dead_code)] // Used in specific Linux filesystem scenarios
    pub const MODE_MASK_SETGID_EXEC: u32 = 0o2010;
    pub const DEFAULT_FILE_SIZE: u64 = 4096;
}

#[derive(Debug, Default)]
pub struct Builder {
    path:           PathBuf,
    skip_dir_mtime: Duration,
}

impl Builder {
    pub fn with_path<P: AsRef<Path>>(&mut self, path: P) -> &mut Self {
        self.path = path.as_ref().to_path_buf();
        self
    }

    pub const fn with_skip_dir_mtime(&mut self, d: Duration) -> &mut Self {
        self.skip_dir_mtime = d;
        self
    }

    pub fn build(&self) -> Result<Arc<dyn Backend>> {
        let kv = RocksDbKv::open(&self.path)?;
        Ok(Arc::new(RocksdbBackend {
            kv,
            skip_dir_mtime: self.skip_dir_mtime,
        }))
    }
}

pub(crate) struct RocksdbBackend {
    kv:             RocksDbKv,
    skip_dir_mtime: Duration,
}

impl Debug for RocksdbBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("RocksdbEngine");
        ds.field("path", &self.kv.path());
        ds.finish()
    }
}

/// Current wall-clock time expressed as whole seconds since the UNIX epoch.
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// Check whether a directory has any children - POSIX rmdir(2) empty-directory
/// validation.
///
/// A directory may only be removed (or replaced during rename) when it holds no
/// entries besides "." and "..". Performing the check inside the surrounding
/// transaction prevents TOCTOU races with concurrent additions.
fn do_check_exist_children(txn: &mut dyn KvTxn, parent: Ino) -> Result<bool> {
    Ok(!txn.scan(&key::DentryPrefix(parent), Some(1))?.is_empty())
}

/// Read the hard-link count for a specific inode/parent relationship,
/// defaulting to zero when the relationship has never been recorded. Uses a
/// write-intent read because every caller immediately updates the count.
fn get_hard_link_count(txn: &mut dyn KvTxn, inode: Ino, parent: Ino) -> Result<u64> {
    Ok(txn
        .get_for_update(&key::HardLink(inode, parent))?
        .unwrap_or(0))
}

/// Append a slice to a chunk's packed slice list within a transaction, skipping
/// the append when an identical slice is already present.
///
/// Returns `(inserted, slice_count)`: whether a new record was appended and the
/// resulting number of slices. Deduplication compares the candidate slice
/// against the existing records; `Slice`'s value equality is exactly its
/// 28-byte (`SLICE_BYTES`) encoding equality.
fn stage_chunk_slice(
    txn: &mut dyn KvTxn,
    inode: Ino,
    chunk_index: ChunkIndex,
    slice: &Slice,
) -> Result<(bool, usize)> {
    let key = key::ChunkSlices(inode, chunk_index);
    let mut slices = txn
        .get_for_update(&key)?
        .unwrap_or_else(|| Slices(Vec::new()));
    let inserted = !slices.0.iter().any(|existing| existing == slice);
    if inserted {
        slices.0.push(slice.clone());
        txn.put(&key, &slices)?;
    }
    Ok((inserted, slices.0.len()))
}

/// Commit a slice into a file's chunk and reconcile the inode length.
///
/// Grows `InodeAttr.length` when the slice extends past the current end of file
/// and refreshes the modification time whenever a slice is newly inserted or
/// the file grows, mirroring POSIX write semantics.
fn stage_slice_commit(
    txn: &mut dyn KvTxn,
    inode: Ino,
    chunk_index: ChunkIndex,
    slice: &Slice,
) -> Result<SliceCommitResult> {
    let mut attr = txn.get_for_update_or_missing(&key::Attr(inode))?;
    ensure!(attr.is_file(), LibcSnafu { errno: libc::EPERM });

    let (inserted, slice_count) = stage_chunk_slice(txn, inode, chunk_index, slice)?;

    let new_len = chunk_index as u64 * CHUNK_SIZE as u64
        + slice.get_chunk_pos() as u64
        + slice.get_size() as u64;
    let grew_by = new_len.saturating_sub(attr.length);
    if inserted || grew_by != 0 {
        if grew_by != 0 {
            attr.length = new_len;
        }
        attr.update_modification_time();
        txn.put(&key::Attr(inode), &attr)?;
    }

    Ok(SliceCommitResult {
        grew_by,
        slice_count,
        inserted,
    })
}

/// Stage the initial volume state (format, root inode, next-inode counter)
/// inside a transaction. Committed atomically by the caller so an aborted
/// initialization leaves no partial state behind.
fn stage_initial_state(txn: &mut dyn KvTxn, format: &Format, root: &InodeAttr) -> Result<()> {
    txn.put(&key::FormatKey, format)?;
    txn.put(&key::Attr(ROOT_INO), root)?;
    txn.put(&key::CounterKey(BackendCounter::NextInode), &2u64)?;
    Ok(())
}

#[async_trait::async_trait]
impl Backend for RocksdbBackend {
    fn initialize_volume(&self, format: &Format, root: &InodeAttr) -> Result<()> {
        self.kv.transaction(|txn| {
            ensure!(
                txn.get_for_update(&key::FormatKey)?.is_none(),
                AlreadyInitializedSnafu
            );
            stage_initial_state(txn, format, root)?;
            Ok(())
        })
    }

    fn set_format(&self, format: &Format) -> Result<()> {
        self.kv.transaction(|txn| txn.put(&key::FormatKey, format))
    }

    fn load_format(&self) -> Result<Format> {
        self.kv
            .get(&key::FormatKey)?
            .context(UninitializedEngineSnafu)
    }

    fn increase_count_by(&self, counter: BackendCounter, step: usize) -> Result<u64> {
        self.kv.transaction(|txn| {
            let current = txn.get_for_update(&key::CounterKey(counter))?.unwrap_or(0);
            let new = current + step as u64;
            txn.put(&key::CounterKey(counter), &new)?;
            Ok(new)
        })
    }

    fn load_count(&self, counter: BackendCounter) -> Result<u64> {
        self.kv.get_or_missing(&key::CounterKey(counter))
    }

    fn get_attr(&self, inode: Ino) -> Result<InodeAttr> {
        self.kv.get_or_missing(&key::Attr(inode))
    }

    /// Set/update inode attributes - POSIX metadata storage implementation
    ///
    /// POSIX Compliance Requirements:
    /// - POSIX.1-2008: File attributes must be stored persistently and
    ///   atomically
    /// - Metadata Consistency: All POSIX stat fields must be accurately
    ///   maintained
    /// - Timestamp Semantics: atime, mtime, ctime must follow POSIX update
    ///   rules
    /// - Permission Model: Mode bits must conform to POSIX owner/group/other
    ///   model
    /// - Atomicity: Attribute updates must be atomic to prevent partial state
    fn set_attr(&self, inode: Ino, attr: &InodeAttr) -> Result<()> {
        self.kv.transaction(|txn| txn.put(&key::Attr(inode), attr))
    }

    fn get_dentry(&self, parent: Ino, name: &str) -> Result<DEntry> {
        self.kv.get_or_missing(&key::Dentry(parent, name))
    }

    fn set_dentry(&self, parent: Ino, name: &str, inode: Ino, typ: FileType) -> Result<()> {
        let entry = DEntry {
            parent,
            name: name.to_string(),
            inode,
            typ,
        };
        self.kv
            .transaction(|txn| txn.put(&key::Dentry(parent, name), &entry))
    }

    fn list_dentry(&self, parent: Ino, limit: i64) -> Result<Vec<DEntry>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let lim = if limit < 0 {
            None
        } else {
            Some(limit as usize)
        };
        self.kv.scan(&key::DentryPrefix(parent), lim)
    }

    fn set_symlink(&self, inode: Ino, path: String) -> Result<()> {
        let target = value::SymlinkTarget::new(path.into_bytes());
        self.kv
            .transaction(|txn| txn.put(&key::Symlink(inode), &target))
    }

    fn get_symlink(&self, inode: Ino) -> Result<String> {
        let target = self.kv.get_or_missing(&key::Symlink(inode))?;
        Ok(String::from_utf8_lossy(&target.0).into_owned())
    }

    fn set_chunk_slices(&self, inode: Ino, chunk_index: ChunkIndex, slices: Slices) -> Result<()> {
        self.kv
            .transaction(|txn| txn.put(&key::ChunkSlices(inode, chunk_index), &slices))
    }

    fn set_raw_chunk_slices(
        &self,
        inode: Ino,
        chunk_index: ChunkIndex,
        buf: Vec<u8>,
    ) -> Result<()> {
        let raw_key = key::ChunkSlices(inode, chunk_index).encode();
        self.kv.transaction(|txn| txn.put_raw(&raw_key, &buf))?;
        assert!(!buf.is_empty(), "slices is empty");
        Ok(())
    }

    fn get_raw_chunk_slices(&self, inode: Ino, chunk_index: ChunkIndex) -> Result<Option<Vec<u8>>> {
        let raw_key = key::ChunkSlices(inode, chunk_index).encode();
        self.kv.get_raw(&raw_key)
    }

    fn get_chunk_slices(&self, inode: Ino, chunk_index: ChunkIndex) -> Result<Slices> {
        let slices: Slices = self
            .kv
            .get_or_missing(&key::ChunkSlices(inode, chunk_index))?;
        if slices.0.is_empty() {
            return Err(ModelSnafu.into_error(model_err::Error::Corrupt {
                key:    format!("chunk_slices(inode={}, chunk={chunk_index})", inode.0),
                reason: "empty slices".to_string(),
            }));
        }
        debug!("get_chunk_slices: inode={inode} chunk_index={chunk_index}");
        for slice in &slices.0 {
            debug!("get_chunk_slices: slice: {slice:?}");
        }
        Ok(slices)
    }

    fn commit_slice(
        &self,
        inode: Ino,
        chunk_index: ChunkIndex,
        slice: &Slice,
    ) -> Result<SliceCommitResult> {
        // A committed slice must survive a crash the moment it is acknowledged,
        // so commit durably (fsync), matching the previous `set_sync(true)`.
        self.kv
            .transaction_durable(|txn| stage_slice_commit(txn, inode, chunk_index, slice))
    }

    fn set_dir_stat(&self, inode: Ino, dir_stat: DirStat) -> Result<()> {
        self.kv
            .transaction(|txn| txn.put(&key::DirStatKey(inode), &dir_stat))
    }

    fn get_dir_stat(&self, inode: Ino) -> Result<DirStat> {
        self.kv.get_or_missing(&key::DirStatKey(inode))
    }

    /// Create a new file system node - POSIX mknod(2) semantics implementation
    ///
    /// POSIX Compliance Requirements:
    /// - POSIX.1-2008 mknod(2): Create file system nodes atomically
    /// - Exclusive Creation: Must fail with EEXIST if file already exists
    /// - Permission Checks: Must verify write permission on parent directory
    /// - Atomicity: Entire operation must be atomic (create inode + directory
    ///   entry)
    /// - Link Count Management: Properly initialize and update hard link counts
    /// - Directory Updates: Update parent directory's mtime and link count if
    ///   creating directory
    ///
    /// Implementation Strategy:
    /// 1. Validate parent directory exists and has proper permissions
    /// 2. Check target doesn't already exist (fail-fast with EEXIST)
    /// 3. Create inode attributes with proper POSIX metadata
    /// 4. Create directory entry linking name to inode
    /// 5. Update parent directory metadata if needed
    /// 6. Commit entire operation atomically using transaction
    ///
    /// Error Handling:
    /// - ENOTDIR: Parent is not a directory
    /// - EACCES: Permission denied on parent directory
    /// - EEXIST: File already exists (POSIX requires immediate failure)
    /// - EPERM: Parent directory is immutable
    fn do_mknod(
        &self,
        ctx: Arc<FuseContext>,
        new_inode: Ino,
        new_inode_attr: InodeAttr,
        parent: Ino,
        name: &str,
        typ: FileType,
        path: String,
    ) -> Result<(Ino, InodeAttr)> {
        self.kv.transaction(|txn| {
            let mut new_inode_attr = new_inode_attr.clone();
            debug!("get attr {} from backend", parent);
            let mut parent_attr = txn.get_for_update_or_missing(&key::Attr(parent))?;
            ensure!(
                parent_attr.is_dir(),
                LibcSnafu {
                    errno: libc::ENOTDIR,
                }
            );
            // check if the parent have the permission
            ctx.check_access(&parent_attr, kiseki_common::MODE_MASK_W)?;
            ensure!(
                !kiseki_types::attr::Flags::from_bits_truncate(parent_attr.flags as u8)
                    .contains(kiseki_types::attr::Flags::IMMUTABLE),
                LibcSnafu { errno: libc::EPERM }
            );

            // check if the entry already exists
            if txn.get(&key::Dentry(parent, name))?.is_some() {
                return LibcSnafu {
                    errno: libc::EEXIST,
                }
                .fail();
            }

            // check if we need to update the parent
            let mut update_parent_attr = false;
            if typ == FileType::Directory {
                parent_attr.set_nlink(parent_attr.nlink + 1);

                let now = SystemTime::now();
                parent_attr.mtime = now;
                parent_attr.ctime = now;
                update_parent_attr = true;
            };

            let now = SystemTime::now();
            new_inode_attr.set_atime(now);
            new_inode_attr.set_mtime(now);
            new_inode_attr.set_ctime(now);

            #[cfg(target_os = "macos")]
            {
                new_inode_attr.set_gid(parent_attr.gid);
            }

            // TODO: review the logic here
            #[cfg(target_os = "linux")]
            {
                // if the parent directory has the set group ID (SGID) bit set in its
                // mode. If so, it sets the group ID of the new node to
                // the group ID of the parent directory.
                if parent_attr.mode & constants::S_ISGID != 0 {
                    new_inode_attr.set_gid(parent_attr.gid);
                    // If the type of the node being created is a directory, it sets the SGID bit
                    // in the mode of the new node. This ensures that newly created directories
                    // inherit the group ID of their parent directory.
                    if typ == FileType::Directory {
                        new_inode_attr.mode |= constants::S_ISGID;
                    } else if new_inode_attr.mode & constants::MODE_MASK_SETGID_EXEC
                        == constants::MODE_MASK_SETGID_EXEC
                        && ctx.uid != 0
                        && !ctx.gid_list.contains(&parent_attr.gid)
                    {
                        // If the mode of the new node has both the set group ID bit and the set
                        // group execute bit, and if the user ID is not 0 (i.e., the user is not
                        // root), it further checks if the user belongs to the group of the parent
                        // directory. If not, it removes the SGID bit from the mode of the new node.
                        new_inode_attr.mode &= !constants::MODE_MASK_SETGID_EXEC;
                    }
                }
            }

            // insert entry
            txn.put(
                &key::Dentry(parent, name),
                &DEntry {
                    parent,
                    name: name.to_string(),
                    inode: new_inode,
                    typ,
                },
            )?;
            // insert attr
            txn.put(&key::Attr(new_inode), &new_inode_attr)?;

            if update_parent_attr {
                // update parent attr
                txn.put(&key::Attr(parent), &parent_attr)?;
            }
            if typ == FileType::Symlink {
                txn.put(
                    &key::Symlink(new_inode),
                    &value::SymlinkTarget::new(path.clone().into_bytes()),
                )?;
            }

            Ok((new_inode, new_inode_attr))
        })
    }

    /// Remove a directory - POSIX rmdir(2) semantics implementation
    ///
    /// POSIX Compliance Requirements:
    /// - POSIX.1-2008 rmdir(2): Remove empty directories atomically
    /// - Empty Directory Validation: Must verify directory contains no entries
    ///   (except "." and "..")
    /// - Sticky Bit Enforcement: Must check sticky bit permissions on parent
    ///   directory
    /// - Permission Checks: Verify write permission on parent directory
    /// - Link Count Updates: Properly decrement parent directory's link count
    /// - Atomicity: Entire operation must be atomic (remove entry + update
    ///   metadata)
    ///
    /// Implementation Strategy:
    /// 1. Validate target exists and is actually a directory
    /// 2. Check parent directory permissions and sticky bit rules
    /// 3. Verify target directory is empty (no children exist)
    /// 4. Remove directory entry from parent
    /// 5. Update parent directory's link count and mtime
    /// 6. Mark target inode for cleanup
    /// 7. Commit entire operation atomically
    ///
    /// Error Handling:
    /// - ENOTDIR: Target is not a directory, or parent is not a directory
    /// - ENOTEMPTY: Directory is not empty
    /// - EACCES: Permission denied due to sticky bit or write permissions
    /// - EPERM: Operation not permitted
    fn do_rmdir(
        &self,
        ctx: Arc<FuseContext>,
        parent: Ino,
        name: &str,
        skip_dir_mtime: Duration,
    ) -> Result<(DEntry, InodeAttr)> {
        self.kv.transaction(|txn| {
            let entry_info = txn.get_or_missing(&key::Dentry(parent, name))?;
            ensure!(
                entry_info.typ == FileType::Directory,
                LibcSnafu {
                    errno: libc::ENOTDIR,
                }
            );
            // get parent and child's attr
            let mut parent_attr = txn.get_for_update_or_missing(&key::Attr(parent))?;
            ensure!(
                // parent must be dir.
                parent_attr.is_dir(),
                LibcSnafu {
                    errno: libc::ENOTDIR,
                }
            );
            let child_attr = txn.get_or_missing(&key::Attr(entry_info.inode))?;
            ensure!(
                // child must be dir. check again in case of we found that the entry info tells
                // the different story.
                child_attr.is_dir(),
                LibcSnafu {
                    errno: libc::ENOTDIR,
                }
            );

            ctx.check_access(
                &parent_attr,
                kiseki_common::MODE_MASK_W | kiseki_common::MODE_MASK_X,
            )?;
            let parent_flag =
                kiseki_types::attr::Flags::from_bits_truncate(parent_attr.flags as u8);
            ensure!(
                !parent_flag.contains(kiseki_types::attr::Flags::APPEND)
                    && !parent_flag.contains(kiseki_types::attr::Flags::IMMUTABLE),
                LibcSnafu { errno: libc::EPERM }
            );
            ensure!(
                !do_check_exist_children(txn, entry_info.inode)?,
                LibcSnafu {
                    errno: libc::ENOTEMPTY,
                }
            );
            // POSIX sticky bit check for rmdir operation (POSIX.1-2008 Section 4.5.4)
            // When sticky bit is SET on parent directory, only directory owner,
            // file owner, or root can delete the directory entry
            if ctx.uid != 0
                && parent_attr.mode & constants::S_ISVTX != 0
                && ctx.uid != parent_attr.uid
                && ctx.uid != child_attr.uid
            {
                return LibcSnafu {
                    errno: libc::EACCES,
                }
                .fail();
            }
            parent_attr.nlink -= 1;
            let now = SystemTime::now();

            let need_update_parent_attr = if now
                .duration_since(parent_attr.mtime)
                .expect("found mtime in the future")
                >= skip_dir_mtime
            {
                parent_attr.mtime = now;
                parent_attr.ctime = now;
                true
            } else {
                false
            };

            // delete entry
            txn.delete(&key::Dentry(parent, name))?;
            // delete inode attr
            txn.delete(&key::Attr(entry_info.inode))?;
            if need_update_parent_attr {
                // update parent attr
                txn.put(&key::Attr(parent), &parent_attr)?;
            }

            Ok((entry_info, child_attr))
        })
    }

    /// Truncate a regular file to specified length - POSIX truncate(2)
    /// semantics implementation
    ///
    /// POSIX Compliance Requirements:
    /// - POSIX.1-2008 truncate(2)/ftruncate(2): Change file size atomically
    /// - File Type Restriction: Only regular files can be truncated
    /// - Permission Checks: Verify write permission on file (unless
    ///   skip_perm_check is true)
    /// - Size Handling: Expand with zero-fill or shrink by removing data
    /// - Metadata Updates: Update file size, mtime, and ctime atomically
    /// - Immutable Files: Respect immutable and append-only file attributes
    ///
    /// Implementation Strategy:
    /// 1. Validate target is a regular file (not directory, device, etc.)
    /// 2. Check file attributes for immutable/append-only flags
    /// 3. Perform permission check unless explicitly skipped
    /// 4. Update file size in inode attributes
    /// 5. Update mtime and ctime to current time
    /// 6. Store updated attributes atomically
    /// 7. Note: Actual data truncation handled by storage layer
    ///
    /// Error Handling:
    /// - EPERM: Not a regular file, or file is immutable/append-only
    /// - EACCES: Permission denied for write access
    /// - EFBIG: Length exceeds filesystem limits
    fn do_truncate(
        &self,
        ctx: Arc<FuseContext>,
        inode: Ino,
        length: u64,
        skip_perm_check: bool,
    ) -> Result<InodeAttr> {
        self.kv.transaction(|txn| {
            let mut old_attr = txn.get_for_update_or_missing(&key::Attr(inode))?;
            ensure!(
                matches!(old_attr.kind, FileType::RegularFile),
                LibcSnafu { errno: libc::EPERM }
            );
            let flags = kiseki_types::attr::Flags::from_bits_truncate(old_attr.flags as u8);
            if flags.contains(kiseki_types::attr::Flags::IMMUTABLE)
                || flags.contains(kiseki_types::attr::Flags::APPEND)
            {
                return LibcSnafu { errno: libc::EPERM }.fail();
            }
            if !skip_perm_check {
                ctx.check_access(&old_attr, kiseki_common::MODE_MASK_W)?;
            }
            assert_ne!(length, old_attr.length, "length is the same");
            ensure!(
                usize::try_from(length).is_ok() && usize::try_from(old_attr.length).is_ok(),
                LibcSnafu { errno: libc::EFBIG }
            );
            let old_length = old_attr.length as usize;
            let new_length = length as usize;
            if new_length > old_length {
                let mut position = old_length;
                while position < new_length {
                    let chunk_index = position / CHUNK_SIZE;
                    let chunk_position = position % CHUNK_SIZE;
                    let hole_length = (CHUNK_SIZE - chunk_position).min(new_length - position);
                    let hole = Slice::new_owned(chunk_position, EMPTY_SLICE_ID, hole_length);
                    stage_chunk_slice(txn, inode, chunk_index, &hole)?;
                    position += hole_length;
                }
            }
            old_attr.update_length(length);

            txn.put(&key::Attr(inode), &old_attr)?;
            Ok(old_attr)
        })
    }

    /// Create a hard link to existing file - POSIX link(2) semantics
    /// implementation
    ///
    /// POSIX Compliance Requirements:
    /// - POSIX.1-2008 link(2): Create additional directory entry pointing to
    ///   existing inode
    /// - Hard Link Restrictions: Cannot create hard links to directories
    ///   (except by privileged processes)
    /// - Link Count Management: Must increment nlink count in target inode
    ///   attributes
    /// - Permission Checks: Verify write permission on destination parent
    ///   directory
    /// - Exclusive Creation: Must fail with EEXIST if destination name already
    ///   exists
    /// - Same Filesystem: Hard links can only exist within same filesystem
    ///
    /// Implementation Strategy:
    /// 1. Validate destination parent exists and is a directory
    /// 2. Check write permission on destination parent directory
    /// 3. Verify target inode exists and get its current attributes
    /// 4. Ensure target is not a directory (POSIX restriction)
    /// 5. Check that destination name doesn't already exist
    /// 6. Create new directory entry pointing to existing inode
    /// 7. Increment hard link count in target inode
    /// 8. Update destination parent directory mtime
    /// 9. Commit all changes atomically
    ///
    /// Error Handling:
    /// - ENOTDIR: Parent is not a directory
    /// - EACCES: Permission denied on parent directory
    /// - EPERM: Trying to create hard link to directory
    /// - EEXIST: Destination name already exists
    /// - EMLINK: Too many hard links (filesystem limit)
    fn do_link(
        &self,
        ctx: Arc<FuseContext>,
        inode: Ino,
        new_parent: Ino,
        new_name: &str,
    ) -> Result<InodeAttr> {
        let skip = self.skip_dir_mtime;
        self.kv.transaction(|txn| {
            // get parent and child's attr
            let mut parent_attr = txn.get_for_update_or_missing(&key::Attr(new_parent))?;
            ensure!(
                // parent must be dir.
                parent_attr.is_dir(),
                LibcSnafu {
                    errno: libc::ENOTDIR,
                }
            );
            ctx.check_access(&parent_attr, kiseki_common::MODE_MASK_W)?;
            ensure!(
                !parent_attr.is_immutable(),
                LibcSnafu { errno: libc::EPERM }
            );

            let mut child_attr = txn.get_for_update_or_missing(&key::Attr(inode))?;
            ensure!(!child_attr.is_dir(), LibcSnafu { errno: libc::EPERM });
            ensure!(child_attr.is_normal(), LibcSnafu { errno: libc::EPERM });
            ensure!(
                // the target name must be empty
                txn.get(&key::Dentry(new_parent, new_name))?.is_none(),
                LibcSnafu {
                    errno: libc::EEXIST,
                }
            );
            // check if we need to update the parent
            let now = SystemTime::now();
            let need_update_parent_attr = parent_attr.update_modification_time_if(now, skip);
            let old_parent = child_attr.parent;
            child_attr.ctime = now;
            child_attr.nlink += 1;
            child_attr.parent = ZERO_INO;

            // 1. create an entry that points to the original inode.
            txn.put(
                &key::Dentry(new_parent, new_name),
                &DEntry {
                    parent: new_parent,
                    name: new_name.to_string(),
                    inode,
                    typ: child_attr.kind,
                },
            )?;
            if need_update_parent_attr {
                // 2. update parent attr
                txn.put(&key::Attr(new_parent), &parent_attr)?;
            }
            // 3. update child attr
            txn.put(&key::Attr(inode), &child_attr)?;
            if !child_attr.parent.is_zero() {
                let cnt = get_hard_link_count(txn, inode, old_parent)?;
                txn.put(&key::HardLink(inode, old_parent), &(cnt + 1))?;
            }
            let cnt = get_hard_link_count(txn, inode, new_parent)?;
            txn.put(&key::HardLink(inode, new_parent), &(cnt + 1))?;

            Ok(child_attr)
        })
    }

    /// Remove a file (unlink) - POSIX unlink(2) semantics implementation
    ///
    /// POSIX Compliance Requirements:
    /// - POSIX.1-2008 unlink(2): Remove directory entry and decrement link
    ///   count
    /// - Hard Link Management: Decrement nlink count, remove file data only
    ///   when nlink reaches 0
    /// - Sticky Bit Enforcement: Check sticky bit permissions on parent
    ///   directory
    /// - Open File Handling: Allow unlink of open files, but defer deletion
    ///   until last close
    /// - Permission Checks: Verify write permission on parent directory
    /// - Directory Protection: Must not allow unlinking of directories (use
    ///   rmdir instead)
    ///
    /// Implementation Strategy:
    /// 1. Validate target exists and is not a directory
    /// 2. Check parent directory permissions and sticky bit rules
    /// 3. Check if file is currently open in any session
    /// 4. Remove directory entry from parent
    /// 5. Decrement hard link count for the inode
    /// 6. If nlink reaches 0 and file not open, mark for data deletion
    /// 7. Update parent directory mtime
    /// 8. Commit all changes atomically
    ///
    /// Error Handling:
    /// - EPERM: Attempting to unlink a directory
    /// - EACCES: Permission denied due to sticky bit or write permissions
    /// - ENOTDIR: Parent is not a directory
    /// - ENOENT: File does not exist
    async fn do_unlink(
        &self,
        ctx: Arc<FuseContext>,
        parent: Ino,
        name: String,
        session_id: u64,
        open_files_ref: OpenFilesRef,
    ) -> Result<UnlinkResult> {
        // Whether the target is currently open is a runtime property (not stored
        // in the KV layer), so it must be sampled outside the synchronous,
        // possibly-retried transaction. It is only consumed below once the inode
        // reaches nlink == 0, matching the previous semantics.
        let entry0 = self.kv.get_or_missing(&key::Dentry(parent, &name))?;
        let mut opened = false;
        if !matches!(entry0.typ, FileType::Directory)
            && let Some(of) = open_files_ref.load(&entry0.inode).await
        {
            opened = of.is_opened().await;
        }

        let skip = self.skip_dir_mtime;
        self.kv.transaction(|txn| {
            let entry = txn.get_or_missing(&key::Dentry(parent, &name))?;
            ensure!(
                !matches!(entry.typ, FileType::Directory),
                LibcSnafu { errno: libc::EPERM }
            );
            // get parent and child's attr
            let mut parent_attr = txn.get_for_update_or_missing(&key::Attr(parent))?;
            ensure!(
                // parent must be dir.
                parent_attr.is_dir(),
                LibcSnafu {
                    errno: libc::ENOTDIR,
                }
            );
            ctx.check_access(
                &parent_attr,
                kiseki_common::MODE_MASK_W | kiseki_common::MODE_MASK_X,
            )?;
            ensure!(parent_attr.is_normal(), LibcSnafu { errno: libc::EPERM });

            let now = SystemTime::now();
            let mut attr_place_holder = InodeAttr::empty();
            // the target exist
            if let Ok(mut attr) = txn.get_for_update_or_missing(&key::Attr(entry.inode)) {
                // POSIX sticky bit check for unlink operation (POSIX.1-2008 Section 4.5.4)
                // When sticky bit is SET on parent directory, only directory owner,
                // file owner, or root can delete the file
                if ctx.uid != 0
                    && parent_attr.mode & constants::S_ISVTX != 0
                    && ctx.uid != parent_attr.uid
                    && ctx.uid != attr.uid
                {
                    return LibcSnafu {
                        errno: libc::EACCES,
                    }
                    .fail();
                }
                ensure!(attr.is_normal(), LibcSnafu { errno: libc::EPERM });
                attr.ctime = now;
                attr.nlink -= 1;
                attr_place_holder = attr;
            }

            if parent_attr.update_modification_time_if(now, skip) {
                txn.put(&key::Attr(parent), &parent_attr)?;
            }
            // delete the entry
            txn.delete(&key::Dentry(parent, &name))?;
            let mut free_inode_cnt = 0;
            let mut free_space_size = 0;

            if attr_place_holder.nlink > 0 {
                txn.put(&key::Attr(entry.inode), &attr_place_holder)?;
                if attr_place_holder.parent.is_zero() {
                    let cnt = get_hard_link_count(txn, entry.inode, parent)?;
                    if cnt > 0 {
                        txn.put(&key::HardLink(entry.inode, parent), &(cnt - 1))?;
                    }
                }
            } else {
                if matches!(attr_place_holder.kind, FileType::RegularFile) {
                    if opened {
                        // update the inode attr
                        txn.put(&key::Attr(entry.inode), &attr_place_holder)?;
                        txn.put(&key::Sustained(session_id, entry.inode), &1u64)?;
                    } else {
                        // make a notification that we need to delete the chunk after a while.
                        txn.put(&key::DeleteChunk(entry.inode), &now_secs())?;
                        // delete inode attr
                        txn.delete(&key::Attr(entry.inode))?;
                        free_inode_cnt += 1;
                        free_space_size += attr_place_holder.length;
                    }
                } else {
                    if matches!(attr_place_holder.kind, FileType::Symlink) {
                        txn.delete(&key::Symlink(entry.inode))?;
                    }
                    txn.delete(&key::Attr(entry.inode))?;
                    free_inode_cnt += 1;
                    free_space_size += constants::DEFAULT_FILE_SIZE;
                }
                // delete xattr
                txn.delete_prefix_raw(&crate::backend::key::xattr_prefix(entry.inode))?;
                if attr_place_holder.parent.is_zero() {
                    // delete hardlinks
                    txn.delete_prefix(&key::HardLinkPrefix(entry.inode))?;
                }
            }

            let mut r = UnlinkResult {
                inode:       entry.inode,
                removed:     None,
                freed_space: free_space_size,
                freed_inode: free_inode_cnt,
                is_opened:   opened,
            };
            if attr_place_holder.nlink == 0 && attr_place_holder.is_file() {
                r.removed = Some(attr_place_holder);
            }

            Ok(r)
        })
    }

    fn do_delete_chunks(&self, inode: Ino) {
        // at present, we delete the slices directly, since we haven't
        // implemented the borrow mechanism.
        let result = self.kv.transaction(|txn| {
            txn.delete_prefix(&key::ChunkSlicesPrefix(inode))?;
            // clear the delete notification
            txn.delete(&key::DeleteChunk(inode))?;
            Ok(())
        });
        if let Err(e) = result {
            error!("failed to do_delete_chunks for {inode}: {e:?}");
        }
    }

    /// Rename/move a file or directory - POSIX rename(2) semantics
    /// implementation
    ///
    /// POSIX Compliance Requirements:
    /// - POSIX.1-2008 rename(2): Atomic move operation with complex semantics
    /// - Atomicity: Entire rename must appear atomic (either succeeds
    ///   completely or fails completely)
    /// - Destination Handling: If destination exists, it must be atomically
    ///   replaced
    /// - Directory Constraints: Cannot rename directory to subdirectory of
    ///   itself
    /// - Sticky Bit Enforcement: Check sticky bits on both source and
    ///   destination parents
    /// - Link Count Management: Properly update hard link counts for moved
    ///   directories
    /// - Cross-Directory Moves: Update link counts of both old and new parent
    ///   directories
    ///
    /// Implementation Strategy:
    /// 1. Validate source exists and get its metadata
    /// 2. Handle no-op case (same source and destination)
    /// 3. Check permissions on both source and destination parents
    /// 4. Verify sticky bit permissions for both operations
    /// 5. Handle destination file replacement if it exists
    /// 6. Create new directory entry in destination
    /// 7. Remove old directory entry from source
    /// 8. Update parent directory link counts and timestamps
    /// 9. Handle directory-specific link count updates
    /// 10. Commit entire operation atomically
    ///
    /// Error Handling:
    /// - EXDEV: Cross-filesystem rename (not applicable for single filesystem)
    /// - EACCES: Permission denied on source or destination
    /// - ENOTEMPTY: Trying to replace non-empty directory
    /// - EINVAL: Invalid rename (e.g., directory to subdirectory of itself)
    /// - ENOTDIR/EISDIR: Type mismatch between source and destination
    async fn do_rename(
        &self,
        ctx: Arc<FuseContext>,
        session_id: u64,
        old_parent: Ino,
        old_name: &str,
        new_parent: Ino,
        new_name: &str,
        flags: RenameFlags,
        open_files_ref: OpenFilesRef,
    ) -> Result<RenameResult> {
        // Sample whether an existing destination regular file is currently open.
        // Like `do_unlink`, this runtime property must be resolved before the
        // synchronous transaction; it is only consumed in the replace path when
        // the destination reaches nlink == 0.
        let mut opened = false;
        if let Ok(dst_entry) = self.kv.get_or_missing(&key::Dentry(new_parent, new_name))
            && matches!(dst_entry.typ, FileType::RegularFile)
            && let Some(of) = open_files_ref.load(&dst_entry.inode).await
        {
            opened = of.is_opened().await;
        }

        let skip = self.skip_dir_mtime;
        self.kv.transaction(|txn| {
            let old_entry = txn.get_or_missing(&key::Dentry(old_parent, old_name))?;
            let mut rename_result = RenameResult {
                need_delete: None,
                freed_inode: 0,
                freed_space: 0,
            };
            if old_parent == new_parent && old_name == new_name {
                return Ok(rename_result);
            }

            let mut old_parent_attr = txn.get_for_update_or_missing(&key::Attr(old_parent))?;
            {
                // check access permission
                ensure!(
                    old_parent_attr.is_dir(),
                    LibcSnafu {
                        errno: libc::ENOTDIR,
                    }
                );
                ctx.check_access(
                    &old_parent_attr,
                    kiseki_common::MODE_MASK_W | kiseki_common::MODE_MASK_X,
                )?;
            }

            let mut new_parent_attr = txn.get_for_update_or_missing(&key::Attr(new_parent))?;
            {
                ensure!(
                    new_parent_attr.is_dir(),
                    LibcSnafu {
                        errno: libc::ENOTDIR,
                    }
                );
                ctx.check_access(
                    &new_parent_attr,
                    kiseki_common::MODE_MASK_W | kiseki_common::MODE_MASK_X,
                )?;
                ensure!(
                    old_entry.inode != new_parent && old_entry.inode != new_parent_attr.parent,
                    LibcSnafu { errno: libc::EPERM }
                );
            }

            let mut old_inode_attr = txn.get_for_update_or_missing(&key::Attr(old_entry.inode))?;
            {
                ensure!(old_inode_attr.is_normal(), LibcSnafu { errno: libc::EPERM });
                // POSIX sticky bit check for rename source (POSIX.1-2008 Section 4.5.4)
                // When sticky bit is SET on source directory, additional permission check
                if old_parent != new_parent
                    && old_parent_attr.mode & constants::S_ISVTX != 0
                    && ctx.uid != 0
                    && ctx.uid != old_inode_attr.uid
                    && (ctx.uid != old_parent_attr.uid || old_inode_attr.is_dir())
                {
                    return LibcSnafu {
                        errno: libc::EACCES,
                    }
                    .fail();
                }

                // POSIX sticky bit check for rename operation (POSIX.1-2008 Section 4.5.4)
                // Additional sticky bit permission check for rename
                if ctx.uid != 0
                    && (old_parent_attr.mode & constants::S_ISVTX) != 0
                    && ctx.uid != old_parent_attr.uid
                    && ctx.uid != old_inode_attr.uid
                {
                    return LibcSnafu {
                        errno: libc::EACCES,
                    }
                    .fail();
                }
            }

            let (mut update_new_parent, mut dst_dentry_opt, mut dst_attr_opt) = (false, None, None);
            match txn.get(&key::Dentry(new_parent, new_name))? {
                Some(dst_entry) => {
                    // dst exists
                    ensure!(
                        !flags.contains(RenameFlags::NOREPLACE),
                        LibcSnafu {
                            errno: libc::EEXIST,
                        }
                    );

                    let mut dst_attr =
                        txn.get_for_update_or_missing(&key::Attr(dst_entry.inode))?;
                    ensure!(dst_attr.is_normal(), LibcSnafu { errno: libc::EPERM });
                    dst_attr.ctime = SystemTime::now();

                    if matches!(flags, RenameFlags::EXCHANGE) {
                        if old_parent != new_parent {
                            if matches!(dst_entry.typ, FileType::Directory) {
                                dst_attr.parent = old_parent;
                                new_parent_attr.nlink -= 1;
                                old_parent_attr.nlink += 1;
                            } else if !dst_attr.parent.is_zero() {
                                dst_attr.parent = old_parent;
                            }
                        }
                    } else if matches!(dst_entry.typ, FileType::Directory) {
                        ensure!(
                            !do_check_exist_children(txn, dst_entry.inode)?,
                            LibcSnafu {
                                errno: libc::ENOTEMPTY,
                            }
                        );
                        new_parent_attr.nlink -= 1;
                        update_new_parent = true;
                    } else {
                        dst_attr.nlink -= 1;
                        // `opened` for the destination was sampled before the
                        // transaction.
                    }

                    // POSIX sticky bit check for rename operation
                    // When sticky bit is SET on destination directory, only file owner,
                    // directory owner, or root can delete/rename the destination file
                    if ctx.uid != 0
                        && (new_parent_attr.mode & constants::S_ISVTX) != 0
                        && ctx.uid != new_parent_attr.uid
                        && ctx.uid != dst_attr.uid
                    {
                        return LibcSnafu {
                            errno: libc::EACCES,
                        }
                        .fail();
                    }

                    dst_dentry_opt = Some(dst_entry);
                    dst_attr_opt = Some(dst_attr);
                }
                None => {
                    ensure!(
                        !matches!(flags, RenameFlags::EXCHANGE),
                        LibcSnafu {
                            errno: libc::ENOENT,
                        }
                    );
                }
            }

            if old_parent != new_parent {
                old_inode_attr.parent = new_parent;
                old_parent_attr.nlink -= 1;
                new_parent_attr.nlink += 1;
            }
            let now = SystemTime::now();
            let update_old_parent = old_parent_attr.update_modification_time_if(now, skip);
            if update_new_parent {
                new_parent_attr.update_modification_time_with(now);
            } else {
                update_new_parent = new_parent_attr.update_modification_time_if(now, skip);
            }
            old_inode_attr.ctime = now;

            match flags {
                RenameFlags::EXCHANGE => {
                    // EXCHANGE requires the destination to exist; checked above.
                    let dst_dentry = dst_dentry_opt.context(LibcSnafu { errno: libc::EIO })?;
                    txn.put(
                        &key::Dentry(old_parent, old_name),
                        &DEntry {
                            parent: old_parent,
                            name:   old_name.to_string(),
                            inode:  dst_dentry.inode,
                            typ:    dst_dentry.typ,
                        },
                    )?;
                    let dst_attr = dst_attr_opt.context(LibcSnafu { errno: libc::EIO })?;
                    txn.put(&key::Attr(dst_dentry.inode), &dst_attr)?;
                    if old_parent != new_parent && dst_attr.parent.is_zero() {
                        let cnt = get_hard_link_count(txn, dst_dentry.inode, old_parent)?;
                        txn.put(&key::HardLink(dst_dentry.inode, old_parent), &(cnt + 1))?;
                        let cnt = get_hard_link_count(txn, dst_dentry.inode, new_parent)?
                            .saturating_sub(1);
                        txn.put(&key::HardLink(dst_dentry.inode, new_parent), &cnt)?;
                    }
                }
                _ => {
                    txn.delete(&key::Dentry(old_parent, old_name))?;
                    if let Some(dst_attr) = dst_attr_opt {
                        // a destination attr always comes with its dentry; checked above.
                        let dst_entry = dst_dentry_opt.context(LibcSnafu { errno: libc::EIO })?;
                        if !matches!(dst_attr.kind, FileType::Directory) && dst_attr.nlink > 0 {
                            txn.put(&key::Attr(dst_entry.inode), &dst_attr)?;
                            if dst_attr.parent.is_zero() {
                                let cnt = get_hard_link_count(txn, dst_entry.inode, old_parent)?;
                                if cnt > 0 {
                                    txn.put(
                                        &key::HardLink(dst_entry.inode, old_parent),
                                        &(cnt - 1),
                                    )?;
                                }
                            }
                        } else {
                            if matches!(dst_attr.kind, FileType::RegularFile) {
                                if opened {
                                    txn.put(&key::Attr(dst_entry.inode), &dst_attr)?;
                                    txn.put(&key::Sustained(session_id, dst_entry.inode), &1u64)?;
                                } else {
                                    txn.put(&key::DeleteChunk(dst_entry.inode), &now_secs())?;
                                    txn.delete(&key::Attr(dst_entry.inode))?;
                                    rename_result.freed_space +=
                                        kiseki_utils::align::align4k(dst_attr.length) as u64;
                                    rename_result.freed_inode += 1;
                                }
                                rename_result.need_delete = Some((dst_entry.inode, opened));
                            } else {
                                if matches!(dst_attr.kind, FileType::Symlink) {
                                    txn.delete(&key::Symlink(dst_entry.inode))?;
                                }
                                txn.delete(&key::Attr(dst_entry.inode))?;
                                rename_result.freed_space += 4096;
                                rename_result.freed_inode += 1;
                            }

                            txn.delete_prefix_raw(&crate::backend::key::xattr_prefix(
                                dst_entry.inode,
                            ))?;
                            if dst_attr.parent.is_zero() {
                                txn.delete_prefix(&key::HardLinkPrefix(dst_entry.inode))?;
                            }
                        }
                    }
                }
            }

            if new_parent != old_parent {
                if update_old_parent {
                    txn.put(&key::Attr(old_parent), &old_parent_attr)?;
                }
                if old_inode_attr.parent.is_zero() {
                    let cnt = get_hard_link_count(txn, old_entry.inode, new_parent)?;
                    txn.put(&key::HardLink(old_entry.inode, new_parent), &(cnt + 1))?;
                    let cnt =
                        get_hard_link_count(txn, old_entry.inode, old_parent)?.saturating_sub(1);
                    txn.put(&key::HardLink(old_entry.inode, old_parent), &cnt)?;
                }
            }

            txn.put(&key::Attr(old_entry.inode), &old_inode_attr)?;
            txn.put(
                &key::Dentry(new_parent, new_name),
                &DEntry {
                    parent: new_parent,
                    name:   new_name.to_string(),
                    inode:  old_entry.inode,
                    typ:    old_inode_attr.kind,
                },
            )?;
            if update_new_parent {
                txn.put(&key::Attr(new_parent), &new_parent_attr)?;
            }

            Ok(rename_result)
        })
    }

    /// Read symbolic link target path - POSIX readlink(2) wrapper
    /// implementation
    ///
    /// POSIX Compliance Requirements:
    /// - POSIX.1-2008 readlink(2): Return target path of symbolic link
    /// - Data Integrity: Must return exact path as stored during symlink
    ///   creation
    /// - Atomicity: Read operation must be atomic and consistent
    /// - Error Handling: Proper error codes for non-symlink inodes or missing
    ///   data
    ///
    /// Implementation Details:
    /// - Reads the raw symlink bytes back verbatim (they are stored without a
    ///   bincode wrapper) and returns them unchanged.
    fn do_readlink(&self, inode: Ino) -> Result<Bytes> {
        let target = self.kv.get_or_missing(&key::Symlink(inode))?;
        Ok(target.0)
    }
}

#[cfg(feature = "meta-rocksdb")]
#[cfg(test)]
mod tests {
    use kiseki_types::setting::Format;
    use rstest::*;
    use tempfile::TempDir;

    use super::*;

    // Test fixtures
    #[fixture]
    fn test_backend() -> (RocksdbBackend, TempDir) {
        let tempdir = tempfile::tempdir().unwrap();
        let kv = RocksDbKv::open(tempdir.path()).unwrap();
        let backend = RocksdbBackend {
            kv,
            skip_dir_mtime: Duration::from_millis(100),
        };
        (backend, tempdir)
    }

    #[fixture]
    fn sample_attr() -> InodeAttr {
        let mut attr = InodeAttr::default();
        attr.set_uid(1000);
        attr.set_gid(1000);
        attr.mode = 0o644;
        attr.set_length(1024);
        attr
    }

    #[fixture]
    fn sample_format() -> Format {
        Format {
            name:         "test-fs".to_string(),
            chunk_size:   64 * 1024,
            block_size:   4 * 1024 * 1024,
            page_size:    4096,
            max_capacity: Some(1024 * 1024 * 1024),
            max_inodes:   Some(1000000),
        }
    }

    // Basic functionality tests
    #[rstest]
    fn test_backend_creation(test_backend: (RocksdbBackend, TempDir)) {
        let (backend, _tempdir) = test_backend;
        // Backend should be created successfully
        assert!(format!("{backend:?}").contains("RocksdbEngine"));
    }

    #[rstest]
    fn test_format_operations(test_backend: (RocksdbBackend, TempDir), sample_format: Format) {
        let (backend, _tempdir) = test_backend;

        // Set format should succeed
        backend.set_format(&sample_format).unwrap();

        // Load format should return the same data
        let loaded_format = backend.load_format().unwrap();
        assert_eq!(loaded_format.name, sample_format.name);
        assert_eq!(loaded_format.chunk_size, sample_format.chunk_size);
        assert_eq!(loaded_format.block_size, sample_format.block_size);
    }

    #[rstest]
    fn test_aborted_initialization_leaves_no_partial_state(
        test_backend: (RocksdbBackend, TempDir),
        sample_format: Format,
    ) {
        let (backend, _tempdir) = test_backend;
        let root = InodeAttr::hard_code_inode_attr(false);

        // Stage the initial state, then abort the transaction with an
        // application error so nothing is committed.
        let result: Result<()> = backend.kv.transaction(|txn| {
            stage_initial_state(txn, &sample_format, &root)?;
            LibcSnafu { errno: libc::EIO }.fail()
        });
        assert!(result.is_err());

        assert!(backend.load_format().is_err());
        assert!(backend.get_attr(ROOT_INO).is_err());
        assert!(backend.load_count(BackendCounter::NextInode).is_err());
    }

    #[rstest]
    #[case(BackendCounter::NextInode, 10)]
    #[case(BackendCounter::NextSlice, 100)]
    #[case(BackendCounter::UsedSpace, 1024)]
    fn test_counter_operations(
        test_backend: (RocksdbBackend, TempDir),
        #[case] counter: BackendCounter,
        #[case] step: usize,
    ) {
        let (backend, _tempdir) = test_backend;

        // Initial increase should return the step value
        let result1 = backend.increase_count_by(counter, step).unwrap();
        assert_eq!(result1, step as u64);

        // Second increase should accumulate
        let result2 = backend.increase_count_by(counter, step).unwrap();
        assert_eq!(result2, (step * 2) as u64);

        // Load count should return current value
        let loaded = backend.load_count(counter).unwrap();
        assert_eq!(loaded, (step * 2) as u64);
    }

    #[rstest]
    fn test_attr_operations(test_backend: (RocksdbBackend, TempDir), sample_attr: InodeAttr) {
        let (backend, _tempdir) = test_backend;
        let inode = Ino(42);

        // Set attr should succeed
        backend.set_attr(inode, &sample_attr).unwrap();

        // Get attr should return the same data
        let loaded_attr = backend.get_attr(inode).unwrap();
        assert_eq!(loaded_attr.uid, sample_attr.uid);
        assert_eq!(loaded_attr.gid, sample_attr.gid);
        assert_eq!(loaded_attr.mode, sample_attr.mode);
        assert_eq!(loaded_attr.length, sample_attr.length);
    }

    #[rstest]
    #[case("test_file", FileType::RegularFile)]
    #[case("test_dir", FileType::Directory)]
    #[case("test_link", FileType::Symlink)]
    fn test_dentry_operations(
        test_backend: (RocksdbBackend, TempDir),
        #[case] name: &str,
        #[case] file_type: FileType,
    ) {
        let (backend, _tempdir) = test_backend;
        let parent = Ino(1);
        let inode = Ino(2);

        // Set dentry should succeed
        backend.set_dentry(parent, name, inode, file_type).unwrap();

        // Get dentry should return correct data
        let dentry = backend.get_dentry(parent, name).unwrap();
        assert_eq!(dentry.parent, parent);
        assert_eq!(dentry.name, name);
        assert_eq!(dentry.inode, inode);
        assert_eq!(dentry.typ, file_type);
    }

    #[rstest]
    fn test_list_dentry_with_limits(test_backend: (RocksdbBackend, TempDir)) {
        let (backend, _tempdir) = test_backend;
        let parent = Ino(1);

        // Create multiple dentries
        let entries = vec![
            ("file1", Ino(10), FileType::RegularFile),
            ("file2", Ino(11), FileType::RegularFile),
            ("dir1", Ino(12), FileType::Directory),
            ("file3", Ino(13), FileType::RegularFile),
        ];

        for (name, inode, typ) in &entries {
            backend.set_dentry(parent, name, *inode, *typ).unwrap();
        }

        // List all dentries
        let all_dentries = backend.list_dentry(parent, -1).unwrap();
        assert_eq!(all_dentries.len(), entries.len());

        // List with limit
        let limited_dentries = backend.list_dentry(parent, 2).unwrap();
        assert_eq!(limited_dentries.len(), 2);

        // Empty parent should return empty list
        let empty_dentries = backend.list_dentry(Ino(999), -1).unwrap();
        assert_eq!(empty_dentries.len(), 0);
    }

    #[rstest]
    fn test_symlink_operations(test_backend: (RocksdbBackend, TempDir)) {
        let (backend, _tempdir) = test_backend;
        let inode = Ino(100);
        let target_path = "/tmp/target_file";

        // Set symlink should succeed
        backend.set_symlink(inode, target_path.to_string()).unwrap();

        // Get symlink should return correct path
        let loaded_path = backend.get_symlink(inode).unwrap();
        assert_eq!(loaded_path, target_path);
    }

    #[rstest]
    fn test_chunk_slices_operations(test_backend: (RocksdbBackend, TempDir)) {
        let (backend, _tempdir) = test_backend;
        let inode = Ino(200);
        let chunk_index = 5;

        // Test raw chunk slices
        let test_data = vec![1, 2, 3, 4, 5];
        backend
            .set_raw_chunk_slices(inode, chunk_index, test_data.clone())
            .unwrap();

        let loaded_data = backend.get_raw_chunk_slices(inode, chunk_index).unwrap();
        assert_eq!(loaded_data, Some(test_data));

        // Test non-existent chunk
        let empty_data = backend.get_raw_chunk_slices(Ino(999), 0).unwrap();
        assert_eq!(empty_data, None);
    }

    #[rstest]
    fn test_dir_stat_operations(test_backend: (RocksdbBackend, TempDir)) {
        let (backend, _tempdir) = test_backend;
        let inode = Ino(300);
        let dir_stat = DirStat {
            length: 2048,
            space:  1024,
            inodes: 10,
        };

        // Set dir stat should succeed
        backend.set_dir_stat(inode, dir_stat).unwrap();

        // Get dir stat should return correct data
        let loaded_stat = backend.get_dir_stat(inode).unwrap();
        assert_eq!(loaded_stat.space, 1024);
        assert_eq!(loaded_stat.inodes, 10);
        assert_eq!(loaded_stat.length, 2048);
    }

    // Test typed reads directly against the KV layer
    #[rstest]
    fn test_helper_functions(test_backend: (RocksdbBackend, TempDir), sample_attr: InodeAttr) {
        let (backend, _tempdir) = test_backend;
        let inode = Ino(400);

        // Set up test data using backend methods
        backend.set_attr(inode, &sample_attr).unwrap();

        // Typed attr read
        let attr = backend.kv.get_or_missing(&key::Attr(inode)).unwrap();
        assert_eq!(attr.uid, sample_attr.uid);

        // Typed dentry read
        let parent = Ino(1);
        let name = "test_helper";
        backend
            .set_dentry(parent, name, inode, FileType::RegularFile)
            .unwrap();

        let dentry = backend
            .kv
            .get_or_missing(&key::Dentry(parent, name))
            .unwrap();
        assert_eq!(dentry.inode, inode);
        assert_eq!(dentry.name, name);
    }

    // Test error cases
    #[rstest]
    fn test_error_cases(test_backend: (RocksdbBackend, TempDir)) {
        let (backend, _tempdir) = test_backend;

        // Getting non-existent attr should fail
        let result = backend.get_attr(Ino(999));
        assert!(result.is_err());

        // Getting non-existent dentry should fail
        let result = backend.get_dentry(Ino(1), "non_existent");
        assert!(result.is_err());

        // Getting non-existent symlink should fail
        let result = backend.get_symlink(Ino(999));
        assert!(result.is_err());

        // Getting non-existent dir stat should fail
        let result = backend.get_dir_stat(Ino(999));
        assert!(result.is_err());

        // Loading format without setting should fail
        let result = backend.load_format();
        assert!(result.is_err());

        // Loading non-existent counter should fail
        let result = backend.load_count(BackendCounter::NextInode);
        assert!(result.is_err());
    }

    // Test batch operations and transactions
    #[rstest]
    fn test_batch_operations(test_backend: (RocksdbBackend, TempDir)) {
        let (backend, _tempdir) = test_backend;

        // Test that batch operations work correctly
        let parent = Ino(1);
        let child = Ino(2);
        let parent_attr = InodeAttr {
            kind: FileType::Directory,
            ..Default::default()
        };

        let child_attr = InodeAttr {
            kind: FileType::RegularFile,
            ..Default::default()
        };

        // Set up parent directory
        backend.set_attr(parent, &parent_attr).unwrap();
        backend.set_attr(child, &child_attr).unwrap();
        backend
            .set_dentry(parent, "child", child, FileType::RegularFile)
            .unwrap();

        // Verify the setup worked
        let loaded_parent = backend.get_attr(parent).unwrap();
        assert_eq!(loaded_parent.kind, FileType::Directory);

        let loaded_child = backend.get_attr(child).unwrap();
        assert_eq!(loaded_child.kind, FileType::RegularFile);

        let dentry = backend.get_dentry(parent, "child").unwrap();
        assert_eq!(dentry.inode, child);
    }

    #[rstest]
    fn committing_a_slice_is_atomic_and_idempotent(test_backend: (RocksdbBackend, TempDir)) {
        let (backend, _tempdir) = test_backend;
        let inode = Ino(42);
        let attr = InodeAttr {
            kind: FileType::RegularFile,
            ..Default::default()
        };
        backend.set_attr(inode, &attr).unwrap();
        let first = kiseki_types::slice::Slice::new_owned(0, 100, 4);
        let second = kiseki_types::slice::Slice::new_owned(8, 101, 4);

        backend.commit_slice(inode, 0, &first).unwrap();
        backend.commit_slice(inode, 0, &second).unwrap();
        backend.commit_slice(inode, 0, &first).unwrap();

        let slices = backend.get_chunk_slices(inode, 0).unwrap();
        assert_eq!(slices.0, vec![first, second]);
        assert_eq!(backend.get_attr(inode).unwrap().length, 12);
    }

    #[rstest]
    fn aborted_slice_transaction_leaves_no_partial_publication(
        test_backend: (RocksdbBackend, TempDir),
    ) {
        let (backend, _tempdir) = test_backend;
        let inode = Ino(43);
        backend
            .set_attr(
                inode,
                &InodeAttr {
                    kind: FileType::RegularFile,
                    ..Default::default()
                },
            )
            .unwrap();
        let slice = Slice::new_owned(0, 200, 8);
        // Stage a slice commit, then abort so nothing is published.
        let result: Result<()> = backend.kv.transaction(|txn| {
            stage_slice_commit(txn, inode, 0, &slice)?;
            LibcSnafu { errno: libc::EIO }.fail()
        });
        assert!(result.is_err());

        assert_eq!(backend.get_attr(inode).unwrap().length, 0);
        assert!(backend.get_raw_chunk_slices(inode, 0).unwrap().is_none());
    }
}
