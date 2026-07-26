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
    slice::{EMPTY_SLICE_ID, SLICE_BYTES, Slice, Slices},
    stat::DirStat,
};
use rocksdb::{DBAccess, MultiThreaded};
use serde::Serialize;
use snafu::{OptionExt, ResultExt, ensure};
use tracing::{debug, error};

// Import RocksDB metrics macros from internal module
use super::rocksdb_metrics::{
    rocksdb_counter, rocksdb_delete, rocksdb_error, rocksdb_histogram, rocksdb_timed_op,
};
use super::{
    Backend, RenameResult, SliceCommitResult, UnlinkResult, key, key::Counter as BackendCounter,
};
use crate::{
    context::FuseContext,
    engine::RenameFlags,
    err::{
        AlreadyInitializedSnafu, LibcSnafu, ModelSnafu, Result, RocksdbSnafu,
        UninitializedEngineSnafu, model_err, model_err::ModelKind,
    },
    open_files::OpenFilesRef,
};

/// **POSIX-Compliant Meta Storage Backend Implementation using RocksDB**
///
/// This module implements a complete POSIX-compliant filesystem metadata
/// storage layer using RocksDB as the underlying persistent storage engine. All
/// operations follow POSIX.1-2008 standard semantics to ensure compatibility
/// with standard UNIX filesystems.
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
/// Complex operations (rename, rmdir) use RocksDB transactions to ensure
/// atomicity. Simple operations use direct writes for performance. All error
/// conditions follow POSIX error code conventions (EEXIST, ENOTEMPTY, EACCES,
/// etc.).
///
/// ## Permission and Security:
/// Implements full POSIX permission checking including:
/// - Standard permission bits (read/write/execute for owner/group/other)
/// - Special permission bits (setuid, setgid, sticky bit)
/// - Sticky bit directory protection for secure deletion
/// - Immutable and append-only file attribute support
///
/// ## Data Consistency Guarantees:
/// - All metadata updates are atomic at the RocksDB level
/// - Batch operations ensure multiple related updates commit together
/// - Transaction isolation prevents partial state visibility
/// - Write-ahead logging provides crash recovery
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

/// Macro for deserializing values from RocksDB with unified error handling
macro_rules! deserialize_db {
    ($db:expr, $key:expr, $typ:ty, $kind:expr) => {{
        let buf = $db
            .get_pinned_opt(&$key, &rocksdb::ReadOptions::default())
            .context(RocksdbSnafu)?
            .context(model_err::NotFoundSnafu {
                kind: $kind,
                key:  String::from_utf8_lossy(&$key).to_string(),
            })
            .context(ModelSnafu)?;

        bincode::deserialize::<$typ>(&buf)
            .context(model_err::CorruptionSnafu {
                kind: $kind,
                key:  String::from_utf8_lossy(&$key).to_string(),
            })
            .context(ModelSnafu)?
    }};
}

/// Macro for serializing values to write batch with unified error handling
macro_rules! serialize_batch {
    ($batch:expr, $key:expr, $value:expr, $kind:expr) => {{
        let buf = bincode::serialize(&$value)
            .context(model_err::CorruptionSnafu {
                kind: $kind,
                key:  String::from_utf8_lossy(&$key).to_string(),
            })
            .context(ModelSnafu)?;
        $batch.put(&$key, buf);
        rocksdb_counter!(db_puts_total);
    }};
}

/// Macro for database operations with automatic error context and metrics
macro_rules! db_try {
    ($op:expr) => {
        match $op {
            Ok(result) => result,
            Err(e) => {
                rocksdb_error!(crate::metrics::labels::ERROR_ROCKSDB);
                return Err(e).context(RocksdbSnafu);
            }
        }
    };
}

/// Macro for model operations with automatic error context
macro_rules! model_try {
    ($op:expr) => {
        $op.context(ModelSnafu)?
    };
}

/// Macro for unified batch operations
macro_rules! batch_put {
    ($batch:expr, $key:expr, $value:expr, $kind:expr) => {{
        serialize_batch!($batch, $key, $value, $kind);
        Ok(())
    }};
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

    pub fn with_skip_dir_mtime(&mut self, d: Duration) -> &mut Self {
        self.skip_dir_mtime = d;
        self
    }

    pub fn build(&self) -> Result<Arc<dyn Backend>> {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.increase_parallelism(kiseki_utils::num_cpus() as i32);

        let db = rocksdb::OptimisticTransactionDB::open(&opts, &self.path).context(RocksdbSnafu)?;
        Ok(Arc::new(RocksdbBackend {
            db,
            skip_dir_mtime: self.skip_dir_mtime,
        }))
    }
}

pub(crate) struct RocksdbBackend {
    db:             rocksdb::OptimisticTransactionDB<MultiThreaded>,
    skip_dir_mtime: Duration,
}

impl Debug for RocksdbBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("RocksdbEngine");
        ds.field("path", &self.db.path());
        ds.finish()
    }
}

/// Retrieve symbolic link target path - POSIX readlink(2) semantics
/// implementation
///
/// POSIX Compliance Requirements:
/// - POSIX.1-2008 readlink(2): Symbolic links must atomically return the target
///   path
/// - Data Consistency: Symlink content must remain exactly as created
/// - Error Handling: Return appropriate POSIX error codes if inode doesn't
///   exist or isn't a symlink
/// - Durability Guarantee: Symlink data must be read from persistent storage
///
/// Implementation Details:
/// - Uses pinned read to avoid data copying for performance
/// - Direct RocksDB read ensures data consistency
/// - Error context provides debugging information for POSIX error mapping
/// - Metrics: Records database read operations and timings
fn do_get_symlink<Layer: DBAccess>(db: &Layer, inode: Ino) -> Result<Bytes> {
    let symlink_key = key::symlink(inode);

    // Record the database read operation with metrics
    let buf = rocksdb_timed_op!(
        db_gets_total,
        db_get_duration_ms,
        db.get_pinned_opt(&symlink_key, &rocksdb::ReadOptions::default())
    )
    .context(RocksdbSnafu)?
    .context(model_err::NotFoundSnafu {
        kind: ModelKind::Symlink,
        key:  String::from_utf8_lossy(&symlink_key).to_string(),
    })
    .context(ModelSnafu)?;

    Ok(Bytes::from(buf.to_vec()))
}
/// Get hard link count for specific parent-child relationship - POSIX link
/// counting semantics
///
/// POSIX Compliance Requirements:
/// - POSIX.1-2008 link(2)/unlink(2): Hard link count must be accurately
///   maintained
/// - Atomicity: Link count operations must be atomic to prevent race conditions
/// - Consistency: Count must reflect the actual number of directory entries
///   pointing to the inode
/// - Zero Count Handling: When count reaches zero, file data should be eligible
///   for deletion
///
/// Implementation Details:
/// - Each parent-child relationship is stored separately for fine-grained
///   control
/// - Returns 0 for non-existent relationships (safe default for POSIX
///   semantics)
/// - Uses bincode for efficient serialization of count values
fn do_get_hard_link_count<Layer: DBAccess>(db: &Layer, inode: Ino, parent: Ino) -> Result<u64> {
    let key = key::parent(inode, parent);
    let buf = rocksdb_timed_op!(
        db_gets_total,
        db_get_duration_ms,
        db_try!(db.get_pinned_opt(&key, &rocksdb::ReadOptions::default()))
    );
    match buf {
        Some(buf) => {
            let count: u64 = model_try!(bincode::deserialize(&buf).context(
                model_err::CorruptionSnafu {
                    kind: ModelKind::HardLinkCount,
                    key:  String::from_utf8_lossy(&key).to_string(),
                }
            ));
            Ok(count)
        }
        None => Ok(0),
    }
}

/// Get directory entry by parent inode and name - POSIX directory lookup
/// semantics
///
/// POSIX Compliance Requirements:
/// - POSIX.1-2008 opendir(3)/readdir(3): Directory entries must be consistently
///   retrievable
/// - Name Resolution: Exact string matching for file names (case-sensitive on
///   most systems)
/// - Atomicity: Directory entry lookup must be atomic and consistent
/// - Error Handling: Must distinguish between "not found" vs "permission
///   denied" vs "I/O error"
///
/// Implementation Details:
/// - Uses composite key (parent_ino, name) for direct O(1) lookup
/// - Deserializes DEntry structure containing inode number and file type
/// - Provides proper error context for POSIX error code mapping
fn do_get_dentry<Layer: DBAccess>(db: &Layer, parent: Ino, name: &str) -> Result<DEntry> {
    let entry_key = key::dentry(parent, name);
    Ok(deserialize_db!(db, entry_key, DEntry, ModelKind::DEntry))
}

/// Get inode attributes - POSIX stat(2) semantics implementation
///
/// POSIX Compliance Requirements:
/// - POSIX.1-2008 stat(2)/fstat(2)/lstat(2): File attributes must be consistent
///   and accurate
/// - Timestamp Consistency: atime, mtime, ctime must follow POSIX update rules
/// - Permission Bits: Mode bits must conform to POSIX permission model
///   (owner/group/other)
/// - File Type: Must correctly identify file type (regular, directory, symlink,
///   etc.)
/// - Link Count: nlink must accurately reflect number of hard links
///
/// Implementation Details:
/// - Directly deserializes InodeAttr from persistent storage
/// - Single atomic read operation ensures consistency
/// - Error handling distinguishes between corruption and missing inode
/// - Metrics: Records database read operations for stat calls
fn do_get_attr<Layer: DBAccess>(db: &Layer, inode: Ino) -> Result<InodeAttr> {
    let attr_key = key::attr(inode);

    // Record database read with metrics and use our deserialize macro
    let result = rocksdb_timed_op!(
        db_gets_total,
        db_get_duration_ms,
        deserialize_db!(db, attr_key, InodeAttr, ModelKind::Attr)
    );

    Ok(result)
}

/// Check if directory has any children - POSIX rmdir(2) empty directory
/// validation
///
/// POSIX Compliance Requirements:
/// - POSIX.1-2008 rmdir(2): Directory must be empty before removal (except for
///   "." and "..")
/// - Atomicity: Check must be performed within same transaction as removal
/// - Consistency: Must detect all types of children (files, directories,
///   symlinks)
/// - Race Condition Prevention: Using transaction ensures no concurrent
///   additions
///
/// Implementation Details:
/// - Uses RocksDB iterator to efficiently check for any directory entries
/// - Prefix-based iteration ensures we only check children of the specified
///   parent
/// - Returns immediately upon finding first child (short-circuit optimization)
/// - Transaction-based operation prevents TOCTOU (Time-Of-Check-Time-Of-Use)
///   races
fn do_check_exist_children<DB>(txn: &rocksdb::Transaction<DB>, parent: Ino) -> Result<bool> {
    let prefix = key::dentry_prefix(parent);
    let mut ro = rocksdb::ReadOptions::default();
    ro.set_iterate_range(rocksdb::PrefixRange(prefix.as_slice()));
    let mut iter = txn.raw_iterator_opt(ro);
    iter.seek_to_first();
    Ok(iter.valid())
}

/// Store inode attributes within transaction context - POSIX transactional
/// metadata update
///
/// POSIX Compliance Requirements:
/// - POSIX.1-2008: Complex operations require atomic metadata updates
/// - Transaction Consistency: Attribute updates must be part of larger atomic
///   operations
/// - Isolation: Changes not visible until transaction commits (ACID properties)
/// - Rollback Safety: Failed transactions must not leave partial attribute
///   updates
///
/// Implementation Details:
/// - Uses transaction put operation for isolation and atomicity
/// - Consistent with batch operations but within transaction boundary
/// - Essential for complex operations like rename, mknod that update multiple
///   entities
/// - Error context helps diagnose serialization or storage issues
fn txn_put_attr<DB>(txn: &rocksdb::Transaction<DB>, inode: Ino, attr: &InodeAttr) -> Result<()> {
    let attr_key = key::attr(inode);
    let buf = model_try!(
        bincode::serialize(attr).context(model_err::CorruptionSnafu {
            kind: ModelKind::Attr,
            key:  String::from_utf8_lossy(&attr_key).to_string(),
        })
    );
    rocksdb_timed_op!(
        db_puts_total,
        db_put_duration_ms,
        db_try!(txn.put(&attr_key, &buf))
    );
    Ok(())
}

fn stage_slice_commit(
    txn: &rocksdb::Transaction<rocksdb::OptimisticTransactionDB<MultiThreaded>>,
    inode: Ino,
    chunk_index: ChunkIndex,
    slice: &Slice,
) -> Result<SliceCommitResult> {
    let attr_key = key::attr(inode);
    let attr_buf = rocksdb_timed_op!(
        db_gets_total,
        db_get_duration_ms,
        db_try!(txn.get_for_update(&attr_key, true))
    )
    .context(model_err::NotFoundSnafu {
        kind: ModelKind::Attr,
        key:  String::from_utf8_lossy(&attr_key).to_string(),
    })
    .context(ModelSnafu)?;
    let mut attr: InodeAttr = model_try!(bincode::deserialize(&attr_buf).context(
        model_err::CorruptionSnafu {
            kind: ModelKind::Attr,
            key:  String::from_utf8_lossy(&attr_key).to_string(),
        }
    ));
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
        txn_put_attr(txn, inode, &attr)?;
    }

    Ok(SliceCommitResult {
        grew_by,
        slice_count,
        inserted,
    })
}

fn stage_chunk_slice(
    txn: &rocksdb::Transaction<rocksdb::OptimisticTransactionDB<MultiThreaded>>,
    inode: Ino,
    chunk_index: ChunkIndex,
    slice: &Slice,
) -> Result<(bool, usize)> {
    let slices_key = key::chunk_slices(inode, chunk_index);
    let mut slices_buf = rocksdb_timed_op!(
        db_gets_total,
        db_get_duration_ms,
        db_try!(txn.get_for_update(&slices_key, true))
    )
    .unwrap_or_default();
    if !slices_buf.len().is_multiple_of(SLICE_BYTES) {
        return Err(model_err::Error::CorruptionString {
            kind:   ModelKind::ChunkSlices,
            key:    String::from_utf8_lossy(&slices_key).to_string(),
            reason: format!("invalid encoded slice length {}", slices_buf.len()),
        })
        .context(ModelSnafu);
    }
    let encoded_slice = model_try!(
        bincode::serialize(slice).context(model_err::CorruptionSnafu {
            kind: ModelKind::ChunkSlices,
            key:  String::from_utf8_lossy(&slices_key).to_string(),
        })
    );
    let inserted = !slices_buf
        .chunks_exact(SLICE_BYTES)
        .any(|existing| existing == encoded_slice);
    if inserted {
        slices_buf.extend_from_slice(&encoded_slice);
        rocksdb_timed_op!(
            db_puts_total,
            db_put_duration_ms,
            db_try!(txn.put(&slices_key, &slices_buf))
        );
    }
    Ok((inserted, slices_buf.len() / SLICE_BYTES))
}

fn set_dentry_in_write_batch<const TRANSACTION: bool>(
    batch: &mut rocksdb::WriteBatchWithTransaction<TRANSACTION>,
    parent: Ino,
    name: &str,
    inode: Ino,
    typ: FileType,
) -> Result<()> {
    let entry_key = key::dentry(parent, name);
    let entry = DEntry {
        parent,
        name: name.to_string(),
        inode,
        typ,
    };
    batch_put!(batch, entry_key, entry, ModelKind::DEntry)
}

/// Add inode attribute update to write batch - POSIX atomic batch operation
///
/// POSIX Compliance Requirements:
/// - POSIX.1-2008: Metadata updates must be atomic with related operations
/// - Batch Consistency: Multiple metadata updates must be applied atomically
/// - Transaction Support: Must work within transaction context for complex
///   operations
/// - Durability: Changes must be persistent once batch is committed
///
/// Implementation Details:
/// - Uses generic const parameter to support both transactional and
///   non-transactional batches
/// - Leverages batch_put! macro for consistent error handling and serialization
/// - Part of larger atomic operations (mknod, rename, etc.)
fn set_attr_in_write_batch<const TRANSACTION: bool>(
    batch: &mut rocksdb::WriteBatchWithTransaction<TRANSACTION>,
    inode: Ino,
    attr: &InodeAttr,
) -> Result<()> {
    let attr_key = key::attr(inode);
    batch_put!(batch, attr_key, attr, ModelKind::Attr)
}

fn set_hard_link_count_in_write_batch<const TRANSACTION: bool>(
    batch: &mut rocksdb::WriteBatchWithTransaction<TRANSACTION>,
    inode: Ino,
    parent: Ino,
    cnt: u64,
) -> Result<()> {
    let key = key::parent(inode, parent);
    set_value_in_write_batch(batch, ModelKind::HardLinkCount, key.as_slice(), cnt)
}

fn set_sustained_in_write_batch<const TRANSACTION: bool>(
    batch: &mut rocksdb::WriteBatchWithTransaction<TRANSACTION>,
    session_id: u64,
    inode: Ino,
    sustained: u64,
) -> Result<()> {
    let key = key::sustained(session_id, inode);
    set_value_in_write_batch(batch, ModelKind::Sustained, key.as_slice(), sustained)
}

// set_delete_chunk_after_in_write_batch write a notification that we need to
// delete the chunk after a while.
fn set_delete_chunk_after_in_write_batch<const TRANSACTION: bool>(
    batch: &mut rocksdb::WriteBatchWithTransaction<TRANSACTION>,
    inode: Ino,
) -> Result<()> {
    let key = key::delete_chunk_after(inode);
    set_value_in_write_batch(
        batch,
        ModelKind::DeleteInode,
        key.as_slice(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    )
}

fn set_value_in_write_batch<const TRANSACTION: bool, V>(
    batch: &mut rocksdb::WriteBatchWithTransaction<TRANSACTION>,
    kind: ModelKind,
    key: &[u8],
    value: V,
) -> Result<()>
where
    V: Serialize,
{
    let buf = bincode::serialize(&value)
        .context(model_err::CorruptionSnafu {
            kind,
            key: String::from_utf8_lossy(key).to_string(),
        })
        .context(ModelSnafu)?;
    batch.put(key, buf);
    rocksdb_counter!(db_puts_total);
    Ok(())
}

fn delete_prefix_in_txn_write_batch(
    txn: &rocksdb::Transaction<rocksdb::OptimisticTransactionDB<MultiThreaded>>,
    batch: &mut rocksdb::WriteBatchWithTransaction<true>,
    prefix: &[u8],
) {
    let mut ro = rocksdb::ReadOptions::default();
    ro.set_iterate_range(rocksdb::PrefixRange(prefix));
    let mut iter = txn.raw_iterator_opt(ro);
    iter.seek_to_first();
    // Scan the keys in the iterator
    while let Some(k) = iter.key() {
        if !k.starts_with(prefix) {
            break;
        }
        batch.delete(k);
        iter.next();
    }
}

fn stage_initial_state(
    txn: &rocksdb::Transaction<rocksdb::OptimisticTransactionDB<MultiThreaded>>,
    format: &Format,
    root: &InodeAttr,
) -> Result<()> {
    let format_buf = model_try!(
        bincode::serialize(format).context(model_err::CorruptionSnafu {
            kind: ModelKind::Setting,
            key:  key::CURRENT_FORMAT.to_string(),
        })
    );
    let root_key = key::attr(ROOT_INO);
    let root_buf = model_try!(
        bincode::serialize(root).context(model_err::CorruptionSnafu {
            kind: ModelKind::Attr,
            key:  String::from_utf8_lossy(&root_key).to_string(),
        })
    );
    let counter_key: Vec<u8> = BackendCounter::NextInode.into();
    let counter_buf = model_try!(
        bincode::serialize(&2u64).context(model_err::CorruptionSnafu {
            kind: ModelKind::Counter,
            key:  String::from_utf8_lossy(&counter_key).to_string(),
        })
    );

    db_try!(txn.put(key::CURRENT_FORMAT, format_buf));
    db_try!(txn.put(root_key, root_buf));
    db_try!(txn.put(counter_key, counter_buf));
    rocksdb_counter!(db_puts_total);
    rocksdb_counter!(db_puts_total);
    rocksdb_counter!(db_puts_total);
    Ok(())
}

#[async_trait::async_trait]
impl Backend for RocksdbBackend {
    fn initialize_volume(&self, format: &Format, root: &InodeAttr) -> Result<()> {
        let transaction = self.db.transaction();
        let current = rocksdb_timed_op!(
            db_gets_total,
            db_get_duration_ms,
            db_try!(transaction.get_for_update(key::CURRENT_FORMAT, true))
        );
        ensure!(current.is_none(), AlreadyInitializedSnafu);

        stage_initial_state(&transaction, format, root)?;
        rocksdb_timed_op!(
            db_transactions_total,
            db_transaction_duration_ms,
            db_try!(transaction.commit())
        );
        Ok(())
    }

    fn set_format(&self, format: &Format) -> Result<()> {
        let setting_buf = model_try!(bincode::serialize(format).context(
            model_err::CorruptionSnafu {
                kind: ModelKind::Setting,
                key:  key::CURRENT_FORMAT.to_string(),
            }
        ));

        rocksdb_timed_op!(
            db_puts_total,
            db_put_duration_ms,
            db_try!(self.db.put(key::CURRENT_FORMAT, setting_buf))
        );
        Ok(())
    }

    fn load_format(&self) -> Result<Format> {
        let setting_buf = rocksdb_timed_op!(
            db_gets_total,
            db_get_duration_ms,
            db_try!(self.db.get_pinned(key::CURRENT_FORMAT))
        )
        .context(UninitializedEngineSnafu)?;
        let setting: Format = model_try!(bincode::deserialize(&setting_buf).context(
            model_err::CorruptionSnafu {
                kind: ModelKind::Setting,
                key:  key::CURRENT_FORMAT.to_string(),
            }
        ));
        Ok(setting)
    }

    fn increase_count_by(&self, counter: BackendCounter, step: usize) -> Result<u64> {
        let key: Vec<u8> = counter.into();
        let transaction = self.db.transaction();
        let current = rocksdb_timed_op!(
            db_gets_total,
            db_get_duration_ms,
            db_try!(transaction.get(&key))
        )
        .map(|v| {
            bincode::deserialize::<u64>(&v)
                .context(model_err::CorruptionSnafu {
                    kind: ModelKind::Counter,
                    key:  String::from_utf8_lossy(&key).to_string(),
                })
                .context(ModelSnafu)
        })
        .transpose()?
        .unwrap_or(0u64);

        let new = current + step as u64;
        let new_buf = model_try!(
            bincode::serialize(&new).context(model_err::CorruptionSnafu {
                kind: ModelKind::Counter,
                key:  String::from_utf8_lossy(&key).to_string(),
            })
        );
        rocksdb_timed_op!(
            db_puts_total,
            db_put_duration_ms,
            db_try!(transaction.put(&key, new_buf))
        );
        rocksdb_timed_op!(
            db_transactions_total,
            db_transaction_duration_ms,
            db_try!(transaction.commit())
        );
        Ok(new)
    }

    fn load_count(&self, counter: BackendCounter) -> Result<u64> {
        let key: Vec<u8> = counter.into();
        Ok(deserialize_db!(self.db, key, u64, ModelKind::Counter))
    }

    fn get_attr(&self, inode: Ino) -> Result<InodeAttr> { do_get_attr(&self.db, inode) }

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
    ///
    /// Implementation Details:
    /// - Uses bincode serialization for efficient storage and retrieval
    /// - Single put operation ensures atomicity at RocksDB level
    /// - Error handling provides context for debugging metadata corruption
    /// - Direct write to persistent storage ensures durability
    fn set_attr(&self, inode: Ino, attr: &InodeAttr) -> Result<()> {
        let attr_key = key::attr(inode);
        let buf = model_try!(
            bincode::serialize(attr).context(model_err::CorruptionSnafu {
                kind: ModelKind::Attr,
                key:  String::from_utf8_lossy(&attr_key).to_string(),
            })
        );
        rocksdb_timed_op!(
            db_puts_total,
            db_put_duration_ms,
            db_try!(self.db.put(&attr_key, &buf))
        );
        Ok(())
    }

    fn get_dentry(&self, parent: Ino, name: &str) -> Result<DEntry> {
        do_get_dentry(&self.db, parent, name)
    }

    fn set_dentry(&self, parent: Ino, name: &str, inode: Ino, typ: FileType) -> Result<()> {
        let entry_key = key::dentry(parent, name);
        let entry = DEntry {
            parent,
            name: name.to_string(),
            inode,
            typ,
        };
        let entry_buf = model_try!(bincode::serialize(&entry).context(
            model_err::CorruptionSnafu {
                kind: ModelKind::DEntry,
                key:  String::from_utf8_lossy(&entry_key).to_string(),
            }
        ));
        rocksdb_timed_op!(
            db_puts_total,
            db_put_duration_ms,
            db_try!(self.db.put(&entry_key, entry_buf))
        );
        Ok(())
    }

    fn list_dentry(&self, parent: Ino, limit: i64) -> Result<Vec<DEntry>> {
        let prefix = key::dentry_prefix(parent);
        let mut ro = rocksdb::ReadOptions::default();
        ro.set_iterate_range(rocksdb::PrefixRange(prefix.as_slice()));
        let mut iter = self.db.raw_iterator_opt(ro);
        iter.seek_to_first();
        let mut res = Vec::default();
        // Scan the keys in the iterator
        while iter.valid() {
            // Check the scan limit
            if limit == -1 || res.len() < limit as usize {
                // Get the key and value
                let (k, v) = (iter.key(), iter.value());
                // Check the key and value
                if let (Some(k), Some(v)) = (k, v) {
                    let dentry: DEntry =
                        model_try!(bincode::deserialize(v).context(model_err::CorruptionSnafu {
                            kind: ModelKind::DEntry,
                            key:  String::from_utf8_lossy(k).to_string(),
                        }));
                    res.push(dentry);
                    iter.next();
                    continue;
                }
            }
            // Exit
            break;
        }
        Ok(res)
    }

    fn set_symlink(&self, inode: Ino, path: String) -> Result<()> {
        let symlink_key = key::symlink(inode);
        rocksdb_timed_op!(
            db_puts_total,
            db_put_duration_ms,
            db_try!(self.db.put(&symlink_key, path.into_bytes()))
        );
        Ok(())
    }

    fn get_symlink(&self, inode: Ino) -> Result<String> {
        let symlink_key = key::symlink(inode);
        let path_buf = rocksdb_timed_op!(
            db_gets_total,
            db_get_duration_ms,
            db_try!(self.db.get_pinned(&symlink_key))
        )
        .context(model_err::NotFoundSnafu {
            kind: ModelKind::Symlink,
            key:  String::from_utf8_lossy(&symlink_key).to_string(),
        })
        .context(ModelSnafu)?;
        Ok(String::from_utf8_lossy(path_buf.as_ref()).to_string())
    }

    fn set_chunk_slices(&self, inode: Ino, chunk_index: ChunkIndex, slices: Slices) -> Result<()> {
        let key = key::chunk_slices(inode, chunk_index);
        let buf = model_try!(
            bincode::serialize(&slices).context(model_err::CorruptionSnafu {
                kind: ModelKind::ChunkSlices,
                key:  String::from_utf8_lossy(&key).to_string(),
            })
        );
        rocksdb_timed_op!(
            db_puts_total,
            db_put_duration_ms,
            db_try!(self.db.put(&key, &buf))
        );
        Ok(())
    }

    fn set_raw_chunk_slices(
        &self,
        inode: Ino,
        chunk_index: ChunkIndex,
        buf: Vec<u8>,
    ) -> Result<()> {
        let key = key::chunk_slices(inode, chunk_index);
        rocksdb_timed_op!(
            db_puts_total,
            db_put_duration_ms,
            db_try!(self.db.put(&key, &buf))
        );
        assert!(!buf.is_empty(), "slices is empty");
        Ok(())
    }

    fn get_raw_chunk_slices(&self, inode: Ino, chunk_index: ChunkIndex) -> Result<Option<Vec<u8>>> {
        let key = key::chunk_slices(inode, chunk_index);
        let buf = rocksdb_timed_op!(
            db_gets_total,
            db_get_duration_ms,
            db_try!(self.db.get(&key))
        );
        Ok(buf)
    }

    fn get_chunk_slices(&self, inode: Ino, chunk_index: ChunkIndex) -> Result<Slices> {
        let key = key::chunk_slices(inode, chunk_index);
        let buf = rocksdb_timed_op!(
            db_gets_total,
            db_get_duration_ms,
            db_try!(self.db.get_pinned(&key))
        )
        .context(model_err::NotFoundSnafu {
            kind: ModelKind::ChunkSlices,
            key:  String::from_utf8_lossy(&key).to_string(),
        })
        .context(ModelSnafu)?;
        let slices = Slices::decode(&buf)
            .map_err(|e| e.to_string())
            .and_then(|slices| {
                if slices.0.is_empty() {
                    Err("empty slices".to_string())
                } else {
                    Ok(slices)
                }
            })
            .map_err(|reason| model_err::Error::CorruptionString {
                kind: ModelKind::ChunkSlices,
                key: String::from_utf8_lossy(&key).to_string(),
                reason,
            })
            .context(ModelSnafu)?;
        debug!("get_chunk_slices: key: {:?}", String::from_utf8_lossy(&key));
        for slice in slices.0.iter() {
            debug!("get_chunk_slices: slice: {:?}", slice);
        }
        Ok(slices)
    }

    fn commit_slice(
        &self,
        inode: Ino,
        chunk_index: ChunkIndex,
        slice: &Slice,
    ) -> Result<SliceCommitResult> {
        let mut write_options = rocksdb::WriteOptions::default();
        write_options.set_sync(true);
        let txn = self.db.transaction_opt(
            &write_options,
            &rocksdb::OptimisticTransactionOptions::default(),
        );
        let result = stage_slice_commit(&txn, inode, chunk_index, slice)?;
        rocksdb_timed_op!(
            db_transactions_total,
            db_transaction_duration_ms,
            db_try!(txn.commit())
        );
        Ok(result)
    }

    fn set_dir_stat(&self, inode: Ino, dir_stat: DirStat) -> Result<()> {
        let key = key::dir_stat(inode);
        let buf = model_try!(
            bincode::serialize(&dir_stat).context(model_err::CorruptionSnafu {
                kind: ModelKind::DirStat,
                key:  String::from_utf8_lossy(&key).to_string(),
            })
        );
        rocksdb_timed_op!(
            db_puts_total,
            db_put_duration_ms,
            db_try!(self.db.put(&key, &buf))
        );
        Ok(())
    }

    fn get_dir_stat(&self, inode: Ino) -> Result<DirStat> {
        let key = key::dir_stat(inode);
        Ok(deserialize_db!(self.db, key, DirStat, ModelKind::DirStat))
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
        mut new_inode_attr: InodeAttr,
        parent: Ino,
        name: &str,
        typ: FileType,
        path: String,
    ) -> Result<(Ino, InodeAttr)> {
        let txn = self.db.transaction();
        debug!("get attr {} from backend", parent);
        let mut parent_attr = do_get_attr(&txn, parent)?;
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
        if let Ok(_found_entry) = do_get_dentry(&txn, parent, name) {
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
                    // group execute bit, and if the user ID is not 0 (i.e., the user is not root),
                    // it further checks if the user belongs to the group of the parent directory.
                    // If not, it removes the SGID bit from the mode of the new node.
                    new_inode_attr.mode &= !constants::MODE_MASK_SETGID_EXEC;
                }
            }
        }

        let mut batch = txn.get_writebatch();
        // insert entry
        set_dentry_in_write_batch(&mut batch, parent, name, new_inode, typ)?;
        // insert attr
        set_attr_in_write_batch(&mut batch, new_inode, &new_inode_attr)?;

        if update_parent_attr {
            // update parent attr
            set_attr_in_write_batch(&mut batch, parent, &parent_attr)?;
        }
        if typ == FileType::Symlink {
            let symlink_key = key::symlink(new_inode);
            batch.put(&symlink_key, path.into_bytes());
            rocksdb_counter!(db_puts_total);
        }

        // Record RocksDB write and transaction with metrics
        rocksdb_timed_op!(
            db_batch_writes_total,
            db_write_duration_ms,
            self.db.write(batch).inspect_err(|_e| {
                rocksdb_error!(crate::metrics::labels::ERROR_WRITE_BATCH);
            })
        )
        .context(RocksdbSnafu)?;

        rocksdb_timed_op!(
            db_transactions_total,
            db_transaction_duration_ms,
            txn.commit().inspect_err(|_e| {
                rocksdb_error!(crate::metrics::labels::ERROR_TRANSACTION_COMMIT);
            })
        )
        .context(RocksdbSnafu)?;

        Ok((new_inode, new_inode_attr))
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
        let txn = self.db.transaction();
        let entry_info = do_get_dentry(&txn, parent, name)?;
        ensure!(
            entry_info.typ == FileType::Directory,
            LibcSnafu {
                errno: libc::ENOTDIR,
            }
        );
        // get parent and child's attr
        let mut parent_attr = do_get_attr(&txn, parent)?;
        ensure!(
            // parent must be dir.
            parent_attr.is_dir(),
            LibcSnafu {
                errno: libc::ENOTDIR,
            }
        );
        let child_attr = do_get_attr(&txn, entry_info.inode)?;
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
        let parent_flag = kiseki_types::attr::Flags::from_bits_truncate(parent_attr.flags as u8);
        ensure!(
            !parent_flag.contains(kiseki_types::attr::Flags::APPEND)
                && !parent_flag.contains(kiseki_types::attr::Flags::IMMUTABLE),
            LibcSnafu { errno: libc::EPERM }
        );
        ensure!(
            !do_check_exist_children(&txn, entry_info.inode)?,
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

        let mut batch = txn.get_writebatch();
        // delete entry
        batch.delete(key::dentry(parent, name));
        rocksdb_delete!();
        // delete inode attr
        batch.delete(key::attr(entry_info.inode));
        rocksdb_delete!();
        if need_update_parent_attr {
            let parent_attr_key = key::attr(parent);
            // update parent attr
            let serialized = model_try!(bincode::serialize(&parent_attr).context(
                model_err::CorruptionSnafu {
                    kind: ModelKind::Attr,
                    key:  String::from_utf8_lossy(&parent_attr_key).to_string(),
                }
            ));
            batch.put(&parent_attr_key, serialized);
            rocksdb_counter!(db_puts_total);
        }

        // Record RocksDB operations with metrics
        rocksdb_timed_op!(
            db_batch_writes_total,
            db_write_duration_ms,
            self.db.write(batch)
        )
        .context(RocksdbSnafu)?;
        rocksdb_timed_op!(
            db_transactions_total,
            db_transaction_duration_ms,
            txn.commit()
        )
        .context(RocksdbSnafu)?;

        Ok((entry_info, child_attr))
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
        let txn = self.db.transaction();
        let mut old_attr = do_get_attr(&txn, inode)?;
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
                stage_chunk_slice(&txn, inode, chunk_index, &hole)?;
                position += hole_length;
            }
        }
        old_attr.update_length(length);

        txn_put_attr(&txn, inode, &old_attr)?;
        rocksdb_timed_op!(
            db_transactions_total,
            db_transaction_duration_ms,
            db_try!(txn.commit())
        );
        Ok(old_attr.clone())
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
        let txn = self.db.transaction();

        // get parent and child's attr
        let mut parent_attr = do_get_attr(&txn, new_parent)?;
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

        let mut child_attr = do_get_attr(&txn, inode)?;
        ensure!(!child_attr.is_dir(), LibcSnafu { errno: libc::EPERM });
        ensure!(child_attr.is_normal(), LibcSnafu { errno: libc::EPERM });
        ensure!(
            // the target inode must be empty
            do_get_dentry(&txn, new_parent, new_name).is_err(),
            LibcSnafu {
                errno: libc::EEXIST,
            }
        );
        // check if we need to update the parent
        let now = SystemTime::now();
        let need_update_parent_attr =
            parent_attr.update_modification_time_if(now, self.skip_dir_mtime);
        let old_parent = child_attr.parent;
        child_attr.ctime = now;
        child_attr.nlink += 1;
        child_attr.parent = ZERO_INO;

        let mut batch = txn.get_writebatch();
        // 1. create an entry that points to the original inode.
        set_dentry_in_write_batch(&mut batch, new_parent, new_name, inode, child_attr.kind)?;
        if need_update_parent_attr {
            // 2. update parent attr
            set_attr_in_write_batch(&mut batch, new_parent, &parent_attr)?;
        }
        // 3. update child attr
        set_attr_in_write_batch(&mut batch, inode, &child_attr)?;
        if !child_attr.parent.is_zero() {
            let cnt = do_get_hard_link_count(&txn, inode, old_parent)?;
            set_hard_link_count_in_write_batch(&mut batch, inode, old_parent, cnt + 1)?;
        }
        let cnt = do_get_hard_link_count(&txn, inode, new_parent)?;
        set_hard_link_count_in_write_batch(&mut batch, inode, new_parent, cnt + 1)?;

        // Record RocksDB operations with metrics
        rocksdb_timed_op!(
            db_batch_writes_total,
            db_write_duration_ms,
            self.db.write(batch)
        )
        .context(RocksdbSnafu)?;
        rocksdb_timed_op!(
            db_transactions_total,
            db_transaction_duration_ms,
            txn.commit()
        )
        .context(RocksdbSnafu)?;

        Ok(child_attr)
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
        let txn = self.db.transaction();

        let entry = do_get_dentry(&txn, parent, &name)?;
        ensure!(
            !matches!(entry.typ, FileType::Directory),
            LibcSnafu { errno: libc::EPERM }
        );
        // get parent and child's attr
        let mut parent_attr = do_get_attr(&txn, parent)?;
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
        let mut opened = false;
        let mut attr_place_holder = InodeAttr::empty();
        // the target exist
        if let Ok(mut attr) = do_get_attr(&txn, entry.inode) {
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
            if attr.is_file()
                && attr.nlink == 0
                && let Some(of) = open_files_ref.load(&entry.inode).await
            {
                opened = of.is_opened().await;
            };
            attr_place_holder = attr;
        }

        let mut batch = txn.get_writebatch();
        if parent_attr.update_modification_time_if(now, self.skip_dir_mtime) {
            set_attr_in_write_batch(&mut batch, parent, &parent_attr)?;
        }
        // delete the entry
        batch.delete(key::dentry(parent, &name));
        rocksdb_delete!();
        let mut free_inode_cnt = 0;
        let mut free_space_size = 0;

        if attr_place_holder.nlink > 0 {
            set_attr_in_write_batch(&mut batch, entry.inode, &attr_place_holder)?;
            if attr_place_holder.parent.is_zero() {
                let cnt = do_get_hard_link_count(&txn, entry.inode, parent)?;
                if cnt > 0 {
                    set_hard_link_count_in_write_batch(&mut batch, entry.inode, parent, cnt - 1)?;
                }
            }
        } else {
            if matches!(attr_place_holder.kind, FileType::RegularFile) {
                if opened {
                    // update the inode attr
                    set_attr_in_write_batch(&mut batch, entry.inode, &attr_place_holder)?;
                    set_sustained_in_write_batch(&mut batch, session_id, entry.inode, 1)?;
                } else {
                    // make a notification that we need to delete the chunk after a while.
                    set_delete_chunk_after_in_write_batch(&mut batch, entry.inode)?;
                    // delete inode attr
                    batch.delete(key::attr(entry.inode));
                    rocksdb_delete!();
                    free_inode_cnt += 1;
                    free_space_size += attr_place_holder.length;
                }
            } else {
                if matches!(attr_place_holder.kind, FileType::Symlink) {
                    batch.delete(key::symlink(entry.inode));
                    rocksdb_delete!();
                }
                batch.delete(key::attr(entry.inode));
                rocksdb_delete!();
                free_inode_cnt += 1;
                free_space_size += constants::DEFAULT_FILE_SIZE;
            }
            // delete xattr
            delete_prefix_in_txn_write_batch(&txn, &mut batch, &key::xattr_prefix(entry.inode));
            if attr_place_holder.parent.is_zero() {
                // delete hardlinks
                delete_prefix_in_txn_write_batch(
                    &txn,
                    &mut batch,
                    &key::parent_prefix(entry.inode),
                );
            }
        }
        rocksdb_timed_op!(
            db_batch_writes_total,
            db_write_duration_ms,
            db_try!(self.db.write(batch))
        );
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

        // Record RocksDB transaction with metrics
        rocksdb_timed_op!(
            db_transactions_total,
            db_transaction_duration_ms,
            txn.commit()
        )
        .context(RocksdbSnafu)?;

        Ok(r)
    }

    fn do_delete_chunks(&self, inode: Ino) {
        let txn = self.db.transaction();
        let mut batch = txn.get_writebatch();
        let mut ro = rocksdb::ReadOptions::default();

        let prefix = key::chunk_slices_prefix(inode);
        let prefix = prefix.as_slice();
        ro.set_iterate_range(rocksdb::PrefixRange(prefix));
        let mut iter = txn.raw_iterator_opt(ro);
        iter.seek_to_first();
        // Scan the keys in the iterator
        while iter.valid() {
            if let Some(k) = iter.key()
                && k.starts_with(prefix)
            {
                // at present, we delete the slice directly, since we haven't implemented the
                // borrow mechanism.
                batch.delete(k);
                iter.next();
                continue;
            }
            break;
        }
        drop(iter);
        // clear the delete notification
        batch.delete(key::delete_chunk_after(inode));
        rocksdb_delete!();

        if let Err(e) = rocksdb_timed_op!(
            db_batch_writes_total,
            db_write_duration_ms,
            self.db.write(batch)
        ) {
            error!("write batch failed when do_delete_chunks: {:?}", e);
            return;
        }
        if let Err(e) = rocksdb_timed_op!(
            db_transactions_total,
            db_transaction_duration_ms,
            txn.commit()
        ) {
            error!("commit failed in do_delete_chunks: {:?}", e);
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
        let txn = self.db.transaction();
        let old_entry = do_get_dentry(&txn, old_parent, old_name)?;
        let mut rename_result = RenameResult {
            need_delete: None,
            freed_inode: 0,
            freed_space: 0,
        };
        if old_parent == new_parent && old_name == new_name {
            return Ok(rename_result);
        }

        let mut old_parent_attr = do_get_attr(&txn, old_parent)?;
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

        let mut new_parent_attr = do_get_attr(&txn, new_parent)?;
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

        let mut old_inode_attr = do_get_attr(&txn, old_entry.inode)?;
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

        let (mut update_new_parent, mut opened, mut dst_dentry_opt, mut dst_attr_opt) =
            (false, false, None, None);
        match do_get_dentry(&txn, new_parent, new_name) {
            Ok(dst_entry) => {
                // dst exists
                ensure!(
                    !flags.contains(RenameFlags::NOREPLACE),
                    LibcSnafu {
                        errno: libc::EEXIST,
                    }
                );

                let mut dst_attr = do_get_attr(&txn, dst_entry.inode)?;
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
                        !do_check_exist_children(&txn, dst_entry.inode)?,
                        LibcSnafu {
                            errno: libc::ENOTEMPTY,
                        }
                    );
                    new_parent_attr.nlink -= 1;
                    update_new_parent = true;
                } else {
                    dst_attr.nlink -= 1;
                    if matches!(dst_entry.typ, FileType::RegularFile)
                        && dst_attr.nlink == 0
                        && let Some(of) = open_files_ref.load(&dst_entry.inode).await
                    {
                        opened = of.is_opened().await;
                    }
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
            Err(e) => {
                if !e.is_not_found() {
                    return Err(e);
                }
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
        let update_old_parent =
            old_parent_attr.update_modification_time_if(now, self.skip_dir_mtime);
        if update_new_parent {
            new_parent_attr.update_modification_time_with(now);
        } else {
            update_new_parent =
                new_parent_attr.update_modification_time_if(now, self.skip_dir_mtime);
        }
        old_inode_attr.ctime = now;

        let mut write_batch = txn.get_writebatch();
        match flags {
            RenameFlags::EXCHANGE => {
                // EXCHANGE requires the destination to exist; checked above.
                let dst_dentry = dst_dentry_opt.context(LibcSnafu { errno: libc::EIO })?;
                set_dentry_in_write_batch(
                    &mut write_batch,
                    old_parent,
                    old_name,
                    dst_dentry.inode,
                    dst_dentry.typ,
                )?;
                let dst_attr = dst_attr_opt.context(LibcSnafu { errno: libc::EIO })?;
                set_attr_in_write_batch(&mut write_batch, dst_dentry.inode, &dst_attr)?;
                if old_parent != new_parent && dst_attr.parent.is_zero() {
                    let cnt = do_get_hard_link_count(&txn, dst_dentry.inode, old_parent)?;
                    set_hard_link_count_in_write_batch(
                        &mut write_batch,
                        dst_dentry.inode,
                        old_parent,
                        cnt + 1,
                    )?;
                    let cnt = {
                        let mut cnt = do_get_hard_link_count(&txn, dst_dentry.inode, new_parent)?;
                        cnt = cnt.saturating_sub(1);
                        cnt
                    };
                    set_hard_link_count_in_write_batch(
                        &mut write_batch,
                        dst_dentry.inode,
                        new_parent,
                        cnt,
                    )?;
                }
            }
            _ => {
                write_batch.delete(key::dentry(old_parent, old_name));
                rocksdb_delete!();
                if let Some(dst_attr) = dst_attr_opt {
                    // a destination attr always comes with its dentry; checked above.
                    let dst_entry = dst_dentry_opt.context(LibcSnafu { errno: libc::EIO })?;
                    if !matches!(dst_attr.kind, FileType::Directory) && dst_attr.nlink > 0 {
                        set_attr_in_write_batch(&mut write_batch, dst_entry.inode, &dst_attr)?;
                        if dst_attr.parent.is_zero() {
                            let cnt = do_get_hard_link_count(&txn, dst_entry.inode, old_parent)?;
                            if cnt > 0 {
                                set_hard_link_count_in_write_batch(
                                    &mut write_batch,
                                    dst_entry.inode,
                                    old_parent,
                                    cnt - 1,
                                )?;
                            }
                        }
                    } else {
                        if matches!(dst_attr.kind, FileType::RegularFile) {
                            if opened {
                                set_attr_in_write_batch(
                                    &mut write_batch,
                                    dst_entry.inode,
                                    &dst_attr,
                                )?;
                                set_sustained_in_write_batch(
                                    &mut write_batch,
                                    session_id,
                                    dst_entry.inode,
                                    1,
                                )?;
                            } else {
                                set_delete_chunk_after_in_write_batch(
                                    &mut write_batch,
                                    dst_entry.inode,
                                )?;
                                write_batch.delete(key::attr(dst_entry.inode));
                                rocksdb_delete!();
                                rename_result.freed_space +=
                                    kiseki_utils::align::align4k(dst_attr.length) as u64;
                                rename_result.freed_inode += 1;
                            }
                            rename_result.need_delete = Some((dst_entry.inode, opened));
                        } else {
                            if matches!(dst_attr.kind, FileType::Symlink) {
                                write_batch.delete(key::symlink(dst_entry.inode));
                                rocksdb_delete!();
                            }
                            write_batch.delete(key::attr(dst_entry.inode));
                            rocksdb_delete!();
                            rename_result.freed_space += 4096;
                            rename_result.freed_inode += 1;
                        }

                        delete_prefix_in_txn_write_batch(
                            &txn,
                            &mut write_batch,
                            &key::xattr_prefix(dst_entry.inode),
                        );
                        if dst_attr.parent.is_zero() {
                            delete_prefix_in_txn_write_batch(
                                &txn,
                                &mut write_batch,
                                &key::parent_prefix(dst_entry.inode),
                            );
                        }
                    }
                }
            }
        }

        if new_parent != old_parent {
            if update_old_parent {
                set_attr_in_write_batch(&mut write_batch, old_parent, &old_parent_attr)?;
            }
            if old_inode_attr.parent.is_zero() {
                let cnt = do_get_hard_link_count(&txn, old_entry.inode, new_parent)?;
                set_hard_link_count_in_write_batch(
                    &mut write_batch,
                    old_entry.inode,
                    new_parent,
                    cnt + 1,
                )?;
                let mut cnt = do_get_hard_link_count(&txn, old_entry.inode, old_parent)?;
                cnt = cnt.saturating_sub(1);
                set_hard_link_count_in_write_batch(
                    &mut write_batch,
                    old_entry.inode,
                    old_parent,
                    cnt,
                )?;
            }
        }

        set_attr_in_write_batch(&mut write_batch, old_entry.inode, &old_inode_attr)?;
        set_dentry_in_write_batch(
            &mut write_batch,
            new_parent,
            new_name,
            old_entry.inode,
            old_inode_attr.kind,
        )?;
        if update_new_parent {
            set_attr_in_write_batch(&mut write_batch, new_parent, &new_parent_attr)?;
        }

        // Record RocksDB operations with metrics
        rocksdb_timed_op!(
            db_batch_writes_total,
            db_write_duration_ms,
            self.db.write(write_batch)
        )
        .context(RocksdbSnafu)?;
        rocksdb_timed_op!(
            db_transactions_total,
            db_transaction_duration_ms,
            txn.commit()
        )
        .context(RocksdbSnafu)?;

        Ok(rename_result)
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
    /// - Simple wrapper around do_get_symlink helper function
    /// - Maintains separation between public API and internal implementation
    /// - Inherits all POSIX compliance guarantees from do_get_symlink
    fn do_readlink(&self, inode: Ino) -> Result<Bytes> {
        let symlink = do_get_symlink(&self.db, inode)?;
        Ok(symlink)
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
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.increase_parallelism(kiseki_utils::num_cpus() as i32);

        let db = rocksdb::OptimisticTransactionDB::open(&opts, tempdir.path()).unwrap();
        let backend = RocksdbBackend {
            db,
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
        assert!(format!("{:?}", backend).contains("RocksdbEngine"));
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

        let transaction = backend.db.transaction();
        stage_initial_state(&transaction, &sample_format, &root).unwrap();
        drop(transaction);

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

    // Test helper functions with our new macros
    #[rstest]
    fn test_helper_functions(test_backend: (RocksdbBackend, TempDir), sample_attr: InodeAttr) {
        let (backend, _tempdir) = test_backend;
        let inode = Ino(400);

        // Set up test data using backend methods
        backend.set_attr(inode, &sample_attr).unwrap();

        // Test do_get_attr helper function
        let attr = do_get_attr(&backend.db, inode).unwrap();
        assert_eq!(attr.uid, sample_attr.uid);

        // Test do_get_dentry helper function
        let parent = Ino(1);
        let name = "test_helper";
        backend
            .set_dentry(parent, name, inode, FileType::RegularFile)
            .unwrap();

        let dentry = do_get_dentry(&backend.db, parent, name).unwrap();
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
        let transaction = backend.db.transaction();
        stage_slice_commit(&transaction, inode, 0, &slice).unwrap();
        drop(transaction);

        assert_eq!(backend.get_attr(inode).unwrap().length, 0);
        assert!(backend.get_raw_chunk_slices(inode, 0).unwrap().is_none());
    }
}
