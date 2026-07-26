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

//! RocksDB implementation of the storage-agnostic [`KvEngine`] / [`KvTxn`]
//! traits. This is the only RocksDB-specific code the metadata backend needs;
//! everything above it works against the traits.

use std::path::Path;

use rocksdb::{Direction, IteratorMode, MultiThreaded, OptimisticTransactionDB, Transaction};
use snafu::ResultExt;

use crate::{
    backend::{
        kv::{KvEngine, KvRead, KvTxn},
        rocksdb_metrics::{rocksdb_counter, rocksdb_error, rocksdb_histogram, rocksdb_timed_op},
    },
    err::{Result, RocksdbSnafu},
};

/// How many times to re-run a transaction closure on an optimistic-commit
/// conflict before giving up.
const MAX_TXN_RETRIES: usize = 20;

/// A RocksDB-backed metadata KV store.
pub struct RocksDbKv {
    db: OptimisticTransactionDB<MultiThreaded>,
}

impl RocksDbKv {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.increase_parallelism(kiseki_utils::num_cpus() as i32);
        let db = OptimisticTransactionDB::open(&opts, path).context(RocksdbSnafu)?;
        Ok(Self { db })
    }

    pub fn path(&self) -> &Path { self.db.path() }
}

/// Collect a forward prefix scan, stopping at the first key outside `prefix` or
/// once `limit` rows have been produced.
fn collect_prefix<I>(
    iter: I,
    prefix: &[u8],
    limit: Option<usize>,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>>
where
    I: Iterator<Item = std::result::Result<(Box<[u8]>, Box<[u8]>), rocksdb::Error>>,
{
    let mut out = Vec::new();
    for item in iter {
        let (k, v) = item.context(RocksdbSnafu)?;
        if !k.starts_with(prefix) {
            break;
        }
        out.push((k.into_vec(), v.into_vec()));
        if limit.is_some_and(|l| out.len() >= l) {
            break;
        }
    }
    Ok(out)
}

impl KvRead for RocksDbKv {
    fn get_raw(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        rocksdb_timed_op!(db_gets_total, db_get_duration_ms, self.db.get(key)).context(RocksdbSnafu)
    }

    fn scan_prefix_raw(
        &self,
        prefix: &[u8],
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let iter = self
            .db
            .iterator(IteratorMode::From(prefix, Direction::Forward));
        collect_prefix(iter, prefix, limit)
    }
}

impl RocksDbKv {
    /// Shared transaction driver. `sync` selects a durable (fsync'd) commit.
    fn run_txn<T>(&self, sync: bool, mut f: impl FnMut(&mut dyn KvTxn) -> Result<T>) -> Result<T> {
        let mut write_opts = rocksdb::WriteOptions::default();
        write_opts.set_sync(sync);
        let txn_opts = rocksdb::OptimisticTransactionOptions::default();

        let mut attempt = 0usize;
        loop {
            let mut handle = RocksDbTxn {
                txn: self.db.transaction_opt(&write_opts, &txn_opts),
            };
            // An application error aborts immediately; dropping `handle` rolls
            // the transaction back.
            let value = f(&mut handle)?;
            let commit = rocksdb_timed_op!(
                db_transactions_total,
                db_transaction_duration_ms,
                handle.txn.commit()
            );
            match commit {
                Ok(()) => return Ok(value),
                Err(e) => {
                    rocksdb_error!(crate::metrics::labels::ERROR_ROCKSDB);
                    let retryable = matches!(
                        e.kind(),
                        rocksdb::ErrorKind::Busy | rocksdb::ErrorKind::TryAgain
                    );
                    attempt += 1;
                    if retryable && attempt < MAX_TXN_RETRIES {
                        continue;
                    }
                    return Err(e).context(RocksdbSnafu);
                }
            }
        }
    }
}

impl KvEngine for RocksDbKv {
    fn transaction<T>(&self, f: impl FnMut(&mut dyn KvTxn) -> Result<T>) -> Result<T> {
        self.run_txn(false, f)
    }

    fn transaction_durable<T>(&self, f: impl FnMut(&mut dyn KvTxn) -> Result<T>) -> Result<T> {
        self.run_txn(true, f)
    }
}

/// A RocksDB optimistic transaction. Puts/deletes are staged on the transaction
/// itself and applied atomically by a single `commit()` — no separate
/// write-batch phase.
struct RocksDbTxn<'a> {
    txn: Transaction<'a, OptimisticTransactionDB<MultiThreaded>>,
}

impl KvRead for RocksDbTxn<'_> {
    fn get_raw(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        rocksdb_timed_op!(db_gets_total, db_get_duration_ms, self.txn.get(key))
            .context(RocksdbSnafu)
    }

    fn scan_prefix_raw(
        &self,
        prefix: &[u8],
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let iter = self
            .txn
            .iterator(IteratorMode::From(prefix, Direction::Forward));
        collect_prefix(iter, prefix, limit)
    }
}

impl KvTxn for RocksDbTxn<'_> {
    fn get_for_update_raw(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        rocksdb_timed_op!(
            db_gets_total,
            db_get_duration_ms,
            self.txn.get_for_update(key, true)
        )
        .context(RocksdbSnafu)
    }

    fn put_raw(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        rocksdb_timed_op!(db_puts_total, db_put_duration_ms, self.txn.put(key, val))
            .context(RocksdbSnafu)
    }

    fn delete_raw(&mut self, key: &[u8]) -> Result<()> {
        rocksdb_counter!(db_deletes_total);
        self.txn.delete(key).context(RocksdbSnafu)
    }

    fn delete_prefix_raw(&mut self, prefix: &[u8]) -> Result<()> {
        let mut keys = Vec::new();
        {
            let iter = self
                .txn
                .iterator(IteratorMode::From(prefix, Direction::Forward));
            for item in iter {
                let (k, _) = item.context(RocksdbSnafu)?;
                if !k.starts_with(prefix) {
                    break;
                }
                keys.push(k.into_vec());
            }
        }
        for k in keys {
            self.txn.delete(&k).context(RocksdbSnafu)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use kiseki_types::{attr::InodeAttr, ino::Ino};
    use tempfile::TempDir;

    use super::*;
    use crate::backend::kv::{KvReadExt, KvTxnExt, key};

    fn test_kv() -> (RocksDbKv, TempDir) {
        let dir = TempDir::new().unwrap();
        let kv = RocksDbKv::open(dir.path()).unwrap();
        (kv, dir)
    }

    #[test]
    fn typed_put_get_delete_roundtrip() {
        let (kv, _dir) = test_kv();
        let ino = Ino(42);
        let attr = InodeAttr::default();

        kv.transaction(|txn| txn.put(&key::Attr(ino), &attr))
            .unwrap();
        assert_eq!(kv.get(&key::Attr(ino)).unwrap(), Some(attr));

        kv.transaction(|txn| txn.delete(&key::Attr(ino))).unwrap();
        assert_eq!(kv.get(&key::Attr(ino)).unwrap(), None);
        assert!(kv.get_or_missing(&key::Attr(ino)).is_err());
    }

    #[test]
    fn prefix_scan_returns_only_matching_children() {
        let (kv, _dir) = test_kv();
        let parent = Ino(1);
        kv.transaction(|txn| {
            for (name, ino) in [("a", 10u64), ("b", 11), ("c", 12)] {
                let dentry = kiseki_types::entry::DEntry {
                    parent,
                    name: name.to_string(),
                    inode: Ino(ino),
                    typ: kiseki_types::FileType::RegularFile,
                };
                txn.put(&key::Dentry(parent, name), &dentry)?;
            }
            // A dentry under a different parent must not be scanned.
            let other = kiseki_types::entry::DEntry {
                parent: Ino(2),
                name:   "z".to_string(),
                inode:  Ino(99),
                typ:    kiseki_types::FileType::RegularFile,
            };
            txn.put(&key::Dentry(Ino(2), "z"), &other)
        })
        .unwrap();

        let children = kv.scan(&key::DentryPrefix(parent), None).unwrap();
        assert_eq!(children.len(), 3);
        assert!(children.iter().all(|d| d.parent == parent));
    }

    #[test]
    fn delete_prefix_removes_all_children() {
        let (kv, _dir) = test_kv();
        let ino = Ino(5);
        kv.transaction(|txn| {
            for idx in 0..4u64 {
                let slices =
                    kiseki_types::slice::Slices(vec![kiseki_types::slice::Slice::new_owned(
                        0,
                        idx + 1,
                        100,
                    )]);
                txn.put(
                    &key::ChunkSlices(ino, idx as kiseki_common::ChunkIndex),
                    &slices,
                )?;
            }
            Ok(())
        })
        .unwrap();
        assert_eq!(
            kv.scan(&key::ChunkSlicesPrefix(ino), None).unwrap().len(),
            4
        );

        kv.transaction(|txn| txn.delete_prefix(&key::ChunkSlicesPrefix(ino)))
            .unwrap();
        assert_eq!(
            kv.scan(&key::ChunkSlicesPrefix(ino), None).unwrap().len(),
            0
        );
    }

    #[test]
    fn transaction_rolls_back_on_application_error() {
        let (kv, _dir) = test_kv();
        let ino = Ino(7);
        let result: Result<()> = kv.transaction(|txn| {
            txn.put(&key::Attr(ino), &InodeAttr::default())?;
            // Abort after staging a write.
            Err(crate::err::LibcSnafu { errno: libc::EIO }.build())
        });
        assert!(result.is_err());
        // The staged write must not have been committed.
        assert_eq!(kv.get(&key::Attr(ino)).unwrap(), None);
    }

    #[test]
    fn read_modify_write_counter_via_get_for_update() {
        let (kv, _dir) = test_kv();
        let ctr = key::CounterKey(key::Counter::NextInode);
        for expected in 1..=3u64 {
            let got = kv
                .transaction(|txn| {
                    let cur = txn.get_for_update(&ctr)?.unwrap_or(0);
                    let next = cur + 1;
                    txn.put(&ctr, &next)?;
                    Ok(next)
                })
                .unwrap();
            assert_eq!(got, expected);
        }
        assert_eq!(kv.get(&ctr).unwrap(), Some(3));
    }
}
