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

//! A typed key/value layer for the metadata store, in the spirit of Databend's
//! `kvapi`: typed [`key::MetaKey`]s bound to their value type, values that
//! (de)serialize through [`value::ValueCodec`], and (added incrementally) a
//! storage-agnostic transaction API on top of which the RocksDB backend and any
//! future backend are implemented.

pub mod key;
pub mod value;

use std::fmt::Write as _;

use snafu::IntoError;

use self::{
    key::{MetaKey, ScanPrefix},
    value::{CodecError, ValueCodec},
};
use crate::err::{ModelSnafu, Result, model_err};

/// Hex-encode a raw key for diagnostics (keys are binary).
fn hex_key(key: &[u8]) -> String {
    let mut s = String::with_capacity(key.len() * 2);
    for b in key {
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn missing(key: &[u8]) -> crate::err::Error {
    ModelSnafu.into_error(model_err::Error::Missing { key: hex_key(key) })
}

fn corrupt(key: &[u8], err: &CodecError) -> crate::err::Error {
    ModelSnafu.into_error(model_err::Error::Corrupt {
        key:    hex_key(key),
        reason: err.to_string(),
    })
}

fn decode_value<V: ValueCodec>(key: &[u8], bytes: &[u8]) -> Result<V> {
    V::decode(bytes).map_err(|e| corrupt(key, &e))
}

fn encode_value<V: ValueCodec>(key: &[u8], value: &V) -> Result<Vec<u8>> {
    value.encode().map_err(|e| corrupt(key, &e))
}

/// Read side of the KV store — implemented by both a non-transactional engine
/// snapshot and an in-flight transaction. Object-safe (raw bytes only).
pub trait KvRead {
    fn get_raw(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn scan_prefix_raw(
        &self,
        prefix: &[u8],
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
}

/// Typed read sugar, blanket-implemented for every [`KvRead`] (including
/// `dyn KvTxn`). Generic methods monomorphize at the call site over the
/// object-safe raw methods.
pub trait KvReadExt: KvRead {
    /// Typed point read; `Ok(None)` if the key is absent.
    fn get<K: MetaKey>(&self, k: &K) -> Result<Option<K::Value>> {
        let raw = k.encode();
        match self.get_raw(&raw)? {
            None => Ok(None),
            Some(bytes) => decode_value::<K::Value>(&raw, &bytes).map(Some),
        }
    }

    /// Typed point read that errors with a NotFound (`ENOENT`) if absent.
    fn get_or_missing<K: MetaKey>(&self, k: &K) -> Result<K::Value> {
        let raw = k.encode();
        let bytes = self.get_raw(&raw)?.ok_or_else(|| missing(&raw))?;
        decode_value::<K::Value>(&raw, &bytes)
    }

    /// Typed prefix scan, decoding each value.
    fn scan<P: ScanPrefix>(&self, prefix: &P, limit: Option<usize>) -> Result<Vec<P::Value>> {
        let raw_prefix = prefix.prefix();
        self.scan_prefix_raw(&raw_prefix, limit)?
            .iter()
            .map(|(k, v)| decode_value::<P::Value>(k, v))
            .collect()
    }
}
impl<T: KvRead + ?Sized> KvReadExt for T {}

/// Write side — an in-flight transaction. Object-safe (raw bytes only); the
/// engine hands a `&mut dyn KvTxn` to the transaction closure.
pub trait KvTxn: KvRead {
    /// Read with a write-intent lock for read-modify-write (optimistic
    /// conflict detection on commit).
    fn get_for_update_raw(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn put_raw(&mut self, key: &[u8], val: &[u8]) -> Result<()>;
    fn delete_raw(&mut self, key: &[u8]) -> Result<()>;
    fn delete_prefix_raw(&mut self, prefix: &[u8]) -> Result<()>;
}

/// Typed write sugar, blanket-implemented for every [`KvTxn`].
pub trait KvTxnExt: KvTxn {
    fn get_for_update<K: MetaKey>(&self, k: &K) -> Result<Option<K::Value>> {
        let raw = k.encode();
        match self.get_for_update_raw(&raw)? {
            None => Ok(None),
            Some(bytes) => decode_value::<K::Value>(&raw, &bytes).map(Some),
        }
    }

    fn get_for_update_or_missing<K: MetaKey>(&self, k: &K) -> Result<K::Value> {
        let raw = k.encode();
        let bytes = self
            .get_for_update_raw(&raw)?
            .ok_or_else(|| missing(&raw))?;
        decode_value::<K::Value>(&raw, &bytes)
    }

    fn put<K: MetaKey>(&mut self, k: &K, v: &K::Value) -> Result<()> {
        let raw = k.encode();
        let bytes = encode_value(&raw, v)?;
        self.put_raw(&raw, &bytes)
    }

    fn delete<K: MetaKey>(&mut self, k: &K) -> Result<()> { self.delete_raw(&k.encode()) }

    fn delete_prefix<P: ScanPrefix>(&mut self, p: &P) -> Result<()> {
        self.delete_prefix_raw(&p.prefix())
    }
}
impl<T: KvTxn + ?Sized> KvTxnExt for T {}

/// A metadata KV store. Not object-safe (generic `transaction`), which is fine:
/// the backend holds a concrete engine. RocksDB is one implementation; a future
/// networked/shared backend implements the same trait.
pub trait KvEngine: Send + Sync + KvRead {
    /// Run `f` inside a transaction and commit once. Retries the closure on
    /// optimistic-conflict a bounded number of times; application errors abort
    /// immediately.
    fn transaction<T>(&self, f: impl FnMut(&mut dyn KvTxn) -> Result<T>) -> Result<T>;

    /// Like [`Self::transaction`], but the commit is flushed durably (fsync)
    /// before returning. Use for writes that must survive a crash the moment
    /// they are acknowledged (e.g. committing a data slice).
    fn transaction_durable<T>(&self, f: impl FnMut(&mut dyn KvTxn) -> Result<T>) -> Result<T>;
}

#[cfg(feature = "meta-rocksdb")]
pub mod rocksdb;
