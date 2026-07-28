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

//! Typed metadata keys.
//!
//! Each record type is a small key struct that binds, at compile time, to the
//! value type stored under it (via [`MetaKey::Value`]). This replaces the old
//! `format!("A{:0>8}...")` string builders and their hand-written `*_prefix`
//! siblings.
//!
//! Keys are encoded in an **order-preserving binary** layout: a one-byte tag,
//! then fixed-width big-endian integers, then any trailing variable bytes (a
//! dentry name). Big-endian integers sort correctly for prefix scans without
//! the old zero-padded-decimal ceiling (ids were capped at 10^8 by `{:0>8}`).

use kiseki_common::ChunkIndex;
use kiseki_types::ino::Ino;

pub use crate::backend::key::Counter;
use crate::backend::kv::value::{SymlinkTarget, ValueCodec};

// Top-level namespace tags. The per-inode namespace (`INODE`) is further
// divided by a sub-tag byte placed right after the fixed-width inode.
const INODE: u8 = b'A';
const DIR_STAT: u8 = b'U';
const DELETE_CHUNK: u8 = b'D';
const SUSTAINED: u8 = b'T';

// Sub-tags inside the per-inode (`A<ino>...`) namespace.
const SUB_ATTR: u8 = b'I';
const SUB_DENTRY: u8 = b'D';
const SUB_SYMLINK: u8 = b'S';
const SUB_CHUNK: u8 = b'C';
const SUB_PARENT: u8 = b'P';

/// Small builder for order-preserving binary keys.
struct KeyWriter {
    buf: Vec<u8>,
}

impl KeyWriter {
    fn with_capacity(cap: usize) -> Self {
        Self {
            buf: Vec::with_capacity(cap),
        }
    }

    fn tag(mut self, tag: u8) -> Self {
        self.buf.push(tag);
        self
    }

    fn ino(self, ino: Ino) -> Self { self.u64(ino.0) }

    fn u64(mut self, v: u64) -> Self {
        self.buf.extend_from_slice(&v.to_be_bytes());
        self
    }

    fn bytes(mut self, b: &[u8]) -> Self {
        self.buf.extend_from_slice(b);
        self
    }

    fn build(self) -> Vec<u8> { self.buf }
}

/// A typed key: knows the value type stored under it and how to encode itself
/// into the raw key bytes.
pub trait MetaKey {
    type Value: ValueCodec;
    fn encode(&self) -> Vec<u8>;
}

/// A key prefix over which the KV layer can range-scan, yielding values of a
/// single type (e.g. all dentries of a directory).
pub trait ScanPrefix {
    type Value: ValueCodec;
    fn prefix(&self) -> Vec<u8>;
}

/// `A<ino>I` -> [`InodeAttr`](kiseki_types::attr::InodeAttr)
pub struct Attr(pub Ino);
impl MetaKey for Attr {
    type Value = kiseki_types::attr::InodeAttr;

    fn encode(&self) -> Vec<u8> {
        KeyWriter::with_capacity(10)
            .tag(INODE)
            .ino(self.0)
            .tag(SUB_ATTR)
            .build()
    }
}

/// `A<parent>D<name>` -> [`DEntry`](kiseki_types::entry::DEntry)
pub struct Dentry<'a>(pub Ino, pub &'a str);
impl MetaKey for Dentry<'_> {
    type Value = kiseki_types::entry::DEntry;

    fn encode(&self) -> Vec<u8> {
        KeyWriter::with_capacity(10 + self.1.len())
            .tag(INODE)
            .ino(self.0)
            .tag(SUB_DENTRY)
            .bytes(self.1.as_bytes())
            .build()
    }
}

/// Scan all dentries of a directory: prefix `A<parent>D`.
pub struct DentryPrefix(pub Ino);
impl ScanPrefix for DentryPrefix {
    type Value = kiseki_types::entry::DEntry;

    fn prefix(&self) -> Vec<u8> {
        KeyWriter::with_capacity(10)
            .tag(INODE)
            .ino(self.0)
            .tag(SUB_DENTRY)
            .build()
    }
}

/// `A<ino>S` -> [`SymlinkTarget`]
pub struct Symlink(pub Ino);
impl MetaKey for Symlink {
    type Value = SymlinkTarget;

    fn encode(&self) -> Vec<u8> {
        KeyWriter::with_capacity(10)
            .tag(INODE)
            .ino(self.0)
            .tag(SUB_SYMLINK)
            .build()
    }
}

/// `A<ino>C<chunk_index>` -> [`Slices`](kiseki_types::slice::Slices)
pub struct ChunkSlices(pub Ino, pub ChunkIndex);
impl MetaKey for ChunkSlices {
    type Value = kiseki_types::slice::Slices;

    fn encode(&self) -> Vec<u8> {
        KeyWriter::with_capacity(18)
            .tag(INODE)
            .ino(self.0)
            .tag(SUB_CHUNK)
            .u64(self.1 as u64)
            .build()
    }
}

/// Scan all chunks of a file: prefix `A<ino>C`.
pub struct ChunkSlicesPrefix(pub Ino);
impl ScanPrefix for ChunkSlicesPrefix {
    type Value = kiseki_types::slice::Slices;

    fn prefix(&self) -> Vec<u8> {
        KeyWriter::with_capacity(10)
            .tag(INODE)
            .ino(self.0)
            .tag(SUB_CHUNK)
            .build()
    }
}

/// `A<ino>P<parent>` -> hard-link count (`u64`)
pub struct HardLink(pub Ino, pub Ino);
impl MetaKey for HardLink {
    type Value = u64;

    fn encode(&self) -> Vec<u8> {
        KeyWriter::with_capacity(18)
            .tag(INODE)
            .ino(self.0)
            .tag(SUB_PARENT)
            .ino(self.1)
            .build()
    }
}

/// Scan all parents of an inode: prefix `A<ino>P`.
pub struct HardLinkPrefix(pub Ino);
impl ScanPrefix for HardLinkPrefix {
    type Value = u64;

    fn prefix(&self) -> Vec<u8> {
        KeyWriter::with_capacity(10)
            .tag(INODE)
            .ino(self.0)
            .tag(SUB_PARENT)
            .build()
    }
}

/// `U<ino>` -> [`DirStat`](kiseki_types::stat::DirStat)
pub struct DirStatKey(pub Ino);
impl MetaKey for DirStatKey {
    type Value = kiseki_types::stat::DirStat;

    fn encode(&self) -> Vec<u8> {
        KeyWriter::with_capacity(9)
            .tag(DIR_STAT)
            .ino(self.0)
            .build()
    }
}

/// `D<ino>` -> delete-after timestamp (`u64`, seconds)
pub struct DeleteChunk(pub Ino);
impl MetaKey for DeleteChunk {
    type Value = u64;

    fn encode(&self) -> Vec<u8> {
        KeyWriter::with_capacity(9)
            .tag(DELETE_CHUNK)
            .ino(self.0)
            .build()
    }
}

/// `T<session_id><ino>` -> sustained marker (`u64`)
pub struct Sustained(pub u64, pub Ino);
impl MetaKey for Sustained {
    type Value = u64;

    fn encode(&self) -> Vec<u8> {
        KeyWriter::with_capacity(17)
            .tag(SUSTAINED)
            .u64(self.0)
            .ino(self.1)
            .build()
    }
}

/// A named counter (`next_inode`, `used_space`, ...). Stored under its ASCII
/// name, matching the singleton counter keyspace.
pub struct CounterKey(pub Counter);
impl MetaKey for CounterKey {
    type Value = u64;

    fn encode(&self) -> Vec<u8> { self.0.as_ref().to_vec() }
}

/// The filesystem [`Format`](kiseki_types::setting::Format), stored under the
/// ASCII `current_format` key.
pub struct FormatKey;
impl MetaKey for FormatKey {
    type Value = kiseki_types::setting::Format;

    fn encode(&self) -> Vec<u8> { crate::backend::key::CURRENT_FORMAT.as_bytes().to_vec() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn per_inode_keys_are_distinguished_by_subtag() {
        let ino = Ino(0x42);
        let attr = Attr(ino).encode();
        let sym = Symlink(ino).encode();
        let dentry = Dentry(ino, "f").encode();
        // Same 9-byte inode prefix, different sub-tag byte.
        assert_eq!(&attr[..9], &sym[..9]);
        assert_eq!(&attr[..9], &dentry[..9]);
        assert_ne!(attr[9], sym[9]);
        assert_ne!(attr[9], dentry[9]);
        assert_eq!(attr.len(), 10);
    }

    #[test]
    fn dentry_prefix_contains_its_entries() {
        let p = DentryPrefix(Ino(7)).prefix();
        for name in ["a", "bb", "ccc"] {
            assert!(Dentry(Ino(7), name).encode().starts_with(&p));
        }
        // A different parent is not caught by this prefix.
        assert!(!Dentry(Ino(8), "a").encode().starts_with(&p));
    }

    #[test]
    fn chunk_prefix_contains_all_chunks() {
        let p = ChunkSlicesPrefix(Ino(3)).prefix();
        for idx in [0u64, 1, 999] {
            assert!(
                ChunkSlices(Ino(3), idx as ChunkIndex)
                    .encode()
                    .starts_with(&p)
            );
        }
    }

    #[test]
    fn big_endian_ids_sort_correctly_past_the_old_decimal_ceiling() {
        // The old `{:0>8}` decimal scheme broke ordering past 10^8; big-endian
        // keys stay ordered for the whole u64 range.
        let small = Attr(Ino(100_000_000)).encode();
        let large = Attr(Ino(100_000_001)).encode();
        assert!(small < large);
        let huge = Attr(Ino(u64::MAX)).encode();
        assert!(large < huge);
    }

    #[test]
    fn namespaces_do_not_collide() {
        let ino = Ino(1);
        let keys = [
            Attr(ino).encode(),
            DirStatKey(ino).encode(),
            DeleteChunk(ino).encode(),
            Sustained(1, ino).encode(),
        ];
        for (i, a) in keys.iter().enumerate() {
            for b in &keys[i + 1..] {
                assert_ne!(a, b);
            }
        }
    }
}
