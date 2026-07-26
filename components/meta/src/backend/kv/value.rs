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

//! Typed value codecs for the metadata KV store.
//!
//! Every metadata record type knows how to (de)serialize itself through
//! [`ValueCodec`]. Most records use bincode (via [`impl_bincode_value`]); the
//! two exceptions preserve the on-disk layouts the previous backend relied on:
//! symlink targets are stored as raw UTF-8 bytes, and a chunk's [`Slices`] are
//! stored as a packed array of fixed-width [`SLICE_BYTES`] records.
//!
//! Codecs are intentionally decoupled from key/namespace context: they fail
//! with a plain [`CodecError`], and the KV layer ([`super`]) attaches the key
//! and NotFound/Corruption context. This removes the `from_utf8_lossy(&key)`
//! boilerplate that used to be copy-pasted at every call site.

use bytes::Bytes;
use kiseki_types::slice::{SLICE_BYTES, Slices};
use snafu::{ResultExt, Snafu};

/// Error produced by a value codec. The KV layer maps this to a
/// `model_err::Error` with the offending key attached.
#[derive(Debug, Snafu)]
pub enum CodecError {
    #[snafu(display("bincode codec error: {source}"))]
    Bincode { source: bincode::Error },
    #[snafu(display("slice codec error: {source}"))]
    Slice { source: kiseki_types::slice::Error },
}

pub type CodecResult<T> = std::result::Result<T, CodecError>;

/// A metadata value that can round-trip through the KV store.
pub trait ValueCodec: Sized {
    fn encode(&self) -> CodecResult<Vec<u8>>;
    fn decode(bytes: &[u8]) -> CodecResult<Self>;
}

/// Implement [`ValueCodec`] via bincode for one or more types.
macro_rules! impl_bincode_value {
    ($($t:ty),+ $(,)?) => {
        $(
            impl ValueCodec for $t {
                fn encode(&self) -> CodecResult<Vec<u8>> {
                    bincode::serialize(self).context(BincodeSnafu)
                }
                fn decode(bytes: &[u8]) -> CodecResult<Self> {
                    bincode::deserialize(bytes).context(BincodeSnafu)
                }
            }
        )+
    };
}

impl_bincode_value!(
    kiseki_types::attr::InodeAttr,
    kiseki_types::entry::DEntry,
    kiseki_types::stat::DirStat,
    kiseki_types::setting::Format,
    u64,
);

/// A symbolic-link target, stored verbatim as raw bytes (not bincode) to match
/// POSIX readlink(2) semantics and the historical on-disk layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SymlinkTarget(pub Bytes);

impl SymlinkTarget {
    pub fn new(bytes: impl Into<Bytes>) -> Self { Self(bytes.into()) }
}

impl ValueCodec for SymlinkTarget {
    fn encode(&self) -> CodecResult<Vec<u8>> { Ok(self.0.to_vec()) }

    fn decode(bytes: &[u8]) -> CodecResult<Self> { Ok(Self(Bytes::copy_from_slice(bytes))) }
}

/// A chunk's slice list, stored as a packed array of fixed-width
/// [`SLICE_BYTES`] records. This is the canonical chunk-slices layout used by
/// the read and commit paths.
impl ValueCodec for Slices {
    fn encode(&self) -> CodecResult<Vec<u8>> {
        let mut buf = Vec::with_capacity(self.0.len() * SLICE_BYTES);
        for slice in &self.0 {
            let encoded = bincode::serialize(slice).context(BincodeSnafu)?;
            debug_assert_eq!(
                encoded.len(),
                SLICE_BYTES,
                "a Slice must bincode to exactly SLICE_BYTES"
            );
            buf.extend_from_slice(&encoded);
        }
        Ok(buf)
    }

    fn decode(bytes: &[u8]) -> CodecResult<Self> {
        // `Self::decode` resolves to the inherent `Slices::decode` (inherent
        // methods win over the trait method of the same name being defined
        // here), so this is not recursive.
        Self::decode(bytes).context(SliceSnafu)
    }
}

#[cfg(test)]
mod tests {
    use kiseki_types::{
        attr::InodeAttr,
        ino::Ino,
        slice::{Slice, Slices},
        stat::DirStat,
    };

    use super::*;

    fn roundtrip<T: ValueCodec + PartialEq + std::fmt::Debug>(v: &T) {
        let bytes = v.encode().expect("encode");
        let back = T::decode(&bytes).expect("decode");
        assert_eq!(v, &back);
    }

    #[test]
    fn bincode_values_roundtrip() {
        roundtrip(&InodeAttr::default());
        roundtrip(&DirStat {
            length: 3,
            space:  4,
            inodes: 5,
        });
        roundtrip(&7u64);
    }

    #[test]
    fn symlink_target_is_raw_bytes() {
        let t = SymlinkTarget::new(Bytes::from_static(b"/a/b/c"));
        let bytes = t.encode().unwrap();
        // Raw, not bincode: no length prefix, exact payload.
        assert_eq!(bytes, b"/a/b/c");
        assert_eq!(SymlinkTarget::decode(&bytes).unwrap(), t);
    }

    #[test]
    fn slices_pack_to_slice_bytes_multiples() {
        let slices = Slices(vec![
            Slice::new_owned(0, 10, 100),
            Slice::new_owned(100, 11, 50),
        ]);
        let bytes = slices.encode().unwrap();
        assert_eq!(bytes.len(), 2 * SLICE_BYTES);
        assert_eq!(Slices::decode(&bytes).unwrap(), slices);
    }

    #[test]
    fn empty_slices_encode_empty() {
        let slices = Slices(vec![]);
        assert!(slices.encode().unwrap().is_empty());
        let _ = Ino::default();
    }
}
