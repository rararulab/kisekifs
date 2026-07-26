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
    cmp::Ordering,
    collections::hash_map::DefaultHasher,
    fmt::{Debug, Display, Formatter},
    hash::{Hash, Hasher},
    num::ParseIntError,
    str::FromStr,
    sync::{Arc, LazyLock},
};

use kiseki_utils::{object_storage::ObjectStoragePath, readable_size::ReadableSize};
use rangemap::RangeMap;
use serde::{Deserialize, Serialize};
use snafu::{Location, ResultExt, Snafu, ensure};

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum Error {
    InvalidSliceBuf {
        len:      usize,
        #[snafu(implicit)]
        location: Location,
    },
    InvalidSliceKeyStr {
        str:      String,
        #[snafu(implicit)]
        location: Location,
    },
    DecodeSliceBufError {
        source:   bincode::Error,
        #[snafu(implicit)]
        location: Location,
    },
    ParseSliceKeyFailed {
        str:      String,
        #[snafu(implicit)]
        location: Location,
        source:   ParseIntError,
    },
}

pub static ID_GENERATOR: LazyLock<sonyflake::Sonyflake> =
    LazyLock::new(|| sonyflake::Sonyflake::new().expect("failed to create id generator"));

pub fn make_slice_object_key(slice_id: SliceID, block_idx: usize, block_size: usize) -> String {
    SliceKey::new(slice_id, block_idx, block_size).gen_path_for_object_sto()
}

pub const EMPTY_SLICE_ID: SliceID = 0;

pub type SliceID = u64;

pub fn random_slice_id() -> u64 { ID_GENERATOR.next_id().expect("failed to generate id") }

pub type OverlookedSlices = RangeMap<usize, Slice>;
pub type OverlookedSlicesRef = Arc<OverlookedSlices>;

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Slices(pub Vec<Slice>);

impl Slices {
    pub fn decode(buf: &[u8]) -> Result<Self, Error> {
        ensure!(
            buf.len().is_multiple_of(SLICE_BYTES),
            InvalidSliceBufSnafu { len: buf.len() }
        );
        let mut slices = Vec::new();
        let mut i = 0;
        while i < buf.len() {
            let slice = Slice::decode(&buf[i..i + SLICE_BYTES])?;
            slices.push(slice);
            i += SLICE_BYTES;
        }
        Ok(Self(slices))
    }

    /// Look over all slices and build a RangeMap for them.
    pub fn overlook(&self) -> RangeMap<usize, Slice> {
        let mut rm = rangemap::RangeMap::new();
        self.0.iter().for_each(|s| {
            rm.insert(
                s.get_chunk_pos()..s.get_chunk_pos() + s.get_size(),
                s.clone(),
            );
        });
        rm
    }

    pub const fn len(&self) -> usize { self.0.len() }

    pub const fn is_empty(&self) -> bool { self.0.is_empty() }
}

#[derive(Eq, PartialEq, Clone, Serialize, Deserialize)]
pub enum Slice {
    /// Owned means this slice is built by write operation.
    Owned {
        /// The chunk position of the slice.
        chunk_pos: u32,
        /// The unique id of the slice.
        id:        SliceID,
        /// the underlying data size
        size:      u32,
        _padding:  u64,
    },
    /// The slice is borrowed from other slice, built by File RangeCopy.
    Borrowed {
        /// The chunk position of the slice.
        chunk_pos: u32,
        /// The unique id of the slice.
        id:        SliceID,
        /// the underlying data size
        size:      u32,
        /// The offset of the borrowed slice in the owned slice.
        off:       u32,
        /// The length of the borrowed slice.
        len:       u32,
    },
}

impl Debug for Slice {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Owned {
                chunk_pos,
                id,
                size,
                _padding,
            } => write!(
                f,
                "Slice::Owned {{ chunk_pos: {}, id: {}, size: {}, _padding: {} }}",
                chunk_pos,
                id,
                ReadableSize(*size as u64),
                _padding
            ),
            Self::Borrowed {
                chunk_pos,
                id,
                size,
                off,
                len,
            } => write!(
                f,
                "Slice::Borrowed {{ chunk_pos: {}, id: {}, size: {}, off: {}, len: {} }}",
                chunk_pos,
                id,
                ReadableSize(*size as u64),
                off,
                len
            ),
        }
    }
}

impl Slice {
    pub const fn new_owned(chunk_pos: usize, slice_id: u64, size: usize) -> Self {
        Self::Owned {
            chunk_pos: chunk_pos as u32,
            id:        slice_id,
            size:      size as u32,
            _padding:  0,
        }
    }

    pub fn decode(buf: &[u8]) -> Result<Self, Error> {
        ensure!(
            buf.len() >= SLICE_BYTES,
            InvalidSliceBufSnafu { len: buf.len() }
        );
        let x: Self = bincode::deserialize(buf).context(DecodeSliceBufSnafu)?;
        Ok(x)
    }

    pub fn get_chunk_pos(&self) -> usize {
        (match self {
            Self::Owned { chunk_pos, .. } => *chunk_pos,
            Self::Borrowed { chunk_pos, off, .. } => *chunk_pos + off,
        }) as usize
    }

    pub const fn get_id(&self) -> SliceID {
        match self {
            Self::Owned { id: slice_id, .. } => *slice_id,
            Self::Borrowed { id: slice_id, .. } => *slice_id,
        }
    }

    pub const fn get_size(&self) -> usize {
        (match self {
            Self::Owned { size, .. } => *size,
            // for borrowed slice, the size is the length of the borrowed part.
            Self::Borrowed { len, .. } => *len,
        }) as usize
    }

    pub const fn get_underlying_size(&self) -> usize {
        (match self {
            Self::Owned { size, .. } => *size,
            Self::Borrowed { size, .. } => *size,
        }) as usize
    }

    /// Return the byte offset where this logical slice starts in its backing
    /// object.
    pub const fn get_underlying_offset(&self) -> usize {
        match self {
            Self::Owned { .. } => 0,
            Self::Borrowed { off, .. } => *off as usize,
        }
    }
}

pub const SLICE_BYTES: usize = 28;

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub struct SliceKey {
    pub slice_id:   SliceID,
    pub block_idx:  usize,
    pub block_size: usize,
}

pub const EMPTY_SLICE_KEY: SliceKey = SliceKey {
    slice_id:   0,
    block_idx:  0,
    block_size: 0,
};

impl Display for SliceKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:08X}_{:08X}_{:08X}_{:08X}_{:08X}",
            // we can overwrite a slice, so we need to avoid the conflict.
            self.slice_id / 1000 / 1000,
            self.slice_id / 1000,
            self.slice_id,
            self.block_idx,
            self.block_size
        )
    }
}

impl PartialOrd for SliceKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let sc = self.slice_id.cmp(&other.slice_id);
        if sc == Ordering::Equal {
            let bc = self.block_idx.cmp(&other.block_idx);
            Some(bc)
        } else {
            Some(sc)
        }
    }
}

impl SliceKey {
    pub const fn new(slice_id: SliceID, block_idx: usize, block_size: usize) -> Self {
        Self {
            slice_id,
            block_idx,
            block_size,
        }
    }

    pub fn gen_path_for_local_sto(&self) -> String { format!("{self}") }

    pub fn gen_path_for_object_sto(&self) -> String { format!("chunks{self}") }

    pub fn random() -> Self {
        Self {
            slice_id:   ID_GENERATOR.next_id().expect("failed to generate id"),
            block_idx:  0,
            block_size: 0,
        }
    }

    pub fn cal_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }

    pub fn make_object_storage_path(&self) -> ObjectStoragePath {
        ObjectStoragePath::from(self.gen_path_for_object_sto())
    }
}

impl TryFrom<&str> for SliceKey {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> { Self::from_str(s) }
}

impl FromStr for SliceKey {
    type Err = Error;

    // Define the error type for parsing

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let Some(encoded) = s.strip_prefix("chunks") else {
            return InvalidSliceKeyStrSnafu { str: s.to_string() }.fail();
        };
        let fields = encoded.split('_').collect::<Vec<_>>();
        let [_, _, slice_id, block_idx, block_size] = fields.as_slice() else {
            return InvalidSliceKeyStrSnafu { str: s.to_string() }.fail();
        };
        let parse_hex = |field: &str| {
            u64::from_str_radix(field, 16).context(ParseSliceKeyFailedSnafu { str: s.to_string() })
        };
        let key = Self {
            slice_id:   parse_hex(slice_id)?,
            block_idx:  usize::try_from(parse_hex(block_idx)?)
                .map_err(|_| InvalidSliceKeyStrSnafu { str: s.to_string() }.build())?,
            block_size: usize::try_from(parse_hex(block_size)?)
                .map_err(|_| InvalidSliceKeyStrSnafu { str: s.to_string() }.build())?,
        };
        ensure!(
            key.gen_path_for_object_sto() == s,
            InvalidSliceKeyStrSnafu { str: s.to_string() }
        );
        Ok(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slice_key_object_path_round_trips_hex_fields() {
        for key in [
            SliceKey::new(0, 0, 0),
            SliceKey::new(0x1234_5678, 0x0012_3456, 0x0040_0000),
            SliceKey::new(0xABCD_EF12_3456_7890, 0x00FE_DCBA, 0x00A0_B0C0),
            SliceKey::new(u64::MAX, usize::MAX, usize::MAX),
        ] {
            let path = key.gen_path_for_object_sto();
            assert_eq!(path.parse::<SliceKey>().unwrap(), key, "path: {path}");
        }
    }

    #[test]
    fn slice_key_parser_rejects_non_canonical_paths() {
        let key = SliceKey::new(0xABCD_EF12, 0xABCD, 0x400000);
        let canonical = key.gen_path_for_object_sto();
        let local_name = key.gen_path_for_local_sto();
        let lower_case = canonical.to_ascii_lowercase();
        let wrong_derived_prefix = format!("chunks00000001{}", &local_name[8..]);

        for invalid in [
            local_name.as_str(),
            lower_case.as_str(),
            wrong_derived_prefix.as_str(),
            "chunks/00000000_00000000_00000000_00000000_00000000",
            "chunks00000000_00000000_00000000_00000000",
            "chunks00000000_00000000_00000000_00000000_00000000_extra",
        ] {
            assert!(
                invalid.parse::<SliceKey>().is_err(),
                "accepted non-canonical slice key: {invalid}"
            );
        }
    }

    #[test]
    fn slice_v2() {
        let slice = Slice::Owned {
            chunk_pos: 0,
            id:        1,
            size:      1024,
            _padding:  0,
        };
        let buf = bincode::serialize(&slice).unwrap();
        let slice2 = bincode::deserialize(&buf).unwrap();
        assert_eq!(slice, slice2);
        println!("{}", buf.len());
        matches!(slice2, Slice::Owned { .. });

        let slice = Slice::Borrowed {
            chunk_pos: 0,
            id:        1,
            size:      1024,
            off:       0,
            len:       1024,
        };
        let buf = bincode::serialize(&slice).unwrap();
        let slice2 = Slice::decode(&buf).unwrap();
        assert_eq!(slice, slice2);
        println!("{}", buf.len());
        matches!(slice2, Slice::Borrowed { .. });

        let slices = Slices(vec![slice, slice2]);
        let buf = bincode::serialize(&slices).unwrap();
        let slices2 = bincode::deserialize(&buf).unwrap();
        assert_eq!(slices, slices2);
    }
}
