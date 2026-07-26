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

use kiseki_types::{ino::Ino, slice::SliceID};

pub const CURRENT_FORMAT: &str = "current_format";
pub const USED_SPACE: &str = "used_space";
pub const TOTAL_INODES: &str = "total_inodes";
pub const LEGACY_SESSIONS: &str = "legacy_sessions";
pub const NEXT_TRASH: &str = "next_trash";
pub const NEXT_INODE: &str = "next_inode";
pub const NEXT_SLICE: &str = "next_slice";

// The variants mirror the counter keys persisted on disk; not all of
// them are wired up yet.
#[allow(dead_code)]
#[derive(Debug, Eq, PartialEq, Copy, Clone, Hash)]
pub enum Counter {
    UsedSpace,
    TotalInodes,
    LegacySessions,
    NextTrash,
    NextInode,
    NextSlice,
}

impl From<Counter> for Vec<u8> {
    fn from(val: Counter) -> Self {
        match val {
            Counter::UsedSpace => USED_SPACE.as_bytes().to_vec(),
            Counter::TotalInodes => TOTAL_INODES.as_bytes().to_vec(),
            Counter::LegacySessions => LEGACY_SESSIONS.as_bytes().to_vec(),
            Counter::NextTrash => NEXT_TRASH.as_bytes().to_vec(),
            Counter::NextInode => NEXT_INODE.as_bytes().to_vec(),
            Counter::NextSlice => NEXT_SLICE.as_bytes().to_vec(),
        }
    }
}

impl AsRef<[u8]> for Counter {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::UsedSpace => USED_SPACE.as_bytes(),
            Self::TotalInodes => TOTAL_INODES.as_bytes(),
            Self::LegacySessions => LEGACY_SESSIONS.as_bytes(),
            Self::NextTrash => NEXT_TRASH.as_bytes(),
            Self::NextInode => NEXT_INODE.as_bytes(),
            Self::NextSlice => NEXT_SLICE.as_bytes(),
        }
    }
}

impl Counter {
    pub fn get_step(&self) -> usize {
        match self {
            Self::NextTrash => 1,
            Self::NextInode => 1 << 10,
            Self::NextSlice => 4 << 10,
            _ => panic!("Counter {self:?} does not have a step"),
        }
    }
}

pub fn attr(inode: Ino) -> Vec<u8> { format!("A{:0>8}I", inode.0).into_bytes() }

pub fn xattr(inode: Ino, name: &str) -> Vec<u8> {
    format!("A{:0>8}X{}", inode.0, name).into_bytes()
}
pub fn xattr_prefix(inode: Ino) -> Vec<u8> { format!("A{:0>8}X", inode.0).into_bytes() }

pub fn dentry(parent: Ino, name: &str) -> Vec<u8> {
    format!("A{:0>8}D/{}", parent.0, name).into_bytes()
}

pub fn dentry_prefix(parent: Ino) -> Vec<u8> { format!("A{:0>8}D/", parent.0).into_bytes() }

/// [parent] generate key for hard links.
///
/// This key is used to store the hard link count of a file.
pub fn parent(inode: Ino, parent: Ino) -> Vec<u8> {
    // AiiiiiiiiPiiiiiiii parents
    format!("A{:0>8}P{:0>8}", inode.0, parent.0).into_bytes()
}
pub fn parent_prefix(inode: Ino) -> Vec<u8> { format!("A{:0>8}P", inode.0).into_bytes() }

pub fn symlink(inode: Ino) -> Vec<u8> { format!("A{:0>8}S", inode.0).into_bytes() }

// sustained is used when we cannot delete inode since it was being used at
// right now.
pub fn sustained(sid: u64, inode: Ino) -> Vec<u8> {
    format!("SS{:0>8}{:0>8}", sid, inode.0).into_bytes()
}

// delete_chunk_after is a marker used to indicate that when we need to delete
// the chunks.
//
// Key: Diiiiiiii
// Val: timestamp
//
// Don't know why juicefs doesn't delete the chunk info directly, it setups a
// background job to delete the chunk info which has been deleted for a while.
// Maybe a performance optimization since the slices amount is huge.
//
// Juice also put the length in the key, doesn't get the point.
pub fn delete_chunk_after(inode: Ino) -> Vec<u8> { format!("D{:0>8}", inode.0).into_bytes() }

pub fn chunk_slices(inode: Ino, chunk_idx: kiseki_common::ChunkIndex) -> Vec<u8> {
    format!("A{:0>8}C/{}", inode.0, chunk_idx).into_bytes()
}
pub fn chunk_slices_prefix(inode: Ino) -> Vec<u8> { format!("A{:0>8}C/", inode.0).into_bytes() }

/// slice_ref tracks how many borrow slices are referencing to an Owned slice.
/// We can only delete the slice when the reference count is zero.
pub fn slice_ref(slice_id: SliceID) -> Vec<u8> { format!("K{slice_id:0>8}").into_bytes() }

pub fn dir_stat(inode: Ino) -> Vec<u8> { format!("U{:0>8}I", inode.0).into_bytes() }
