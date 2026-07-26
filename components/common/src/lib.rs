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

pub const MAX_NAME_LENGTH: usize = 255;
pub const DOT: &str = ".";
pub const DOT_DOT: &str = "..";

pub const MODE_MASK_R: u8 = 0b100;
pub const MODE_MASK_W: u8 = 0b010;
pub const MODE_MASK_X: u8 = 0b001;

pub const KISEKI: &str = "kiseki";
pub const KISEKI_DEBUG_META_ADDR: &str = "rocksdb://:/tmp/kiseki.meta";
pub const PAGE_BUFFER_SIZE: usize = 300 << 20;
// 300MiB
// pub const PAGE_SIZE: usize = 64 << 10;
pub const PAGE_SIZE: usize = 128 << 10;
// 128 KiB
// The max block size is 4MB.
pub const BLOCK_SIZE: usize = 4 << 20; // 4 MiB

pub const MIN_BLOCK_SIZE: usize = PAGE_SIZE; // 128 KiB

pub const MAX_BLOCK_SIZE: usize = 16 << 20; // 16 MB

// The max size of a slice buffer can grow.
pub const CHUNK_SIZE: usize = 64 << 20; // 64 MiB

pub const MAX_FILE_SIZE: usize = CHUNK_SIZE << 31; // 64 MiB * 2^31 = 64 PiB

pub const MIN_FILE_SYSTEM_CAPACITY: usize = 1 << 30; // 1 GiB

pub const MAX_SYMLINK_LEN: usize = 4096;

pub const fn cal_chunk_idx(offset: usize, chunk_size: usize) -> usize { offset / chunk_size }

pub const fn cal_chunk_offset(offset: usize, chunk_size: usize) -> usize { offset % chunk_size }

pub type PageSize = usize;
pub type BlockIndex = usize;
pub type BlockSize = usize;
pub type ChunkIndex = usize;
pub type ChunkOffset = usize;
pub type ChunkSize = usize;
pub type FileOffset = usize;

pub type FH = u64;
