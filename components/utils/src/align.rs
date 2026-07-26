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

use kiseki_common::{MAX_BLOCK_SIZE, MIN_BLOCK_SIZE};
use tracing::warn;

pub const fn align4k(length: u64) -> i64 {
    if length == 0 {
        return 1 << 12; // 4096
    }

    // Calculate the number of 4K blocks needed to hold the data
    let blocks_needed = (length - 1) / 4096 + 1;

    // Return the aligned length (number of blocks * block size) as i64
    (blocks_needed * 4096) as i64
}

pub fn align_to_block(s: usize) -> usize {
    let mut s = s;
    let mut bits = 0;
    while s > 1 {
        bits += 1;
        s >>= 1;
    }

    let adjusted_size = s << bits;

    if adjusted_size < MIN_BLOCK_SIZE {
        warn!(
            "Block size is too small: {}, using {} instead",
            adjusted_size, MIN_BLOCK_SIZE
        );
        MIN_BLOCK_SIZE
    } else if adjusted_size > MAX_BLOCK_SIZE {
        warn!(
            "Block size is too large: {}, using {} instead",
            adjusted_size, MAX_BLOCK_SIZE
        );
        MAX_BLOCK_SIZE
    } else {
        adjusted_size
    }
}
