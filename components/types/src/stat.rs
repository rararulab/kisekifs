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

use std::fmt::{Debug, Formatter};

use kiseki_utils::readable_size::ReadableSize;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct DirStat {
    pub length: i64,
    pub space:  i64,
    pub inodes: i64,
}

/// [FSStat] represents the filesystem statistics.
#[derive(Clone, Copy)]
pub struct FSStat {
    /// Represents the total available size.
    pub total_size: u64,
    /// Represents the used size.
    pub used_size:  u64,
    /// Represents the total used file count.
    pub file_count: u64,
}

impl Debug for FSStat {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FSStat")
            .field("total_size", &ReadableSize(self.total_size))
            .field("used_size", &ReadableSize(self.used_size))
            .field("file_count", &self.file_count)
            .finish()
    }
}

impl Default for FSStat {
    fn default() -> Self {
        Self {
            total_size: u64::MAX,
            used_size:  0,
            file_count: 0,
        }
    }
}
