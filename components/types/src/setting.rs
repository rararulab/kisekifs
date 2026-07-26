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

use std::fmt::{Display, Formatter};

use kiseki_common::{BLOCK_SIZE, CHUNK_SIZE, KISEKI, PAGE_SIZE};
use serde::{Deserialize, Serialize};

/// A persisted format field that determines how file data is laid out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormatLayoutField {
    ChunkSize,
    BlockSize,
    PageSize,
}

impl Display for FormatLayoutField {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ChunkSize => f.write_str("chunk_size"),
            Self::BlockSize => f.write_str("block_size"),
            Self::PageSize => f.write_str("page_size"),
        }
    }
}

/// The first incompatible layout value found between two formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FormatLayoutMismatch {
    pub field:     FormatLayoutField,
    pub stored:    usize,
    pub requested: usize,
}

/// [Format] can be thought of as the configuration of the filesystem.
/// We can set up different filesystems with different configurations
/// on the same infrastructure, kind of like tenants. We can use
/// Rocksdb's column family to implement this feature. But tikv doesn't
/// open that feature yet. So there may some work to implement that.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Format {
    /// `name` of the filesystem.
    pub name: String,

    /// `chunk_size` is the maximum size a single buffer can
    /// hold no matter it is for reading or writing.
    pub chunk_size: usize,
    /// `block_size` is the maximum object size when uploading
    /// the file content data to the remote.
    ///
    /// When the data is not enough to fill the block,
    /// then the block size is equal to the data size,
    /// for example, the last block of the file.
    pub block_size: usize,
    /// `page_size` can also be called `MIN_BLOCK_SIZE`;
    /// which is the min size of the block. Since under the hood,
    /// each block is divided into fixed size pages.
    pub page_size:  usize,

    /// `max_capacity` limits the capacity of the filesystem.
    pub max_capacity: Option<usize>,
    /// `max_inodes` limits the number of inodes.
    pub max_inodes:   Option<usize>,
}

impl Default for Format {
    fn default() -> Self {
        Self {
            name:         String::from(KISEKI),
            chunk_size:   CHUNK_SIZE, // 64MB
            block_size:   BLOCK_SIZE, // 4MB
            page_size:    PAGE_SIZE,  // 64KB
            max_capacity: None,
            max_inodes:   None,
        }
    }
}

impl Format {
    pub fn with_name(&mut self, name: &str) -> &mut Self {
        self.name = name.to_string();
        self
    }

    /// Returns the first immutable layout field that differs from `requested`.
    pub fn layout_mismatch(&self, requested: &Self) -> Option<FormatLayoutMismatch> {
        [
            (
                FormatLayoutField::ChunkSize,
                self.chunk_size,
                requested.chunk_size,
            ),
            (
                FormatLayoutField::BlockSize,
                self.block_size,
                requested.block_size,
            ),
            (
                FormatLayoutField::PageSize,
                self.page_size,
                requested.page_size,
            ),
        ]
        .into_iter()
        .find_map(|(field, stored, requested)| {
            (stored != requested).then_some(FormatLayoutMismatch {
                field,
                stored,
                requested,
            })
        })
    }

    /// Applies user-editable settings while retaining this format's layout.
    pub fn merge_mutable_from(self, requested: Self) -> Self {
        Self {
            name: requested.name,
            max_capacity: requested.max_capacity,
            max_inodes: requested.max_inodes,
            ..self
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Format, FormatLayoutField, FormatLayoutMismatch};

    #[test]
    fn equal_layouts_have_no_mismatch() {
        let stored = Format::default();
        let mut requested = stored.clone();
        requested.name = "renamed".to_string();
        requested.max_capacity = Some(1024);
        requested.max_inodes = Some(128);

        assert_eq!(stored.layout_mismatch(&requested), None);
    }

    #[test]
    fn identifies_each_immutable_layout_mismatch() {
        let stored = Format::default();
        let cases = [
            (
                FormatLayoutField::ChunkSize,
                stored.chunk_size,
                stored.chunk_size + 1,
            ),
            (
                FormatLayoutField::BlockSize,
                stored.block_size,
                stored.block_size + 1,
            ),
            (
                FormatLayoutField::PageSize,
                stored.page_size,
                stored.page_size + 1,
            ),
        ];

        for (field, stored_value, requested_value) in cases {
            let mut requested = stored.clone();
            match field {
                FormatLayoutField::ChunkSize => requested.chunk_size = requested_value,
                FormatLayoutField::BlockSize => requested.block_size = requested_value,
                FormatLayoutField::PageSize => requested.page_size = requested_value,
            }

            assert_eq!(
                stored.layout_mismatch(&requested),
                Some(FormatLayoutMismatch {
                    field,
                    stored: stored_value,
                    requested: requested_value,
                })
            );
        }
    }

    #[test]
    fn mutable_merge_preserves_stored_layout() {
        let stored = Format::default();
        let requested = Format {
            name:         "renamed".to_string(),
            chunk_size:   stored.chunk_size + 1,
            block_size:   stored.block_size + 1,
            page_size:    stored.page_size + 1,
            max_capacity: Some(1024),
            max_inodes:   Some(128),
        };

        let merged = stored.clone().merge_mutable_from(requested);
        assert_eq!(merged.name, "renamed");
        assert_eq!(merged.max_capacity, Some(1024));
        assert_eq!(merged.max_inodes, Some(128));
        assert_eq!(merged.chunk_size, stored.chunk_size);
        assert_eq!(merged.block_size, stored.block_size);
        assert_eq!(merged.page_size, stored.page_size);
    }
}
