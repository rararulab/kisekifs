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

use snafu::{Location, Snafu};

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum Error {
    ObjectStorageError {
        #[snafu(implicit)]
        location: Location,
        source:   kiseki_utils::object_storage::ObjectStorageError,
    },

    CacheError {
        error: String,
    },

    UnknownIOError {
        #[snafu(implicit)]
        location: Location,
        source:   std::io::Error,
    },

    JoinErr {
        #[snafu(implicit)]
        location: Location,
        source:   tokio::task::JoinError,
    },

    FlushBlockFailed {
        #[snafu(implicit)]
        location: Location,
    },

    InvalidSliceBufferWriteOffset {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "invalid {subject} range: offset {offset} + length {length} exceeds bound {bound}"
    ))]
    InvalidRange {
        subject:  &'static str,
        offset:   usize,
        length:   usize,
        bound:    usize,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "invalid page pool configuration: page size {page_size}, capacity {capacity}"
    ))]
    InvalidPagePoolConfig {
        page_size: usize,
        capacity:  usize,
        #[snafu(implicit)]
        location:  Location,
    },

    #[snafu(display("unexpected {subject} length: expected {expected}, got {actual}"))]
    UnexpectedLength {
        subject:  &'static str,
        expected: usize,
        actual:   usize,
        #[snafu(implicit)]
        location: Location,
    },

    DiskPoolMmapError {
        #[snafu(implicit)]
        location: Location,
        source:   fmmap::error::Error,
    },

    #[snafu(display("no more space in cache dir {}", cache_dir))]
    ErrStageNoMoreSpace {
        cache_dir: String,
        #[snafu(implicit)]
        location:  Location,
    },
}

impl Error {
    pub const fn is_not_found(&self) -> bool {
        matches!(self, Self::ObjectStorageError { source, .. } if kiseki_utils::object_storage::is_not_found_error(source))
    }
}

pub type Result<T> = std::result::Result<T, Error>;
