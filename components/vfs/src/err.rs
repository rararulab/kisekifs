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

use kiseki_types::slice::SliceKey;
use snafu::{Location, Snafu};
use tracing::error;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum Error {
    JoinErr {
        #[snafu(implicit)]
        location: Location,
        source:   tokio::task::JoinError,
    },
    ObjectStorage {
        #[snafu(implicit)]
        location: Location,
        source:   kiseki_utils::object_storage::ObjectStorageError,
    },
    ObjectStorageConfig {
        #[snafu(implicit)]
        location: Location,
        source:   kiseki_utils::object_storage::ObjectStorageConfigError,
    },

    #[snafu(display("invalid mount configuration: {reason}"))]
    InvalidConfig {
        reason:   String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("mount is draining and no longer accepts background work"))]
    MountDraining {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("failed to prepare cache root {}: {source}", path.display()))]
    CacheRoot {
        path:     std::path::PathBuf,
        #[snafu(implicit)]
        location: Location,
        source:   std::io::Error,
    },

    ObjectBlockNotFound {
        #[snafu(implicit)]
        location: Location,
        key:      SliceKey,
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

    #[snafu(display("unexpected {subject} length: expected {expected}, got {actual}"))]
    UnexpectedLength {
        subject:  &'static str,
        expected: usize,
        actual:   usize,
        #[snafu(implicit)]
        location: Location,
    },

    Storage {
        source: kiseki_storage::err::Error,
    },
    Meta {
        source: kiseki_meta::Error,
    },

    // ====VFS====
    Libc {
        errno:    libc::c_int,
        #[snafu(implicit)]
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<kiseki_meta::Error> for Error {
    fn from(value: kiseki_meta::Error) -> Self { Self::Meta { source: value } }
}

impl From<kiseki_storage::err::Error> for Error {
    fn from(value: kiseki_storage::err::Error) -> Self { Self::Storage { source: value } }
}

impl kiseki_types::ToErrno for Error {
    fn to_errno(&self) -> kiseki_types::Errno {
        match self {
            Self::Libc { errno, .. } => {
                error!("libc error: {errno}");
                *errno
            }
            Self::Meta { source } => {
                error!("meta error: {source}");
                source.to_errno()
            }
            Self::Storage { source } if source.is_no_space() => libc::ENOSPC,
            _ => {
                // a panic here would wedge the FUSE mount; EIO is the honest
                // fallback for storage/join errors without a precise mapping.
                error!("unhandled error type in to_errno {self}");
                libc::EIO
            }
        }
    }
}
