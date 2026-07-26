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

use kiseki_types::{ToErrno, setting::FormatLayoutField};
use snafu::{Location, Snafu};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    Unknown {
        #[snafu(implicit)]
        location: Location,
        source:   Box<dyn std::error::Error + Send + Sync>,
    },
    UnsupportedMetaDSN {
        #[snafu(implicit)]
        location: Location,
        dsn:      String,
    },

    TokioJoinError {
        #[snafu(implicit)]
        location: Location,
        source:   tokio::task::JoinError,
    },

    #[cfg(feature = "meta-rocksdb")]
    RocksdbError {
        #[snafu(implicit)]
        location: Location,
        source:   rocksdb::Error,
    },

    // Model Error
    #[snafu(display("Model error: {:?}, {:?}", source, location))]
    ModelError {
        #[snafu(implicit)]
        location: Location,
        source:   model_err::Error,
    },

    // Setting
    #[snafu(display("FileSystem has not been initialized yet. Location: {}", location))]
    UninitializedEngine {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("FileSystem has already been initialized. Location: {}", location))]
    AlreadyInitialized {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Cannot change immutable format field {field} from {stored} to {requested}. Location: \
         {location}"
    ))]
    IncompatibleFormat {
        #[snafu(implicit)]
        location:  Location,
        field:     FormatLayoutField,
        stored:    usize,
        requested: usize,
    },

    #[snafu(display("Invalid setting: {:?}, {:?}", String::from_utf8_lossy(key.as_slice()).to_string(), location))]
    InvalidSetting {
        #[snafu(implicit)]
        location: Location,
        key:      Vec<u8>,
    },

    LibcError {
        #[snafu(implicit)]
        location: Location,
        errno:    libc::c_int,
    },
}

impl Error {
    pub const fn is_not_found(&self) -> bool {
        matches!(self, Self::ModelError { source, .. } if source.is_not_found())
    }
}

pub mod model_err {
    use snafu::Snafu;

    /// Errors produced by the typed KV layer (`backend::kv`), which is generic
    /// over the key type. `key` is the hex-encoded raw key, for diagnostics.
    #[derive(Debug, Snafu)]
    #[snafu(visibility(pub))]
    pub enum Error {
        #[snafu(display("record not found: {key}"))]
        Missing { key: String },
        #[snafu(display("record corrupted: {key}: {reason}"))]
        Corrupt { key: String, reason: String },
    }

    impl Error {
        pub const fn is_not_found(&self) -> bool { matches!(self, Self::Missing { .. }) }
    }
}

impl ToErrno for Error {
    fn to_errno(&self) -> libc::c_int {
        match self {
            Self::Unknown { .. } => libc::EINTR,
            Self::UnsupportedMetaDSN { .. } => libc::EINTR,
            Self::TokioJoinError { .. } => libc::EINTR,
            #[cfg(feature = "meta-rocksdb")]
            Self::RocksdbError { .. } => libc::EINTR,
            Self::ModelError { source, .. } => {
                if source.is_not_found() {
                    libc::ENOENT
                } else {
                    libc::EINTR
                }
            }
            Self::UninitializedEngine { .. } => libc::EINTR,
            Self::AlreadyInitialized { .. } => libc::EEXIST,
            Self::IncompatibleFormat { .. } => libc::EINVAL,
            Self::InvalidSetting { .. } => libc::EINTR,
            Self::LibcError { errno, .. } => *errno,
        }
    }
}
