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

use std::{error::Error, str::FromStr};

use snafu::{ResultExt, Whatever, whatever};

/// Reads an environment variable for the current process.
///
/// Compared to [std::env::var] there are a couple of differences:
///
/// - [var] uses [dotenvy] which loads the `.env` file from the current or
///   parent directories before returning the value.
///
/// - [var] returns `Ok(None)` (instead of `Err`) if an environment variable
///   wasn't set.
#[track_caller]
pub fn var(key: &str) -> Result<Option<String>, Whatever> {
    match dotenvy::var(key) {
        Ok(content) => Ok(Some(content)),
        Err(dotenvy::Error::EnvVar(std::env::VarError::NotPresent)) => Ok(None),
        Err(error) => Err(error)
            .with_whatever_context(|_| format!("Failed to read {key} environment variable")),
    }
}

/// Reads an environment variable for the current process, and parses it if
/// it is set.
///
/// Compared to [std::env::var] there are a couple of differences:
///
/// - [var] uses [dotenvy] which loads the `.env` file from the current or
///   parent directories before returning the value.
///
/// - [var] returns `Ok(None)` (instead of `Err`) if an environment variable
///   wasn't set.
#[track_caller]
pub fn var_parsed<R>(key: &str) -> Result<Option<R>, Whatever>
where
    R: FromStr,
    R::Err: Error + Send + Sync + 'static,
{
    match var(key) {
        Ok(Some(content)) => Ok(Some(content.parse().with_whatever_context(|e| {
            format!("Failed to parse {key} environment variable; {e}")
        })?)),
        Ok(None) => Ok(None),
        Err(error) => Err(error),
    }
}

/// Reads an environment variable for the current process, and fails if it was
/// not found.
///
/// Compared to [std::env::var] there are a couple of differences:
///
/// - [var] uses [dotenvy] which loads the `.env` file from the current or
///   parent directories before returning the value.
#[track_caller]
pub fn required_var(key: &str) -> Result<String, Whatever> { required(var(key), key) }

fn required<T>(res: Result<Option<T>, Whatever>, key: &str) -> Result<T, Whatever> {
    match res {
        Ok(Some(value)) => Ok(value),
        Ok(None) => {
            whatever!("Failed to find required {key} environment variable");
        }
        Err(error) => Err(error),
    }
}
