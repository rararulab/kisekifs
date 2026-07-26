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

pub use sentry::release_name;
use sentry::{ClientInitGuard, IntoDsn, types::Dsn};
use snafu::{ResultExt, Whatever};
use tracing::error;

use crate::env::{required_var, var, var_parsed};

#[derive(Debug)]
pub struct SentryConfig {
    pub dsn:                Option<Dsn>,
    pub environment:        Option<String>,
    pub release:            Option<String>,
    pub traces_sample_rate: f32,
}

impl SentryConfig {
    pub fn from_environment() -> Result<Self, Whatever> {
        let dsn = var("SENTRY_DSN")?.into_dsn().with_whatever_context(|e| {
            format!("SENTRY_DSN_API is not a valid Sentry DSN value {e}")
        })?;

        let environment =
            match dsn {
                None => None,
                Some(_) => Some(required_var("SENTRY_ENVIRONMENT").with_whatever_context(
                    |_| "SENTRY_ENV_API must be set when using SENTRY_DSN_API",
                )?),
            };

        Ok(Self {
            dsn,
            environment,
            release: var("SENTRY_RELEASE")?,
            traces_sample_rate: var_parsed("SENTRY_TRACES_SAMPLE_RATE")?.unwrap_or(1.0),
        })
    }
}

/// Initializes the Sentry SDK from the environment variables.
///
/// If `SENTRY_DSN_API` is not set then Sentry will not be initialized,
/// otherwise it is required to be a valid DSN string. `SENTRY_ENV_API` must
/// be set if a DSN is provided.
///
/// `HEROKU_SLUG_COMMIT`, if present, will be used as the `release` property
/// on all events.
#[must_use]
pub fn init_sentry() -> Option<ClientInitGuard> {
    let config = match SentryConfig::from_environment() {
        Ok(config) => config,
        Err(error) => {
            error!(
                "Failed to read Sentry configuration from environment: {}",
                error
            );
            return None;
        }
    };

    println!("found sentry config: {config:?}");

    let opts = sentry::ClientOptions {
        auto_session_tracking: true,
        dsn: config.dsn,
        environment: config.environment.map(Into::into),
        release: config.release.map(Into::into),
        session_mode: sentry::SessionMode::Request,
        traces_sample_rate: config.traces_sample_rate,
        ..Default::default()
    };

    let guard = sentry::init(opts);
    Some(guard)
}
