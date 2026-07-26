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

use std::{str::FromStr, sync::LazyLock};

use clap::Args;
use kiseki_types::setting::Format;
use kiseki_utils::{
    align::{align_to_block, align4k},
    readable_size::ReadableSize,
};
use regex::Regex;
use snafu::{OptionExt, ResultExt, Whatever};
use tracing::info;

const FORMAT_OPTIONS_HEADER: &str = "DATA FORMAT";
const MANAGEMENT_OPTIONS_HEADER: &str = "MANAGEMENT";

#[derive(Debug, Clone, Args)]
#[command(args_conflicts_with_subcommands = true)]
#[command(flatten_help = true)]
#[command(long_about = r"

Create a new KisekiFS volume. Here META_ADDRESS is used to set up the
metadata engine location, the SCHEME is used to specific the meta sto
engine, and NAME is the prefix of all objects in data storage.
")]
pub struct FormatArgs {
    #[arg(
    value_name = "FILE_SYSTEM_NAME",
    help = r"Your file system name, like 'demo-fs'.",
    value_parser = validate_name,
    default_value = "hello-fs",
    )]
    pub name: String,

    #[arg(
    long,
    help = "Specify the address of the meta store",
    help_heading = FORMAT_OPTIONS_HEADER,
    default_value = kiseki_common::KISEKI_DEBUG_META_ADDR,
    )]
    pub meta_dsn: Option<String>,

    #[arg(long, short, help = "overwrite existing format", help_heading = FORMAT_OPTIONS_HEADER)]
    pub force: bool,

    // #[arg(long, help = "compression algorithm", help_heading = FORMAT_OPTIONS_HEADER)]
    // pub compression: Option<Compression>,
    #[arg(
    long,
    help = "size of block in KiB",
    help_heading = FORMAT_OPTIONS_HEADER,
    default_value = "4M",
    value_parser = validate_block_size,
    )]
    pub block_size: ReadableSize,

    #[arg(
    long,
    short,
    help = "hard quota of the volume limiting its usage of space in GiB",
    help_heading = MANAGEMENT_OPTIONS_HEADER,
    value_parser = validate_capacity,
    )]
    pub capacity: Option<ReadableSize>,

    #[arg(
    long,
    short,
    help = "hard quota of the volume limiting its number of inodes",
    help_heading = MANAGEMENT_OPTIONS_HEADER,
    )]
    pub inodes: Option<usize>,
}

impl FormatArgs {
    fn generate_format(&self) -> Format {
        let mut format = Format {
            max_capacity: self
                .capacity
                .map(kiseki_utils::readable_size::ReadableSize::as_bytes_usize),
            block_size: self.block_size.as_bytes_usize(),
            name: self.name.clone(),
            ..Default::default()
        };
        if let Some(inodes) = self.inodes {
            format.max_inodes = Some(inodes);
        }
        format
    }

    pub fn run(&self) -> Result<(), Whatever> {
        kiseki_utils::logger::install_fmt_log();
        let dsn = self
            .meta_dsn
            .as_deref()
            .whatever_context("metadata DSN is required")?;
        let format = self.generate_format();
        kiseki_meta::update_format(dsn, format, self.force)
            .with_whatever_context(|error| format!("failed to format metadata store: {error}"))?;
        info!("format file system {:?} success", self.name);
        Ok(())
    }
}

const NAME_PATTERN: &str = r"^[a-z0-9][a-z0-9\-]{1,61}[a-z0-9]$";
static NAME_REGEX: LazyLock<Result<Regex, regex::Error>> =
    LazyLock::new(|| Regex::new(NAME_PATTERN));

// Validation function for file system names
fn validate_name(name: &str) -> Result<String, String> {
    if name.len() <= 3 {
        return Err(format!("File system name {name:?} is too short"));
    }
    if name.len() >= 30 {
        return Err(format!("File system name {name:?} is too long"));
    }
    let reg = NAME_REGEX
        .as_ref()
        .map_err(|error| format!("invalid built-in name pattern: {error}"))?;
    if !reg.is_match(name) {
        return Err(format!("File system name {name:?} is invalid"));
    }

    Ok(name.to_string())
}

fn validate_block_size(s: &str) -> Result<ReadableSize, String> {
    let n = ReadableSize::from_str(s).map_err(|e| format!("invalid block size: {e}"))?;
    let n = n.as_bytes() as usize;
    let n = align_to_block(n);
    if n < kiseki_common::MIN_BLOCK_SIZE {
        return Err(format!("block size {n} too small"));
    }
    if n > kiseki_common::MAX_BLOCK_SIZE {
        return Err(format!("block size {n} too large"));
    }
    Ok(ReadableSize(n as u64))
}

fn validate_capacity(s: &str) -> Result<ReadableSize, String> {
    let n = ReadableSize::from_str(s).map_err(|e| format!("invalid capacity: {e}"))?;
    let n = n.as_bytes() as usize;

    Ok(ReadableSize(align4k(n as u64) as u64))
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    use super::*;

    static TEST_ID: AtomicU64 = AtomicU64::new(0);

    struct TestStore(PathBuf);

    impl TestStore {
        fn new() -> Self {
            let id = TEST_ID.fetch_add(1, Ordering::Relaxed);
            Self(
                std::env::temp_dir()
                    .join(format!("kisekifs-format-test-{}-{id}", std::process::id())),
            )
        }

        fn dsn(&self) -> String { format!("rocksdb://:{}", self.0.display()) }
    }

    impl Drop for TestStore {
        fn drop(&mut self) {
            if let Err(error) = fs::remove_dir_all(&self.0)
                && error.kind() != std::io::ErrorKind::NotFound
            {
                panic!("remove metadata test directory: {error}");
            }
        }
    }

    fn args(store: &TestStore, name: &str, force: bool, block_size: usize) -> FormatArgs {
        FormatArgs {
            name: name.to_string(),
            meta_dsn: Some(store.dsn()),
            force,
            block_size: ReadableSize(block_size as u64),
            capacity: None,
            inodes: None,
        }
    }

    #[test]
    fn run_propagates_lifecycle_errors_and_honors_force() {
        let store = TestStore::new();
        let initial = args(&store, "initial-volume", false, kiseki_common::BLOCK_SIZE);
        initial.run().expect("initialize volume");

        let repeated = args(&store, "repeated-volume", false, kiseki_common::BLOCK_SIZE);
        assert!(repeated.run().is_err());

        let forced = args(&store, "renamed-volume", true, kiseki_common::BLOCK_SIZE);
        forced.run().expect("force mutable update");

        let incompatible = args(
            &store,
            "renamed-volume",
            true,
            kiseki_common::BLOCK_SIZE * 2,
        );
        assert!(incompatible.run().is_err());
    }

    #[test]
    fn run_reports_missing_metadata_dsn() {
        let mut args = args(
            &TestStore::new(),
            "missing-meta-dsn",
            false,
            kiseki_common::BLOCK_SIZE,
        );
        args.meta_dsn = None;

        assert!(args.run().is_err());
    }
}
