// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    convert::Infallible,
    fmt::{Debug, Formatter},
    path::{Path, PathBuf},
    str::FromStr,
};

use clap::{ArgAction, Args};
use fuser::MountOption;
use kiseki_common::KISEKI;
use kiseki_fuse::{FuseConfig, null};
use kiseki_meta::MetaConfig;
use kiseki_utils::{logger::LoggingOptions, object_storage::ObjectStorageConfig};
use kiseki_vfs::{Config as VFSConfig, KisekiVFS};
use snafu::{ResultExt, Whatever, whatever};
use tracing::info;

use crate::build_info;

const MOUNT_OPTIONS_HEADER: &str = "Mount options";
const LOGGING_OPTIONS_HEADER: &str = "Logging options";
const META_OPTIONS_HEADER: &str = "Meta options";
const STORAGE_OPTIONS_HEADER: &str = "Object storage options";
const CACHE_OPTIONS_HEADER: &str = "Cache options";

#[derive(Clone)]
pub struct ObjectStorageDsn(String);

impl Debug for ObjectStorageDsn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("ObjectStorageDsn(<redacted>)")
    }
}

impl FromStr for ObjectStorageDsn {
    type Err = Infallible;

    fn from_str(dsn: &str) -> Result<Self, Self::Err> { Ok(Self(dsn.to_string())) }
}

#[derive(Debug, Clone, Args)]
#[command(flatten_help = true)]
#[command(long_about = r"

Mount the target volume at the mount point.
Examples:

# Mount in foreground
kiseki mount -f /tmp/kiseki
")]
pub struct MountArgs {
    #[arg(
        help = "Directory to mount the fs at",
        value_name = "MOUNT_POINT",
        default_value = "/tmp/kiseki"
    )]
    pub mount_point: PathBuf,

    #[arg(
    long,
    help = "Mount file system in read-only mode",
    help_heading = MOUNT_OPTIONS_HEADER
    )]
    pub read_only: bool,

    #[arg(
    long,
    help = "Automatically unmount on exit",
    help_heading = MOUNT_OPTIONS_HEADER,
    default_value = "true",
    action = ArgAction::Set,
    )]
    pub auto_unmount: bool,

    #[arg(long, help = "Allow root user to access file system", help_heading = MOUNT_OPTIONS_HEADER)]
    pub allow_root: bool,

    #[arg(
    long,
    help = "Allow other users, including root, to access file system",
    help_heading = MOUNT_OPTIONS_HEADER,
    conflicts_with = "allow_root",
    default_value_t = true,
    action = ArgAction::Set,
    )]
    pub allow_other: bool,

    #[arg(
    long,
    help = "Number of threads to use for tokio async runtime",
    help_heading = MOUNT_OPTIONS_HEADER,
    default_value = "10",
    )]
    pub async_work_threads: usize,

    #[clap(
    long,
    help = "Write log files to a directory [default: logs written to syslog]",
    help_heading = LOGGING_OPTIONS_HEADER,
    value_name = "DIRECTORY",
    default_value = "/tmp/kiseki.log"
    )]
    pub log_directory: String,

    #[clap(
    short,
    long,
    help = "Log level",
    help_heading = LOGGING_OPTIONS_HEADER,
    value_name = "LEVEL",
    default_value = "info"
    )]
    pub level: Option<String>,

    #[clap(
    long,
    help = "Enable OTLP tracing",
    help_heading = LOGGING_OPTIONS_HEADER,
    default_value = "true"
    )]
    pub enable_otlp_tracing: bool,

    #[clap(
    long,
    help = "Specify the OTLP endpoint",
    help_heading = LOGGING_OPTIONS_HEADER,
    value_name = "URL",
    default_value = "localhost:4317",
    )]
    pub otlp_endpoint: Option<String>,

    #[clap(
    long,
    help = "Specify the tracing sample ratio",
    help_heading = LOGGING_OPTIONS_HEADER,
    default_value = "0.5",
    value_name = "RATIO",
    )]
    pub tracing_sample_ratio: Option<f64>,

    #[clap(
    long,
    help = "Append stdout to log files",
    help_heading = LOGGING_OPTIONS_HEADER,
    default_value = "true",
    )]
    pub append_stdout: bool,

    #[clap(
    long,
    help = "Disable all logging. You will still see stdout messages.",
    help_heading = LOGGING_OPTIONS_HEADER,
    conflicts_with_all(["log_directory", "level", "enable_otlp_tracing"])
    )]
    pub no_log: bool,

    #[clap(
        short,
        long,
        help = "Run as foreground process",
        default_value = "true"
    )]
    pub foreground: bool,

    #[arg(
    long,
    help = "Specify the address of the meta store",
    help_heading = META_OPTIONS_HEADER,
    default_value = kiseki_common::KISEKI_DEBUG_META_ADDR,
    )]
    pub meta_dsn: String,

    #[arg(
        long,
        value_name = "DSN",
        help = "Object store: file:///absolute/path or s3://bucket[/prefix]",
        long_help = "Object store DSN. Use file:///absolute/path for local storage or \
                     s3://bucket[/prefix] for S3. S3 credentials are loaded from the standard \
                     AWS environment or identity chain; credentials in the DSN are rejected.",
        help_heading = STORAGE_OPTIONS_HEADER,
        required = true,
    )]
    pub object_storage: ObjectStorageDsn,

    #[arg(
        long,
        value_name = "DIRECTORY",
        help = "Directory for restart-recoverable staged blocks",
        help_heading = CACHE_OPTIONS_HEADER,
        default_value = kiseki_common::KISEKI_DEBUG_STAGE_CACHE,
    )]
    pub stage_cache_dir: PathBuf,
}

impl MountArgs {
    fn fuse_config(&self) -> FuseConfig {
        let mut options = vec![
            MountOption::DefaultPermissions,
            MountOption::FSName(KISEKI.to_string()),
            MountOption::NoAtime,
        ];
        if self.read_only {
            options.push(MountOption::RO);
        }
        if self.auto_unmount {
            options.push(MountOption::AutoUnmount);
        }
        if self.allow_root {
            options.push(MountOption::AllowRoot);
        }
        if self.allow_other {
            options.push(MountOption::AllowOther);
        }
        FuseConfig {
            mount_point:        self.mount_point.clone(),
            mount_options:      options,
            async_work_threads: self.async_work_threads,
        }
    }

    fn meta_config(&self) -> Result<MetaConfig, Whatever> {
        let mut mc = MetaConfig::default();
        mc.with_dsn(&self.meta_dsn);
        Ok(mc)
    }

    fn load_logging_opts(&self) -> Option<LoggingOptions> {
        if self.no_log {
            return None;
        }
        let opts = LoggingOptions {
            dir:                  self.log_directory.clone(),
            level:                self.level.clone(),
            enable_otlp_tracing:  self.enable_otlp_tracing,
            otlp_endpoint:        self.otlp_endpoint.clone(),
            tracing_sample_ratio: self.tracing_sample_ratio,
            append_stdout:        self.append_stdout,
            tokio_console_addr:   Some(
                kiseki_utils::logger::DEFAULT_TOKIO_CONSOLE_ADDR.to_string(),
            ),
        };
        Some(opts)
    }

    fn vfs_config(&self) -> Result<VFSConfig, Whatever> {
        let object_storage = self
            .object_storage
            .0
            .parse::<ObjectStorageConfig>()
            .with_whatever_context(|error| {
                format!("invalid object storage configuration: {error}")
            })?;
        if matches!(object_storage, ObjectStorageConfig::Memory) {
            whatever!("memory object storage is available only in tests");
        }
        Ok(VFSConfig {
            object_storage,
            stage_cache_dir: self.stage_cache_dir.clone(),
            ..VFSConfig::default()
        })
    }

    pub fn run(self) -> Result<(), Whatever> {
        // the `setup_panic!` expansion still uses the deprecated
        // `std::panic::PanicInfo` alias; nothing to fix on our side.
        #[allow(deprecated)]
        {
            human_panic::setup_panic!();
        }
        kiseki_utils::panic_hook::set_panic_hook();

        if self.foreground {
            let (_guard, _sentry_guard) = if let Some(opts) = self.load_logging_opts() {
                kiseki_utils::logger::init_global_logging_without_runtime("kiseki-fuse", &opts)
            } else {
                (vec![], None)
            };

            let pyroscope_guard = kiseki_utils::pyroscope_init::init_pyroscope()?;

            mount(self)?;

            if let Some(agent_running) = pyroscope_guard {
                // Stop Agent
                let agent_ready = agent_running
                    .stop()
                    .with_whatever_context(|e| format!("failed to stop pyroscope agent {e} "))?;

                // Shutdown the Agent
                agent_ready.shutdown();
            }
        }
        Ok(())
    }
}

pub fn print_versions() {
    // Report app version as gauge.
    // APP_VERSION
    //     .with_label_values(&[short_version(), full_version()])
    //     .inc();

    // Report the build version without dumping process arguments.
    println!(
        "PKG_VERSION: {}, FULL_VERSION: {}",
        build_info::PKG_VERSION,
        build_info::FULL_VERSION,
    );
}

fn mount(args: MountArgs) -> Result<(), Whatever> {
    info!("try to mount kiseki on {:?}", &args.mount_point);
    print_versions();

    let fuse_config = args.fuse_config();
    let meta_config = args.meta_config()?;
    let vfs_config = args.vfs_config()?;

    validate_mount_point(&args.mount_point)?;

    let meta = kiseki_meta::open(meta_config)
        .with_whatever_context(|e| format!("failed to open meta, {e:?}"))?;
    let startup_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .with_whatever_context(|error| format!("failed to build startup runtime: {error}"))?;
    let file_system = startup_runtime
        .block_on(KisekiVFS::new_checked(vfs_config, meta))
        .with_whatever_context(|e| format!("failed to create file system, {e:?}"))?;
    drop(startup_runtime);

    let fs = kiseki_fuse::KisekiFuse::create(fuse_config.clone(), file_system)?;
    fuser::mount2(fs, &args.mount_point, &fuse_config.mount_options).with_whatever_context(
        |e| {
            format!(
                "failed to mount kiseki on {}; {}",
                args.mount_point.display(),
                e
            )
        },
    )?;
    Ok(())
}

fn validate_mount_point(path: impl AsRef<Path>) -> Result<(), Whatever> {
    let mount_point = path.as_ref();
    if !mount_point.exists() {
        whatever!("mount point {} does not exist", mount_point.display());
    }

    if !mount_point.is_dir() {
        whatever!("mount point {} is not a directory", mount_point.display());
    }

    #[cfg(target_os = "linux")]
    {
        use procfs::process::Process;

        // This is a best-effort validation, so don't fail if we can't read
        // /proc/self/mountinfo for some reason.
        let mounts = match Process::myself().and_then(|me| me.mountinfo()) {
            Ok(mounts) => mounts,
            Err(e) => {
                tracing::debug!(
                    "failed to read mountinfo, not checking for existing mounts: {e:?}"
                );
                return Ok(());
            }
        };

        if mounts
            .into_iter()
            .any(|mount| mount.mount_point == path.as_ref())
        {
            whatever!("mount point {} is already mounted", path.as_ref().display());
        }
    }

    null::mount_check(path)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[derive(Parser)]
    struct TestCli {
        #[command(flatten)]
        mount: MountArgs,
    }

    #[test]
    fn object_storage_cli_value_reaches_vfs_config() {
        let cli = TestCli::try_parse_from([
            "test",
            "--object-storage",
            "s3://volume-bucket/tenant/volume?region=test-region",
            "/tmp/kiseki",
        ])
        .expect("parse mount arguments");

        assert_eq!(
            cli.mount.vfs_config().unwrap().object_storage,
            ObjectStorageConfig::S3 {
                bucket:     "volume-bucket".to_string(),
                prefix:     Some("tenant/volume".to_string()),
                region:     Some("test-region".to_string()),
                endpoint:   None,
                allow_http: false,
            }
        );
    }

    #[test]
    fn object_storage_is_required_and_invalid_dsns_fail_before_mounting() {
        assert!(TestCli::try_parse_from(["test", "/tmp/kiseki"]).is_err());
        let invalid = TestCli::try_parse_from([
            "test",
            "--object-storage",
            "file://relative/path",
            "/tmp/kiseki",
        ])
        .unwrap();
        assert!(invalid.mount.vfs_config().is_err());

        let memory =
            TestCli::try_parse_from(["test", "--object-storage", "memory://", "/tmp/kiseki"])
                .unwrap();
        assert!(memory.mount.vfs_config().is_err());
    }

    #[test]
    fn mount_argument_debug_never_contains_storage_dsn_values() {
        let secret_marker = "do-not-echo-this-value";
        let dsn = format!("s3://user:{secret_marker}@volume-bucket/prefix");
        let cli =
            TestCli::try_parse_from(["test", "--object-storage", &dsn, "/tmp/kiseki"]).unwrap();

        assert!(!format!("{:?}", cli.mount).contains(secret_marker));
        let error = cli.mount.vfs_config().unwrap_err();
        assert!(!error.to_string().contains(secret_marker));
    }

    #[test]
    fn mounted_tests_can_disable_allow_other_and_isolate_the_stage_cache() {
        let cli = TestCli::try_parse_from([
            "test",
            "--auto-unmount",
            "false",
            "--object-storage",
            "file:///tmp/kiseki-objects",
            "--allow-other",
            "false",
            "--stage-cache-dir",
            "/tmp/kiseki-stage-isolated",
            "/tmp/kiseki",
        ])
        .expect("parse isolated mount arguments");

        assert!(!cli.mount.auto_unmount);
        assert!(!cli.mount.allow_other);
        assert_eq!(
            cli.mount.vfs_config().unwrap().stage_cache_dir,
            PathBuf::from("/tmp/kiseki-stage-isolated")
        );
    }
}
