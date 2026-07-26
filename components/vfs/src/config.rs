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

use std::{
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};

use fs4::FileExt;
use kiseki_common::{BLOCK_SIZE, CHUNK_SIZE, PAGE_BUFFER_SIZE, PAGE_SIZE};
use kiseki_types::setting::Format;
use kiseki_utils::{object_storage::ObjectStorageConfig, readable_size::ReadableSize};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, ensure};

use crate::err::{CacheRootSnafu, InvalidConfigSnafu, Result};

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub enum ShutdownPolicy {
    #[default]
    LocalDurable,
    RemoteDurable,
}

impl FromStr for ShutdownPolicy {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "local" => Ok(Self::LocalDurable),
            "remote" => Ok(Self::RemoteDurable),
            _ => Err("expected 'local' or 'remote'".to_string()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub prefix_internal: bool,
    pub hide_internal:   bool,

    // attributes cache timeout in seconds
    pub attr_timeout:       Duration,
    // dir entry cache timeout in seconds
    pub dir_entry_timeout:  Duration,
    // file entry cache timeout in seconds
    pub file_entry_timeout: Duration,

    // ========Object Storage Configs ===>
    pub object_storage: ObjectStorageConfig,

    // ========Cache Configs ===>
    /// Explicit, mount-specific root for every mutable cache resource.
    pub cache_dir:                  PathBuf,
    pub memory_page_capacity:       ReadableSize,
    pub disk_page_capacity:         Option<ReadableSize>,
    pub stage_cache_capacity:       ReadableSize,
    pub stage_cache_ttl:            Duration,
    pub memory_read_cache_capacity: ReadableSize,
    pub shutdown_deadline:          Duration,
    pub shutdown_policy:            ShutdownPolicy,

    // ========Buffer configs ===>
    /// chunk_size is the max size can one buffer
    /// hold no matter it is for reading or writing.
    pub chunk_size: usize,
    /// block_size is the max size when we upload
    /// the data to the cloud.
    ///
    /// When the data is not enough to fill the block,
    /// then the block size is equal to the data size,
    /// for example, the last block of the file.
    pub block_size: usize,
    /// The page_size can be also called as the MIN_BLOCK_SIZE,
    /// which is the min size of the block.
    ///
    /// And under the hood, the block is divided into pages.
    pub page_size:  usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            prefix_internal:            false,
            hide_internal:              false,
            attr_timeout:               Duration::from_secs(1),
            dir_entry_timeout:          Duration::from_secs(1),
            file_entry_timeout:         Duration::from_secs(1),
            object_storage:             ObjectStorageConfig::Memory,
            cache_dir:                  PathBuf::new(),
            memory_page_capacity:       ReadableSize(PAGE_BUFFER_SIZE as u64),
            disk_page_capacity:         None,
            stage_cache_capacity:       ReadableSize(
                kiseki_storage::cache::file_cache::DEFAULT_STAGE_CACHE_SIZE,
            ),
            stage_cache_ttl:            kiseki_storage::cache::file_cache::DEFAULT_CACHE_TTL,
            memory_read_cache_capacity: ReadableSize::gb(1),
            shutdown_deadline:          Duration::from_secs(30),
            shutdown_policy:            ShutdownPolicy::default(),
            chunk_size:                 CHUNK_SIZE, // 64MB
            block_size:                 BLOCK_SIZE, // 4MB
            page_size:                  PAGE_SIZE,  // 64KB
        }
    }
}

pub(crate) struct PreparedCacheConfig {
    root: PathBuf,
    stage_dir: PathBuf,
    pub(crate) disk_page_pool_path: PathBuf,
    pub(crate) memory_page_capacity: usize,
    pub(crate) disk_page_capacity: Option<usize>,
    pub(crate) stage_cache_capacity: ReadableSize,
    pub(crate) stage_cache_ttl: Duration,
    pub(crate) memory_read_cache_capacity: ReadableSize,
    _lease: File,
    _hierarchy_leases: Vec<File>,
}

impl PreparedCacheConfig {
    pub(crate) fn root(&self) -> &Path { &self.root }

    pub(crate) fn stage_dir(&self) -> &Path { &self.stage_dir }
}

impl Config {
    pub fn validate_mount_paths(
        &self,
        mount_point: &Path,
        ready_file: Option<&Path>,
    ) -> Result<()> {
        self.validate_resource_config()?;
        let mut paths = vec![
            ("mount point", mount_point),
            ("cache directory", self.cache_dir.as_path()),
        ];
        if let ObjectStorageConfig::File { root } = &self.object_storage {
            paths.push(("file object storage root", root.as_path()));
        }
        if let Some(ready_file) = ready_file {
            paths.push(("ready file", ready_file));
        }

        let resolved = paths
            .iter()
            .map(|(name, path)| {
                validate_absolute_clean_path(name, path)?;
                resolve_path_for_overlap(path).map(|path| (*name, path))
            })
            .collect::<Result<Vec<_>>>()?;
        for (index, (left_name, left)) in resolved.iter().enumerate() {
            for (right_name, right) in &resolved[index + 1..] {
                ensure!(
                    !paths_overlap(left, right),
                    InvalidConfigSnafu {
                        reason: format!(
                            "{left_name} and {right_name} must not overlap ({} and {})",
                            left.display(),
                            right.display()
                        ),
                    }
                );
            }
        }
        Ok(())
    }

    pub(crate) fn prepare(&self, stored_format: &Format) -> Result<PreparedCacheConfig> {
        self.validate_layout(stored_format)?;
        let (memory_page_capacity, disk_page_capacity) = self.validate_resource_config()?;
        self.validate_storage_cache_paths()?;

        self.prepare_cache_paths(memory_page_capacity, disk_page_capacity)
    }

    pub(crate) fn validate_storage_cache_paths(&self) -> Result<()> {
        let ObjectStorageConfig::File { root } = &self.object_storage else {
            return Ok(());
        };
        validate_absolute_clean_path("cache directory", &self.cache_dir)?;
        validate_absolute_clean_path("file object storage root", root)?;
        let cache = resolve_path_for_overlap(&self.cache_dir)?;
        let objects = resolve_path_for_overlap(root)?;
        ensure!(
            !paths_overlap(&cache, &objects),
            InvalidConfigSnafu {
                reason: format!(
                    "cache directory and file object storage root must not overlap ({} and {})",
                    cache.display(),
                    objects.display()
                ),
            }
        );
        Ok(())
    }

    fn validate_resource_config(&self) -> Result<(usize, Option<usize>)> {
        let memory_page_capacity =
            self.valid_page_capacity("memory page capacity", self.memory_page_capacity)?;
        let disk_page_capacity = self
            .disk_page_capacity
            .map(|capacity| self.valid_page_capacity("disk page capacity", capacity))
            .transpose()?;
        ensure!(
            self.stage_cache_capacity.as_bytes() > 0,
            InvalidConfigSnafu {
                reason: "stage cache capacity must be greater than zero".to_string(),
            }
        );
        ensure!(
            self.memory_read_cache_capacity.as_bytes() > 0,
            InvalidConfigSnafu {
                reason: "memory read cache capacity must be greater than zero".to_string(),
            }
        );
        ensure!(
            !self.shutdown_deadline.is_zero(),
            InvalidConfigSnafu {
                reason: "shutdown deadline must be greater than zero".to_string(),
            }
        );
        ensure!(
            self.cache_dir.is_absolute(),
            InvalidConfigSnafu {
                reason: "cache directory must be an absolute path".to_string(),
            }
        );

        Ok((memory_page_capacity, disk_page_capacity))
    }

    fn prepare_cache_paths(
        &self,
        memory_page_capacity: usize,
        disk_page_capacity: Option<usize>,
    ) -> Result<PreparedCacheConfig> {
        validate_existing_cache_path(&self.cache_dir, CachePathKind::Directory)?;
        let expected_root = resolve_path_for_overlap(&self.cache_dir)?;
        let hierarchy_leases = prepare_and_lock_cache_hierarchy(&expected_root)?;
        let root = self.cache_dir.canonicalize().context(CacheRootSnafu {
            path: self.cache_dir.clone(),
        })?;
        ensure!(
            root == expected_root,
            InvalidConfigSnafu {
                reason: "cache directory changed while its hierarchy was being locked".to_string(),
            }
        );

        let stage_dir = root.join("stage");
        validate_existing_cache_path(&stage_dir, CachePathKind::Directory)?;
        std::fs::create_dir_all(&stage_dir).context(CacheRootSnafu {
            path: stage_dir.clone(),
        })?;
        let stage_dir = stage_dir.canonicalize().context(CacheRootSnafu {
            path: stage_dir.clone(),
        })?;
        ensure!(
            stage_dir.starts_with(&root),
            InvalidConfigSnafu {
                reason: "derived stage cache path escaped the cache root".to_string(),
            }
        );

        let lock_path = root.join(".mount.lock");
        validate_existing_cache_path(&lock_path, CachePathKind::File)?;
        let lease = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)
            .context(CacheRootSnafu {
                path: lock_path.clone(),
            })?;
        FileExt::try_lock_exclusive(&lease).context(CacheRootSnafu { path: lock_path })?;

        let disk_page_pool_path = root.join("page-pool.bin");
        validate_existing_cache_path(&disk_page_pool_path, CachePathKind::File)?;

        Ok(PreparedCacheConfig {
            stage_dir,
            disk_page_pool_path,
            root,
            memory_page_capacity,
            disk_page_capacity,
            stage_cache_capacity: self.stage_cache_capacity,
            stage_cache_ttl: self.stage_cache_ttl,
            memory_read_cache_capacity: self.memory_read_cache_capacity,
            _lease: lease,
            _hierarchy_leases: hierarchy_leases,
        })
    }

    fn validate_layout(&self, stored: &Format) -> Result<()> {
        ensure!(
            self.page_size > 0
                && self.block_size > 0
                && self.chunk_size > 0
                && self.block_size.is_multiple_of(self.page_size)
                && self.chunk_size.is_multiple_of(self.block_size),
            InvalidConfigSnafu {
                reason: "page, block, and chunk sizes must be non-zero aligned multiples"
                    .to_string(),
            }
        );
        let requested = Format {
            chunk_size: self.chunk_size,
            block_size: self.block_size,
            page_size: self.page_size,
            ..stored.clone()
        };
        if let Some(mismatch) = stored.layout_mismatch(&requested) {
            return InvalidConfigSnafu {
                reason: format!(
                    "stored {} is {}, but mount requested {}",
                    mismatch.field, mismatch.stored, mismatch.requested
                ),
            }
            .fail();
        }
        Ok(())
    }

    fn valid_page_capacity(&self, name: &str, capacity: ReadableSize) -> Result<usize> {
        let capacity = usize::try_from(capacity.as_bytes()).map_err(|_| {
            InvalidConfigSnafu {
                reason: format!("{name} does not fit this platform"),
            }
            .build()
        })?;
        ensure!(
            capacity > 0 && capacity.is_multiple_of(self.page_size),
            InvalidConfigSnafu {
                reason: format!("{name} must be a non-zero multiple of page size"),
            }
        );
        Ok(capacity)
    }
}

fn validate_absolute_clean_path(name: &str, path: &Path) -> Result<()> {
    ensure!(
        path.is_absolute(),
        InvalidConfigSnafu {
            reason: format!("{name} must be an absolute path"),
        }
    );
    ensure!(
        !path
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir)),
        InvalidConfigSnafu {
            reason: format!("{name} may not contain '..'"),
        }
    );
    Ok(())
}

/// Resolve symlinks in the longest existing prefix, then append any missing
/// suffix. This catches aliases even when the configured leaf is created only
/// after validation.
fn resolve_path_for_overlap(path: &Path) -> Result<PathBuf> {
    let mut existing = path;
    let mut missing = Vec::new();
    loop {
        match std::fs::symlink_metadata(existing) {
            Ok(_) => break,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let name = existing.file_name().ok_or_else(|| {
                    InvalidConfigSnafu {
                        reason: format!(
                            "could not resolve an existing ancestor for {}",
                            path.display()
                        ),
                    }
                    .build()
                })?;
                missing.push(name.to_os_string());
                existing = existing.parent().ok_or_else(|| {
                    InvalidConfigSnafu {
                        reason: format!(
                            "could not resolve an existing ancestor for {}",
                            path.display()
                        ),
                    }
                    .build()
                })?;
            }
            Err(error) => {
                return InvalidConfigSnafu {
                    reason: format!("failed to inspect {}: {error}", path.display()),
                }
                .fail();
            }
        }
    }
    let mut resolved = existing.canonicalize().map_err(|error| {
        InvalidConfigSnafu {
            reason: format!("failed to resolve {}: {error}", path.display()),
        }
        .build()
    })?;
    for component in missing.into_iter().rev() {
        resolved.push(component);
    }
    Ok(resolved)
}

fn paths_overlap(left: &Path, right: &Path) -> bool {
    left == right || left.starts_with(right) || right.starts_with(left)
}

/// Lock every existing/created directory from the filesystem root down to the
/// cache root. Strict ancestors are shared; the selected root is exclusive.
/// Two sibling cache roots can coexist, while either ordering of nested roots
/// conflicts on the same directory inode.
fn prepare_and_lock_cache_hierarchy(root: &Path) -> Result<Vec<File>> {
    ensure!(
        root.parent().is_some(),
        InvalidConfigSnafu {
            reason: "cache directory may not be the filesystem root".to_string(),
        }
    );
    let mut current = PathBuf::new();
    let mut leases = Vec::new();
    for component in root.components() {
        current.push(component.as_os_str());
        match std::fs::symlink_metadata(&current) {
            Ok(_) => validate_existing_cache_path(&current, CachePathKind::Directory)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match std::fs::create_dir(&current) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                        validate_existing_cache_path(&current, CachePathKind::Directory)?;
                    }
                    Err(error) => {
                        return Err(error).context(CacheRootSnafu {
                            path: current.clone(),
                        });
                    }
                }
            }
            Err(error) => {
                return Err(error).context(CacheRootSnafu {
                    path: current.clone(),
                });
            }
        }
        let lease = File::open(&current).context(CacheRootSnafu {
            path: current.clone(),
        })?;
        ensure!(
            opened_path_identity_matches(&lease, &current)?,
            InvalidConfigSnafu {
                reason: format!(
                    "cache hierarchy path {} changed while it was being locked",
                    current.display()
                ),
            }
        );
        if current == root {
            FileExt::try_lock_exclusive(&lease).context(CacheRootSnafu {
                path: current.clone(),
            })?;
        } else {
            FileExt::try_lock_shared(&lease).context(CacheRootSnafu {
                path: current.clone(),
            })?;
        }
        leases.push(lease);
    }
    Ok(leases)
}

#[cfg(unix)]
fn opened_path_identity_matches(file: &File, path: &Path) -> Result<bool> {
    use std::os::unix::fs::MetadataExt;

    let opened = file.metadata().context(CacheRootSnafu {
        path: path.to_path_buf(),
    })?;
    let current = std::fs::symlink_metadata(path).context(CacheRootSnafu {
        path: path.to_path_buf(),
    })?;
    Ok(opened.dev() == current.dev()
        && opened.ino() == current.ino()
        && opened.is_dir()
        && current.is_dir()
        && !current.file_type().is_symlink())
}

#[cfg(not(unix))]
fn opened_path_identity_matches(file: &File, path: &Path) -> Result<bool> {
    let opened = file.metadata().context(CacheRootSnafu {
        path: path.to_path_buf(),
    })?;
    let current = std::fs::symlink_metadata(path).context(CacheRootSnafu {
        path: path.to_path_buf(),
    })?;
    Ok(opened.is_dir() && current.is_dir() && !current.file_type().is_symlink())
}

#[derive(Clone, Copy)]
enum CachePathKind {
    Directory,
    File,
}

fn validate_existing_cache_path(path: &Path, expected: CachePathKind) -> Result<()> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(error).context(CacheRootSnafu {
                path: path.to_path_buf(),
            });
        }
    };
    let expected_type = match expected {
        CachePathKind::Directory => metadata.is_dir(),
        CachePathKind::File => metadata.is_file(),
    };
    #[cfg(unix)]
    let is_unshared_file = {
        use std::os::unix::fs::MetadataExt;

        !matches!(expected, CachePathKind::File) || metadata.nlink() == 1
    };
    #[cfg(not(unix))]
    let is_unshared_file = true;
    ensure!(
        expected_type && !metadata.file_type().is_symlink() && is_unshared_file,
        InvalidConfigSnafu {
            reason: format!(
                "cache path {} must be an unshared {} and not a symlink",
                path.display(),
                match expected {
                    CachePathKind::Directory => "directory",
                    CachePathKind::File => "regular file",
                }
            ),
        }
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use kiseki_types::setting::Format;
    use kiseki_utils::readable_size::ReadableSize;

    use super::*;

    #[test]
    fn rejects_invalid_page_capacity_before_preparing_cache_paths() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = Config {
            cache_dir: tempdir.path().join("cache"),
            memory_page_capacity: ReadableSize((PAGE_SIZE + 1) as u64),
            ..Config::default()
        };

        assert!(config.prepare(&Format::default()).is_err());
        assert!(!config.cache_dir.exists());
    }

    #[test]
    fn rejects_layout_mismatch_before_allocating_resources() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut format = Format::default();
        format.block_size *= 2;
        let config = Config {
            cache_dir: tempdir.path().join("cache"),
            ..Config::default()
        };

        assert!(config.prepare(&format).is_err());
        assert!(!config.cache_dir.exists());
    }

    #[test]
    fn one_live_mount_exclusively_owns_its_cache_root() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = Config {
            cache_dir: tempdir.path().join("cache"),
            ..Config::default()
        };

        let first = config.prepare(&Format::default()).unwrap();
        assert_eq!(first.stage_dir(), first.root().join("stage"));
        assert!(config.prepare(&Format::default()).is_err());
        drop(first);
        assert!(config.prepare(&Format::default()).is_ok());
    }

    #[test]
    fn live_nested_cache_roots_conflict_in_both_start_orders() {
        let tempdir = tempfile::tempdir().unwrap();
        let outer_root = tempdir.path().join("outer-first");
        let outer = Config {
            cache_dir: outer_root.clone(),
            ..Config::default()
        };
        let outer_lease = outer.prepare(&Format::default()).unwrap();
        let nested = Config {
            cache_dir: outer_root.join("stage"),
            ..Config::default()
        };
        assert!(nested.prepare(&Format::default()).is_err());
        assert!(!outer_root.join("stage/stage").exists());
        assert!(!outer_root.join("stage/.mount.lock").exists());
        drop(outer_lease);

        let reverse_outer_root = tempdir.path().join("nested-first");
        let reverse_nested = Config {
            cache_dir: reverse_outer_root.join("stage"),
            ..Config::default()
        };
        let nested_lease = reverse_nested.prepare(&Format::default()).unwrap();
        let reverse_outer = Config {
            cache_dir: reverse_outer_root,
            ..Config::default()
        };
        assert!(reverse_outer.prepare(&Format::default()).is_err());
        drop(nested_lease);
    }

    #[test]
    fn rejects_file_object_root_inside_cache_without_touching_existing_data() {
        let tempdir = tempfile::tempdir().unwrap();
        let cache_dir = tempdir.path().join("cache");
        let object_root = cache_dir.join("stage");
        std::fs::create_dir_all(&object_root).unwrap();
        let victim = object_root.join("sole-object");
        std::fs::write(&victim, b"preserve me").unwrap();
        let mount_point = tempdir.path().join("mount");
        std::fs::create_dir(&mount_point).unwrap();
        let config = Config {
            cache_dir,
            object_storage: ObjectStorageConfig::File { root: object_root },
            ..Config::default()
        };

        assert!(config.prepare(&Format::default()).is_err());
        assert!(config.validate_mount_paths(&mount_point, None).is_err());
        assert_eq!(std::fs::read(&victim).unwrap(), b"preserve me");
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlink_aliases_and_ready_files_inside_owned_paths() {
        use std::os::unix::fs::symlink;

        let tempdir = tempfile::tempdir().unwrap();
        let cache_dir = tempdir.path().join("cache");
        std::fs::create_dir(&cache_dir).unwrap();
        let cache_alias = tempdir.path().join("cache-alias");
        symlink(&cache_dir, &cache_alias).unwrap();
        let mount_point = tempdir.path().join("mount");
        std::fs::create_dir(&mount_point).unwrap();
        let aliased = Config {
            cache_dir: cache_alias.join("nested"),
            object_storage: ObjectStorageConfig::File {
                root: cache_dir.join("nested").join("objects"),
            },
            ..Config::default()
        };
        assert!(aliased.validate_mount_paths(&mount_point, None).is_err());

        let separate = Config {
            cache_dir: cache_dir.clone(),
            object_storage: ObjectStorageConfig::File {
                root: tempdir.path().join("objects"),
            },
            ..Config::default()
        };
        assert!(
            separate
                .validate_mount_paths(&mount_point, Some(&cache_dir.join("ready.json")))
                .is_err()
        );
        assert!(
            separate
                .validate_mount_paths(&mount_point, Some(&mount_point.join("ready.json")))
                .is_err()
        );
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlinked_disk_pool_without_touching_its_target() {
        use std::os::unix::fs::symlink;

        let tempdir = tempfile::tempdir().unwrap();
        let cache_dir = tempdir.path().join("cache");
        std::fs::create_dir(&cache_dir).unwrap();
        let victim = tempdir.path().join("must-not-be-truncated");
        std::fs::write(&victim, b"preserve me").unwrap();
        symlink(&victim, cache_dir.join("page-pool.bin")).unwrap();
        let config = Config {
            cache_dir,
            disk_page_capacity: Some(ReadableSize(PAGE_SIZE as u64)),
            ..Config::default()
        };

        assert!(config.prepare(&Format::default()).is_err());
        assert_eq!(std::fs::read(&victim).unwrap(), b"preserve me");
    }

    #[cfg(unix)]
    #[test]
    fn rejects_hardlinked_disk_pool_without_touching_its_target() {
        let tempdir = tempfile::tempdir().unwrap();
        let cache_dir = tempdir.path().join("cache");
        std::fs::create_dir(&cache_dir).unwrap();
        let victim = tempdir.path().join("must-not-be-truncated");
        std::fs::write(&victim, b"preserve me").unwrap();
        std::fs::hard_link(&victim, cache_dir.join("page-pool.bin")).unwrap();
        let config = Config {
            cache_dir,
            disk_page_capacity: Some(ReadableSize(PAGE_SIZE as u64)),
            ..Config::default()
        };

        assert!(config.prepare(&Format::default()).is_err());
        assert_eq!(std::fs::read(&victim).unwrap(), b"preserve me");
    }
}
