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
    fmt::{Debug, Formatter},
    sync::Arc,
    time::Duration,
};

use dashmap::DashMap;
use fuser::FileType;
use kiseki_common::MAX_FILE_SIZE;
#[cfg(test)]
use kiseki_common::{MODE_MASK_R, MODE_MASK_W};
use kiseki_meta::{MetaEngineRef, context::FuseContext};
use kiseki_types::{
    attr::InodeAttr,
    ino::{CONTROL_INODE, Ino, ROOT_INO},
    internal_nodes::{CONFIG_INODE_NAME, InternalNodeTable},
};
use kiseki_utils::object_storage::ObjectStorage;
use libc::{EACCES, EBADF, EFBIG, EPERM, mode_t};
use snafu::{ResultExt, ensure};
use tracing::{debug, info, trace};

use crate::{
    config::Config,
    data_manager::{DataManager, DataManagerRef},
    err::{LibcSnafu, MetaSnafu, ObjectStorageConfigSnafu, ObjectStorageSnafu, Result},
    handle::{HandleTable, HandleTableRef},
};

pub struct KisekiVFS {
    pub config: Config,

    // Runtime status
    internal_nodes:          InternalNodeTable,
    modified_at:             DashMap<Ino, std::time::Instant>,
    pub(crate) handle_table: HandleTableRef,
    pub(crate) data_manager: DataManagerRef,

    // Dependencies
    pub(crate) meta: MetaEngineRef,
}

impl Debug for KisekiVFS {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "KisekiFS based on {}", self.meta)
    }
}

// All helper functions for KisekiVFS
impl KisekiVFS {
    /// 验证文件名是否有效
    /// 拒绝空字符串、"." 和 ".." 这些系统保留名称
    fn validate_filename(name: &str) -> Result<()> {
        if name.is_empty() {
            return LibcSnafu {
                errno: libc::EINVAL,
            }
            .fail();
        }
        if name == "." || name == ".." {
            return LibcSnafu {
                errno: libc::EEXIST,
            }
            .fail();
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn new(vfs_config: Config, meta: MetaEngineRef) -> Result<Self> {
        let object_storage = vfs_config
            .object_storage
            .build()
            .context(ObjectStorageConfigSnafu)?;
        Self::new_with_object_storage(vfs_config, meta, object_storage)
    }

    pub async fn new_checked(vfs_config: Config, meta: MetaEngineRef) -> Result<Self> {
        let object_storage = vfs_config
            .object_storage
            .build()
            .context(ObjectStorageConfigSnafu)?;
        object_storage.probe().await.context(ObjectStorageSnafu)?;
        info!(
            provider = vfs_config.object_storage.provider(),
            bucket = ?vfs_config.object_storage.bucket(),
            prefix = ?vfs_config.object_storage.prefix(),
            "object storage is ready"
        );
        Self::new_with_object_storage(vfs_config, meta, object_storage)
    }

    fn new_with_object_storage(
        vfs_config: Config,
        meta: MetaEngineRef,
        object_storage: ObjectStorage,
    ) -> Result<Self> {
        let mut internal_nodes =
            InternalNodeTable::new((vfs_config.file_entry_timeout, vfs_config.dir_entry_timeout));
        let config_inode = internal_nodes
            .get_mut_internal_node_by_name(CONFIG_INODE_NAME)
            .unwrap();
        let config_buf = bincode::serialize(&vfs_config).expect("unable to serialize vfs config");
        config_inode.0.attr.set_length(config_buf.len() as u64);
        // if meta.config.sub_dir.is_some() {
        //     don't show trash directory
        // internal_nodes.remove_trash_node();
        // }
        if vfs_config.prefix_internal {
            internal_nodes.add_prefix();
        }

        let data_manager = Arc::new(DataManager::new(
            vfs_config.chunk_size,
            meta.clone(),
            object_storage,
            kiseki_storage::cache::file_cache::Config {
                stage_cache_dir: vfs_config.stage_cache_dir.clone(),
                ..Default::default()
            },
        )?);

        let vfs = Self {
            config: vfs_config,
            internal_nodes,
            modified_at: DashMap::new(),
            handle_table: HandleTable::new(data_manager.clone()),
            data_manager,
            meta,
        };

        // TODO: spawn a background task to clean up modified time.

        Ok(vfs)
    }

    async fn truncate(
        &self,
        ctx: Arc<FuseContext>,
        ino: Ino,
        size: u64,
        _fh: Option<u64>,
    ) -> Result<InodeAttr> {
        ensure!(!ino.is_special(), LibcSnafu { errno: EPERM });
        ensure!((size as usize) < MAX_FILE_SIZE, LibcSnafu { errno: EFBIG });

        // acquire a big lock for all file handles of the inode
        let handles = self.handle_table.get_handles(ino).await;
        // when we release the guards, the lock will be released automatically.
        let mut _guards = Vec::with_capacity(handles.len());
        for h in handles {
            if let Some(fh) = h.as_file_handle() {
                if let Some(write_guard) = fh.write_lock(ctx.clone()).await {
                    _guards.push(write_guard);
                } else if matches!(_fh, Some(_fh) if _fh == h.get_fh()) {
                    // not find FileWriter
                    return LibcSnafu { errno: EACCES }.fail();
                }
            } else {
                // Try to truncate a DirHandle
                if matches!(_fh, Some(_fh) if _fh == h.get_fh()) {
                    return LibcSnafu { errno: EBADF }.fail();
                }
            }
        }

        // safety: we hold all the locks for the exists file handles
        self.data_manager.direct_flush(ino).await?;
        // TODO: call meta to truncate the file length
        let attr = self
            .meta
            .truncate(ctx, ino, size, _fh.is_some())
            .await
            .context(MetaSnafu)?;
        self.data_manager.set_length(ino, size as usize);
        self.data_manager.truncate_reader(ino, size).await;
        Ok(attr)
    }

    /// [`Self::get_entry_ttl`] returns the entry timeout for the given file
    /// type.
    pub fn get_entry_ttl(&self, kind: FileType) -> &Duration {
        if kind == FileType::Directory {
            &self.config.dir_entry_timeout
        } else {
            &self.config.file_entry_timeout
        }
    }

    /// [`Self::try_update_file_reader_length`] checks and updates the file
    /// reader length.
    pub async fn try_update_file_reader_length(&self, inode: Ino, attr: &mut InodeAttr) {
        if attr.is_file() {
            let len = self.data_manager.get_length(inode);
            if len > attr.length {
                attr.length = len;
            }
            if len < attr.length {
                self.data_manager.truncate_reader(inode, attr.length).await;
            }
        }
    }

    pub fn modified_since(&self, inode: Ino, start_at: std::time::Instant) -> bool {
        match self.modified_at.get(&inode) {
            Some(v) => v.value() > &start_at,
            None => false,
        }
    }

    fn invalidate_length(&self, ino: Ino) {
        if let Some(mut v) = self.modified_at.get_mut(&ino) {
            *v = std::time::Instant::now();
        }
    }

    #[cfg(test)]
    fn check_access(
        &self,
        ctx: Arc<FuseContext>,
        _inode: Ino,
        attr: &InodeAttr,
        mask: libc::c_int,
    ) -> Result<()> {
        let mut my_mask = 0;
        if mask & libc::R_OK != 0 {
            my_mask |= MODE_MASK_R;
        }
        if mask & libc::W_OK != 0 {
            my_mask |= MODE_MASK_W;
        }
        if mask & libc::X_OK != 0 {
            my_mask |= (libc::S_IXUSR | libc::S_IXGRP | libc::S_IXOTH) as u8;
        }
        ctx.check_access(attr, my_mask)?;
        Ok(())
    }
}

impl KisekiVFS {
    /// Initialize the KisekiVFS filesystem.
    ///
    /// This function initializes the virtual file system and prepares it for
    /// operation. It performs initial setup tasks such as validating the
    /// meta format and handling any subdirectory chroot operations if
    /// configured.
    ///
    /// # Arguments
    /// * `_ctx`: [FuseContext] - The FUSE context containing request metadata
    ///   like uid, gid, process information, etc. Currently unused in
    ///   initialization.
    ///
    /// # Returns
    /// * [Result<()>] - Returns `Ok(())` on successful initialization, or an
    ///   error if initialization fails.
    ///
    /// # Errors
    /// This function may return errors if:
    /// - The metadata engine format validation fails
    /// - Subdirectory chroot operation fails (when configured)
    ///
    /// # Examples
    /// ```ignore
    /// use kiseki_meta::context::FuseContext;
    ///
    /// let ctx = FuseContext::background();
    /// vfs.init(&ctx).await?;
    /// ```
    ///
    /// # References
    /// * [FUSE init](https://libfuse.github.io/doxygen/structfuse__operations.html#a39a048c8a19c7b02fec706f43ec68b29)
    pub async fn init(&self, ctx: &FuseContext) -> Result<()> {
        debug!("vfs:init - starting KisekiFS initialization");

        // 1. Validate filesystem format and version
        let format = self.meta.get_format();
        info!(
            "Initializing KisekiFS: {} (chunk_size={}MB, block_size={}KB)",
            format.name,
            format.chunk_size / (1024 * 1024),
            format.block_size / 1024
        );

        // 2. Validate root directory integrity
        let root_attr = self.meta.get_attr(ROOT_INO).await.context(MetaSnafu)?;
        ensure!(
            root_attr.is_dir(),
            LibcSnafu {
                errno: libc::ENOTDIR,
            }
        );
        debug!("Root directory validation passed (inode: {})", ROOT_INO.0);

        // 3. Validate internal nodes consistency
        if self.config.prefix_internal || !self.config.hide_internal {
            debug!("Internal nodes are enabled, validating consistency");
            // Internal nodes are already initialized in new(), just log their status
            // Check that key internal nodes are accessible
            let control_node = self.internal_nodes.get_internal_node(CONTROL_INODE);
            let config_node = self
                .internal_nodes
                .get_internal_node_by_name(CONFIG_INODE_NAME);
            ensure!(control_node.is_some(), LibcSnafu { errno: libc::EIO });
            ensure!(config_node.is_some(), LibcSnafu { errno: libc::EIO });
            debug!("Internal nodes validated successfully");
        }

        // 4. Initialize data manager cache and buffers
        debug!(
            "Initializing data manager with {} total buffer capacity",
            self.config.total_buffer_capacity
        );

        // 5. Log configuration summary
        info!("KisekiFS configuration:");
        info!(
            "  - Chunk size: {}MB",
            self.config.chunk_size / (1024 * 1024)
        );
        info!("  - Block size: {}KB", self.config.block_size / 1024);
        info!("  - Page size: {}KB", self.config.page_size / 1024);
        info!(
            "  - Total buffer capacity: {}MB",
            self.config.total_buffer_capacity / (1024 * 1024)
        );
        info!("  - Cache capacity: {}KB", self.config.capacity / 1024);
        info!("  - Attr timeout: {:?}", self.config.attr_timeout);
        info!("  - Dir entry timeout: {:?}", self.config.dir_entry_timeout);
        info!(
            "  - File entry timeout: {:?}",
            self.config.file_entry_timeout
        );

        // 6. Validate filesystem statistics
        let fs_stat = self
            .meta
            .stat_fs(Arc::new(ctx.clone()), ROOT_INO)
            .context(MetaSnafu)?;
        debug!(
            "Filesystem statistics - Total size: {}MB, Used: {}MB, Files: {}",
            fs_stat.total_size / (1024 * 1024),
            fs_stat.used_size / (1024 * 1024),
            fs_stat.file_count
        );

        info!("KisekiFS initialization completed successfully");
        Ok(())
    }

    /// Get filesystem statistics.
    ///
    /// This function retrieves statistical information about the filesystem,
    /// such as total space, free space, inode counts, and other filesystem
    /// metadata. This corresponds to the statfs(2) system call.
    ///
    /// # Arguments
    /// * `ctx`: `Arc<FuseContext>` - The FUSE context containing request
    ///   metadata
    /// * `ino`: `I: Into<Ino>` - The inode number to query. Can be any valid
    ///   inode, though typically the root inode is used for filesystem
    ///   statistics.
    ///
    /// # Returns
    /// * `Result<kiseki_types::stat::FSStat>` - Returns filesystem statistics
    ///   including:
    ///   - Total blocks and free blocks
    ///   - Total inodes and free inodes
    ///   - Block size and fragment size
    ///   - Maximum filename length
    ///   - Filesystem flags
    ///
    /// # Errors
    /// This function may return errors if:
    /// - The inode is invalid or inaccessible
    /// - The metadata engine fails to retrieve statistics
    /// - Permission denied for the requested inode
    ///
    /// # Examples
    /// ```ignore
    /// use kiseki_types::ino::ROOT_INO;
    ///
    /// let stats = vfs.stat_fs(ctx.clone(), ROOT_INO)?;
    /// println!("Total space: {} bytes", stats.blocks * stats.block_size);
    /// ```
    ///
    /// # References
    /// * [statfs(2)](https://man7.org/linux/man-pages/man2/statfs.2.html)
    /// * [FUSE statfs](https://libfuse.github.io/doxygen/structfuse__operations.html#a1589a59c8d9ef9a0b94dac33e6e7b1c3)
    pub fn stat_fs<I: Into<Ino>>(
        self: &Arc<Self>,
        ctx: Arc<FuseContext>,
        ino: I,
    ) -> Result<kiseki_types::stat::FSStat> {
        let ino = ino.into();
        trace!("fs:stat_fs with ino {ino:?}");
        let h = self.meta.stat_fs(ctx, ino)?;
        Ok(h)
    }
}

mod file_io;
mod metadata;
mod namespace;

/// Reply to a `open` or `opendir` call
#[derive(Debug)]
pub struct Opened {
    pub fh:    u64,
    pub flags: u32,
    pub inode: Ino,
    pub attr:  InodeAttr,
}

// TODO: review me, use a better way.
fn get_file_type(mode: mode_t) -> Result<FileType> {
    match mode & libc::S_IFMT {
        libc::S_IFIFO => Ok(FileType::NamedPipe),
        libc::S_IFSOCK => Ok(FileType::Socket),
        libc::S_IFLNK => Ok(FileType::Symlink),
        libc::S_IFREG => Ok(FileType::RegularFile),
        libc::S_IFBLK => Ok(FileType::BlockDevice),
        libc::S_IFDIR => Ok(FileType::Directory),
        libc::S_IFCHR => Ok(FileType::CharDevice),
        _ => LibcSnafu { errno: EPERM }.fail()?,
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod object_storage_tests {
    use kiseki_types::setting::Format;
    use kiseki_utils::object_storage::ObjectStorageConfig;

    use super::KisekiVFS;
    use crate::{Config, err::Error};

    #[tokio::test]
    async fn checked_constructor_uses_configured_store_before_mounting() {
        let meta_dir = tempfile::tempdir().unwrap();
        let mut meta_config = kiseki_meta::MetaConfig::default();
        meta_config.with_dsn(&format!("rocksdb://:{}", meta_dir.path().display()));
        kiseki_meta::update_format(&meta_config.dsn, Format::default(), false).unwrap();
        let meta = kiseki_meta::open(meta_config).unwrap();

        let storage_dir = tempfile::tempdir().unwrap();
        let invalid_root = storage_dir.path().join("regular-file");
        std::fs::write(&invalid_root, b"not a directory").unwrap();
        let config = Config {
            object_storage: ObjectStorageConfig::File { root: invalid_root },
            ..Config::default()
        };

        let result = KisekiVFS::new_checked(config, meta).await;
        assert!(matches!(result, Err(Error::ObjectStorageConfig { .. })));
    }
}
