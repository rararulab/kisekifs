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

//! A write-back cache for immutable Object Blocks, which will be used both for
//! the reading and flushing. It puts data in the local file system, and also
//! maintains a memory index.
//!
//! ## Where it will be used?
//! 1. When slice buffer is going to flush, we take over the flush process,
//!    stage the data to the local file system first, then flush the data to the
//!    remote storage in the background.
//! 2. When we read the slice, we first check the cache.
//!
//! ## Key Components
//! 2. StageCache: when the block data is large, we treat the data as stage data
//!    put them in the stage cache dir, each block represents a file.
//! 3. Disk Eviction: When the cache is full, we need to evict the data from the
//!    disk to the remote storage.
//! 4. Flusher: a background task periodically flush the expired data to the
//!    remote storage.
//! 5. Recover: when the system restarts, we will clean all cache and flush old
//!    staged data to the remote storage.

use std::{
    cmp::min,
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use bytes::Bytes;
use kiseki_common::{BlockIndex, PAGE_SIZE};
use kiseki_types::slice::{SliceID, SliceKey};
use kiseki_utils::{
    object_storage::{LocalStorage, ObjectReader, ObjectStorage},
    readable_size::ReadableSize,
};
use snafu::{ResultExt, ensure};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio_util::io::StreamReader;
use tracing::{debug, error, warn};

use crate::{
    err::{Error::CacheError, FlushBlockFailedSnafu, ObjectStorageSnafu, Result, UnknownIOSnafu},
    pool::Page,
};

pub const DEFAULT_STAGE_CACHE_SIZE: u64 = 10 << 30;
// 10GiB
pub const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(24 * 3600); // 24 hours

pub struct Config {
    /// The directory to store the stage cache.
    pub stage_cache_dir: PathBuf,
    /// The max size of the stage cache. 10GiB by default.
    pub max_stage_size:  ReadableSize,
    /// How long can an object live after it has been staged.
    /// Zero means it will never expire, default by 24 hour.
    pub cache_ttl:       Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            stage_cache_dir: PathBuf::from(kiseki_common::KISEKI_DEBUG_STAGE_CACHE),
            max_stage_size:  ReadableSize(DEFAULT_STAGE_CACHE_SIZE),
            cache_ttl:       DEFAULT_CACHE_TTL,
        }
    }
}

pub type FileCacheRef = Arc<FileCache>;

pub struct FileCache {
    /// Authoritative index for locally readable staged blocks. Unlike the
    /// policy cache below, entries stay here until remote migration succeeds.
    staged_index:   Arc<tokio::sync::RwLock<HashMap<SliceKey, CacheIndex>>>,
    /// TTL/capacity policy only. Eviction schedules migration but does not own
    /// the staged block's readable lifetime.
    index:          moka::future::Cache<SliceKey, CacheIndex>,
    remote_storage: ObjectStorage,
    local_storage:  LocalStorage,
}

impl FileCache {
    pub fn new(config: Config, remote_storage: ObjectStorage) -> Result<Self> {
        // TODO: flush all the staged data to the remote storage at the beginning.

        let local_storage =
            kiseki_utils::object_storage::new_local_object_store(&config.stage_cache_dir)
                .context(ObjectStorageSnafu)?;
        let recovered = recover_stage_index(&config.stage_cache_dir)?;
        let recovered_entries = recovered.values().cloned().collect::<Vec<_>>();
        let staged_index = Arc::new(tokio::sync::RwLock::new(recovered));

        let local_storage_clone = local_storage.clone();
        let remote_storage_clone = remote_storage.clone();
        let staged_index_clone = staged_index.clone();
        let eviction_listener =
            move |k: Arc<SliceKey>, v: CacheIndex, cause| -> moka::notification::ListenerFuture {
                debug!("evicting block from the stage cache: {k:?}, reason: {cause:?}");
                let local_storage = local_storage_clone.clone();
                let remote_storage = remote_storage_clone.clone();
                let staged_index = staged_index_clone.clone();
                // Create a Future that removes the block from the local storage and
                // flushes it to the remote storage.
                //
                // Convert the regular Future into ListenerFuture. This method is
                // provided by moka::future::FutureExt trait.
                moka::future::FutureExt::boxed(async move {
                    migrate_with_retry(local_storage, remote_storage, staged_index, v).await;
                })
            };

        // The index-cache will try to evict entries that have not been used recently or
        // very often.
        let index = moka::future::Cache::builder()
            // the index-cache will be bounded by the total weighted size of entries.
            .weigher(|_, index: &CacheIndex| -> u32 { index.slice_key.block_size as u32 })
            // the cache should not grow beyond a certain size,
            // use the max_capacity method of the CacheBuilder
            // to set the upper bound.
            .max_capacity(config.max_stage_size.as_bytes())
            // A cached entry will be expired after the specified duration past from insert.
            // No matter how often the entry is accessed, it will be expired after the duration.
            .time_to_live(config.cache_ttl)
            .async_eviction_listener(eviction_listener)
            .build();

        let runtime = tokio::runtime::Handle::try_current().map_err(|error| CacheError {
            error: format!("file cache requires a Tokio runtime: {error}"),
        })?;
        for entry in recovered_entries {
            let local_storage = local_storage.clone();
            let remote_storage = remote_storage.clone();
            let staged_index = staged_index.clone();
            runtime.spawn(async move {
                migrate_with_retry(local_storage, remote_storage, staged_index, entry).await;
            });
        }

        Ok(Self {
            staged_index,
            index,
            remote_storage,
            local_storage,
        })
    }

    pub async fn get(self: &Arc<Self>, slice_key: &SliceKey) -> Result<Option<ObjectReader>> {
        if !self.staged_index.read().await.contains_key(slice_key) {
            return Ok(None);
        }
        let path = slice_key.make_object_storage_path();
        match self.local_storage.get(&path).await {
            Ok(reader) => Ok(Some(reader)),
            Err(error) if kiseki_utils::object_storage::is_not_found_error(&error) => {
                if self.remote_block_is_confirmed(slice_key).await? {
                    self.staged_index.write().await.remove(slice_key);
                    Ok(None)
                } else {
                    FlushBlockFailedSnafu.fail()
                }
            }
            Err(error) => Err(error).context(ObjectStorageSnafu),
        }
    }

    pub async fn get_range(
        self: &Arc<Self>,
        slice_key: &SliceKey,
        offset: usize,
        length: usize,
    ) -> Result<Option<Bytes>> {
        if !self.staged_index.read().await.contains_key(slice_key) {
            warn!("block not found in the stage cache: {slice_key:?}");
            return Ok(None);
        }
        let path = slice_key.make_object_storage_path();
        debug!(
            "find block in the stage cache: {:?}, try to use path: {:?} to load",
            slice_key, &path
        );
        match self
            .local_storage
            .get_range(&path, offset..offset + length)
            .await
        {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if kiseki_utils::object_storage::is_not_found_error(&error) => {
                if self.remote_block_is_confirmed(slice_key).await? {
                    self.staged_index.write().await.remove(slice_key);
                    Ok(None)
                } else {
                    FlushBlockFailedSnafu.fail()
                }
            }
            Err(error) => Err(error).context(ObjectStorageSnafu),
        }
    }

    pub async fn stage(
        self: &Arc<Self>,
        sid: SliceID,
        block_index: BlockIndex,
        block_length: usize,
        pages: &[Option<Page>],
    ) -> Result<(usize, usize)> {
        let key = SliceKey::new(sid, block_index, block_length);
        debug!("staging block: {key:?}");
        let total_release_page_cnt = pages.iter().filter(|page| page.is_some()).count();
        let cache_index = self
            .index
            .try_get_with(key, async {
                let mut writer = self.local_storage.writer(&key.make_object_storage_path());
                copy_from_buffer_to_local(block_length, pages, &mut writer).await?;
                let idx = CacheIndex::new(key);
                Ok(idx) as Result<CacheIndex>
            })
            .await
            .map_err(|e| CacheError {
                error: e.to_string(),
            })?;
        self.staged_index.write().await.insert(key, cache_index);

        Ok((block_length, total_release_page_cnt))
    }

    pub async fn flush_key(self: &Arc<Self>, slice_key: &SliceKey) -> Result<()> {
        let Some(entry) = self.staged_index.read().await.get(slice_key).cloned() else {
            self.index.invalidate(slice_key).await;
            return Ok(());
        };

        migrate_once(
            self.local_storage.clone(),
            self.remote_storage.clone(),
            self.staged_index.clone(),
            &entry,
        )
        .await?;
        self.index.invalidate(slice_key).await;
        Ok(())
    }

    /// Flush every locally staged block for a slice to remote storage.
    ///
    /// This is the durability barrier used before publishing slice metadata.
    /// A missing staged entry is already remote-confirmed (or the slice has no
    /// data blocks), so it is safe to treat it as complete.
    pub async fn flush_slice(self: &Arc<Self>, slice_id: SliceID) -> Result<()> {
        let mut keys = self
            .staged_index
            .read()
            .await
            .keys()
            .filter(|key| key.slice_id == slice_id)
            .copied()
            .collect::<Vec<_>>();
        keys.sort_unstable_by_key(|key| key.block_idx);
        for key in keys {
            self.flush_key(&key).await?;
        }
        Ok(())
    }

    async fn remote_block_is_confirmed(&self, slice_key: &SliceKey) -> Result<bool> {
        match self
            .remote_storage
            .get(&slice_key.make_object_storage_path())
            .await
        {
            Ok(object) => Ok(object.meta.size == slice_key.block_size as u64),
            Err(error) if kiseki_utils::object_storage::is_not_found_error(&error) => Ok(false),
            Err(error) => Err(error).context(ObjectStorageSnafu),
        }
    }
}

fn recover_stage_index(stage_dir: &Path) -> Result<HashMap<SliceKey, CacheIndex>> {
    let mut recovered = HashMap::new();
    for entry in std::fs::read_dir(stage_dir).context(UnknownIOSnafu)? {
        let entry = entry.context(UnknownIOSnafu)?;
        let file_type = entry.file_type().context(UnknownIOSnafu)?;
        if !file_type.is_file() {
            return Err(CacheError {
                error: format!("unexpected non-file stage entry: {:?}", entry.file_name()),
            });
        }
        let file_name = entry.file_name();
        let file_name = file_name.to_str().ok_or_else(|| CacheError {
            error: "stage entry name is not valid UTF-8".to_string(),
        })?;
        if is_incomplete_stage_file(file_name) {
            std::fs::remove_file(entry.path()).context(UnknownIOSnafu)?;
            continue;
        }
        let slice_key = file_name.parse::<SliceKey>().map_err(|error| CacheError {
            error: format!("invalid stage entry {file_name:?}: {error}"),
        })?;
        let actual_len = entry.metadata().context(UnknownIOSnafu)?.len();
        if actual_len != slice_key.block_size as u64 {
            return Err(CacheError {
                error: format!(
                    "stage entry {file_name:?} has length {actual_len}, expected {}",
                    slice_key.block_size
                ),
            });
        }
        recovered.insert(slice_key, CacheIndex::new(slice_key));
    }
    Ok(recovered)
}

fn is_incomplete_stage_file(file_name: &str) -> bool {
    let Some(rest) = file_name.strip_prefix('.') else {
        return false;
    };
    let Some((canonical, suffix)) = rest.rsplit_once(".tmp-") else {
        return false;
    };
    canonical.parse::<SliceKey>().is_ok()
        && suffix.split('-').count() == 2
        && suffix
            .split('-')
            .all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()))
}

async fn migrate_with_retry(
    local_storage: LocalStorage,
    remote_storage: ObjectStorage,
    staged_index: Arc<tokio::sync::RwLock<HashMap<SliceKey, CacheIndex>>>,
    entry: CacheIndex,
) {
    let mut retry_delay = Duration::from_millis(20);
    loop {
        match migrate_once(
            local_storage.clone(),
            remote_storage.clone(),
            staged_index.clone(),
            &entry,
        )
        .await
        {
            Ok(()) => break,
            Err(error) => {
                error!(
                    slice_key = %entry.slice_key,
                    ?retry_delay,
                    %error,
                    "failed to migrate staged block; retrying"
                );
                tokio::time::sleep(retry_delay).await;
                retry_delay = min(retry_delay * 2, Duration::from_secs(1));
            }
        }
    }
}

async fn migrate_once(
    local_storage: LocalStorage,
    remote_storage: ObjectStorage,
    staged_index: Arc<tokio::sync::RwLock<HashMap<SliceKey, CacheIndex>>>,
    entry: &CacheIndex,
) -> Result<()> {
    let _migration_guard = entry.migration_lock.lock().await;
    let is_current = staged_index
        .read()
        .await
        .get(&entry.slice_key)
        .is_some_and(|current| current.same_generation(entry));
    if !is_current {
        return Ok(());
    }

    migrate_from_local_to_remote(
        local_storage,
        remote_storage,
        entry.slice_key.block_size,
        &entry.slice_key,
    )
    .await?;

    {
        let mut staged_index = staged_index.write().await;
        if staged_index
            .get(&entry.slice_key)
            .is_some_and(|current| current.same_generation(entry))
        {
            staged_index.remove(&entry.slice_key);
        }
    }
    Ok(())
}

async fn copy_from_buffer_to_local(
    block_length: usize,
    pages: &[Option<Page>],
    writer: &mut Box<dyn AsyncWrite + Unpin + Send>,
) -> Result<(usize, usize)> {
    let mut total_released_page_cnt = 0;
    let mut current_flush_length = 0;

    while current_flush_length < block_length {
        let page_idx = current_flush_length / PAGE_SIZE;
        let page_offset = current_flush_length % PAGE_SIZE;
        let to_flush_len = min(PAGE_SIZE - page_offset, block_length - current_flush_length);
        match &pages[page_idx] {
            None => {
                let buf = vec![0u8; to_flush_len];
                writer.write_all(&buf).await.context(UnknownIOSnafu)?;
                // for _ in 0..to_flush_len {
                //     writer.write_u8(0).await.context(UnknownIOSnafu)?;
                // }
            }
            Some(page) => {
                total_released_page_cnt += 1;
                page.copy_to_writer(page_offset, to_flush_len, writer)
                    .await?;
            }
        }
        current_flush_length += to_flush_len;
    }
    writer.flush().await.context(UnknownIOSnafu)?;
    writer.shutdown().await.context(UnknownIOSnafu)?;
    Ok((block_length, total_released_page_cnt))
}

async fn migrate_from_local_to_remote(
    local_storage: LocalStorage,
    remote_storage: ObjectStorage,
    expect_copy_len: usize,
    key: &SliceKey,
) -> Result<()> {
    let path = key.make_object_storage_path();
    let reader = local_storage.get(&path).await.context(ObjectStorageSnafu)?;
    let reader_stream = reader.into_stream();
    let mut reader = StreamReader::new(reader_stream);
    // let mut reader = reader_stream.into_async_read();
    let mut writer = remote_storage.writer(&path);
    let copy_len = tokio::io::copy(&mut reader, &mut writer)
        .await
        .context(UnknownIOSnafu)?;
    writer.flush().await.context(UnknownIOSnafu)?;
    writer.shutdown().await.context(UnknownIOSnafu)?;
    ensure!(copy_len == expect_copy_len as u64, FlushBlockFailedSnafu);
    let remote = remote_storage
        .get(&path)
        .await
        .context(ObjectStorageSnafu)?;
    ensure!(
        remote.meta.size == expect_copy_len as u64,
        FlushBlockFailedSnafu
    );

    local_storage
        .delete(&path)
        .await
        .context(ObjectStorageSnafu)?;

    Ok(())
}

#[derive(Clone, Debug)]
struct CacheIndex {
    slice_key:      SliceKey,
    migration_lock: Arc<tokio::sync::Mutex<()>>,
}

impl CacheIndex {
    fn new(slice_key: SliceKey) -> Self {
        Self {
            slice_key,
            migration_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    fn same_generation(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.migration_lock, &other.migration_lock)
    }
}

#[cfg(test)]
mod tests {
    use std::process::Command;

    use kiseki_common::BLOCK_SIZE;

    use super::*;
    use crate::pool;

    fn test_config(stage_cache_dir: PathBuf, cache_ttl: Duration) -> Config {
        Config {
            stage_cache_dir,
            max_stage_size: ReadableSize(30 << 20),
            cache_ttl,
        }
    }

    async fn stage_bytes(cache: &Arc<FileCache>, slice_key: SliceKey, content: &[u8]) {
        let memory_pool = pool::memory_pool::MemoryPagePool::new(PAGE_SIZE, PAGE_SIZE * 2).unwrap();
        let mut pages: Box<[Option<Page>]> = (0..(BLOCK_SIZE / PAGE_SIZE)).map(|_| None).collect();
        let mut mem_page = memory_pool.acquire_page().await;
        let mut reader = std::io::Cursor::new(content);
        mem_page
            .copy_from_reader(0, content.len(), &mut reader)
            .await
            .unwrap();
        pages[0] = Some(Page::Memory(mem_page));

        cache
            .stage(
                slice_key.slice_id,
                slice_key.block_idx,
                slice_key.block_size,
                &pages,
            )
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_remote_migration_keeps_staged_block_readable() {
        kiseki_utils::logger::install_fmt_log();
        let tempdir = tempfile::tempdir().unwrap();
        let stage_dir = tempdir.path().join("stage");
        let remote_dir = tempdir.path().join("remote");
        let remote_storage =
            kiseki_utils::object_storage::new_local_object_store(&remote_dir).unwrap();
        let remote_probe = remote_storage.clone();
        std::fs::remove_dir_all(&remote_dir).unwrap();
        std::fs::write(&remote_dir, b"block remote directory recreation").unwrap();
        let cache = Arc::new(
            FileCache::new(
                test_config(stage_dir, Duration::from_millis(10)),
                remote_storage,
            )
            .unwrap(),
        );
        let content = b"must remain readable after migration failure";
        let slice_key = SliceKey::new(0xABCD_EF01, 0, content.len());
        stage_bytes(&cache, slice_key, content).await;

        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = cache.index.get(&slice_key).await;
        let policy_cache = cache.index.clone();
        let migration = tokio::spawn(async move {
            policy_cache.run_pending_tasks().await;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;

        let bytes = cache
            .get_range(&slice_key, 0, content.len())
            .await
            .unwrap()
            .expect("staged block must remain indexed after failed migration");
        assert_eq!(bytes.as_ref(), content);

        std::fs::remove_file(&remote_dir).unwrap();
        std::fs::create_dir(&remote_dir).unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if remote_probe
                    .get(&slice_key.make_object_storage_path())
                    .await
                    .is_ok()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("failed migration must be retried");
        migration.await.unwrap();
        assert!(
            cache
                .get_range(&slice_key, 0, content.len())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn explicit_flush_is_retryable_and_removes_local_only_after_remote_confirmation() {
        let tempdir = tempfile::tempdir().unwrap();
        let stage_dir = tempdir.path().join("stage");
        let remote_dir = tempdir.path().join("remote");
        let remote_storage =
            kiseki_utils::object_storage::new_local_object_store(&remote_dir).unwrap();
        let remote_probe = remote_storage.clone();
        let cache = Arc::new(
            FileCache::new(
                test_config(stage_dir, Duration::from_secs(3600)),
                remote_storage,
            )
            .unwrap(),
        );
        let content = b"explicit flush must confirm remote durability";
        let slice_key = SliceKey::new(0xABCD_EF05, 0, content.len());
        stage_bytes(&cache, slice_key, content).await;

        std::fs::remove_dir_all(&remote_dir).unwrap();
        std::fs::write(&remote_dir, b"remote unavailable").unwrap();
        assert!(cache.flush_key(&slice_key).await.is_err());
        assert_eq!(
            cache
                .get_range(&slice_key, 0, content.len())
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            content
        );

        std::fs::remove_file(&remote_dir).unwrap();
        std::fs::create_dir(&remote_dir).unwrap();
        cache.flush_key(&slice_key).await.unwrap();
        assert!(
            cache
                .get_range(&slice_key, 0, content.len())
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            remote_probe
                .get(&slice_key.make_object_storage_path())
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            content
        );
    }

    #[tokio::test]
    async fn missing_local_stage_is_not_mistaken_for_remote_durability() {
        let tempdir = tempfile::tempdir().unwrap();
        let cache = Arc::new(
            FileCache::new(
                test_config(tempdir.path().join("stage"), Duration::from_secs(3600)),
                kiseki_utils::object_storage::new_local_object_store(tempdir.path().join("remote"))
                    .unwrap(),
            )
            .unwrap(),
        );
        let content = b"a vanished stage is data loss, not success";
        let slice_key = SliceKey::new(0xABCD_EF07, 0, content.len());
        stage_bytes(&cache, slice_key, content).await;
        cache
            .local_storage
            .delete(&slice_key.make_object_storage_path())
            .await
            .unwrap();

        assert!(cache.get_range(&slice_key, 0, content.len()).await.is_err());
        assert!(cache.flush_key(&slice_key).await.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn restart_recovers_staged_block_index() {
        let tempdir = tempfile::tempdir().unwrap();
        let stage_dir = tempdir.path().join("stage");
        let remote_dir = tempdir.path().join("remote");
        let remote_storage =
            kiseki_utils::object_storage::new_local_object_store(&remote_dir).unwrap();
        let remote_probe = remote_storage.clone();
        let content = b"recover me after restart";
        let slice_key = SliceKey::new(0xABCD_EF02, 1, content.len());

        let cache = Arc::new(
            FileCache::new(
                test_config(stage_dir.clone(), Duration::from_secs(3600)),
                remote_storage.clone(),
            )
            .unwrap(),
        );
        stage_bytes(&cache, slice_key, content).await;
        drop(cache);
        std::fs::remove_dir_all(&remote_dir).unwrap();
        std::fs::write(&remote_dir, b"keep recovered data local first").unwrap();

        let recovered = Arc::new(
            FileCache::new(
                test_config(stage_dir, Duration::from_secs(3600)),
                remote_storage,
            )
            .unwrap(),
        );
        let bytes = recovered
            .get_range(&slice_key, 0, content.len())
            .await
            .unwrap()
            .expect("restart must rebuild the stage index");
        assert_eq!(bytes.as_ref(), content);

        std::fs::remove_file(&remote_dir).unwrap();
        std::fs::create_dir(&remote_dir).unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if remote_probe
                    .get(&slice_key.make_object_storage_path())
                    .await
                    .is_ok()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("recovered stage entry must be scheduled for migration");
    }

    #[test]
    fn abrupt_process_exit_preserves_stage_for_restart_recovery() {
        let tempdir = tempfile::tempdir().unwrap();
        let stage_dir = tempdir.path().join("stage");
        let remote_dir = tempdir.path().join("remote");
        let output = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("cache::file_cache::tests::crash_after_stage_helper")
            .arg("--ignored")
            .env("KISEKI_STAGE_CRASH_HELPER", "1")
            .env("KISEKI_STAGE_CRASH_DIR", &stage_dir)
            .env("KISEKI_STAGE_CRASH_REMOTE", &remote_dir)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "stage helper failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let remote_storage =
                kiseki_utils::object_storage::new_local_object_store(&remote_dir).unwrap();
            let remote_probe = remote_storage.clone();
            std::fs::remove_dir_all(&remote_dir).unwrap();
            std::fs::write(&remote_dir, b"hold recovery in the local-readable state").unwrap();
            let cache = Arc::new(
                FileCache::new(
                    test_config(stage_dir, Duration::from_secs(3600)),
                    remote_storage,
                )
                .unwrap(),
            );
            let content = b"survive abrupt process exit";
            let slice_key = SliceKey::new(0xABCD_EF08, 0, content.len());
            assert_eq!(
                cache
                    .get_range(&slice_key, 0, content.len())
                    .await
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                content
            );

            std::fs::remove_file(&remote_dir).unwrap();
            std::fs::create_dir_all(&remote_dir).unwrap();
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    if remote_probe
                        .get(&slice_key.make_object_storage_path())
                        .await
                        .is_ok()
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("recovered stage must migrate after remote repair");
        });
    }

    #[ignore = "subprocess helper"]
    #[tokio::test]
    async fn crash_after_stage_helper() {
        if std::env::var_os("KISEKI_STAGE_CRASH_HELPER").is_none() {
            return;
        }
        let stage_dir = PathBuf::from(std::env::var_os("KISEKI_STAGE_CRASH_DIR").unwrap());
        let remote_dir = PathBuf::from(std::env::var_os("KISEKI_STAGE_CRASH_REMOTE").unwrap());
        let cache = Arc::new(
            FileCache::new(
                test_config(stage_dir, Duration::from_secs(3600)),
                kiseki_utils::object_storage::new_local_object_store(remote_dir).unwrap(),
            )
            .unwrap(),
        );
        let content = b"survive abrupt process exit";
        stage_bytes(
            &cache,
            SliceKey::new(0xABCD_EF08, 0, content.len()),
            content,
        )
        .await;
        std::process::exit(0);
    }

    #[tokio::test]
    async fn restart_discards_only_recognizable_incomplete_stage_files() {
        let tempdir = tempfile::tempdir().unwrap();
        let stage_dir = tempdir.path().join("stage");
        std::fs::create_dir(&stage_dir).unwrap();
        let key = SliceKey::new(0xABCD_EF04, 0, 16);
        let temp_name = format!(".{}.tmp-123-456", key.gen_path_for_object_sto());
        let temp_path = stage_dir.join(temp_name);
        std::fs::write(&temp_path, b"partial").unwrap();

        let cache = FileCache::new(
            test_config(stage_dir, Duration::from_secs(3600)),
            kiseki_utils::object_storage::new_memory_object_store(),
        )
        .unwrap();

        assert!(!temp_path.exists());
        assert!(cache.staged_index.read().await.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn basic() {
        kiseki_utils::logger::install_fmt_log();

        let tempdir = tempfile::tempdir().unwrap();
        let config = Config {
            stage_cache_dir: tempdir.path().to_path_buf(),
            max_stage_size:  ReadableSize(30 << 20), // 30M
            cache_ttl:       Duration::from_secs(1),
        };
        let remote_storage = kiseki_utils::object_storage::new_memory_object_store();
        let cache = Arc::new(FileCache::new(config, remote_storage).unwrap());

        let pool_size = 20 << 20; // 20M
        let memory_pool = pool::memory_pool::MemoryPagePool::new(PAGE_SIZE, pool_size).unwrap();

        let mut pages: Box<[Option<Page>]> = (0..(BLOCK_SIZE / PAGE_SIZE)).map(|_| None).collect();
        let mut mem_page = memory_pool.acquire_page().await;
        let content = b"hello world".to_vec();
        let mut reader = std::io::Cursor::new(&content);
        mem_page
            .copy_from_reader(0, content.len(), &mut reader)
            .await
            .unwrap();
        pages[0] = Some(Page::Memory(mem_page));

        let slice_key = SliceKey::new(1, 1, content.len());

        // the local storage should be empty at beginning.
        let get_result = cache
            .local_storage
            .get(&slice_key.make_object_storage_path())
            .await;
        assert!(get_result.is_err());

        let (total_flush_len, total_release_page) = cache
            .stage(
                slice_key.slice_id,
                slice_key.block_idx,
                slice_key.block_size,
                &pages,
            )
            .await
            .unwrap();
        assert_eq!(total_flush_len, slice_key.block_size);
        assert_eq!(total_release_page, 1);

        let cache_index = cache.index.get(&slice_key).await.unwrap();
        println!("{cache_index:?}");
        // we should be able to find the block from local storage.
        let bytes = cache
            .local_storage
            .get(&slice_key.make_object_storage_path())
            .await
            .unwrap();
        let buf = bytes.bytes().await.unwrap();
        assert_eq!(buf.len(), slice_key.block_size);
        assert_eq!(buf.as_ref(), content.as_slice());

        tokio::time::sleep(Duration::from_secs(2)).await;

        let cached = cache.index.get(&slice_key).await;
        assert!(cached.is_none());

        // the local storage should be empty after the cache expired.
        let bytes = cache
            .local_storage
            .get(&slice_key.make_object_storage_path())
            .await;
        assert!(bytes.is_err());

        // now, we could find the block from the remote storage.
        let bytes = cache
            .remote_storage
            .get(&slice_key.make_object_storage_path())
            .await
            .unwrap();
        let buf = bytes.bytes().await.unwrap();
        assert_eq!(buf.len(), slice_key.block_size);
        assert_eq!(buf.as_ref(), content.as_slice());
    }
}
