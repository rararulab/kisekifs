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

pub mod disk_pool;
pub mod memory_pool;

use std::{
    fmt::{Debug, Formatter},
    path::PathBuf,
    sync::Arc,
};

use kiseki_utils::readable_size::ReadableSize;
use tracing::debug;

use crate::err::{InvalidPagePoolConfigSnafu, MissingDiskPagePoolPathSnafu, Result};

#[derive(Debug, Default)]
pub struct PagePoolBuilder {
    page_size:       usize,
    memory_capacity: usize,
    // disk page pool is optional
    disk_capacity:   Option<usize>,
    disk_pool_path:  Option<PathBuf>,
}

impl PagePoolBuilder {
    pub const fn with_page_size(mut self, page_size: usize) -> Self {
        self.page_size = page_size;
        self
    }

    pub const fn with_memory_capacity(mut self, memory_capacity: usize) -> Self {
        self.memory_capacity = memory_capacity;
        self
    }

    pub const fn with_disk_capacity(mut self, disk_capacity: usize) -> Self {
        self.disk_capacity = Some(disk_capacity);
        self
    }

    pub fn with_disk_capacity_and_path(
        mut self,
        disk_capacity: usize,
        path: impl Into<PathBuf>,
    ) -> Self {
        self.disk_capacity = Some(disk_capacity);
        self.disk_pool_path = Some(path.into());
        self
    }

    pub async fn build(self) -> Result<HybridPagePool> {
        validate_page_pool_config(self.page_size, self.memory_capacity)?;
        let mut total_page_cnt = self.memory_capacity / self.page_size;
        let memory_pool = memory_pool::MemoryPagePool::new(self.page_size, self.memory_capacity)?;
        let (disk_pool, disk_pool_path, disk_capacity) =
            if let Some(disk_capacity) = self.disk_capacity {
                validate_page_pool_config(self.page_size, disk_capacity)?;
                total_page_cnt += disk_capacity / self.page_size;
                let disk_pool_path = self
                    .disk_pool_path
                    .ok_or_else(|| MissingDiskPagePoolPathSnafu.build())?;
                let disk_pool =
                    disk_pool::DiskPagePool::new(&disk_pool_path, self.page_size, disk_capacity)
                        .await?;
                (Some(disk_pool), Some(disk_pool_path), disk_capacity)
            } else {
                (None, None, 0)
            };

        Ok(HybridPagePool {
            page_size: self.page_size,
            memory_capacity: self.memory_capacity,
            disk_capacity,
            total_page_cnt,
            memory_pool,
            disk_pool,
            disk_pool_path,
        })
    }
}

fn validate_page_pool_config(page_size: usize, capacity: usize) -> Result<()> {
    if page_size == 0 || capacity == 0 || !capacity.is_multiple_of(page_size) {
        return InvalidPagePoolConfigSnafu {
            page_size,
            capacity,
        }
        .fail();
    }
    Ok(())
}

/// HybridPagePool is a hybrid page pool that can store pages in memory and on
/// disk. It is used to store pages in memory when the memory is sufficient, and
/// to store pages on disk when the memory is insufficient.
pub struct HybridPagePool {
    page_size:       usize,
    memory_capacity: usize,
    disk_capacity:   usize,
    disk_pool_path:  Option<PathBuf>,
    total_page_cnt:  usize,

    memory_pool: Arc<memory_pool::MemoryPagePool>,
    disk_pool:   Option<Arc<disk_pool::DiskPagePool>>,
}

impl Debug for HybridPagePool {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "HybridPagePool {{ page_size: {}, memory_capacity: {}, disk_capacity: {}, disk_path: \
             {}, remain_page_cnt: {}, total_page_cnt: {} }}",
            ReadableSize(self.page_size as u64),
            ReadableSize(self.memory_capacity as u64),
            ReadableSize(self.disk_capacity as u64),
            self.disk_pool_path
                .as_deref()
                .map_or_else(|| "disabled".to_string(), |path| path.display().to_string()),
            self.remain(),
            self.total_page_cnt,
        )
    }
}

impl HybridPagePool {
    pub fn try_acquire_page(self: &Arc<Self>) -> Option<Page> {
        if let Some(page) = self.memory_pool.try_acquire_page() {
            return Some(Page::Memory(page));
        }

        if let Some(disk_pool) = &self.disk_pool
            && let Some(page) = disk_pool.try_acquire_page()
        {
            return Some(Page::Disk(page));
        }

        None
    }

    /// acquire_page will wait and  acquire a page from the page pool.
    pub async fn acquire_page(self: &Arc<Self>) -> Page {
        // let disk_pool = self.disk_pool.as_ref().unwrap();
        // let page = disk_pool.acquire_page().await;
        // return Page::Disk(page);

        debug!("pool free ratio {:?}", self.free_ratio());

        if self.memory_pool.remain_page_cnt() > 0
            && let Some(page) = self.try_acquire_page()
        {
            return page;
        }

        if let Some(disk_pool) = &self.disk_pool {
            let page = disk_pool.acquire_page().await;
            return Page::Disk(page);
        }
        let page = self.memory_pool.acquire_page().await;
        Page::Memory(page)
    }

    pub fn remain(&self) -> usize {
        self.memory_pool.remain_page_cnt()
            + self
                .disk_pool
                .as_ref()
                .map_or(0, |pool| pool.remain_page_cnt())
    }

    #[allow(dead_code)] // only exercised by tests so far
    pub const fn total_page_cnt(&self) -> usize { self.total_page_cnt }

    #[allow(dead_code)] // only exercised by tests so far
    pub const fn capacity(&self) -> usize { self.memory_capacity + self.disk_capacity }

    pub fn free_ratio(&self) -> f64 { self.remain() as f64 / self.total_page_cnt as f64 }
}

pub enum Page {
    Memory(memory_pool::Page),
    Disk(disk_pool::Page),
}

impl Page {
    pub(crate) async fn copy_to_writer<W>(
        &self,
        offset: usize,
        length: usize,
        writer: &mut W,
    ) -> Result<()>
    where
        W: tokio::io::AsyncWrite + Unpin + ?Sized,
    {
        match self {
            Self::Memory(page) => page.copy_to_writer(offset, length, writer).await,
            Self::Disk(page) => page.copy_to_writer(offset, length, writer).await,
        }
    }

    pub(crate) async fn copy_from_reader<R>(
        &mut self,
        offset: usize,
        length: usize,
        reader: &mut R,
    ) -> Result<()>
    where
        R: tokio::io::AsyncRead + Unpin + ?Sized,
    {
        match self {
            Self::Memory(page) => page.copy_from_reader(offset, length, reader).await,
            Self::Disk(page) => page.copy_from_reader(offset, length, reader).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use kiseki_common::PAGE_SIZE;
    use kiseki_utils::logger::install_fmt_log;
    use tokio::time::Instant;
    use tracing::debug;

    use super::*;

    #[tokio::test]
    async fn rejects_invalid_pool_configuration() {
        assert!(
            PagePoolBuilder::default()
                .with_page_size(0)
                .with_memory_capacity(PAGE_SIZE)
                .build()
                .await
                .is_err()
        );
        assert!(
            PagePoolBuilder::default()
                .with_page_size(PAGE_SIZE)
                .with_memory_capacity(PAGE_SIZE + 1)
                .build()
                .await
                .is_err()
        );
        assert!(
            PagePoolBuilder::default()
                .with_page_size(PAGE_SIZE)
                .with_memory_capacity(PAGE_SIZE)
                .with_disk_capacity(PAGE_SIZE)
                .build()
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn supports_a_single_memory_page() {
        let pool = Arc::new(
            PagePoolBuilder::default()
                .with_page_size(PAGE_SIZE)
                .with_memory_capacity(PAGE_SIZE)
                .build()
                .await
                .unwrap(),
        );

        let page = pool.acquire_page().await;
        assert_eq!(pool.remain(), 0);
        drop(page);
        assert_eq!(pool.remain(), 1);
    }

    #[tokio::test]
    async fn basic() {
        install_fmt_log();

        let start = Instant::now();

        // 为测试创建唯一的临时目录避免并发冲突
        let temp_dir = tempfile::tempdir().unwrap();
        let disk_pool_path = temp_dir.path().join("test_page_pool");

        let pool = Arc::new(
            PagePoolBuilder::default()
                .with_page_size(PAGE_SIZE)
                .with_memory_capacity(PAGE_SIZE * 3)
                .with_disk_capacity_and_path(PAGE_SIZE * 3, disk_pool_path.to_str().unwrap())
                .build()
                .await
                .unwrap(),
        );

        let total_page_cnt = pool.total_page_cnt();
        let handles = (0..total_page_cnt)
            .map(|i| {
                let pool = pool.clone();
                tokio::spawn(async move {
                    let mut page = pool.acquire_page().await;
                    let mut data = Vec::from(format!("hello {i}"));
                    let data_len = data.len();
                    let mut cursor = Cursor::new(&mut data);
                    page.copy_from_reader(0, data_len, &mut cursor)
                        .await
                        .unwrap();

                    let mut dst = vec![0u8; data_len];
                    let mut writer = Cursor::new(&mut dst);
                    page.copy_to_writer(0, data_len, &mut writer).await.unwrap();
                    assert_eq!(dst, data);
                })
            })
            .collect::<Vec<_>>();

        futures::future::join_all(handles).await;

        debug!(
            "total time: {:?} for {}",
            start.elapsed(),
            ReadableSize(pool.capacity() as u64)
        );
    }
}
