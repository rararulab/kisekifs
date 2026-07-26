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
    fmt::{Display, Formatter},
    path::Path,
    sync::Arc,
};

use crossbeam_queue::ArrayQueue;
use fmmap::tokio::{AsyncMmapFileExt, AsyncMmapFileMut, AsyncMmapFileMutExt, AsyncOptions};
use kiseki_utils::readable_size::ReadableSize;
use snafu::ResultExt;
use tokio::{
    sync::{Notify, RwLock},
    time::Instant,
};
use tracing::debug;

use crate::err::{
    DiskPoolMmapSnafu, InvalidPagePoolConfigSnafu, InvalidRangeSnafu, Result,
    UnexpectedLengthSnafu, UnknownIOSnafu,
};

pub(crate) struct DiskPagePool {
    // the size of each page.
    page_size: usize,
    // the total space of the file will use.
    capacity:  usize,
    // the queue of the pages.
    queue:     ArrayQueue<u64>,
    // ready notify.
    notify:    Notify,
    // the underlying persistent storage support
    file:      RwLock<AsyncMmapFileMut>,
}

impl DiskPagePool {
    pub(crate) async fn new<P: AsRef<Path>>(
        path: P,
        page_size: usize,
        capacity: usize,
    ) -> Result<Arc<Self>> {
        let start = Instant::now();
        if page_size == 0 || capacity == 0 || !capacity.is_multiple_of(page_size) {
            return InvalidPagePoolConfigSnafu {
                page_size,
                capacity,
            }
            .fail();
        }
        let cnt = capacity / page_size;
        let mut file = AsyncOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .max_size(capacity as u64)
            .open_mmap_file_mut(path)
            .await
            .context(DiskPoolMmapSnafu)?;

        file.truncate(capacity as u64)
            .await
            .context(DiskPoolMmapSnafu)?;
        let queue = ArrayQueue::new(cnt);
        (0..cnt as u64).for_each(|page_id| {
            queue.push(page_id).unwrap();
        });
        debug!("create disk pool finished, cost: {:?}", start.elapsed());
        Ok(Arc::new(Self {
            page_size,
            capacity,
            queue,
            notify: Notify::default(),
            file: RwLock::new(file),
        }))
    }

    pub(crate) fn try_acquire_page(self: &Arc<Self>) -> Option<Page> {
        let page_id = self.queue.pop();
        page_id.map(|page_id| Page {
            page_id,
            pool: self.clone(),
        })
    }

    pub(crate) async fn acquire_page(self: &Arc<Self>) -> Page {
        let mut page_id = self.queue.pop();
        while page_id.is_none() {
            self.notify.notified().await;
            page_id = self.queue.pop();
        }
        Page {
            page_id: page_id.unwrap(),
            pool:    self.clone(),
        }
    }

    pub(crate) fn remain_page_cnt(&self) -> usize { self.queue.len() }

    pub(crate) const fn total_page_cnt(&self) -> usize { self.capacity / self.page_size }
}

impl Display for DiskPagePool {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DiskPool {{ page_size: {}, capacity: {}, remain: {}, total_cnt: {} }}",
            ReadableSize(self.page_size as u64),
            ReadableSize(self.capacity as u64),
            self.remain_page_cnt(),
            self.total_page_cnt(),
        )
    }
}

pub struct Page {
    page_id: u64,
    pool:    Arc<DiskPagePool>,
}

impl Page {
    fn checked_range(&self, offset: usize, length: usize) -> Result<std::ops::Range<usize>> {
        let bound = self.pool.page_size;
        let Some(end) = offset.checked_add(length).filter(|end| *end <= bound) else {
            return InvalidRangeSnafu {
                subject: "disk page",
                offset,
                length,
                bound,
            }
            .fail();
        };
        Ok(offset..end)
    }

    // `reader` borrows the mmap read guard for the whole copy, so the guard
    // cannot drop before the block's tail expression; the lint can't see
    // through that borrow.
    #[allow(clippy::significant_drop_tightening)]
    pub(crate) async fn copy_to_writer<W>(
        &self,
        offset: usize,
        length: usize,
        writer: &mut W,
    ) -> Result<()>
    where
        W: tokio::io::AsyncWrite + Unpin + ?Sized,
    {
        let range = self.checked_range(offset, length)?;
        // Hold the mmap read lock only for the duration of the copy, not the
        // subsequent length validation.
        let copy_len = {
            let guard = self.pool.file.read().await;
            let mut reader = guard
                .range_reader(self.cal_offset() + range.start, range.len())
                .context(DiskPoolMmapSnafu)?;
            tokio::io::copy(&mut reader, writer)
                .await
                .context(UnknownIOSnafu)?
        };
        if copy_len as usize != length {
            return UnexpectedLengthSnafu {
                subject:  "disk page write",
                expected: length,
                actual:   copy_len as usize,
            }
            .fail();
        }
        Ok(())
    }

    // `writer` borrows the mmap write guard for the whole copy, so the guard
    // cannot drop before the block's tail expression; the lint can't see
    // through that borrow.
    #[allow(clippy::significant_drop_tightening)]
    pub(crate) async fn copy_from_reader<R>(
        &mut self,
        offset: usize,
        length: usize,
        reader: &mut R,
    ) -> Result<()>
    where
        R: tokio::io::AsyncRead + Unpin + ?Sized,
    {
        let range = self.checked_range(offset, length)?;
        // Hold the mmap write lock only for the duration of the copy, not the
        // subsequent length validation.
        let copy_len = {
            let mut guard = self.pool.file.write().await;
            let mut writer = guard
                .range_writer(self.cal_offset() + range.start, range.len())
                .context(DiskPoolMmapSnafu)?;
            tokio::io::copy(reader, &mut writer)
                .await
                .context(UnknownIOSnafu)?
        };
        if copy_len as usize != length {
            return UnexpectedLengthSnafu {
                subject:  "disk page read",
                expected: length,
                actual:   copy_len as usize,
            }
            .fail();
        }
        Ok(())
    }

    fn cal_offset(&self) -> usize { self.page_id as usize * self.pool.page_size }
}

impl Drop for Page {
    fn drop(&mut self) {
        self.pool.queue.push(self.page_id).unwrap();
        self.pool.notify.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use bytes::Bytes;
    use kiseki_utils::logger::install_fmt_log;
    use tokio_util::io::StreamReader;
    use tracing::info;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn basic() {
        install_fmt_log();

        let tempfile = tempfile::NamedTempFile::new().unwrap();
        let path = tempfile.path();

        let page_size = 128 << 10;
        let cap = page_size * 3;

        let pool = DiskPagePool::new(path, page_size, cap).await.unwrap();
        let meta = fs::metadata(path).unwrap();
        assert_eq!(meta.len(), cap as u64);

        assert_eq!(pool.remain_page_cnt(), pool.total_page_cnt());
        let page = pool.acquire_page().await;
        assert_eq!(pool.remain_page_cnt(), pool.total_page_cnt() - 1);
        drop(page);
        assert_eq!(pool.remain_page_cnt(), pool.total_page_cnt());
    }

    #[tokio::test]
    async fn get_page_concurrently() {
        install_fmt_log();
        let tempfile = tempfile::NamedTempFile::new().unwrap();
        let path = tempfile.path();
        let page_size = 128 << 10;
        let cap = page_size * 3;

        let pool = DiskPagePool::new(path, page_size, cap).await.unwrap();
        let start = std::time::Instant::now();
        let mut handles = vec![];
        for _ in 0..pool.total_page_cnt() {
            let pool = pool.clone();
            let handle = tokio::spawn(async move {
                let mut page = pool.acquire_page().await;
                // tokio::time::sleep(Duration::from_millis(1)).await;
                let mut reader = StreamReader::new(tokio_stream::iter(vec![std::io::Result::Ok(
                    Bytes::from_static(b"hello"),
                )]));

                page.copy_from_reader(0, 5, &mut reader).await.unwrap();
                let mut actual = [0u8; 5];
                page.copy_to_writer(
                    0,
                    actual.len(),
                    &mut std::io::Cursor::new(actual.as_mut_slice()),
                )
                .await
                .unwrap();
                assert_eq!(&actual, b"hello");
            });
            handles.push(handle);
        }

        assert!(pool.remain_page_cnt() <= pool.total_page_cnt());
        for result in futures::future::join_all(handles).await {
            result.unwrap();
        }

        info!(
            "fill the whole pool {} cost: {:?}",
            ReadableSize(pool.capacity as u64),
            start.elapsed(),
        );

        assert_eq!(pool.remain_page_cnt(), pool.total_page_cnt());
    }
}
