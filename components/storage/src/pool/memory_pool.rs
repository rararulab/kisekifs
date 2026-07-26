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
    io::Cursor,
    sync::{Arc, Weak},
};

use crossbeam_queue::ArrayQueue;
use kiseki_utils::readable_size::ReadableSize;
use snafu::ResultExt;
use tokio::{sync::Notify, time::Instant};
use tracing::debug;

use crate::err::{
    InvalidPagePoolConfigSnafu, InvalidRangeSnafu, UnexpectedLengthSnafu, UnknownIOSnafu,
};

pub struct MemoryPagePool {
    page_size: usize,
    capacity:  usize,
    queue:     ArrayQueue<Box<[u8]>>,
    notify:    Notify,
}

impl MemoryPagePool {
    pub fn new(page_size: usize, capacity: usize) -> crate::err::Result<Arc<Self>> {
        let start_at = Instant::now();
        if page_size == 0 || capacity == 0 || !capacity.is_multiple_of(page_size) {
            return InvalidPagePoolConfigSnafu {
                page_size,
                capacity,
            }
            .fail();
        }

        debug!(
            "page pool: page_size: {}, capacity: {}",
            ReadableSize(page_size as u64),
            ReadableSize(capacity as u64)
        );
        let page_cnt = capacity / page_size;

        let pool = Arc::new(Self {
            page_size,
            capacity,
            queue: ArrayQueue::new(page_cnt),
            notify: Notify::default(),
        });

        for _ in 0..page_cnt {
            pool.queue
                .push(vec![0u8; page_size].into_boxed_slice())
                .expect("new page queue has exactly page_cnt slots");
        }

        debug!(
            "{} initialize finished, cost: {:?}",
            &pool,
            start_at.elapsed(),
        );
        Ok(pool)
    }

    pub fn try_acquire_page(self: &Arc<Self>) -> Option<Page> {
        Some(Page {
            buffer: Some(self.queue.pop()?),
            pool:   Arc::downgrade(self),
        })
    }

    pub async fn acquire_page(self: &Arc<Self>) -> Page {
        loop {
            if let Some(buffer) = self.queue.pop() {
                return Page {
                    buffer: Some(buffer),
                    pool:   Arc::downgrade(self),
                };
            }
            self.notify.notified().await;
        }
    }

    fn recycle(&self, mut buffer: Box<[u8]>) {
        buffer.fill(0);
        if self.queue.push(buffer).is_ok() {
            self.notify.notify_one();
        }
    }

    pub fn remain_page_cnt(&self) -> usize { self.queue.len() }

    #[inline]
    pub const fn total_page_cnt(&self) -> usize { self.capacity / self.page_size }

    #[allow(dead_code)] // only exercised by tests so far
    #[inline]
    pub const fn capacity(&self) -> usize { self.capacity }
}

impl Display for MemoryPagePool {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PagePool {{ page_size: {}, capacity: {}, remain: {}, total_cnt: {} }}",
            ReadableSize(self.page_size as u64),
            ReadableSize(self.capacity as u64),
            self.remain_page_cnt(),
            self.total_page_cnt(),
        )
    }
}

/// An exclusively owned page returned by the pool.
///
/// Dropping it clears and returns the buffer while the pool is alive. If the
/// pool has already been dropped, the buffer allocation is reclaimed directly.
pub struct Page {
    buffer: Option<Box<[u8]>>,
    pool:   Weak<MemoryPagePool>,
}

impl Page {
    fn checked_range(
        &self,
        offset: usize,
        length: usize,
    ) -> crate::err::Result<std::ops::Range<usize>> {
        let bound = self.size();
        let Some(end) = offset.checked_add(length).filter(|end| *end <= bound) else {
            return InvalidRangeSnafu {
                subject: "memory page",
                offset,
                length,
                bound,
            }
            .fail();
        };
        Ok(offset..end)
    }

    pub(crate) async fn copy_to_writer<W>(
        &self,
        offset: usize,
        length: usize,
        writer: &mut W,
    ) -> crate::err::Result<()>
    where
        W: tokio::io::AsyncWrite + Unpin + ?Sized,
    {
        let range = self.checked_range(offset, length)?;
        let slice = &self.buffer.as_deref().expect("page buffer is present")[range];
        let mut cursor = Cursor::new(slice);
        let copy_len = tokio::io::copy(&mut cursor, writer)
            .await
            .context(UnknownIOSnafu)?;
        if copy_len as usize != length {
            return UnexpectedLengthSnafu {
                subject:  "memory page write",
                expected: length,
                actual:   copy_len as usize,
            }
            .fail();
        }
        Ok(())
    }

    pub(crate) async fn copy_from_reader<R>(
        &mut self,
        offset: usize,
        length: usize,
        reader: &mut R,
    ) -> crate::err::Result<()>
    where
        R: tokio::io::AsyncRead + Unpin + ?Sized,
    {
        let range = self.checked_range(offset, length)?;
        let slice = &mut self.buffer.as_deref_mut().expect("page buffer is present")[range];
        let mut cursor = Cursor::new(slice);
        let copy_len = tokio::io::copy(reader, &mut cursor)
            .await
            .context(UnknownIOSnafu)?;
        if copy_len as usize != length {
            return UnexpectedLengthSnafu {
                subject:  "memory page read",
                expected: length,
                actual:   copy_len as usize,
            }
            .fail();
        }
        Ok(())
    }

    #[allow(dead_code)] // only exercised by tests so far
    pub(crate) fn size(&self) -> usize {
        self.buffer
            .as_deref()
            .expect("page buffer is present")
            .len()
    }
}

impl Drop for Page {
    fn drop(&mut self) {
        let Some(buffer) = self.buffer.take() else {
            return;
        };
        if let Some(pool) = self.pool.upgrade() {
            pool.recycle(buffer);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use kiseki_utils::logger::install_fmt_log;
    use tokio::{sync::Barrier, time::Duration};

    use super::*;

    #[test]
    fn miri_owned_buffer_recycles_without_aliasing() {
        let pool = MemoryPagePool::new(16, 16).unwrap();
        let mut page = pool.try_acquire_page().unwrap();
        page.buffer.as_deref_mut().unwrap()[3..8].copy_from_slice(b"hello");
        assert_eq!(&page.buffer.as_deref().unwrap()[3..8], b"hello");

        drop(page);

        let page = pool.try_acquire_page().unwrap();
        assert_eq!(page.buffer.as_deref().unwrap(), &[0; 16]);
    }

    #[test]
    fn miri_outstanding_page_drops_after_its_pool() {
        let pool = MemoryPagePool::new(16, 16).unwrap();
        let weak = Arc::downgrade(&pool);
        let page = pool.try_acquire_page().unwrap();

        drop(pool);
        assert!(weak.upgrade().is_none());
        drop(page);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn basic() {
        install_fmt_log();

        let pool = MemoryPagePool::new(128, 128 * 3).unwrap();
        let page = pool.acquire_page().await;
        assert_eq!(page.size(), 128);
        assert_eq!(pool.remain_page_cnt(), pool.total_page_cnt() - 1);
        drop(page);
        assert_eq!(pool.remain_page_cnt(), pool.total_page_cnt());
    }

    #[tokio::test]
    async fn exhausted_waiter_wakes_after_recycle() {
        let pool = MemoryPagePool::new(32, 32).unwrap();
        let page = pool.acquire_page().await;
        let waiter = {
            let pool = pool.clone();
            tokio::spawn(async move { pool.acquire_page().await })
        };
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        drop(page);

        let recycled = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap();
        drop(recycled);
        assert_eq!(pool.remain_page_cnt(), 1);
    }

    #[tokio::test]
    async fn recycled_page_is_zeroed() {
        let pool = MemoryPagePool::new(16, 16).unwrap();
        let mut page = pool.acquire_page().await;
        let mut reader = Cursor::new(b"hello");
        page.copy_from_reader(3, 5, &mut reader).await.unwrap();
        drop(page);

        let page = pool.acquire_page().await;
        let mut actual = [0xA5; 8];
        page.copy_to_writer(0, actual.len(), &mut Cursor::new(actual.as_mut_slice()))
            .await
            .unwrap();
        assert_eq!(actual, [0; 8]);
    }

    #[tokio::test]
    async fn rejects_out_of_range_page_access() {
        let pool = MemoryPagePool::new(16, 16).unwrap();
        let mut page = pool.acquire_page().await;

        assert!(
            page.copy_from_reader(15, 2, &mut Cursor::new([1, 2]))
                .await
                .is_err()
        );
        assert!(
            page.copy_to_writer(usize::MAX, 1, &mut Cursor::new(Vec::new()))
                .await
                .is_err()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn independent_pages_can_be_used_concurrently() {
        const PAGE_COUNT: usize = 4;
        let pool = MemoryPagePool::new(32, 32 * PAGE_COUNT).unwrap();
        let barrier = Arc::new(Barrier::new(PAGE_COUNT));
        let mut handles = Vec::new();
        for value in 0..PAGE_COUNT as u8 {
            let pool = pool.clone();
            let barrier = barrier.clone();
            handles.push(tokio::spawn(async move {
                let mut page = pool.acquire_page().await;
                page.copy_from_reader(7, 1, &mut Cursor::new([value]))
                    .await
                    .unwrap();
                barrier.wait().await;
                let mut actual = [0xFF];
                page.copy_to_writer(7, 1, &mut Cursor::new(actual.as_mut_slice()))
                    .await
                    .unwrap();
                assert_eq!(actual, [value]);
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        assert_eq!(pool.remain_page_cnt(), pool.total_page_cnt());
    }

    #[tokio::test]
    async fn outstanding_page_does_not_keep_pool_alive() {
        let pool = MemoryPagePool::new(16, 16).unwrap();
        let weak = Arc::downgrade(&pool);
        let page = pool.acquire_page().await;

        drop(pool);

        assert!(weak.upgrade().is_none());
        drop(page);
    }
}
