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
    cmp::{max, min},
    io::Cursor,
};

use kiseki_common::{BLOCK_SIZE, BlockIndex, BlockSize, CHUNK_SIZE, PAGE_SIZE};
use kiseki_types::slice::SliceID;
use kiseki_utils::{
    object_storage::{ObjectStorage, ObjectStoragePath},
    readable_size::ReadableSize,
};
use snafu::ResultExt;
use tokio::{io::AsyncWriteExt, time::Instant};
use tracing::{debug, instrument};

use crate::{
    cache,
    err::{
        InvalidRangeSnafu, InvalidSliceBufferWriteOffsetSnafu, ObjectStorageSnafu, Result,
        UnexpectedLengthSnafu, UnknownIOSnafu,
    },
    pool::{GLOBAL_HYBRID_PAGE_POOL, Page},
};

// read_slice_from_object_storage will allocate memory in place and then drop
// it.
#[instrument(skip_all, fields(length, offset))]
pub async fn read_slice_from_object_storage<F: Fn(BlockIndex, BlockSize) -> String>(
    gen_key: F,
    object_storage: ObjectStorage,
    length: usize, // length of the slice.
    offset: usize, // read offset
    dst: &mut [u8],
) -> Result<usize> {
    if length > CHUNK_SIZE {
        return InvalidRangeSnafu {
            subject: "object slice",
            offset: 0usize,
            length,
            bound: CHUNK_SIZE,
        }
        .fail();
    }
    if dst.is_empty() || offset >= length {
        return Ok(0);
    }

    let expected_read_len = min(length - offset, dst.len());
    let mut total_read_len = 0;
    let mut remaining_dst = &mut dst[..expected_read_len];
    let mut reads = vec![];

    while total_read_len < expected_read_len {
        let new_pos = total_read_len + offset;
        let block_idx = new_pos / BLOCK_SIZE;
        let block_offset = new_pos % BLOCK_SIZE;
        let obj_block_size = cal_object_block_size(length, block_idx, BLOCK_SIZE);
        let current_block_to_read_len = min(
            expected_read_len - total_read_len,
            obj_block_size - block_offset, // don't exceed the block boundary.
        );

        let key = gen_key(block_idx, obj_block_size);
        let sto = object_storage.clone();
        let (dst_slice, rest) = remaining_dst.split_at_mut(current_block_to_read_len);
        remaining_dst = rest;

        total_read_len += current_block_to_read_len;

        reads.push(async move {
            let path = kiseki_utils::object_storage::ObjectStoragePath::parse(&key)
                .map_err(kiseki_utils::object_storage::ObjectStorageError::from)
                .context(ObjectStorageSnafu)?;
            let block = sto.get(&path).await.context(ObjectStorageSnafu)?;
            let block_buf = block.bytes().await.context(ObjectStorageSnafu)?;
            let Some(block_end) = block_offset.checked_add(current_block_to_read_len) else {
                return InvalidRangeSnafu {
                    subject: "object block",
                    offset:  block_offset,
                    length:  current_block_to_read_len,
                    bound:   block_buf.len(),
                }
                .fail();
            };
            let Some(source) = block_buf.get(block_offset..block_end) else {
                return InvalidRangeSnafu {
                    subject: "object block",
                    offset:  block_offset,
                    length:  current_block_to_read_len,
                    bound:   block_buf.len(),
                }
                .fail();
            };
            dst_slice.copy_from_slice(source);
            Ok(current_block_to_read_len)
        });
    }

    let actual_read_cnt = futures::future::try_join_all(reads)
        .await?
        .into_iter()
        .sum::<usize>();
    if actual_read_cnt != total_read_len {
        return UnexpectedLengthSnafu {
            subject:  "object slice read",
            expected: total_read_len,
            actual:   actual_read_cnt,
        }
        .fail();
    }

    Ok(total_read_len)
}

fn cal_object_block_size(length: usize, block_idx: BlockIndex, block_size: BlockSize) -> usize {
    // min(1025 - 0 * 1024, 1024) = min(1024) = 1024
    // min(1023 - 0 * 1024, 1024) = min(1023, 1024) = 1023
    // min(2049 - 2 * 1024, 1024) = min(1, 1024) = 1
    min(length - block_idx * block_size, block_size)
}

/// SliceAppendOnlyBuffer is a buffer that handle the write requests.
///
/// Random write requests may hit the same slice buffer.
///
/// As long as the SliceBuffer is not frozen, we should be able to
/// modify on it, for achieving better random write performance.
pub struct SliceBuffer {
    /// the slice length, the total write len of the slice.
    /// we may write some block we just write before.
    length:         usize,
    // the last flushed block index.
    flushed_length: usize,
    /// Each block is variable size when we flush, from 128KB to 4MB.
    /// So we can have max CHUNK_SIZE / PAGE_SIZE blocks,
    /// but we use CHUNK_SIZE / BLOCK_SIZE to simplify the logic.
    block_slots:    Box<[Block]>,
    /// how many page do we have in the slice buffer.
    total_page_cnt: usize,
}

impl Default for SliceBuffer {
    fn default() -> Self { Self::new() }
}

impl SliceBuffer {
    pub fn new() -> Self {
        Self {
            length:         0,
            flushed_length: 0,
            block_slots:    (0..(CHUNK_SIZE / BLOCK_SIZE))
                .map(|_| Block::Empty)
                .collect(),
            total_page_cnt: 0,
        }
    }

    /// read_at only for debug.
    pub async fn read_at(&self, offset: usize, dst: &mut [u8]) -> Result<usize> {
        if offset >= self.length {
            return Ok(0);
        }
        if dst.is_empty() {
            return Ok(0);
        }
        let expected_read_len = min(dst.len(), self.length - offset);

        let mut total_read_len = 0;
        while total_read_len < expected_read_len {
            let new_pos = total_read_len + offset;
            let block_idx = new_pos / BLOCK_SIZE;
            let block_offset = new_pos % BLOCK_SIZE;
            let Some(block) = self.block_slots.get(block_idx) else {
                return InvalidRangeSnafu {
                    subject: "slice buffer block",
                    offset:  block_idx,
                    length:  1usize,
                    bound:   self.block_slots.len(),
                }
                .fail();
            };

            let current_block_to_read_len = min(
                expected_read_len - total_read_len,
                BLOCK_SIZE - block_offset, // don't exceed the block boundary.
            );
            // how many bytes we can read from the block.
            let mut total_current_block_read_len = 0;
            while total_current_block_read_len < current_block_to_read_len {
                let new_block_offset = block_offset + total_current_block_read_len;
                let page_idx = new_block_offset / PAGE_SIZE;
                let page_offset = new_block_offset % PAGE_SIZE;

                let current_page_to_read_len = min(
                    current_block_to_read_len - total_current_block_read_len,
                    PAGE_SIZE - page_offset, // don't exceed the page boundary.
                );

                match block {
                    Block::Empty => {
                        // it is a hole, we should pad zero to the dst.
                        dst[total_read_len..total_read_len + current_page_to_read_len].fill(0);
                    }
                    Block::Data(db) => {
                        let Some(page) = db.pages.get(page_idx) else {
                            return InvalidRangeSnafu {
                                subject: "data block page",
                                offset:  page_idx,
                                length:  1usize,
                                bound:   db.pages.len(),
                            }
                            .fail();
                        };
                        if let Some(page) = page {
                            let mut cursor = Cursor::new(
                                &mut dst
                                    [total_read_len..(total_read_len + current_page_to_read_len)],
                            );
                            page.copy_to_writer(page_offset, current_page_to_read_len, &mut cursor)
                                .await?;
                        } else {
                            // it is a hole, we should pad zero to the dst.
                            dst[total_read_len..total_read_len + current_page_to_read_len].fill(0);
                        }
                    }
                };
                total_current_block_read_len += current_page_to_read_len;
                total_read_len += current_page_to_read_len;
            }
        }

        Ok(total_read_len)
    }

    // offset: the offset in the slice buffer, range: [0, CHUNK_SIZE).
    // data size: [0, PAGE_SIZE).
    //
    // usually one write_at call only need to wait on one page available.
    // the worst case is but when we cross the page boundary,
    // we may need to wait on two pages.
    pub async fn write_at(&mut self, offset: usize, data: &[u8]) -> Result<usize> {
        let expected_write_len = data.len();
        if expected_write_len == 0 {
            return Ok(0);
        }

        let Some(write_end) = offset
            .checked_add(expected_write_len)
            .filter(|end| *end <= CHUNK_SIZE)
        else {
            return InvalidRangeSnafu {
                subject: "slice buffer write",
                offset,
                length: expected_write_len,
                bound: CHUNK_SIZE,
            }
            .fail();
        };

        if offset < self.flushed_length {
            return InvalidSliceBufferWriteOffsetSnafu.fail()?;
        }

        debug_assert!(
            offset >= self.flushed_length,
            "offset: {} should be greater than flushed length: {}",
            offset,
            self.flushed_length
        );

        let mut total_write_len = 0;
        while total_write_len < expected_write_len {
            let new_offset = offset + total_write_len;
            let block_index = new_offset / BLOCK_SIZE;
            let block_offset = new_offset % BLOCK_SIZE;
            let block_slots_len = self.block_slots.len();
            let Some(block) = self.block_slots.get_mut(block_index) else {
                return InvalidRangeSnafu {
                    subject: "slice buffer block",
                    offset:  block_index,
                    length:  1usize,
                    bound:   block_slots_len,
                }
                .fail();
            };

            if matches!(block, Block::Empty) {
                *block = Block::new_data_block();
            }
            let Block::Data(data_block) = block else {
                return InvalidRangeSnafu {
                    subject: "empty data block",
                    offset:  block_index,
                    length:  1usize,
                    bound:   block_slots_len,
                }
                .fail();
            };

            // how many bytes we can write to the block.
            let mut total_page_write_len = 0;
            let to_write_block_len = min(
                expected_write_len - total_write_len,
                BLOCK_SIZE - block_offset, // don't exceed the block boundary.
            );
            while total_page_write_len < to_write_block_len {
                let new_block_offset = block_offset + total_page_write_len;
                let page_index = new_block_offset / PAGE_SIZE;
                let page_offset = new_block_offset % PAGE_SIZE;
                let (page, new_one) = data_block.get_page(page_index).await?;
                if new_one {
                    self.total_page_cnt += 1;
                }
                let to_write_page_len = min(
                    to_write_block_len - total_page_write_len,
                    PAGE_SIZE - page_offset, // don't exceed the page boundary.
                );
                let mut reader =
                    Cursor::new(&data[total_write_len..(total_write_len + to_write_page_len)]);
                page.copy_from_reader(page_offset, to_write_page_len, &mut reader)
                    .await?;

                total_page_write_len += to_write_page_len;
                total_write_len += to_write_page_len;
                data_block.length = max(data_block.length, block_offset + total_page_write_len);
            }
        }
        self.length = max(self.length, write_end);
        Ok(total_write_len)
    }

    fn full_block_cnt(&self) -> usize {
        self.block_slots
            .iter()
            .filter(|block| block.is_full())
            .count()
    }

    pub const fn flushed_length(&self) -> usize { self.flushed_length }

    pub const fn length(&self) -> usize { self.length }

    pub fn status(&self) -> SliceBufferStatus {
        let full_cnt = self.full_block_cnt();
        SliceBufferStatus {
            length:            self.length,
            logic_size:        ReadableSize(self.length as u64),
            page_cnt:          self.total_page_cnt,
            real_size:         ReadableSize((self.total_page_cnt * PAGE_SIZE) as u64),
            full_block_cnt:    full_cnt,
            partial_block_cnt: self.block_slots.len() - full_cnt,
        }
    }

    /// flush_bulk flush as much as possible blocks to the storage.
    #[instrument(skip(self, key_gen, object_storage))]
    pub async fn flush_bulk<F: Fn(BlockIndex, BlockSize) -> String>(
        &mut self,
        key_gen: F,
        object_storage: ObjectStorage,
    ) -> Result<usize> {
        self.flush_bulk_to(self.length, key_gen, object_storage)
            .await
    }

    /// flush all written data to the storage.
    #[instrument(skip(self, key_gen, object_storage))]
    pub async fn flush<F: Fn(BlockIndex, BlockSize) -> String>(
        &mut self,
        key_gen: F,
        object_storage: ObjectStorage,
    ) -> Result<usize> {
        if self.length == 0 {
            return Ok(0);
        }
        self.flush_bulk_to(
            ((self.length - 1) / BLOCK_SIZE + 1) * BLOCK_SIZE,
            key_gen,
            object_storage,
        )
        .await
    }

    /// flush_to flush the slice buffer to the storage until the offset.
    ///
    /// Ignore all empty block.
    pub async fn flush_bulk_to<F: Fn(BlockIndex, BlockSize) -> String>(
        &mut self,
        offset: usize,
        key_gen: F,
        object_storage: ObjectStorage,
    ) -> Result<usize> {
        if self.flushed_length > offset {
            return InvalidSliceBufferWriteOffsetSnafu.fail();
        }
        if offset > CHUNK_SIZE {
            return InvalidRangeSnafu {
                subject: "slice buffer flush",
                offset,
                length: 0usize,
                bound: CHUNK_SIZE,
            }
            .fail();
        }

        let pending_block_idxes = self
            .block_slots
            .iter()
            .enumerate()
            .filter(|(idx, block)| match block {
                Block::Empty => false,
                Block::Data(..) => {
                    let block_idx = *idx;
                    // let start = block_idx * BLOCK_SIZE;
                    let end = (block_idx + 1) * BLOCK_SIZE;
                    // dummy_flushed_length = end;
                    end <= offset
                }
            })
            .map(|(idx, _)| idx)
            .collect::<Vec<_>>();

        let mut total_released_page_cnt = 0;
        for idx in pending_block_idxes {
            let Block::Data(data_block) = &self.block_slots[idx] else {
                continue;
            };
            let block_length = data_block.length;
            let released_page_cnt = data_block.pages.iter().flatten().count();
            let key = key_gen(idx, block_length);
            copy_data_block_to_object_storage(data_block, &key, &object_storage).await?;

            self.block_slots[idx] = Block::Empty;
            self.flushed_length = max(self.flushed_length, idx * BLOCK_SIZE + block_length);
            self.total_page_cnt -= released_page_cnt;
            total_released_page_cnt += released_page_cnt;
        }

        debug!(
            "flushed length: {}, total_released_page: {}",
            self.flushed_length, total_released_page_cnt
        );
        Ok(total_released_page_cnt)
    }

    /// flush all written data to the storage.
    #[instrument(skip_all)]
    pub async fn flush_v2(
        &mut self,
        sid: SliceID,
        cache: cache::file_cache::FileCacheRef,
    ) -> Result<usize> {
        if self.length == 0 {
            return Ok(0);
        }
        self.stage(
            sid,
            ((self.length - 1) / BLOCK_SIZE + 1) * BLOCK_SIZE,
            cache,
        )
        .await
    }

    // stage blocks to the local file system.
    pub async fn stage(
        &mut self,
        sid: SliceID,
        offset: usize,
        cache: cache::file_cache::FileCacheRef,
    ) -> Result<usize> {
        if self.flushed_length > offset {
            return InvalidSliceBufferWriteOffsetSnafu.fail();
        }
        if offset > CHUNK_SIZE {
            return InvalidRangeSnafu {
                subject: "slice buffer stage",
                offset,
                length: 0usize,
                bound: CHUNK_SIZE,
            }
            .fail();
        }

        let pending_block_idxes = self
            .block_slots
            .iter()
            .enumerate()
            .filter(|(idx, block)| match block {
                Block::Empty => false,
                Block::Data(..) => {
                    let block_idx = *idx;
                    // let start = block_idx * BLOCK_SIZE;
                    let end = (block_idx + 1) * BLOCK_SIZE;
                    // dummy_flushed_length = end;
                    end <= offset
                }
            })
            .map(|(idx, _)| idx)
            .collect::<Vec<_>>();

        let mut total_released_page_cnt = 0;
        for idx in pending_block_idxes {
            let Block::Data(data_block) = &self.block_slots[idx] else {
                continue;
            };
            let block_length = data_block.length;
            let (_, released_page_cnt) = cache
                .stage(sid, idx, block_length, &data_block.pages)
                .await?;

            self.block_slots[idx] = Block::Empty;
            self.flushed_length = max(self.flushed_length, idx * BLOCK_SIZE + block_length);
            total_released_page_cnt += released_page_cnt;
        }

        self.total_page_cnt -= total_released_page_cnt;
        debug!(
            "slice_id({}), flushed length: {}, total_released_page: {}",
            sid, self.flushed_length, total_released_page_cnt
        );
        Ok(total_released_page_cnt)
    }
}

async fn copy_data_block_to_object_storage(
    data_block: &DataBlock,
    key: &str,
    object_storage: &ObjectStorage,
) -> Result<()> {
    let path = ObjectStoragePath::parse(key)
        .map_err(kiseki_utils::object_storage::ObjectStorageError::from)
        .context(ObjectStorageSnafu)?;
    let mut writer = object_storage.writer(&path);
    let mut current_flush_data = 0;

    while current_flush_data < data_block.length {
        let page_idx = current_flush_data / PAGE_SIZE;
        let page_offset = current_flush_data % PAGE_SIZE;
        let to_flush_len = min(
            PAGE_SIZE - page_offset,
            data_block.length - current_flush_data,
        );
        let Some(page) = data_block.pages.get(page_idx) else {
            return InvalidRangeSnafu {
                subject: "data block page",
                offset:  page_idx,
                length:  1usize,
                bound:   data_block.pages.len(),
            }
            .fail();
        };
        match page {
            None => writer
                .write_all(&vec![0u8; to_flush_len])
                .await
                .context(UnknownIOSnafu)?,
            Some(page) => {
                page.copy_to_writer(page_offset, to_flush_len, &mut writer)
                    .await?;
            }
        }
        current_flush_data += to_flush_len;
    }
    writer.flush().await.context(UnknownIOSnafu)?;
    writer.shutdown().await.context(UnknownIOSnafu)?;
    debug!(
        "write object to {:?}, len: {:?}",
        key,
        ReadableSize(data_block.length as u64).to_string()
    );
    Ok(())
}

#[derive(Debug)]
pub struct SliceBufferStatus {
    pub length:            usize,
    pub logic_size:        ReadableSize,
    // how many pages we use in the slice buffer.
    pub page_cnt:          usize,
    pub real_size:         ReadableSize,
    pub full_block_cnt:    usize,
    pub partial_block_cnt: usize,
}

/// Block represents the real data that is written to the storage.
///
/// Once a block has been flushed, then it become Empty, then we
/// can write new data to it.
#[derive(Default)]
enum Block {
    // The block is empty, doesn't hold any memory,
    // 1. we may just flush it
    // 2. just haven't written data yet.
    // 3. it is a hole.(we don't upload it, reader should fill the gap)
    #[default]
    Empty,
    // The actual data block we have written.
    // Block is composed by one or more pages.
    // Block size from PAGE_SIZE to BLOCK_SIZE.
    // One block can max hold BLOCK_SIZE / PAGE_SIZE pages.
    // It is the smallest unit we can flush to the storage.
    Data(DataBlock),
}

struct DataBlock {
    // the written len of the block
    length: usize,
    pages:  Box<[Option<Page>]>,
}

impl Block {
    fn new_data_block() -> Self {
        Self::Data(DataBlock {
            length: 0,
            pages:  (0..(BLOCK_SIZE / PAGE_SIZE)).map(|_| None).collect(),
        })
    }

    const fn is_full(&self) -> bool {
        match self {
            Self::Empty => false,
            Self::Data(db) => db.length == BLOCK_SIZE,
        }
    }
}

impl DataBlock {
    async fn get_page(&mut self, page_idx: usize) -> Result<(&mut Page, bool)> {
        let start = Instant::now();
        debug!("try to get a page from block.");
        let pages_len = self.pages.len();
        let Some(slot) = self.pages.get_mut(page_idx) else {
            return InvalidRangeSnafu {
                subject: "data block page",
                offset:  page_idx,
                length:  1usize,
                bound:   pages_len,
            }
            .fail();
        };
        let new_one = slot.is_none();
        if new_one {
            *slot = Some(GLOBAL_HYBRID_PAGE_POOL.acquire_page().await);
        }
        debug!(
            "get a page from block, new: {}, cost: {:?}",
            new_one,
            start.elapsed(),
        );
        let Some(page) = slot.as_mut() else {
            return UnexpectedLengthSnafu {
                subject:  "initialized data block page count",
                expected: 1usize,
                actual:   0usize,
            }
            .fail();
        };
        Ok((page, new_one))
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use futures::StreamExt;
    use kiseki_utils::{
        logger::install_fmt_log,
        object_storage::{new_local_object_store, new_memory_object_store},
    };
    use tracing::info;

    use super::*;

    #[tokio::test]
    async fn basic_write() {
        let mut slice_buffer = SliceBuffer::new();
        let data = b"hello".as_slice();

        let write_len = slice_buffer.write_at(0, data).await.unwrap();
        assert_eq!(write_len, data.len());
        assert_eq!(slice_buffer.length, data.len());
        println!("status {:?}", slice_buffer.status());

        let write_len = slice_buffer.write_at(PAGE_SIZE - 3, data).await.unwrap();
        assert_eq!(write_len, data.len());
        assert_eq!(slice_buffer.length, PAGE_SIZE + data.len() - 3);
        println!("status {:?}", slice_buffer.status());

        let write_len = slice_buffer
            .write_at(PAGE_SIZE - 3, vec![1u8; PAGE_SIZE].as_slice())
            .await
            .unwrap();
        assert_eq!(write_len, PAGE_SIZE);
        assert_eq!(slice_buffer.length, PAGE_SIZE - 3 + PAGE_SIZE);
        println!("status {:?}", slice_buffer.status());

        let write_len = slice_buffer
            .write_at(BLOCK_SIZE - 3, vec![1u8; BLOCK_SIZE].as_slice())
            .await
            .unwrap();
        assert_eq!(write_len, BLOCK_SIZE);
        assert_eq!(slice_buffer.length, BLOCK_SIZE + BLOCK_SIZE - 3);
        println!("status {:?}", slice_buffer.status());
    }

    #[tokio::test]
    async fn basic_read() {
        let mut slice_buffer = SliceBuffer::new();
        let data = b"hello".as_slice();

        let write_len = slice_buffer.write_at(0, data).await.unwrap();
        assert_eq!(write_len, data.len());
        assert_eq!(slice_buffer.length, data.len());

        let mut dst = vec![0u8; 5];
        let read_len = slice_buffer.read_at(0, dst.as_mut_slice()).await.unwrap();
        assert_eq!(read_len, 5);
        assert_eq!(dst, data);

        let write_len = slice_buffer.write_at(PAGE_SIZE - 3, data).await.unwrap();
        assert_eq!(write_len, data.len());

        let mut dst = vec![0u8; 5];
        let read_len = slice_buffer
            .read_at(PAGE_SIZE - 3, dst.as_mut_slice())
            .await
            .unwrap();
        assert_eq!(read_len, 5);
        assert_eq!(dst, data);

        // read a hole
        let mut dst = vec![0u8; 5];
        let read_len = slice_buffer.read_at(5, dst.as_mut_slice()).await.unwrap();
        assert_eq!(read_len, 5);
        assert_eq!(dst, vec![0u8; 5]);
    }

    #[tokio::test]
    async fn read_clamps_at_logical_eof() {
        let mut slice_buffer = SliceBuffer::new();
        slice_buffer.write_at(0, b"hello").await.unwrap();
        let mut dst = [0xA5; 8];

        let read_len = slice_buffer.read_at(3, &mut dst).await.unwrap();

        assert_eq!(read_len, 2);
        assert_eq!(&dst[..2], b"lo");
        assert_eq!(&dst[2..], &[0xA5; 6]);
    }

    #[tokio::test]
    async fn rejects_writes_outside_the_chunk() {
        let mut slice_buffer = SliceBuffer::new();

        assert!(slice_buffer.write_at(CHUNK_SIZE, b"x").await.is_err());
        assert!(slice_buffer.write_at(usize::MAX, b"x").await.is_err());
    }

    #[tokio::test]
    async fn failed_stage_keeps_buffered_pages_retryable() {
        let tempdir = tempfile::tempdir().unwrap();
        let stage_dir = tempdir.path().join("stage");
        let cache = Arc::new(
            crate::cache::file_cache::FileCache::new(
                crate::cache::file_cache::Config {
                    stage_cache_dir: stage_dir.clone(),
                    max_stage_size:  ReadableSize((BLOCK_SIZE * 2) as u64),
                    cache_ttl:       Duration::from_secs(3600),
                },
                new_memory_object_store(),
            )
            .unwrap(),
        );
        std::fs::remove_dir_all(&stage_dir).unwrap();
        std::fs::write(&stage_dir, b"prevent local stage writes").unwrap();

        let data = b"buffered bytes must survive a failed stage";
        let mut slice_buffer = SliceBuffer::new();
        slice_buffer.write_at(0, data).await.unwrap();
        let page_count = slice_buffer.total_page_cnt;

        assert!(
            slice_buffer
                .stage(0xABCD_EF03, BLOCK_SIZE, cache.clone())
                .await
                .is_err()
        );
        assert_eq!(slice_buffer.flushed_length(), 0);
        assert_eq!(slice_buffer.total_page_cnt, page_count);
        let mut actual = vec![0; data.len()];
        slice_buffer.read_at(0, &mut actual).await.unwrap();
        assert_eq!(actual, data);

        std::fs::remove_file(&stage_dir).unwrap();
        std::fs::create_dir(&stage_dir).unwrap();
        assert_eq!(
            slice_buffer
                .stage(0xABCD_EF03, BLOCK_SIZE, cache)
                .await
                .unwrap(),
            page_count
        );
        assert_eq!(slice_buffer.flushed_length(), data.len());
        assert_eq!(slice_buffer.total_page_cnt, 0);
    }

    #[tokio::test]
    async fn staging_sparse_data_advances_the_flushed_high_watermark() {
        let tempdir = tempfile::tempdir().unwrap();
        let cache = Arc::new(
            crate::cache::file_cache::FileCache::new(
                crate::cache::file_cache::Config {
                    stage_cache_dir: tempdir.path().join("stage"),
                    max_stage_size:  ReadableSize((BLOCK_SIZE * 2) as u64),
                    cache_ttl:       Duration::from_secs(3600),
                },
                new_memory_object_store(),
            )
            .unwrap(),
        );
        let mut slice_buffer = SliceBuffer::new();
        let write_offset = BLOCK_SIZE + 3;
        let data = b"sparse";
        slice_buffer.write_at(write_offset, data).await.unwrap();

        slice_buffer.flush_v2(0xABCD_EF06, cache).await.unwrap();

        assert_eq!(slice_buffer.flushed_length(), write_offset + data.len());
        assert!(slice_buffer.write_at(write_offset, data).await.is_err());
    }

    #[tokio::test]
    async fn failed_direct_flush_keeps_buffered_pages_retryable() {
        let tempdir = tempfile::tempdir().unwrap();
        let remote_dir = tempdir.path().join("remote");
        let object_storage = new_local_object_store(&remote_dir).unwrap();
        std::fs::remove_dir_all(&remote_dir).unwrap();
        std::fs::write(&remote_dir, b"prevent remote writes").unwrap();

        let data = b"buffered bytes must survive a failed direct flush";
        let mut slice_buffer = SliceBuffer::new();
        slice_buffer.write_at(0, data).await.unwrap();
        let page_count = slice_buffer.total_page_cnt;
        let key_gen = |_: BlockIndex, _: BlockSize| "block".to_string();

        assert!(
            slice_buffer
                .flush(key_gen, object_storage.clone())
                .await
                .is_err()
        );
        assert_eq!(slice_buffer.flushed_length(), 0);
        assert_eq!(slice_buffer.total_page_cnt, page_count);
        let mut actual = vec![0; data.len()];
        slice_buffer.read_at(0, &mut actual).await.unwrap();
        assert_eq!(actual, data);

        std::fs::remove_file(&remote_dir).unwrap();
        std::fs::create_dir(&remote_dir).unwrap();
        assert_eq!(
            slice_buffer
                .flush(key_gen, object_storage.clone())
                .await
                .unwrap(),
            page_count
        );
        assert_eq!(slice_buffer.flushed_length(), data.len());
        assert_eq!(slice_buffer.total_page_cnt, 0);
        assert_eq!(
            object_storage
                .get(&ObjectStoragePath::parse("block").unwrap())
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            data
        );
    }

    #[tokio::test]
    async fn flushing_an_empty_buffer_is_a_noop() {
        let mut slice_buffer = SliceBuffer::new();
        let object_storage = new_memory_object_store();
        let key_gen = |_: BlockIndex, _: BlockSize| "unused".to_string();

        assert_eq!(
            slice_buffer.flush(key_gen, object_storage).await.unwrap(),
            0
        );
        assert_eq!(slice_buffer.flushed_length(), 0);
    }

    #[tokio::test]
    async fn flush() {
        install_fmt_log();

        let mut slice_buffer = SliceBuffer::new();
        let data = b"hello".as_slice();

        let write_len = slice_buffer.write_at(0, data).await.unwrap();
        assert_eq!(write_len, data.len());
        assert_eq!(slice_buffer.length, data.len());

        let my_id = kiseki_utils::random_id();
        let key_gen = |block_idx: BlockIndex, block_size: BlockSize| {
            let key = format!(
                "{:08X}_{:08X}_{:08X}_{:08X}_{:08X}",
                // we can overwrite a slice, so we need to avoid the conflict.
                my_id / 1000 / 1000,
                my_id / 1000,
                my_id,
                block_idx,
                block_size
            );
            info!("generate key: {}", &key);
            key
        };

        let object_sto = new_memory_object_store();

        slice_buffer
            .flush_bulk(key_gen, object_sto.clone())
            .await
            .unwrap();
        assert_eq!(slice_buffer.flushed_length, 0); // we have nothing to flush

        let write_len = slice_buffer
            .write_at(0, vec![1u8; BLOCK_SIZE].as_slice())
            .await
            .unwrap();
        assert_eq!(write_len, BLOCK_SIZE);
        assert_eq!(slice_buffer.length, BLOCK_SIZE);
        assert_eq!(slice_buffer.total_page_cnt, BLOCK_SIZE / PAGE_SIZE);

        let released_page_cnt = slice_buffer
            .flush_bulk(key_gen, object_sto.clone())
            .await
            .unwrap();
        assert_eq!(released_page_cnt, BLOCK_SIZE / PAGE_SIZE); // we should flush all the pages.
        assert_eq!(slice_buffer.total_page_cnt, 0); // we should flush all the pages.

        // we cannot write at the flushed block ever again.
        assert!(slice_buffer.write_at(0, b"hello".as_slice()).await.is_err()); // we cannot write at the flushed block ever again.
        // we should be able to write the next block
        let write_len = slice_buffer
            .write_at(BLOCK_SIZE, vec![1u8; BLOCK_SIZE].as_slice())
            .await
            .unwrap();
        assert_eq!(write_len, BLOCK_SIZE);
        assert_eq!(slice_buffer.length, BLOCK_SIZE * 2);
        assert_eq!(slice_buffer.total_page_cnt, BLOCK_SIZE / PAGE_SIZE);

        let released_page_cnt = slice_buffer
            .flush(key_gen, object_sto.clone())
            .await
            .unwrap(); // try to flush all
        assert_eq!(released_page_cnt, BLOCK_SIZE / PAGE_SIZE);
        assert_eq!(slice_buffer.flushed_length, BLOCK_SIZE * 2);
        assert_eq!(slice_buffer.total_page_cnt, 0);

        let write_len = slice_buffer
            .write_at(BLOCK_SIZE * 2 + 3, vec![1u8; BLOCK_SIZE].as_slice())
            .await
            .unwrap();
        assert_eq!(write_len, BLOCK_SIZE);
        assert_eq!(slice_buffer.length, BLOCK_SIZE * 3 + 3);
        assert_eq!(slice_buffer.total_page_cnt, BLOCK_SIZE / PAGE_SIZE + 1);
        let released_page_cnt = slice_buffer
            .flush(key_gen, object_sto.clone())
            .await
            .unwrap(); // try to flush all
        assert_eq!(released_page_cnt, BLOCK_SIZE / PAGE_SIZE + 1);
        assert_eq!(slice_buffer.flushed_length, BLOCK_SIZE * 3 + 3);
        assert_eq!(slice_buffer.total_page_cnt, 0);

        // we can flush it again, but we have nothing to flush.
        let released_page_cnt = slice_buffer
            .flush(key_gen, object_sto.clone())
            .await
            .unwrap(); // try to flush all
        assert_eq!(released_page_cnt, 0);
        assert_eq!(slice_buffer.flushed_length, BLOCK_SIZE * 3 + 3);
        assert_eq!(slice_buffer.total_page_cnt, 0);

        let mut lister = object_sto.list(None);
        while let Some(entry) = lister.next().await.transpose().unwrap() {
            info!("entry: {:?}", entry.location);
        }
    }

    #[tokio::test]
    async fn basic_rw() {
        install_fmt_log();
        let my_id = kiseki_utils::random_id();
        let key_gen = |block_idx: BlockIndex, block_size: BlockSize| {
            let key = format!(
                "{:08}_{:08}_{:08}_{:08}_{:08}",
                // we can overwrite a slice, so we need to avoid the conflict.
                my_id / 1000 / 1000,
                my_id / 1000,
                my_id,
                block_idx,
                block_size
            );
            info!("generate key: {}", &key);
            key
        };

        let object_sto = new_memory_object_store();

        let mut slice_buffer = SliceBuffer::new();
        let data = b"hello world".as_slice();

        let write_len = slice_buffer.write_at(0, data).await.unwrap();
        assert_eq!(write_len, data.len());
        assert_eq!(slice_buffer.length, data.len());

        let write_len = slice_buffer.write_at(BLOCK_SIZE - 3, data).await.unwrap();
        assert_eq!(write_len, data.len());
        assert_eq!(slice_buffer.length, BLOCK_SIZE - 3 + data.len());

        slice_buffer
            .flush(key_gen, object_sto.clone())
            .await
            .unwrap();

        let mut dst = vec![0u8; data.len()];
        let read_len = read_slice_from_object_storage(
            key_gen,
            object_sto.clone(),
            slice_buffer.length,
            0,
            dst.as_mut_slice(),
        )
        .await
        .unwrap();
        assert_eq!(read_len, data.len());
        assert_eq!(dst, data);

        let mut dst = vec![0u8; data.len()];
        let read_len = read_slice_from_object_storage(
            key_gen,
            object_sto.clone(),
            slice_buffer.length,
            BLOCK_SIZE - 3,
            dst.as_mut_slice(),
        )
        .await
        .unwrap();
        assert_eq!(read_len, data.len());
        assert_eq!(dst, data);
    }

    #[tokio::test]
    async fn parallel_object_read_handles_three_unaligned_blocks() {
        let object_storage = new_memory_object_store();
        let final_block_len = 17;
        for (block_idx, (block_len, value)) in [
            (BLOCK_SIZE, b'a'),
            (BLOCK_SIZE, b'b'),
            (final_block_len, b'c'),
        ]
        .into_iter()
        .enumerate()
        {
            let path = ObjectStoragePath::parse(format!("block-{block_idx}")).unwrap();
            let mut writer = object_storage.writer(&path);
            writer.write_all(&vec![value; block_len]).await.unwrap();
            writer.shutdown().await.unwrap();
        }

        let length = BLOCK_SIZE * 2 + final_block_len;
        let offset = BLOCK_SIZE - 3;
        let mut dst = vec![0; BLOCK_SIZE + 20];
        let read_len = read_slice_from_object_storage(
            |block_idx, _| format!("block-{block_idx}"),
            object_storage,
            length,
            offset,
            &mut dst,
        )
        .await
        .unwrap();

        assert_eq!(read_len, dst.len());
        assert_eq!(&dst[..3], &[b'a'; 3]);
        assert!(dst[3..3 + BLOCK_SIZE].iter().all(|byte| *byte == b'b'));
        assert_eq!(&dst[3 + BLOCK_SIZE..], &[b'c'; 17]);
    }

    #[tokio::test]
    async fn object_read_handles_eof_and_rejects_short_blocks() {
        let object_storage = new_memory_object_store();
        let mut dst = [0xA5; 8];
        assert_eq!(
            read_slice_from_object_storage(
                |_, _| "unused".to_string(),
                object_storage.clone(),
                4,
                4,
                &mut dst,
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(dst, [0xA5; 8]);

        let path = ObjectStoragePath::parse("short-block").unwrap();
        let mut writer = object_storage.writer(&path);
        writer.write_all(b"xy").await.unwrap();
        writer.shutdown().await.unwrap();
        assert!(
            read_slice_from_object_storage(
                |_, _| "short-block".to_string(),
                object_storage,
                4,
                0,
                &mut dst[..4],
            )
            .await
            .is_err()
        );
    }
}
