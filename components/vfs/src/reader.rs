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
    fmt::Debug,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use dashmap::DashMap;
use kiseki_common::{BLOCK_SIZE, BlockIndex, BlockSize, CHUNK_SIZE, ChunkIndex, FH};
use kiseki_storage::cache::{file_cache::FileCacheRef, mem_cache::MemCacheRef};
use kiseki_types::{
    ino::Ino,
    slice::{EMPTY_SLICE_ID, OverlookedSlicesRef, Slice, SliceID, SliceKey},
};
use kiseki_utils::readable_size::ReadableSize;
use rangemap::RangeMap;
use tracing::{debug, instrument, warn};

use crate::{
    data_manager::DataManager,
    err::{InvalidRangeSnafu, ObjectBlockNotFoundSnafu, Result, UnexpectedLengthSnafu},
};

impl DataManager {
    /// [`Self::open_file_reader`] creates a [FileReader] for the given [Ino]
    /// and [FH] if not exists.
    pub(crate) async fn open_file_reader(
        self: &Arc<Self>,
        inode: Ino,
        fh: FH,
        length: usize,
    ) -> Arc<FileReader> {
        // Release the outer lock before acquiring the inner lock to avoid
        // deadlock.
        let inner_map = {
            let outer_read_guard = self.file_readers.read().await;
            match outer_read_guard.get(&inode) {
                Some(inner) => inner.clone(),
                None => {
                    drop(outer_read_guard);
                    let mut out_write_guard = self.file_readers.write().await;
                    out_write_guard.entry(inode).or_default().clone()
                }
            }
        };

        // check if exists the file reader for the specified FH.
        let inner_read_guard = inner_map.read().await;
        if let Some(fr) = inner_read_guard.get(&fh) {
            return fr.clone();
        }
        // not found, we have to create one.
        drop(inner_read_guard);

        // use write lock
        let mut inner_write_guard = inner_map.write().await;
        // check again in case of someone has created the file reader already.
        if let Some(fr) = inner_write_guard.get(&fh) {
            return fr.clone();
        }
        // no one create the file reader for real, we have to create it.
        let fr = Arc::new(FileReader {
            data_engine: Arc::downgrade(self),
            ino: inode,
            fh,
            length: AtomicUsize::new(length),
            file_cache: self.file_cache.clone(),
            mem_cache: self.mem_cache.clone(),
            chunks: Default::default(),
            closing: Default::default(),
        });
        inner_write_guard.insert(fh, fr.clone());
        fr
    }

    /// [`Self::truncate_reader`] is called when the file length changes.
    /// Invoke this method to truncate the file reader's length.
    pub(crate) async fn truncate_reader(self: &Arc<Self>, inode: Ino, length: u64) {
        // It's possible that the [FileReader] doesn't exist, even if we create
        // the FileReader in the [HandleTable::new_file_handle].
        // Since there are some other operation that can modify the file
        // length, such as [set_attr], etc. Especially when we haven't
        // open the file handle yet.
        let out_read_guard = self.file_readers.read().await;
        if let Some(inner_map) = out_read_guard.get(&inode) {
            let inner_map = inner_map.clone();
            // drop the outer lock before we try to acquire the inner lock.
            drop(out_read_guard);

            // acquire the inner lock.
            let inner_read_guard = inner_map.read().await;
            for fr in inner_read_guard.values() {
                // TODO: review me, should we use compare and swap?
                fr.length.store(length as usize, Ordering::Release);
            }
        } else {
            warn!("not found any file reader under: {inode:?}");
        }
    }
}

/// A [FileReader] is used for read content from a file.
/// Each [FileReader] is held by a FileHandle: Ino+Fh.
pub(crate) struct FileReader {
    data_engine: Weak<DataManager>,
    // The file inode.
    ino:         Ino,
    fh:          FH,
    // The max file read length, it was set when we crate the file handle.
    length:      AtomicUsize,
    // the write back cache.
    // Cache integration on the read path is pending; not read yet.
    #[allow(dead_code)]
    file_cache:  FileCacheRef,
    // the read-only cache.
    #[allow(dead_code)]
    mem_cache:   MemCacheRef,
    // A file be divided into multiple chunks,
    // each chunk is composed by multiple slices.
    // This map is used to store latest slices that compose the chunk.
    // TODO: get rid of DashMap
    #[allow(dead_code)]
    chunks:      DashMap<ChunkIndex, OverlookedSlicesRef>,
    // The file is closing or not.
    closing:     AtomicBool,
}

impl FileReader {
    /// [`Self::read`] reads file content from the specified `offset`.
    pub(crate) async fn read(self: &Arc<Self>, offset: usize, dst: &mut [u8]) -> Result<usize> {
        let expected_read_len = dst.len();
        let length = self.length.load(Ordering::Acquire);
        // read offset should not exceed the file length.
        if offset >= length || expected_read_len == 0 {
            return Ok(0);
        }

        // cal the real read length.
        let expected_read_len = min(expected_read_len, length - offset);

        debug!(
            "{:?}, actual can read length: {}",
            self.ino,
            ReadableSize(expected_read_len as u64)
        );

        // get the slice inside the chunk.
        let engine = self
            .data_engine
            .upgrade()
            .expect("engine should not be dropped");
        let meta_engine = engine.meta_engine.clone();
        let chunk_size = engine.chunk_size;
        let start_chunk_idx = offset / chunk_size;
        let end_chunk_idx = (offset + expected_read_len - 1) / chunk_size;

        let mut total_read_len = 0;
        let mut left_to_read = expected_read_len;
        for chunk_idx in start_chunk_idx..=end_chunk_idx {
            // offset to current chunk.
            let mut chunk_pos = (offset + total_read_len) % chunk_size;
            // max can read in current chunk.
            let max_can_read = min(chunk_size - chunk_pos, left_to_read);
            // then we get the range to read in current chunk.
            let current_read_range = chunk_pos..chunk_pos + max_can_read;
            // according to current chunk idx, we can get the slices.
            let raw_slices = meta_engine.read_slice(self.ino, chunk_idx).await?;
            let Some(raw_slices) = raw_slices else {
                debug!("no slice in chunk: {chunk_idx:?}");
                return Ok(0);
            };

            for x in &raw_slices.0 {
                debug!("find raw-slice in chunk: {chunk_idx:?}, slice: {x:?}");
            }

            // make a virtual slice map to record the slice and hole.
            let mut virtual_slice_map = RangeMap::new();
            {
                // let overlap = raw_slices.overlook();
                // let overlap_slices = overlap.iter().collect_vec();
                // println!("overlap_slices: {:?}", overlap_slices);
            }

            let range_map = raw_slices.overlook();
            for x in range_map.gaps(&current_read_range) {
                debug!("current range: {current_read_range:?}, find gap: {x:?}");
                virtual_slice_map.insert(x, VirtualSlice::Hole);
            }
            for (r, s) in range_map.overlapping(&current_read_range) {
                let new_r =
                    max(r.start, current_read_range.start)..min(r.end, current_read_range.end);
                debug!(
                    "find overlapping slice in chunk: {:?}, range: {:?}, new_range: {:?}, slice: \
                     {:?}",
                    chunk_idx, r, new_r, s
                );
                let virtual_slice = if s.get_id() == EMPTY_SLICE_ID {
                    VirtualSlice::Hole
                } else {
                    VirtualSlice::Slice(s.clone())
                };
                virtual_slice_map.insert(new_r, virtual_slice);
            }
            drop(range_map);

            for (r, vs) in virtual_slice_map {
                let start = total_read_len + r.start - chunk_pos;
                let end = total_read_len + r.end - chunk_pos;
                let len = end - start;

                // assert!(start <= end, "VS: {:?}, start: {}, end: {}, expected_read_len: {}",
                // vs, ReadableSize(start as u64), ReadableSize(end as u64),
                // ReadableSize(expected_read_len as u64)); assert!(start <
                // expected_read_len, "VS: {:?}, start: {}, end: {}, expected_read_len: {}", vs,
                // ReadableSize(start as u64), ReadableSize(end as u64),
                // ReadableSize(expected_read_len as u64)); assert!(end <=
                // expected_read_len, "VS: {:?}, start: {}, end: {}, expected_read_len: {}", vs,
                // ReadableSize(start as u64), ReadableSize(end as u64),
                // ReadableSize(expected_read_len as u64));

                match vs {
                    VirtualSlice::Hole => {
                        debug!(
                            "chunk_size{}, find hole in chunk: {:?}, range: {:?}",
                            chunk_size, chunk_idx, r,
                        );
                        dst[start..end].fill(0);
                    }
                    VirtualSlice::Slice(s) => {
                        debug!(
                            "find slice in chunk: {:?}, range: {:?}, slice: {:?}, write buf \
                             [{start}, {end}]",
                            chunk_idx, r, s
                        );
                        let sid = s.get_id();
                        let source_offset =
                            s.get_underlying_offset() + r.start.saturating_sub(s.get_chunk_pos());

                        let _read_len = read_slice_from_cache(
                            sid,
                            engine.file_cache.clone(),
                            engine.mem_cache.clone(),
                            s.get_underlying_size(),
                            source_offset,
                            &mut dst[start..end],
                        )
                        .await?;

                        // let rb = engine.new_read_buffer(s.get_id(),
                        // s.get_underlying_size());
                        // rb.read_at(0, &mut
                        // dst[start..end]).context(StorageSnafu)?;
                    }
                }
                total_read_len += len;
                left_to_read -= len;
                chunk_pos += len;
            }
        }

        Ok(total_read_len)
    }

    /// When release file handle, we will try to close the associated
    /// FileReader.
    pub(crate) async fn close(self: &Arc<Self>) {
        if self
            .closing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            debug!("someone else win the close contention: {:?}", self.ino);
            return;
        }

        let engine = self
            .data_engine
            .upgrade()
            .expect("engine should not be dropped");

        let outer_guard = engine.file_readers.read().await;
        if let Some(inner_map) = outer_guard.get(&self.ino) {
            let inner_map = inner_map.clone();
            drop(outer_guard);

            let inner_read_guard = inner_map.read().await;
            if inner_read_guard.contains_key(&self.fh) {
                drop(inner_read_guard);

                let mut inner_write_guard = inner_map.write().await;
                inner_write_guard.remove(&self.fh);
            }
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum VirtualSlice {
    Hole,
    Slice(Slice),
}

#[instrument(skip_all, fields(length, offset))]
async fn read_slice_from_cache(
    slice_id: SliceID,
    file_cache: FileCacheRef,
    mem_cache: MemCacheRef,
    length: usize, // length of the slice.
    offset: usize, // read offset
    dst: &mut [u8],
) -> Result<usize> {
    if length > CHUNK_SIZE {
        return InvalidRangeSnafu {
            subject: "cached slice",
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

        let (dst_slice, rest) = remaining_dst.split_at_mut(current_block_to_read_len);
        remaining_dst = rest;

        total_read_len += current_block_to_read_len;

        let file_cache_clone = file_cache.clone();
        let mem_cache_clone = mem_cache.clone();
        reads.push(async move {
            let key = SliceKey::new(slice_id, block_idx, obj_block_size);
            // 1. first of all, try to get block from file-cache
            if let Some(block_reader) = file_cache_clone
                .get_range(&key, block_offset, current_block_to_read_len)
                .await?
            {
                if block_reader.len() != current_block_to_read_len {
                    return UnexpectedLengthSnafu {
                        subject:  "file cache block",
                        expected: current_block_to_read_len,
                        actual:   block_reader.len(),
                    }
                    .fail();
                }
                dst_slice.copy_from_slice(&block_reader);
                return Ok(current_block_to_read_len);
            }

            // 2. try to get block from mem-cache
            if let Some(block) = mem_cache_clone.get(&key).await? {
                let Some(block_end) = block_offset.checked_add(current_block_to_read_len) else {
                    return InvalidRangeSnafu {
                        subject: "memory cache block",
                        offset:  block_offset,
                        length:  current_block_to_read_len,
                        bound:   block.len(),
                    }
                    .fail();
                };
                let Some(source) = block.get(block_offset..block_end) else {
                    return InvalidRangeSnafu {
                        subject: "memory cache block",
                        offset:  block_offset,
                        length:  current_block_to_read_len,
                        bound:   block.len(),
                    }
                    .fail();
                };
                dst_slice.copy_from_slice(source);
                return Ok(current_block_to_read_len);
            }

            // 3. report not found error
            ObjectBlockNotFoundSnafu { key }.fail()?
        });
    }

    let actual_read_cnt = futures::future::try_join_all(reads)
        .await?
        .into_iter()
        .sum::<usize>();
    if actual_read_cnt != total_read_len {
        return UnexpectedLengthSnafu {
            subject:  "cached slice read",
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

#[cfg(test)]
mod tests {
    use kiseki_meta::{MetaConfig, context::FuseContext};
    use kiseki_storage::cache::{file_cache::FileCache, mem_cache::MemCache};
    use kiseki_types::{ino::ROOT_INO, setting::Format};
    use kiseki_utils::{
        logger::install_fmt_log, object_storage::new_memory_object_store,
        readable_size::ReadableSize,
    };
    use tokio::io::AsyncWriteExt;

    use super::*;

    #[test]
    fn get_from_range_map() {
        let mut range_map = rangemap::RangeMap::new();
        range_map.insert(0..10, 1);
        range_map.insert(10..12, 2);
        range_map.iter().for_each(|(r, v)| {
            println!("{r:?} -> {v:?}");
        });
    }

    #[test]
    fn make_virtual_map() {
        let chunk_size = 1024usize;
        let mut rm = rangemap::RangeMap::new();
        rm.insert(0..3, Slice::new_owned(0, 0, 3));
        rm.insert(12..15, Slice::new_owned(12, 1, 3));
        rm.insert(
            chunk_size - 3..chunk_size,
            Slice::new_owned(chunk_size - 3, 2, 3),
        );
        rm.insert(
            chunk_size..chunk_size + 8,
            Slice::new_owned(chunk_size, 3, 8),
        );

        let mut virtual_slice_map = RangeMap::new();
        let read_range = chunk_size - 4..chunk_size + 8;
        for x in rm.gaps(&read_range) {
            if x.end < read_range.start {
                continue;
            }
            virtual_slice_map.insert(x, VirtualSlice::Hole);
        }
        for (r, s) in rm.overlapping(&read_range) {
            virtual_slice_map.insert(r.clone(), VirtualSlice::Slice(s.clone()));
        }
        for (r, vs) in virtual_slice_map {
            match vs {
                VirtualSlice::Hole => {
                    println!("find hole in range: {r:?}");
                }
                VirtualSlice::Slice(s) => {
                    println!("find slice in range: {r:?}, slice: {s:?}");
                }
            }
        }
    }

    #[tokio::test]
    async fn cached_read_handles_three_unaligned_blocks() {
        let remote = new_memory_object_store();
        let slice_id = 0xABCD_EF10;
        let final_block_len = 17;
        for (block_idx, (block_len, value)) in [
            (BLOCK_SIZE, b'a'),
            (BLOCK_SIZE, b'b'),
            (final_block_len, b'c'),
        ]
        .into_iter()
        .enumerate()
        {
            let key = SliceKey::new(slice_id, block_idx, block_len);
            let mut writer = remote.writer(&key.make_object_storage_path());
            writer.write_all(&vec![value; block_len]).await.unwrap();
            writer.shutdown().await.unwrap();
        }

        let stage_dir = tempfile::tempdir().unwrap();
        let file_cache = Arc::new(
            FileCache::new(
                kiseki_storage::cache::file_cache::Config {
                    stage_cache_dir: stage_dir.path().to_path_buf(),
                    max_stage_size: ReadableSize::mb(16),
                    ..Default::default()
                },
                remote.clone(),
                Arc::new(kiseki_storage::task_registry::MountTaskRegistry::new()),
            )
            .unwrap(),
        );
        let mem_cache = Arc::new(MemCache::new(
            kiseki_storage::cache::mem_cache::Config {
                capacity: ReadableSize::mb(16),
            },
            remote,
        ));
        let length = BLOCK_SIZE * 2 + final_block_len;
        let offset = BLOCK_SIZE - 3;
        let mut dst = vec![0; BLOCK_SIZE + 20];

        let read_len =
            read_slice_from_cache(slice_id, file_cache, mem_cache, length, offset, &mut dst)
                .await
                .unwrap();

        assert_eq!(read_len, dst.len());
        assert_eq!(&dst[..3], &[b'a'; 3]);
        assert!(dst[3..3 + BLOCK_SIZE].iter().all(|byte| *byte == b'b'));
        assert_eq!(&dst[3 + BLOCK_SIZE..], &[b'c'; 17]);
    }

    #[tokio::test]
    async fn cached_read_handles_eof_and_rejects_short_blocks() {
        let remote = new_memory_object_store();
        let slice_id = 0xABCD_EF11;
        let key = SliceKey::new(slice_id, 0, 4);
        let mut writer = remote.writer(&key.make_object_storage_path());
        writer.write_all(b"xy").await.unwrap();
        writer.shutdown().await.unwrap();

        let stage_dir = tempfile::tempdir().unwrap();
        let file_cache = Arc::new(
            FileCache::new(
                kiseki_storage::cache::file_cache::Config {
                    stage_cache_dir: stage_dir.path().to_path_buf(),
                    ..Default::default()
                },
                remote.clone(),
                Arc::new(kiseki_storage::task_registry::MountTaskRegistry::new()),
            )
            .unwrap(),
        );
        let mem_cache = Arc::new(MemCache::new(Default::default(), remote));
        let mut dst = [0xA5; 8];

        assert_eq!(
            read_slice_from_cache(
                slice_id,
                file_cache.clone(),
                mem_cache.clone(),
                4,
                4,
                &mut dst,
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(dst, [0xA5; 8]);
        assert!(
            read_slice_from_cache(slice_id, file_cache, mem_cache, 4, 0, &mut dst[..4],)
                .await
                .is_err()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reads_preserve_unmodified_bytes_across_overlapping_slices() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut meta_config = MetaConfig::default();
        meta_config.with_dsn(&format!("rocksdb://:{}", tempdir.path().display()));
        let format = Format::default();
        kiseki_meta::update_format(&meta_config.dsn, format.clone(), true).unwrap();
        let meta_engine = kiseki_meta::open(meta_config).unwrap();
        let (inode, _) = meta_engine
            .create(
                Arc::new(FuseContext::background()),
                ROOT_INO,
                "overlap",
                0o600,
                0,
                0,
            )
            .await
            .unwrap();
        let data_manager = Arc::new(
            DataManager::new(
                format.chunk_size,
                meta_engine,
                new_memory_object_store(),
                kiseki_storage::cache::file_cache::Config {
                    stage_cache_dir: tempdir.path().join("stage"),
                    ..Default::default()
                },
                kiseki_storage::cache::mem_cache::Config::default(),
                crate::data_manager::new_test_page_pool().await,
            )
            .unwrap(),
        );
        let writer = data_manager.open_file_writer(inode, 0).unwrap();

        for (offset, data) in [
            (0, b"0123456789".as_slice()),
            (0, b"abc".as_slice()),
            (7, b"XYZ".as_slice()),
        ] {
            writer.write(offset, data).await.unwrap();
            writer.finish().await.unwrap();
        }

        let reader = data_manager.open_file_reader(inode, 1, 10).await;
        let mut actual = [0; 10];
        assert_eq!(reader.read(0, &mut actual).await.unwrap(), actual.len());
        assert_eq!(&actual, b"abc3456XYZ");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read() {
        install_fmt_log();

        let meta_config = MetaConfig::default();
        let format = Format::default();
        kiseki_meta::update_format(&meta_config.dsn, format.clone(), true).unwrap();

        let meta_engine = kiseki_meta::open(meta_config).unwrap();
        let fuse_ctx = Arc::new(FuseContext::background());
        let (inode, _attr) = meta_engine
            .create(fuse_ctx, ROOT_INO, "a", 0o650, 0, 0)
            .await
            .unwrap();

        let stage_dir = tempfile::tempdir().unwrap();
        let data_manager = Arc::new(
            DataManager::new(
                format.chunk_size,
                meta_engine,
                new_memory_object_store(),
                kiseki_storage::cache::file_cache::Config {
                    stage_cache_dir: stage_dir.path().to_path_buf(),
                    ..Default::default()
                },
                kiseki_storage::cache::mem_cache::Config::default(),
                crate::data_manager::new_test_page_pool().await,
            )
            .unwrap(),
        );

        data_manager.open_file_writer(inode, 0).unwrap();
        let data = b"hello world" as &[u8];

        let write_len = data_manager
            .write(inode, 0, data)
            .await
            .map_err(|e| println!("{e}"))
            .unwrap();

        let fw = data_manager.find_file_writer(inode).unwrap();
        fw.finish().await.unwrap();

        let file_reader = data_manager.open_file_reader(inode, 0, write_len).await;

        let mut read_data = vec![0u8; 11];
        let read_len = file_reader.read(0, read_data.as_mut_slice()).await;
        assert!(read_len.is_ok());
        let read_len = read_len.unwrap();
        assert_eq!(read_len, 11);
        assert!(read_data.starts_with(b"hello world"));
        println!("{}", String::from_utf8_lossy(&read_data));

        let chunk_size = data_manager.chunk_size;
        let write_len = data_manager
            .write(inode, chunk_size - 3, data)
            .await
            .map_err(|e| println!("{e}"))
            .unwrap();
        assert_eq!(write_len, data.len());

        fw.finish().await.unwrap();
        let file_len = fw.get_length();
        assert_eq!(file_len, chunk_size + 8);

        let mut read_data = vec![0u8; 11];

        file_reader.close().await;

        let file_reader = data_manager.open_file_reader(inode, 0, file_len).await;
        let read_len = file_reader
            .read(chunk_size - 3, read_data.as_mut_slice())
            .await
            .unwrap();
        assert_eq!(read_len, 11);
        println!("{}", String::from_utf8_lossy(&read_data));
        assert!(read_data.starts_with(b"hello world"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_write_1_g() {
        install_fmt_log();

        let mut meta_config = MetaConfig::default();
        let tempdir = tempfile::tempdir().unwrap();
        let format = Format::default();
        let temppath = tempdir.path().to_str().unwrap();
        meta_config.dsn = format!("rocksdb://:{temppath}");
        kiseki_meta::update_format(&meta_config.dsn, format.clone(), true).unwrap();

        let meta_engine = kiseki_meta::open(meta_config).unwrap();
        let fuse_ctx = Arc::new(FuseContext::background());
        let (inode, _attr) = meta_engine
            .create(fuse_ctx, ROOT_INO, "a", 0o650, 0, 0)
            .await
            .unwrap();

        let data_manager = Arc::new(
            DataManager::new(
                format.chunk_size,
                meta_engine,
                new_memory_object_store(),
                kiseki_storage::cache::file_cache::Config {
                    stage_cache_dir: tempdir.path().join("stage"),
                    ..Default::default()
                },
                kiseki_storage::cache::mem_cache::Config::default(),
                crate::data_manager::new_test_page_pool().await,
            )
            .unwrap(),
        );

        data_manager.open_file_writer(inode, 0).unwrap();
        let step_size: usize = 4 << 20;
        let total_step: usize = 1 << (30 / step_size);
        let data = vec![1u8; step_size];

        let fw = data_manager.find_file_writer(inode).unwrap();
        for i in 0..total_step {
            let write_len = fw.write(i * step_size, &data).await.unwrap();
            assert_eq!(write_len, step_size);
        }
        fw.finish().await.unwrap();

        let file_reader = data_manager
            .open_file_reader(inode, 0, total_step * step_size)
            .await;
        let page_size: usize = 128 << 10;
        let total_read_step = 1 << (30 / page_size);
        let expect_read_content = vec![1u8; page_size];
        for i in 0..total_read_step {
            let mut read_data = vec![0u8; page_size];
            let read_len = file_reader
                .read(i * page_size, &mut read_data)
                .await
                .unwrap();
            assert_eq!(read_len, page_size);
            assert_eq!(read_data, expect_read_content);
        }
    }
}
