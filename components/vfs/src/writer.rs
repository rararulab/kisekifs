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

//! A FileWriter can have multiple ChunkWriters, and a chunk writer can have
//! multiple slice writers. ChunkWriters control how(flush full or flush a part)
//! and when to flush the data (1. have a long time no write, 2. we are running
//! out of space, 3. initiative). to the remote storage. When call flush on
//! FileWriter we should wait for all the chunk writers to finish. So each chunk
//! writer has a background committer to flush the SliceWriters to the remote
//! storage by order.

use std::{
    cmp::min,
    collections::{BTreeMap, HashMap, HashSet},
    fmt::{Debug, Display, Formatter},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicIsize, AtomicU8, AtomicU64, AtomicUsize, Ordering},
    },
};

use crossbeam::atomic::AtomicCell;
use dashmap::{DashMap, mapref::entry::Entry};
use kiseki_common::{BLOCK_SIZE, CHUNK_SIZE, ChunkIndex, cal_chunk_idx, cal_chunk_offset};
use kiseki_meta::MetaEngineRef;
use kiseki_storage::slice_buffer::SliceBuffer;
use kiseki_types::{
    ino::Ino,
    slice::{EMPTY_SLICE_ID, SliceID},
};
use kiseki_utils::readable_size::ReadableSize;
#[cfg(test)]
use libc::EBADF;
use snafu::OptionExt;
use tokio::{
    sync::{Notify, RwLock, mpsc},
    task::yield_now,
    time::Instant,
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, error, instrument};

use crate::{
    data_manager::DataManager,
    err::{Error, LibcSnafu, MountDrainingSnafu, Result},
};

impl DataManager {
    /// Creates a new [FileWriter] for the file.
    /// All file handle share the single one [FileWriter].
    pub(crate) fn open_file_writer(
        self: &Arc<Self>,
        ino: Ino,
        len: u64,
    ) -> Result<Arc<FileWriter>> {
        let fw = match self.file_writers.entry(ino) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
                let (tx, rx) = mpsc::channel(200);
                let fw = FileWriter {
                    inode:                  ino,
                    length:                 AtomicUsize::new(len as usize),
                    chunk_writers:          Default::default(),
                    slice_flush_queue:      tx,
                    total_slice_writer_cnt: Arc::new(Default::default()),
                    cancel_token:           CancellationToken::new(),
                    seq_generate:           self.id_generator.clone(),
                    ref_count:              Arc::new(Default::default()),
                    operation_lock:         Default::default(),
                    pattern:                Default::default(),
                    early_flush_threshold:  0.3,
                    flush_error:            Mutex::new(None),
                    published_slices:       Mutex::new(HashSet::new()),
                    data_manager:           Arc::downgrade(self),
                };

                let fw = Arc::new(fw);
                let fw_cloned = fw.clone();
                if !self.tasks.spawn(async move {
                    let flusher = FileWriterFlusher {
                        fw:       fw_cloned,
                        flush_rx: rx,
                    };
                    flusher.run().await;
                }) {
                    return MountDrainingSnafu.fail();
                }
                entry.insert(fw.clone());
                fw
            }
        };
        fw.ref_count.fetch_add(1, Ordering::AcqRel);
        Ok(fw)
    }

    /// Use the [FileWriter] to write data to the file.
    /// Return the number of bytes written and current file length.
    ///
    /// Only used by tests; the production write path goes through the file
    /// handle (see `KisekiVFS::write`).
    #[cfg(test)]
    pub(crate) async fn write(
        self: &Arc<Self>,
        ino: Ino,
        offset: usize,
        data: &[u8],
    ) -> Result<usize> {
        debug!("write {} bytes to ino {}", data.len(), ino);
        // Clone the `Arc<FileWriter>` out so the DashMap shard lock is released
        // before the (awaiting) write, rather than held across it.
        let fw = self
            .file_writers
            .get(&ino)
            .context(LibcSnafu { errno: EBADF })?
            .clone();
        debug!("Ino({ino}) get file write success");
        let write_len = fw.write(offset, data).await?;
        let current_len = fw.get_length();
        debug!(
            "Ino({}) write len: {}, current_len: {}",
            ino,
            ReadableSize(write_len as u64),
            ReadableSize(current_len as u64),
        );
        self.truncate_reader(ino, current_len as u64).await;
        Ok(write_len)
    }

    /// [`Self::direct_flush`] flushes the content of the specified inode to
    /// remote storage. TODO: review me.
    /// Problem: This function call the underlying flush logic without acquiring
    /// lock of `FileHandle`.
    #[instrument(skip(self), fields(ino))]
    pub(crate) async fn direct_flush(&self, ino: Ino) -> Result<()> {
        if let Some(fw) = self.file_writers.get(&ino) {
            fw.finish().in_current_span().await?;
            debug!(
                "{} file writer exists and flush success, current_len: {}",
                ino,
                ReadableSize(fw.get_length() as u64)
            );
        } else {
            debug!("{ino} file writer not exists, don't need to flush");
        }
        Ok(())
    }

    pub(crate) async fn flush_all_writers_local(&self) -> (usize, usize, Vec<String>) {
        let writers = self
            .file_writers
            .iter()
            .map(|entry| entry.value().clone())
            .collect::<Vec<_>>();
        let total = writers.len();
        let mut flushed = 0;
        let mut errors = Vec::new();
        for writer in writers {
            match writer.finish().await {
                Ok(()) => flushed += 1,
                Err(error) if errors.len() < 8 => {
                    errors.push(format!("writer local flush failed: {error}"));
                }
                Err(_) => {}
            }
        }
        (total, flushed, errors)
    }

    pub(crate) fn cancel_all_writers(&self) {
        for writer in self.file_writers.iter() {
            writer.cancel_token.cancel();
        }
    }
}

type InternalSliceSeq = u64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Durability {
    Local,
    Remote,
}

#[derive(Debug, Default)]
struct WriterPattern {
    score:                  AtomicIsize,
    last_write_stop_offset: AtomicUsize,
}

impl WriterPattern {
    const MAX_RANDOM_SCORE: isize = -5;
    const MAX_SEQ_SCORE: isize = 20;

    fn monitor_write_at(&self, offset: usize, size: usize) {
        let last_offset = self
            .last_write_stop_offset
            .swap(offset + size, Ordering::AcqRel);
        let score = self.score.load(Ordering::Acquire);
        if last_offset == offset {
            // seq
            if score < Self::MAX_SEQ_SCORE {
                // add weight on the seq side.
                self.score.fetch_add(1, Ordering::AcqRel);
            }
        } else if score > Self::MAX_RANDOM_SCORE {
            // add weight on the random side.
            self.score.fetch_add(-1, Ordering::AcqRel);
        }
    }

    fn is_seq(&self) -> bool { self.score.load(Ordering::Acquire) >= 0 }
}

pub(crate) type FileWritersRef = Arc<DashMap<Ino, Arc<FileWriter>>>;

/// FileWriter is the entry point for writing data to a file.
pub struct FileWriter {
    pub inode: Ino,

    // runtime
    // the length of current file.
    length:            AtomicUsize,
    // buffer writers.
    chunk_writers:     RwLock<HashMap<ChunkIndex, Arc<ChunkWriter>>>,
    // we may need to wait on the flush queue to flush the data to the remote storage.
    slice_flush_queue: mpsc::Sender<FlushReq>,

    cancel_token:           CancellationToken,
    seq_generate:           Arc<sonyflake::Sonyflake>,
    // tracking how many operations is using the file writer.
    // 1. when we open, we increase the counter.
    // 2. when we close the file, decrease the counter.
    ref_count:              Arc<AtomicUsize>,
    operation_lock:         RwLock<()>,
    total_slice_writer_cnt: Arc<AtomicUsize>,

    // random write early flush
    // when we reach the threshold, we should flush some flush to the remote storage.
    pattern:               Arc<WriterPattern>,
    // when should we flush the buffer in advance, according to current buffer pool free ratio.
    // reserved for the not-yet-wired early-flush heuristic.
    #[allow(dead_code)]
    early_flush_threshold: f64,

    // The first fatal error produced by a background flush task.
    // Background tasks have no Result channel back to the caller, so they
    // record the error here and it is surfaced (and cleared) by the next
    // write/flush call — the same way the kernel reports async writeback
    // errors on the next write/fsync.
    flush_error: Mutex<Option<Error>>,

    // Slice ids whose metadata is published and whose local stage remains the
    // durability owner until an explicit fsync confirms remote storage.
    published_slices: Mutex<HashSet<SliceID>>,

    // dependencies
    // the underlying object storage.
    data_manager: Weak<DataManager>,
}

impl FileWriter {
    pub fn get_length(self: &Arc<Self>) -> usize { self.length.load(Ordering::Acquire) }

    pub(crate) fn set_length(&self, length: usize) { self.length.store(length, Ordering::Release) }

    /// Record a fatal error from a background flush task.
    /// Only the first error is kept (later ones are logged and dropped),
    /// it will be returned by the next [FileWriter::write] or
    /// [FileWriter::finish] call.
    fn record_flush_error(&self, err: Error) {
        error!("Ino({}) background flush failed: {}", self.inode, err);
        let mut guard = self
            .flush_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if guard.is_none() {
            *guard = Some(err);
        }
    }

    /// Take the recorded background flush error, if any.
    fn take_flush_error(&self) -> Option<Error> {
        self.flush_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }

    fn has_flush_error(&self) -> bool {
        self.flush_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some()
    }

    fn record_published_slice(&self, slice_id: SliceID) {
        self.published_slices
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(slice_id);
    }

    /// Write data to the file.
    ///
    /// 1. calculate the location
    /// 2. write to the location
    /// 3. get the written length
    /// 4. check if we buffered enough data to flush:
    ///    1. full; we remove the ref from map;
    ///    2. a whole block has been buffered, we keep the ref in the map.
    ///
    /// Summary:
    ///     We can write while flushing
    ///     1. for flushing block, we can reuse that SliceWriter.
    ///     2. for flushing whole chunk and manually flushing, we will use a new
    ///        SliceWriter. But we lock the map when we make flushReq.
    #[instrument(skip_all, fields(self.inode, offset, write_len = ReadableSize( data.len() as u64).to_string()))]
    pub async fn write(self: &Arc<Self>, offset: usize, data: &[u8]) -> Result<usize> {
        let expected_write_len = data.len();
        if expected_write_len == 0 {
            return Ok(0);
        }

        // Explicit flushes take the write side so they cannot observe a slice
        // halfway through its Writing -> Dirty transition. Independent writes
        // retain read-side concurrency.
        let _operation_guard = self.operation_lock.read().await;

        // surface any error recorded by a background flush task before
        // accepting new data.
        if let Some(e) = self.take_flush_error() {
            return Err(e);
        }

        self.pattern.monitor_write_at(offset, expected_write_len);
        loop {
            let cnt = self.total_slice_writer_cnt.load(Ordering::Acquire);
            if cnt < 1000 {
                break;
            }
            // we have too many slice writers, we should wait for some to finish.
            yield_now().await;
        }

        // 1. find write location.
        let start = Instant::now();
        debug!(
            "Ino({}) try to find slice writer {:?}",
            self.inode,
            start.elapsed()
        );
        let slice_writers = self
            .find_writable_slice_writer(offset, expected_write_len)
            .in_current_span()
            .await;
        debug!(
            "Ino({}) find slice writer success {:?}",
            self.inode,
            start.elapsed()
        );

        let mut write_len = 0;
        for (sw, state, l) in &slice_writers {
            let data = &data[l.buf_start_at..l.buf_start_at + l.need_write_len];
            let n = sw
                .write_at(*state, l.chunk_offset - sw.offset_of_chunk, data)
                .await?;
            write_len += n;
            let sw = sw.clone();

            if let Some(req) = sw.make_flush_req(self.pattern.is_seq(), false).await
                && let Err(e) = self.slice_flush_queue.send(req).await
            {
                // the flusher task is gone (writer is being closed);
                // fail this write with EIO instead of wedging the mount.
                error!("Ino({}) failed to send flush request: {e}", self.inode);
                return LibcSnafu { errno: libc::EIO }.fail();
            }
        }

        // 4. update the new file length
        let mut old_len = self.length.load(Ordering::Acquire);
        // do compare and exchange
        let may_new_len = offset + write_len;
        if may_new_len > old_len {
            // give up if someone's length is larger.
            loop {
                debug!(
                    "Ino({}) try to update file length from {} to {}",
                    self.inode,
                    ReadableSize(old_len as u64),
                    ReadableSize(may_new_len as u64)
                );
                match self.length.compare_exchange(
                    old_len,
                    may_new_len,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(new_old_len) => {
                        if new_old_len >= may_new_len {
                            break;
                        }
                        // use the new old len to try CAS.
                        old_len = new_old_len;
                    }
                }
            }
        }

        Ok(write_len)
    }

    #[instrument(skip_all, fields(self.inode))]
    async fn find_writable_slice_writer(
        self: &Arc<Self>,
        offset: usize,
        expected_write_len: usize,
    ) -> Vec<(Arc<SliceWriter>, SliceWriterState, ChunkWriteCtx)> {
        let mut sws = Vec::with_capacity(2);
        for l in locate_chunk(CHUNK_SIZE, offset, expected_write_len) {
            let mut write_guard = self.chunk_writers.write().await;
            let cw = write_guard
                .entry(l.chunk_idx)
                .or_insert(Arc::new(ChunkWriter {
                    inode:             self.inode,
                    chunk_index:       l.chunk_idx,
                    slice_writers:     Default::default(),
                    fw:                Arc::downgrade(self),
                    total_slice_count: self.total_slice_writer_cnt.clone(),
                    slice_done_notify: Default::default(),
                }))
                .clone();
            drop(write_guard);

            let (sw, state) = cw.find_writable_slice_writer(&l).await;
            sws.push((sw, state, l));
        }
        sws
    }

    /// Flush the data to the remote storage.
    #[instrument(skip(self), fields(self.inode))]
    pub async fn finish(self: &Arc<Self>) -> Result<()> { self.flush_to(Durability::Local).await }

    #[instrument(skip(self), fields(self.inode))]
    pub async fn sync_remote(self: &Arc<Self>) -> Result<()> {
        self.flush_to(Durability::Remote).await
    }

    async fn flush_to(self: &Arc<Self>, durability: Durability) -> Result<()> {
        // Flushes are serialized, and start only after active writes have
        // completed their SliceWriter state transition.
        let _operation_guard = self.operation_lock.write().await;

        self.finish_local().await?;
        if durability == Durability::Remote {
            self.flush_published_slices().await?;
        }
        Ok(())
    }

    async fn finish_local(self: &Arc<Self>) -> Result<()> {
        // A previous attempt left the writer state intact for retry. Report
        // that failure once; the next finish call will retry the same slices.
        if let Some(e) = self.take_flush_error() {
            return Err(e);
        }

        let read_guard = self.chunk_writers.read().await;
        debug!(
            "Ino({}) flush is waiting for all chunk writer finish, cnt: {}",
            self.inode,
            read_guard.len()
        );
        if read_guard.is_empty() {
            return Ok(());
        }
        let finishes = read_guard
            .values()
            .map(|cw| {
                let cw = cw.clone();
                async move {
                    cw.finish().await;
                }
            })
            .collect::<Vec<_>>();
        drop(read_guard);

        futures::future::join_all(finishes).await;

        // surface errors recorded by the background flush tasks that this
        // flush triggered, fsync-style. Keep the chunk writers so a later
        // finish can retry the exact same slice id and metadata publication.
        if let Some(e) = self.take_flush_error() {
            return Err(e);
        }

        self.chunk_writers.write().await.clear();
        Ok(())
    }

    async fn flush_published_slices(self: &Arc<Self>) -> Result<()> {
        let slice_ids = self
            .published_slices
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .copied()
            .collect::<Vec<_>>();
        let file_cache = self
            .data_manager
            .upgrade()
            .context(LibcSnafu { errno: libc::EIO })?
            .file_cache
            .clone();
        for slice_id in slice_ids {
            file_cache.flush_slice(slice_id).await?;
            self.published_slices
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&slice_id);
        }
        Ok(())
    }

    /// Close won't really close the file writer, it tries to decrease the ref
    /// count first.
    #[instrument(skip(self), fields(self.inode))]
    pub(crate) async fn close(self: &Arc<Self>) {
        if let Err(e) = self.finish().await {
            error!("failed to flush in close {e}");
        }

        let Some(dm) = self.data_manager.upgrade() else {
            error!(
                "Ino({}) cannot close after data manager dropped",
                self.inode
            );
            return;
        };
        let Entry::Occupied(entry) = dm.file_writers.entry(self.inode) else {
            error!("Ino({}) writer is missing while closing", self.inode);
            return;
        };
        if !Arc::ptr_eq(entry.get(), self) {
            error!(
                "Ino({}) writer was replaced before its handle closed",
                self.inode
            );
            return;
        }

        // Opening and last-close retirement both hold the same DashMap shard
        // write lock. A new handle therefore either increments this writer
        // before the last-close decision, or observes a vacant entry and gets
        // a new writer after retirement.
        let references = self.ref_count.load(Ordering::Acquire);
        if references == 0 {
            error!("Ino({}) writer reference count underflow", self.inode);
            return;
        }
        if self.ref_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.cancel_token.cancel();
            entry.remove();
        }
    }

    #[cfg(test)]
    pub(crate) fn get_reference_count(self: &Arc<Self>) -> usize {
        self.ref_count.load(Ordering::Acquire)
    }
}

enum FlushReq {
    FlushBulk {
        sw:     Arc<SliceWriter>,
        offset: usize,
    },
    FlushFull(Arc<SliceWriter>),
    CommitIdle(Arc<SliceWriter>),
}

/// FileWriterFlusher flushes the data to the remote storage periodically.
struct FileWriterFlusher {
    fw:       Arc<FileWriter>,
    flush_rx: mpsc::Receiver<FlushReq>,
}

impl FileWriterFlusher {
    #[instrument(skip(self), fields(self.fw.inode))]
    async fn run(mut self) {
        debug!("Ino({}) flush task started", self.fw.inode);
        let cancel_token = self.fw.cancel_token.clone();
        let ino = self.fw.inode;
        let meta_engine = self
            .fw
            .data_manager
            .upgrade()
            .expect("data manager is dropped")
            .meta_engine
            .clone();

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    debug!("flusher of [{ino}] is cancelled");
                    return;
                }
                req = self.flush_rx.recv() => {
                    if let Some(req) = req {
                        match req {
                            FlushReq::FlushBulk {sw, offset} => {
                                debug!("{ino} flush bulk to {offset}");
                                if let Err(e) = sw.do_flush_bulk(offset).await {
                                    self.fw.record_flush_error(e);
                                    sw.reset_after_flush_failure();
                                }
                            },
                            FlushReq::FlushFull(sw) => {
                                if let Err(e) = sw.flush_and_commit(ino, meta_engine.clone()).await {
                                    self.fw.record_flush_error(e);
                                    sw.reset_after_flush_failure();
                                }
                            },
                            FlushReq::CommitIdle(sw) => {
                                if let Err(e) = sw.commit_partial(ino, meta_engine.clone()).await {
                                    self.fw.record_flush_error(e);
                                    sw.reset_after_commit_failure();
                                }
                            },
                        }
                    }
                }
            }
        }
    }
}

struct ChunkWriter {
    inode:             Ino,
    chunk_index:       ChunkIndex,
    slice_writers:     RwLock<BTreeMap<InternalSliceSeq, Arc<SliceWriter>>>,
    fw:                Weak<FileWriter>,
    total_slice_count: Arc<AtomicUsize>,
    slice_done_notify: Arc<Notify>,
}

impl ChunkWriter {
    #[instrument(skip(self), fields(chunk_index = self.chunk_index))]
    async fn find_writable_slice_writer(
        self: &Arc<Self>,
        l: &ChunkWriteCtx,
    ) -> (Arc<SliceWriter>, SliceWriterState) {
        let fw = self
            .fw
            .upgrade()
            .expect("file writer dropped while chunk writer is active");
        {
            let read_guard = self.slice_writers.read().await;
            for (idx, (_, sw)) in read_guard.iter().rev().enumerate() {
                let (flushed, length) = sw.get_flushed_length_and_total_write_length().await;
                let old_state = SliceWriterState::from(sw.state.load(Ordering::Acquire));
                if !matches!(old_state, SliceWriterState::Idle | SliceWriterState::Dirty) {
                    // we skip the writing state as well since it means we may in random writing.
                    // skip unwritable slice writer.
                    continue;
                }

                if sw.offset_of_chunk + flushed <= l.chunk_offset
                    && l.chunk_offset <= sw.offset_of_chunk + length
                {
                    if sw
                        .state
                        .compare_exchange(
                            old_state as u8,
                            SliceWriterState::Writing as u8,
                            Ordering::AcqRel,
                            Ordering::Relaxed,
                        )
                        .is_err()
                    {
                        // someone else has taken the slice writer.
                        continue;
                    }
                    // we win the contention. start to write.
                    return (sw.clone(), old_state);
                } else if idx >= 3 {
                    // try to flush in advance
                    let sw = sw.clone();
                    let req = sw.make_flush_req(fw.pattern.is_seq(), true).await;
                    if let Some(req) = req
                        && let Err(e) = fw.slice_flush_queue.send(req).await
                    {
                        // the flusher task is gone (writer is being closed);
                        // skip the early flush, the data stays buffered.
                        error!("Ino({}) failed to send flush request: {e}", self.inode);
                        fw.record_flush_error(LibcSnafu { errno: libc::EIO }.build());
                    }
                    continue;
                }
            }
        }

        // in random mode or we can't find a suitable slice writer.
        // we need to create a new one.

        let sw = Arc::new(SliceWriter::new(
            l.chunk_idx,
            self,
            SliceWriterState::Writing,
            fw.seq_generate
                .next_id()
                .expect("should not fail when generate internal seq"),
            l.chunk_offset,
            fw.data_manager.clone(),
        ));
        self.total_slice_count.fetch_add(1, Ordering::AcqRel);
        self.slice_writers
            .write()
            .await
            .insert(sw._internal_seq, sw.clone());
        (sw, SliceWriterState::Idle)
    }

    async fn finish(self: &Arc<Self>) {
        let fw = self.fw.upgrade().expect("file writer is dropped");
        loop {
            if fw.has_flush_error() {
                return;
            }
            let read_guard = self.slice_writers.read().await;
            let len = read_guard.len();
            if len == 0 {
                return;
            }

            let mut done = vec![];
            let sws = read_guard.values().cloned().collect::<Vec<_>>();
            drop(read_guard);
            let _current_len = sws.len();
            for sw in sws {
                let sw = sw.clone();
                let id = sw._internal_seq;
                let state = SliceWriterState::from(sw.state.load(Ordering::Acquire));
                if matches!(
                    state,
                    SliceWriterState::Writing | SliceWriterState::Flushing
                ) {
                    continue;
                }
                if state == SliceWriterState::Done {
                    done.push(id);
                    continue;
                }

                // actually we don't care if we are in seq or random mode when the flush is
                // called.
                let req = sw.make_flush_req(false, true).await;
                if let Some(req) = req
                    && let Err(e) = fw.slice_flush_queue.send(req).await
                {
                    // the flusher task is gone (writer is being closed);
                    // stop waiting instead of panicking the flush task.
                    error!("Ino({}) failed to send flush request: {e}", self.inode);
                    fw.record_flush_error(LibcSnafu { errno: libc::EIO }.build());
                    return;
                }
            }

            let done_len = done.len();
            if done_len != 0 {
                let mut write_guard = self.slice_writers.write().await;
                for sid in done {
                    write_guard.remove(&sid);
                }
                if write_guard.is_empty() {
                    return;
                }
            }
            // wait for the slice writer to finish.
            self.slice_done_notify.notified().await;
        }
    }
}

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
#[repr(u8)]
enum SliceWriterState {
    // An idle SliceWriter, its ready for write.
    // Two case:
    // 1. just initialized.
    // 2. just flushed bulk, but we can still write data to the slice writer.
    //
    // When come to finish, we can just drop the Idle SliceWriter,
    // since either it has not been written, or it has been flushed.
    Idle,
    // Prepare for writing or in writing process.
    // When we write finished, it goes to dirty state.
    Writing,
    // a dirty slice, have some data need to be flushed.
    Dirty,
    // in flushing process.
    Flushing,
    // in committing process.
    Committing,
    // commit finished, we need to remove the slice writer from memory.
    Done,
}

impl From<u8> for SliceWriterState {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Idle,
            1 => Self::Writing,
            2 => Self::Dirty,
            3 => Self::Flushing,
            4 => Self::Committing,
            5 => Self::Done,
            // Invariant: the backing AtomicU8 is only ever stored/CASed with
            // `SliceWriterState as u8` values in this file, so this arm is
            // unreachable. Returning a made-up state here instead would
            // silently corrupt the flush state machine (and the data it
            // guards), so the panic stays.
            _ => panic!("invalid slice writer state: {value}"),
        }
    }
}

/// SliceWriter is the entry point for writing data to a slice,
/// it depends on a SliceBuffer to buffer the write request.
/// SliceWriter can only grow from left to right, we can update it,
/// but cannot change the start offset.
struct SliceWriter {
    // dependencies
    // the underlying object storage.
    data_manager: Weak<DataManager>,
    chunk_index:  ChunkIndex,
    chunk_writer: Weak<ChunkWriter>,

    state: Arc<AtomicU8>,

    // the internal seq of the slice writer,
    // used to identify the slice writer,
    // we have it since we delay the slice-id assignment until we
    // really do the flush.
    _internal_seq:   u64,
    // the slice id of the slice, assigned by meta engine.
    slice_id:        AtomicU64,
    // where the slice start at of the chunk
    offset_of_chunk: usize,
    // the buffer to serve the write request.
    slice_buffer:    RwLock<SliceBuffer>,
    last_modified:   AtomicCell<Instant>,
}

impl Display for SliceWriter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SliceWriter[{}]", self._internal_seq)
    }
}

impl SliceWriter {
    fn new(
        chunk_index: ChunkIndex,
        cw: &Arc<ChunkWriter>,
        state: SliceWriterState,
        seq: u64,
        offset_of_chunk: usize,
        data_manager: Weak<DataManager>,
    ) -> Self {
        let page_pool = data_manager
            .upgrade()
            .expect("data manager is dropped")
            .page_pool
            .clone();
        Self {
            chunk_index,
            chunk_writer: Arc::downgrade(cw),
            state: Arc::new(AtomicU8::new(state as u8)),
            _internal_seq: seq,
            slice_id: AtomicU64::new(EMPTY_SLICE_ID),
            offset_of_chunk,
            slice_buffer: RwLock::new(SliceBuffer::new(page_pool)),
            last_modified: AtomicCell::new(Instant::now()),
            data_manager,
        }
    }

    async fn write_at(
        self: &Arc<Self>,
        // we may come from IDLE or DIRTY.
        before_state: SliceWriterState,
        offset: usize,
        data: &[u8],
    ) -> Result<usize> {
        let write_result = {
            let mut write_guard = self.slice_buffer.write().await;
            write_guard.write_at(offset, data).await
        };
        let written = match write_result {
            Ok(written) => written,
            Err(error) => {
                if let Err(state) = self.state.compare_exchange(
                    SliceWriterState::Writing as u8,
                    before_state as u8,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    error!(
                        "{self} cannot roll back failed write from {:?}",
                        SliceWriterState::from(state)
                    );
                }
                if let Some(cw) = self.chunk_writer.upgrade() {
                    cw.slice_done_notify.notify_one();
                }
                return Err(error.into());
            }
        };
        self.last_modified.store(Instant::now());
        let new_state = if written == 0 {
            before_state
        } else {
            SliceWriterState::Dirty
        };

        // we must be in the writing state.
        // the find writableSliceWriter will set the in_writing to true.
        if let Err(e) = self.state.compare_exchange(
            SliceWriterState::Writing as u8,
            new_state as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            // only the writer that won the CAS in find_writable_slice_writer
            // may transition out of Writing; a mismatch means the state
            // machine was corrupted. Fail this write with EIO instead of
            // wedging the mount.
            error!(
                "{self} failed to change state from {:?} to {:?}, current state: {:?}",
                SliceWriterState::Writing,
                new_state,
                SliceWriterState::from(e)
            );
            return LibcSnafu { errno: libc::EIO }.fail();
        }
        Ok(written)
    }

    async fn do_flush_bulk(self: &Arc<Self>, offset: usize) -> Result<()> {
        let slice_id = self.prepare_slice_id().await?;
        let mut write_guard = self.slice_buffer.write().await;
        write_guard
            .stage(
                slice_id,
                offset,
                self.data_manager
                    .upgrade()
                    .expect("data manager dropped during slice flush")
                    .file_cache
                    .clone(),
            )
            .await?;
        drop(write_guard);

        if let Err(state) = self.state.compare_exchange(
            SliceWriterState::Flushing as u8,
            SliceWriterState::Idle as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            error!(
                "{self} cannot finish bulk flush from {:?}",
                SliceWriterState::from(state)
            );
            return LibcSnafu { errno: libc::EIO }.fail();
        }
        if let Some(cw) = self.chunk_writer.upgrade() {
            cw.slice_done_notify.notify_one();
        }

        Ok(())
    }

    /// Return a failed full flush to Dirty after the owning FileWriter has
    /// recorded the error. The buffer may now contain pages, durable staged
    /// blocks, or both; retrying with the same slice id handles every case.
    fn reset_after_flush_failure(self: &Arc<Self>) {
        if let Err(state) = self.state.compare_exchange(
            SliceWriterState::Flushing as u8,
            SliceWriterState::Dirty as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            error!(
                "{self} cannot reset failed flush from {:?}",
                SliceWriterState::from(state)
            );
        }
        if let Some(cw) = self.chunk_writer.upgrade() {
            cw.slice_done_notify.notify_one();
        }
    }

    fn reset_after_commit_failure(self: &Arc<Self>) {
        if let Err(state) = self.state.compare_exchange(
            SliceWriterState::Committing as u8,
            SliceWriterState::Idle as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            error!(
                "{self} cannot reset failed metadata commit from {:?}",
                SliceWriterState::from(state)
            );
        }
        if let Some(cw) = self.chunk_writer.upgrade() {
            cw.slice_done_notify.notify_one();
        }
    }

    #[instrument(skip_all, fields(_internal_seq = self._internal_seq))]
    async fn flush_and_commit(
        self: &Arc<Self>,
        inode: Ino,
        meta_engine_ref: MetaEngineRef,
    ) -> Result<()> {
        let slice_id = self.prepare_slice_id().in_current_span().await?;
        let mut write_guard = self.slice_buffer.write().await;
        write_guard
            .flush_v2(
                slice_id,
                self.data_manager
                    .upgrade()
                    .expect("data manager dropped during slice flush")
                    .file_cache
                    .clone(),
            )
            .await?;

        let len = write_guard.length();
        drop(write_guard);

        // then the sw is done.
        // write the meta info of this slice.
        meta_engine_ref
            .write_slice(
                inode,
                self.chunk_index,
                self.offset_of_chunk,
                kiseki_types::slice::Slice::Owned {
                    chunk_pos: self.offset_of_chunk as u32,
                    id:        slice_id,
                    size:      len as u32,
                    _padding:  0,
                },
                self.last_modified.load(),
            )
            .await?;

        let cw = self
            .chunk_writer
            .upgrade()
            .context(LibcSnafu { errno: libc::EIO })?;
        cw.fw
            .upgrade()
            .context(LibcSnafu { errno: libc::EIO })?
            .record_published_slice(slice_id);
        cw.total_slice_count.fetch_sub(1, Ordering::AcqRel);
        if let Err(e) = self.state.compare_exchange(
            SliceWriterState::Flushing as u8,
            SliceWriterState::Done as u8,
            Ordering::AcqRel,
            Ordering::Relaxed,
        ) {
            // data and meta are already durable at this point and only this
            // task transitions out of Flushing; log and fall through, the
            // slice is finished either way.
            error!(
                "{self} failed to change state from flushing to done after flush, current state: \
                 {:?}",
                SliceWriterState::from(e)
            );
        };
        cw.slice_done_notify.notify_one();

        Ok(())
    }

    async fn prepare_slice_id(self: &Arc<Self>) -> Result<SliceID> {
        let old = self.slice_id.load(Ordering::Acquire);
        if old != EMPTY_SLICE_ID {
            return Ok(old);
        }

        let data_manager = self
            .data_manager
            .upgrade()
            .expect("data manager should not be dropped");
        let slice_id = data_manager
            .meta_engine
            .next_slice_id()
            .in_current_span()
            .await?;
        // don't care about the result, since we only care about the first time.
        let _ = self
            .slice_id
            .compare_exchange(0, slice_id, Ordering::AcqRel, Ordering::Relaxed);
        Ok(slice_id)
    }

    // make_flush_req try to make flush req if we write enough data.
    async fn make_flush_req(self: Arc<Self>, seq: bool, finish: bool) -> Option<FlushReq> {
        let state = self.state.load(Ordering::Acquire);
        match SliceWriterState::from(state) {
            SliceWriterState::Idle => {
                if finish {
                    // we should try to commit this slice writer.
                    if self
                        .state
                        .compare_exchange(
                            SliceWriterState::Idle as u8,
                            SliceWriterState::Committing as u8,
                            Ordering::AcqRel,
                            Ordering::Relaxed,
                        )
                        .is_err()
                    {
                        // someone win the contention. we give up
                        return None;
                    }
                    Some(FlushReq::CommitIdle(self.clone()))
                } else {
                    None
                }
            }
            SliceWriterState::Writing
            | SliceWriterState::Flushing
            | SliceWriterState::Committing
            | SliceWriterState::Done => {
                // Idle: nothing to flush
                // Writing: someone take the slice writer again, let him cook
                // Flushing: already in flushing, let it go.
                // Committing: we are committing, let it go.
                None
            }
            SliceWriterState::Dirty => {
                if finish {
                    if let Err(e) = self.state.compare_exchange(
                        SliceWriterState::Dirty as u8,
                        SliceWriterState::Flushing as u8,
                        Ordering::AcqRel,
                        Ordering::Relaxed,
                    ) {
                        // someone won the contention, give up like the
                        // non-finish paths do; the finish loop re-checks the
                        // slice writer's state on its next round.
                        debug!(
                            "{self} give up flushing, state changed to {:?}",
                            SliceWriterState::from(e)
                        );
                        return None;
                    }
                    return Some(FlushReq::FlushFull(self.clone()));
                }

                let read_guard = self.slice_buffer.read().await;
                let length = read_guard.length();
                let flushed_len = read_guard.flushed_length();
                drop(read_guard);

                if length == CHUNK_SIZE {
                    if self
                        .state
                        .compare_exchange(
                            SliceWriterState::Dirty as u8,
                            SliceWriterState::Flushing as u8,
                            Ordering::AcqRel,
                            Ordering::Relaxed,
                        )
                        .is_err()
                    {
                        // someone win the contention.
                        return None;
                    }
                    Some(FlushReq::FlushFull(self.clone()))
                } else if length - flushed_len >= BLOCK_SIZE && !seq {
                    if self
                        .state
                        .compare_exchange(
                            SliceWriterState::Dirty as u8,
                            SliceWriterState::Flushing as u8,
                            Ordering::AcqRel,
                            Ordering::Relaxed,
                        )
                        .is_err()
                    {
                        // someone win the contention. we give up
                        return None;
                    }
                    Some(FlushReq::FlushBulk {
                        sw:     self.clone(),
                        offset: length,
                    })
                } else {
                    None
                }
            }
        }
    }

    // get the underlying write buffer's released length and total write length.
    // The whole body is the critical section (two reads under one guard), so
    // there is nothing to tighten.
    #[allow(clippy::significant_drop_tightening)]
    async fn get_flushed_length_and_total_write_length(self: &Arc<Self>) -> (usize, usize) {
        let guard = self.slice_buffer.read().await;
        let flushed_len = guard.flushed_length();
        let write_len = guard.length();
        (flushed_len, write_len)
    }

    async fn commit_partial(
        self: &Arc<Self>,
        inode: Ino,
        meta_engine_ref: MetaEngineRef,
    ) -> Result<()> {
        let cw = self
            .chunk_writer
            .upgrade()
            .expect("chunk writer dropped while committing partial slice");

        // write slice meta info to meta engine.
        let slice_id = self.slice_id.load(Ordering::Acquire);
        let len = self.slice_buffer.read().await.length();
        if len != 0 {
            // then the sw is done.
            // write the meta info of this slice.
            meta_engine_ref
                .write_slice(
                    inode,
                    self.chunk_index,
                    self.offset_of_chunk,
                    kiseki_types::slice::Slice::Owned {
                        chunk_pos: self.offset_of_chunk as u32,
                        id:        slice_id,
                        size:      len as u32,
                        _padding:  0,
                    },
                    self.last_modified.load(),
                )
                .await?;

            cw.fw
                .upgrade()
                .context(LibcSnafu { errno: libc::EIO })?
                .record_published_slice(slice_id);
        }

        cw.total_slice_count.fetch_sub(1, Ordering::AcqRel);
        if let Err(e) = self.state.compare_exchange(
            SliceWriterState::Committing as u8,
            SliceWriterState::Done as u8,
            Ordering::AcqRel,
            Ordering::Relaxed,
        ) {
            error!(
                "{self} failed to change state from committing to done, current state: {:?}",
                SliceWriterState::from(e)
            );
        }
        cw.slice_done_notify.notify_one();

        Ok(())
    }
}

impl Eq for SliceWriter {}

impl PartialEq for SliceWriter {
    fn eq(&self, other: &Self) -> bool { self._internal_seq == other._internal_seq }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
struct ChunkWriteCtx {
    file_offset:    usize,
    // write to which chunk
    chunk_idx:      usize,
    // the start offset of this write
    chunk_offset:   usize,
    // the length of this write
    need_write_len: usize,
    // the start offset of the input buf
    buf_start_at:   usize,
}

fn locate_chunk(chunk_size: usize, offset: usize, expect_write_len: usize) -> Vec<ChunkWriteCtx> {
    let expected_write_len = expect_write_len;

    let start_chunk_idx = cal_chunk_idx(offset, chunk_size);
    let end_chunk_idx = cal_chunk_idx(offset + expected_write_len - 1, chunk_size);

    let mut chunk_pos = cal_chunk_offset(offset, chunk_size);
    let mut buf_start_at = 0;
    let mut left = expected_write_len;

    (start_chunk_idx..=end_chunk_idx)
        .map(move |idx| {
            let max_can_write = min(chunk_size - chunk_pos, left);
            debug!(
                "offset: {}, chunk-size: {}, chunk: {} chunk_pos: {}, left: {}, buf start at: {}, \
                 max can write: {}",
                ReadableSize(offset as u64),
                ReadableSize(chunk_size as u64),
                idx,
                chunk_pos,
                ReadableSize(left as u64),
                ReadableSize(buf_start_at as u64),
                ReadableSize(max_can_write as u64)
            );

            let ctx = ChunkWriteCtx {
                file_offset: offset + buf_start_at,
                chunk_idx: idx,
                chunk_offset: chunk_pos,
                need_write_len: max_can_write,
                buf_start_at,
            };
            chunk_pos = cal_chunk_offset(chunk_pos + max_can_write, chunk_size);
            buf_start_at += max_can_write;
            left -= max_can_write;
            ctx
        })
        .collect::<Vec<_>>()
}

#[cfg(test)]
mod tests {
    use std::{process::Command, sync::Arc, time::Duration};

    use futures::StreamExt;
    use kiseki_meta::{MetaConfig, context::FuseContext};
    use kiseki_storage::cache::{
        file_cache::{Config as FileCacheConfig, FileCache},
        mem_cache::{Config as MemCacheConfig, MemCache},
    };
    use kiseki_types::{
        ino::ROOT_INO,
        setting::Format,
        slice::{Slice, SliceKey},
    };
    use kiseki_utils::{
        object_storage::{new_local_object_store, new_memory_object_store},
        readable_size::ReadableSize,
    };

    use super::*;
    use crate::data_manager::new_test_page_pool;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_finishes_do_not_lose_slice_completion() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut meta_config = MetaConfig::default();
        meta_config.with_dsn(&format!(
            "rocksdb://:{}",
            tempdir.path().join("meta").to_str().unwrap()
        ));
        let format = Format::default();
        kiseki_meta::update_format(&meta_config.dsn, format.clone(), true).unwrap();
        let meta_engine = kiseki_meta::open(meta_config).unwrap();
        let (inode, _) = meta_engine
            .create(
                Arc::new(FuseContext::background()),
                ROOT_INO,
                "concurrent-finish",
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
                FileCacheConfig {
                    stage_cache_dir: tempdir.path().join("stage"),
                    max_stage_size:  ReadableSize((BLOCK_SIZE * 2) as u64),
                    cache_ttl:       Duration::from_secs(3600),
                },
                MemCacheConfig::default(),
                crate::data_manager::new_test_page_pool().await,
            )
            .unwrap(),
        );
        let writer = data_manager.open_file_writer(inode, 0).unwrap();
        writer.write(0, b"concurrent flush").await.unwrap();

        tokio::time::timeout(Duration::from_secs(3), async {
            let (first, second) = tokio::join!(writer.finish(), writer.finish());
            first.unwrap();
            second.unwrap();
        })
        .await
        .expect("concurrent finishes must not lose a slice completion notification");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn flush_is_local_durable_while_fsync_requires_remote_durability() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut meta_config = MetaConfig::default();
        meta_config.with_dsn(&format!(
            "rocksdb://:{}",
            tempdir.path().join("meta").to_str().unwrap()
        ));
        let format = Format::default();
        kiseki_meta::update_format(&meta_config.dsn, format.clone(), true).unwrap();
        let meta_engine = kiseki_meta::open(meta_config).unwrap();
        let (inode, _) = meta_engine
            .create(
                Arc::new(FuseContext::background()),
                ROOT_INO,
                "retryable",
                0o600,
                0,
                0,
            )
            .await
            .unwrap();

        let remote_dir = tempdir.path().join("remote");
        let stage_dir = tempdir.path().join("stage");
        let remote_storage = new_local_object_store(&remote_dir).unwrap();
        let tasks = Arc::new(kiseki_storage::task_registry::MountTaskRegistry::new());
        let file_cache = Arc::new(
            FileCache::new(
                FileCacheConfig {
                    stage_cache_dir: stage_dir.clone(),
                    max_stage_size:  ReadableSize((BLOCK_SIZE * 2) as u64),
                    cache_ttl:       Duration::from_secs(3600),
                },
                remote_storage.clone(),
                tasks.clone(),
            )
            .unwrap(),
        );
        let data_manager = Arc::new(DataManager {
            chunk_size: format.chunk_size,
            file_writers: Arc::new(Default::default()),
            file_readers: Default::default(),
            id_generator: Arc::new(sonyflake::Sonyflake::new().unwrap()),
            meta_engine: meta_engine.clone(),
            file_cache,
            mem_cache: Arc::new(MemCache::new(
                MemCacheConfig::default(),
                remote_storage.clone(),
            )),
            page_pool: new_test_page_pool().await,
            tasks,
        });
        let writer = data_manager.open_file_writer(inode, 0).unwrap();
        let data = b"failed fsync must remain retryable";
        writer.write(0, data).await.unwrap();

        std::fs::remove_dir_all(&stage_dir).unwrap();
        std::fs::write(&stage_dir, b"local stage unavailable").unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(3), writer.finish())
                .await
                .expect("failed full flush must not hang")
                .is_err()
        );
        let chunk_writer = writer.chunk_writers.read().await.get(&0).unwrap().clone();
        let slice_writer = chunk_writer
            .slice_writers
            .read()
            .await
            .values()
            .next()
            .unwrap()
            .clone();
        assert_eq!(
            SliceWriterState::from(slice_writer.state.load(Ordering::Acquire)),
            SliceWriterState::Dirty
        );
        assert!(meta_engine.read_slice(inode, 0).await.is_err());
        std::fs::remove_file(&stage_dir).unwrap();
        std::fs::create_dir(&stage_dir).unwrap();

        std::fs::remove_dir_all(&remote_dir).unwrap();
        std::fs::write(&remote_dir, b"remote unavailable").unwrap();
        tokio::time::timeout(Duration::from_secs(3), writer.finish())
            .await
            .expect("local-durable flush must not hang")
            .unwrap();
        assert!(writer.chunk_writers.read().await.is_empty());
        let slices = meta_engine.read_slice(inode, 0).await.unwrap().unwrap();
        assert_eq!(slices.len(), 1);
        let Slice::Owned { id, size, .. } = slices.0[0] else {
            panic!("writer must publish an owned slice");
        };
        let slice_key = SliceKey::new(id, 0, size as usize);
        assert_eq!(
            data_manager
                .file_cache
                .get_range(&slice_key, 0, data.len())
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            data
        );
        assert!(
            tokio::time::timeout(Duration::from_secs(3), writer.sync_remote())
                .await
                .expect("failed fsync must not hang")
                .is_err()
        );

        std::fs::remove_file(&remote_dir).unwrap();
        std::fs::create_dir(&remote_dir).unwrap();
        tokio::time::timeout(Duration::from_secs(3), writer.sync_remote())
            .await
            .expect("fsync retry must not hang")
            .unwrap();
        assert!(writer.chunk_writers.read().await.is_empty());
        assert!(
            data_manager
                .file_cache
                .get_range(&slice_key, 0, data.len())
                .await
                .unwrap()
                .is_none()
        );

        let mut objects = remote_storage.list(None);
        let object = objects.next().await.unwrap().unwrap();
        assert!(objects.next().await.is_none());
        assert_eq!(
            remote_storage
                .get(&object.location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            data
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_bulk_and_idle_commit_remain_retryable() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut meta_config = MetaConfig::default();
        meta_config.with_dsn(&format!(
            "rocksdb://:{}",
            tempdir.path().join("meta").to_str().unwrap()
        ));
        let format = Format::default();
        kiseki_meta::update_format(&meta_config.dsn, format.clone(), true).unwrap();
        let meta_engine = kiseki_meta::open(meta_config).unwrap();
        let (inode, _) = meta_engine
            .create(
                Arc::new(FuseContext::background()),
                ROOT_INO,
                "bulk-retry",
                0o600,
                0,
                0,
            )
            .await
            .unwrap();

        let stage_dir = tempdir.path().join("stage");
        let remote_storage = new_memory_object_store();
        let tasks = Arc::new(kiseki_storage::task_registry::MountTaskRegistry::new());
        let file_cache = Arc::new(
            FileCache::new(
                FileCacheConfig {
                    stage_cache_dir: stage_dir.clone(),
                    max_stage_size:  ReadableSize((BLOCK_SIZE * 2) as u64),
                    cache_ttl:       Duration::from_secs(3600),
                },
                remote_storage.clone(),
                tasks.clone(),
            )
            .unwrap(),
        );
        let data_manager = Arc::new(DataManager {
            chunk_size: format.chunk_size,
            file_writers: Arc::new(Default::default()),
            file_readers: Default::default(),
            id_generator: Arc::new(sonyflake::Sonyflake::new().unwrap()),
            meta_engine: meta_engine.clone(),
            file_cache,
            mem_cache: Arc::new(MemCache::new(MemCacheConfig::default(), remote_storage)),
            page_pool: new_test_page_pool().await,
            tasks,
        });
        let writer = data_manager.open_file_writer(inode, 0).unwrap();
        let data = vec![0x5A; BLOCK_SIZE];
        writer.write(0, &data).await.unwrap();

        let chunk_writer = writer.chunk_writers.read().await.get(&0).unwrap().clone();
        let slice_writer = chunk_writer
            .slice_writers
            .read()
            .await
            .values()
            .next()
            .unwrap()
            .clone();
        let request = slice_writer
            .clone()
            .make_flush_req(false, false)
            .await
            .expect("a full block must create a bulk flush request");
        std::fs::remove_dir_all(&stage_dir).unwrap();
        std::fs::write(&stage_dir, b"prevent stage writes").unwrap();
        writer.slice_flush_queue.send(request).await.unwrap();

        assert!(
            tokio::time::timeout(Duration::from_secs(3), writer.finish())
                .await
                .expect("failed bulk flush must wake finish")
                .is_err()
        );
        assert_eq!(
            SliceWriterState::from(slice_writer.state.load(Ordering::Acquire)),
            SliceWriterState::Dirty
        );
        let mut actual = vec![0; data.len()];
        slice_writer
            .slice_buffer
            .read()
            .await
            .read_at(0, &mut actual)
            .await
            .unwrap();
        assert_eq!(actual, data);

        std::fs::remove_file(&stage_dir).unwrap();
        std::fs::create_dir(&stage_dir).unwrap();
        let retry = slice_writer
            .clone()
            .make_flush_req(false, false)
            .await
            .expect("dirty slice must be retryable as a bulk flush");
        let FlushReq::FlushBulk { sw, offset } = retry else {
            panic!("expected a bulk retry");
        };
        sw.do_flush_bulk(offset).await.unwrap();
        assert_eq!(
            SliceWriterState::from(slice_writer.state.load(Ordering::Acquire)),
            SliceWriterState::Idle
        );

        let mut wrong_meta_config = MetaConfig::default();
        wrong_meta_config.with_dsn(&format!(
            "rocksdb://:{}",
            tempdir.path().join("wrong-meta").to_str().unwrap()
        ));
        kiseki_meta::update_format(&wrong_meta_config.dsn, format.clone(), true).unwrap();
        let wrong_meta = kiseki_meta::open(wrong_meta_config).unwrap();
        let commit = slice_writer
            .clone()
            .make_flush_req(false, true)
            .await
            .expect("a staged idle slice must create a metadata commit request");
        let FlushReq::CommitIdle(sw) = commit else {
            panic!("expected an idle metadata commit");
        };
        let error = sw.commit_partial(inode, wrong_meta).await.unwrap_err();
        writer.record_flush_error(error);
        sw.reset_after_commit_failure();
        assert_eq!(
            SliceWriterState::from(slice_writer.state.load(Ordering::Acquire)),
            SliceWriterState::Idle
        );
        assert!(meta_engine.read_slice(inode, 0).await.is_err());
        assert!(writer.finish().await.is_err());

        tokio::time::timeout(Duration::from_secs(3), writer.finish())
            .await
            .expect("metadata commit retry must not hang")
            .unwrap();
        assert!(writer.chunk_writers.read().await.is_empty());
        assert_eq!(
            meta_engine
                .read_slice(inode, 0)
                .await
                .unwrap()
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn abrupt_exit_after_local_flush_reopens_exact_published_bytes() {
        let tempdir = tempfile::tempdir().unwrap();
        let meta_dir = tempdir.path().join("meta");
        let stage_dir = tempdir.path().join("stage");
        let remote_dir = tempdir.path().join("remote");
        let output = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("writer::tests::crash_after_local_flush_helper")
            .arg("--ignored")
            .env("KISEKI_WRITER_CRASH_HELPER", "1")
            .env("KISEKI_WRITER_CRASH_META", &meta_dir)
            .env("KISEKI_WRITER_CRASH_STAGE", &stage_dir)
            .env("KISEKI_WRITER_CRASH_REMOTE", &remote_dir)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "writer crash helper failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let remote_storage = new_local_object_store(&remote_dir).unwrap();
        std::fs::remove_dir_all(&remote_dir).unwrap();
        std::fs::write(&remote_dir, b"keep recovered data local").unwrap();
        let mut meta_config = MetaConfig::default();
        meta_config.with_dsn(&format!("rocksdb://:{}", meta_dir.to_str().unwrap()));
        let meta_engine = kiseki_meta::open(meta_config).unwrap();
        let format = meta_engine.get_format().clone();
        let (inode, attr) = meta_engine
            .lookup(
                Arc::new(FuseContext::background()),
                ROOT_INO,
                "crash-published",
                false,
            )
            .await
            .unwrap();
        let data_manager = Arc::new(
            DataManager::new(
                format.chunk_size,
                meta_engine,
                remote_storage,
                FileCacheConfig {
                    stage_cache_dir: stage_dir,
                    max_stage_size:  ReadableSize((BLOCK_SIZE * 2) as u64),
                    cache_ttl:       Duration::from_secs(3600),
                },
                MemCacheConfig::default(),
                crate::data_manager::new_test_page_pool().await,
            )
            .unwrap(),
        );
        let reader = data_manager
            .open_file_reader(inode, 1, attr.length as usize)
            .await;
        let expected = b"published bytes survive process death";
        let mut actual = vec![0; expected.len()];
        assert_eq!(reader.read(0, &mut actual).await.unwrap(), expected.len());
        assert_eq!(actual, expected);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn abrupt_exit_after_fsync_reopens_from_remote_without_stage() {
        let tempdir = tempfile::tempdir().unwrap();
        let meta_dir = tempdir.path().join("meta");
        let stage_dir = tempdir.path().join("stage");
        let remote_dir = tempdir.path().join("remote");
        let output = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("writer::tests::crash_after_local_flush_helper")
            .arg("--ignored")
            .env("KISEKI_WRITER_CRASH_HELPER", "1")
            .env("KISEKI_WRITER_CRASH_SYNC_REMOTE", "1")
            .env("KISEKI_WRITER_CRASH_META", &meta_dir)
            .env("KISEKI_WRITER_CRASH_STAGE", &stage_dir)
            .env("KISEKI_WRITER_CRASH_REMOTE", &remote_dir)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "writer crash helper failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        if stage_dir.exists() {
            std::fs::remove_dir_all(&stage_dir).unwrap();
        }
        let remote_storage = new_local_object_store(&remote_dir).unwrap();
        let mut meta_config = MetaConfig::default();
        meta_config.with_dsn(&format!("rocksdb://:{}", meta_dir.to_str().unwrap()));
        let meta_engine = kiseki_meta::open(meta_config).unwrap();
        let format = meta_engine.get_format().clone();
        let (inode, attr) = meta_engine
            .lookup(
                Arc::new(FuseContext::background()),
                ROOT_INO,
                "crash-published",
                false,
            )
            .await
            .unwrap();
        let data_manager = Arc::new(
            DataManager::new(
                format.chunk_size,
                meta_engine,
                remote_storage,
                FileCacheConfig {
                    stage_cache_dir: stage_dir,
                    max_stage_size:  ReadableSize((BLOCK_SIZE * 2) as u64),
                    cache_ttl:       Duration::from_secs(3600),
                },
                MemCacheConfig::default(),
                crate::data_manager::new_test_page_pool().await,
            )
            .unwrap(),
        );
        let reader = data_manager
            .open_file_reader(inode, 1, attr.length as usize)
            .await;
        let expected = b"published bytes survive process death";
        let mut actual = vec![0; expected.len()];
        assert_eq!(reader.read(0, &mut actual).await.unwrap(), expected.len());
        assert_eq!(actual, expected);
    }

    #[ignore = "subprocess helper"]
    #[tokio::test]
    async fn crash_after_local_flush_helper() {
        if std::env::var_os("KISEKI_WRITER_CRASH_HELPER").is_none() {
            return;
        }
        let meta_dir =
            std::path::PathBuf::from(std::env::var_os("KISEKI_WRITER_CRASH_META").unwrap());
        let stage_dir =
            std::path::PathBuf::from(std::env::var_os("KISEKI_WRITER_CRASH_STAGE").unwrap());
        let remote_dir =
            std::path::PathBuf::from(std::env::var_os("KISEKI_WRITER_CRASH_REMOTE").unwrap());
        let mut meta_config = MetaConfig::default();
        meta_config.with_dsn(&format!("rocksdb://:{}", meta_dir.to_str().unwrap()));
        let format = Format::default();
        kiseki_meta::update_format(&meta_config.dsn, format.clone(), true).unwrap();
        let meta_engine = kiseki_meta::open(meta_config).unwrap();
        let (inode, _) = meta_engine
            .create(
                Arc::new(FuseContext::background()),
                ROOT_INO,
                "crash-published",
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
                new_local_object_store(remote_dir).unwrap(),
                FileCacheConfig {
                    stage_cache_dir: stage_dir,
                    max_stage_size:  ReadableSize((BLOCK_SIZE * 2) as u64),
                    cache_ttl:       Duration::from_secs(3600),
                },
                MemCacheConfig::default(),
                crate::data_manager::new_test_page_pool().await,
            )
            .unwrap(),
        );
        let writer = data_manager.open_file_writer(inode, 0).unwrap();
        writer
            .write(0, b"published bytes survive process death")
            .await
            .unwrap();
        if std::env::var_os("KISEKI_WRITER_CRASH_SYNC_REMOTE").is_some() {
            writer.sync_remote().await.unwrap();
        } else {
            writer.finish().await.unwrap();
        }
        std::process::exit(0);
    }
}
