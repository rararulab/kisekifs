// Copyright 2024 kisekifs
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

//! io_uring-backed local file I/O for the stage-cache data path.
//!
//! `tokio-uring` runs a **current-thread** runtime with its own io_uring
//! instance, whereas the VFS runs a multi-threaded Tokio runtime. This module
//! bridges the two with a small offload pool: a dedicated tokio-uring worker
//! thread owns the ring, and callers on any runtime submit read/write jobs and
//! await the result over a `oneshot`.
//!
//! Concurrency comes from io_uring itself — each job is `tokio_uring::spawn`ed,
//! so one worker thread drives many in-flight I/Os. Stage blocks are ordinary
//! files, so the object-store local backend still reads/lists/deletes the exact
//! same paths; only the hot write/read go through io_uring.

use std::{
    io,
    path::{Path, PathBuf},
};

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

/// Read buffer chunk when the target length is unknown / large.
const READ_CHUNK: usize = 1 << 20;

enum Job {
    Write {
        path: PathBuf,
        data: Bytes,
        resp: oneshot::Sender<io::Result<()>>,
    },
    Read {
        path: PathBuf,
        resp: oneshot::Sender<io::Result<Bytes>>,
    },
    ReadRange {
        path:   PathBuf,
        offset: u64,
        len:    usize,
        resp:   oneshot::Sender<io::Result<Bytes>>,
    },
}

fn channel_closed() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "uring pool worker is gone")
}

/// Handle to the io_uring offload worker.
#[derive(Clone)]
pub struct UringPool {
    tx: mpsc::UnboundedSender<Job>,
}

impl UringPool {
    /// Spawn the io_uring worker thread. Cheap to clone; the worker lives until
    /// every clone is dropped.
    pub fn new() -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel::<Job>();
        std::thread::Builder::new()
            .name("kiseki-uring".to_string())
            .spawn(move || {
                tokio_uring::start(async move {
                    while let Some(job) = rx.recv().await {
                        tokio_uring::spawn(handle(job));
                    }
                });
            })
            .expect("spawn io_uring worker thread");
        Self { tx }
    }

    /// Atomically write `data` to `path` (temp file + rename).
    pub async fn write(&self, path: PathBuf, data: Bytes) -> io::Result<()> {
        let (resp, rx) = oneshot::channel();
        self.tx
            .send(Job::Write { path, data, resp })
            .map_err(|_| channel_closed())?;
        rx.await.map_err(|_| channel_closed())?
    }

    /// Read the whole file at `path`.
    pub async fn read(&self, path: PathBuf) -> io::Result<Bytes> {
        let (resp, rx) = oneshot::channel();
        self.tx
            .send(Job::Read { path, resp })
            .map_err(|_| channel_closed())?;
        rx.await.map_err(|_| channel_closed())?
    }

    /// Read `len` bytes from `path` starting at `offset`.
    pub async fn read_range(&self, path: PathBuf, offset: u64, len: usize) -> io::Result<Bytes> {
        let (resp, rx) = oneshot::channel();
        self.tx
            .send(Job::ReadRange {
                path,
                offset,
                len,
                resp,
            })
            .map_err(|_| channel_closed())?;
        rx.await.map_err(|_| channel_closed())?
    }
}

impl Default for UringPool {
    fn default() -> Self { Self::new() }
}

async fn handle(job: Job) {
    match job {
        Job::Write { path, data, resp } => {
            let _ = resp.send(write_atomic(&path, data).await);
        }
        Job::Read { path, resp } => {
            let _ = resp.send(read_all(&path).await);
        }
        Job::ReadRange {
            path,
            offset,
            len,
            resp,
        } => {
            let _ = resp.send(read_exact_at(&path, offset, len).await);
        }
    }
}

/// Write `data` to a sibling temp file, fsync, then rename into place so a
/// concurrent reader (io_uring or object-store) never sees a partial block.
async fn write_atomic(path: &Path, data: Bytes) -> io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "stage path has no parent"))?;
    std::fs::create_dir_all(parent)?; // idempotent when the directory exists
    let tmp = tmp_path(path);

    let file = tokio_uring::fs::File::create(&tmp).await?;
    // tokio-uring's IoBuf is implemented for `Vec<u8>`, not `Bytes`.
    let write = file.write_all_at(data.to_vec(), 0).await.0;
    let sync = if write.is_ok() {
        file.sync_all().await
    } else {
        Ok(())
    };
    let _ = file.close().await;
    if let Err(e) = write.and(sync) {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    std::fs::rename(&tmp, path).inspect_err(|_| {
        let _ = std::fs::remove_file(&tmp);
    })
}

async fn read_all(path: &Path) -> io::Result<Bytes> {
    let file = tokio_uring::fs::File::open(path).await?;
    let mut out: Vec<u8> = Vec::new();
    let mut offset = 0u64;
    loop {
        let buf = vec![0u8; READ_CHUNK];
        let (res, buf) = file.read_at(buf, offset).await;
        let n = res?;
        if n == 0 {
            break;
        }
        out.extend_from_slice(&buf[..n]);
        offset += n as u64;
    }
    let _ = file.close().await;
    Ok(Bytes::from(out))
}

async fn read_exact_at(path: &Path, offset: u64, len: usize) -> io::Result<Bytes> {
    let file = tokio_uring::fs::File::open(path).await?;
    let mut out: Vec<u8> = Vec::with_capacity(len);
    let mut pos = offset;
    while out.len() < len {
        let want = (len - out.len()).min(READ_CHUNK);
        let buf = vec![0u8; want];
        let (res, buf) = file.read_at(buf, pos).await;
        let n = res?;
        if n == 0 {
            break;
        }
        out.extend_from_slice(&buf[..n]);
        pos += n as u64;
    }
    let _ = file.close().await;
    if out.len() < len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "short read from stage block",
        ));
    }
    Ok(Bytes::from(out))
}

fn tmp_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".uring.tmp");
    path.with_file_name(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn write_then_read_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let pool = UringPool::new();
        let path = dir.path().join("sub/block-0");
        let payload = Bytes::from_static(b"kiseki io_uring stage block payload");

        pool.write(path.clone(), payload.clone()).await.unwrap();
        assert!(path.exists(), "block file should be published after write");

        let whole = pool.read(path.clone()).await.unwrap();
        assert_eq!(whole, payload);

        let range = pool.read_range(path.clone(), 7, 7).await.unwrap();
        assert_eq!(&range[..], &payload[7..14]);
    }

    #[tokio::test]
    async fn read_missing_file_errors() {
        let dir = tempfile::tempdir().unwrap();
        let pool = UringPool::new();
        let err = pool.read(dir.path().join("nope")).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }
}
