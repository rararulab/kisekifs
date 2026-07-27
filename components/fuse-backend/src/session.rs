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

//! Mount session + worker-thread pool.

use std::{io, path::Path, sync::Arc};

use fuse_backend_rs::{
    api::server::Server,
    transport::{FuseChannel, FuseSession},
};
use kiseki_vfs::KisekiVFS;
use tokio::runtime::Handle;
use tracing::{error, info};

use crate::fs::KisekiFsBackend;

/// A mounted fuse-backend-rs session and its worker-thread pool. The caller
/// orchestrates the mount lifecycle (readiness, shutdown) around it.
pub struct FbrSession {
    session: FuseSession,
    workers: Vec<std::thread::JoinHandle<()>>,
}

impl FbrSession {
    /// Have all worker threads finished (i.e. the mount is torn down)?
    pub fn all_finished(&self) -> bool {
        self.workers
            .iter()
            .all(std::thread::JoinHandle::is_finished)
    }

    /// Signal the workers to stop and tear down the kernel mount. Waking the
    /// channels makes `get_request` return `None`; `umount` removes the mount
    /// so `is_mounted` becomes false.
    pub fn stop(&mut self) -> io::Result<()> {
        let _ = self.session.wake();
        self.session
            .umount()
            .map_err(|e| io::Error::other(format!("umount fuse session: {e}")))
    }

    /// Join all worker threads (blocks).
    pub fn join(self) {
        for w in self.workers {
            let _ = w.join();
        }
    }
}

/// Mount KisekiFS at `mountpoint` via fuse-backend-rs and spawn `num_threads`
/// worker threads. Returns once the kernel mount is live; the caller drives the
/// lifecycle via the returned [`FbrSession`].
///
/// `vfs_rt` must be a live multi-threaded Tokio runtime handle owning the VFS'
/// background tasks; it has to outlive this call.
pub fn mount(
    vfs: Arc<KisekiVFS>,
    vfs_rt: Handle,
    mountpoint: &Path,
    fsname: &str,
    allow_other: bool,
    read_only: bool,
    num_threads: usize,
) -> io::Result<FbrSession> {
    let backend = KisekiFsBackend::new(vfs, vfs_rt);
    let server = Arc::new(Server::new(backend));

    let mut session = FuseSession::new(mountpoint, fsname, "", read_only)
        .map_err(|e| io::Error::other(format!("create fuse session: {e}")))?;
    session.set_allow_other(allow_other);
    session
        .mount()
        .map_err(|e| io::Error::other(format!("mount fuse session: {e}")))?;
    info!("kiseki (fuse-backend-rs) mounted at {mountpoint:?} with {num_threads} worker(s)");

    let workers = (0..num_threads.max(1))
        .map(|i| {
            let channel = session
                .new_channel()
                .map_err(|e| io::Error::other(format!("create fuse channel: {e}")))?;
            let server = server.clone();
            std::thread::Builder::new()
                .name(format!("kiseki-fuse-{i}"))
                .spawn(move || serve(&server, channel))
        })
        .collect::<io::Result<Vec<_>>>()?;

    Ok(FbrSession { session, workers })
}

/// Per-worker request loop.
fn serve(server: &Server<KisekiFsBackend>, mut channel: FuseChannel) {
    loop {
        match channel.get_request() {
            Ok(Some((reader, writer))) => {
                if let Err(e) = server.handle_message(reader, writer.into(), None, None) {
                    error!("fuse: handle_message error: {e}");
                }
            }
            // `None` means the exit event fired (unmount); the device error path
            // ends the worker as well.
            Ok(None) => break,
            Err(e) => {
                error!("fuse: get_request error: {e}");
                break;
            }
        }
    }
}
