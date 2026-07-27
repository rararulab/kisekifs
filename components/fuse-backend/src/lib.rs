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

//! The `fuse-backend-rs` FUSE frontend for KisekiFS (issue #71).
//!
//! Production-target replacement for the `fuser`-based `kiseki-fuse` crate; the
//! two coexist during migration so behaviour can be compared side by side.
//!
//! ## Runtime model
//!
//! [`mount`] returns an [`FbrSession`] holding a
//! [`fuse_backend_rs::transport::FuseSession`] plus a pool of worker threads,
//! each looping on `FuseChannel::get_request` + `Server::handle_message`. That
//! path dispatches to the *synchronous*
//! [`fuse_backend_rs::api::filesystem::FileSystem`] trait, implemented by
//! [`KisekiFsBackend`], which bridges to the async KisekiFS VFS on its own
//! Tokio runtime. The fusedev workers are plain OS threads with no runtime
//! entered, so `block_on` does not panic, and N workers give N concurrent
//! in-flight requests.
//!
//! (fuse-backend-rs 0.14's async fusedev task `FuseDevTask` is gated behind a
//! non-existent `async_io` feature and is dead code, so the async server has no
//! usable driver; the sync worker-pool path is the production one.)
//!
//! ## Layout
//!
//! * [`convert`] — KisekiFS ↔ libc/ABI type conversions.
//! * `fs` — the `FileSystem` implementation ([`KisekiFsBackend`]).
//! * `session` — mount + worker-thread pool ([`mount`], [`FbrSession`]).

mod convert;
mod fs;
mod session;

pub use fs::KisekiFsBackend;
pub use session::{FbrSession, mount};
