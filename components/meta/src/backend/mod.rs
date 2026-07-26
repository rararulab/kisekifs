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

use std::{str::FromStr, sync::Arc, time::Duration};

use bytes::Bytes;
use kiseki_common::ChunkIndex;
use kiseki_types::{
    FileType, attr::InodeAttr, entry::DEntry, ino::Ino, setting::Format, slice::Slices,
    stat::DirStat,
};
use snafu::ensure;
use strum_macros::EnumString;
use tracing::debug;

use crate::{backend::key::Counter, context::FuseContext, err::Result};

pub mod key;
#[cfg(feature = "meta-rocksdb")]
mod rocksdb;
#[cfg(feature = "meta-rocksdb")]
mod rocksdb_metrics;

use crate::{engine::RenameFlags, err::UnsupportedMetaDSNSnafu, open_files::OpenFilesRef};

// TODO: optimize me
pub fn open_backend(dsn: &str, skip_dir_mtime: Duration) -> Result<BackendRef> {
    let x = dsn.splitn(2, "://:").collect::<Vec<_>>();
    ensure!(x.len() == 2, UnsupportedMetaDSNSnafu { dsn });
    let backend_kind = x[0];
    let path = x[1];

    let backend = BackendKinds::from_str(backend_kind).expect("unsupported backend kind");
    backend.build(path, skip_dir_mtime)
}

#[derive(Debug, EnumString)]
enum BackendKinds {
    #[cfg(feature = "meta-rocksdb")]
    #[strum(serialize = "rocksdb", serialize = "Rocksdb")]
    Rocksdb,
}

impl BackendKinds {
    fn build(&self, path: &str, skip_dir_mtime: Duration) -> Result<BackendRef> {
        match self {
            #[cfg(feature = "meta-rocksdb")]
            Self::Rocksdb => {
                let mut builder = rocksdb::Builder::default();
                builder.with_path(path).with_skip_dir_mtime(skip_dir_mtime);
                debug!("backend [rocksdb] is built with path: {}", path);
                builder.build()
            }
            // Only reachable when no backend feature is enabled.
            #[cfg(not(feature = "meta-rocksdb"))]
            _ => unimplemented!("unsupported backend"),
        }
    }
}

pub type BackendRef = Arc<dyn Backend>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SliceCommitResult {
    pub grew_by:     u64,
    pub slice_count: usize,
    pub inserted:    bool,
}

#[allow(clippy::too_many_arguments)]
#[async_trait::async_trait]
pub trait Backend: Send + Sync {
    fn initialize_volume(&self, format: &Format, root: &InodeAttr) -> Result<()>;
    fn set_format(&self, format: &Format) -> Result<()>;
    fn load_format(&self) -> Result<Format>;

    fn increase_count_by(&self, counter: Counter, step: usize) -> Result<u64>;
    fn load_count(&self, counter: Counter) -> Result<u64>;

    fn get_attr(&self, inode: Ino) -> Result<InodeAttr>;
    fn set_attr(&self, inode: Ino, attr: &InodeAttr) -> Result<()>;

    fn get_dentry(&self, parent: Ino, name: &str) -> Result<DEntry>;
    fn set_dentry(&self, parent: Ino, name: &str, inode: Ino, typ: FileType) -> Result<()>;
    fn list_dentry(&self, parent: Ino, limit: i64) -> Result<Vec<DEntry>>;

    fn set_symlink(&self, inode: Ino, path: String) -> Result<()>;
    fn get_symlink(&self, inode: Ino) -> Result<String>;

    fn set_chunk_slices(&self, inode: Ino, chunk_index: ChunkIndex, slices: Slices) -> Result<()>;
    fn set_raw_chunk_slices(&self, inode: Ino, chunk_index: ChunkIndex, buf: Vec<u8>)
    -> Result<()>;
    fn get_raw_chunk_slices(&self, inode: Ino, chunk_index: ChunkIndex) -> Result<Option<Vec<u8>>>;
    fn get_chunk_slices(&self, inode: Ino, chunk_index: ChunkIndex) -> Result<Slices>;
    fn commit_slice(
        &self,
        inode: Ino,
        chunk_index: ChunkIndex,
        slice: &kiseki_types::slice::Slice,
    ) -> Result<SliceCommitResult>;

    fn set_dir_stat(&self, inode: Ino, dir_stat: DirStat) -> Result<()>;
    fn get_dir_stat(&self, inode: Ino) -> Result<DirStat>;

    /// [`Self::do_mknod`] creates a node in a directory with given name, type
    /// and permissions.
    fn do_mknod(
        &self,
        ctx: Arc<FuseContext>,
        new_inode: Ino,
        new_inode_attr: InodeAttr,
        parent: Ino,
        name: &str,
        typ: FileType,
        path: String,
    ) -> Result<(Ino, InodeAttr)>;

    /// [`Self::do_rmdir`] removes a directory from the filesystem. The
    /// directory must be empty. return the removed directory entry and its
    /// attribute
    fn do_rmdir(
        &self,
        ctx: Arc<FuseContext>,
        parent: Ino,
        name: &str,
        // skip updating attribute of a directory if the mtime difference is smaller
        // than this value
        skip_dir_mtime: Duration,
    ) -> Result<(DEntry, InodeAttr)>;

    /// [`Self::do_truncate`] changes the length of the given file.
    fn do_truncate(
        &self,
        ctx: Arc<FuseContext>,
        inode: Ino,
        length: u64,
        skip_perm_check: bool,
    ) -> Result<InodeAttr>;

    /// [`Self::do_link`] creates an entry for the inode and returns the new
    /// [InodeAttr]. Creating another directory entry (filename) that points
    /// directly to the same inode as the original file.
    fn do_link(
        &self,
        ctx: Arc<FuseContext>,
        inode: Ino,
        new_parent: Ino,
        new_name: &str,
    ) -> Result<InodeAttr>;

    /// [`Self::do_unlink`] removes a file entry from a directory.
    /// return the freed space size and inode count.
    async fn do_unlink(
        &self,
        ctx: Arc<FuseContext>,
        parent: Ino,
        name: String,
        session_id: u64,
        open_files_ref: OpenFilesRef,
    ) -> Result<UnlinkResult>;

    /// `do_delete_chunks` tries to delete all free slices of a file;
    /// free means that slice is not been borrowed.
    fn do_delete_chunks(&self, inode: Ino);

    async fn do_rename(
        &self,
        ctx: Arc<FuseContext>,
        session_id: u64,
        old_parent: Ino,
        old_name: &str,
        new_parent: Ino,
        new_name: &str,
        flags: RenameFlags,
        open_files_ref: OpenFilesRef,
    ) -> Result<RenameResult>;

    fn do_readlink(&self, inode: Ino) -> Result<Bytes>;
}

pub struct UnlinkResult {
    // The inode of the file
    pub inode:       Ino,
    // the removed inode attr, if the file is removed
    pub removed:     Option<InodeAttr>,
    // the freed space size
    pub freed_space: u64,
    // the freed inode count
    pub freed_inode: u64,
    // whether the file is opened while we're trying to unlink
    pub is_opened:   bool,
}

pub struct RenameResult {
    // may need to delete the replaced file
    pub need_delete: Option<(Ino, bool)>,
    pub freed_space: u64,
    pub freed_inode: u64,
}
