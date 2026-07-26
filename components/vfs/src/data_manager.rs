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

use std::{collections::HashMap, sync::Arc, time::SystemTime};

use kiseki_common::FH;
use kiseki_meta::MetaEngineRef;
use kiseki_storage::{
    cache,
    cache::{
        file_cache::{FileCache, FileCacheRef},
        mem_cache::{MemCache, MemCacheRef},
    },
};
use kiseki_types::ino::Ino;
use kiseki_utils::object_storage::ObjectStorage;
use tokio::sync::RwLock;
use tracing::debug;

use crate::{
    err::Result,
    reader::FileReader,
    writer::{FileWriter, FileWritersRef},
};

pub(crate) type DataManagerRef = Arc<DataManager>;
type FileReadersByHandle = HashMap<FH, Arc<FileReader>>;
type FileReaderTable = HashMap<Ino, Arc<RwLock<FileReadersByHandle>>>;

/// DataManager is responsible for managing the data of the VFS.
pub(crate) struct DataManager {
    pub(crate) chunk_size:   usize,
    pub(crate) file_writers: FileWritersRef,
    pub(crate) file_readers: RwLock<FileReaderTable>,
    pub(crate) id_generator: Arc<sonyflake::Sonyflake>,
    // Dependencies
    pub(crate) meta_engine:  MetaEngineRef,
    pub(crate) file_cache:   FileCacheRef,
    pub(crate) mem_cache:    MemCacheRef,
}

impl DataManager {
    pub(crate) fn new(
        chunk_size: usize,
        meta_engine_ref: MetaEngineRef,
        object_storage: ObjectStorage,
        file_cache_config: cache::file_cache::Config,
    ) -> Result<Self> {
        let file_cache = Arc::new(FileCache::new(file_cache_config, object_storage.clone())?);
        let mem_cache = Arc::new(MemCache::new(
            cache::mem_cache::Config::default(),
            object_storage,
        ));

        Ok(Self {
            chunk_size,
            file_writers: Arc::new(Default::default()),
            file_readers: Default::default(),
            id_generator: Arc::new(
                sonyflake::Sonyflake::new()
                    .expect("failed to initialize the data manager id generator"),
            ),
            meta_engine: meta_engine_ref,
            file_cache,
            mem_cache,
        })
    }

    #[allow(dead_code)] // only exercised by tests so far
    pub(crate) fn find_file_writer(&self, ino: Ino) -> Option<Arc<FileWriter>> {
        self.file_writers.get(&ino).map(|r| r.value().clone())
    }

    pub(crate) fn get_length(&self, ino: Ino) -> u64 {
        self.file_writers
            .get(&ino)
            .map_or(0, |w| w.value().get_length() as u64)
    }

    pub(crate) fn set_length(&self, ino: Ino, length: usize) {
        if let Some(writer) = self.file_writers.get(&ino) {
            writer.set_length(length);
        }
    }

    pub(crate) fn update_mtime(&self, ino: Ino, mtime: SystemTime) -> Result<()> {
        debug!("update_mtime do nothing, {ino}: {:?}", mtime);
        Ok(())
    }
}
