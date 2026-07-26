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

use fuser::FileType;
use kiseki_common::{DOT, DOT_DOT};
use serde::{Deserialize, Serialize};

use crate::{
    attr::InodeAttr,
    ino::{Ino, ZERO_INO},
};

#[derive(Clone, Debug)]
pub enum Entry {
    Full(FullEntry),
    DEntry(DEntry),
}

/// lookup
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FullEntry {
    pub inode: Ino,
    pub name:  String,
    pub attr:  InodeAttr,
}

impl FullEntry {
    pub fn new(ino: Ino, name: &str, attr: InodeAttr) -> Self {
        Self {
            inode: ino,
            name: name.to_string(),
            attr,
        }
    }

    pub fn to_fuse_attr<I: Into<u64>>(&self, ino: I) -> fuser::FileAttr {
        self.attr.to_fuse_attr(ino)
    }
}

/// DEntry represents the storage structure of an entry in a directory.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct DEntry {
    pub parent: Ino,
    pub name:   String,
    pub inode:  Ino,
    pub typ:    FileType,
}

impl Entry {
    pub fn new_basic_entry_pair(ino: Ino, parent: Ino) -> Vec<Self> {
        vec![
            Self::DEntry(DEntry {
                parent: ZERO_INO,
                name:   DOT.to_string(),
                inode:  ino,
                typ:    FileType::Directory,
            }),
            Self::DEntry(DEntry {
                parent: ZERO_INO,
                name:   DOT_DOT.to_string(),
                inode:  parent,
                typ:    FileType::Directory,
            }),
        ]
    }

    pub const fn get_inode(&self) -> Ino {
        match self {
            Self::Full(e) => e.inode,
            Self::DEntry(e) => e.inode,
        }
    }

    pub fn is_file(&self) -> bool {
        match self {
            Self::Full(e) => e.attr.kind == FileType::RegularFile,
            Self::DEntry(e) => e.typ == FileType::RegularFile,
        }
    }

    pub const fn get_file_type(&self) -> FileType {
        match self {
            Self::Full(e) => e.attr.kind,
            Self::DEntry(e) => e.typ,
        }
    }

    pub fn get_name(&self) -> &str {
        match self {
            Self::Full(e) => &e.name,
            Self::DEntry(e) => &e.name,
        }
    }
}
