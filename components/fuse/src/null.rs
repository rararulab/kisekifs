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

use std::path::Path;

use fuser::{Filesystem, MountOption, spawn_mount2};
use kiseki_common::KISEKI;
use snafu::{ResultExt, Whatever};

/// An empty FUSE file system. It can be used in a mounting test aimed to
/// determine whether or not the real file system can be mounted as well. If the
/// test fails, the application can fail early instead of wasting time
/// constructing the real file system.
struct NullFs {}

impl Filesystem for NullFs {}

pub fn mount_check<P: AsRef<Path>>(mountpoint: P) -> Result<(), Whatever> {
    let mountpoint = mountpoint.as_ref();
    let options = [MountOption::FSName(String::from(KISEKI))];
    let session = spawn_mount2(NullFs {}, mountpoint, &options).with_whatever_context(|e| {
        format!("failed to mount null fs on {}; {}", mountpoint.display(), e)
    })?;
    drop(session);

    Ok(())
}
