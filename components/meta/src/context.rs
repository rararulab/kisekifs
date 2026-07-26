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

use std::{sync::LazyLock, time::Instant};

use kiseki_types::attr::InodeAttr;
use snafu::ensure;
use tokio_util::sync::CancellationToken;

use crate::err::{LibcSnafu, Result};

pub static EMPTY_CONTEXT: LazyLock<FuseContext> = LazyLock::new(FuseContext::background);

#[derive(Debug, Clone)]
pub struct FuseContext {
    pub unique:             u64,
    pub gid:                u32,
    pub gid_list:           Vec<u32>,
    pub uid:                u32,
    pub pid:                u32,
    pub check_permission:   bool,
    pub start_at:           Instant,
    pub cancellation_token: CancellationToken,
}

impl<'a> From<&'a kiseki_types::Request<'a>> for FuseContext {
    fn from(req: &'a kiseki_types::Request) -> Self {
        Self::from_identity(req.unique(), req.uid(), req.gid(), req.pid())
    }
}

impl FuseContext {
    /// Build a context from a caller identity without a FUSE request `unique`
    /// (used by the fuse-backend-rs path, whose `Context` carries only
    /// uid/gid/pid). The `unique` is set to 0.
    pub fn from_uid_gid_pid(uid: u32, gid: u32, pid: u32) -> Self {
        Self::from_identity(0, uid, gid, pid)
    }

    fn from_identity(unique: u64, uid: u32, gid: u32, pid: u32) -> Self {
        Self {
            unique,
            gid,
            // FUSE supplies the request's primary gid. Supplementary groups are
            // not available here, so permission checks deliberately use the
            // primary group instead of silently falling back to "other" bits.
            gid_list: vec![gid],
            uid,
            pid,
            check_permission: true,
            start_at: Instant::now(),
            cancellation_token: CancellationToken::new(),
        }
    }

    // Access checks the access permission on given inode.
    pub fn check_access(&self, attr: &InodeAttr, perm_mask: u8) -> Result<()> {
        if self.uid == 0 {
            return Ok(());
        }
        if !self.check_permission {
            return Ok(());
        }

        let mode = attr.access_mode(self.uid, &self.gid_list);
        // This condition checks if all the bits set in mmask (requested permissions)
        // are also set in mode (file's permissions).
        //
        // perm = 0o644 (rw-r--r--)
        // perm_mask = 0o4 (read permission)
        // perm & perm_mask = 0o4 (read permission is granted)
        ensure!(
            mode & perm_mask == perm_mask,
            LibcSnafu {
                errno: libc::EACCES,
            }
        );
        Ok(())
    }

    pub fn is_cancelled(&self) -> bool { self.cancellation_token.is_cancelled() }
}

impl FuseContext {
    #[allow(dead_code)]
    pub fn background() -> Self {
        Self {
            unique:             0,
            gid:                2,
            gid_list:           vec![2, 3],
            uid:                1,
            pid:                10,
            check_permission:   true,
            start_at:           Instant::now(),
            cancellation_token: CancellationToken::new(),
        }
    }

    pub fn contains_gid(&self, gid: u32) -> bool { self.gid_list.contains(&gid) }
}

#[cfg(test)]
mod tests {
    use kiseki_types::attr::InodeAttr;

    use super::FuseContext;

    #[test]
    fn primary_gid_participates_in_access_checks() {
        let ctx = FuseContext::from_identity(1, 1000, 2000, 42);
        let attr = InodeAttr {
            uid: 3000,
            gid: 2000,
            mode: 0o040,
            ..InodeAttr::default()
        };

        assert!(ctx.check_access(&attr, 0o4).is_ok());
        assert!(ctx.contains_gid(2000));
    }
}
