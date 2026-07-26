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

use std::time::{Duration, SystemTime};

use bitflags::bitflags;
use fuser::{FileAttr, FileType};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::ino::{Ino, ROOT_INO};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SetAttrFlags(pub u32);

bitflags! {
    impl SetAttrFlags: u32 {
        const MODE = 1 << 0;
        const UID = 1 << 1;
        const GID = 1 << 2;
        const SIZE = 1 << 3;
        const ATIME = 1 << 4;
        const MTIME = 1 << 5;
        const CTIME = 1 << 6;
        const ATIME_NOW = 1 << 7;
        const MTIME_NOW = 1 << 8;
        const FLAG = 1 << 15;
    }
}

bitflags! {
    #[derive(Debug, Clone, Copy, Eq, PartialEq)]
    pub struct Flags: u8 {
        const IMMUTABLE = 0x01;
        const APPEND = 0x02;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InodeAttr {
    /// Flags (macOS only, see chflags(2))
    pub flags:      u32,
    /// Kind of file (directory, file, pipe, etc)
    pub kind:       FileType,
    /// permission mode
    pub mode:       u32,
    /// owner id
    pub uid:        u32,
    /// group id of owner
    pub gid:        u32,
    /// device number
    pub rdev:       u32,
    /// Time of last access
    pub atime:      SystemTime,
    /// Time of last modification
    pub mtime:      SystemTime,
    /// Time of last change
    pub ctime:      SystemTime,
    /// Time of creation (macOS only)
    pub crtime:     SystemTime,
    /// Number of hard links
    pub nlink:      u32,
    /// length of regular file
    pub length:     u64,
    /// inode of parent; 0 means tracked by parentKey (for hardlinks)
    pub parent:     Ino,
    // whether to keep the cached page or not
    pub keep_cache: bool,
}

// Setter
impl InodeAttr {
    pub const fn set_flags(&mut self, flags: u32) -> &mut Self {
        self.flags = flags;
        self
    }

    pub const fn set_mode(&mut self, perm: u32) -> &mut Self {
        self.mode = perm;
        self
    }

    pub const fn set_kind(&mut self, kind: fuser::FileType) -> &mut Self {
        self.kind = kind;
        self
    }

    pub const fn set_nlink(&mut self, nlink: u32) -> &mut Self {
        self.nlink = nlink;
        self
    }

    pub const fn set_length(&mut self, length: u64) -> &mut Self {
        self.length = length;
        self
    }

    pub const fn set_rdev(&mut self, rdev: u32) -> &mut Self {
        self.rdev = rdev;
        self
    }

    pub const fn set_gid(&mut self, gid: u32) -> &mut Self {
        self.gid = gid;
        self
    }

    pub const fn set_uid(&mut self, uid: u32) -> &mut Self {
        self.uid = uid;
        self
    }

    pub const fn set_parent(&mut self, parent: Ino) -> &mut Self {
        self.parent = parent;
        self
    }

    pub const fn set_atime(&mut self, t: SystemTime) -> &mut Self {
        self.atime = t;
        self
    }

    pub const fn set_mtime(&mut self, t: SystemTime) -> &mut Self {
        self.mtime = t;
        self
    }

    pub const fn set_ctime(&mut self, t: SystemTime) -> &mut Self {
        self.ctime = t;
        self
    }
}

// Getter
impl InodeAttr {
    pub const fn get_filetype(&self) -> FileType { self.kind }

    /// Providing default values guarantees for some critical inode,
    /// makes them always available, even under slow or unreliable conditions.
    pub const fn hard_code_inode_attr(is_trash: bool) -> Self {
        Self {
            flags:      0,
            kind:       FileType::Directory,
            mode:       if is_trash { 0o555 } else { 0o777 },
            uid:        0,
            gid:        0,
            rdev:       0,
            atime:      SystemTime::UNIX_EPOCH,
            mtime:      SystemTime::UNIX_EPOCH,
            ctime:      SystemTime::UNIX_EPOCH,
            crtime:     SystemTime::UNIX_EPOCH,
            nlink:      2,
            length:     4 << 10,
            parent:     ROOT_INO,
            keep_cache: false,
        }
    }

    pub const fn empty() -> Self {
        Self {
            flags:      0,
            kind:       FileType::Directory,
            mode:       0,
            uid:        0,
            gid:        0,
            rdev:       0,
            atime:      SystemTime::UNIX_EPOCH,
            mtime:      SystemTime::UNIX_EPOCH,
            ctime:      SystemTime::UNIX_EPOCH,
            crtime:     SystemTime::UNIX_EPOCH,
            nlink:      0,
            length:     0,
            parent:     ROOT_INO,
            keep_cache: false,
        }
    }

    pub const fn keep_cache(&mut self) -> &mut Self {
        self.keep_cache = true;
        self
    }

    pub fn update_length(&mut self, length: u64) {
        self.length = length;
        self.update_modification_time();
    }

    pub fn update_modification_time(&mut self) {
        self.mtime = SystemTime::now();
        self.ctime = SystemTime::now();
    }

    pub const fn update_modification_time_with(&mut self, time: SystemTime) {
        self.mtime = time;
        self.ctime = time;
    }

    /// Checks whether the modification time needs updating according to
    /// `skip_update_duration`.
    pub fn update_modification_time_if(
        &mut self,
        now: SystemTime,
        skip_update_duration: Duration,
    ) -> bool {
        // A future mtime (clock skew, or a setattr-set timestamp) makes
        // duration_since err; treat it as "just modified" so we skip the
        // update instead of panicking.
        if now.duration_since(self.mtime).unwrap_or(Duration::ZERO) > skip_update_duration {
            self.mtime = now;
            self.ctime = now;
            true
        } else {
            false
        }
    }

    // Enforces different access levels for owner, group, and others.
    // Grants full access to the root user.
    // Determines access based on user and group IDs.
    pub fn access_mode(&self, uid: u32, gids: &[u32]) -> u8 {
        if uid == 0 {
            // If uid is 0 (root user), returns 0x7 (full access) unconditionally.
            return 0x7;
        }
        let perm = self.mode;
        if uid == self.uid {
            // If uid matches attr.Uid (file owner),
            // extracts owner permissions by shifting mode 6 bits to the right and masking
            // with 7, returning a value like 4 (read-only),
            // 6 (read-write), or 7 (read-write-execute).
            return (perm >> 6) as u8 & 7;
        }
        // If any gid matches attr.Gid (file group),
        // extracts group permissions by shifting mode 3 bits to the right and masking
        // with 7.
        if gids.contains(&self.gid) {
            return (perm >> 3) as u8 & 7;
        }
        // If no previous conditions match,
        // returns other permissions by masking mode with 7.
        (perm & 7) as u8
    }

    pub fn to_fuse_attr<I: Into<u64>>(&self, ino: I) -> fuser::FileAttr {
        let inode = ino.into();
        info!("ino: {inode}, to_fuse_attr: {:?}", self);
        let mut fa = FileAttr {
            ino:     inode,
            size:    0,
            blocks:  0,
            atime:   self.atime,
            mtime:   self.mtime,
            ctime:   self.ctime,
            crtime:  self.crtime,
            kind:    self.kind,
            // POSIX-compliant file systems typically represent file types and permissions together
            // in a single attribute, known as the mode. This is a widely recognized convention
            // that aligns with how file attributes are managed in traditional UNIX-like operating
            // systems
            perm:    make_smode(&self.kind, self.mode),
            nlink:   self.nlink,
            uid:     self.uid,
            gid:     self.gid,
            rdev:    self.rdev,
            blksize: 0x10000,
            flags:   self.flags,
        };

        match fa.kind {
            FileType::Directory | FileType::Symlink | FileType::RegularFile => {
                fa.size = self.length;
                fa.blocks = fa.size.div_ceil(512);
            }
            FileType::BlockDevice | FileType::CharDevice => {
                fa.rdev = self.rdev;
            }
            _ => {
                // Handle other types if needed
            }
        }

        fa
    }
}

// Checker
impl InodeAttr {
    pub fn is_filetype(&self, typ: FileType) -> bool { self.kind == typ }

    pub fn is_dir(&self) -> bool { self.kind == FileType::Directory }

    pub fn is_file(&self) -> bool { self.kind == FileType::RegularFile }

    pub const fn is_immutable(&self) -> bool {
        let flag = Flags::from_bits_truncate(self.flags as u8);
        flag.contains(Flags::IMMUTABLE)
    }

    pub const fn is_append_only(&self) -> bool {
        let flag = Flags::from_bits_truncate(self.flags as u8);
        flag.contains(Flags::APPEND)
    }

    pub const fn is_normal(&self) -> bool {
        let flag = Flags::from_bits_truncate(self.flags as u8);
        let contains = flag.contains(Flags::IMMUTABLE) || flag.contains(Flags::APPEND);
        !contains
    }
}

const fn get_mode_t_from_filetype(kind: &FileType) -> libc::mode_t {
    match kind {
        FileType::Directory => libc::S_IFDIR,
        FileType::RegularFile => libc::S_IFREG,
        FileType::Symlink => libc::S_IFLNK,
        FileType::BlockDevice => libc::S_IFBLK,
        FileType::CharDevice => libc::S_IFCHR,
        FileType::NamedPipe => libc::S_IFIFO,
        FileType::Socket => libc::S_IFSOCK,
    }
}

const fn make_smode(kind: &FileType, perm: u32) -> u16 {
    // libc::mode_t is u16 on macOS but u32 on Linux; widen both to u32 for the
    // OR, then narrow to the u16 FUSE perm field (valid modes fit in 16 bits).
    let mode = get_mode_t_from_filetype(kind) as u32;
    (mode | perm) as u16
}

impl Default for InodeAttr {
    fn default() -> Self {
        let now = SystemTime::now();
        Self {
            atime:      now,
            mtime:      now,
            ctime:      now,
            crtime:     now,
            kind:       FileType::RegularFile,
            mode:       0,
            nlink:      1,
            length:     0,
            parent:     Ino::default(),
            uid:        0,
            gid:        0,
            rdev:       0,
            flags:      0,
            keep_cache: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attr_modify() {
        let mut attr = InodeAttr::default()
            .set_mode(0o777)
            .set_kind(FileType::Directory)
            .set_gid(11)
            .set_uid(22)
            .to_owned();
        attr.set_parent(Ino::from(1));

        assert_eq!(attr.mode, 0o777);
        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(attr.gid, 11);
        assert_eq!(attr.uid, 22);
        assert_eq!(attr.parent, Ino::from(1));

        let mode = 0o2777u32;
        let mode_u16 = mode as u16;
        assert_eq!(mode_u16, 0o2777u16);
    }

    #[test]
    fn smode() {
        let mode = get_mode_t_from_filetype(&FileType::Directory);
        let new_mode = mode | 0o777;
        let new_mode_u16 = new_mode as u16;
        println!("{new_mode:?}, {new_mode_u16:?}");

        let x = libc::S_ISUID;
        let x2 = 0o1000;
        println!("{x:x}, {x2:x}");
    }
}
