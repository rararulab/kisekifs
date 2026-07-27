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

//! Conversions between KisekiFS types and the libc/fuse-backend-rs ABI types.

use std::{
    io,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use fuse_backend_rs::abi::fuse_abi::stat64;
use kiseki_types::{FileType, ToErrno, attr::InodeAttr};

/// Map any KisekiFS error carrying an errno into an `io::Error`.
pub(crate) fn to_io<E: ToErrno>(e: E) -> io::Error { io::Error::from_raw_os_error(e.to_errno()) }

pub(crate) fn errno_io(errno: i32) -> io::Error { io::Error::from_raw_os_error(errno) }

/// Zero-initialise a POD C struct. Only for `#[repr(C)]` libc types whose
/// all-zero bit pattern is valid (`stat64`, `statvfs64`).
#[allow(unsafe_code)]
pub(crate) const fn zeroed_pod<T>() -> T {
    // SAFETY: callers restrict `T` to POD C structs where all-zero is valid.
    unsafe { std::mem::zeroed() }
}

pub(crate) const fn file_type_bits(kind: FileType) -> u32 {
    match kind {
        FileType::NamedPipe => libc::S_IFIFO,
        FileType::CharDevice => libc::S_IFCHR,
        FileType::BlockDevice => libc::S_IFBLK,
        FileType::Directory => libc::S_IFDIR,
        FileType::RegularFile => libc::S_IFREG,
        FileType::Symlink => libc::S_IFLNK,
        FileType::Socket => libc::S_IFSOCK,
    }
}

pub(crate) const fn dir_entry_type(kind: FileType) -> u32 {
    let dt = match kind {
        FileType::NamedPipe => libc::DT_FIFO,
        FileType::CharDevice => libc::DT_CHR,
        FileType::Directory => libc::DT_DIR,
        FileType::BlockDevice => libc::DT_BLK,
        FileType::RegularFile => libc::DT_REG,
        FileType::Symlink => libc::DT_LNK,
        FileType::Socket => libc::DT_SOCK,
    };
    dt as u32
}

fn system_time_parts(t: SystemTime) -> (i64, i64) {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => (d.as_secs() as i64, d.subsec_nanos() as i64),
        Err(_) => (0, 0),
    }
}

pub(crate) fn system_time_from(secs: i64, nsecs: i64) -> SystemTime {
    UNIX_EPOCH + Duration::new(secs.max(0) as u64, nsecs.clamp(0, 999_999_999) as u32)
}

/// Convert a KisekiFS [`InodeAttr`] into a `libc::stat64`.
pub(crate) fn attr_to_stat64(attr: &InodeAttr, ino: u64) -> stat64 {
    let mut st: stat64 = zeroed_pod();
    st.st_ino = ino;
    st.st_mode = file_type_bits(attr.kind) | (attr.mode & 0o7777);
    st.st_nlink = attr.nlink as libc::nlink_t;
    st.st_uid = attr.uid;
    st.st_gid = attr.gid;
    st.st_rdev = attr.rdev as libc::dev_t;
    st.st_blksize = 0x10000;
    match attr.kind {
        FileType::Directory | FileType::Symlink | FileType::RegularFile => {
            st.st_size = attr.length as libc::off64_t;
            st.st_blocks = attr.length.div_ceil(512) as libc::blkcnt64_t;
        }
        _ => {}
    }
    let (atime, atime_n) = system_time_parts(attr.atime);
    let (mtime, mtime_n) = system_time_parts(attr.mtime);
    let (ctime, ctime_n) = system_time_parts(attr.ctime);
    st.st_atime = atime;
    st.st_atime_nsec = atime_n;
    st.st_mtime = mtime;
    st.st_mtime_nsec = mtime_n;
    st.st_ctime = ctime;
    st.st_ctime_nsec = ctime_n;
    st
}
