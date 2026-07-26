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

use std::sync::{Arc, atomic::Ordering};

use bytes::Bytes;
use kiseki_common::{FH, MAX_FILE_SIZE};
use kiseki_meta::context::FuseContext;
use kiseki_types::ino::Ino;
use libc::{EBADF, EFBIG, EINTR, EINVAL, ENOENT, ENOTSUP};
use snafu::{OptionExt, ResultExt, ensure};
use tokio::task::JoinHandle;
use tracing::{debug, instrument};

use super::{KisekiVFS, Opened};
use crate::err::{LibcSnafu, MetaSnafu, Result};

#[cfg(target_os = "macos")]
const UNLOCK_LOCK_TYPE: libc::c_int = libc::F_UNLCK as libc::c_int;
#[cfg(not(target_os = "macos"))]
const UNLOCK_LOCK_TYPE: libc::c_int = libc::F_UNLCK;

fn checked_io_range(offset: i64, length: usize) -> Result<(usize, usize)> {
    ensure!(offset >= 0, LibcSnafu { errno: EINVAL });
    let offset = usize::try_from(offset)
        .ok()
        .context(LibcSnafu { errno: EFBIG })?;
    let end = offset
        .checked_add(length)
        .context(LibcSnafu { errno: EFBIG })?;
    ensure!(end <= MAX_FILE_SIZE, LibcSnafu { errno: EFBIG });
    Ok((offset, end))
}

impl KisekiVFS {
    /// Open a file for I/O operations.
    ///
    /// This function opens a file and returns a file handle that can be used
    /// for subsequent read, write, and other I/O operations. It corresponds
    /// to the open(2) system call and the open operation in FUSE.
    ///
    /// # Arguments
    /// * `ctx`: `&FuseContext` - The FUSE context containing request metadata
    /// * `inode`: [Ino] - The inode number of the file to open
    /// * `flags`: [i32] - Open flags (O_RDONLY, O_WRONLY, O_RDWR, etc.)
    ///
    /// # Returns
    /// * `Result<Opened>` - Returns an `Opened` structure containing:
    ///   - `fh`: File handle identifier for subsequent operations
    ///   - `flags`: Open flags for the file handle
    ///   - `inode`: The inode number that was opened
    ///   - `attr`: Current file attributes
    ///
    /// # Errors
    /// This function may return errors if:
    /// - The inode does not exist (`ENOENT`)
    /// - Permission denied for the requested access mode (`EACCES`)
    /// - Too many files are already open (`EMFILE`, `ENFILE`)
    /// - The file is a directory and write access was requested (`EISDIR`)
    ///
    /// # Behavior
    /// - Updates file reader length before returning
    /// - Creates a new file handle in the handle table
    /// - Sets appropriate open flags (FOPEN_DIRECT_IO for special inodes,
    ///   FOPEN_KEEP_CACHE for cacheable files)
    /// - Delegates permission checking to the metadata engine
    ///
    /// # Examples
    /// ```ignore
    /// use libc::{O_CREAT, O_RDWR};
    ///
    /// let opened = vfs.open(&ctx, inode, O_RDWR).await?;
    /// println!("Opened file with handle: {}", opened.fh);
    /// ```
    ///
    /// # References
    /// * [open(2)](https://man7.org/linux/man-pages/man2/open.2.html)
    /// * [FUSE open](https://libfuse.github.io/doxygen/structfuse__operations.html#a14b98b31a494b4cb0479819871c2ddbd)
    pub async fn open(&self, ctx: &FuseContext, inode: Ino, flags: i32) -> Result<Opened> {
        debug!(
            "fs:open with ino {:?} flags {:#b} pid {:?}",
            inode, flags, ctx.pid
        );

        let mut attr = if inode.is_special() {
            self.internal_nodes
                .get_internal_node(inode)
                .map(kiseki_types::internal_nodes::InternalNode::get_attr)
                .context(LibcSnafu { errno: ENOENT })?
        } else {
            self.meta
                .open_inode(ctx, inode, flags)
                .await
                .context(MetaSnafu)?
        };
        self.try_update_file_reader_length(inode, &mut attr).await;
        let opened_fh = self
            .handle_table
            .new_file_handle(inode, attr.length, flags)
            .await?;

        let opened_flags = if inode.is_special() {
            fuser::consts::FOPEN_DIRECT_IO
        } else if attr.keep_cache {
            fuser::consts::FOPEN_KEEP_CACHE
        } else {
            0
        };

        Ok(Opened {
            fh: opened_fh,
            flags: opened_flags,
            inode,
            attr,
        })
    }

    /// Read data from a file.
    ///
    /// This function reads data from an open file at the specified offset.
    /// It corresponds to the read(2) and pread(2) system calls and the
    /// read operation in FUSE.
    ///
    /// # Arguments
    /// * `ctx`: `Arc<FuseContext>` - The FUSE context containing request
    ///   metadata
    /// * `ino`: [Ino] - The inode number of the file to read from
    /// * `fh`: [u64] - File handle returned by open operation
    /// * `offset`: [i64] - Byte offset within the file to start reading from
    /// * `size`: [u32] - Number of bytes to read
    /// * `_flags`: [i32] - Read flags (currently unused)
    /// * `_lock`: `Option<u64>` - File lock information (currently unused)
    ///
    /// # Returns
    /// * `Result<Bytes>` - Returns the read data as a `Bytes` object. The
    ///   actual number of bytes read may be less than requested if EOF is
    ///   reached.
    ///
    /// # Errors
    /// This function may return errors if:
    /// - The file handle is invalid (`EBADF`)
    /// - The inode does not exist (`ENOENT`)
    /// - Permission denied for read access (`EACCES`)
    /// - The offset + size would exceed maximum file size (`EFBIG`)
    /// - The operation was interrupted (`EINTR`)
    /// - I/O error occurred during read (`EIO`)
    ///
    /// # Behavior
    /// - Special inode reads return `ENOTSUP`
    /// - Flushes any pending writes before reading to ensure consistency
    /// - Uses file handle locking to coordinate with concurrent operations
    /// - Reads through the data manager for caching and buffering
    ///
    /// # Examples
    /// ```ignore
    /// let data = vfs.read(ctx.clone(), inode, fh, 0, 1024, 0, None).await?;
    /// println!("Read {} bytes", data.len());
    /// ```
    ///
    /// # References
    /// * [read(2)](https://man7.org/linux/man-pages/man2/read.2.html)
    /// * [pread(2)](https://man7.org/linux/man-pages/man2/pread.2.html)
    /// * [FUSE read](https://libfuse.github.io/doxygen/structfuse__operations.html#a2a1c6b56565c0bb1b7e5a0d5b36b2d0b)
    #[allow(clippy::too_many_arguments)]
    pub async fn read(
        &self,
        ctx: Arc<FuseContext>,
        ino: Ino,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock: Option<u64>,
    ) -> Result<Bytes> {
        debug!(
            "fs:read with ino {:?} fh {:?} offset {:?} size {:?}",
            ino, fh, offset, size
        );

        let size = size as usize;
        let (offset, _) = checked_io_range(offset, size)?;

        let handle = self
            .handle_table
            .find_handle(ino, fh)
            .await
            .context(LibcSnafu { errno: EBADF })?;
        let file_handle = handle
            .as_file_handle()
            .context(LibcSnafu { errno: EBADF })?;
        if ino.is_special() {
            return LibcSnafu { errno: ENOTSUP }.fail();
        }

        let mut buf = vec![0u8; size];
        let read_guard = file_handle
            .read_lock(ctx.clone())
            .await
            .context(LibcSnafu { errno: EINTR })?; // failed to lock

        self.data_manager.direct_flush(ino).await?;

        let read_result = read_guard.read(offset, buf.as_mut_slice()).await;
        file_handle.remove_operation(&ctx).await;
        let read_len = read_result?;
        buf.truncate(read_len);
        debug!(
            "vfs:read with ino {:?} fh {:?} offset {:?} expected_read_size {:?} actual_read_len: \
             {:?}",
            ino, fh, offset, size, read_len
        );
        Ok(Bytes::from(buf))
    }

    /// Write data to a file.
    ///
    /// This function writes data to an open file at the specified offset.
    /// It corresponds to the write(2) and pwrite(2) system calls and the
    /// write operation in FUSE.
    ///
    /// # Arguments
    /// * `ctx`: `Arc<FuseContext>` - The FUSE context containing request
    ///   metadata
    /// * `ino`: [Ino] - The inode number of the file to write to
    /// * `fh`: [u64] - File handle returned by open operation
    /// * `offset`: [i64] - Byte offset within the file to start writing at
    /// * `data`: [&[u8]] - The data to write to the file
    /// * `_write_flags`: [u32] - Write flags (currently unused)
    /// * `_flags`: [i32] - Additional flags (currently unused)
    /// * `_lock_owner`: `Option<u64>` - Lock owner information (currently
    ///   unused)
    ///
    /// # Returns
    /// * `Result<u32>` - Returns the number of bytes actually written
    ///
    /// # Errors
    /// This function may return errors if:
    /// - The file handle is invalid (`EBADF`)
    /// - Permission denied for write access (`EACCES`)
    /// - The file is opened read-only (`EBADF`)
    /// - The offset + data size would exceed maximum file size (`EFBIG`)
    /// - No space left on device (`ENOSPC`)
    /// - The operation was interrupted (`EINTR`)
    /// - I/O error occurred during write (`EIO`)
    ///
    /// # Behavior
    /// - Special inode writes return `ENOTSUP`
    /// - Uses file handle write locking to coordinate with concurrent
    ///   operations
    /// - Writes through the data manager for buffering and caching
    /// - Updates file reader length after write to maintain consistency
    /// - May extend the file size if writing beyond current EOF
    ///
    /// # Examples
    /// ```ignore
    /// let data = b"Hello, world!";
    /// let written = vfs
    ///     .write(ctx.clone(), inode, fh, 0, data, 0, 0, None)
    ///     .await?;
    /// println!("Wrote {} bytes", written);
    /// ```
    ///
    /// # References
    /// * [write(2)](https://man7.org/linux/man-pages/man2/write.2.html)
    /// * [pwrite(2)](https://man7.org/linux/man-pages/man2/pwrite.2.html)
    /// * [FUSE write](https://libfuse.github.io/doxygen/structfuse__operations.html#a897dd1b109e81c65b1ad7fc1e5de160a)
    #[allow(clippy::too_many_arguments)]
    pub async fn write(
        &self,
        ctx: Arc<FuseContext>,
        ino: Ino,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
    ) -> Result<u32> {
        let size = data.len();
        debug!(
            "fs:write with {:?} fh {:?} offset {:?} size {:?}",
            ino, fh, offset, size
        );

        let (offset, _) = checked_io_range(offset, size)?;

        let handle = self
            .handle_table
            .find_handle(ino, fh)
            .await
            .context(LibcSnafu { errno: EBADF })?;
        if ino.is_special() {
            return LibcSnafu { errno: ENOTSUP }.fail();
        }

        let handle = handle
            .as_file_handle()
            .context(LibcSnafu { errno: EBADF })?;

        let write_guard = handle
            .write_lock(ctx.clone())
            .await
            .context(LibcSnafu { errno: EINTR })?;
        let write_result = write_guard.write(offset, data).await;
        handle.remove_operation(&ctx).await;
        let write_len = write_result?;

        self.data_manager
            .truncate_reader(ino, write_guard.get_length() as u64)
            .await;

        Ok(write_len as u32)
    }

    /// Flush cached data for a file.
    ///
    /// This function flushes any cached data for an open file to the underlying
    /// storage. It's called when a file descriptor is closed or when an
    /// explicit flush is requested. This corresponds to the close(2) system
    /// call behavior and the flush operation in FUSE.
    ///
    /// # Arguments
    /// * `ctx`: `Arc<FuseContext>` - The FUSE context containing request
    ///   metadata
    /// * `ino`: [Ino] - The inode number of the file to flush
    /// * `fh`: [u64] - File handle to flush
    /// * `lock_owner`: [u64] - Lock owner identifier for POSIX lock management
    ///
    /// # Returns
    /// * [Result<()>] - Returns `Ok(())` on successful flush
    ///
    /// # Errors
    /// This function may return errors if:
    /// - The file handle is invalid (`EBADF`)
    /// - The inode does not exist (`ENOENT`)
    /// - The operation was interrupted while waiting for locks (`EINTR`)
    /// - I/O error occurred during flush (`EIO`)
    /// - Insufficient space to complete flush (`ENOSPC`)
    ///
    /// # Behavior
    /// - Special inodes are handled without flushing
    /// - Acquires write lock before flushing to ensure consistency
    /// - Handles operation cancellation gracefully
    /// - Manages POSIX file locks during flush operation
    /// - Cancels pending operations on interruption
    /// - Sets OFD (Open File Description) lock owner
    /// - Releases POSIX locks if held by the lock owner
    ///
    /// # Examples
    /// ```ignore
    /// vfs.flush(ctx.clone(), inode, fh, lock_owner).await?;
    /// println!("File flushed successfully");
    /// ```
    ///
    /// # References
    /// * [close(2)](https://man7.org/linux/man-pages/man2/close.2.html)
    /// * [FUSE flush](https://libfuse.github.io/doxygen/structfuse__operations.html#a5c2ff30b08d9e2ecf70b697ae0aeb949)
    #[instrument(skip(self), fields(ino, fh))]
    pub async fn flush(
        &self,
        ctx: Arc<FuseContext>,
        ino: Ino,
        fh: u64,
        lock_owner: u64,
    ) -> Result<()> {
        let h = self
            .handle_table
            .find_handle(ino, fh)
            .await
            .context(LibcSnafu { errno: ENOENT })?;
        let h = h.as_file_handle().context(LibcSnafu { errno: EBADF })?;
        if ino.is_special() {
            return Ok(());
        };

        if h.has_writer() {
            let guard = loop {
                match h.write_lock(ctx.clone()).await {
                    Some(guard) => break Some(guard),
                    None => {
                        if !ctx.is_cancelled() {
                            // as long as it's not cancelled, we should continue to wait
                            h.cancel_operations(&ctx.pid).await;
                            continue; // go back to acquire lock again.
                        } else {
                            // we get cancelled while waiting for the lock
                            break None;
                        }
                    }
                }
            };
            let guard = guard.context(LibcSnafu { errno: EINTR })?;
            guard.flush().await?;
            h.remove_operation(&ctx).await;
        } else {
            // still need to cancel
            h.cancel_operations(&ctx.pid).await;
        }

        h.try_set_ofd_owner(lock_owner);
        if h.locks.load(Ordering::Acquire) & 2 != 0 {
            self.meta
                .set_lk(
                    ctx,
                    ino,
                    lock_owner,
                    false,
                    UNLOCK_LOCK_TYPE,
                    0,
                    0x7FFFFFFFFFFFFFFF,
                )
                .await
                .context(MetaSnafu)?;
        }
        Ok(())
    }

    /// Synchronize file data to storage.
    ///
    /// This function forces a write of all cached data for the given file to
    /// the underlying storage device. It corresponds to the fsync(2) and
    /// fdatasync(2) system calls and the fsync operation in FUSE.
    ///
    /// # Arguments
    /// * `ctx`: `Arc<FuseContext>` - The FUSE context containing request
    ///   metadata
    /// * `ino`: [Ino] - The inode number of the file to synchronize
    /// * `fh`: [u64] - File handle to synchronize
    /// * `_data_sync`: [bool] - If true, sync only data (not metadata) like
    ///   fdatasync(2) (currently unused, always performs full sync)
    ///
    /// # Returns
    /// * [Result<()>] - Returns `Ok(())` on successful synchronization
    ///
    /// # Errors
    /// This function may return errors if:
    /// - The file handle is invalid (`EBADF`)
    /// - The inode does not exist (`ENOENT`)
    /// - The operation was interrupted while waiting for locks (`EINTR`)
    /// - I/O error occurred during synchronization (`EIO`)
    /// - Insufficient space to complete sync (`ENOSPC`)
    ///
    /// # Behavior
    /// - Special inodes return immediately without syncing
    /// - Acquires write lock to ensure exclusive access during sync
    /// - Flushes all buffered data through the data manager
    /// - Ensures data persistence on the underlying storage device
    /// - Coordinates with concurrent operations through file handle locking
    ///
    /// # Examples
    /// ```ignore
    /// vfs.fsync(ctx.clone(), inode, fh, false).await?;
    /// println!("File data synchronized to storage");
    /// ```
    ///
    /// # References
    /// * [fsync(2)](https://man7.org/linux/man-pages/man2/fsync.2.html)
    /// * [fdatasync(2)](https://man7.org/linux/man-pages/man2/fdatasync.2.html)
    /// * [FUSE fsync](https://libfuse.github.io/doxygen/structfuse__operations.html#a13a08af5ce7b5c4a5b1a5ad9ea1c4d8f)
    #[instrument(skip(self), fields(ino, fh))]
    pub async fn fsync(
        &self,
        ctx: Arc<FuseContext>,
        ino: Ino,
        fh: u64,
        _data_sync: bool,
    ) -> Result<()> {
        if ino.is_special() {
            return Ok(());
        }

        let handle = self
            .handle_table
            .find_handle(ino, fh)
            .await
            .context(LibcSnafu { errno: EBADF })?;
        if let Some(fh) = handle.as_file_handle() {
            let write_guard = fh
                .write_lock(ctx.clone())
                .await
                .context(LibcSnafu { errno: EINTR })?;

            write_guard.sync_remote().await?;
            fh.remove_operation(&ctx).await;
        }
        Ok(())
    }

    #[instrument(skip(self), fields(ino, fh))]
    pub async fn fallocate(
        &self,
        ctx: Arc<FuseContext>,
        inode: Ino,
        fh: FH,
        offset: i64,
        length: i64,
        mode: i32,
    ) -> Result<()> {
        ensure!(offset >= 0 && length > 0, LibcSnafu { errno: EINVAL });
        let length = usize::try_from(length)
            .ok()
            .context(LibcSnafu { errno: EFBIG })?;
        let (offset, _) = checked_io_range(offset, length)?;
        let mode = u8::try_from(mode)
            .ok()
            .context(LibcSnafu { errno: EINVAL })?;
        let h = self
            .handle_table
            .find_handle(inode, fh)
            .await
            .context(LibcSnafu { errno: EBADF })?;
        let _ = h.as_file_handle().context(LibcSnafu { errno: EBADF })?;
        if inode.is_special() {
            return LibcSnafu { errno: ENOTSUP }.fail();
        }
        self.meta.fallocate(inode, offset, length, mode).await?;
        Ok(())
    }

    pub async fn release(
        &self,
        ctx: Arc<FuseContext>,
        inode: Ino,
        fh: FH,
    ) -> Result<JoinHandle<()>> {
        if inode.is_special() {
            self.handle_table.release_file_handle(inode, fh).await;
            return Ok(tokio::spawn(async {}));
        }
        if let Some(handle) = self.handle_table.find_handle(inode, fh).await {
            handle.wait_all_operations_done(ctx.clone()).await?;
            if let Some(fh) = handle.as_file_handle() {
                if fh.has_writer() {
                    fh.unsafe_flush().await?;
                    self.invalidate_length(inode);
                }
                let (locks, fowner, powner) = fh.get_posix_lock_info();
                if locks & 1 != 0 {
                    self.meta
                        .flock(ctx.clone(), inode, fowner, UNLOCK_LOCK_TYPE)
                        .await?;
                }
                if locks & 2 != 0 && powner != 0 {
                    self.meta
                        .set_lk(
                            ctx,
                            inode,
                            powner,
                            false,
                            UNLOCK_LOCK_TYPE,
                            0,
                            0x7FFFFFFFFFFFFFFF,
                        )
                        .await?;
                }
            }
        }
        self.meta.close(inode).await?;
        let ht = self.handle_table.clone();
        Ok(tokio::spawn(async move {
            ht.release_file_handle(inode, fh).await;
        }))
    }
}

#[cfg(test)]
mod boundary_tests {
    use kiseki_types::ToErrno;

    use super::checked_io_range;

    #[test]
    fn checked_io_range_rejects_negative_offsets() {
        assert_eq!(
            checked_io_range(-1, 1).unwrap_err().to_errno(),
            libc::EINVAL
        );
    }

    #[test]
    fn checked_io_range_rejects_addition_overflow() {
        assert_eq!(
            checked_io_range(i64::MAX, usize::MAX)
                .unwrap_err()
                .to_errno(),
            libc::EFBIG
        );
    }
}
