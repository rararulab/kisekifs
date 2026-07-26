# Linux filesystem support contract

This document defines KisekiFS support manifest version 1. It is deliberately a
bounded Linux contract, not a claim of full POSIX compliance. A behavior is
**supported** only when the real binary is exercised through the Linux kernel
and `/dev/fuse` by the required mounted test named in the table. **Unsupported**
means the kernel-visible errno is intentional and stable. **Experimental**
means the implementation exists but is not a release promise.

The machine-readable source of case IDs and statuses is
`tests/fixtures/posix-support.toml`; a unit test rejects duplicate IDs, invalid
statuses, documentation drift, and supported entries without a registered
mounted test.

## Supported operations

| Case ID | Operations and promise | Mounted test |
|---|---|---|
| `mount.smoke` | Mount, create, open, write, close, and clean unmount | `mounted::smoke::mount_create_unmount` |
| `namespace.create-remove` | Regular-file and directory creation/removal | `mounted::semantics::namespace_and_metadata` |
| `namespace.rename` | Rename within and across directories | `mounted::semantics::namespace_and_metadata` |
| `namespace.links` | Hard links, symbolic links, and `readlink` | `mounted::semantics::namespace_and_metadata` |
| `directory.enumeration` | `opendir`, `readdir`, and `releasedir`; visible names are asserted | `mounted::semantics::namespace_and_metadata` |
| `metadata.attributes` | `getattr`; chmod; atime/mtime; truncate shrink/grow | `mounted::semantics::namespace_and_metadata` |
| `io.eof-empty` | Empty files and reads at, across, and past EOF | `mounted::semantics::io_boundaries` |
| `io.sparse-truncate` | Sparse holes and truncation | `mounted::semantics::io_boundaries` |
| `io.multiblock-chunk` | Unaligned multi-block writes and files spanning the 64 MiB chunk boundary | `mounted::semantics::io_boundaries` |
| `descriptor.lifecycle` | Multiple descriptors, repeated flush, and unlink/rename while open | `mounted::semantics::descriptor_lifecycle` |
| `concurrency.disjoint` | Parallel writes to disjoint ranges | `mounted::concurrency::ordered_and_disjoint_writes` |
| `concurrency.ordered-overlap` | Overlapping writes with explicit ordering and concurrent post-flush reads | `mounted::concurrency::ordered_and_disjoint_writes` |
| `durability.flush-local` | A successful FUSE flush/close is recoverable from the configured local stage | `mounted::lifecycle::crash_after_local_flush` |
| `durability.fsync-remote` | `fsync`/`fdatasync` return only after data reaches object storage | `mounted::lifecycle::crash_after_fsync` |
| `lifecycle.clean-remount` | Clean remount preserves namespace, metadata, sparse layout, and bytes | `mounted::lifecycle::clean_remount_and_read_only` |
| `lifecycle.read-only` | Reads work; create, write, truncate, and unlink fail with `EROFS` | `mounted::lifecycle::clean_remount_and_read_only` |
| `lifecycle.invalid-storage` | Invalid object-store configuration exits nonzero before mount readiness | `mounted::lifecycle::clean_remount_and_read_only` |
| `lifecycle.crash-fsync` | Immediate `SIGKILL` after `fsync` preserves exact remote-durable bytes | `mounted::lifecycle::crash_after_fsync` |
| `lifecycle.crash-flush` | Immediate `SIGKILL` after local flush recovers the staged copy | `mounted::lifecycle::crash_after_local_flush` |
| `fs.statfs` | `statfs` returns internally consistent block and inode counts | `mounted::semantics::namespace_and_metadata` |

## Intentionally unsupported

| Case ID | Operation | Stable Linux errno |
|---|---|---|
| `unsupported.fallocate` | Recognized `fallocate` allocation/punch modes | `EOPNOTSUPP` |
| `unsupported.xattr` | Extended attributes and ACL storage | `ENOSYS` |
| `unsupported.locks` | POSIX and BSD advisory locking callbacks | `ENOSYS` |
| `unsupported.ioctl` | Filesystem-specific `ioctl` | `ENOSYS` |
| `unsupported.copy-file-range` | FUSE-native `copy_file_range` acceleration | `ENOSYS` |

The mounted unsupported test directly asserts `fallocate` and then performs a
normal write/read to prove the mount remains live. The other entries retain
fuser's default `ENOSYS` callback behavior and are documented so future callback
implementations must update this matrix and the manifest in the same change.

## Experimental behavior

| Case ID | Operation | Caveat |
|---|---|---|
| `experimental.mknod` | Special-file `mknod` | Callback coverage does not yet prove device or FIFO semantics. |
| `experimental.readdirplus` | `readdirplus` | The kernel can choose plain `readdir`; the callback is not independently forced by the gate. |
| `experimental.supplementary-groups` | Supplementary-group authorization | Primary gid participates in checks; a complete supplementary-group contract is not mounted-tested. |

## Durability and platform boundaries

- The contract is Linux-only and requires FUSE 3, `/dev/fuse`, and
  `fusermount3`.
- `write(2)` alone may remain memory-buffered.
- Close/flush acknowledges synced local staging plus atomic metadata
  publication. `fsync` and `fdatasync` additionally wait for remote object
  durability.
- Required tests use RocksDB metadata and `file:///...` object storage rooted in
  a per-test temporary directory. They require no network service or secrets.
- KisekiFS remains a learning project. Passing this matrix does not imply broad
  POSIX, multi-node, upgrade, backup, or long-running workload qualification.

Run the gate on a capable Linux host with `just test-mounted`. The command
refuses to skip when `/dev/fuse` or `fusermount3` is unavailable.
