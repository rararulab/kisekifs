# Kiseki

Go check the [doc](https://crrow.github.io/kisekifs/).

Kiseki is a distributed FUSE filesystem, originally a port of
[JuiceFS](https://github.com/juicedata/juicefs), now being built toward a
**production-usable, performance-oriented** product. It separates data
(object storage such as S3) from metadata (RocksDB today, a shared/network
backend planned for multi-node).

> **Status:** the project has moved beyond its original "learning project"
> phase and is being hardened for production. Expect breaking changes while core
> gaps (read-path caching, distributed metadata, FUSE layer) are addressed.

If you don't know juicefs very much, the following is the introduction of juicefs:

```
JuiceFS is an open-source, high-performance distributed file system designed for the cloud. By providing full POSIX
compatibility, it allows almost all kinds of object storage to be used as massive
local disks and to be mounted and accessed on different hosts across platforms and regions.

JuiceFS separates "data" and "metadata" storage. Files are split into chunks and stored in object storage like Amazon
S3. The corresponding metadata can be stored in various databases such as Redis, MySQL, TiKV, and SQLite, based on the
scenarios and requirements.
```

FUSE must be installed to build or run programs that use fuser (i.e. kernel driver and libraries. Some platforms may
also require userland utils like `fusermount`). A default installation of FUSE is usually sufficient.

To build fuser or any program that depends on it, `pkg-config` needs to be installed as well.

## Difference with juicefs

### 1. Write Buffer

JuiceFS uses a pre-allocated memory and growable bytes pool as write buffer,
but this pool is also used for make reading buffer.

Kiseki's write buffer pool is fixed-size, and it is consist of a in-memory bytes
pool and a mmap file.

### 2. Cache

JuiceFs use disk-eviction mechinism to manage the writeback cache,
in Kisekifs, it employs [moka](https://github.com/moka-rs/moka) to implement the
write back cache, much cleaner and efficient.

### 3. How to read slices

JuiceFs reorganize the slices into a linkedlist, kisekifs use [rangemap](https://github.com/jeffparsons/rangemap) to
handle the trick part.


# Filesystem semantics

KisekiFS does not claim full POSIX compliance. Its versioned, Linux-only
supported and intentionally unsupported behavior is listed in the
[filesystem support matrix](./docs/src/posix-support.md).

## Disclaimer

Kiseki is an independent project
and is not endorsed by or affiliated with the Juice company.

# License

Apache-2.0
