# Project Direction

## Status

As of **2026-07**, KisekiFS has moved beyond its original "learning project"
phase. It is being built toward a **production-usable, performance-oriented
distributed filesystem** in the spirit of
[JuiceFS](https://github.com/juicedata/juicefs). Treat production-readiness and
performance as first-class concerns in design decisions. Older docs, commit
messages, and the `background.md` narrative may still reflect the earlier
framing.

## Positioning

- JuiceFS-class distributed filesystem: POSIX-oriented, Linux-first.
- **Data plane:** files chunked into slices, stored in object storage (S3 or
  local `file://`).
- **Control plane:** metadata in RocksDB today; a shared/network backend is
  required for true multi-node and is planned.

## Roadmap

The near-term roadmap is the set of architecture-review issues
**#68–#76** on [`rararulab/kisekifs`](https://github.com/rararulab/kisekifs/issues).
These are **all committed work items and are intentionally not ranked by
priority** — they are all in scope on the way to the production target. Highlights:

| Area | Issue | Summary |
| --- | --- | --- |
| Data path | #68 | Wire the read path through the file/mem cache (currently reads bypass caching). |
| Metadata | #69 | Move from single-node embedded RocksDB toward a shared/network metadata backend. |
| FUSE | #71 | Replace the serial `fuser` + `block_on` layer — see the decision below. |
| Storage | #70 | Remove the global singleton page pool; make it configurable/injected. |
| Cleanups | #72–#76 | Concurrency-map unification, `DataManager` decoupling, mtime propagation, etc. |

## Key Decision: FUSE layer → `fuse-backend-rs`

The FUSE layer will migrate from **`fuser`** to
[**`fuse-backend-rs`**](https://github.com/cloud-hypervisor/fuse-backend-rs)
(rust-vmm / Nydus / virtiofsd lineage).

**Why**, given the production + performance goal:

- **Zero-copy read/write** (`ZeroCopyReader` / `ZeroCopyWriter`) — data moves
  directly between the FUSE request and our slice buffer / cache, avoiding the
  extra buffer copy an async spawn would require.
- **True multi-threaded dispatch** — multiple worker threads read the FUSE
  device concurrently (kernel `FUSE_DEV_IOC_CLONE`), removing today's fully
  serial request processing.
- **One `FileSystem` impl serves both `/dev/fuse` and virtio-fs** through the
  transport-agnostic `Server<F>`. Adopting it keeps a future virtio-fs
  deployment open with no rewrite, without forcing it now.

`fuse3` (async-native FUSE) was considered and rejected for this goal: its main
advantage is async ergonomics (developer experience), not performance, and it
cannot serve virtio-fs.

### Consequence for issue #71

Issue #71 (the FUSE layer processes requests serially because `fuser::mount2`
runs a single-threaded loop and every callback `block_on`s) is resolved by
**migrating directly to `fuse-backend-rs`**. Do **not** invest in the interim
"`fuser` + spawn" approach — it would be throwaway work on the same layer.

The synchronous `FileSystem` trait keeps an async bridge (each worker thread
`block_on`s the existing async VFS via a `runtime::Handle`); this is acceptable
because object-storage latency dwarfs the bridge cost.

## Note on where performance actually lives

For an object-storage-backed filesystem, the FUSE transport is a *secondary*
performance lever. The dominant levers are cache hit-rate on the read path (#68)
and the metadata backend (#69). The FUSE-layer migration removes the serial
bottleneck and unlocks zero-copy, but should be evaluated alongside — not ahead
of — the data-path and metadata work.
