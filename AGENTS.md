# KisekiFS Project Context

## What This Project Is

KisekiFS is a distributed FUSE filesystem, originally ported from
[JuiceFS](https://github.com/juicedata/juicefs), now being built toward a
**production-usable, performance-oriented** product. It separates data storage
(object storage like S3) from metadata storage (RocksDB).

## Project Direction

KisekiFS is being hardened from a learning exercise into a production distributed
filesystem; **production-readiness and performance are now first-class goals** (a
change from older docs/commits that still call it a "learning project"). The
roadmap (architecture-review issues #68–#76 on `rararulab/kisekifs`) and the key
architecture decisions — notably the **FUSE layer migrating to `fuse-backend-rs`**
— live in [docs/src/direction.md](docs/src/direction.md). Read it before touching
the FUSE layer, metadata backend, or data path.

**Workspace Structure:**

```
components/
├── binary/      # CLI entry point (package kiseki-binary, binary `kiseki`)
├── fuse/        # FUSE layer (filesystem operations)
├── vfs/         # Virtual filesystem logic
├── meta/        # Metadata management (RocksDB backend, `meta-rocksdb` feature)
├── storage/     # Data storage layer (write buffer, caches)
├── types/       # Shared types
├── common/      # Common utilities
└── utils/       # Helper functions (incl. object_store wrapper)
tests/           # Integration tests
benches/         # Criterion benchmarks
docs/            # mdbook sources (`just book` to serve)
```

**Key Technologies:**

- **FUSE**: `fuser` today, migrating to `fuse-backend-rs` (see Project Direction
  & Goals); requires libfuse3 installed
- **Async Runtime**: tokio
- **Object Storage**: opendal and object_store (see storage note below)
- **Metadata**: RocksDB
- **Observability**: tracing, OpenTelemetry

## Why This Architecture

This project mirrors JuiceFS's separation of concerns but with Rust-specific
improvements:

1. Uses `moka` for write-back cache (cleaner than JuiceFS's disk-eviction)
2. Uses `rangemap` for slice management (instead of linked lists)
3. Fixed-size write buffer pool (memory + mmap)

## Development Workflow

**`main` is protected. Every change — however small — goes through a worktree
branch and a PR.** Never commit directly to `main` and never edit files on the
main checkout; a `guard-main-branch` hook enforces this. The flow is
spec/issue → worktree → local commits → verify → review → push → PR → merge,
adopted from the reference repo `rararulab/rara` for **parallel multi-agent
development**. The full normative process is in
[docs/guides/workflow.md](docs/guides/workflow.md); commit format is in
[docs/guides/commit-style.md](docs/guides/commit-style.md) (Conventional Commits,
enforced by a `commit-msg` hook).

- **Every change is issue-first + PR-based.** `git worktree add
  .worktrees/issue-N-<slug> -b issue-N-<slug>`, commit with `Closes #N`, open a
  PR, merge with `--squash --delete-branch` once CI is green and review approves.
- **Storage abstraction is frozen.** Both `opendal` (components/storage, vfs)
  and `object_store` (components/utils) are present. The intended direction is
  opendal, but do not migrate anything until explicitly requested.
- **The test suite is expensive.** Do not run it as routine verification. Use
  `cargo check`, `cargo clippy`, and `cargo +nightly fmt` as the quality gate.
- **Toolchain is pinned** in `rust-toolchain.toml` (1.97.1); CI reads the
  version from that file.

**Quality gate:**

```bash
cargo check --all --all-features                                        # or: just check
cargo clippy --workspace --all-targets --all-features --no-deps -- -D warnings
cargo +nightly fmt --all
```

**Build & Run:**

```bash
just build              # cargo build --package kiseki-binary
just test               # Run tests with nextest (expensive — only when asked)
just mount              # Mount filesystem (debug mode, mounts at /tmp/kiseki)
just umount             # Unmount filesystem
just book               # Serve the mdbook docs
just lint               # clippy -D warnings + cargo doc
just fmt                # cargo +nightly fmt + taplo + hawkeye
```
