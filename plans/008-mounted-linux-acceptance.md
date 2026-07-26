# Plan 008: Add a mounted Linux acceptance gate and supported-semantics contract

> **Executor instructions**: This plan verifies KisekiFS through the Linux
> kernel/FUSE boundary. A unit-level substitute is not completion. Make every
> mount test own unique metadata, object, cache, and mount directories; always
> unmount/kill in a guard. Update this plan's row in `plans/README.md` when done.
>
> **Drift check (run first)**:
> `git diff --stat 2fb2c95..HEAD -- tests components/binary/src/cmd/mount.rs components/binary/src/cmd/unmount.rs components/fuse/src/lib.rs components/vfs/src docs/src README.md justfile .github/workflows/test.yml`
> Plans 006 and 007 are dependencies and will change data-path behavior. Re-run
> their completed test suites before adding mounted expectations.

## Status

- **State**: DONE
- **Priority**: P1
- **Effort**: L
- **Risk**: MED
- **Depends on**: `plans/006-crash-consistent-write-publication.md`, `plans/007-safe-buffer-data-path.md`
- **Category**: tests / docs
- **Planned at**: commit `2fb2c95`, 2026-07-20

### Local completion evidence at 2026-07-26

Implemented the versioned Linux support contract, machine-checked manifest,
hermetic mounted-test harness, explicit CLI cache/permission controls, and a
required privileged Linux CI job. Mounted regressions also fixed overlapping
slice reads, truncate growth holes, concurrent flush notification loss, and an
open/last-close race that could hand a new file descriptor a retired writer.

Local verification on `rara-linux`:

- `cargo check --all --all-features`
- `cargo clippy --workspace --all-targets --all-features --no-deps -- -D warnings`
- `cargo +nightly fmt --all -- --check`
- focused overlap, truncate, concurrent-finish, and manifest tests
- two consecutive full mounted runs, both 9/9
- 20 consecutive lifecycle-group runs
- an intentional mounted smoke failure followed by zero leaked mount,
  process, or temporary root

### Remote completion evidence at 2026-07-26

- GitHub Actions run `30191772097`: `Mounted Linux Acceptance` passed.
- GitHub Actions run `30191908925`: `Mounted Linux Acceptance` passed again.
- GitHub Actions run `30192201516`: the full CI workflow passed, including a
  third `Mounted Linux Acceptance` run and Miri buffer-ownership checks.
- All three runs also passed the workspace test, Clippy, formatting,
  documentation, coverage, advisory, and mounted-test aggregator jobs.

The first two runs' unrelated Miri job exposed two CI/nightly compatibility
defects: missing FUSE headers and expression-position use of a SNAFU macro.
Commits `1fabe9e` and `3e9b59d` fix those defects without weakening the mounted
gate.

## Why this matters

The workspace currently has 112 passing unit/component tests but no test mounts
the filesystem. `tests/src/lib.rs` is empty, so kernel caching, FUSE callback
mapping, real file-descriptor behavior, unmount/remount recovery, and the
claimed errno contract are unproven. Production support needs a versioned,
honest POSIX subset and a required Linux test that exercises it.

## Current state

- `tests/Cargo.toml` depends only on `rangemap`; `tests/src/lib.rs` contains no
  tests or harness.
- `.github/workflows/test.yml` runs only
  `cargo nextest run --workspace --all-features`; it installs `libfuse3-dev` but
  never checks `/dev/fuse` or mounts the built binary.
- `components/fuse/src/lib.rs` implements lookup, attrs, namespace operations,
  open/read/write/flush/release/fsync, directory reads, statfs, create, and
  fallocate. Other `fuser::Filesystem` methods use fuser's default `ENOSYS`.
- `components/vfs/src/kiseki/file_io.rs` deliberately returns `ENOTSUP` for
  currently recognized fallocate modes. Plan 005 made invalid boundary inputs
  non-panicking, but only below FUSE.
- `README.md` says the repository is a learning project and “Totally not
  compliant with posix”; there is no support matrix distinguishing implemented,
  unsupported, and untested behavior.
- Mount defaults `allow_other = true`. The argument needs an explicit false
  form for unprivileged/isolated CI environments instead of relying on global
  `/etc/fuse.conf`.
- `justfile` has build/test/mount recipes but no hermetic mounted acceptance
  command.

## Acceptance contract

The gate targets Linux only. It must use a local object-store DSN and temporary
RocksDB metadata, run without external services, finish under a bounded timeout,
and leave no mount/process/temp files behind. Every case is classified as:

- **Supported**: exact behavior/errno asserted and required in CI.
- **Unsupported**: exact stable errno asserted; no panic/hang/silent success.
- **Experimental**: not a release promise and not a required green assertion.

Do not label a syscall supported solely because a FUSE callback exists.

## Commands you will need

| Purpose | Command | Expected on success |
|---------|---------|---------------------|
| Build binary | `cargo build -p kiseki-binary` | exit 0 |
| FUSE probe | `test -c /dev/fuse && command -v fusermount3` | exit 0 on mounted-test host |
| Mounted suite | `just test-mounted` | all cases pass; no remaining mount/process |
| Unit suite | `KISEKI_DISABLE_DISK_POOL=1 cargo nextest run --workspace --all-features` | all pass |
| Gates | `just check && just lint && cargo +nightly fmt --all -- --check` | all exit 0 |

## Suggested executor toolkit

- Use `rust-router`, `domain-cli`, and `superpowers:systematic-debugging` if
  available.
- Use Linux process/mount inspection (`/proc/<pid>/mountinfo`) rather than fixed
  sleeps to detect readiness and cleanup.

## Scope

**In scope**:

- `tests/Cargo.toml`
- `tests/src/lib.rs` and new files below `tests/`
- `components/binary/src/cmd/mount.rs` and `unmount.rs` only for testable CLI
  boolean/exit/readiness behavior uncovered by the harness
- `components/fuse/src/lib.rs` and `components/vfs/src/**` only for a concrete
  mounted regression with a failing lower-level test added first
- `docs/src/posix-support.md` (create) and `docs/src/SUMMARY.md`
- `README.md` for support-claim links only
- `justfile`
- `.github/workflows/test.yml` and, if clearer, one new reusable mounted workflow
- `plans/README.md`

**Out of scope**:

- Claiming full POSIX compliance
- Implementing xattrs, ACLs, `ioctl`, `copy_file_range`, file locking, or all
  fallocate modes merely to make the table look complete
- macOS/macFUSE support
- Real S3 credentials or network services in required CI
- Large performance benchmarks; this is correctness acceptance

## Git workflow

- Work directly on `main` after plans 006 and 007.
- Commit message: `test: add mounted Linux acceptance gate`.
- Push only after the mounted job passes twice consecutively and cleanup is
  verified after an intentionally failed test.

## Steps

### Step 1: Define the support manifest before writing expectations

Create `docs/src/posix-support.md` with a versioned table for every currently
implemented FUSE operation and important defaulted operation. Record the test
case ID, supported/unsupported/experimental status, expected errno where
unsupported, and known caveats (Linux only, primary gid behavior, no full
supplementary-group contract, local vs remote durability from plan 006).

Keep a small machine-readable manifest under `tests/fixtures/` (TOML or JSON)
containing the same case IDs/statuses. Add a test that rejects duplicate IDs,
unknown status values, or documented supported cases without a registered
test. The prose remains for humans; the manifest prevents promises from
drifting silently.

**Verify**: `cargo nextest run -p tests support_manifest` -> manifest
parses and every `supported` case maps to a test registration.

### Step 2: Build a hermetic mount harness

Add a Rust integration harness that:

1. Creates one temp root containing distinct `meta`, `objects`, `stage`,
   `page-pool`, and `mount` paths.
2. Runs the built `kiseki format` with a unique volume name and RocksDB DSN.
3. Spawns `kiseki mount --foreground` with local object storage and explicit
   per-test cache paths from plans 006/009. Pass `allow_other=false`; adjust the
   clap bool action if the current true default cannot be negated.
4. Waits for the mount to appear in mountinfo and for a root `stat` to succeed,
   while concurrently checking that the child has not exited.
5. Captures bounded logs without leaking DSNs/secrets.
6. On every success, panic, or timeout, unmounts with `fusermount3 -u`, then
   terminates/reaps the child and removes temp data.

Use RAII guards and polling with a deadline. Never use `sleep(N)` as the only
readiness condition. Refuse to run if the target resolves outside the owned
temp root.

**Verify**: a smoke test mounts, creates one file, unmounts, and confirms both
the child and mountinfo entry are gone. Force the test body to fail once and
confirm cleanup still occurs; restore it before commit.

### Step 3: Cover namespace, metadata, and read semantics

Add mounted cases for create/open/close, mkdir/rmdir, rename within/across
directories, hard links, symlinks/readlink, unlink-open-file behavior,
readdir/readdirplus-visible results, chmod, timestamps, truncate shrink/grow,
sparse holes, empty files, stat/statfs, read at/exactly/past EOF, unaligned
multi-block writes, overwrite, and files spanning chunks. Assert byte-for-byte
content and observable metadata, not only command exit codes.

For unsupported operations such as recognized fallocate modes, assert the
documented errno and then perform a normal read/write to prove the mount remains
alive.

**Verify**: `just test-mounted --case semantics` -> all registered namespace,
metadata, and read/write cases pass.

### Step 4: Cover concurrency and descriptor lifecycle

Use deterministic data patterns to test parallel writers to disjoint offsets,
overlapping writes with explicitly ordered synchronization, concurrent readers
during/after flush, multiple descriptors for one inode, repeated flush, fsync,
close, unlink while open, and rename while open. Bound each case with a timeout
and verify checksums after all descriptors close.

Do not encode scheduler-dependent “last writer wins” assertions without an
ordering barrier. A hang is a test failure with child logs attached.

**Verify**: run the concurrency group at least 20 iterations locally with no
hang, checksum change, leaked mount, or leaked child.

### Step 5: Prove restart, read-only, and crash boundaries

Add these separate mount cycles:

- clean unmount/remount preserves namespace, metadata, sparse layout, and data;
- successful `fsync`, immediate `SIGKILL`, remount preserves the exact remote-
  durable bytes from plan 006;
- acknowledged `flush` followed by process kill recovers the local staged copy
  when the same stage directory is supplied;
- read-only mount permits reads and rejects create/write/truncate/unlink with
  `EROFS` (or the kernel's stable read-only errno);
- malformed/unreachable object-storage configuration fails before a ready mount
  and leaves no mount entry.

**Verify**: `just test-mounted --case lifecycle` -> all cycles pass and the
temp root contains no unclassified staged files afterward.

### Step 6: Add a required Linux mounted CI job

Add a separate job so unit-test failures and host-capability failures are easy
to distinguish. Install `fuse3`/`libfuse3-dev`, build the binary once, probe
`/dev/fuse` and `fusermount3`, and run `just test-mounted` under an outer timeout.
Upload bounded logs on failure and run an always-step that reports lingering
`fuse.kiseki` mounts/processes before cleanup.

If standard hosted runners do not expose usable `/dev/fuse`, use a dedicated
privileged/self-hosted Linux runner label and make that job required. Do not
silently skip the suite and report CI green.

**Verify**: the remote mounted job passes twice on `main`. Introduce a temporary
wrong checksum expectation and confirm the job fails; restore it.

### Step 7: Align documentation and run all gates

Replace the blanket README POSIX sentence with an honest Linux/support-matrix
link. Keep the learning-project history if desired, but do not call the system
production-ready. Document exact local prerequisites and `just test-mounted`.

**Verify**:
`just test-mounted && KISEKI_DISABLE_DISK_POOL=1 cargo nextest run --workspace --all-features && just check && just lint && cargo +nightly fmt --all -- --check`
-> all exit 0.

## Test plan

- One process-owning harness; case functions share setup but never state.
- Unique temp paths and volume names for every run.
- Exact data patterns/checksums across block and chunk boundaries.
- Exact errno assertions for documented unsupported behavior.
- Timeout diagnostics include child exit status, last bounded logs, mountinfo,
  and current case ID.
- CI cleanup runs with `if: always()`.

## Done criteria

- [x] A required Linux job mounts the real binary through `/dev/fuse`.
- [x] Every documented supported case has a registered mounted test.
- [x] Namespace, metadata, I/O, concurrency, fsync/crash, remount, and read-only
      groups pass.
- [x] Unsupported cases return stable errno without killing the mount.
- [x] Failed/timed-out tests leave no mount, process, or foreign temp path.
- [x] README claims match the versioned support matrix.
- [x] Mounted suite passes twice remotely; unit/check/lint/format gates pass.
- [x] `plans/README.md` is updated with completion evidence.

## STOP conditions

- The available CI environment cannot provide `/dev/fuse` or required mount
  privileges. Provision/label a privileged runner; do not downgrade the gate.
- A supported-case failure requires a semantic choice not derivable from Linux
  behavior or existing docs. Mark it experimental and report the choice rather
  than inventing a promise.
- Cleanup would target a path or process not created by the current harness.
  Abort cleanup and report the resolved identifiers.
- Plan 006's completed durability contract is not actually observable through
  FUSE. Reopen plan 006 instead of weakening restart assertions.

## Maintenance notes

Every newly implemented FUSE callback must update the support manifest, prose
matrix, and mounted case in the same commit. Keep the required suite small and
deterministic; put long stress/performance runs in a separate scheduled job.
