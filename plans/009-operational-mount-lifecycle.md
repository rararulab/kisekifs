# Plan 009: Make mount resources instance-scoped, observable, and gracefully drained

> **Executor instructions**: Treat mount startup and shutdown as explicit state
> machines. No detached task or global cache may outlive its owning mount. Use
> bounded deadlines, propagate drain failures, and update this plan's row in
> `plans/README.md` when done.
>
> **Drift check (run first)**:
> `git diff --stat 2fb2c95..HEAD -- components/binary/src/cmd/mount.rs components/fuse/src/lib.rs components/vfs/src/config.rs components/vfs/src/kiseki.rs components/vfs/src/data_manager.rs components/vfs/src/writer.rs components/storage/src/pool components/storage/src/cache/file_cache.rs components/utils/src/logger.rs justfile docs/src`
> Reconcile the cache/task APIs completed by plans 006 and 007 before changing
> ownership. STOP if those plans left detached migration/flush work without a
> drain handle.

## Status

- **State**: DONE
- **Priority**: P1
- **Effort**: L
- **Risk**: MED
- **Depends on**: `plans/006-crash-consistent-write-publication.md`, `plans/007-safe-buffer-data-path.md`, `plans/008-mounted-linux-acceptance.md`
- **Category**: correctness / tech-debt / dx
- **Planned at**: commit `2fb2c95`, 2026-07-20

### Completion evidence at 2026-07-26

Implemented mount-scoped page pools, cache roots and task ownership; validated
resource limits and stored layout; bounded idempotent shutdown reports; shared
FUSE destroy/signal drain semantics; truthful owner-aware readiness; bounded
telemetry shutdown; stage/page pressure backpressure; and the operator runbook.

Final verification on `rara-linux`:

- `KISEKI_DISABLE_DISK_POOL=1 cargo nextest run -p kiseki-storage -p kiseki-vfs -p kiseki-fuse -p kiseki-binary`
- `KISEKI_DISABLE_DISK_POOL=1 cargo nextest run --workspace --all-features`
- `tests/scripts/run-mounted.sh` and `tests/scripts/run-mounted.sh --case lifecycle`
- `cargo check --all --all-features`
- `cargo clippy --workspace --all-targets --all-features --no-deps -- -D warnings`
- `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --document-private-items`
- `cargo +nightly fmt --all -- --check` and `git diff --check`
- a real mount with an unreachable OTLP endpoint, followed by bounded clean
  `SIGTERM` exit and ready-file removal

The final workspace run passed 179/179 tests (22 skipped mounted/specialized
cases), the real FUSE gate passed 20/20, and an independent second-round review
returned `APPROVE` after the path-isolation and ready-file race fixes.

## Why this matters

The mount currently blocks in `fuser::mount2`, has no `Filesystem::destroy`
implementation, and does not coordinate process signals with writer/cache
draining. The page pool is a process-global singleton that ignores VFS capacity
settings and truncates a fixed `/tmp/kiseki.page_pool`; the stage cache also
defaults to a global `/tmp` path. Multiple mounts can collide, shutdown can
drop unobserved work, and configuration logs values that are not actually used.

## Current state

- `components/binary/src/cmd/mount.rs:305-344` constructs VFS, calls blocking
  `fuser::mount2`, and returns after unmount. It does not register `ctrlc` even
  though `components/binary/Cargo.toml` already depends on it.
- `components/fuse/src/lib.rs` implements `Filesystem::init` but not
  `destroy`; dropping its Tokio runtime is the only cleanup.
- fuser 0.16 provides `spawn_mount2` and a `BackgroundSession`; dropping/joining
  the session unmounts and invokes `Filesystem::destroy`. Use the actual pinned
  crate API, not a new mount library.
- `components/storage/src/pool/mod.rs:26-53` creates
  `GLOBAL_HYBRID_PAGE_POOL` by spawning a thread/runtime and joining it. It
  hardcodes 1 GiB memory plus 1 GiB disk and uses
  `/tmp/kiseki.page_pool` for every production process.
- `components/vfs/src/config.rs` exposes `total_buffer_capacity`, `block_size`,
  `page_size`, `capacity`, and `backup_meta_interval`; only configuration
  summaries read several of them. Slice/page code uses compile-time constants,
  and backup scheduling does not exist.
- `components/vfs/src/data_manager.rs` constructs file/memory caches from their
  defaults, so mount CLI/config cannot isolate or bound them.
- `components/vfs/src/writer.rs` starts a flusher task without one mount-level
  task registry. Plan 006 makes those tasks retryable; this plan must own and
  drain them.
- Logging/OTLP can be enabled, but there is no stable “starting/ready/draining/
  stopped” signal or readiness file for an operator/service manager.

## Lifecycle contract

Use one monotonic state enum or equivalent guarded transition:

`Starting -> Recovering -> Ready -> Draining -> Stopped` with terminal
`Failed(reason)` before readiness. Only `Ready` accepts new operations.

On SIGINT/SIGTERM or FUSE destroy:

1. transition once to `Draining` and reject/finish new work safely;
2. wait for active FUSE callbacks and writer operations;
3. flush all writers to the local-durable boundary;
4. drain cache migration according to a configured shutdown policy/deadline;
5. stop and join every background task;
6. remove readiness signal and return a non-zero process status if a required
   durability step failed.

SIGKILL remains crash recovery, covered by plans 006/008.

## Commands you will need

| Purpose | Command | Expected on success |
|---------|---------|---------------------|
| Targeted tests | `KISEKI_DISABLE_DISK_POOL=1 cargo nextest run -p kiseki-storage -p kiseki-vfs -p kiseki-fuse -p kiseki-binary` | all pass |
| Mounted lifecycle | `just test-mounted --case lifecycle` | signal/drain/restart cases pass |
| Workspace | `KISEKI_DISABLE_DISK_POOL=1 cargo nextest run --workspace --all-features` | all pass |
| Gates | `just check && just lint && cargo +nightly fmt --all -- --check` | all exit 0 |

## Suggested executor toolkit

- Use `rust-router`, `rust-async-patterns`, `m07-concurrency`, and
  `m12-lifecycle` if available.
- Use task trackers/cancellation tokens already available through
  `tokio-util`; do not invent a second async runtime per subsystem.

## Scope

**In scope**:

- `components/binary/src/cmd/mount.rs`
- `components/binary/Cargo.toml` only for lifecycle dependencies already absent
- `components/fuse/src/lib.rs` and `config.rs`
- `components/vfs/src/config.rs`
- `components/vfs/src/kiseki.rs`
- `components/vfs/src/data_manager.rs`
- `components/vfs/src/writer.rs` only for mount-owned task/drain interfaces
- `components/storage/src/pool/mod.rs`, `memory_pool.rs`, and `disk_pool.rs`
- `components/storage/src/cache/file_cache.rs` only for configuration/drain
- `components/utils/src/logger.rs` only for telemetry shutdown/flush
- Mounted tests from plan 008
- `justfile`, `docs/src/operator-runbook.md` (create), `docs/src/SUMMARY.md`
- `plans/README.md`

**Out of scope**:

- Changing write durability semantics from plan 006
- Metadata backup/restore; plan 010 owns it
- Object garbage collection or slice compaction
- Daemonization/process supervision; systemd/containers should supervise the
  foreground process
- A new HTTP control plane. Readiness file and structured lifecycle logs are
  sufficient for this CLI tranche.

## Git workflow

- Work directly on `main` after the mounted acceptance gate is required.
- Commit message: `feat: add graceful mount lifecycle`.
- Push only after graceful signal and forced-timeout mounted cases pass.

## Steps

### Step 1: Replace global resource construction with validated mount config

Define one cache/buffer config owned by `VFSConfig`: memory page capacity,
optional disk spill capacity/path, stage directory/capacity/TTL, memory read
cache capacity, and shutdown deadline/policy. Expose CLI flags with readable
size/duration validation. Require an explicit `--cache-dir` for production
mounts or derive child paths beneath one explicit mount-specific root; never
use shared bare `/tmp/kiseki.*` paths.

Validate page/block/chunk constants against the stored volume `Format` before
allocating large buffers. Remove or reject no-op config fields. In particular,
do not keep `backup_meta_interval` as a false promise; plan 010 will document
external scheduling for the first backup implementation.

Redact DSNs and credentials in `Debug`/errors exactly as the object-storage CLI
tests already require.

**Verify**: CLI/config tests reject zero/misaligned/overflowing sizes, shared or
file-valued cache roots, conflicting mount roots, and format mismatches before
allocating/mounting. Debug output contains no DSN value.

### Step 2: Inject an owned page pool and caches into `DataManager`

Delete `GLOBAL_HYBRID_PAGE_POOL`. Build one `Arc<HybridPagePool>` asynchronously
during `KisekiVFS::new_checked`, using the validated mount config, and inject it
through `DataManager` into new `SliceBuffer` instances. Build file/memory caches
from the same config. Each mount instance must have independent capacities,
paths, queues, and task registries.

Do not spawn a thread solely to create a Tokio runtime. Pool creation already
runs inside startup async context. Ensure partial startup failure drops/removes
only resources created under the owned cache root.

**Verify**: construct two VFS instances in one process with different small
capacities/paths. Exhaust one pool without affecting the other; drop both and
reopen their disk paths cleanly. `rg -n 'GLOBAL_HYBRID_PAGE_POOL|/tmp/kiseki\.(page_pool|stage_cache)' components` returns no production matches.

### Step 3: Add mount-level task ownership and one shutdown barrier

Create a lifecycle/task owner under `KisekiVFS` or `DataManager`. Register the
writer flusher, cache migration/retry worker, and future maintenance workers in
one `TaskTracker`/`JoinSet`-style registry. Every worker listens to the same
cancellation token but stops only at a cancellation-safe state defined by plan
006. New task spawning after `Draining` must fail predictably.

Implement idempotent `KisekiVFS::shutdown(deadline) -> Result<ShutdownReport>`.
The report records active operations, writers flushed, staged/remote pending
counts, tasks joined, elapsed time, and any durability error without high-
cardinality inode/key labels.

**Verify**: unit tests call shutdown concurrently/repeatedly, inject a stuck
worker and a flush error, and assert one transition/result with no detached task.

### Step 4: Wire FUSE destroy and process signals to the same lifecycle

Implement `Filesystem::destroy` by running the idempotent VFS shutdown on the
owned runtime and logging the structured report. Replace blocking `mount2` with
`spawn_mount2` so the foreground main thread can wait for SIGINT/SIGTERM or an
unexpected session exit. On signal, drop/join the session to unmount and invoke
destroy; do not call async/storage APIs directly inside the signal handler.

Handle races among external unmount, signal, init failure, and session error.
Avoid fuser's `BackgroundSession::join` panic surface by joining its public
guard/result explicitly if necessary and mapping thread/session failures into
`Whatever`.

**Verify**: mounted tests send SIGTERM during idle, active write, local stage,
and remote migration. Process exits within the deadline, mount disappears, and
acknowledged data obeys plan 006. External unmount without signal produces the
same drain report.

### Step 5: Add stable readiness and lifecycle observability

Add optional `--ready-file <path>`. Create it atomically only after object-store
probe, stage recovery, metadata/root validation, successful FUSE init, and a
mounted root stat. Its contents contain non-secret version, pid, mount point,
volume name, and lifecycle state. Remove it on every drain/failure path, but
only if its pid/token proves this process created it.

Emit structured `starting`, `recovering`, `ready`, `draining`, and `stopped` log
events with duration and bounded counts. Flush tracing/OTLP providers before
process exit; a telemetry exporter outage must not block filesystem shutdown
beyond its own smaller deadline. Audit metric labels for bounded cardinality;
never label by inode, slice key, filename, or raw error string.

**Verify**: readiness is absent during delayed recovery, appears only after a
successful mount, and is removed after SIGTERM, external unmount, init failure,
and shutdown timeout. OTLP-disabled and unreachable-OTLP modes both exit.

### Step 6: Write the operator runbook and resource-pressure cases

Document foreground supervision, required cache paths/permissions/disk budget,
readiness semantics, SIGTERM deadline, recovery behavior after SIGKILL, stage
capacity exhaustion, remote outage, safe unmount, log/metric signals, and the
exact mounted diagnostic commands. Include the distinction between local
`flush` and remote `fsync` durability.

Extend mounted tests with full stage cache/backpressure, exhausted memory+disk
page pools, remote migration delay, graceful-timeout exit, and two simultaneous
mounts. The system may return `ENOSPC`/`EIO` according to the documented
contract, but must not deadlock, abort, or touch another mount's cache.

**Verify**: `just test-mounted --case lifecycle` passes all pressure/signal
cases and the runbook commands match the current CLI help.

### Step 7: Run all gates

**Verify**:
`KISEKI_DISABLE_DISK_POOL=1 cargo nextest run --workspace --all-features && just test-mounted --case lifecycle && just check && just lint && cargo +nightly fmt --all -- --check`
-> all exit 0.

## Test plan

- Unit-test lifecycle transition idempotency and task registration closure.
- Use small per-instance pools and temp roots; no global environment mutation
  except the existing explicit disk-pool disable in unit CI.
- Mounted tests cover SIGINT/SIGTERM/external unmount/session failure at each
  lifecycle state.
- Assert ready-file ownership and cleanup without deleting foreign files.
- Assert shutdown reports and log fields are bounded/non-secret.
- Run two mounts concurrently to prove path/capacity isolation.

## Done criteria

- [x] No global page/cache singleton or fixed production `/tmp/kiseki.*` path
      remains.
- [x] Runtime buffer/cache settings are validated and actually used.
- [x] Every background task is owned, cancellable at a safe point, and joined.
- [x] SIGINT, SIGTERM, external unmount, and FUSE destroy share one idempotent
      drain path.
- [x] Readiness is truthful, atomic, non-secret, and cleaned safely.
- [x] Lifecycle logs/reports are structured and bounded.
- [x] Two simultaneous mounts are resource-isolated.
- [x] Mounted lifecycle, workspace, check, lint, and format gates pass.
- [x] Operator runbook and `plans/README.md` are updated.

## STOP conditions

- Plans 006/007 did not expose owned task/drain APIs. Fix their completion
  before wrapping detached tasks with another cancellation layer.
- fuser shutdown ordering permits requests after `destroy` begins in the pinned
  version. Confirm the crate behavior and add a request gate before proceeding.
- A cache root resolves outside the explicitly configured directory or is
  shared by a live mount. Fail startup; never clean or truncate it.
- Graceful shutdown cannot meet the deadline without violating acknowledged
  durability. Exit non-zero with recoverable staged data; do not delete it or
  claim a clean stop.

## Maintenance notes

All future background work must register with the mount lifecycle owner at
creation. Configuration fields are contracts: reject or remove fields that do
not affect runtime behavior. Keep foreground operation as the primitive and
delegate restart policy to the service manager.
