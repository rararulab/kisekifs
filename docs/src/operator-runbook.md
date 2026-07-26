# Mount operations runbook

KisekiFS currently targets foreground-supervised Linux deployments. Run one
process per volume and let systemd, a container runtime, or another supervisor
own restarts. Do not daemonize the process or share a cache root between
mounts.

## Before mounting

Create distinct locations for metadata, the mount, cache, object data (for
`file://`), and the ready-file parent:

- a RocksDB metadata directory on persistent local storage;
- an empty mount point;
- an absolute, mount-specific cache root on persistent local storage.

The mount, cache root, local object root, and ready path must not contain or
overlap one another, including through symlinks. The cache root must be a real
directory, not a symlink. KisekiFS locks `<cache-dir>/.mount.lock`; a second
live mount using that root is rejected. It also holds shared advisory locks on
every canonical ancestor and an exclusive lock on the selected cache root, so
active nested roots such as `/cache` and `/cache/stage` are rejected in either
startup order. The service user needs
read/write/search permission on the metadata, cache, object-store (for
`file://`), mount, log, and ready-file parent directories.

Budget at least the configured stage-cache and disk-page capacities plus
filesystem overhead. Memory usage includes the memory page pool and read cache.
Capacities use values such as `512MiB` or `10GiB`; durations use values such as
`30s` or `24h`.

Example foreground mount:

```bash
kiseki mount \
  --foreground \
  --meta-dsn rocksdb://:/var/lib/kiseki/volume/meta \
  --object-storage s3://bucket/volume-prefix?region=ap-southeast-1 \
  --cache-dir /var/lib/kiseki/volume/cache \
  --memory-page-capacity 512MiB \
  --disk-page-capacity 4GiB \
  --stage-cache-capacity 20GiB \
  --memory-read-cache-capacity 1GiB \
  --shutdown-policy local \
  --shutdown-deadline 30s \
  --ready-file /run/kiseki/volume.ready.json \
  /mnt/volume
```

S3 credentials come from the standard AWS environment or identity chain. Do
not put credentials in the DSN. Use `file:///absolute/path` for a local object
store.

For systemd, keep `Type=simple`, send `SIGTERM`, and set `TimeoutStopSec` above
the KisekiFS shutdown deadline (for example, 40 seconds for a 30-second
deadline). A non-zero exit is a failed durability or drain operation and should
alert the operator.

## Readiness

`--ready-file` is optional but recommended. KisekiFS atomically publishes JSON
only after metadata and cache recovery, FUSE initialization, and a successful
stat of the mounted root. The record contains the version, PID, unique owner
token, mount point, volume, and `"state":"ready"`; it contains no credentials.

Consumers must parse the file and verify that its PID is the supervised
process. File existence alone is insufficient: `SIGKILL` deliberately leaves a
stale record. A subsequent mount removes a well-formed record only when Linux
confirms that its PID is dead. Unknown, malformed, live-owner, or concurrently
replaced files are preserved and cause startup to fail.

Publication and cleanup hold a companion `<parent>/.<name>.lock` lease. On
Linux, cleanup first atomically retires the public name with
`renameat2(RENAME_NOREPLACE)`, then verifies the captured regular file's inode
and owner token. It never unlinks the public path; symlinks and other unknown
file types are restored and rejected. If a second foreign writer occupies the
path during restoration, both records are preserved, with the captured one
under a hidden `.retired-*` name. The lock file is intentionally persistent
and must not be replaced, linked, or deleted while the service is running.
Keep the ready-file parent private to the service user.

On `SIGINT`, `SIGTERM`, external unmount, initialization failure, or a shutdown
failure, the owner removes its ready file before exiting. `SIGKILL` cannot run
cleanup; treat the mount as unready as soon as the process dies.

## Durability and shutdown

KisekiFS has two explicit durability boundaries:

- FUSE `flush`/close makes dirty data restart-recoverable in the local stage
  cache before publishing slice metadata.
- `fsync`/`fdatasync` confirms the corresponding immutable blocks in the
  remote object store before success.

The default `--shutdown-policy local` drains active requests, flushes writers
to the local stage cache, cancels and joins all mount workers, and then exits.
It permits a clean stop during a remote outage because the next mount recovers
and retries staged blocks. `--shutdown-policy remote` additionally requires
every staged block to reach object storage and therefore exits non-zero if the
remote service is unavailable.

On shutdown the lifecycle moves once from `ready` to `draining`, rejects new
operations, and waits only until `--shutdown-deadline`. A timeout or flush/task
failure produces a bounded error report, removes readiness, unmounts, and exits
non-zero. Recoverable staged files are retained; never delete the cache root to
silence a shutdown error.

Use `SIGTERM` for the normal stop path:

```bash
ready=/run/kiseki/volume.ready.json
kill -TERM "$(jq -r .pid "$ready")"
timeout 40s sh -c 'while findmnt -rn /mnt/volume >/dev/null; do sleep 0.1; done'
```

An external `fusermount3 -u /mnt/volume` enters the same drain path. Use lazy or
forced unmount only for a dead/unresponsive daemon, then retain the metadata,
cache, and object-store data for recovery.

## Resource pressure and remote outages

Each mount owns independent memory pages, optional disk spill, read cache,
stage files, and background tasks. Page-pool exhaustion fails the write quickly
with `ENOSPC`; it does not block the whole mount. Stage-cache exhaustion also
returns `ENOSPC` and schedules one owned migration worker. When object storage
recovers, retry the failed flush/fsync; successful migrations release stage
capacity. `EIO` indicates an I/O or required remote-durability failure.

If stage usage keeps growing, first restore object-store connectivity and
inspect lifecycle logs. Increasing capacity is a restart-time configuration
change; never point another mount at the same cache or delete staged files by
hand.

## Diagnostics

Useful commands on the Linux host:

```bash
jq . /run/kiseki/volume.ready.json
findmnt -T /mnt/volume
du -sh /var/lib/kiseki/volume/cache/stage
journalctl -u kiseki-volume --since -30min | \
  grep -E 'lifecycle (starting|recovering|ready|draining|stopped|failed)'
```

The structured stop event includes clean/timeout status, writer totals,
remaining staged blocks, task spawn/completion/panic/abort counts, and elapsed
milliseconds. Counts are bounded and logs do not label operations by inode,
slice key, file name, DSN, or raw credential-bearing values. An unreachable
OTLP exporter has its own shorter timeout and cannot extend filesystem shutdown
indefinitely.

Run the production lifecycle acceptance gate on a Linux host with FUSE 3:

```bash
KISEKI_DISABLE_DISK_POOL=1 cargo nextest run \
  -p kiseki-storage -p kiseki-vfs -p kiseki-fuse -p kiseki-binary
just test-mounted --case lifecycle
```

The mounted gate covers clean/external unmount, `SIGINT`/`SIGTERM` during
startup, idle, and active writes, forced deadline failure, remote outage and
recovery, `SIGKILL` restart recovery, combined memory/disk and stage pressure,
path-alias rejection, read-only remount, and simultaneous cache-isolated
mounts.
