#!/usr/bin/env bash
set -euo pipefail

case_group=""
while (($#)); do
    case "$1" in
        --case)
            [[ $# -ge 2 ]] || { echo "--case requires a value" >&2; exit 2; }
            case_group="$2"
            shift 2
            ;;
        *)
            echo "unknown mounted-test argument: $1" >&2
            exit 2
            ;;
    esac
done

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

use_docker=0
if [[ "$(uname -s)" != "Linux" || "${KISEKI_MOUNT_TEST_USE_DOCKER:-0}" == "1" ]]; then
    use_docker=1
fi

if [[ "${KISEKI_MOUNT_TEST_IN_CONTAINER:-0}" != "1" && "$use_docker" == "1" ]]; then
    command -v docker >/dev/null || {
        echo "mounted tests require Linux or Docker" >&2
        exit 1
    }
    docker_args=(
        run --rm --privileged
        -e KISEKI_MOUNT_TEST_IN_CONTAINER=1
        -e CARGO_TARGET_DIR=/workspace/target-linux
        -e RUST_BACKTRACE=1
        -v "$repo_root:/workspace"
        -v kisekifs-cargo-registry:/usr/local/cargo/registry
        -v kisekifs-linux-target:/workspace/target-linux
        -w /workspace
        rust:1.97.1-bookworm
        bash -c
    )
    inner="apt-get update >/dev/null && apt-get install -y clang fuse3 libclang-dev libfuse3-dev libssl-dev pkg-config >/dev/null && tests/scripts/run-mounted.sh"
    if [[ -n "$case_group" ]]; then
        inner+=" --case $(printf '%q' "$case_group")"
    fi
    exec docker "${docker_args[@]}" "$inner"
fi

[[ -c /dev/fuse ]] || { echo "/dev/fuse is unavailable" >&2; exit 1; }
command -v fusermount3 >/dev/null || { echo "fusermount3 is unavailable" >&2; exit 1; }
command -v timeout >/dev/null || { echo "GNU timeout is unavailable" >&2; exit 1; }

mounted_root=$(mktemp -d /tmp/kisekifs-mounted.XXXXXX)
case "$mounted_root" in
    /tmp/kisekifs-mounted.*) ;;
    *) echo "refusing unsafe mounted-test root: $mounted_root" >&2; exit 1 ;;
esac

cleanup() {
    local mount_path pid cmdline
    while IFS= read -r -d '' mount_path; do
        if mountpoint -q "$mount_path"; then
            fusermount3 -uz "$mount_path" || true
        fi
    done < <(find "$mounted_root" -mindepth 2 -maxdepth 2 -type d -name mount -print0 2>/dev/null)

    for proc in /proc/[0-9]*; do
        [[ -r "$proc/cmdline" ]] || continue
        cmdline=$(tr '\0' ' ' < "$proc/cmdline")
        if [[ "$cmdline" == *"kiseki-binary mount"* && "$cmdline" == *"$mounted_root"* ]]; then
            pid=${proc##*/}
            kill "$pid" 2>/dev/null || true
        fi
    done

    while IFS= read -r -d '' mount_path; do
        if mountpoint -q "$mount_path"; then
            echo "leaked mount after cleanup: $mount_path" >&2
            return 1
        fi
    done < <(find "$mounted_root" -mindepth 2 -maxdepth 2 -type d -name mount -print0 2>/dev/null)
    rm -rf -- "$mounted_root"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

cd "$repo_root"
cargo build -p kiseki-binary
target_dir=${CARGO_TARGET_DIR:-"$repo_root/target"}
export KISEKI_BIN="$target_dir/debug/kiseki-binary"
export KISEKI_MOUNT_TEST_ROOT="$mounted_root"
export KISEKI_DISABLE_DISK_POOL=1

test_filter="mounted::"
if [[ -n "$case_group" ]]; then
    case "$case_group" in
        smoke|semantics|concurrency|lifecycle|unsupported)
            test_filter="mounted::$case_group::"
            ;;
        *)
            echo "unknown mounted-test case group: $case_group" >&2
            exit 2
            ;;
    esac
fi

timeout --foreground 10m \
    cargo test -p tests "$test_filter" -- --ignored --nocapture --test-threads=1
