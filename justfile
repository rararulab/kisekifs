# Load environment variables from .env.local if it exists
set dotenv-load
set dotenv-filename := ".env.local"

RUST_TOOLCHAIN := `grep 'channel = ' rust-toolchain.toml | cut -d '"' -f 2`
TARGET_PLATFORM := "linux/arm64"
DISTRI_PLATFORM := "ubuntu"
HOME_DIR := env_var('HOME')
CARGO_REGISTRY_CACHE := HOME_DIR + "/.cargo/registry"
PWD := invocation_directory()
OBJECT_STORAGE_DSN := env_var_or_default("KISEKI_OBJECT_STORAGE_DSN", "file:///tmp/kiseki.data")
CACHE_DIR := env_var_or_default("KISEKI_CACHE_DIR", "/tmp/kiseki.cache")

@env:
    echo "RUST_TOOLCHAIN: {{RUST_TOOLCHAIN}}"
    echo "TARGET_PLATFORM: {{TARGET_PLATFORM}}"

# List available just recipes
@help:
    just -l

# Calculate code
@cloc:
    cloc . --exclude-dir=vendor,docs,tests,examples,build,scripts,tools,target

# Install workspace tools
@install-tools:
    cargo install cargo-nextest
    cargo install cargo-release
    cargo install git-cliff
    cargo install cargo-criterion
    cargo install cargo-outdated
    cargo install cargo-edit

# Lint and automatically fix what we can fix
@lint:
    cargo clippy --workspace --all-targets --all-features --no-deps -- -D warnings
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --document-private-items

@fmt:
    cargo +nightly fmt --all
    taplo format
    taplo format --check
    hawkeye format

alias c := check
@check:
    cargo check --all --all-features

alias t := test
@test:
    KISEKI_DISABLE_DISK_POOL=1 cargo nextest run --workspace --all-features

@test-mounted *args:
    tests/scripts/run-mounted.sh {{args}}

alias b := bench
@bench:
    #cargo bench -- --verbose --color always --nocapture --show-output
    cargo bench

@book:
    mdbook serve docs

# Applications:

@build:
    #cargo build --bin kiseki
    cargo build --package kiseki-binary

@build-release:
    cargo build --release --package kiseki-binary

alias sh := show-help
@show-help:
    cargo run --bin kiseki help
    #cargo run --bin kiseki

# ==================================================== mount

alias sh-m := help-mount
@help-mount:
    cargo run --color=always --package kiseki-binary help mount

@mount:
    just clean
    just prepare
    cargo run --color=always --package kiseki-binary mount --level debug --object-storage "{{OBJECT_STORAGE_DSN}}" --cache-dir "{{CACHE_DIR}}"

@release-mount:
    just clean
    just prepare
    cargo run --release --color=always --package kiseki-binary mount --no-log --object-storage "{{OBJECT_STORAGE_DSN}}" --cache-dir "{{CACHE_DIR}}"

@profile-mount:
    just clean
    just prepare
    cargo flamegraph --package kiseki-binary -- mount --no-log --object-storage "{{OBJECT_STORAGE_DSN}}" --cache-dir "{{CACHE_DIR}}"

# ==================================================== umount

alias sh-um := help-umount
@help-umount:
    cargo run --color=always --package kiseki-binary help umount

@umount:
    cargo run --release --color=always --package kiseki-binary umount

# ==================================================== format

@format:
    cargo run --color=always --package kiseki-binary format

@prepare:
    mkdir -p /tmp/kiseki /tmp/kiseki.meta/ /tmp/kiseki.data/
    just format

alias sh-f := help-format
@help-format:
    cargo run --color=always --bin kiseki help format

# ==================================================== MinIO

@minio:
    docker run -p 9000:9000 -p 9001:9001 \
      quay.io/minio/minio server /data --console-address ":9001"

# ==================================================== fio test

@clean:
    - rm -r /tmp/kiseki
    echo "Done: remove mount point"
    - rm -r /tmp/kiseki.meta/
    echo "Done: remove meta dir"
    - rm -r /tmp/kiseki.cache/
    echo "Done: remove cache dir"
    - rm -r /tmp/kiseki.data/
    echo "Done: remove data dir"
    - rm -r /tmp/kiseki.log/
    echo "Done: remove log dir"
    - rm -r /home/dh/kiseki/kiseki.data


alias sw := seq-write
@seq-write:
    - rm -r /tmp/kiseki/fio
    mkdir -p /tmp/kiseki/fio
    fio --name=jfs-test --directory=/tmp/kiseki/fio --ioengine=libaio --rw=write --bs=1m --size=1g --numjobs=4 --direct=1 --group_reporting

alias rw := random-write
@random-write:
    - rm -r /tmp/kiseki/fio
    mkdir -p /tmp/kiseki/fio
    fio --name=jfs-test --directory=/tmp/kiseki/fio --ioengine=libaio --rw=randwrite --bs=1m --size=512m --numjobs=4 --direct=1 --group_reporting

alias sr := seq-read
@seq-read:
    - rm -r /tmp/kiseki/fio
    mkdir -p /tmp/kiseki/fio
    fio --name=jfs-test --directory=/tmp/kiseki/fio --ioengine=libaio --rw=read --bs=1m --size=1g --numjobs=4 --direct=1 --group_reporting

alias rr := random-read
@random-read:
    - rm -r /tmp/kiseki/fio
    mkdir -p /tmp/kiseki/fio
    fio --name=jfs-test --directory=/tmp/kiseki/fio --ioengine=libaio --rw=randread --bs=1m --size=1g --numjobs=4 --direct=1 --group_reporting

build-base-image:
    docker build -t kiseki-ubuntu-builder:v0.0.1 \
        -f docker/Dockerfile.ubuntu.builder . 

build-by-docker:
    docker run --network=host \
        -v {{PWD}}:/kiseki -v {{CARGO_REGISTRY_CACHE}}:/root/.cargo/registry \
        -w /kiseki kiseki-ubuntu-builder:v0.0.1 \
        cargo build --release --package kiseki-binary

build-image:
    docker build -t kisekifs:v0.0.1 \
        -f docker/Dockerfile.ubuntu .

create-lima:
    limactl create --name=kiseki dev/lima-k8s.yml

start-lima:
    limactl start --name=kiseki

stop-lima:
    limactl stop kiseki
