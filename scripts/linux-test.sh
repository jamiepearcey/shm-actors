#!/bin/sh
# ADR-0011 (Holon P0.4): EXECUTE the full test suite on a real Linux kernel
# from the macOS dev box, via Docker (aarch64 VM-backed; kernel >= 6.x supports
# futex / eventfd / memfd seals / pidfd_open).
#
# --shm-size is mandatory: Docker's default /dev/shm is 64 MB and the named
# (shm_open) segments the tests create live there on Linux.
#
# The in-repo target_linux/ dir (gitignored) keeps container artifacts out of
# both the shared host target dir and the commit surface. Functional results
# are real; PERFORMANCE numbers from inside the VM are not representative.
set -eu
cd "$(dirname "$0")/.."
IMAGE="${IMAGE:-rust:1.91}"
exec docker run --rm --shm-size=1g \
    -v "$(pwd)":/work -w /work \
    -e CARGO_TERM_COLOR=always \
    -e CARGO_TARGET_DIR=/work/target_linux \
    "$IMAGE" \
    cargo test --workspace "$@"
