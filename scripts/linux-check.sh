#!/bin/sh
# ADR-0011 (Holon P0.4): compile-level Linux assurance from the macOS dev box.
#
# `cargo check`/`clippy` need no linker, so the cross-compiled std target gives
# every `#[cfg(target_os = "linux")]` line — libc constants included — full
# compile-and-lint proof without a Linux machine. Install the target once with:
#
#     rustup target add x86_64-unknown-linux-gnu
#
# A private CARGO_TARGET_DIR keeps this out of the (shared) native target dir.
set -eu
cd "$(dirname "$0")/.."
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/shm-linux-check}"
TARGET="${1:-x86_64-unknown-linux-gnu}"

echo "== cargo check --workspace --all-targets --target $TARGET =="
cargo check --workspace --all-targets --target "$TARGET"
echo "== cargo clippy --workspace --all-targets --target $TARGET (-D warnings) =="
cargo clippy --workspace --all-targets --target "$TARGET" -- -D warnings
echo "linux-check OK ($TARGET)"
