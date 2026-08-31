# Contributing

Thanks for looking under the hood. This project has a strong engineering
culture; contributions are welcome and are expected to keep it.

## Ground rules

1. **Every number is measured.** Performance claims in code comments, docs, or
   the website must come from a run of `shm-bench` (state the machine, build
   profile, and run count). Results at the timer floor are written "≤ 1 tick",
   never as a constant.
2. **Capabilities land through ADRs.** Anything that changes an on-shm ABI, a
   lock-free protocol, or a public API starts as an ADR in `docs/decisions/`
   (copy the shape of an existing one: context, decision, rejected
   alternatives, verification, measured results). Bug fixes and docs don't
   need one.
3. **Losing shapes ship with the win.** A change that helps one workload gets
   a bench row for the workload it hurts before it defaults on.
4. **The census must balance.** Tests that allocate pool chunks end by
   counting free chunks against a baseline. If your test can't account for
   every chunk, it isn't finished.

## Practical

```sh
cargo build && cargo test --workspace   # must be green
cargo clippy --workspace --all-targets  # -D warnings in CI
cargo fmt --all                         # rustfmt, pinned toolchain
```

- The toolchain is pinned by `rust-toolchain.toml`; `rustup` picks it up.
- Concurrency changes: run the relevant `loom` harness
  (`cargo test -p shm-artifact --test loom_pin` etc. with `RUSTFLAGS="--cfg loom"`)
  and say so in the PR.
- Parser changes: run the matching fuzz target for a bounded time
  (`cargo +nightly fuzz run manifest -- -max_total_time=60`).
- Keep PRs focused; mechanical churn (formatting, renames) goes in its own
  commit so review diffs stay readable.

## Conduct

See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).
