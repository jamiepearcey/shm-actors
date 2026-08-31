## What & why

<!-- One paragraph. Link the ADR if this changes an ABI, protocol, or public API. -->

## Evidence

- [ ] `cargo test --workspace` green
- [ ] `cargo clippy --workspace --all-targets` clean
- [ ] Touched a lock-free core → relevant `loom` harness run
- [ ] Touched a parser → fuzz target run (bounded)
- [ ] Performance claim → `shm-bench` output pasted (2 runs, machine profile)
- [ ] Mechanical churn separated into its own commit
