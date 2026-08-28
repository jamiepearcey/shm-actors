# ADR-0011 — Holon P0.4: the Linux fast paths

- Status: Accepted
- Date: 2026-08-28
- Builds on: [ADR-0008](ADR-0008-holon-phase-0-substrate-prerequisites.md)
  (P0.4 scope), [ADR-0004](ADR-0004-v0.4-adversarial-release.md) (item H's
  standing rulings), [ADR-0003](ADR-0003-v0.3-scope.md) (the ABI-reserved
  doorbell words this ADR activates).

## Scope and the inherited rulings

ADR-0004 deferred item H with two rulings this ADR inherits verbatim: *"impls
only; any new `Platform` method gets a POSIX-fallback default. The doorbell
word is already ABI-reserved — nothing to do now."* All five fast paths land as
a single Linux-only module, `crates/shm-core/src/platform_linux.rs`, declared
under `#[cfg(target_os = "linux")]` — on macOS none of it is parsed past
cfg-stripping, so the POSIX baseline (and its 173-test suite) is untouched by
construction.

The critical constraint: the dev box is macOS and no Linux CI runner exists.
§Verification below records exactly what assurance this box can and cannot
produce; nothing beyond it is claimed.

## Decision 1 — division of labor: futex is the doorbell of record; eventfd only where an fd is required

Two wake primitives exist because two consumer shapes exist:

- **futex** — the wake word rides *inside* the already-mapped region
  (`RingHeader.doorbell_seq`, `TaskQueueHeader.doorbell_seq` — both
  ABI-reserved by ADR-0003 in the former `_pad` words; header sizes and the
  96-byte/64-byte const asserts are unchanged). No fd to create, grant, retain,
  or leak; notify is one `fetch_add` + `FUTEX_WAKE`, park is one `FUTEX_WAIT`.
  This is the Holon Phase 1 mailbox doorbell (the <200 ns wake budget).
  Shipped as `FutexNotifier`/`FutexParker` in `shm-ring::hooks` plus
  all-platform accessors `Ring::doorbell_word()` / `TaskQueue::doorbell_word()`.
  The syscall deliberately omits `FUTEX_PRIVATE_FLAG` — the word lives in
  `MAP_SHARED` memory and must wake other processes.
- **eventfd** — replaces the anonymous pipe *behind the existing free
  functions* `doorbell_pair`/`doorbell_ring` wherever a pollable, grantable
  **fd** is required (the coordinator's per-topic and work/done doorbells,
  distributed over `SCM_RIGHTS`). One eventfd object serves as both "ends" of
  the `DoorbellPair` (two dups of one open file description), so the granting
  protocol, `doorbell_park`, and the drain loop are byte-for-byte unchanged —
  every existing consumer upgrades with zero re-plumbing. A saturated counter
  (`EAGAIN`) is treated as success exactly like the full pipe: the doorbell is
  already readable, so the wake is guaranteed.

The futex park keeps the **single-phase `Parker` contract**: read the word,
`FUTEX_WAIT` while it still holds that value, bounded 50 ms timeout. A notify
that lands entirely between `recv`'s post-registration re-check and the
parker's own read is a missed wake recovered by the bounded timeout — the SAME
liveness argument the pipe parker already documents, so no new proof burden. A
two-phase prepare/park API that closes the window is deferred until Phase 1's
mailbox numbers prove it necessary.

## Decision 2 — memfd sealing: `SHRINK | GROW | SEAL`, never `WRITE`

`Segment::create_sealed` (Linux) backs a segment with
`memfd_create(MFD_CLOEXEC | MFD_ALLOW_SEALING)` + `ftruncate` +
`F_ADD_SEALS(F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_SEAL)`:

- `SHRINK` kills the hostile-truncate SIGBUS class (risk R3 in the
  actor-framework design): a peer holding any granted fd gets `EPERM`, mapped
  readers keep their pages.
- `GROW` pins the size the header claims.
- `SEAL_SEAL` stops a peer adding *further* seals (e.g. `F_SEAL_FUTURE_WRITE`)
  that would wedge legitimate writers.
- `F_SEAL_WRITE` is **never** added — segments are mutable shared state.

The coordinator's `create_segment` now routes through the `Platform` seam's new
`segment_create_sealed` (POSIX default: plain `segment_create`), so **every**
coordinator segment on Linux is an anonymous sealed memfd. This costs nothing:
the runtime already distributes every segment by fd (`SCM_RIGHTS` →
`Segment::from_raw_fd`); nothing production attaches coordinator segments by
name. Named `Segment::create`/`attach` stay `shm_open`-backed on all platforms
as the dev/bench path.

**Shared-code change** (the one, exercised by the macOS suite): `Segment.name`
became `Option<CString>`. `from_raw_fd` handles are now **unnamed** — their
`unlink` is a no-op, so an `SCM_RIGHTS` receiver can never remove the
creator's namespace entry (previously it silently could; the new
`adopted_fd_segment_is_unnamed_and_unlink_is_a_noop` test fails on the old
behavior). Memfd segments are unnamed by nature.

## Decision 3 — pidfd is an accelerator; leases remain the correctness backstop

At registration the coordinator resolves the actor's pid (`SO_PEERCRED` on the
control UDS) and opens a pidfd. The lease monitor's per-tick sleep becomes a
`poll(2)` over all alive actors' pidfds with the tick as timeout: an exit ends
the tick early and the actor is driven through the **exact same** mark-dead →
journal-replay reclaim path a lease expiry takes (no new reclaim logic).
`LinuxPlatform::death_detection()` reports the pre-existing
`DeathDetection::KernelNotified`.

Leases stay authoritative because a pidfd (a) only detects exit, never a
wedged-but-alive actor, (b) is best-effort (`None` on any acquisition failure),
and (c) the `SO_PEERCRED`-pid → `pidfd_open` window races pid reuse — on a
reused pid the fd watches the wrong process and simply never helps.
`SCM_PIDFD` (kernel ≥ 6.5) would close that window; deferred until the nix API
covers it — the race's only cost is falling back to lease latency. An
in-process node yields a pidfd on the coordinator's own pid: harmless, it never
fires.

## Decision 4 — `CLOCK_MONOTONIC` is the Linux task-deadline clock

`shm_task::now_nanos()` is `clock_gettime(CLOCK_MONOTONIC)` on Linux (wall
clock elsewhere). Sound because `CLOCK_MONOTONIC` is system-wide consistent
across processes on one boot, and a queue's participants by definition share
one host (it is shared memory) and one build — hence one clock domain. Deadline
values are only compared against each other, never read as dates, so the epoch
change is invisible to the contract; what it buys is immunity from NTP steps
spuriously reaping or immortalizing claim leases. Pre-launch, no compat shim:
mixed wall/monotonic participants on one queue would mis-reap, and the one-host
one-build invariant makes that configuration unconstructible. Coordinator
leases already use `std::time::Instant` and needed nothing.

The seam also gained `Platform::now_nanos()` (default: wall clock; Linux:
monotonic) for future consumers that want the platform clock without a cfg.

## ABI impact

**None from this ADR — that is the point of the pre-reservation.** The futex
words occupy the former `_pad` in both control blocks; `RingHeader` stays 96 B;
`SegmentHeader` (32 B) is untouched by memfd backing; `SHMRING1` and `SHMACTR1`
stand. `Platform` gained only default methods. The `Segment.name` option is
process-local state, not ABI.

`TaskQueueHeader` **was** subsequently re-laid-out by
[ADR-0012](ADR-0012-task-queue-contention.md) (64 → 1088 bytes, `SHMTASK3 →
SHMTASK4`) for reasons unrelated to this ADR. The futex path is unaffected
because it addresses `doorbell_seq` through the struct field
(`TaskQueue::doorbell_word`), never a hard-coded offset, and the field is on
the control line at offset 36 (4-aligned, as `FUTEX_WAIT` requires).

## Perf

- **macOS descriptor path: zero by construction** — every fast path is cfg'd
  out; the only shared-code touch (`Segment.name`) sits on create/attach/unlink,
  not on any per-descriptor operation. Gate re-run post-change (release,
  M4 Max, ≥2 runs): pin p50 42 ns flat 1→10 k versions, as_arrow p50
  208–250 ns, Replace commit p50 ~1.2 µs flat — at reference.
- **Linux publish hot path**: one `fetch_add` on the doorbell word, and only
  inside the pre-existing `waiters > 0` gate; `FUTEX_WAKE` is fire-and-forget
  best-effort, preserving the wait-free publish contract.
- **All Linux performance numbers ship unmeasured.** The only Linux available
  is an aarch64 VM-backed container; wake-latency numbers under VM scheduling
  are not representative, and ADR-0004 itself flagged the idle-wakeup
  bottleneck as unproven. The expected win (µs-scale idle wake vs the pipe's
  poll path) is stated as an expectation, not a measurement.

## Verification actually performed (and its honest limits)

1. **macOS gate** — full workspace suite + clippy + loom, unchanged baseline.
2. **Cross-target compile assurance on the dev box** —
   `scripts/linux-check.sh`: `cargo check` + `cargo clippy -D warnings`,
   `--workspace --all-targets --target x86_64-unknown-linux-gnu` (target std
   installed via `rustup target add`; check/clippy need no linker). Every
   cfg(linux) line, libc constants included, gets compile-and-lint proof.
3. **Real Linux execution in Docker** — `scripts/linux-test.sh`: the full
   workspace suite inside `rust:1.91` with `--shm-size=1g` (Docker's default
   64 MB `/dev/shm` is too small for the named segments) and an isolated
   in-repo `target_linux/` (gitignored). The container kernel (6.x, aarch64)
   supports futex, eventfd, memfd seals, and `pidfd_open`, so the Linux-only
   tests below **execute**, not just compile.
4. **Linux-only tests** (`#[cfg(target_os = "linux")]`): eventfd
   ring/park/drain + un-rung timeout; futex wake + stale-value immediate
   return; sealed segment refuses shrink *and* grow with `EPERM` while
   mappings stay valid, adopts by fd, and has no namespace entry; pidfd
   readable on child exit, not on a live process; monotonic clock sanity;
   `FutexNotifier`/`FutexParker` waking a genuinely idle ring subscriber
   promptly, plus the bounded-timeout recovery of a notifier-less publish; and
   the pidfd **reclaim race**: a kill-9'ed pin-holding worker is reclaimed in
   seconds under a 60 s lease deadline — a test that fails by construction
   without the pidfd wiring, since leases alone would need the full minute.

**Unexecuted residue** (what ships without having run): native-Linux execution
on real hardware, x86_64 execution of any kind (compile-checked only; the
container is aarch64), all Linux perf numbers, and the authored-but-runnerless
CI linux leg. No loom model was added: the futex parker's miss window
reduces to the pipe parker's existing bounded-timeout argument (single-phase
`Parker`, no new protocol branch), so there is no new interleaving structure to
model — revisit if Phase 1 introduces the two-phase park.

## Addendum — 2026-08-28, post-review

An adversarial read of this tree found `Coordinator::peer_pidfd` left as an
**ablation stub** (`// AB-CHECK: pidfd acquisition disabled`, returning `None`)
— an A/B experiment ("does the reclaim test really fail without pidfd?") whose
body was never restored before the implementing run was stopped. Consequences
while the stub stood, all Linux-only: every `ActorEntry.pidfd` was `None`, the
lease monitor degraded to a plain sleep, Decision 3 was not implemented,
`linux_fast_paths::pidfd_reclaims_killed_worker_long_before_the_lease_deadline`
failed by construction, and the unused `AsRawFd` import failed the Linux
`clippy -D warnings` gate. macOS was never affected (the function is cfg'd out).

Restored: `socket_peer_pid(stream) → pidfd_open(pid)`, best-effort `None` on
either failure. Re-verified on this box: `scripts/linux-check.sh` equivalent
(`cargo clippy --target x86_64-unknown-linux-gnu --all-targets -D warnings`)
clean. **Not re-verified:** execution of any kind. §Verification items 3 and 4
above describe a container run that may have predated the stub; treat them as
unconfirmed for this tree until the Linux-only tests are run again on a Linux
machine. The CI `test` job loop also now includes `shm-store`, which it had
omitted.
