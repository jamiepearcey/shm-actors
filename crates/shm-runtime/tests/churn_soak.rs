//! v0.4 stage O §4 — **churn soak with a periodic zero-leak census**.
//!
//! N worker processes hammer one shared artifact + task queue with a deterministic
//! (seeded) mix of reclaimable operations — optimistic/exclusive commits,
//! journalled version pins, task submits/claims — while the driver periodically
//! `kill -9`s and restarts one of them. Every K ms the driver snapshots the
//! artifact data pool's free-chunk count. The soak proves there is **no slow
//! leak**: the free count never collapses toward exhaustion during the run, and at
//! quiescence (all workers stopped, every crashed worker's journal replayed) it
//! returns EXACTLY to the one-live-version baseline.
//!
//! Duration is a short CI-friendly default (a few seconds); set `SHM_SOAK_SECS`
//! for a longer opt-in run. Actor count is `SHM_SOAK_ACTORS` (default 4). All
//! randomness (kill victim + restart seeds) is derived from a fixed base seed, so
//! a failure reproduces.

use std::process::{Child, Command};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use shm_arrow::SchemaRegistry;
use shm_core::PoolConfig;
use shm_runtime::demo::{demo_batch, demo_schema};
use shm_runtime::{Coordinator, Node, RuntimeConfig};
use shm_stream::{Commit, Coordination};

const CHURN_ARTIFACT: &str = "churn";

fn unique_seg_base() -> u32 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let pid = std::process::id() as u64;
    1_300_000 + (((pid.wrapping_mul(2_654_435_761)) ^ nanos) % 2_000_000) as u32
}

fn wait_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if cond() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    cond()
}

struct Reaper(Child);
impl Drop for Reaper {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn exe() -> &'static str {
    env!("CARGO_BIN_EXE_shm-cacheloop")
}

fn registry() -> Arc<SchemaRegistry> {
    Arc::new(SchemaRegistry::new())
}

/// Same xorshift64* PRNG the churn role uses — keeps the driver's kill schedule
/// reproducible from a fixed base seed.
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Rng {
        Rng(seed ^ 0x9E37_79B9_7F4A_7C15 | 1)
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

fn spawn_churn(uds: &str, seed: u64) -> Reaper {
    Reaper(
        Command::new(exe())
            .args([
                "churn",
                "--uds",
                uds,
                "--art",
                CHURN_ARTIFACT,
                "--seed",
                &seed.to_string(),
            ])
            .spawn()
            .unwrap_or_else(|e| panic!("spawn churn {seed}: {e}")),
    )
}

/// A churn-friendly config: a roomy artifact data pool so concurrent versions +
/// in-flight staging never exhaust it, which would mask (or masquerade as) a leak.
fn churn_config() -> RuntimeConfig {
    RuntimeConfig {
        artifact_pool: PoolConfig::power_of_two(256, 8192, 64),
        artifact_data_size: 8 << 20,
        ..RuntimeConfig::with_seg_base(unique_seg_base())
    }
}

fn commit_one(node: &Node, expect: u64) -> shm_runtime::Result<u64> {
    let stream = node.stream(CHURN_ARTIFACT)?;
    let mut w = stream.writer(
        Commit::Replace,
        Coordination::Optimistic {
            expect_version: expect,
        },
    )?;
    w.append_batch(&demo_batch())?;
    Ok(w.commit()?)
}

#[test]
fn churn_soak_no_leak() {
    let soak_secs: u64 = std::env::var("SHM_SOAK_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4);
    let actors: usize = std::env::var("SHM_SOAK_ACTORS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4)
        .clamp(2, 8);

    let dir = tempfile::tempdir().expect("tempdir");
    let uds = dir.path().join("coord.sock");
    let uds_s = uds.to_str().unwrap().to_string();

    let mut coord = Coordinator::bind(&uds, churn_config()).expect("bind coordinator");
    coord.start().expect("start coordinator");

    // A long-lived survivor establishes the one-live-version baseline.
    let mut survivor = Node::connect(&uds, "survivor", registry()).expect("survivor connect");
    survivor.start_heartbeat(Duration::from_millis(150));
    survivor
        .open_artifact(CHURN_ARTIFACT)
        .expect("open_artifact");
    survivor
        .intern_schema(&demo_schema())
        .expect("intern schema");
    commit_one(&survivor, 0).expect("survivor commits v1");
    let baseline = coord
        .artifact_free_total(CHURN_ARTIFACT)
        .expect("artifact known");
    assert!(
        baseline > 0,
        "the churn pool must have free chunks to start"
    );

    // Spawn the churn workers (deterministic seeds).
    const BASE_SEED: u64 = 0x00C0_FFEE_1234;
    let mut workers: Vec<Reaper> = (0..actors)
        .map(|i| spawn_churn(&uds_s, BASE_SEED.wrapping_add(i as u64)))
        .collect();

    // --- The soak: sample the census every 100 ms, kill+restart a worker every
    //     ~500 ms. Track min/max free; a monotone decay toward 0 would be a leak. ---
    let mut rng = Rng::new(BASE_SEED);
    let sample_every = Duration::from_millis(100);
    let kill_every = Duration::from_millis(500);
    let end = Instant::now() + Duration::from_secs(soak_secs);
    let mut next_kill = Instant::now() + kill_every;
    let mut restart_counter: u64 = 0;
    let mut min_free = baseline;
    let mut max_free = baseline;
    let mut samples: u64 = 0;

    while Instant::now() < end {
        std::thread::sleep(sample_every);
        let free = coord.artifact_free_total(CHURN_ARTIFACT).unwrap_or(0);
        min_free = min_free.min(free);
        max_free = max_free.max(free);
        samples += 1;
        // Runaway-leak tripwire: with an 8 MiB / 64-per-class pool, a healthy
        // working set of a handful of concurrent versions never gets close to 0.
        assert!(
            free > 0,
            "churn exhausted the pool (free=0) — a runaway leak (min={min_free}, baseline={baseline})"
        );

        if Instant::now() >= next_kill {
            let victim = rng.below(actors as u64) as usize;
            restart_counter += 1;
            // Assigning a new Reaper drops the old one → SIGKILL (kill -9) it, then
            // restart with a fresh deterministic seed.
            workers[victim] = spawn_churn(&uds_s, BASE_SEED.wrapping_add(10_000 + restart_counter));
            next_kill += kill_every;
        }
    }

    // --- Quiescence: stop all churn, let every crashed worker's lease lapse and
    //     its journal replay, then census. The pool must return to the one-live-
    //     version baseline: any un-reclaimed chunk / leaked pin / stuck lease's
    //     staged chunks would leave `free` below it forever.
    //
    //     The churn mix includes `Append` (ADR-0013), so the version the churn
    //     left current may head a manifest *chain* whose members are all
    //     legitimately live. The survivor's clean `Replace` supersedes it, and
    //     this census is therefore also the retire cascade's census: the whole
    //     chain — every member's data + manifest chunk and every link — must
    //     come back. A stale `expect` (a worker's last commit landing late) is
    //     just retried by the poll. ---
    drop(workers);
    let recovered = wait_until(Duration::from_secs(15), || {
        let current = coord.artifact_current_version(CHURN_ARTIFACT).unwrap_or(0);
        let _ = commit_one(&survivor, current);
        coord.artifact_free_total(CHURN_ARTIFACT) == Some(baseline)
    });
    let final_free = coord.artifact_free_total(CHURN_ARTIFACT).unwrap_or(0);

    // Report the observed band (visible with `--nocapture`).
    println!(
        "churn soak: actors={actors} secs={soak_secs} samples={samples} restarts={restart_counter} \
         free[min={min_free} max={max_free} baseline={baseline} final={final_free}] \
         current_version={:?}",
        coord.artifact_current_version(CHURN_ARTIFACT)
    );

    assert!(
        recovered,
        "ZERO-LEAK CENSUS FAILED at quiescence: free={final_free} != baseline={baseline} \
         (min={min_free}, max={max_free})"
    );
    // No net decay: the run ended back at (not below) the baseline it started at.
    assert_eq!(
        final_free, baseline,
        "the pool returned exactly to its one-version baseline"
    );
    assert!(
        max_free <= baseline,
        "free never exceeded the baseline (would imply a version was double-freed): max={max_free}"
    );

    // The artifact is still fully functional after the whole soak.
    let current = coord
        .artifact_current_version(CHURN_ARTIFACT)
        .expect("current known");
    let v = commit_one(&survivor, current).expect("survivor commits a clean version post-soak");
    assert_eq!(
        v,
        current + 1,
        "the artifact still installs clean versions after the soak"
    );
    // Retiring the predecessor is *deferred*, not synchronous: the install only
    // reclaims it if it is unpinned at that instant, and otherwise leaves the
    // retire to the pin-drop that follows. So the census here is the same
    // `wait_until` the quiescence census above uses. Asserting instantly made
    // this line fail ~12% of runs (it did so on the pre-P0.1 tree too) — a race
    // in the test, not in the reclaimer: the footprint always converged.
    assert!(
        wait_until(Duration::from_secs(5), || {
            coord.artifact_free_total(CHURN_ARTIFACT) == Some(baseline)
        }),
        "the clean post-soak version settles to the same one-version footprint \
         (still zero leak): free={:?} baseline={baseline}",
        coord.artifact_free_total(CHURN_ARTIFACT)
    );
}
