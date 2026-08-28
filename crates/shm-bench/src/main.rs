//! `shm-bench` — a first-class, committable performance harness for the
//! `shm-actors` substrate.
//!
//! Every number is **measured on this machine** with a warmup + timed-loop +
//! percentile methodology (see [`stats`]); nothing is extrapolated. On macOS
//! (no futex / memfd sealing) these are the "macOS dev profile" numbers — the
//! design's production target is a Linux x86/ARM server, so the macOS busy-poll
//! and (especially) the doorbell idle-wakeup figures should not be read as the
//! Linux target.
//!
//! ## Usage
//!
//! ```text
//! cargo run --release -p shm-bench -- [suite]
//! ```
//!
//! where `suite` is one of `xproc`, `ring`, `pool`, `artifact`, `arrow`,
//! `task`, or `all` (the default). `xproc` runs first when `all` is selected so
//! the fork happens before any bench thread is spawned.

mod bench_arrow;
mod bench_artifact;
mod bench_pool;
mod bench_ring;
mod bench_task;
mod bench_xproc;
mod fixtures;
mod stats;

fn machine_context() {
    fn sysctl(key: &str) -> String {
        std::process::Command::new("sysctl")
            .args(["-n", key])
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .map(|s| s.trim().to_string())
            .unwrap_or_else(|| "?".to_string())
    }
    println!("shm-bench — MEASURED numbers, macOS dev profile (NOT the Linux target)");
    println!(
        "build: {}",
        if cfg!(debug_assertions) {
            "DEBUG (run with --release!)"
        } else {
            "release"
        }
    );
    println!("cpu:   {}", sysctl("machdep.cpu.brand_string"));
    println!(
        "cores: {} logical / {} physical",
        sysctl("hw.ncpu"),
        sysctl("hw.physicalcpu")
    );
    let os = std::process::Command::new("sw_vers")
        .arg("-productVersion")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_default();
    println!("os:    macOS {os}");
    if cfg!(debug_assertions) {
        eprintln!("WARNING: running a DEBUG build; numbers are not representative. Use --release.");
    }
}

fn main() {
    let suite = std::env::args().nth(1).unwrap_or_else(|| "all".to_string());
    machine_context();

    match suite.as_str() {
        "xproc" => bench_xproc::run(),
        "ring" => bench_ring::run(),
        "pool" => bench_pool::run(),
        "artifact" => bench_artifact::run(),
        "arrow" => bench_arrow::run(),
        "task" => bench_task::run(),
        "all" => {
            // Fork-based cross-process bench FIRST, before any thread is spawned.
            bench_xproc::run();
            bench_ring::run();
            bench_pool::run();
            bench_artifact::run();
            bench_arrow::run();
            bench_task::run();
        }
        other => {
            eprintln!(
                "unknown suite '{other}'; use one of: xproc ring pool artifact arrow task all"
            );
            std::process::exit(2);
        }
    }
    println!("\ndone.");
}
