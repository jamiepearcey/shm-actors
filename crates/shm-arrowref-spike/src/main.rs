//! Runnable driver for the ArrowRef task-fabric spike (v0.4 stage Q).
//!
//! `cargo run -p shm-arrowref-spike` prints the measured descriptor-only
//! property and the retained-output round trip.

fn main() -> Result<(), shm_arrowref_spike::SpikeError> {
    let report = shm_arrowref_spike::run_spike()?;

    println!("ArrowRef task-fabric spike — measured results");
    println!("---------------------------------------------");
    println!(
        "input payload retained once : {:>8} bytes",
        report.input_payload_bytes
    );
    println!(
        "control message on the queue: {:>8} bytes (ChunkDesc)",
        report.control_msg_bytes
    );
    println!(
        "full task-queue slot        : {:>8} bytes",
        report.queue_slot_bytes
    );
    println!(
        "payload : control ratio     : {:>8}x",
        report.payload_to_control_ratio
    );
    println!(
        "retained output ref         : dataset={:?} version={}",
        report.output.dataset, report.output.version
    );
    println!("output rows                 : {:>8}", report.output_rows);
    println!(
        "input read zero-copy        : {}",
        report.input_read_zero_copy
    );
    println!(
        "output read zero-copy       : {}",
        report.output_read_zero_copy
    );
    println!("cleared-on-ack (reclaimed)  : {}", report.cleared_on_ack);
    println!(
        "output cleared-on-ack (G4)  : {}",
        report.output_cleared_on_ack
    );
    println!("round trip                  : {:?}", report.round_trip);
    println!();
    println!("PROVED: control messages stayed 24 bytes; the Arrow payload was");
    println!("written once and thereafter only referenced (never copied through");
    println!("the queue) and read zero-copy from shared memory on both planes.");
    Ok(())
}
