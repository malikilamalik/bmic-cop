//! cm-scheduler binary — hits `coordinator.Coordinator/SubmitJob` via gRPC.
//!
//! Programmatic equivalent of:
//! ```bash
//! grpcurl -plaintext -d '{
//!     "files": ["data/transaction/transaction20260618.txt"],
//!     "output_dir": "data/output",
//!     "app": "benefit-evaluator",
//!     "n_reduce": 2,
//!     "key": "transaction_today",
//!     "entity": "transaction"
//! }' localhost:10162 coordinator.Coordinator/SubmitJob
//! ```
//! Note: `files` is a JSON array (`repeated string` in proto).

use core_module::application::scheduler::{
    build_submit_job_request, default_submit_job_request, Scheduler,
};
use pkg::log;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    log::init_logger();

    let sched = Scheduler::from_env();
    let args: Vec<String> = std::env::args().collect();

    // --loop: run on cron, each tick does SubmitJob (like a scheduled grpcurl)
    // default (no args): one-shot SubmitJob matching the grpcurl example
    let is_loop = args.iter().any(|a| a == "--loop" || a == "loop")
        || std::env::var("SCHEDULER_LOOP")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

    if is_loop {
        log::info!("cm-scheduler running in loop mode (cron='{}')", sched.config().cron);
        // Each tick submits the default job — array `files` as in grpcurl -d
        sched.run_with_submit().await;
        return Ok(());
    }

    // One-shot mode: allow overriding files via CLI args after `--`
    // e.g. `cargo run -p core_module --bin cm-scheduler -- data/transaction/a.txt data/transaction/b.txt`
    // or env `SCHEDULER_FILES="a.txt,b.txt"` (comma-separated)
    let files: Vec<String> = if args.len() > 1 && !args[1].starts_with('-') {
        // args after binary name are treated as file list
        args[1..].iter().filter(|a| !a.starts_with('-')).cloned().collect()
    } else if let Ok(env_files) = std::env::var("SCHEDULER_FILES") {
        if env_files.trim().is_empty() {
            default_submit_job_request().files
        } else {
            env_files
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        }
    } else {
        // default exactly like the grpcurl -d example — single-element array
        default_submit_job_request().files
    };

    // Allow overrides via env for the other fields (defaults match grpcurl example)
    let output_dir = std::env::var("SCHEDULER_OUTPUT_DIR").unwrap_or_else(|_| "data/output".into());
    let app = std::env::var("SCHEDULER_APP").unwrap_or_else(|_| "benefit-evaluator".into());
    let n_reduce: u32 = std::env::var("SCHEDULER_N_REDUCE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2);
    let key = std::env::var("SCHEDULER_KEY").unwrap_or_else(|_| "transaction_today".into());
    let entity = std::env::var("SCHEDULER_ENTITY").unwrap_or_else(|_| "transaction".into());

    let req = build_submit_job_request(files.clone(), output_dir.clone(), app.clone(), n_reduce, key.clone(), entity.clone());

    log::info!(
        "cm-scheduler SubmitJob files={:?} output_dir={} app={} n_reduce={} key={} entity={}",
        req.files, req.output_dir, req.app, req.n_reduce, req.key, req.entity
    );

    match sched.submit_job_request(req).await {
        Ok(job_id) => {
            // Print JSON like grpcurl would: {"jobId": 0} / {"job_id": 0}
            println!(r#"{{"job_id": {}}}"#, job_id);
            log::info!("SubmitJob success job_id={}", job_id);
            Ok(())
        }
        Err(e) => {
            log::error!("SubmitJob failed: {:?}", e);
            eprintln!("SubmitJob failed: {:#}", e);
            std::process::exit(1);
        }
    }
}
