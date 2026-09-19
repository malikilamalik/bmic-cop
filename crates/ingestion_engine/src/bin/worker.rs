//! The MapReduce worker.
//!
// Do not modify this file.

use anyhow::Result;
use ingestion_engine::application;
use pkg::{config::{coordinator::CoordinatorConfig, worker::WorkerConfig}, log};




#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let  coordinator_config = CoordinatorConfig::from_env();
    let  worker_config = WorkerConfig::from_env();
    log::init_logger();
    application::worker::start(&coordinator_config, &worker_config).await
}

