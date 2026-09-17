//! The MapReduce worker.
//!
// Do not modify this file.

use anyhow::Result;
use ingestion_engine::application;
use pkg::log;


#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    log::init_logger();
    application::worker::start().await
}
