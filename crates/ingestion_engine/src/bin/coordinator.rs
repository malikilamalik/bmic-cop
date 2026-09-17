use ingestion_engine::application;
use pkg::log;

use anyhow::Result;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    log::init_logger();
    application::coordinator::start().await
}
