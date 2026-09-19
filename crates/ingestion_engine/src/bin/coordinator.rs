use ingestion_engine::application;
use pkg::{config::coordinator::CoordinatorConfig, log};

use anyhow::Result;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let  coordinator_config = CoordinatorConfig::from_env();
    log::init_logger();
    application::coordinator::start(&coordinator_config).await
}
