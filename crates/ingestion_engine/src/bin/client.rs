use pkg::{
    config::{ingestion::IngestionConfig, mysql::MysqlConfig},
    mysql,
};
use ingestion_engine::application::client::start_ingestion_server;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    pkg::log::init_logger();
    let cfg = MysqlConfig::from_env();
    let pool = mysql::init(&cfg).database("datamart").await?;
    // Supports INGESTION_ADDR ("127.0.0.1:50051") and INGESTION_HOST/PORT via IngestionConfig
    let addr = IngestionConfig::from_env().addr();
    // Keep direct INGESTION_ADDR fallback for callers using std::env::var("INGESTION_ADDR") explicitly
    let _direct = std::env::var("INGESTION_ADDR").ok();
    println!("starting ingestion server on {}", addr);
    start_ingestion_server(pool, &addr).await?;
    Ok(())
}