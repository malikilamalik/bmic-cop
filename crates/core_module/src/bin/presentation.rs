use core_module::application::presentation::evaluator::start_presentation_server;
use pkg::config::mysql::MysqlConfig;
use pkg::mysql;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    pkg::log::init_logger();
    let cfg = MysqlConfig::from_env();
    let pool = mysql::init(&cfg).database("business_rule").await?;
    // Use PRESENTATION_ADDR env or default 127.0.0.1:8080
    let addr = std::env::var("PRESENTATION_ADDR").unwrap_or_else(|_| "127.0.0.1:8080".to_string());
    println!("starting presentation server on {}", addr);
    start_presentation_server(pool, &addr).await?;
    Ok(())
}
