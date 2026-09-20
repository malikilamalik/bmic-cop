use core_module::application::evaluator::start_evaluator_server;
use pkg::config::evaluator::EvaluatorConfig;
use pkg::config::mysql::MysqlConfig;
use pkg::mysql;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    pkg::log::init_logger();
    let cfg = MysqlConfig::from_env();
    let pool = mysql::init(&cfg).database("business_rule").await?;
    let addr = EvaluatorConfig::from_env().addr();
    println!("starting evaluator server on {}", addr);
    start_evaluator_server(pool, &addr).await?;
    Ok(())
}
