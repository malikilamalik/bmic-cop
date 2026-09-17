use core::error;

use chrono::Local;
use ingestion_engine::interface::repository::job::{JobFilter, JobRepository};
use ingestion_engine::interface::repository::job_detail::{JobDetailFilter, JobDetailRepository};
use ingestion_engine::internal::repository::sql::job::MySqlJobRepository;
use ingestion_engine::internal::repository::sql::job_detail::MySqlJobDetailRepository;
use pkg::config::mysql::MysqlConfig;

#[tokio::main]
async fn main() {
    let today = Local::now().date_naive();
    println!("today: {today}");

    let mysql_config = MysqlConfig::from_env();
    println!(
        "connecting to {}:{}/datamart as {}",
        mysql_config.host, mysql_config.port, mysql_config.user
    );

    let pool = match pkg::mysql::init(&mysql_config).database("datamart").await {
        Ok(pool) => {
            println!("connected to datamart");
            pool
        }
        Err(e) => {
            eprintln!(
                "db connect failed ({}): {e}",
                mysql_config.dsn().replace(&mysql_config.password, "***")
            );
            println!("today: {today} — no db, skipping list");
            return;
        }
    };

    let repo = MySqlJobDetailRepository::new(pool.clone());
    let job_repo: MySqlJobRepository = MySqlJobRepository::new(pool);

    // Filter by job_id as requested - prints to console
    let filter = JobDetailFilter {
        job_id: Some(1),
        ..Default::default()
    };
    println!("listing job_detail by job_id=1...");
    match repo.list(&filter).await {
        Ok(rows) => {
            println!("found {} rows for job_id=1", rows.len());
            for row in &rows {
                println!(
                    " - id={} job_id={} entity={} key={} start={:?} end={:?}",
                    row.id,
                    row.job_id,
                    row.entity,
                    row.key,
                    row.file_start_range,
                    row.file_end_range
                );
            }
            match serde_json::to_string_pretty(&rows) {
                Ok(j) => println!("json:\n{j}"),
                Err(e) => eprintln!("json serialize failed: {e}"),
            }
        }
        Err(e) => eprintln!("list failed: {e}"),
    }

    // Also demonstrate no filter
    let filter_all = JobDetailFilter::default();
    match repo.list(&filter_all).await {
        Ok(rows) => println!("all rows count: {}", rows.len()),
        Err(e) => eprintln!("list all failed: {e}"),
    }

    let filter_evaluator = JobFilter::default();
    match job_repo.get(&filter_evaluator).await {
        Ok(job) => {
            println!(
                " - id={} evaluator_id={}  created_at={:?} updated_at={:?}",
                job.id, job.evaluator_id, job.created_at, job.updated_at
            );
            match serde_json::to_string_pretty(&job) {
                Ok(j) => println!("json:\n{j}"),
                Err(e) => eprintln!("json serialize failed: {e}"),
            }
        }
        Err(e) => eprintln!("list all failed: {e}"),
    }
}
