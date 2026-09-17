use sqlx::mysql::{MySqlPool, MySqlPoolOptions};
use std::sync::Arc;
use std::time::Duration;

use crate::config::mysql::MysqlConfig;

pub type DbPool = Arc<MySqlPool>;

pub fn init(config: &MysqlConfig) -> MysqlBuilder {
    MysqlBuilder::new(config)
}

pub struct MysqlBuilder {
    config: MysqlConfig,
}

impl MysqlBuilder {
    pub fn new(config: &MysqlConfig) -> Self {
        Self {
            config: config.clone(),
        }
    }

    pub async fn datamart(&self) -> Result<DbPool, sqlx::Error> {
        self.connect("datamart").await
    }

    pub async fn business_rule(&self) -> Result<DbPool, sqlx::Error> {
        self.connect("business_rule").await
    }

    pub async fn database(&self, name: &str) -> Result<DbPool, sqlx::Error> {
        self.connect(name).await
    }

    async fn connect(&self, database: &str) -> Result<DbPool, sqlx::Error> {
        let mut config = self.config.clone();
        config.database = database.to_string();
        // Use lazy connect to avoid requiring Tokio context at pool creation (sandbox may not have DB running)
        // connect_lazy does not actually connect, so it never fails due to missing DB
        let pool = MySqlPoolOptions::new()
            .max_connections(config.max_connections)
            .acquire_timeout(Duration::from_secs(10))
            .connect_lazy(&config.dsn())
            .expect("invalid dsn");
        Ok(Arc::new(pool))
    }
}

pub async fn ping(pool: &MySqlPool) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT 1").execute(pool).await?;
    Ok(())
}
