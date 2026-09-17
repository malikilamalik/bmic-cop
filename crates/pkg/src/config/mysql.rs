use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct MysqlConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    #[serde(default = "default_max_conn")]
    pub max_connections: u32,
}

fn default_max_conn() -> u32 {
    50
}

impl MysqlConfig {
    pub fn from_env() -> Self {
        let _ = dotenvy::dotenv();
        Self {
            host: std::env::var("MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".into()),
            port: std::env::var("MYSQL_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(3306),
            user: std::env::var("MYSQL_USER").unwrap_or_else(|_| "root".into()),
            password: std::env::var("MYSQL_PASSWORD").unwrap_or_default(),
            database: std::env::var("MYSQL_DATABASE").unwrap_or_default(),
            max_connections: std::env::var("MYSQL_MAX_CONN")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(50),
        }
    }

    pub fn dsn(&self) -> String {
        format!(
            "mysql://{}:{}@{}:{}/{}",
            self.user, self.password, self.host, self.port, self.database
        )
    }
}
