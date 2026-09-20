use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct IngestionConfig {
    pub host: String,
    pub port: u16,
}

fn default_host() -> String {
    "127.0.0.1".into()
}

fn default_port() -> u16 {
    50051
}

impl IngestionConfig {
    pub fn from_env() -> Self {
        let _ = dotenvy::dotenv();
        // Primary: INGESTION_ADDR like "127.0.0.1:50051" – matches direct `std::env::var("INGESTION_ADDR")` usage
        if let Ok(addr) = std::env::var("INGESTION_ADDR") {
            if let Some((h, p)) = addr.rsplit_once(':') {
                if let Ok(port) = p.parse::<u16>() {
                    let host = if h.is_empty() { default_host() } else { h.to_string() };
                    return Self { host, port };
                }
            }
            // if INGESTION_ADDR is set but unparseable, treat as host:port fallback to defaults
            // else continue to host/port vars
        }
        Self {
            host: std::env::var("INGESTION_HOST").unwrap_or_else(|_| default_host()),
            port: std::env::var("INGESTION_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or_else(default_port),
        }
    }

    pub fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}
