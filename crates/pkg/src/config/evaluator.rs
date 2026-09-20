use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct EvaluatorConfig {
    pub host: String,
    pub port: u16,
}

fn default_host() -> String {
    "127.0.0.1".into()
}
fn default_port() -> u16 {
    50052
}

impl EvaluatorConfig {
    pub fn from_env() -> Self {
        let _ = dotenvy::dotenv();
        if let Ok(addr) = std::env::var("EVALUATOR_ADDR") {
            if let Some((h, p)) = addr.rsplit_once(':') {
                if let Ok(port) = p.parse::<u16>() {
                    let host = if h.is_empty() { default_host() } else { h.to_string() };
                    return Self { host, port };
                }
            }
        }
        Self {
            host: std::env::var("EVALUATOR_HOST").unwrap_or_else(|_| default_host()),
            port: std::env::var("EVALUATOR_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or_else(default_port),
        }
    }
    pub fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}
