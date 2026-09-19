use serde::Deserialize;



#[derive(Debug, Clone, Deserialize)]
pub struct CoordinatorConfig {
    pub host: String,
    pub port: u16,
    pub initial_worker_id: u32,
    pub initial_job_id: u32,
    pub task_timeout_secs: u64,
    pub coordinator_startup_ms: u64,
}

fn default_host() -> String {
    "127.0.0.1".into()
}

fn default_port() -> u16 {
    10162
}

fn default_initial_worker_id() -> u32 {
    0
}

fn default_initial_job_id() -> u32 {
    0
}

fn default_task_timeout_secs() -> u64 {
    7
}

fn default_coordinator_startup_ms() -> u64 {
    1000
}

impl CoordinatorConfig {
    pub fn from_env() -> Self {
        let _ = dotenvy::dotenv();
        Self {
            host: std::env::var("COORDINATOR_HOST").unwrap_or_else(|_| default_host()),
            port: std::env::var("COORDINATOR_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or_else(default_port),
            initial_worker_id: std::env::var("INITIAL_WORKER_ID")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(default_initial_worker_id),
            initial_job_id: std::env::var("INITIAL_JOB_ID")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(default_initial_job_id),
            task_timeout_secs: std::env::var("TASK_TIMEOUT_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(default_task_timeout_secs),
            coordinator_startup_ms: std::env::var("COORDINATOR_STARTUP_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(default_coordinator_startup_ms),
        }
    }

    pub fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}
