use serde::Deserialize;

/// The initial buffer size workers should use for temporary buffers.
pub const BUF_SIZE: usize = 4096;

/// If instructed to wait, workers should wait `WAIT_TIME_MS` milliseconds
/// before requesting another task from the coordinator.
pub const WAIT_TIME_MS: u64 = 1000;

/// How often workers should send heartbeats to the coordinator.
pub const HEARTBEAT_INTERVAL_MS: u64 = 2000;

/// The port that worker 0 should use.
///
/// Worker 1 should listen on port `INITIAL_WORKER_PORT + 1`,
/// worker 2 should listen on port `INITIAL_WORKER_PORT + 2`, and so on.
pub const INITIAL_WORKER_PORT: u16 = 10163;

/// How long MapReduce clients should between RPC calls to `PollJob`.
pub const POLL_JOB_TIMEOUT_MS: u64 = 250;

/// The maximum allowable time it can take for a worker to start up.
///
/// The autograder will assume that after `WORKER_STARTUP_MS` milliseconds
/// have passed from starting a worker, the worker is fully initialized
/// and ready to receive tasks from the coordinator.
pub const WORKER_STARTUP_MS: u64 = 500;

#[derive(Debug, Clone, Deserialize)]
pub struct WorkerConfig {
    pub buf_size: usize,
    pub wait_time_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub initial_worker_port: u16,
    pub poll_job_timeout_ms: u64,
    pub worker_startup_ms: u64,
}

fn default_buf_size() -> usize {
    BUF_SIZE
}

fn default_wait_time_ms() -> u64 {
    WAIT_TIME_MS
}

fn default_heartbeat_interval_ms() -> u64 {
    HEARTBEAT_INTERVAL_MS
}

fn default_initial_worker_port() -> u16 {
    INITIAL_WORKER_PORT
}

fn default_poll_job_timeout_ms() -> u64 {
    POLL_JOB_TIMEOUT_MS
}

fn default_worker_startup_ms() -> u64 {
    WORKER_STARTUP_MS
}

impl WorkerConfig {
    pub fn from_env() -> Self {
        let _ = dotenvy::dotenv();
        Self {
            buf_size: std::env::var("BUF_SIZE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(default_buf_size),
            wait_time_ms: std::env::var("WAIT_TIME_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(default_wait_time_ms),
            heartbeat_interval_ms: std::env::var("HEARTBEAT_INTERVAL_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(default_heartbeat_interval_ms),
            initial_worker_port: std::env::var("INITIAL_WORKER_PORT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(default_initial_worker_port),
            poll_job_timeout_ms: std::env::var("POLL_JOB_TIMEOUT_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(default_poll_job_timeout_ms),
            worker_startup_ms: std::env::var("WORKER_STARTUP_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(default_worker_startup_ms),
        }
    }
}
