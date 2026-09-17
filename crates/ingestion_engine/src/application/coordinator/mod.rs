//! The MapReduce coordinator.

use anyhow::Result;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::Instant;
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};

use pkg::log;


// Local shims for missing app/args/constants when running as binary
// The coordinator binary is standalone; these definitions satisfy the
// references that previously used `crate::app` and `args`.
type JobId = u32;
type WorkerId = u32;
const INITIAL_JOB_ID: JobId = 0;
const INITIAL_WORKER_ID: WorkerId = 1;
const TASK_TIMEOUT_SECS: u64 = 10;
const COORDINATOR_ADDR: &str = "127.0.0.1:50051";
mod app {
    pub fn try_named(_: &str) -> Option<()> {
        Some(())
    }
}
mod args {
    #[derive(Debug)]
    pub struct Args;
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TaskState {
    Idle,
    InProgress,
    Completed,
}

#[derive(Debug, Clone)]
struct TaskRecord {
    state: TaskState,
    worker_id: Option<WorkerId>,
    assigned_at: Option<Instant>,
}

impl TaskRecord {
    fn idle() -> Self {
        Self {
            state: TaskState::Idle,
            worker_id: None,
            assigned_at: None,
        }
    }
}

struct Job {
    job_id: JobId,
    files: Vec<String>,
    output_dir: String,
    app: String,
    n_reduce: u32,
    n_map: u32,
    map_tasks: Vec<TaskRecord>,
    reduce_tasks: Vec<TaskRecord>,
    done: bool,
    failed: bool,
    errors: Vec<String>,
}

struct Inner {
    next_job_id: JobId,
    next_worker_id: WorkerId,
    jobs: HashMap<JobId, Job>,
    job_order: VecDeque<JobId>,
    workers: HashMap<WorkerId, Instant>,
}

pub struct Coordinator {
    inner: Arc<Mutex<Inner>>,
}

impl Coordinator {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                next_job_id: INITIAL_JOB_ID,
                next_worker_id: INITIAL_WORKER_ID,
                jobs: HashMap::new(),
                job_order: VecDeque::new(),
                workers: HashMap::new(),
            })),
        }
    }

    fn is_worker_alive(workers: &HashMap<WorkerId, Instant>, wid: WorkerId) -> bool {
        if let Some(ts) = workers.get(&wid) {
            ts.elapsed() < Duration::from_secs(TASK_TIMEOUT_SECS)
        } else {
            false
        }
    }

    fn is_worker_crashed(workers: &HashMap<WorkerId, Instant>, wid: WorkerId) -> bool {
        !Self::is_worker_alive(workers, wid)
    }
}



pub async fn start() -> Result<()> {
    let _ = Coordinator::new();
    Ok(())
}
