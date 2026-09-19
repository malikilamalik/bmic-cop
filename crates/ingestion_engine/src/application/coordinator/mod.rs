//! The MapReduce coordinator.

use anyhow::Result;
use pkg::proto::coordinator::*;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::Instant;
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};

use pkg::log;

use pkg::config::coordinator::CoordinatorConfig;

#[derive(Debug, Clone, PartialEq, Eq)]
enum TaskState {
    Idle,
    InProgress,
    Completed,
}

#[derive(Debug, Clone)]
struct TaskRecord {
    state: TaskState,
    worker_id: Option<u32>,
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
    job_id: u32,
    files: Vec<String>,
    output_dir: String,
    app: String,
    n_map: u32,
    map_tasks: Vec<TaskRecord>,
    reduce_tasks: Vec<TaskRecord>,
    done: bool,
    failed: bool,
    key: String,
    entity: String,
    errors: Vec<String>,
}

struct Inner {
    next_job_id: u32,
    next_worker_id: u32,
    jobs: HashMap<u32, Job>,
    job_order: VecDeque<u32>,
    workers: HashMap<u32, Instant>,
}

pub struct Coordinator {
    config: CoordinatorConfig,
    inner: Arc<Mutex<Inner>>,
}

impl Coordinator {
    pub fn new(config: &CoordinatorConfig) -> Self {
        Self {
            config: config.clone(),
            inner: Arc::new(Mutex::new(Inner {
                next_job_id: config.initial_job_id,
                next_worker_id: config.initial_worker_id,
                jobs: HashMap::new(),
                job_order: VecDeque::new(),
                workers: HashMap::new(),
            })),
        }
    }

    fn is_worker_alive(&self, workers: &HashMap<u32, Instant>, wid: u32) -> bool {
        let config = self.config.clone();
        if let Some(ts) = workers.get(&wid) {
            ts.elapsed() < Duration::from_secs(config.task_timeout_secs)
        } else {
            false
        }
    }

    fn is_worker_crashed(&self, workers: &HashMap<u32, Instant>, wid: u32) -> bool {
        !Self::is_worker_alive(&self, workers, wid)
    }
}

#[tonic::async_trait]
impl coordinator_server::Coordinator for Coordinator {
    async fn heartbeat(
        &self,
        req: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatReply>, Status> {
        let r = req.into_inner();
        let mut inner = self.inner.lock().await;
        inner.workers.insert(r.worker_id, Instant::now());
        Ok(Response::new(HeartbeatReply {}))
    }

    async fn register(
        &self,
        _req: Request<RegisterRequest>,
    ) -> Result<Response<RegisterReply>, Status> {
        let mut inner = self.inner.lock().await;
        let wid = inner.next_worker_id;
        inner.next_worker_id += 1;
        inner.workers.insert(wid, Instant::now());
        log::info!("Registered worker {}", wid);
        Ok(Response::new(RegisterReply { worker_id: wid }))
    }

    async fn submit_job(
        &self,
        req: Request<SubmitJobRequest>,
    ) -> Result<Response<SubmitJobReply>, Status> {
        let r = req.into_inner();
        let mut inner = self.inner.lock().await;
        let job_id = inner.next_job_id;
        inner.next_job_id += 1;


        let n_map = r.files.len() as u32;
        let map_tasks = (0..n_map).map(|_| TaskRecord::idle()).collect::<Vec<_>>();
        let reduce_tasks = (0..r.n_reduce).map(|_| TaskRecord::idle()).collect::<Vec<_>>();

        let job = Job {
            job_id,
            files: r.files,
            output_dir: r.output_dir,
            app: r.app,
            n_map,
            map_tasks,
            done: false,
            failed: false,
            reduce_tasks,
            errors: Vec::new(),
            key: r.key,
            entity: r.entity
        };

        //Queue job
        inner.jobs.insert(job_id, job);
        inner.job_order.push_back(job_id);

        log::info!("SubmitJob assigned job_id {}", job_id);

        Ok(Response::new(SubmitJobReply { job_id }))
    }

    async fn poll_job(
        &self,
        req: Request<PollJobRequest>,
    ) -> Result<Response<PollJobReply>, Status> {
        let r = req.into_inner();
        let inner = self.inner.lock().await;
        let job = inner.jobs.get(&r.job_id);
        match job {
            None => Err(Status::new(Code::NotFound, "job id is invalid")),
            Some(j) => Ok(Response::new(PollJobReply {
                id: j.job_id,
                done: j.done,
                failed: j.failed,
                errors: j.errors.clone(),
            })),
        }
    }

    async fn fail_task(
        &self,
        _req: Request<FailTaskRequest>,
    ) -> Result<Response<FailTaskReply>, Status> {
        Ok(Response::new(FailTaskReply {}))
    }

    async fn finish_task(
        &self,
        req: Request<FinishTaskRequest>,
    ) -> Result<Response<FinishTaskReply>, Status> {
        let r = req.into_inner();
        let mut inner = self.inner.lock().await;
        // Snapshot workers to avoid borrow conflicts while holding job mut
        let workers_snapshot = inner.workers.clone();
        let job = inner.jobs.get_mut(&r.job_id);
        if job.is_none() {
            return Err(Status::new(Code::NotFound, "job id is invalid"));
        }
        let job = job.unwrap();
        if job.failed || job.done {
            return Ok(Response::new(FinishTaskReply {}));
        }

        // Handle map vs reduce correctly (worker sends reduce flag)
        let (tasks, kind) = if r.reduce {
            (&mut job.reduce_tasks, "reduce")
        } else {
            (&mut job.map_tasks, "map")
        };
        if (r.task as usize) >= tasks.len() {
            return Err(Status::new(Code::InvalidArgument, "invalid task number"));
        }
        let rec = &mut tasks[r.task as usize];

        if rec.state == TaskState::Completed {
            return Ok(Response::new(FinishTaskReply {}));
        }
        rec.state = TaskState::Completed;
        rec.worker_id = Some(r.worker_id);
        rec.assigned_at = Some(Instant::now());
        log::info!(
            "Finish {} task {} job {} worker {}",
            kind, r.task, r.job_id, r.worker_id
        );
        // Check if job done
        let all_done = job
            .reduce_tasks
            .iter()
            .all(|t| t.state == TaskState::Completed)
            && job.map_tasks.iter().all(|t| {
                if t.state != TaskState::Completed {
                    return false;
                }
                if let Some(wid) = t.worker_id {
                    if let Some(ts) = workers_snapshot.get(&wid) {
                        ts.elapsed() < Duration::from_secs(self.config.task_timeout_secs)
                    } else {
                        false
                    }
                } else {
                    false
                }
            });
        if all_done {
            job.done = true;
            log::info!("Job {} marked done after reduce finish", r.job_id);
        }

        Ok(Response::new(FinishTaskReply {}))
    }

    async fn get_task(
        &self,
        req: Request<GetTaskRequest>,
    ) -> Result<Response<GetTaskReply>, Status> {
        let r = req.into_inner();
        let worker_id = r.worker_id;
        let mut inner = self.inner.lock().await;

        // Ensure worker entry exists (in case heartbeat map missing) – update liveness? Not as heartbeat but keep entry.
        if !inner.workers.contains_key(&worker_id) {
            // Unknown worker, still insert with now to avoid immediate crash detection.
            inner.workers.insert(worker_id, Instant::now());
        }

        // Iterate jobs in order
        let job_order = inner.job_order.clone();
        for job_id in job_order {
            let job_failed;
            let job_done;
            let job = inner.jobs.get(&job_id).unwrap();
            {
                job_failed = job.failed;
                job_done = job.done;
            }
            if job_failed || job_done {
                continue;
            }

            // Check map phase: are all map tasks completed with alive workers?
            let all_maps_done = {
                job.map_tasks.iter().all(|t| {
                    if t.state != TaskState::Completed {
                        return false;
                    }
                    if let Some(wid) = t.worker_id {
                        Self::is_worker_alive(&self, &inner.workers, wid)
                    } else {
                        false
                    }
                })
            };

            if !all_maps_done {
                // Look for available map task
                let mut map_idx: Option<usize> = None;
                {
                    let job = inner.jobs.get(&job_id).unwrap();
                    for (i, t) in job.map_tasks.iter().enumerate() {
                        let available = match t.state {
                            TaskState::Idle => true,
                            TaskState::InProgress => {
                                if let Some(wid) = t.worker_id {
                                    Self::is_worker_crashed(&self, &inner.workers, wid)
                                } else {
                                    true
                                }
                            }
                            TaskState::Completed => {
                                if let Some(wid) = t.worker_id {
                                    Self::is_worker_crashed(&self, &inner.workers, wid)
                                } else {
                                    false
                                }
                            }
                        };
                        if available {
                            map_idx = Some(i);
                            break;
                        }
                    }
                }
                if let Some(idx) = map_idx {
                    // Assign it
                    let job = inner.jobs.get_mut(&job_id).unwrap();
                    let task_rec = &mut job.map_tasks[idx];
                    task_rec.state = TaskState::InProgress;
                    task_rec.worker_id = Some(worker_id);
                    task_rec.assigned_at = Some(Instant::now());

                    log::info!(
                        "Assign map task {} of job {} to worker {}",
                        idx,
                        job_id,
                        worker_id
                    );

                    let reply = GetTaskReply {
                        job_id,
                        output_dir: job.output_dir.clone(),
                        file: job.files[idx].clone(),
                        wait: false,
                        task: idx as u32,
                        reduce: false,
                        n_reduce: job.reduce_tasks.len() as u32,
                        app: job.app.clone(),
                        key: job.key.clone(),
                        entity: job.entity.clone(),
                        map_task_assignments: Vec::new(),
                    };
                    return Ok(Response::new(reply));
                }
            } else {
                // All maps done -> look for reduce task
                let mut reduce_idx: Option<usize> = None;
                {
                    let job = inner.jobs.get(&job_id).unwrap();
                    for (i, t) in job.reduce_tasks.iter().enumerate() {
                        let available = match t.state {
                            TaskState::Idle => true,
                            TaskState::InProgress => {
                                if let Some(wid) = t.worker_id {
                                    Self::is_worker_crashed(&self, &inner.workers, wid)
                                } else {
                                    true
                                }
                            }
                            TaskState::Completed => {
                                if let Some(wid) = t.worker_id {
                                    Self::is_worker_crashed(&self, &inner.workers, wid)
                                } else {
                                    false
                                }
                            }
                        };
                        if available {
                            reduce_idx = Some(i);
                            break;
                        }
                    }
                }
                if let Some(idx) = reduce_idx {
                    let job = inner.jobs.get_mut(&job_id).unwrap();
                    let task_rec = &mut job.reduce_tasks[idx];
                    task_rec.state = TaskState::InProgress;
                    task_rec.worker_id = Some(worker_id);
                    task_rec.assigned_at = Some(Instant::now());

                    log::info!(
                        "Assign reduce task {} of job {} to worker {}",
                        idx,
                        job_id,
                        worker_id
                    );

                    // Build map assignments
                    let assignments = job
                        .map_tasks
                        .iter()
                        .enumerate()
                        .map(|(mi, mt)| MapTaskAssignment {
                            task: mi as u32,
                            worker_id: mt.worker_id.unwrap(),
                        })
                        .collect::<Vec<_>>();

                    let reply = GetTaskReply {
                        job_id,
                        output_dir: job.output_dir.clone(),
                        file: String::new(), // reduce has no input file
                        wait: false,
                        task: idx as u32,
                        reduce: true,
                        n_reduce: job.reduce_tasks.len() as u32,
                        app: job.app.clone(),
                        key: job.key.clone(),
                        entity: job.entity.clone(),
                        map_task_assignments: assignments
                    };
                    return Ok(Response::new(reply));
                }
            }
        }

        // 1s delay before dispatching job to worker (as requested)
        tokio::time::sleep(Duration::from_secs(1)).await;

        // No tasks available across all jobs -> send demo file-read job (with correct app/n_reduce for benefit-evaluator)
        Ok(Response::new(GetTaskReply {
            job_id: 0,
            output_dir: "../../../data/output/transaction.txt".to_string(),
            task: 0,
            file: "../../../data/transaction/transaction20260914.txt".to_string(),
            wait: true,
            reduce: false,
            n_reduce: 4,
            app: "benefit-evaluator".to_string(),
            key: "".to_string(),
            entity: "".to_string(),
            map_task_assignments: Vec::new()
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Request;

    fn test_config() -> CoordinatorConfig {
        CoordinatorConfig {
            host: "127.0.0.1".into(),
            port: 0,
            initial_worker_id: 0,
            initial_job_id: 0,
            task_timeout_secs: 7,
            coordinator_startup_ms: 1000,
        }
    }

    #[tokio::test]
    async fn poll_job_not_found_for_unknown_id() {
        let coord = Coordinator::new(&test_config());
        let svc = coord;
        let req = Request::new(PollJobRequest { job_id: 999 });
        let res = coordinator_server::Coordinator::poll_job(&svc, req).await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().code(), Code::NotFound);
    }

    #[tokio::test]
    async fn poll_job_returns_pending_job_with_id_zero_and_defaults() {
        let coord = Coordinator::new(&test_config());
        // Submit a job to create job_id 0 (SubmitJob increments from initial_job_id 0)
        let submit_req = Request::new(SubmitJobRequest {
            files: vec!["data/transaction/transaction20260914.txt".into()],
            output_dir: "/tmp/out".into(),
            app: "test".into(),
            n_reduce: 1,
            key: "".into(),
            entity: "".into()
        });
        let submit_resp = coordinator_server::Coordinator::submit_job(&coord, submit_req)
            .await
            .expect("submit should succeed");
        assert_eq!(submit_resp.into_inner().job_id, 0);

        // Poll the just-created job. Proto3 default values (0/false) are omitted by
        // `grpcurl` unless `-emit-defaults` is used, so `grpcurl ... PollJob`
        // showing `{}` is actually `{id:0, done:false, failed:false, errors:[]}`.
        let poll_req = Request::new(PollJobRequest { job_id: 0 });
        let poll_resp = coordinator_server::Coordinator::poll_job(&coord, poll_req)
            .await
            .expect("poll should succeed for existing job");
        let reply = poll_resp.into_inner();
        assert_eq!(reply.id, 0);
        assert_eq!(reply.done, false);
        assert_eq!(reply.failed, false);
        assert!(reply.errors.is_empty());
        // Demonstrate JSON omission: prost -> serde_json would also hide defaults if configured unconditionally,
        // but the struct itself holds the values. grpcurl needs `-emit-defaults` to see them.
    }

    #[tokio::test]
    async fn submit_then_poll_reflects_job_lifecycle() {
        let coord = Coordinator::new(&test_config());
        // Initially no jobs -> NotFound
        let not_found = coordinator_server::Coordinator::poll_job(
            &coord,
            Request::new(PollJobRequest { job_id: 0 }),
        )
        .await;
        assert_eq!(not_found.unwrap_err().code(), Code::NotFound);

        // Submit two jobs -> ids 0 and 1
        for expected_id in [0u32, 1] {
            let r = coordinator_server::Coordinator::submit_job(
                &coord,
                Request::new(SubmitJobRequest {
                    files: vec!["f".into()],
                    output_dir: "/tmp".into(),
                    app: "app".into(),
                    n_reduce: 1,
                    key: "".into(),
                    entity: "".into()
                }),
            )
            .await
            .unwrap();
            assert_eq!(r.into_inner().job_id, expected_id);
        }

        // Both jobs pending
        for job_id in [0, 1] {
            let reply = coordinator_server::Coordinator::poll_job(
                &coord,
                Request::new(PollJobRequest { job_id }),
            )
            .await
            .unwrap()
            .into_inner();
            assert_eq!(reply.id, job_id);
            assert_eq!(reply.done, false);
        }
    }
}

pub async fn start(config: &CoordinatorConfig) -> Result<()> {
    let coordinator = Coordinator::new(config);
    let svc = coordinator_server::CoordinatorServer::new(coordinator);
    let reflection_svc = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(pkg::proto::FILE_DESCRIPTOR_SET)
        .build_v1()
        .unwrap();
    Server::builder()
        .add_service(svc)
        .add_service(reflection_svc)
        .serve(config.addr().parse().unwrap())
        .await?;
    Ok(())
}
