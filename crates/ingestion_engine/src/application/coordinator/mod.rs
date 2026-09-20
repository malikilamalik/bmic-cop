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
use pkg::config::evaluator::EvaluatorConfig;
use pkg::config::mysql::MysqlConfig;
use pkg::proto::evaluator::{
    evaluator_client::EvaluatorClient, Customer, EvaluateRequest, Transaction, TransactionData,
};

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

#[derive(Debug, Clone)]
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
    evaluator_id: u64,
    ingestion_job_id: u64,
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

// ---------------------------------------------------------------------------
// Completion handling: update job/job_file to COMPLETED and call evaluator
// Reads reduced files from data/output/{job_id}/mr-out-{} (per spec)
// ---------------------------------------------------------------------------

async fn handle_job_completion(job: Job) {
    // 1) Update DB: job and job_file to COMPLETED (finished)
    if job.ingestion_job_id != 0 {
        if let Err(e) = mark_job_and_files_completed(job.ingestion_job_id).await {
            log::error!(
                "Job {} (ingestion_job_id={}) failed to update DB to COMPLETED: {:?}",
                job.job_id,
                job.ingestion_job_id,
                e
            );
        } else {
            log::info!(
                "Job {} (ingestion_job_id={}) updated job/job_file to COMPLETED",
                job.job_id,
                job.ingestion_job_id
            );
        }
    } else {
        log::warn!(
            "Job {} has no ingestion_job_id (evaluator_id={}), skipping DB update",
            job.job_id,
            job.evaluator_id
        );
    }

    if job.evaluator_id == 0 {
        log::warn!("Job {} has no evaluator_id, skipping evaluator", job.job_id);
        return;
    }

    // 3) Read reduced outputs from data/output/{job_id}/mr-out-{} — per-customer transaction
    let pairs = match read_reduced_for_job(&job).await {
        Ok(v) => v,
        Err(e) => {
            log::error!("Job {} failed to read reduced files: {:?}", job.job_id, e);
            return;
        }
    };

    if pairs.is_empty() {
        log::warn!("Job {} reduced output empty, skipping evaluator", job.job_id);
        return;
    }

    let eval_addr = EvaluatorConfig::from_env().addr();
    let endpoint = format!("http://{}", eval_addr);
    // Per spec, evaluator expects one request per customer with per-customer transaction_today
    // pairs is deduped from transaction's customer_id column; this produces the array:
    // [{customer:{customer_id:1}, transaction:{transaction_today:{sum:2500000300,count:5}}}, ...] not combined
    for (customer, transaction) in pairs {
        let cid = customer.customer_id;
        let eval_req = EvaluateRequest {
            evaluator_id: job.evaluator_id as i64,
            customer: Some(customer),
            transaction: Some(transaction),
            job_id: job.ingestion_job_id,
        };
        log::info!(
            "Job {} calling Evaluator at {} with evaluator_id={} job_id={} customer_id={} transaction_today={:?}",
            job.job_id,
            endpoint,
            eval_req.evaluator_id,
            eval_req.job_id,
            cid,
            eval_req.transaction.as_ref().and_then(|t| t.transaction_today.as_ref())
        );
        match EvaluatorClient::connect(endpoint.clone()).await {
            Ok(mut client) => match client.evaluate(eval_req).await {
                Ok(resp) => {
                    let inner = resp.into_inner();
                    log::info!(
                        "Job {} evaluator call success for customer {}: result_json={} benefits={:?}",
                        job.job_id,
                        cid,
                        inner.result_json,
                        inner.benefits
                    );
                }
                Err(e) => log::error!("Job {} evaluator RPC failed for customer {}: {}", job.job_id, cid, e),
            },
            Err(e) => log::error!(
                "Job {} evaluator connect failed to {}: {}",
                job.job_id,
                endpoint,
                e
            ),
        }
    }
}

async fn mark_job_and_files_completed(ingestion_job_id: u64) -> Result<(), sqlx::Error> {
    let cfg = MysqlConfig::from_env();
    let pool = pkg::mysql::init(&cfg).database("datamart").await?;
    // Update job status to COMPLETED
    sqlx::query("UPDATE job SET status='COMPLETED', updated_at=NOW() WHERE id=?")
        .bind(ingestion_job_id as i64)
        .execute(pool.as_ref())
        .await?;
    // Update job_file via job_detail
    sqlx::query(
        "UPDATE job_file SET status='COMPLETED', updated_at=NOW() WHERE job_detail_id IN (SELECT id FROM job_detail WHERE job_id=?)",
    )
    .bind(ingestion_job_id as i64)
    .execute(pool.as_ref())
    .await?;
    Ok(())
}

/// Read reduced files for a job from data/output/{job_id}/mr-out-{}.
/// Returns (customers, transactions) for evaluator.
/// Each mr-out file is length-delimited: key (customer_id) + value (32 bytes: sum, count, debit, credit).
fn decode_reduce_bytes(buf: &[u8]) -> Vec<(String, u64, u64)> {
    let mut out = Vec::new();
    let mut pos = 0usize;
    while pos + 4 <= buf.len() {
        let len = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + len > buf.len() {
            break;
        }
        let key = String::from_utf8_lossy(&buf[pos..pos + len]).to_string();
        pos += len;
        if pos + 4 > buf.len() {
            break;
        }
        let vlen = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + vlen > buf.len() {
            break;
        }
        let val = &buf[pos..pos + vlen];
        pos += vlen;
        if val.len() >= 32 {
            // value is 4x u64 big-endian: sum, count, debit, credit (see transaction_summary::reduce)
            let sum = u64::from_be_bytes(val[0..8].try_into().unwrap());
            let count = u64::from_be_bytes(val[8..16].try_into().unwrap());
            // debit/credit not needed for evaluator, ignore
            out.push((key, sum, count));
        }
    }
    out
}

async fn read_reduced_for_job(
    job: &Job,
) -> Result<Vec<(Customer, Transaction)>, anyhow::Error> {
    use std::collections::HashMap;
    let n_reduce = job.reduce_tasks.len();
    let base = job.output_dir.trim_end_matches('/');
    let mut per_customer: HashMap<String, (u64, u64)> = HashMap::new();
    let mut total_sum: u64 = 0;
    let mut total_count: u64 = 0;

    for task in 0..n_reduce {
        let primary = format!("{}/{}/mr-out-{}", base, job.job_id, task);
        let fallback = format!("{}/mr-out-{}", base, task);
        let candidates = [primary, fallback];
        let mut data: Option<Vec<u8>> = None;
        for cand in &candidates {
            match tokio::fs::read(cand).await {
                Ok(b) => {
                    log::info!("Job {} read reduce file {} ({} bytes)", job.job_id, cand, b.len());
                    data = Some(b);
                    break;
                }
                Err(_) => continue,
            }
        }
        let Some(bytes) = data else {
            log::warn!("Job {} missing reduce file for task {}", job.job_id, task);
            continue;
        };
        if bytes.is_empty() {
            continue;
        }
        // Try length-delimited decode; fallback to text lines if binary decode yields empty
        let decoded = decode_reduce_bytes(&bytes);
        if !decoded.is_empty() {
            for (cid, sum, count) in decoded {
                let entry = per_customer.entry(cid.clone()).or_insert((0, 0));
                entry.0 += sum;
                entry.1 += count;
                total_sum += sum;
                total_count += count;
            }
        } else {
            // Fallback: file may be text output (process_output) – parse lines customer,sum,count,avg,debit,credit
            let txt = String::from_utf8_lossy(&bytes);
            for line in txt.lines() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                // format: customer_id,sum,count,avg,debit,credit
                let parts: Vec<&str> = line.split(',').collect();
                if parts.len() >= 3 {
                    let cid = parts[0].trim().to_string();
                    let sum: u64 = parts[1].trim().parse().unwrap_or(0);
                    let count: u64 = parts[2].trim().parse().unwrap_or(0);
                    let entry = per_customer.entry(cid.clone()).or_insert((0, 0));
                    entry.0 += sum;
                    entry.1 += count;
                    total_sum += sum;
                    total_count += count;
                }
            }
        }
    }

    // Fallback to reading transaction source files directly (customer_id from transaction's id_customer column, deduplicated)
    if per_customer.is_empty() && !job.files.is_empty() {
        log::info!("Job {} no reduce data, falling back to reading source transaction files", job.job_id);
        for f in &job.files {
            let content = read_transaction_file_fallback(f).await.unwrap_or_default();
            for line in content.lines() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                if let Some(p) = parse_transaction_line_for_coord(line) {
                    let cid = if !p.1.is_empty() { p.1 } else { p.0 };
                    let amt: u64 = p.2.parse().unwrap_or(0);
                    let entry = per_customer.entry(cid.clone()).or_insert((0, 0));
                    entry.0 += amt;
                    entry.1 += 1;
                    total_sum += amt;
                    total_count += 1;
                }
            }
        }
    }

    let mut pairs: Vec<(Customer, Transaction)> = Vec::new();
    for (cid, (sum, count)) in &per_customer {
        let cid_i64: i64 = cid.parse().unwrap_or(0);
        if cid_i64 == 0 && cid != "0" {
            log::warn!("Job {} customer_id {} is not numeric, skipping", job.job_id, cid);
            continue;
        }
        let avg = if *count > 0 { (*sum / *count) as i64 } else { 0 };
        let customer = Customer { customer_id: cid_i64 };
        let transaction = Transaction {
            transaction_today: Some(TransactionData {
                sum: *sum as i64,
                avg,
                count: *count as i64,
            }),
        };
        pairs.push((customer, transaction));
    }

    // Ensure deterministic ordering by customer_id
    pairs.sort_by(|a, b| a.0.customer_id.cmp(&b.0.customer_id));

    log::info!(
        "Job {} aggregated {} customers total_sum={} total_count={} tx_key={} per_customer={:?}",
        job.job_id,
        pairs.len(),
        total_sum,
        total_count,
        job.key,
        pairs.iter().map(|(c, t)| (c.customer_id, t.transaction_today.as_ref().unwrap().sum, t.transaction_today.as_ref().unwrap().count)).collect::<Vec<_>>()
    );

    Ok(pairs)
}

async fn read_transaction_file_fallback(path: &str) -> Result<String, anyhow::Error> {
    match tokio::fs::read_to_string(path).await {
        Ok(c) => Ok(c),
        Err(_) => {
            let basename = std::path::Path::new(path)
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or(path);
            let candidates = [
                format!("data/transaction/{}", basename),
                format!("data/transaction/{}", path),
                format!("./data/transaction/{}", basename),
                format!("./data/transaction/{}", path),
            ];
            for cand in &candidates {
                if let Ok(c) = tokio::fs::read_to_string(cand).await {
                    return Ok(c);
                }
            }
            Err(anyhow::anyhow!("cannot read transaction file {}", path))
        }
    }
}

fn parse_transaction_line_for_coord(line: &str) -> Option<(String, String, String)> {
    // Returns (id, ref_id, amount) minimal for aggregation
    let parts: Vec<&str> = line.splitn(6, ',').collect();
    if parts.len() < 6 {
        return None;
    }
    Some((
        parts[0].trim().to_string(),
        parts[1].trim().to_string(),
        parts[3].trim().to_string(),
    ))
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
            entity: r.entity,
            evaluator_id: r.evaluator_id,
            ingestion_job_id: r.ingestion_job_id,
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
        let completed_job = if all_done {
            job.done = true;
            log::info!("Job {} marked done after reduce finish", r.job_id);
            Some(job.clone())
        } else {
            None
        };
        drop(inner);
        if let Some(cj) = completed_job {
            log::info!(
                "Job {} completed – updating DB and calling evaluator (evaluator_id={}, ingestion_job_id={}, entity={}, key={})",
                cj.job_id, cj.evaluator_id, cj.ingestion_job_id, cj.entity, cj.key
            );
            tokio::spawn(async move {
                handle_job_completion(cj).await;
            });
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
            entity: "".into(),
            evaluator_id: 1,
            ingestion_job_id: 100,
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
                    entity: "".into(),
                    evaluator_id: 99,
                    ingestion_job_id: expected_id as u64 + 10,
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

    #[test]
    fn decode_reduce_bytes_roundtrip() {
        // Build a sample reduce file via same encoding as worker
        use bytes::{BufMut, BytesMut};
        // Simulate LengthDelimitedWriter encoding
        fn encode(pairs: Vec<(&str, u64, u64)>) -> Vec<u8> {
            let mut buf = Vec::new();
            for (k, sum, count) in pairs {
                let kb = k.as_bytes();
                buf.extend_from_slice(&(kb.len() as u32).to_le_bytes());
                buf.extend_from_slice(kb);
                let mut val = BytesMut::with_capacity(32);
                val.put_u64(sum);
                val.put_u64(count);
                val.put_u64(0);
                val.put_u64(0);
                let vb = val.freeze();
                buf.extend_from_slice(&(vb.len() as u32).to_le_bytes());
                buf.extend_from_slice(&vb);
            }
            buf
        }
        let data = encode(vec![("123", 1000, 2), ("456", 2000, 1)]);
        let decoded = decode_reduce_bytes(&data);
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].0, "123");
        assert_eq!(decoded[0].1, 1000);
        assert_eq!(decoded[0].2, 2);
        assert_eq!(decoded[1].0, "456");
        assert_eq!(decoded[1].1, 2000);
    }

    #[tokio::test]
    async fn read_reduced_for_job_from_output_dir() {
        // Create a temp output dir with job_id subdirectory and mr-out files
        let base = format!("/tmp/test-coord-{}", chrono::Utc::now().timestamp_nanos_opt().unwrap());
        let job_id = 42u32;
        let dir = format!("{}/{}", base, job_id);
        tokio::fs::create_dir_all(&dir).await.unwrap();
        // Write one reduce file with two customers
        use bytes::{BufMut, BytesMut};
        let mut buf = Vec::new();
        for (cid, sum, count) in [("10", 5000u64, 1u64), ("20", 7000u64, 2u64)] {
            let kb = cid.as_bytes();
            buf.extend_from_slice(&(kb.len() as u32).to_le_bytes());
            buf.extend_from_slice(kb);
            let mut val = BytesMut::with_capacity(32);
            val.put_u64(sum);
            val.put_u64(count);
            val.put_u64(0);
            val.put_u64(0);
            let vb = val.freeze();
            buf.extend_from_slice(&(vb.len() as u32).to_le_bytes());
            buf.extend_from_slice(&vb);
        }
        tokio::fs::write(format!("{}/mr-out-0", dir), &buf).await.unwrap();
        // also need at least one more reduce file empty for n_reduce=2
        tokio::fs::write(format!("{}/mr-out-1", dir), Vec::<u8>::new()).await.unwrap();

        let job = Job {
            job_id,
            files: vec![],
            output_dir: base.clone(),
            app: "benefit-evaluator".into(),
            n_map: 1,
            map_tasks: vec![TaskRecord::idle()],
            reduce_tasks: vec![TaskRecord::idle(), TaskRecord::idle()],
            done: false,
            failed: false,
            key: "data_transaction_test".into(),
            entity: "transaction".into(),
            evaluator_id: 99,
            ingestion_job_id: 555,
            errors: Vec::new(),
        };
        let pairs = read_reduced_for_job(&job).await.unwrap();
        assert_eq!(pairs.len(), 2);
        // per-customer: 10→5000/1, 20→7000/2 sorted
        assert_eq!(pairs[0].0.customer_id, 10);
        assert_eq!(pairs[0].1.transaction_today.as_ref().unwrap().sum, 5000);
        assert_eq!(pairs[0].1.transaction_today.as_ref().unwrap().count, 1);
        assert_eq!(pairs[0].1.transaction_today.as_ref().unwrap().avg, 5000);
        assert_eq!(pairs[1].0.customer_id, 20);
        assert_eq!(pairs[1].1.transaction_today.as_ref().unwrap().sum, 7000);
        assert_eq!(pairs[1].1.transaction_today.as_ref().unwrap().count, 2);
        assert_eq!(pairs[1].1.transaction_today.as_ref().unwrap().avg, 3500);
        // cleanup
        let _ = tokio::fs::remove_dir_all(&base).await;
    }

    #[tokio::test]
    async fn read_reduced_fallback_to_source_file_when_no_mr_out() {
        // When no mr-out files exist, fallback to reading source transaction file
        // Create a temp transaction file
        let base_out = format!("/tmp/test-coord-fallback-{}", chrono::Utc::now().timestamp_nanos_opt().unwrap());
        tokio::fs::create_dir_all(&base_out).await.unwrap();
        let tx_path = format!("{}/tx.txt", base_out);
        let content = "1,101,2026-09-20T00:00:00Z,1000,Debit,desc\n2,101,2026-09-20T01:00:00Z,2000,Credit,desc\n3,102,2026-09-20T02:00:00Z,3000,Debit,desc\n";
        tokio::fs::write(&tx_path, content).await.unwrap();

        let job = Job {
            job_id: 77,
            files: vec![tx_path.clone()],
            output_dir: base_out.clone(),
            app: "benefit-evaluator".into(),
            n_map: 1,
            map_tasks: vec![TaskRecord::idle()],
            reduce_tasks: vec![TaskRecord::idle()],
            done: false,
            failed: false,
            key: "fallback_key".into(),
            entity: "transaction".into(),
            evaluator_id: 5,
            ingestion_job_id: 999,
            errors: Vec::new(),
        };
        let pairs = read_reduced_for_job(&job).await.unwrap();
        // Should have 2 customers: 101 → sum 3000 count2 avg1500, 102 → sum3000 count1 avg3000
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].0.customer_id, 101);
        assert_eq!(pairs[0].1.transaction_today.as_ref().unwrap().sum, 3000);
        assert_eq!(pairs[0].1.transaction_today.as_ref().unwrap().count, 2);
        assert_eq!(pairs[0].1.transaction_today.as_ref().unwrap().avg, 1500);
        assert_eq!(pairs[1].0.customer_id, 102);
        assert_eq!(pairs[1].1.transaction_today.as_ref().unwrap().sum, 3000);
        assert_eq!(pairs[1].1.transaction_today.as_ref().unwrap().count, 1);
        let _ = tokio::fs::remove_dir_all(&base_out).await;
    }

    #[test]
    fn submit_job_stores_evaluator_and_ingestion_ids() {
        // Verify Job stores evaluator_id and ingestion_job_id from request
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let coord = Coordinator::new(&test_config());
            let req = Request::new(SubmitJobRequest {
                files: vec!["f".into()],
                output_dir: "/tmp".into(),
                app: "app".into(),
                n_reduce: 1,
                key: "k".into(),
                entity: "transaction".into(),
                evaluator_id: 777,
                ingestion_job_id: 888,
            });
            let resp = coordinator_server::Coordinator::submit_job(&coord, req).await.unwrap();
            let jid = resp.into_inner().job_id;
            let inner = coord.inner.lock().await;
            let job = inner.jobs.get(&jid).unwrap();
            assert_eq!(job.evaluator_id, 777);
            assert_eq!(job.ingestion_job_id, 888);
            assert_eq!(job.key, "k");
            assert_eq!(job.entity, "transaction");
        });
    }

    #[tokio::test]
    async fn read_reduced_for_customer_fallback_to_source_file() {
        // Fallback should build customers deduplicated from transaction id_customer column (not customer.txt)
        let base_out = format!("/tmp/test-coord-cust-{}", chrono::Utc::now().timestamp_nanos_opt().unwrap());
        tokio::fs::create_dir_all(&base_out).await.unwrap();
        let tx_path = format!("{}/tx.txt", base_out);
        let content = "1,101,2026-09-14T03:21:15Z,1000000000,Debit,Test1\n2,101,2026-09-14T03:22:00Z,200,Credit,Test2\n3,102,2026-09-14T03:23:00Z,300,Debit,Test3\n";
        tokio::fs::write(&tx_path, content).await.unwrap();

        let job = Job {
            job_id: 88,
            files: vec![tx_path.clone()],
            output_dir: base_out.clone(),
            app: "benefit-evaluator".into(),
            n_map: 1,
            map_tasks: vec![TaskRecord::idle()],
            reduce_tasks: vec![TaskRecord::idle()],
            done: false,
            failed: false,
            key: "data_customer_widow".into(),
            entity: "transaction".into(),
            evaluator_id: 13,
            ingestion_job_id: 180,
            errors: Vec::new(),
        };
        let pairs = read_reduced_for_job(&job).await.unwrap();
        // deduplicated: 101 and 102 only, despite 101 appearing twice — per-customer sums
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].0.customer_id, 101);
        assert_eq!(pairs[0].1.transaction_today.as_ref().unwrap().sum, 1000000200);
        assert_eq!(pairs[0].1.transaction_today.as_ref().unwrap().count, 2);
        assert_eq!(pairs[1].0.customer_id, 102);
        assert_eq!(pairs[1].1.transaction_today.as_ref().unwrap().sum, 300);
        assert_eq!(pairs[1].1.transaction_today.as_ref().unwrap().count, 1);
        let _ = tokio::fs::remove_dir_all(&base_out).await;
    }

    #[tokio::test]
    async fn read_reduced_for_customer_from_mr_out() {
        // When mr-out files exist, customers are read from them (deduplicated via per_customer)
        let base = format!("/tmp/test-coord-cust-mr-{}", chrono::Utc::now().timestamp_nanos_opt().unwrap());
        let job_id = 99u32;
        let dir = format!("{}/{}", base, job_id);
        tokio::fs::create_dir_all(&dir).await.unwrap();
        use bytes::{BufMut, BytesMut};
        let mut buf = Vec::new();
        for cid in ["10", "20"] {
            let kb = cid.as_bytes();
            buf.extend_from_slice(&(kb.len() as u32).to_le_bytes());
            buf.extend_from_slice(kb);
            let mut val = BytesMut::with_capacity(32);
            val.put_u64(0);
            val.put_u64(1);
            val.put_u64(0);
            val.put_u64(0);
            let vb = val.freeze();
            buf.extend_from_slice(&(vb.len() as u32).to_le_bytes());
            buf.extend_from_slice(&vb);
        }
        tokio::fs::write(format!("{}/mr-out-0", dir), &buf).await.unwrap();
        tokio::fs::write(format!("{}/mr-out-1", dir), Vec::<u8>::new()).await.unwrap();

        let job = Job {
            job_id,
            files: vec![],
            output_dir: base.clone(),
            app: "benefit-evaluator".into(),
            n_map: 1,
            map_tasks: vec![TaskRecord::idle()],
            reduce_tasks: vec![TaskRecord::idle(), TaskRecord::idle()],
            done: false,
            failed: false,
            key: "data_customer_widow".into(),
            entity: "transaction".into(),
            evaluator_id: 13,
            ingestion_job_id: 180,
            errors: Vec::new(),
        };
        let pairs = read_reduced_for_job(&job).await.unwrap();
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].0.customer_id, 10);
        assert_eq!(pairs[0].1.transaction_today.as_ref().unwrap().sum, 0);
        assert_eq!(pairs[0].1.transaction_today.as_ref().unwrap().count, 1);
        assert_eq!(pairs[1].0.customer_id, 20);
        assert_eq!(pairs[1].1.transaction_today.as_ref().unwrap().sum, 0);
        assert_eq!(pairs[1].1.transaction_today.as_ref().unwrap().count, 1);
        let _ = tokio::fs::remove_dir_all(&base).await;
    }

    #[tokio::test]
    async fn read_reduced_per_customer_matches_prompt_example() {
        // Prompt example: 3 customers from transaction20260914.txt
        // expected: 1→ sum2500000300 count5 avg500000060, 2→ sum200 count1, 3→ sum500 count1
        let base = format!("/tmp/test-coord-prompt-{}", chrono::Utc::now().timestamp_nanos_opt().unwrap());
        let job_id = 101u32;
        let dir = format!("{}/{}", base, job_id);
        tokio::fs::create_dir_all(&dir).await.unwrap();
        use bytes::{BufMut, BytesMut};
        let mut buf = Vec::new();
        for (cid, sum, count) in [("1", 2500000300u64, 5u64), ("2", 200u64, 1u64), ("3", 500u64, 1u64)] {
            let kb = cid.as_bytes();
            buf.extend_from_slice(&(kb.len() as u32).to_le_bytes());
            buf.extend_from_slice(kb);
            let mut val = BytesMut::with_capacity(32);
            val.put_u64(sum);
            val.put_u64(count);
            val.put_u64(0);
            val.put_u64(0);
            let vb = val.freeze();
            buf.extend_from_slice(&(vb.len() as u32).to_le_bytes());
            buf.extend_from_slice(&vb);
        }
        tokio::fs::write(format!("{}/mr-out-0", dir), &buf).await.unwrap();
        tokio::fs::write(format!("{}/mr-out-1", dir), Vec::<u8>::new()).await.unwrap();

        let job = Job {
            job_id,
            files: vec![],
            output_dir: base.clone(),
            app: "benefit-evaluator".into(),
            n_map: 1,
            map_tasks: vec![TaskRecord::idle()],
            reduce_tasks: vec![TaskRecord::idle(), TaskRecord::idle()],
            done: false,
            failed: false,
            key: "transaction_today".into(),
            entity: "transaction".into(),
            evaluator_id: 1,
            ingestion_job_id: 100,
            errors: Vec::new(),
        };
        let pairs = read_reduced_for_job(&job).await.unwrap();
        assert_eq!(pairs.len(), 3);
        // sorted 1,2,3
        assert_eq!(pairs[0].0.customer_id, 1);
        assert_eq!(pairs[0].1.transaction_today.as_ref().unwrap().sum, 2500000300);
        assert_eq!(pairs[0].1.transaction_today.as_ref().unwrap().count, 5);
        assert_eq!(pairs[0].1.transaction_today.as_ref().unwrap().avg, 500000060);
        assert_eq!(pairs[1].0.customer_id, 2);
        assert_eq!(pairs[1].1.transaction_today.as_ref().unwrap().sum, 200);
        assert_eq!(pairs[1].1.transaction_today.as_ref().unwrap().count, 1);
        assert_eq!(pairs[1].1.transaction_today.as_ref().unwrap().avg, 200);
        assert_eq!(pairs[2].0.customer_id, 3);
        assert_eq!(pairs[2].1.transaction_today.as_ref().unwrap().sum, 500);
        assert_eq!(pairs[2].1.transaction_today.as_ref().unwrap().count, 1);
        assert_eq!(pairs[2].1.transaction_today.as_ref().unwrap().avg, 500);
        // Verify JSON array shape as prompt expects (looped evaluator requests)
        let array: Vec<serde_json::Value> = pairs
            .iter()
            .map(|(c, t)| {
                let d = t.transaction_today.as_ref().unwrap();
                serde_json::json!({
                    "customer": {"customer_id": c.customer_id},
                    "transaction": {"transaction_today": {"sum": d.sum, "avg": d.avg, "count": d.count}}
                })
            })
            .collect();
        assert_eq!(array[0]["customer"]["customer_id"], serde_json::json!(1));
        assert_eq!(array[0]["transaction"]["transaction_today"]["sum"], serde_json::json!(2500000300i64));
        assert_eq!(array.len(), 3);
        let _ = tokio::fs::remove_dir_all(&base).await;
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
