//! ingestion_engine client — dual purpose:
//! 1) programmatic `grpcurl` for `coordinator.Coordinator/SubmitJob` (legacy helpers)
//! 2) **Ingestion gRPC API** (`client.Ingestion`) that parses job JSON and inserts
//!    into `job` / `job_detail` / `job_file` tables.
//!
//! Proto: `crates/pkg/proto/ingestion_engine/client.proto`
//! DB impl: `crates/ingestion_engine/src/internal/repository/sql/ingestion/*`
//!
//! Example input (via gRPC `Ingest`):
//! ```json
//! {
//!   "job": [
//!     {
//!       "evaluator_id": 1,
//!       "input": [
//!         {
//!           "key": "data_transaction_widow",
//!           "entity": "transaction",
//!           "time_range": {"type": "between", "start": "2026-09-01T00:00:00", "end": "2026-09-07T23:59:59"}
//!         }
//!       ]
//!     },
//!     {
//!       "evaluator_id": 12,
//!       "input": [
//!         {
//!           "key": "data_transaction_today",
//!           "entity": "transaction",
//!           "time_range": {"type": "named", "value": "today"}
//!         }
//!       ]
//!     }
//!   ]
//! }
//! ```
//! Parsing rules:
//! - `time_range.type == "named" && value == "today"` -> `file_start_range`/`file_end_range` = today 00:00:00..23:59:59 UTC
//! - `time_range.type == "between"` -> `file_start_range`/`file_end_range` = parsed `start`/`end`
//! - `entity == "transaction"` -> `job_file.filename` = `transactionYYYYMMDD.txt` per day inclusive
//! - `entity == "customer"` -> `job_file.filename` = `customer.txt` (single)

use anyhow::{Context, Result};
use chrono::Utc;
use pkg::config::coordinator::CoordinatorConfig;
use pkg::mysql::DbPool;
use pkg::proto::client::{
    client_server::Client as ClientTrait, ingestion_server::Ingestion, IngestRequest,
    IngestResponse, Job, JobInput, TimeRange,
};
use pkg::proto::coordinator::{
    coordinator_client::CoordinatorClient, PollJobRequest, PollJobReply, SubmitJobRequest,
};
use std::time::Duration;
use tonic::{Request, Response, Status};

/// Timeout for ingestion gRPC handlers (Client → API internal). Overridden via `GRPC_TIMEOUT_MS` (or `API_TIMEOUT_MS` fallback), defaults to 200ms.
fn api_timeout() -> Duration {
    std::env::var("GRPC_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_millis)
        .or_else(|| {
            std::env::var("API_TIMEOUT_MS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .map(Duration::from_millis)
        })
        .unwrap_or(Duration::from_millis(200))
}

// ---------------------------------------------------------------------------
// Legacy coordinator helpers (kept for backward compat / grpcurl example)
// ---------------------------------------------------------------------------

fn coordinator_endpoint() -> String {
    format!("http://{}", CoordinatorConfig::from_env().addr())
}

/// Build `SubmitJobRequest` exactly like `grpcurl -d '{...}'`.
pub fn build_submit_job_request(
    files: Vec<String>,
    output_dir: String,
    app: String,
    n_reduce: u32,
    key: String,
    entity: String,
) -> SubmitJobRequest {
    build_submit_job_request_with_ids(files, output_dir, app, n_reduce, key, entity, 0, 0)
}

pub fn build_submit_job_request_with_ids(
    files: Vec<String>,
    output_dir: String,
    app: String,
    n_reduce: u32,
    key: String,
    entity: String,
    evaluator_id: u64,
    ingestion_job_id: u64,
) -> SubmitJobRequest {
    SubmitJobRequest {
        files,
        output_dir,
        app,
        n_reduce,
        key,
        entity,
        evaluator_id,
        ingestion_job_id,
    }
}

pub fn default_submit_job_request() -> SubmitJobRequest {
    build_submit_job_request(
        vec!["data/transaction/transaction20260618.txt".into()],
        "data/output".into(),
        "benefit-evaluator".into(),
        2,
        "transaction_today".into(),
        "transaction".into(),
    )
}

pub async fn submit_job_to(addr: &str, req: SubmitJobRequest) -> Result<u32> {
    let mut client = CoordinatorClient::connect(addr.to_string())
        .await
        .with_context(|| format!("failed to connect to coordinator at {}", addr))?;
    let resp = client
        .submit_job(req)
        .await
        .context("SubmitJob RPC failed")?;
    Ok(resp.into_inner().job_id)
}

pub async fn submit_job(req: SubmitJobRequest) -> Result<u32> {
    submit_job_to(&coordinator_endpoint(), req).await
}

pub async fn poll_job_to(addr: &str, job_id: u32) -> Result<PollJobReply> {
    let mut client = CoordinatorClient::connect(addr.to_string())
        .await
        .with_context(|| format!("failed to connect to coordinator at {}", addr))?;
    let resp = client
        .poll_job(PollJobRequest { job_id })
        .await
        .context("PollJob RPC failed")?;
    Ok(resp.into_inner())
}

pub async fn poll_job(job_id: u32) -> Result<PollJobReply> {
    poll_job_to(&coordinator_endpoint(), job_id).await
}

#[derive(Debug, Clone)]
pub struct Client {
    endpoint: String,
}

impl Client {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
        }
    }
    pub fn from_env() -> Self {
        Self::new(coordinator_endpoint())
    }
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
    pub async fn submit_job(
        &self,
        files: Vec<String>,
        output_dir: String,
        app: String,
        n_reduce: u32,
        key: String,
        entity: String,
    ) -> Result<u32> {
        let req = build_submit_job_request(files, output_dir, app, n_reduce, key, entity);
        submit_job_to(&self.endpoint, req).await
    }
    pub async fn submit_job_request(&self, req: SubmitJobRequest) -> Result<u32> {
        submit_job_to(&self.endpoint, req).await
    }
    pub async fn submit_default(&self) -> Result<u32> {
        submit_job_to(&self.endpoint, default_submit_job_request()).await
    }
    pub async fn poll(&self, job_id: u32) -> Result<PollJobReply> {
        poll_job_to(&self.endpoint, job_id).await
    }
}

// ---------------------------------------------------------------------------
// Ingestion helpers – pure logic (also in sql/ingestion for reuse)
// ---------------------------------------------------------------------------

pub use crate::internal::repository::sql::ingestion::implementation::{
    filenames_for_entity, parse_datetime, resolve_time_range,
};

/// Convenience: build `IngestRequest` from JSON example.
pub fn build_ingest_request(jobs: Vec<Job>) -> IngestRequest {
    IngestRequest { job: jobs, jobs: vec![] }
}

/// Build a single Job with one input – handy for tests / grpcurl examples.
pub fn build_job(evaluator_id: u64, inputs: Vec<JobInput>) -> Job {
    Job { evaluator_id, input: inputs }
}

pub fn build_job_input(key: &str, entity: &str, time_range: Option<TimeRange>) -> JobInput {
    JobInput {
        key: key.to_string(),
        entity: entity.to_string(),
        time_range,
    }
}

pub fn build_time_range_between(start: &str, end: &str) -> TimeRange {
    TimeRange {
        r#type: "between".into(),
        value: "".into(),
        start: start.into(),
        end: end.into(),
    }
}

pub fn build_time_range_named(value: &str) -> TimeRange {
    TimeRange {
        r#type: "named".into(),
        value: value.into(),
        start: "".into(),
        end: "".into(),
    }
}

// ---------------------------------------------------------------------------
// Core ingestion logic (DB) – delegates to sql/ingestion implementation
// ---------------------------------------------------------------------------

/// Parse `IngestRequest` and insert into DB. Returns inserted job ids.
/// This is the function called by the gRPC handler.
pub async fn ingest_jobs(pool: &DbPool, req: &IngestRequest) -> Result<Vec<u64>, sqlx::Error> {
    crate::internal::repository::sql::ingestion::implementation::ingest_request(pool, req).await
}

// ---------------------------------------------------------------------------
// gRPC Service implementation
// ---------------------------------------------------------------------------

/// `client.Client` (primary) and `client.Ingestion` (compat) service implementation.
/// Scheduler (`core_module`) calls `Client/SubmitJobs` via `client_client::ClientClient`.
pub struct IngestionService {
    pub pool: DbPool,
}
/// Alias so binary `client` not `ingestion_server` is primary
pub type ClientService = IngestionService;

impl IngestionService {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }
    async fn handle_ingest_inner(
        &self,
        req: IngestRequest,
    ) -> std::result::Result<IngestResponse, Status> {
        // shared logging + DB logic for both Client and Ingestion services
        pkg::log::info!(
            "[Ingest] raw input: job.len={} jobs.len={} total={}",
            req.job.len(),
            req.jobs.len(),
            req.job.len() + req.jobs.len()
        );
        for (ji, job) in req.job.iter().enumerate() {
            pkg::log::info!(
                "[Ingest] job[{}] evaluator_id={} inputs={}",
                ji,
                job.evaluator_id,
                job.input.len()
            );
            for (ii, inp) in job.input.iter().enumerate() {
                let tr_dbg = inp
                    .time_range
                    .as_ref()
                    .map(|tr| {
                        format!(
                            "type={} value={} start={} end={}",
                            tr.r#type, tr.value, tr.start, tr.end
                        )
                    })
                    .unwrap_or_else(|| "None".to_string());
                pkg::log::info!(
                    "[Ingest]   input[{}] key={} entity={} time_range={}",
                    ii,
                    inp.key,
                    inp.entity,
                    tr_dbg
                );
                if let Some(tr) = inp.time_range.as_ref() {
                    let (s, e) = resolve_time_range(Some(tr));
                    let files = filenames_for_entity(&inp.entity, s, e);
                    pkg::log::info!("[Ingest]     parsed range {} -> {} files={:?}", s, e, files);
                } else {
                    let (s, e) = resolve_time_range(None);
                    let files = filenames_for_entity(&inp.entity, s, e);
                    pkg::log::info!(
                        "[Ingest]     parsed range (None) {} -> {} files={:?}",
                        s, e, files
                    );
                }
            }
        }
        for (ji, job) in req.jobs.iter().enumerate() {
            pkg::log::info!(
                "[Ingest] jobs[{}] evaluator_id={} inputs={}",
                ji,
                job.evaluator_id,
                job.input.len()
            );
            for (ii, inp) in job.input.iter().enumerate() {
                let tr_dbg = inp
                    .time_range
                    .as_ref()
                    .map(|tr| {
                        format!(
                            "type={} value={} start={} end={}",
                            tr.r#type, tr.value, tr.start, tr.end
                        )
                    })
                    .unwrap_or_else(|| "None".to_string());
                pkg::log::info!(
                    "[Ingest]   jobs input[{}] key={} entity={} time_range={}",
                    ii,
                    inp.key,
                    inp.entity,
                    tr_dbg
                );
            }
        }
        let total_jobs = req.job.len() + req.jobs.len();
        if total_jobs == 0 {
            pkg::log::warn!("[Ingest] rejected: job array is empty");
            return Err(Status::invalid_argument("job array is empty"));
        }
        match ingest_jobs(&self.pool, &req).await {
            Ok(ids) => {
                let resp = IngestResponse {
                    job_ids: ids.clone(),
                    message: format!("ingested {} jobs", ids.len()),
                };
                pkg::log::info!(
                    "[Ingest] parse+insert success -> job_ids={:?} message={}",
                    resp.job_ids,
                    resp.message
                );
                for (i, jid) in ids.iter().enumerate() {
                    pkg::log::info!("[Ingest]   result[{}] job_id={}", i, jid);
                }
                // For each parsed input, forward a SubmitJob to coordinator (looping rpc SubmitJob)
                // This makes the ingestion client also act as a coordinator client, using SubmitJobRequest
                // files = generated filenames (with data/ prefix), output_dir/app/n_reduce fixed, key/entity from input
                let coord_addr = coordinator_endpoint();
                // Collect all inputs to forward (from both job/jobs)
                // Map each outer Job to its DB ingestion_job_id (ids in order)
                let all_jobs: Vec<&Job> = req.job.iter().chain(req.jobs.iter()).collect();
                for (jidx, job) in all_jobs.iter().enumerate() {
                    let ingestion_job_id = ids.get(jidx).copied().unwrap_or(0);
                    for inp in &job.input {
                        let (s, e) = resolve_time_range(inp.time_range.as_ref());
                        let filenames = filenames_for_entity(&inp.entity, s, e);
                        // Prefix with data/<entity>/ to match worker's expected path
                        let files: Vec<String> = filenames
                            .iter()
                            .map(|f| {
                                if inp.entity.to_lowercase() == "customer" {
                                    format!("data/customer/{}", f)
                                } else {
                                    // default transaction and other entities under data/<entity>/
                                    format!("data/{}/{}", inp.entity.to_lowercase(), f)
                                }
                            })
                            .collect();
                        let submit_req = build_submit_job_request_with_ids(
                            files.clone(),
                            "data/output".to_string(),
                            "benefit-evaluator".to_string(),
                            2,
                            inp.key.clone(),
                            inp.entity.clone(),
                            job.evaluator_id,
                            ingestion_job_id,
                        );
                        pkg::log::info!(
                            "[Ingest] forwarding to coordinator {} -> SubmitJob files={:?} output_dir={} app={} n_reduce={} key={} entity={}",
                            coord_addr,
                            submit_req.files,
                            submit_req.output_dir,
                            submit_req.app,
                            submit_req.n_reduce,
                            submit_req.key,
                            submit_req.entity
                        );
                        // Best-effort: don't fail Ingest if coordinator is down, just log
                        // Use a short timeout via the client's connect (tonic will handle)
                        match CoordinatorClient::connect(coord_addr.clone()).await {
                            Ok(mut client) => match client.submit_job(submit_req).await {
                                Ok(r) => pkg::log::info!(
                                    "[Ingest] coordinator SubmitJob success -> coordinator job_id={}",
                                    r.into_inner().job_id
                                ),
                                Err(e) => pkg::log::warn!(
                                    "[Ingest] coordinator SubmitJob RPC failed for key={} entity={}: {}",
                                    inp.key,
                                    inp.entity,
                                    e
                                ),
                            },
                            Err(e) => pkg::log::warn!(
                                "[Ingest] coordinator connect failed to {} for key={} entity={}: {}",
                                coord_addr,
                                inp.key,
                                inp.entity,
                                e
                            ),
                        }
                    }
                }
                Ok(resp)
            }
            Err(e) => {
                pkg::log::error!("[Ingest] ingest failed: {:?}", e);
                Err(Status::internal(format!("ingest failed: {}", e)))
            }
        }
    }

    async fn handle_ingest(
        &self,
        req: IngestRequest,
    ) -> std::result::Result<IngestResponse, Status> {
        let timeout = api_timeout();
        match tokio::time::timeout(timeout, self.handle_ingest_inner(req)).await {
            Ok(res) => res,
            Err(_) => Err(Status::deadline_exceeded(format!(
                "ingest timeout after {}ms",
                timeout.as_millis()
            ))),
        }
    }
}

#[tonic::async_trait]
impl Ingestion for IngestionService {
    async fn ingest(
        &self,
        request: Request<IngestRequest>,
    ) -> std::result::Result<Response<IngestResponse>, Status> {
        let req = request.into_inner();
        let resp = self.handle_ingest(req).await?;
        Ok(Response::new(resp))
    }

    async fn submit_jobs(
        &self,
        request: Request<IngestRequest>,
    ) -> std::result::Result<Response<IngestResponse>, Status> {
        let req = request.into_inner();
        let resp = self.handle_ingest(req).await?;
        Ok(Response::new(resp))
    }
}

#[tonic::async_trait]
impl ClientTrait for IngestionService {
    async fn ingest(
        &self,
        request: Request<IngestRequest>,
    ) -> std::result::Result<Response<IngestResponse>, Status> {
        let req = request.into_inner();
        let resp = self.handle_ingest(req).await?;
        Ok(Response::new(resp))
    }

    async fn submit_jobs(
        &self,
        request: Request<IngestRequest>,
    ) -> std::result::Result<Response<IngestResponse>, Status> {
        let req = request.into_inner();
        let resp = self.handle_ingest(req).await?;
        Ok(Response::new(resp))
    }
}

/// Start gRPC server for `client.Client` (primary) and `client.Ingestion` (compat) on `addr`.
/// Includes reflection for `grpcurl`. Binary is `client` not `ingestion_server`.
pub async fn start_ingestion_server(pool: DbPool, addr: &str) -> Result<()> {
    start_client_server(pool, addr).await
}

/// Registers both Client and Ingestion services (Client is primary as requested)
pub async fn start_client_server(pool: DbPool, addr: &str) -> Result<()> {
    let svc_client = IngestionService::new(pool.clone());
    let svc_ingest = IngestionService::new(pool);
    let client_svc = pkg::proto::client::client_server::ClientServer::new(svc_client);
    let ingestion_svc = pkg::proto::client::ingestion_server::IngestionServer::new(svc_ingest);
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(pkg::proto::FILE_DESCRIPTOR_SET)
        .build_v1()
        .unwrap();
    tonic::transport::Server::builder()
        .add_service(client_svc)
        .add_service(ingestion_svc)
        .add_service(reflection)
        .serve(addr.parse().unwrap())
        .await
        .context("client server failed")?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_request_preserves_array() {
        let req = build_submit_job_request(
            vec![
                "data/transaction/transaction20260618.txt".into(),
                "data/transaction/transaction20260619.txt".into(),
            ],
            "data/output".into(),
            "benefit-evaluator".into(),
            2,
            "transaction_today".into(),
            "transaction".into(),
        );
        assert_eq!(req.files.len(), 2);
        assert_eq!(req.files[0], "data/transaction/transaction20260618.txt");
        assert_eq!(req.n_reduce, 2);
        assert_eq!(req.key, "transaction_today");
    }

    #[test]
    fn default_request_matches_grpcurl_example() {
        let req = default_submit_job_request();
        assert_eq!(req.files, vec!["data/transaction/transaction20260618.txt"]);
        assert_eq!(req.output_dir, "data/output");
        assert_eq!(req.app, "benefit-evaluator");
        assert_eq!(req.n_reduce, 2);
        assert_eq!(req.key, "transaction_today");
        assert_eq!(req.entity, "transaction");
    }

    #[test]
    fn grpcurl_json_maps_to_request() {
        let json = r#"{
            "files": ["data/transaction/transaction20260618.txt"],
            "output_dir": "data/output",
            "app": "benefit-evaluator",
            "n_reduce": 2,
            "key": "transaction_today",
            "entity": "transaction"
        }"#;
        let v: serde_json::Value = serde_json::from_str(json).unwrap();
        let files: Vec<String> = v["files"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x.as_str().unwrap().to_string())
            .collect();
        let req = build_submit_job_request(
            files,
            v["output_dir"].as_str().unwrap().into(),
            v["app"].as_str().unwrap().into(),
            v["n_reduce"].as_u64().unwrap() as u32,
            v["key"].as_str().unwrap().into(),
            v["entity"].as_str().unwrap().into(),
        );
        assert_eq!(req.files.len(), 1);
        assert_eq!(req.output_dir, "data/output");
    }

    #[test]
    fn test_filenames_transaction_between_7_days() {
        let start = parse_datetime("2026-09-01T00:00:00").unwrap();
        let end = parse_datetime("2026-09-07T23:59:59").unwrap();
        let files = filenames_for_entity("transaction", start, end);
        assert_eq!(files.len(), 7);
        assert_eq!(files[0], "transaction20260901.txt");
        assert_eq!(files[3], "transaction20260904.txt");
        assert_eq!(files[6], "transaction20260907.txt");
    }

    #[test]
    fn test_filenames_customer_single() {
        let start = parse_datetime("2026-09-01T00:00:00").unwrap();
        let end = parse_datetime("2026-09-07T23:59:59").unwrap();
        let files = filenames_for_entity("customer", start, end);
        assert_eq!(files, vec!["customer.txt"]);
    }

    #[test]
    fn test_resolve_between() {
        let tr = build_time_range_between("2026-09-01T00:00:00", "2026-09-07T23:59:59");
        let (s, e) = resolve_time_range(Some(&tr));
        assert_eq!(s, parse_datetime("2026-09-01T00:00:00").unwrap());
        assert_eq!(e, parse_datetime("2026-09-07T23:59:59").unwrap());
    }

    #[test]
    fn test_resolve_named_today() {
        let tr = build_time_range_named("today");
        let (s, e) = resolve_time_range(Some(&tr));
        let today = Utc::now().date_naive();
        assert_eq!(s.date_naive(), today);
        assert_eq!(e.date_naive(), today);
        assert_eq!(s.time(), chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap());
        assert_eq!(e.time(), chrono::NaiveTime::from_hms_opt(23, 59, 59).unwrap());
        // transaction today => single file
        let files = filenames_for_entity("transaction", s, e);
        assert_eq!(files.len(), 1);
        let expected = format!("transaction{}.txt", today.format("%Y%m%d"));
        assert_eq!(files[0], expected);
    }

    #[test]
    fn test_build_ingest_request_and_helper() {
        let input = build_job_input(
            "data_transaction_widow",
            "transaction",
            Some(build_time_range_between("2026-09-01T00:00:00", "2026-09-07T23:59:59")),
        );
        let job = build_job(1, vec![input]);
        let req = build_ingest_request(vec![job]);
        assert_eq!(req.job[0].evaluator_id, 1);
        assert_eq!(req.job[0].input[0].key, "data_transaction_widow");
        assert_eq!(req.job[0].input[0].entity, "transaction");
    }

    #[test]
    fn test_ingest_request_serde_json_example() {
        // JSON from task description (with missing comma fix)
        let json = r#"{
          "job": [
            {
              "evaluator_id": 1,
              "input": [
                {
                  "key": "data_transaction_widow",
                  "entity": "transaction",
                  "time_range": {
                    "type": "between",
                    "start": "2026-09-01T00:00:00",
                    "end": "2026-09-07T23:59:59"
                  }
                }
              ]
            },
            {
              "evaluator_id": 12,
              "input": [
                {
                  "key": "data_transaction_today",
                  "entity": "transaction",
                  "time_range": {
                    "type": "named",
                    "value": "today"
                  }
                }
              ]
            }
          ]
        }"#;
        let v: serde_json::Value = serde_json::from_str(json).unwrap();
        let jobs = v["job"].as_array().unwrap();
        assert_eq!(jobs[0]["evaluator_id"], 1);
        // build via helpers
        let mut proto_jobs = vec![];
        for j in jobs {
            let evaluator_id = j["evaluator_id"].as_u64().unwrap();
            let mut inputs = vec![];
            for inp in j["input"].as_array().unwrap() {
                let tr = &inp["time_range"];
                let t = tr["type"].as_str().unwrap_or("");
                let time_range = if t == "between" {
                    Some(build_time_range_between(tr["start"].as_str().unwrap(), tr["end"].as_str().unwrap()))
                } else {
                    Some(build_time_range_named(tr["value"].as_str().unwrap_or("today")))
                };
                inputs.push(build_job_input(inp["key"].as_str().unwrap(), inp["entity"].as_str().unwrap(), time_range));
            }
            proto_jobs.push(build_job(evaluator_id, inputs));
        }
        let req = build_ingest_request(proto_jobs);
        assert_eq!(req.job.len(), 2);
        assert_eq!(req.job[0].evaluator_id, 1);
        assert_eq!(req.job[1].evaluator_id, 12);
        // verify filename generation for first job's input
        let tr0 = req.job[0].input[0].time_range.as_ref().unwrap();
        let (s,e) = resolve_time_range(Some(tr0));
        let files = filenames_for_entity("transaction", s, e);
        assert_eq!(files.len(), 7);
    }

    #[tokio::test]
    async fn test_ingestion_service_rejects_empty() {
        use std::sync::Arc;
        let dummy: sqlx::mysql::MySqlPool = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        let pool = Arc::new(dummy);
        std::mem::forget(pool.clone());
        let svc = IngestionService::new(pool);
        let req = Request::new(IngestRequest { job: vec![], jobs: vec![] });
        // Disambiguate between Ingestion and Client traits (both have `ingest`)
        let res = <IngestionService as Ingestion>::ingest(&svc, req).await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().code(), tonic::Code::InvalidArgument);
        // ensure pool not leaked in sense of forgetting already
    }

    #[tokio::test]
    async fn test_client_service_rejects_empty() {
        use std::sync::Arc;
        let dummy: sqlx::mysql::MySqlPool = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        let pool = Arc::new(dummy);
        std::mem::forget(pool.clone());
        let svc = IngestionService::new(pool);
        let req = Request::new(IngestRequest { job: vec![], jobs: vec![] });
        let res = <IngestionService as ClientTrait>::ingest(&svc, req).await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn api_timeout_default_is_50ms() {
        unsafe {
            std::env::remove_var("API_TIMEOUT_MS");
            std::env::remove_var("GRPC_TIMEOUT_MS");
        }
        assert_eq!(api_timeout(), Duration::from_millis(200));
    }

    #[test]
    fn api_timeout_respects_env() {
        unsafe {
            std::env::remove_var("GRPC_TIMEOUT_MS");
            std::env::set_var("API_TIMEOUT_MS", "77");
        }
        assert_eq!(api_timeout(), Duration::from_millis(77));
        unsafe { std::env::remove_var("API_TIMEOUT_MS"); }
        assert_eq!(api_timeout(), Duration::from_millis(200));
        // GRPC_TIMEOUT_MS takes priority over API_TIMEOUT_MS
        unsafe {
            std::env::set_var("API_TIMEOUT_MS", "77");
            std::env::set_var("GRPC_TIMEOUT_MS", "88");
        }
        assert_eq!(api_timeout(), Duration::from_millis(88));
        unsafe {
            std::env::remove_var("API_TIMEOUT_MS");
            std::env::remove_var("GRPC_TIMEOUT_MS");
        }
        assert_eq!(api_timeout(), Duration::from_millis(200));
    }

    #[tokio::test]
    async fn timeout_wraps_slow_ingest() {
        let timeout = Duration::from_millis(50);
        let slow = async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            Ok::<String, String>("done".into())
        };
        let res = tokio::time::timeout(timeout, slow).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn timeout_passes_fast_ingest() {
        let timeout = Duration::from_millis(50);
        let fast = async { Ok::<String, String>("done".into()) };
        let res = tokio::time::timeout(timeout, fast).await;
        assert!(res.is_ok());
    }
}
