use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use cron::Schedule;
use pkg::config::coordinator::CoordinatorConfig;
use pkg::config::scheduler::SchedulerConfig;
use pkg::log;
use pkg::proto::coordinator::{coordinator_client::CoordinatorClient, SubmitJobRequest};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn normalize_cron(cron: &str) -> String {
    let trimmed = cron.trim();
    let parts: Vec<&str> = trimmed.split_whitespace().collect();
    match parts.len() {
        5 => format!("0 {}", trimmed), // add seconds
        6 | 7 => trimmed.to_string(),
        _ => trimmed.to_string(), // let Schedule::from_str error out
    }
}

fn parse_schedule(cron: &str) -> Result<Schedule> {
    let normalized = normalize_cron(cron);
    Schedule::from_str(&normalized)
        .with_context(|| format!("invalid cron expression: '{}' (normalized: '{}')", cron, normalized))
}

fn parse_timezone(tz: &str) -> Tz {
    tz.parse().unwrap_or(chrono_tz::UTC)
}

fn coordinator_endpoint() -> String {
    format!("http://{}", CoordinatorConfig::from_env().addr())
}

// ---------------------------------------------------------------------------
// SubmitJob helpers — grpcurl equivalent
// ---------------------------------------------------------------------------

/// Build the exact `SubmitJobRequest` that matches:
///
/// ```bash
/// grpcurl -plaintext -d '{
///     "files": ["data/transaction/transaction20260618.txt"],
///     "output_dir": "data/output",
///     "app": "benefit-evaluator",
///     "n_reduce": 2,
///     "key": "transaction_today",
///     "entity": "transaction"
/// }' localhost:10162 coordinator.Coordinator/SubmitJob
/// ```
///
/// `files` is `repeated string` — pass an array like `vec!["data/transaction/....txt".into()]`.
pub fn build_submit_job_request(
    files: Vec<String>,
    output_dir: String,
    app: String,
    n_reduce: u32,
    key: String,
    entity: String,
) -> SubmitJobRequest {
    SubmitJobRequest {
        files,
        output_dir,
        app,
        n_reduce,
        key,
        entity,
    }
}

/// Default request matching the example in the task description.
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

/// Low-level gRPC call: connect to `addr` (e.g. `"http://127.0.0.1:10162"`) and
/// call `coordinator.Coordinator/SubmitJob` with `req`.
///
/// This is the programmatic equivalent of:
///
/// ```bash
/// grpcurl -plaintext -d '{"files":["data/transaction/transaction20260618.txt"],"output_dir":"data/output","app":"benefit-evaluator","n_reduce":2,"key":"transaction_today","entity":"transaction"}' localhost:10162 coordinator.Coordinator/SubmitJob
/// ```
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

/// Submit `req` to the coordinator resolved from env (`COORDINATOR_HOST`/`COORDINATOR_PORT`,
/// defaults `127.0.0.1:10162`). Equivalent to `grpcurl -plaintext localhost:10162 ...`.
pub async fn submit_job(req: SubmitJobRequest) -> Result<u32> {
    let endpoint = coordinator_endpoint();
    submit_job_to(&endpoint, req).await
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Scheduler that ticks according to `SchedulerConfig`.
#[derive(Debug, Clone)]
pub struct Scheduler {
    config: SchedulerConfig,
}

impl Scheduler {
    pub fn new(config: SchedulerConfig) -> Self {
        Self { config }
    }

    pub fn from_env() -> Self {
        Self::new(SchedulerConfig::from_env())
    }

    pub fn config(&self) -> &SchedulerConfig {
        &self.config
    }

    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Parse the cron schedule, normalized to 6-field.
    pub fn schedule(&self) -> Result<Schedule> {
        parse_schedule(&self.config.cron)
    }

    /// Resolve timezone, falling back to UTC on unknown names.
    pub fn timezone(&self) -> Tz {
        parse_timezone(&self.config.timezone)
    }

    /// Next run instant strictly after `from` (in UTC).
    /// Returns `None` if disabled or cron invalid.
    pub fn next_run_from(&self, from: DateTime<Utc>) -> Option<DateTime<Utc>> {
        if !self.config.enabled {
            return None;
        }
        let tz = self.timezone();
        let schedule = self.schedule().ok()?;
        // Convert `from` to target tz, query schedule, convert back to UTC.
        let from_tz = from.with_timezone(&tz);
        let next_tz = schedule.after(&from_tz).next()?;
        Some(next_tz.with_timezone(&Utc))
    }

    /// Next run from now.
    pub fn next_run(&self) -> Option<DateTime<Utc>> {
        self.next_run_from(Utc::now())
    }

    /// Duration until next tick from now.
    pub fn duration_until_next(&self) -> Option<Duration> {
        let now = Utc::now();
        let next = self.next_run_from(now)?;
        let secs = (next - now).num_milliseconds().max(0) as u64;
        Some(Duration::from_millis(secs))
    }

    /// Duration until next tick from an arbitrary `from` (useful for testing).
    pub fn duration_until_next_from(&self, from: DateTime<Utc>) -> Option<Duration> {
        let next = self.next_run_from(from)?;
        let secs = (next - from).num_milliseconds().max(0) as u64;
        Some(Duration::from_millis(secs))
    }

    // -----------------------------------------------------------------------
    // SubmitJob — array `files` API (mirrors grpcurl -d JSON)
    // -----------------------------------------------------------------------

    /// Submit a `SubmitJobRequest` to the coordinator (addr from env).
    /// Returns the assigned `job_id`.
    pub async fn submit_job_request(&self, req: SubmitJobRequest) -> Result<u32> {
        log::info!(
            "Scheduler SubmitJob files={:?} output_dir={} app={} n_reduce={} key={} entity={}",
            req.files,
            req.output_dir,
            req.app,
            req.n_reduce,
            req.key,
            req.entity
        );
        let job_id = submit_job(req).await?;
        log::info!("Scheduler SubmitJob returned job_id={}", job_id);
        Ok(job_id)
    }

    /// Submit with explicit `files` array — direct analogue of `grpcurl -d '{"files": [...] ...}'`.
    pub async fn submit_job_with_files(
        &self,
        files: Vec<String>,
        output_dir: String,
        app: String,
        n_reduce: u32,
        key: String,
        entity: String,
    ) -> Result<u32> {
        let req = build_submit_job_request(files, output_dir, app, n_reduce, key, entity);
        self.submit_job_request(req).await
    }

    /// Submit to an explicit `addr` (e.g. `"http://127.0.0.1:10162"`).
    pub async fn submit_job_to_addr(&self, addr: &str, req: SubmitJobRequest) -> Result<u32> {
        let job_id = submit_job_to(addr, req).await?;
        log::info!("Scheduler SubmitJob to {} returned job_id={}", addr, job_id);
        Ok(job_id)
    }

    /// Submit the default example job:
    /// `files=["data/transaction/transaction20260618.txt"], output_dir="data/output",
    ///  app="benefit-evaluator", n_reduce=2, key="transaction_today", entity="transaction"`
    pub async fn submit_default_job(&self) -> Result<u32> {
        self.submit_job_request(default_submit_job_request()).await
    }

    /// Submit the default job to an explicit addr.
    pub async fn submit_default_job_to(&self, addr: &str) -> Result<u32> {
        self.submit_job_to_addr(addr, default_submit_job_request()).await
    }

    /// Run `job` forever, sleeping until the next cron tick.
    ///
    /// If `enabled == false`, returns immediately without running the job.
    /// `job` is `Arc`-wrapped so it can be cloned into the loop.
    pub async fn run<F, Fut>(&self, job: F)
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        if !self.is_enabled() {
            log::info!("Scheduler disabled (SCHEDULER_ENABLED=false) — not running");
            return;
        }
        let schedule = match self.schedule() {
            Ok(s) => s,
            Err(e) => {
                log::error!("Scheduler invalid cron '{}': {:?}", self.config.cron, e);
                return;
            }
        };
        let tz = self.timezone();
        let job = Arc::new(job);

        log::info!(
            "Scheduler started: cron='{}' (normalized='{}') tz='{}'",
            self.config.cron,
            normalize_cron(&self.config.cron),
            self.config.timezone
        );

        loop {
            let now_utc = Utc::now();
            let now_tz = now_utc.with_timezone(&tz);
            let next_tz = match schedule.after(&now_tz).next() {
                Some(n) => n,
                None => {
                    log::error!("Scheduler has no upcoming fire time — stopping");
                    break;
                }
            };
            let next_utc = next_tz.with_timezone(&Utc);
            let wait = (next_utc - now_utc).num_milliseconds().max(0) as u64;
            let wait_dur = Duration::from_millis(wait);

            log::info!(
                "Scheduler next run at {} (in {}ms / {:.1}s)",
                next_tz,
                wait,
                wait as f64 / 1000.0
            );

            tokio::time::sleep(wait_dur).await;

            log::info!("Scheduler firing job at {}", Utc::now());
            let job_clone = Arc::clone(&job);
            // Run inline so jobs don't overlap; spawn if overlap desired.
            job_clone().await;
        }
    }

    /// Run the scheduler where each tick submits the default `SubmitJob`
    /// (`benefit-evaluator`, `files=[...]`, `n_reduce=2`, ...).
    ///
    /// This wires the cron tick directly to `coordinator.Coordinator/SubmitJob`,
    /// so you don't need `grpcurl` — the scheduler hits the same RPC.
    pub async fn run_with_submit(&self) {
        let sched = self.clone();
        self.run(move || {
            let s = sched.clone();
            async move {
                match s.submit_default_job().await {
                    Ok(job_id) => log::info!("Scheduled SubmitJob succeeded job_id={}", job_id),
                    Err(e) => log::error!("Scheduled SubmitJob failed: {:?}", e),
                }
            }
        })
        .await
    }

    /// Run once with SubmitJob: wait until next tick, submit default job, return `job_id`.
    pub async fn tick_once_with_submit(&self) -> Result<u32> {
        let dur = self
            .duration_until_next()
            .context("scheduler disabled or no upcoming tick")?;
        tokio::time::sleep(dur).await;
        self.submit_default_job().await
    }

    /// Convenience: run once after waiting until the next tick, then return.
    /// Useful for `tokio::select!` or one-shot demos/tests.
    pub async fn tick_once<F, Fut>(&self, job: F) -> Result<()>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        let dur = self
            .duration_until_next()
            .context("scheduler disabled or no upcoming tick")?;
        tokio::time::sleep(dur).await;
        job().await;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Top-level helpers (ergonomic re-exports)
// ---------------------------------------------------------------------------

/// Start scheduler from env and run `job` forever (blocking).
pub async fn start<F, Fut>(job: F) -> Result<()>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let sched = Scheduler::from_env();
    sched.run(job).await;
    Ok(())
}

/// Start scheduler from env and on each tick submit the default
/// `SubmitJob` (`files=["data/transaction/transaction20260618.txt"], ...`).
/// This is the `grpcurl` equivalent running on a cron.
pub async fn start_with_submit() -> Result<()> {
    let sched = Scheduler::from_env();
    sched.run_with_submit().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Timelike};

    fn cfg(cron: &str) -> SchedulerConfig {
        SchedulerConfig {
            enabled: true,
            cron: cron.to_string(),
            timezone: "UTC".into(),
        }
    }

    #[test]
    fn normalize_5_to_6() {
        assert_eq!(normalize_cron("0 2 * * *"), "0 0 2 * * *");
        assert_eq!(normalize_cron("*/5 * * * *"), "0 */5 * * * *");
        assert_eq!(normalize_cron("0 0 2 * * *"), "0 0 2 * * *");
    }

    #[test]
    fn parse_5_field() {
        let s = parse_schedule("0 2 * * *").unwrap();
        let from = chrono_tz::UTC.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let next = s.after(&from).next().unwrap();
        assert_eq!(next.hour(), 2);
    }

    #[test]
    fn parse_6_field() {
        parse_schedule("0 0 2 * * *").unwrap();
    }

    #[test]
    fn invalid_cron_errors() {
        assert!(parse_schedule("not a cron").is_err());
        assert!(parse_schedule("").is_err());
    }

    #[test]
    fn next_run_daily_2am() {
        let sched = Scheduler::new(cfg("0 2 * * *"));
        let from = Utc.with_ymd_and_hms(2026, 1, 1, 1, 0, 0).unwrap();
        let next = sched.next_run_from(from).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2026, 1, 1, 2, 0, 0).unwrap());
        // after 02:00 -> next day
        let from2 = Utc.with_ymd_and_hms(2026, 1, 1, 3, 0, 0).unwrap();
        let next2 = sched.next_run_from(from2).unwrap();
        assert_eq!(next2, Utc.with_ymd_and_hms(2026, 1, 2, 2, 0, 0).unwrap());
    }

    #[test]
    fn every_minute() {
        let sched = Scheduler::new(cfg("* * * * *"));
        let from = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let next = sched.next_run_from(from).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2026, 1, 1, 0, 1, 0).unwrap());
    }

    #[test]
    fn disabled_returns_none() {
        let mut c = cfg("0 2 * * *");
        c.enabled = false;
        let sched = Scheduler::new(c);
        assert!(sched.next_run_from(Utc::now()).is_none());
        assert!(sched.duration_until_next().is_none());
    }

    #[test]
    fn timezone_jakarta_ahead_of_utc() {
        let mut c = cfg("0 2 * * *");
        c.timezone = "Asia/Jakarta".into(); // UTC+7
        let sched = Scheduler::new(c);
        // 2026-01-01 02:00 Jakarta = 2026-01-01 19:00 UTC previous day?
        // Just check it parses and next run is not UTC 02:00.
        let from = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let next = sched.next_run_from(from).unwrap();
        // 02:00 Jakarta is 19:00 UTC previous? Actually 02:00 Jakarta = 19:00 UTC prev day, so from 00:00 UTC Jan1, next Jakarta 02:00 is 19:00 UTC Jan1? Wait compute:
        // Jan1 00:00 UTC = Jan1 07:00 Jakarta, so next 02:00 Jakarta is Jan2 02:00 Jakarta = Jan1 19:00 UTC.
        let expected = Utc.with_ymd_and_hms(2026, 1, 1, 19, 0, 0).unwrap();
        assert_eq!(next, expected);
    }

    #[test]
    fn duration_until_next_is_positive() {
        let sched = Scheduler::new(cfg("*/5 * * * *"));
        let from = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let dur = sched.duration_until_next_from(from).unwrap();
        assert!(dur.as_secs() > 0);
        assert!(dur.as_secs() <= 5 * 60);
    }

    #[tokio::test]
    async fn tick_once_fires_quickly() {
        // every second cron (6-field: "* * * * * *" = every second)
        // Use "* * * * * *" normalized? Use every second via 6-field.
        let mut c = cfg("* * * * * *");
        c.cron = "* * * * * *".into(); // every second
        let sched = Scheduler::new(c);
        let start = Utc::now();
        sched
            .tick_once(|| async { /* no-op */ })
            .await
            .unwrap();
        let elapsed = Utc::now() - start;
        assert!(elapsed.num_milliseconds() < 2000, "should fire within 2s");
    }

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
}
