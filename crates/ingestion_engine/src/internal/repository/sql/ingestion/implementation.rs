use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use pkg::proto::client::{IngestRequest, TimeRange};

use super::MySqlIngestionRepository;

// ---------------------------------------------------------------------------
// Pure helpers (no DB) – also re-used by application/client
// ---------------------------------------------------------------------------

/// Parse an ISO8601 datetime string like "2026-09-01T00:00:00" or "2026-09-01T00:00:00Z"
/// into `DateTime<Utc>`.  Falls back to NaiveDateTime interpreted as UTC.
pub fn parse_datetime(s: &str) -> Result<DateTime<Utc>, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty datetime".into());
    }
    // Try RFC3339 first (handles Z and offset)
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.with_timezone(&Utc));
    }
    // Try NaiveDateTime with T separator
    for fmt in &[
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d",
    ] {
        if let Ok(naive) = NaiveDateTime::parse_from_str(s, fmt) {
            return Ok(naive.and_utc());
        }
        // also try date only
        if *fmt == "%Y-%m-%d" {
            if let Ok(d) = NaiveDate::parse_from_str(s, fmt) {
                return Ok(d.and_hms_opt(0, 0, 0).unwrap().and_utc());
            }
        }
    }
    Err(format!("invalid datetime '{}'", s))
}

/// Resolve `TimeRange` into (start, end) `DateTime<Utc>`.
///
/// - `type == "between"` => parse `start` and `end` fields.
/// - `type == "named"` with `value == "today"` => today 00:00:00 .. 23:59:59 UTC.
/// - Missing / unknown => today.
pub fn resolve_time_range(tr: Option<&TimeRange>) -> (DateTime<Utc>, DateTime<Utc>) {
    let today = Utc::now().date_naive();
    let today_start = today.and_hms_opt(0, 0, 0).unwrap().and_utc();
    let today_end = today.and_hms_opt(23, 59, 59).unwrap().and_utc();

    match tr {
        None => (today_start, today_end),
        Some(r) => {
            let t = r.r#type.trim().to_lowercase();
            if t == "between" {
                let s = parse_datetime(&r.start);
                let e = parse_datetime(&r.end);
                match (s, e) {
                    (Ok(s), Ok(e)) => {
                        // ensure s <= e, swap if needed
                        if s <= e { (s, e) } else { (e, s) }
                    }
                    _ => (today_start, today_end),
                }
            } else if t == "named" {
                let v = r.value.trim().to_lowercase();
                if v == "today" || v.is_empty() {
                    (today_start, today_end)
                } else if v == "yesterday" {
                    let y = today - chrono::Duration::days(1);
                    (y.and_hms_opt(0,0,0).unwrap().and_utc(), y.and_hms_opt(23,59,59).unwrap().and_utc())
                } else {
                    // unknown named => treat as today for forward compatibility
                    (today_start, today_end)
                }
            } else {
                // unknown type => today
                (today_start, today_end)
            }
        }
    }
}

/// Generate filenames for a given entity and date range.
///
/// - `transaction` => `transactionYYYYMMDD.txt` per day inclusive.
/// - `customer` => `customer.txt` single file (ignores range).
/// - other => `<entity>.txt` single file.
pub fn filenames_for_entity(entity: &str, start: DateTime<Utc>, end: DateTime<Utc>) -> Vec<String> {
    let e = entity.trim().to_lowercase();
    if e == "transaction" {
        let mut out = Vec::new();
        let mut cur = start.date_naive();
        let end_date = end.date_naive();
        // inclusive loop, guard against infinite if start > end (swap)
        let (mut cur_date, end_date) = if cur <= end_date { (cur, end_date) } else { (end_date, cur) };
        while cur_date <= end_date {
            out.push(format!("transaction{}.txt", cur_date.format("%Y%m%d")));
            cur_date = cur_date.succ_opt().unwrap();
        }
        if out.is_empty() {
            // fallback single day
            out.push(format!("transaction{}.txt", start.format("%Y%m%d")));
        }
        out
    } else if e == "customer" {
        vec!["customer.txt".to_string()]
    } else if e.is_empty() {
        vec!["customer.txt".to_string()]
    } else {
        // generic fallback: one file per entity
        vec![format!("{}.txt", e)]
    }
}

// ---------------------------------------------------------------------------
// DB helpers
// ---------------------------------------------------------------------------

pub async fn insert_job(pool: &pkg::mysql::DbPool, evaluator_id: u64) -> Result<u64, sqlx::Error> {
    let res = sqlx::query("INSERT INTO job (evaluator_id, status) VALUES (?, 'PROCESSING')")
        .bind(evaluator_id as i64)
        .execute(pool.as_ref())
        .await?;
    Ok(res.last_insert_id())
}

pub async fn insert_job_detail(
    pool: &pkg::mysql::DbPool,
    job_id: u64,
    entity: &str,
    key: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<u64, sqlx::Error> {
    let start_naive: NaiveDateTime = start.naive_utc();
    let end_naive: NaiveDateTime = end.naive_utc();
    let res = sqlx::query(
        "INSERT INTO job_detail (job_id, entity, `key`, file_start_range, file_end_range) VALUES (?, ?, ?, ?, ?)",
    )
    .bind(job_id as i64)
    .bind(entity)
    .bind(key)
    .bind(start_naive)
    .bind(end_naive)
    .execute(pool.as_ref())
    .await?;
    Ok(res.last_insert_id())
}

pub async fn insert_job_file(
    pool: &pkg::mysql::DbPool,
    job_detail_id: u64,
    filename: &str,
) -> Result<u64, sqlx::Error> {
    let res = sqlx::query(
        "INSERT INTO job_file (job_detail_id, filename, status) VALUES (?, ?, 'PROCESSING')",
    )
    .bind(job_detail_id as i64)
    .bind(filename)
    .execute(pool.as_ref())
    .await?;
    Ok(res.last_insert_id())
}

/// High-level ingestion: takes `IngestRequest`, inserts job / job_detail / job_file.
/// Returns inserted job ids. Uses one transaction per job for atomicity.
pub async fn ingest_request(
    pool: &pkg::mysql::DbPool,
    req: &IngestRequest,
) -> Result<Vec<u64>, sqlx::Error> {
    // Merge both `job` and `jobs` fields for compatibility
    let mut all_jobs = Vec::new();
    all_jobs.extend(req.job.iter().cloned());
    all_jobs.extend(req.jobs.iter().cloned());

    let mut job_ids = Vec::with_capacity(all_jobs.len());

    for j in all_jobs {
        // Use a transaction per job so job_detail/job_file are atomic
        let mut tx = pool.begin().await?;
        let res = sqlx::query("INSERT INTO job (evaluator_id, status) VALUES (?, 'PROCESSING')")
            .bind(j.evaluator_id as i64)
            .execute(&mut *tx)
            .await?;
        let job_id = res.last_insert_id();

        for inp in &j.input {
            let (start, end) = resolve_time_range(inp.time_range.as_ref());
            let start_naive = start.naive_utc();
            let end_naive = end.naive_utc();
            let detail_res = sqlx::query(
                "INSERT INTO job_detail (job_id, entity, `key`, file_start_range, file_end_range) VALUES (?, ?, ?, ?, ?)",
            )
            .bind(job_id as i64)
            .bind(&inp.entity)
            .bind(&inp.key)
            .bind(start_naive)
            .bind(end_naive)
            .execute(&mut *tx)
            .await?;
            let detail_id = detail_res.last_insert_id();

            let filenames = filenames_for_entity(&inp.entity, start, end);
            for fname in filenames {
                sqlx::query(
                    "INSERT INTO job_file (job_detail_id, filename, status) VALUES (?, ?, 'PROCESSING')",
                )
                .bind(detail_id as i64)
                .bind(&fname)
                .execute(&mut *tx)
                .await?;
            }
        }
        tx.commit().await?;
        job_ids.push(job_id);
    }
    Ok(job_ids)
}

impl MySqlIngestionRepository {
    pub async fn ingest(&self, req: &IngestRequest) -> Result<Vec<u64>, sqlx::Error> {
        ingest_request(&self.pool, req).await
    }

    // Expose pure helpers for testing / reuse
    pub fn filenames_for(entity: &str, start: DateTime<Utc>, end: DateTime<Utc>) -> Vec<String> {
        filenames_for_entity(entity, start, end)
    }
    pub fn resolve_range(tr: Option<&TimeRange>) -> (DateTime<Utc>, DateTime<Utc>) {
        resolve_time_range(tr)
    }
}

// For unit tests that don't need DB: also expose list helpers
pub fn all_jobs_from_request(req: &IngestRequest) -> Vec<pkg::proto::client::Job> {
    let mut v = Vec::new();
    v.extend(req.job.clone());
    v.extend(req.jobs.clone());
    v
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use pkg::proto::client::{Job, TimeRange};

    fn dt(s: &str) -> DateTime<Utc> {
        parse_datetime(s).unwrap()
    }

    #[test]
    fn test_parse_between() {
        let tr = TimeRange { r#type: "between".into(), value: "".into(), start: "2026-09-01T00:00:00".into(), end: "2026-09-07T23:59:59".into() };
        let (s,e) = resolve_time_range(Some(&tr));
        assert_eq!(s, dt("2026-09-01T00:00:00"));
        assert_eq!(e, dt("2026-09-07T23:59:59"));
    }

    #[test]
    fn test_filenames_transaction_between() {
        let s = dt("2026-09-01T00:00:00");
        let e = dt("2026-09-07T23:59:59");
        let files = filenames_for_entity("transaction", s, e);
        assert_eq!(files.len(), 7);
        assert_eq!(files[0], "transaction20260901.txt");
        assert_eq!(files[6], "transaction20260907.txt");
    }

    #[test]
    fn test_filenames_transaction_single() {
        let s = dt("2026-09-14T00:00:00");
        let files = filenames_for_entity("transaction", s, s);
        assert_eq!(files, vec!["transaction20260914.txt"]);
    }

    #[test]
    fn test_filenames_customer() {
        let s = dt("2026-09-01T00:00:00");
        let e = dt("2026-09-07T23:59:59");
        let files = filenames_for_entity("customer", s, e);
        assert_eq!(files, vec!["customer.txt"]);
    }

    #[test]
    fn test_named_today_range_span_is_single_day() {
        let tr = TimeRange { r#type: "named".into(), value: "today".into(), start: "".into(), end: "".into() };
        let (s,e) = resolve_time_range(Some(&tr));
        assert_eq!(s.date_naive(), e.date_naive());
        assert_eq!(s.date_naive(), Utc::now().date_naive());
        let files = filenames_for_entity("transaction", s, e);
        assert_eq!(files.len(), 1);
        let expected = format!("transaction{}.txt", Utc::now().format("%Y%m%d"));
        assert_eq!(files[0], expected);
    }

    #[test]
    fn test_all_jobs_merge() {
        let req = IngestRequest {
            job: vec![Job { evaluator_id: 1, input: vec![] }],
            jobs: vec![Job { evaluator_id: 2, input: vec![] }],
        };
        let all = all_jobs_from_request(&req);
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].evaluator_id, 1);
        assert_eq!(all[1].evaluator_id, 2);
    }

    #[test]
    fn test_parse_datetime_variants() {
        assert!(parse_datetime("2026-09-01T00:00:00").is_ok());
        assert!(parse_datetime("2026-09-01T00:00:00Z").is_ok());
        assert!(parse_datetime("2026-09-01 00:00:00").is_ok());
        assert!(parse_datetime("").is_err());
    }
}
