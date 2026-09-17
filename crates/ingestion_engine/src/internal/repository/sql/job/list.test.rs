use super::*;
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};

use crate::interface::repository::job::JobFilter;

fn dt(s: &str) -> DateTime<Utc> {
    Utc.from_utc_datetime(&NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").unwrap())
}

#[test]
fn test_sql_no_filter() {
    let f = JobFilter::default();
    assert_eq!(
        list_jobs_sql(&f),
        "SELECT id, evaluator_id, status, created_at, updated_at FROM job WHERE 1=1"
    );
}

#[test]
fn test_sql_id_only() {
    let f = JobFilter {
        id: Some(1),
        ..Default::default()
    };
    assert_eq!(
        list_jobs_sql(&f),
        "SELECT id, evaluator_id, status, created_at, updated_at FROM job WHERE 1=1 AND id = ?"
    );
}

#[test]
fn test_sql_evaluator_id_only() {
    let f = JobFilter {
        evaluator_id: Some(2),
        ..Default::default()
    };
    assert_eq!(
        list_jobs_sql(&f),
        "SELECT id, evaluator_id, status, created_at, updated_at FROM job WHERE 1=1 AND evaluator_id = ?"
    );
}

#[test]
fn test_sql_id_and_evaluator_id() {
    let f = JobFilter {
        id: Some(1),
        evaluator_id: Some(2),
        ..Default::default()
    };
    assert_eq!(
        list_jobs_sql(&f),
        "SELECT id, evaluator_id, status, created_at, updated_at FROM job WHERE 1=1 AND id = ? AND evaluator_id = ?"
    );
}

#[test]
fn test_sql_status_only() {
    let f = JobFilter {
        status: Some("PROCESSING".into()),
        ..Default::default()
    };
    assert_eq!(
        list_jobs_sql(&f),
        "SELECT id, evaluator_id, status, created_at, updated_at FROM job WHERE 1=1 AND status = ?"
    );
}

#[test]
fn test_sql_created_at_only() {
    let f = JobFilter {
        created_at: Some(dt("2024-01-01 00:00:00")),
        ..Default::default()
    };
    assert_eq!(
        list_jobs_sql(&f),
        "SELECT id, evaluator_id, status, created_at, updated_at FROM job WHERE 1=1 AND created_at = ?"
    );
}

#[test]
fn test_sql_deleted_at_only() {
    let f = JobFilter {
        deleted_at: Some(dt("2024-01-15 00:00:00")),
        ..Default::default()
    };
    assert_eq!(
        list_jobs_sql(&f),
        "SELECT id, evaluator_id, status, created_at, updated_at FROM job WHERE 1=1 AND deleted_at = ?"
    );
}

#[test]
fn test_sql_status_created_deleted() {
    let f = JobFilter {
        status: Some("COMPLETED".into()),
        created_at: Some(dt("2024-01-01 00:00:00")),
        deleted_at: Some(dt("2024-01-31 23:59:59")),
        ..Default::default()
    };
    assert_eq!(
        list_jobs_sql(&f),
        "SELECT id, evaluator_id, status, created_at, updated_at FROM job WHERE 1=1 AND status = ? AND created_at = ? AND deleted_at = ?"
    );
}

#[test]
fn test_sql_all_fields() {
    let f = JobFilter {
        id: Some(1),
        evaluator_id: Some(2),
        status: Some("ERROR".into()),
        created_at: Some(dt("2024-01-01 00:00:00")),
        deleted_at: Some(dt("2024-01-31 23:59:59")),
    };
    assert_eq!(
        list_jobs_sql(&f),
        "SELECT id, evaluator_id, status, created_at, updated_at FROM job WHERE 1=1 AND id = ? AND evaluator_id = ? AND status = ? AND created_at = ? AND deleted_at = ?"
    );
}

#[test]
fn test_sql_id_status() {
    let f = JobFilter {
        id: Some(5),
        status: Some("PROCESSING".into()),
        ..Default::default()
    };
    assert_eq!(
        list_jobs_sql(&f),
        "SELECT id, evaluator_id, status, created_at, updated_at FROM job WHERE 1=1 AND id = ? AND status = ?"
    );
}

#[test]
fn test_sql_evaluator_created() {
    let f = JobFilter {
        evaluator_id: Some(10),
        created_at: Some(dt("2024-02-01 00:00:00")),
        ..Default::default()
    };
    assert_eq!(
        list_jobs_sql(&f),
        "SELECT id, evaluator_id, status, created_at, updated_at FROM job WHERE 1=1 AND evaluator_id = ? AND created_at = ?"
    );
}
