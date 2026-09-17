use super::*;
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};

use crate::interface::repository::job_file::JobFileFilter;

fn dt(s: &str) -> DateTime<Utc> {
    Utc.from_utc_datetime(&NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").unwrap())
}

#[test]
fn test_sql_no_filter() {
    let f = JobFileFilter::default();
    assert_eq!(
        list_job_files_sql(&f),
        "SELECT id, job_detail_id, filename, status, created_at, updated_at FROM job_file WHERE 1=1"
    );
}

#[test]
fn test_sql_id_only() {
    let f = JobFileFilter {
        id: Some(1),
        ..Default::default()
    };
    assert_eq!(
        list_job_files_sql(&f),
        "SELECT id, job_detail_id, filename, status, created_at, updated_at FROM job_file WHERE 1=1 AND id = ?"
    );
}

#[test]
fn test_sql_job_detail_id_only() {
    let f = JobFileFilter {
        job_detail_id: Some(2),
        ..Default::default()
    };
    assert_eq!(
        list_job_files_sql(&f),
        "SELECT id, job_detail_id, filename, status, created_at, updated_at FROM job_file WHERE 1=1 AND job_detail_id = ?"
    );
}

#[test]
fn test_sql_id_and_job_detail_id() {
    let f = JobFileFilter {
        id: Some(1),
        job_detail_id: Some(2),
        ..Default::default()
    };
    assert_eq!(
        list_job_files_sql(&f),
        "SELECT id, job_detail_id, filename, status, created_at, updated_at FROM job_file WHERE 1=1 AND id = ? AND job_detail_id = ?"
    );
}

#[test]
fn test_sql_filename_only() {
    let f = JobFileFilter {
        filename: Some("data.parquet".into()),
        ..Default::default()
    };
    assert_eq!(
        list_job_files_sql(&f),
        "SELECT id, job_detail_id, filename, status, created_at, updated_at FROM job_file WHERE 1=1 AND filename = ?"
    );
}

#[test]
fn test_sql_status_only() {
    let f = JobFileFilter {
        status: Some("PROCESSING".into()),
        ..Default::default()
    };
    assert_eq!(
        list_job_files_sql(&f),
        "SELECT id, job_detail_id, filename, status, created_at, updated_at FROM job_file WHERE 1=1 AND status = ?"
    );
}

#[test]
fn test_sql_created_at_only() {
    let f = JobFileFilter {
        created_at: Some(dt("2024-01-01 00:00:00")),
        ..Default::default()
    };
    assert_eq!(
        list_job_files_sql(&f),
        "SELECT id, job_detail_id, filename, status, created_at, updated_at FROM job_file WHERE 1=1 AND created_at = ?"
    );
}

#[test]
fn test_sql_deleted_at_only() {
    let f = JobFileFilter {
        deleted_at: Some(dt("2024-01-15 00:00:00")),
        ..Default::default()
    };
    assert_eq!(
        list_job_files_sql(&f),
        "SELECT id, job_detail_id, filename, status, created_at, updated_at FROM job_file WHERE 1=1 AND deleted_at = ?"
    );
}

#[test]
fn test_sql_all_fields() {
    let f = JobFileFilter {
        id: Some(1),
        job_detail_id: Some(2),
        filename: Some("file.txt".into()),
        status: Some("COMPLETED".into()),
        created_at: Some(dt("2024-01-01 00:00:00")),
        deleted_at: Some(dt("2024-01-31 23:59:59")),
    };
    assert_eq!(
        list_job_files_sql(&f),
        "SELECT id, job_detail_id, filename, status, created_at, updated_at FROM job_file WHERE 1=1 AND id = ? AND job_detail_id = ? AND filename = ? AND status = ? AND created_at = ? AND deleted_at = ?"
    );
}

#[test]
fn test_sql_filename_status() {
    let f = JobFileFilter {
        filename: Some("a.csv".into()),
        status: Some("ERROR".into()),
        ..Default::default()
    };
    assert_eq!(
        list_job_files_sql(&f),
        "SELECT id, job_detail_id, filename, status, created_at, updated_at FROM job_file WHERE 1=1 AND filename = ? AND status = ?"
    );
}
