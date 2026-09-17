use super::*;
use chrono::{DateTime, TimeZone, Utc};

fn dt(s: &str) -> DateTime<Utc> {
    // UTC datetime for SQL: "2024-01-01 00:00:00" as UTC
    Utc.from_utc_datetime(&NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").unwrap())
}

#[test]
fn test_sql_no_filter() {
    let f = JobDetailFilter::default();
    assert_eq!(
            list_job_details_sql(&f),
            "SELECT id, job_id, entity, `key`, file_start_range, file_end_range FROM job_detail WHERE 1=1"
        );
}

#[test]
fn test_sql_job_id_only() {
    let f = JobDetailFilter {
        job_id: Some(42),
        ..Default::default()
    };
    assert_eq!(
            list_job_details_sql(&f),
            "SELECT id, job_id, entity, `key`, file_start_range, file_end_range FROM job_detail WHERE 1=1 AND job_id = ?"
        );
}

#[test]
fn test_sql_between_range() {
    let f = JobDetailFilter {
        job_id: Some(1),
        file_start_range: Some(dt("2024-01-01 00:00:00")),
        file_end_range: Some(dt("2024-01-31 23:59:59")),
        ..Default::default()
    };
    assert_eq!(
            list_job_details_sql(&f),
            "SELECT id, job_id, entity, `key`, file_start_range, file_end_range FROM job_detail WHERE 1=1 AND job_id = ? AND file_start_range >= ? AND file_end_range <= ?"
        );
}

#[test]
fn test_sql_between_without_job_id() {
    let f = JobDetailFilter {
        file_start_range: Some(dt("2024-01-01 00:00:00")),
        file_end_range: Some(dt("2024-01-31 23:59:59")),
        ..Default::default()
    };
    assert_eq!(
            list_job_details_sql(&f),
            "SELECT id, job_id, entity, `key`, file_start_range, file_end_range FROM job_detail WHERE 1=1 AND file_start_range >= ? AND file_end_range <= ?"
        );
}

#[test]
fn test_sql_typo_field_compat() {
    let f = JobDetailFilter {
        file_start_range: Some(dt("2024-01-01 00:00:00")),
        file_end_range: Some(dt("2024-01-31 23:59:59")),
        ..Default::default()
    };
    assert_eq!(
            list_job_details_sql(&f),
            "SELECT id, job_id, entity, `key`, file_start_range, file_end_range FROM job_detail WHERE 1=1 AND file_start_range >= ? AND file_end_range <= ?"
        );
}

#[test]
fn test_sql_id_only() {
    let f = JobDetailFilter {
        id: Some(5),
        ..Default::default()
    };
    assert_eq!(
            list_job_details_sql(&f),
            "SELECT id, job_id, entity, `key`, file_start_range, file_end_range FROM job_detail WHERE 1=1 AND id = ?"
        );
}

#[test]
fn test_sql_file_start_only() {
    let f = JobDetailFilter {
        file_start_range: Some(dt("2024-01-01 00:00:00")),
        ..Default::default()
    };
    assert_eq!(
            list_job_details_sql(&f),
            "SELECT id, job_id, entity, `key`, file_start_range, file_end_range FROM job_detail WHERE 1=1 AND file_start_range >= ?"
        );
}

#[test]
fn test_sql_file_end_only() {
    let f = JobDetailFilter {
        file_end_range: Some(dt("2024-01-31 23:59:59")),
        ..Default::default()
    };
    assert_eq!(
            list_job_details_sql(&f),
            "SELECT id, job_id, entity, `key`, file_start_range, file_end_range FROM job_detail WHERE 1=1 AND file_end_range <= ?"
        );
}

#[test]
fn test_sql_id_job_both_ranges() {
    let f = JobDetailFilter {
        id: Some(9),
        job_id: Some(1),
        file_start_range: Some(dt("2024-01-01 00:00:00")),
        file_end_range: Some(dt("2024-01-31 23:59:59")),
        ..Default::default()
    };
    assert_eq!(
            list_job_details_sql(&f),
            "SELECT id, job_id, entity, `key`, file_start_range, file_end_range FROM job_detail WHERE 1=1 AND id = ? AND job_id = ? AND file_start_range >= ? AND file_end_range <= ?"
        );
}

#[test]
fn test_sql_file_star_alone() {
    let f = JobDetailFilter {
        file_start_range: Some(dt("2024-01-01 00:00:00")),
        ..Default::default()
    };
    assert_eq!(
            list_job_details_sql(&f),
            "SELECT id, job_id, entity, `key`, file_start_range, file_end_range FROM job_detail WHERE 1=1 AND file_start_range >= ?"
        );
}

#[test]
fn test_utc_roundtrip() {
    let s = "2024-01-01 00:00:00";
    let utc = dt(s);
    assert_eq!(utc, Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap());
    // Naive -> Utc roundtrip preserves UTC
    let naive = utc.naive_utc();
    assert_eq!(naive.and_utc(), utc);
}

#[test]
fn test_decode_datetime_as_utc() {
    let naive = NaiveDateTime::parse_from_str("2024-01-15 08:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
    let utc = naive.and_utc();
    assert_eq!(utc, Utc.with_ymd_and_hms(2024, 1, 15, 8, 0, 0).unwrap());
}

#[test]
fn test_decode_datetime_option_mapping() {
    let some: Option<NaiveDateTime> =
        Some(NaiveDateTime::parse_from_str("2024-02-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap());
    let none: Option<NaiveDateTime> = None;
    assert!(some.map(|n| n.and_utc()).is_some());
    assert!(none.map(|n| n.and_utc()).is_none());
    let mapped_none: Option<DateTime<Utc>> = none.map(|n| n.and_utc());
    assert_eq!(mapped_none, None);
}
