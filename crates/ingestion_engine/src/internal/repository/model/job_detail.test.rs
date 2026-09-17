use super::*;
use chrono::{TimeZone, Utc};
use serde_json::Value;

#[test]
fn test_create_model_detail() {
    // file_start_range/file_end_range now: Option<DateTime<Utc>> plain UTC for SQL DATETIME
    let now = Some(Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap());
    let transaction_entity = "transaction";
    let transaction_key = "transaction_data";

    let model: JobDetailModel = JobDetailModel {
        id: 1,
        job_id: 1,
        entity: transaction_entity.to_string(),
        key: transaction_key.to_string(),
        file_start_range: now,
        file_end_range: now,
    };

    assert_eq!(model.table_name(), "job_detail");
    assert_eq!(model.get_models_map().get("1").unwrap().id, 1);
    assert_eq!(
        model.get_columns(),
        vec!["id", "job_id", "entity", "key", "file_start_range", "file_end_range"]
    );
    let vals_proj = model.get_val_struct(&[
        "entity".to_string(),
        "key".to_string(),
        "file_end_range".to_string(),
        "file_start_range".to_string(),
    ]);
    assert_eq!(vals_proj.len(), 4);
    assert_eq!(vals_proj[0].as_str().unwrap(), transaction_entity);
    assert_eq!(vals_proj[1].as_str().unwrap(), transaction_key);
    // file_* are Option<DateTime<Utc>> serialized as RFC3339 UTC (plain)
    let expected = serde_json::to_value(&now).unwrap();
    assert_eq!(vals_proj[2], expected);
    assert_eq!(vals_proj[3], expected);
    // serde roundtrip via UTC
    let json = serde_json::to_string(&model).unwrap();
    let back: JobDetailModel = serde_json::from_str(&json).unwrap();
    assert_eq!(back.file_start_range, now);
}

#[test]
fn test_utc_datetime_sql_type() {
    // Ensures file_start_range is DateTime<Utc> with sqlx DATETIME (UTC) semantics, not NaiveDateTime
    let utc = Utc.with_ymd_and_hms(2024, 2, 10, 15, 30, 0).unwrap();
    let model = JobDetailModel {
        id: 1,
        job_id: 1,
        entity: "e".into(),
        key: "k".into(),
        file_start_range: Some(utc),
        file_end_range: None,
    };
    let vals = model.get_val_struct(&["file_start_range".to_string()]);
    assert_eq!(vals[0], serde_json::to_value(&Some(utc)).unwrap());
    let _: Option<chrono::DateTime<chrono::Utc>> = model.file_start_range;
    // SQL DATETIME decode: Naive -> Utc via and_utc()
    let naive = utc.naive_utc();
    assert_eq!(naive.and_utc(), utc);
}
