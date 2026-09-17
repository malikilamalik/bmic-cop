use super::*;
use chrono::{TimeZone, Utc};

#[test]
fn test_create_model_job() {
    // Use UTC datetime SQL type directly
    let now = Some(Utc.with_ymd_and_hms(2024, 1, 15, 10, 0, 0).unwrap());
    let status = "PROCESSING";

    let model: JobModel = JobModel {
        id: 1,
        evaluator_id: 42,
        status: status.to_string(),
        created_at: now,
        updated_at: now,
    };

    assert_eq!(model.table_name(), "job");
    assert_eq!(model.get_models_map().get("1").unwrap().id, 1);
    assert_eq!(
        model.get_columns(),
        vec!["id", "evaluator_id", "status", "created_at", "updated_at"]
    );
    let vals_proj = model.get_val_struct(&[
        "status".to_string(),
        "evaluator_id".to_string(),
        "created_at".to_string(),
    ]);
    assert_eq!(vals_proj.len(), 3);
    assert_eq!(vals_proj[0].as_str().unwrap(), status);
    assert_eq!(vals_proj[1].as_i64().unwrap(), 42);
    // UTC datetime SQL: get_val_struct uses serde_json::to_value (RFC3339)
    let expected = serde_json::to_value(&now).unwrap();
    assert_eq!(vals_proj[2], expected);
    // serde roundtrip
    let json = serde_json::to_string(&model).unwrap();
    let back: JobModel = serde_json::from_str(&json).unwrap();
    assert_eq!(back.created_at, now);
}
