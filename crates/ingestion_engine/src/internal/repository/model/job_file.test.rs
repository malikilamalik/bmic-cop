use super::*;
use chrono::{TimeZone, Utc};

#[test]
fn test_create_model_file() {
    let now = Some(Utc.with_ymd_and_hms(2024, 1, 15, 10, 0, 0).unwrap());
    let filename = "data.parquet";
    let status = "PROCESSING";

    let model: JobFileModel = JobFileModel {
        id: 1,
        job_detail_id: 10,
        filename: filename.to_string(),
        status: status.to_string(),
        created_at: now,
        updated_at: now,
    };

    assert_eq!(model.table_name(), "job_file");
    assert_eq!(model.get_models_map().get("1").unwrap().id, 1);
    assert_eq!(
        model.get_columns(),
        vec![
            "id",
            "job_detail_id",
            "filename",
            "status",
            "created_at",
            "updated_at"
        ]
    );
    let vals_proj = model.get_val_struct(&[
        "filename".to_string(),
        "status".to_string(),
        "created_at".to_string(),
        "job_detail_id".to_string(),
    ]);
    assert_eq!(vals_proj.len(), 4);
    assert_eq!(vals_proj[0].as_str().unwrap(), filename);
    assert_eq!(vals_proj[1].as_str().unwrap(), status);
    let expected = serde_json::to_value(&now).unwrap();
    assert_eq!(vals_proj[2], expected);
    assert_eq!(vals_proj[3].as_i64().unwrap(), 10);
    let json = serde_json::to_string(&model).unwrap();
    let back: JobFileModel = serde_json::from_str(&json).unwrap();
    assert_eq!(back.created_at, now);
}
