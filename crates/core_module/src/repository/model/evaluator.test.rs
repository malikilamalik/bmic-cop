use super::*;

fn fixture() -> EvaluatorModel {
    EvaluatorModel {
        id: 1,
        name: "eval".to_string(),
        start_valid_date: None,
        end_valid_date: None,
        is_active: true,
        running_frequency: Some("MONTHLY".to_string()),
        created_at: None,
        created_by: None,
        updated_at: None,
        updated_by: None,
        deleted_at: None,
        deleted_by: None,
    }
}

#[test]
fn test_table_name() {
    assert_eq!(fixture().table_name(), "evaluator");
}

#[test]
fn test_get_models() {
    let models = fixture().get_models();
    assert_eq!(models.len(), 1);
    assert_eq!(models[0].id, 1);
}

#[test]
fn test_get_models_map() {
    let map = fixture().get_models_map();
    assert_eq!(map.len(), 1);
    assert!(map.contains_key("1"));
    assert_eq!(map["1"].name, "eval");
}

#[test]
fn test_get_columns() {
    let f = fixture();
    assert_eq!(
        f.get_columns(),
        vec![
            "id",
            "name",
            "start_valid_date",
            "end_valid_date",
            "is_active",
            "running_frequency",
            "created_at",
            "created_by",
            "updated_at",
            "updated_by",
            "deleted_at",
            "deleted_by"
        ]
    );
}

#[test]
fn test_get_val_struct() {
    let f = fixture();
    let columns = f.get_columns();
    let values = f.get_val_struct(&columns);
    assert_eq!(values[0], serde_json::json!(1));
    assert_eq!(values[1], serde_json::json!("eval"));
    assert_eq!(values[4], serde_json::json!(true));
    assert_eq!(values[5], serde_json::json!("MONTHLY"));
}

#[test]
fn test_get_val_struct_missing_column_returns_null() {
    let f = fixture();
    let values = f.get_val_struct(&["nonexistent".to_string()]);
    assert_eq!(values[0], Value::Null);
}
