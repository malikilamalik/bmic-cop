use super::*;

fn fixture() -> RuleModel {
    RuleModel {
        id: 1,
        evaluator_id: 2,
        content: serde_json::json!({"key": "value"}),
        input: serde_json::json!({"in": 1}),
        version: Some(1),
        description: Some("test".to_string()),
        is_active: true,
        created_at: None,
        created_by: None,
    }
}

#[test]
fn test_table_name() {
    assert_eq!(fixture().table_name(), "rule");
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
    assert_eq!(map["1"].evaluator_id, 2);
}

#[test]
fn test_get_columns() {
    let f = fixture();
    assert_eq!(
        f.get_columns(),
        vec![
            "id",
            "evaluator_id",
            "content",
            "input",
            "version",
            "description",
            "is_active",
            "created_at",
            "created_by"
        ]
    );
}

#[test]
fn test_get_val_struct() {
    let f = fixture();
    let columns = f.get_columns();
    let values = f.get_val_struct(&columns);
    assert_eq!(values[0], serde_json::json!(1));
    assert_eq!(values[1], serde_json::json!(2));
    assert_eq!(values[2], serde_json::json!({"key": "value"}));
    assert_eq!(values[4], serde_json::json!(1));
    assert_eq!(values[6], serde_json::json!(true));
}

#[test]
fn test_get_val_struct_missing_column_returns_null() {
    let f = fixture();
    let values = f.get_val_struct(&["nonexistent".to_string()]);
    assert_eq!(values[0], Value::Null);
}
