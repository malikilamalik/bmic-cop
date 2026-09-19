use super::*;

fn fixture() -> BenefitModel {
    BenefitModel {
        id: 1,
        evaluator_id: 2,
        customer_id: 3,
        value: None,
        description: Some("test".to_string()),
        expired_at: None,
        created_at: None,
    }
}

#[test]
fn test_model_benefit() {
    assert_eq!(fixture().table_name(), "benefit");
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
    assert_eq!(map["1"].customer_id, 3);
}

#[test]
fn test_get_columns() {
    let f = fixture();
    assert_eq!(
        f.get_columns(),
        vec![
            "id",
            "evaluator_id",
            "customer_id",
            "value",
            "description",
            "expired_at",
            "created_at"
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
    assert_eq!(values[2], serde_json::json!(3));
    assert_eq!(values[4], serde_json::json!("test"));
}

#[test]
fn test_get_val_struct_missing_column_returns_null() {
    let f = fixture();
    let values = f.get_val_struct(&["nonexistent".to_string()]);
    assert_eq!(values[0], Value::Null);
}
