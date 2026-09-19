use super::*;
use crate::interface::repository::rule_repository::RuleFilter;
use crate::repository::model::rule::RuleModel;

fn fixture(id: i64, evaluator_id: i64) -> RuleModel {
    RuleModel {
        id,
        evaluator_id,
        content: serde_json::json!({"key": "value"}),
        input: serde_json::json!({"in": 1}),
        version: Some(1),
        description: Some("test".into()),
        is_active: true,
        created_at: None,
        created_by: Some(1),
    }
}

#[test]
fn get_sql_uses_is_active_true_with_evaluator_id() {
    // Verify the SQL that get() will use for the required use-case:
    // get rule with evaluator_id and is_active=true
    let filter = RuleFilter {
        evaluator_id: Some(42),
        ..Default::default()
    };
    let sql = crate::repository::sql::rule::list::list_rules_sql(&filter);
    assert!(sql.contains("evaluator_id = ?"));
    assert!(sql.contains("is_active = TRUE"));
    // Should be fetch_one path — ensure placeholder count is 1 (only evaluator_id)
    assert_eq!(sql.matches('?').count(), 1);
}

#[test]
fn get_sql_with_id_and_evaluator_binds_both() {
    let filter = RuleFilter {
        id: Some(1),
        evaluator_id: Some(7),
    };
    let sql = crate::repository::sql::rule::list::list_rules_sql(&filter);
    assert_eq!(sql.matches('?').count(), 2);
}
