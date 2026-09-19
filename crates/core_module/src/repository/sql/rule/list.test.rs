use super::*;
use crate::interface::repository::rule_repository::RuleFilter;

#[test]
fn list_sql_always_filters_is_active_true() {
    let f = RuleFilter::default();
    let sql = list_rules_sql(&f);
    assert!(sql.contains("is_active = TRUE"), "sql must filter is_active: {}", sql);
    assert!(!sql.contains("is_active = ?"), "is_active should be hardcoded TRUE, not bind");
}

#[test]
fn list_sql_with_evaluator_id_and_is_active() {
    let f = RuleFilter {
        evaluator_id: Some(7),
        ..Default::default()
    };
    let sql = list_rules_sql(&f);
    assert!(sql.contains("is_active = TRUE"));
    assert!(sql.contains("evaluator_id = ?"));
    assert_eq!(sql, "SELECT id, evaluator_id, content, input, version, description, is_active, created_at, created_by FROM rule WHERE is_active = TRUE AND evaluator_id = ?");
}

#[test]
fn list_sql_with_id_and_evaluator_id() {
    let f = RuleFilter {
        id: Some(1),
        evaluator_id: Some(7),
    };
    let sql = list_rules_sql(&f);
    assert!(sql.contains("is_active = TRUE"));
    assert!(sql.contains("id = ?"));
    assert!(sql.contains("evaluator_id = ?"));
    // id before evaluator_id (insertion order)
    assert!(sql.find("id = ?").unwrap() < sql.find("evaluator_id = ?").unwrap());
}

#[test]
fn get_active_rule_by_evaluator_sql_is_correct() {
    let sql = get_active_rule_by_evaluator_sql();
    assert_eq!(
        sql,
        "SELECT id, evaluator_id, content, input, version, description, is_active, created_at, created_by FROM rule WHERE evaluator_id = ? AND is_active = TRUE"
    );
}

#[test]
fn list_sql_no_injection_placeholder_for_is_active() {
    let f = RuleFilter {
        id: Some(1),
        ..Default::default()
    };
    let sql = list_rules_sql(&f);
    // only id placeholder, is_active is literal
    assert_eq!(sql.matches('?').count(), 1);
}
