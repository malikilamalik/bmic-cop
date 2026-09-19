use super::*;

#[test]
fn list_all_sql_has_no_id_predicate_but_keeps_guards() {
    let sql = list_evaluators_sql(false);
    assert!(!sql.contains("id = ?"));
    assert!(sql.contains("deleted_at IS NULL"));
    assert!(sql.contains("start_valid_date IS NULL OR start_valid_date <= ?"));
    assert!(sql.contains("end_valid_date IS NULL OR end_valid_date >= ?"));
    assert!(!sql.contains("NOW()"));
}

#[test]
fn list_by_id_sql_filters_on_id_and_keeps_guards() {
    let sql = list_evaluators_sql(true);
    assert!(sql.contains("id = ?"));
    assert!(sql.contains("deleted_at IS NULL"));
    assert!(sql.contains("start_valid_date IS NULL OR start_valid_date <= ?"));
    assert!(sql.contains("end_valid_date IS NULL OR end_valid_date >= ?"));
    assert!(!sql.contains("NOW()"));
}

#[test]
fn list_queries_use_bind_placeholders_for_now() {
    for has_id in [true, false] {
        let sql = list_evaluators_sql(has_id);
        // each query must have exactly two `?` for the `now` bounds, plus optional id
        let expected = if has_id { 3 } else { 2 };
        assert_eq!(
            sql.matches('?').count(),
            expected,
            "unexpected placeholder count for has_id={has_id}: {sql}"
        );
    }
}
