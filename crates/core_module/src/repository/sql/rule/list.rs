use crate::interface::repository::rule_repository::RuleFilter;

/// Builds SQL for rule get/list. Always filters `is_active = TRUE` as
/// required by the business rule (only active rules are valid), and
/// optionally filters by `id` and `evaluator_id`.
///
/// Used for `get` (single rule by evaluator_id where is_active=true) and
/// `list` variants.
pub fn list_rules_sql(filter: &RuleFilter) -> String {
    let mut sql = String::from(
        "SELECT id, evaluator_id, content, input, version, description, is_active, created_at, created_by FROM rule WHERE is_active = TRUE",
    );

    if filter.id.is_some() {
        sql.push_str(" AND id = ?");
    }

    if filter.evaluator_id.is_some() {
        sql.push_str(" AND evaluator_id = ?");
    }

    sql
}

/// Convenience helper for the exact use-case: get active rule(s) by evaluator_id.
///
/// Equivalent to `list_rules_sql` with only `evaluator_id` set.
pub fn get_active_rule_by_evaluator_sql() -> &'static str {
    "SELECT id, evaluator_id, content, input, version, description, is_active, created_at, created_by FROM rule WHERE evaluator_id = ? AND is_active = TRUE"
}

#[cfg(test)]
#[path = "list.test.rs"]
mod tests;
