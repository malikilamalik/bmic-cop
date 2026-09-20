/// List-all query: every row counts only when it is not soft-deleted
/// (`deleted_at IS NULL`) and each validity bound either is NULL
/// (open-ended) or contains the current timestamp. The timestamp is
/// supplied from Rust (`Utc::now()`) and bound as `?`, never via
/// MySQL `NOW()`.
const LIST_ALL_SQL: &str = "SELECT id, name, CAST(start_valid_date AS DATETIME) AS start_valid_date, CAST(end_valid_date AS DATETIME) AS end_valid_date, is_active, running_frequency, CAST(created_at AS DATETIME) AS created_at, created_by, CAST(updated_at AS DATETIME) AS updated_at, updated_by, CAST(deleted_at AS DATETIME) AS deleted_at, deleted_by FROM evaluator WHERE is_active = TRUE AND deleted_at IS NULL AND (start_valid_date IS NULL OR start_valid_date <= ?) AND (end_valid_date IS NULL OR end_valid_date >= ?)";

const LIST_BY_ID_SQL: &str = "SELECT id, name, CAST(start_valid_date AS DATETIME) AS start_valid_date, CAST(end_valid_date AS DATETIME) AS end_valid_date, is_active, running_frequency, CAST(created_at AS DATETIME) AS created_at, created_by, CAST(updated_at AS DATETIME) AS updated_at, updated_by, CAST(deleted_at AS DATETIME) AS deleted_at, deleted_by FROM evaluator WHERE is_active = TRUE AND deleted_at IS NULL AND id = ? AND (start_valid_date IS NULL OR start_valid_date <= ?) AND (end_valid_date IS NULL OR end_valid_date >= ?)";

/// Returns the list query for the given filter shape. `has_id == true`
/// adds an `AND id = ?` predicate; otherwise all rows (still excluding
/// soft-deleted and out-of-window ones) are returned.
///
/// Both queries are `&'static str` literals, which also satisfies
/// sqlx's `SqlSafeStr` bound on `query_as`.
pub fn list_evaluators_sql(has_id: bool) -> &'static str {
    if has_id {
        LIST_BY_ID_SQL
    } else {
        LIST_ALL_SQL
    }
}

#[cfg(test)]
#[path = "list.test.rs"]
mod tests;
