use pkg::config::mysql::MysqlConfig;
use pkg::mysql::DbPool;

use super::MySqlEvaluatorRepository;

/// Init `MySqlEvaluatorRepository` from `MysqlConfig` (connects to `business_rule`).
/// Uses `pkg::mysql::init` lazy pool — does not require live DB at init time.
pub async fn init(config: &MysqlConfig) -> Result<MySqlEvaluatorRepository, sqlx::Error> {
    let pool = pkg::mysql::init(config).database("business_rule").await?;
    Ok(MySqlEvaluatorRepository::new(pool))
}

/// Init from an existing pool (useful for tests / DI).
pub fn init_with_pool(pool: DbPool) -> MySqlEvaluatorRepository {
    MySqlEvaluatorRepository::new(pool)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn lazy_pool() -> DbPool {
        let dummy: sqlx::mysql::MySqlPool = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        let arc = Arc::new(dummy);
        std::mem::forget(arc.clone());
        arc
    }

    #[test]
    fn test_init_with_pool() {
        let pool = lazy_pool();
        let repo = init_with_pool(pool.clone());
        assert!(Arc::ptr_eq(&repo.pool, &pool));
    }
}
