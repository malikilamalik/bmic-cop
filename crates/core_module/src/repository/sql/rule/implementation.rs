use async_trait::async_trait;

use super::MySqlRuleRepository;
use super::list::list_rules_sql;
use crate::interface::repository::rule_repository::{RuleFilter, RuleRepository};
use crate::repository::model::rule::RuleModel;

#[async_trait]
impl RuleRepository for MySqlRuleRepository {
    async fn get(&self, filter: &RuleFilter) -> Result<RuleModel, sqlx::Error> {
        let sql = list_rules_sql(filter);
        let mut query = sqlx::query_as::<_, RuleModel>(sqlx::AssertSqlSafe(sql));
        if let Some(id) = filter.id {
            query = query.bind(id);
        }
        if let Some(evaluator_id) = filter.evaluator_id {
            query = query.bind(evaluator_id);
        }
        query.fetch_one(self.pool.as_ref()).await
    }

    async fn list(&self, filter: &RuleFilter) -> Result<Vec<RuleModel>, sqlx::Error> {
        let sql = list_rules_sql(filter);
        let mut query = sqlx::query_as::<_, RuleModel>(sqlx::AssertSqlSafe(sql));
        if let Some(id) = filter.id {
            query = query.bind(id);
        }
        if let Some(evaluator_id) = filter.evaluator_id {
            query = query.bind(evaluator_id);
        }
        query.fetch_all(self.pool.as_ref()).await
    }
}

#[cfg(test)]
#[path = "get.test.rs"]
mod tests;
