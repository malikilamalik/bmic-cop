use async_trait::async_trait;
use chrono::Utc;

use super::MySqlEvaluatorRepository;
use crate::interface::repository::evaluator_repository::{EvaluatorFilter, EvaluatorRepository};
use crate::repository::model::evaluator::EvaluatorModel;

use super::list::list_evaluators_sql;

#[async_trait]
impl EvaluatorRepository for MySqlEvaluatorRepository {
    async fn list(&self, filter: &EvaluatorFilter) -> Result<Vec<EvaluatorModel>, sqlx::Error> {
        let now = Utc::now().naive_utc();
        let sql = list_evaluators_sql(filter.effective_id().is_some());
        if let Some(id) = filter.effective_id() {
            sqlx::query_as::<_, EvaluatorModel>(sql)
                .bind(id)
                .bind(now)
                .bind(now)
                .fetch_all(self.pool.as_ref())
                .await
        } else {
            sqlx::query_as::<_, EvaluatorModel>(sql)
                .bind(now)
                .bind(now)
                .fetch_all(self.pool.as_ref())
                .await
        }
    }
}


