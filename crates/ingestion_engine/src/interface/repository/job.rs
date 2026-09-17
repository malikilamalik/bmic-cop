use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::internal::repository::model::job::JobModel;

#[derive(Debug, Clone, Default)]
pub struct JobFilter {
    pub id: Option<i64>,
    pub evaluator_id: Option<i64>,
    pub status: Option<String>,
    pub created_at: Option<DateTime<Utc>>,
    pub deleted_at: Option<DateTime<Utc>>,
}

#[async_trait]
pub trait JobRepository {
    async fn list(&self, filter: &JobFilter) -> Result<Vec<JobModel>, sqlx::Error>;
    async fn get(&self, filter: &JobFilter) -> Result<JobModel, sqlx::Error>;
}
