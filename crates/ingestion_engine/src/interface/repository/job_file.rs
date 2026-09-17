use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::internal::repository::model::job_file::JobFileModel;

#[derive(Debug, Clone, Default)]
pub struct JobFileFilter {
    pub id: Option<i64>,
    pub job_detail_id: Option<i64>,
    pub filename: Option<String>,
    pub status: Option<String>,
    pub created_at: Option<DateTime<Utc>>,
    pub deleted_at: Option<DateTime<Utc>>,
}

#[async_trait]
pub trait JobFileRepository {
    async fn list(&self, filter: &JobFileFilter) -> Result<Vec<JobFileModel>, sqlx::Error>;
    async fn get(&self, filter: &JobFileFilter) -> Result<JobFileModel, sqlx::Error>;
}
