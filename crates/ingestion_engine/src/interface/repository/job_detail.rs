use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::internal::repository::model::job_detail::JobDetailModel;

#[derive(Debug, Clone, Default)]
pub struct JobDetailFilter {
    pub id: Option<i64>,
    pub job_id: Option<i64>,
    pub file_start_range: Option<DateTime<Utc>>,
    pub file_end_range: Option<DateTime<Utc>>,
}

impl JobDetailFilter {
    pub fn effective_file_start(&self) -> Option<DateTime<Utc>> {
        self.file_start_range.or(self.file_start_range)
    }
}

#[async_trait]
pub trait JobDetailRepository {
    async fn list(&self, filter: &JobDetailFilter) -> Result<Vec<JobDetailModel>, sqlx::Error>;
}
