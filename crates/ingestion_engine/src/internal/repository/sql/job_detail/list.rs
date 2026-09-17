use async_trait::async_trait;
use sqlx::Row;

use super::MySqlJobDetailRepository;

use chrono::NaiveDateTime;

use crate::interface::repository::job_detail::{JobDetailFilter, JobDetailRepository};
use crate::internal::repository::model::job_detail::JobDetailModel;

pub fn list_job_details_sql(filter: &JobDetailFilter) -> String {
    let mut sql = String::from(
        "SELECT id, job_id, entity, `key`, file_start_range, file_end_range FROM job_detail WHERE 1=1",
    );

    if filter.id.is_some() {
        sql.push_str(" AND id = ?");
    }

    if filter.job_id.is_some() {
        sql.push_str(" AND job_id = ?");
    }

    let start = filter.effective_file_start();
    let end = filter.file_end_range;

    if start.is_some() && end.is_some() {
        sql.push_str(" AND file_start_range >= ? AND file_end_range <= ?");
    } else if start.is_some() {
        sql.push_str(" AND file_start_range >= ?");
    } else if end.is_some() {
        sql.push_str(" AND file_end_range <= ?");
    }

    sql
}

#[async_trait]
impl JobDetailRepository for MySqlJobDetailRepository {
    async fn list(&self, filter: &JobDetailFilter) -> Result<Vec<JobDetailModel>, sqlx::Error> {
        let sql = list_job_details_sql(filter);
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.clone()));

        if let Some(id) = filter.id {
            query = query.bind(id);
        }

        if let Some(job_id) = filter.job_id {
            query = query.bind(job_id);
        }

        let start = filter.effective_file_start();
        let end = filter.file_end_range;

        // Use UTC directly for SQL DATETIME (store as UTC)
        if let (Some(s), Some(e)) = (start, end) {
            query = query.bind(s).bind(e);
        } else if let Some(s) = start {
            query = query.bind(s);
        } else if let Some(e) = end {
            query = query.bind(e);
        }

        let rows = query.fetch_all(self.pool.as_ref()).await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            // MySQL DATETIME is NaiveDateTime in DB, interpret as UTC directly
            let file_start_range: Option<NaiveDateTime> = row.try_get("file_start_range")?;
            let file_end_range: Option<NaiveDateTime> = row.try_get("file_end_range")?;
            out.push(JobDetailModel {
                id: row.try_get("id")?,
                job_id: row.try_get("job_id")?,
                entity: row.try_get("entity")?,
                key: row.try_get("key")?,
                file_start_range: file_start_range.map(|n| n.and_utc()),
                file_end_range: file_end_range.map(|n| n.and_utc()),
            });
        }
        Ok(out)
    }
}

#[cfg(test)]
#[path = "list.test.rs"]
mod tests;
