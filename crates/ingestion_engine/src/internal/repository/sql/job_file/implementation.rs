use async_trait::async_trait;
use chrono::NaiveDateTime;
use sqlx::Row;

use super::MySqlJobFileRepository;

use crate::interface::repository::job_file::{JobFileFilter, JobFileRepository};
use crate::internal::repository::model::job_file::JobFileModel;

pub fn list_job_files_sql(filter: &JobFileFilter) -> String {
    let mut sql = String::from(
        "SELECT id, job_detail_id, filename, status, created_at, updated_at FROM job_file WHERE 1=1",
    );

    if filter.id.is_some() {
        sql.push_str(" AND id = ?");
    }

    if filter.job_detail_id.is_some() {
        sql.push_str(" AND job_detail_id = ?");
    }

    if filter.filename.is_some() {
        sql.push_str(" AND filename = ?");
    }

    if filter.status.is_some() {
        sql.push_str(" AND status = ?");
    }

    if filter.created_at.is_some() {
        sql.push_str(" AND created_at = ?");
    }

    if filter.deleted_at.is_some() {
        sql.push_str(" AND deleted_at = ?");
    }

    sql
}

#[async_trait]
impl JobFileRepository for MySqlJobFileRepository {
    async fn list(&self, filter: &JobFileFilter) -> Result<Vec<JobFileModel>, sqlx::Error> {
        let sql = list_job_files_sql(filter);
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.clone()));

        if let Some(id) = filter.id {
            query = query.bind(id);
        }

        if let Some(job_detail_id) = filter.job_detail_id {
            query = query.bind(job_detail_id);
        }

        if let Some(filename) = &filter.filename {
            query = query.bind(filename);
        }

        if let Some(status) = &filter.status {
            query = query.bind(status);
        }

        if let Some(created_at) = filter.created_at {
            query = query.bind(created_at);
        }

        if let Some(deleted_at) = filter.deleted_at {
            query = query.bind(deleted_at);
        }

        let rows = query.fetch_all(self.pool.as_ref()).await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let created_at: Option<NaiveDateTime> = row.try_get("created_at")?;
            let updated_at: Option<NaiveDateTime> = row.try_get("updated_at")?;
            out.push(JobFileModel {
                id: row.try_get("id")?,
                job_detail_id: row.try_get("job_detail_id")?,
                filename: row.try_get("filename")?,
                status: row.try_get("status")?,
                created_at: created_at.map(|n| n.and_utc()),
                updated_at: updated_at.map(|n| n.and_utc()),
            });
        }
        Ok(out)
    }

    async fn get(&self, filter: &JobFileFilter) -> Result<JobFileModel, sqlx::Error> {
        let sql = list_job_files_sql(filter);
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.clone()));

        if let Some(id) = filter.id {
            query = query.bind(id);
        }

        if let Some(job_detail_id) = filter.job_detail_id {
            query = query.bind(job_detail_id);
        }

        if let Some(filename) = &filter.filename {
            query = query.bind(filename);
        }

        if let Some(status) = &filter.status {
            query = query.bind(status);
        }

        if let Some(created_at) = filter.created_at {
            query = query.bind(created_at);
        }

        if let Some(deleted_at) = filter.deleted_at {
            query = query.bind(deleted_at);
        }

        let row = query.fetch_one(self.pool.as_ref()).await?;
        let created_at: Option<NaiveDateTime> = row.try_get("created_at")?;
        let updated_at: Option<NaiveDateTime> = row.try_get("updated_at")?;
        Ok(JobFileModel {
            id: row.try_get("id")?,
            job_detail_id: row.try_get("job_detail_id")?,
            filename: row.try_get("filename")?,
            status: row.try_get("status")?,
            created_at: created_at.map(|n| n.and_utc()),
            updated_at: updated_at.map(|n| n.and_utc()),
        })
    }
}

#[cfg(test)]
#[path = "list.test.rs"]
mod tests;
