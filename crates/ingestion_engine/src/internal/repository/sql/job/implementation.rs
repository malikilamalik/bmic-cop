use async_trait::async_trait;
use chrono::NaiveDateTime;
use sqlx::Row;

use super::MySqlJobRepository;

use crate::interface::repository::job::{JobFilter, JobRepository};
use crate::internal::repository::model::job::JobModel;

pub fn list_jobs_sql(filter: &JobFilter) -> String {
    let mut sql = String::from(
        "SELECT id, evaluator_id, status, created_at, updated_at FROM job WHERE 1=1",
    );

    if filter.id.is_some() {
        sql.push_str(" AND id = ?");
    }

    if filter.evaluator_id.is_some() {
        sql.push_str(" AND evaluator_id = ?");
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
impl JobRepository for MySqlJobRepository {
    async fn list(&self, filter: &JobFilter) -> Result<Vec<JobModel>, sqlx::Error> {
        let sql = list_jobs_sql(filter);
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.clone()));

        if let Some(id) = filter.id {
            query = query.bind(id);
        }

        if let Some(evaluator_id) = filter.evaluator_id {
            query = query.bind(evaluator_id);
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
            out.push(JobModel {
                id: row.try_get("id")?,
                evaluator_id: row.try_get("evaluator_id")?,
                status: row.try_get("status")?,
                created_at: created_at.map(|n| n.and_utc()),
                updated_at: updated_at.map(|n| n.and_utc()),
            });
        }
        Ok(out)
    }

    async fn get(&self, filter: &JobFilter) -> Result<JobModel, sqlx::Error> {
        let sql = list_jobs_sql(filter);
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.clone()));

        if let Some(id) = filter.id {
            query = query.bind(id);
        }

        if let Some(evaluator_id) = filter.evaluator_id {
            query = query.bind(evaluator_id);
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
        Ok(JobModel {
            id: row.try_get("id")?,
            evaluator_id: row.try_get("evaluator_id")?,
            status: row.try_get("status")?,
            created_at: created_at.map(|n| n.and_utc()),
            updated_at: updated_at.map(|n| n.and_utc()),
        })
    }
}

#[cfg(test)]
#[path = "list.test.rs"]
mod tests;
