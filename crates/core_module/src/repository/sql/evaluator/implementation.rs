use async_trait::async_trait;
use chrono::{NaiveDateTime, Utc};
use sqlx::Row;

use super::MySqlEvaluatorRepository;
use crate::interface::repository::evaluator_repository::{EvaluatorFilter, EvaluatorRepository};
use crate::repository::model::evaluator::EvaluatorModel;

use super::list::list_evaluators_sql;

fn parse_naive_opt(s: Option<String>) -> Option<NaiveDateTime> {
    s.and_then(|v| {
        // MySQL TIMESTAMP/DATETIME comes as "2020-01-01 00:00:00" or with fractional seconds
        NaiveDateTime::parse_from_str(&v, "%Y-%m-%d %H:%M:%S").ok().or_else(|| {
            NaiveDateTime::parse_from_str(&v, "%Y-%m-%d %H:%M:%S%.f").ok()
        })
    })
}

#[async_trait]
impl EvaluatorRepository for MySqlEvaluatorRepository {
    async fn list(&self, filter: &EvaluatorFilter) -> Result<Vec<EvaluatorModel>, sqlx::Error> {
        let now = Utc::now().naive_utc();
        let now_str = now.format("%Y-%m-%d %H:%M:%S").to_string();
        let sql = list_evaluators_sql(filter.effective_id().is_some());
        // Use sqlx::query and manual decoding to handle TIMESTAMP vs DATETIME mismatch
        let rows = if let Some(id) = filter.effective_id() {
            sqlx::query(sql)
                .bind(id)
                .bind(&now_str)
                .bind(&now_str)
                .fetch_all(self.pool.as_ref())
                .await?
        } else {
            sqlx::query(sql)
                .bind(&now_str)
                .bind(&now_str)
                .fetch_all(self.pool.as_ref())
                .await?
        };
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            // Try to get as String first for timestamp columns, fallback to NaiveDateTime
            let start_valid_date: Option<NaiveDateTime> = {
                let s: Option<String> = row.try_get("start_valid_date").unwrap_or(None);
                if s.is_some() {
                    parse_naive_opt(s)
                } else {
                    // Try direct NaiveDateTime (in case driver returns it)
                    row.try_get::<Option<NaiveDateTime>, _>("start_valid_date")
                        .unwrap_or(None)
                }
            };
            let end_valid_date: Option<NaiveDateTime> = {
                let s: Option<String> = row.try_get("end_valid_date").unwrap_or(None);
                if s.is_some() {
                    parse_naive_opt(s)
                } else {
                    row.try_get::<Option<NaiveDateTime>, _>("end_valid_date")
                        .unwrap_or(None)
                }
            };
            let created_at: Option<NaiveDateTime> = {
                let s: Option<String> = row.try_get("created_at").unwrap_or(None);
                if s.is_some() {
                    parse_naive_opt(s)
                } else {
                    row.try_get::<Option<NaiveDateTime>, _>("created_at")
                        .unwrap_or(None)
                }
            };
            let updated_at: Option<NaiveDateTime> = {
                let s: Option<String> = row.try_get("updated_at").unwrap_or(None);
                if s.is_some() {
                    parse_naive_opt(s)
                } else {
                    row.try_get::<Option<NaiveDateTime>, _>("updated_at")
                        .unwrap_or(None)
                }
            };
            let deleted_at: Option<NaiveDateTime> = {
                let s: Option<String> = row.try_get("deleted_at").unwrap_or(None);
                if s.is_some() {
                    parse_naive_opt(s)
                } else {
                    row.try_get::<Option<NaiveDateTime>, _>("deleted_at")
                        .unwrap_or(None)
                }
            };
            out.push(EvaluatorModel {
                id: row.try_get("id")?,
                name: row.try_get("name")?,
                start_valid_date,
                end_valid_date,
                is_active: row.try_get("is_active")?,
                running_frequency: row.try_get("running_frequency")?,
                created_at,
                created_by: row.try_get("created_by")?,
                updated_at,
                updated_by: row.try_get("updated_by")?,
                deleted_at,
                deleted_by: row.try_get("deleted_by")?,
            });
        }
        Ok(out)
    }
}
