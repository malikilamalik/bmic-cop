#[cfg(test)]
#[path = "job_file.test.rs"]
mod tests;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::FromRow;

use crate::interface::repository::common::ModelsCommon;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct JobFileModel {
    pub id: u64,
    pub job_detail_id: u64,
    pub filename: String,
    pub status: String,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

pub type JobFile = JobFileModel;

impl ModelsCommon for JobFileModel {
    type Model = JobFileModel;

    fn table_name(&self) -> &str {
        "job_file"
    }

    fn get_models_map(&self) -> std::collections::HashMap<String, Self::Model> {
        let mut map = std::collections::HashMap::new();
        map.insert(self.id.to_string(), self.clone());
        map
    }

    fn get_columns(&self) -> Vec<String> {
        vec![
            "id".to_string(),
            "job_detail_id".to_string(),
            "filename".to_string(),
            "status".to_string(),
            "created_at".to_string(),
            "updated_at".to_string(),
        ]
    }

    fn get_val_struct(&self, arr_column: &[String]) -> Vec<Value> {
        let obj = serde_json::to_value(self).expect("serialize JobFileModel");
        arr_column
            .iter()
            .map(|col| obj.get(col).cloned().unwrap_or(Value::Null))
            .collect()
    }
}
