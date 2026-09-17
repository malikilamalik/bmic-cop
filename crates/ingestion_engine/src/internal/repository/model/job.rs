#[cfg(test)]
#[path = "job.test.rs"]
mod tests;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::FromRow;

use crate::interface::repository::common::ModelsCommon;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct JobModel {
    pub id: u64,
    pub evaluator_id: u64,
    pub status: String,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

// Keep alias for backward compatibility
pub type Job = JobModel;

impl ModelsCommon for JobModel {
    type Model = JobModel;

    fn table_name(&self) -> &str {
        "job"
    }

    fn get_models_map(&self) -> std::collections::HashMap<String, Self::Model> {
        let mut map = std::collections::HashMap::new();
        map.insert(self.id.to_string(), self.clone());
        map
    }

    fn get_columns(&self) -> Vec<String> {
        vec![
            "id".to_string(),
            "evaluator_id".to_string(),
            "status".to_string(),
            "created_at".to_string(),
            "updated_at".to_string(),
        ]
    }

    fn get_val_struct(&self, arr_column: &[String]) -> Vec<Value> {
        let obj = serde_json::to_value(self).expect("serialize JobModel");
        arr_column
            .iter()
            .map(|col| obj.get(col).cloned().unwrap_or(Value::Null))
            .collect()
    }
}
