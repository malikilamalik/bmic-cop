#[cfg(test)]
#[path = "job_detail.test.rs"]
mod tests;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::FromRow;

use crate::interface::repository::common::ModelsCommon;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct JobDetailModel {
    pub id: u64,
    pub job_id: u64,
    pub entity: String,
    pub key: String,
    pub file_start_range: Option<DateTime<Utc>>,
    pub file_end_range: Option<DateTime<Utc>>,
}

pub type JobDetail = JobDetailModel;

impl ModelsCommon for JobDetailModel {
    type Model = JobDetailModel;

    fn table_name(&self) -> &str {
        "job_detail"
    }

    fn get_models_map(&self) -> std::collections::HashMap<String, Self::Model> {
        let mut map = std::collections::HashMap::new();
        map.insert(self.id.to_string(), self.clone());
        map
    }

    fn get_columns(&self) -> Vec<String> {
        vec![
            "id".to_string(),
            "job_id".to_string(),
            "entity".to_string(),
            "key".to_string(),
            "file_start_range".to_string(),
            "file_end_range".to_string(),
        ]
    }

    fn get_val_struct(&self, arr_column: &[String]) -> Vec<Value> {
        let obj = serde_json::to_value(self).expect("serialize JobDetailModel");
        arr_column
            .iter()
            .map(|col| obj.get(col).cloned().unwrap_or(Value::Null))
            .collect()
    }
}
