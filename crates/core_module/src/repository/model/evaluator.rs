#[cfg(test)]
#[path = "evaluator.test.rs"]
mod tests;

use crate::interface::repository::common::ModelsCommon;
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::FromRow;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct EvaluatorModel {
    pub id: i64,
    pub name: String,
    pub start_valid_date: Option<NaiveDateTime>,
    pub end_valid_date: Option<NaiveDateTime>,
    pub is_active: bool,
    pub running_frequency: Option<String>,
    pub created_at: Option<NaiveDateTime>,
    pub created_by: Option<i64>,
    pub updated_at: Option<NaiveDateTime>,
    pub updated_by: Option<i64>,
    pub deleted_at: Option<NaiveDateTime>,
    pub deleted_by: Option<i64>,
}

impl ModelsCommon for EvaluatorModel {
    type Model = EvaluatorModel;

    fn table_name(&self) -> &str {
        "evaluator"
    }

    fn get_models(&self) -> Vec<Self::Model> {
        vec![self.clone()]
    }

    fn get_models_map(&self) -> std::collections::HashMap<String, Self::Model> {
        let mut map = std::collections::HashMap::new();
        map.insert(self.id.to_string(), self.clone());
        map
    }

    fn get_columns(&self) -> Vec<String> {
        vec![
            "id".to_string(),
            "name".to_string(),
            "start_valid_date".to_string(),
            "end_valid_date".to_string(),
            "is_active".to_string(),
            "running_frequency".to_string(),
            "created_at".to_string(),
            "created_by".to_string(),
            "updated_at".to_string(),
            "updated_by".to_string(),
            "deleted_at".to_string(),
            "deleted_by".to_string(),
        ]
    }

    fn get_val_struct(&self, arr_column: &[String]) -> Vec<Value> {
        let obj = serde_json::to_value(self).expect("serialize EvaluatorModel");
        arr_column
            .iter()
            .map(|col| obj.get(col).cloned().unwrap_or(Value::Null))
            .collect()
    }
}
