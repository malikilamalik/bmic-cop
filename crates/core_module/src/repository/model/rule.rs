#[cfg(test)]
#[path = "rule.test.rs"]
mod tests;

use crate::interface::repository::common::ModelsCommon;
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::FromRow;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct RuleModel {
    pub id: i64,
    pub evaluator_id: i64,
    pub content: Value,
    pub input: Value,
    pub version: Option<i32>,
    pub description: Option<String>,
    pub is_active: bool,
    pub created_at: Option<NaiveDateTime>,
    pub created_by: Option<i64>,
}

impl ModelsCommon for RuleModel {
    type Model = RuleModel;

    fn table_name(&self) -> &str {
        "rule"
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
            "evaluator_id".to_string(),
            "content".to_string(),
            "input".to_string(),
            "version".to_string(),
            "description".to_string(),
            "is_active".to_string(),
            "created_at".to_string(),
            "created_by".to_string(),
        ]
    }

    fn get_val_struct(&self, arr_column: &[String]) -> Vec<Value> {
        let obj = serde_json::to_value(self).expect("serialize RuleModel");
        arr_column
            .iter()
            .map(|col| obj.get(col).cloned().unwrap_or(Value::Null))
            .collect()
    }
}
