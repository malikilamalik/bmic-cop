#[cfg(test)]
#[path = "benefit.test.rs"]
mod tests;

use crate::interface::repository::common::ModelsCommon;
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::FromRow;


#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct BenefitModel {
    pub id: i64,
    pub evaluator_id: i64,
    pub customer_id: i64,
    pub value: Option<Value>,
    pub description: Option<String>,
    pub expired_at: Option<NaiveDateTime>,
    pub created_at: Option<NaiveDateTime>,
}

impl ModelsCommon for BenefitModel {
    type Model = BenefitModel;

    fn table_name(&self) -> &str {
        "benefit"
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
            "customer_id".to_string(),
            "value".to_string(),
            "description".to_string(),
            "expired_at".to_string(),
            "created_at".to_string(),
        ]
    }

    fn get_val_struct(&self, arr_column: &[String]) -> Vec<Value> {
        let obj = serde_json::to_value(self).expect("serialize BenefitModel");
        arr_column
            .iter()
            .map(|col| obj.get(col).cloned().unwrap_or(Value::Null))
            .collect()
    }
}