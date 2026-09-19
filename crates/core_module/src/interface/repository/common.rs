
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

pub trait ModelsCommon {
    type Model: Clone + Serialize + for<'de> Deserialize<'de>;

    fn table_name(&self) -> &str;
    fn get_models(&self) -> Vec<Self::Model>;
    fn get_models_map(&self) -> HashMap<String, Self::Model>;
    fn get_columns(&self) -> Vec<String>;
    fn get_val_struct(&self, arr_column: &[String]) -> Vec<Value>;
}
