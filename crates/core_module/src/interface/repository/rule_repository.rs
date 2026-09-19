use crate::repository::model::rule::RuleModel;
use async_trait::async_trait;

/// Filter for rule get/list.
///
/// Both fields are optional: when neither carries a value the whole
/// table is matched. For `get`, at least `id` should be set; for `list`,
/// filter by `evaluator_id` to scope to an evaluator.
#[derive(Debug, Clone, Default)]
pub struct RuleFilter {
    pub id: Option<i64>,
    pub evaluator_id: Option<i64>,
}

impl RuleFilter {
    pub fn has_any(&self) -> bool {
        self.id.is_some() || self.evaluator_id.is_some()
    }
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait RuleRepository {
    async fn get(&self, filter: &RuleFilter) -> Result<RuleModel, sqlx::Error>;
    async fn list(&self, filter: &RuleFilter) -> Result<Vec<RuleModel>, sqlx::Error>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(id: i64, evaluator_id: i64) -> RuleModel {
        RuleModel {
            id,
            evaluator_id,
            content: serde_json::json!({"key": "value"}),
            input: serde_json::json!({"in": 1}),
            version: Some(1),
            description: Some("test".into()),
            is_active: true,
            created_at: None,
            created_by: Some(1),
        }
    }

    #[test]
    fn filter_default_has_no_filter() {
        let f = RuleFilter::default();
        assert!(!f.has_any());
    }

    #[test]
    fn filter_with_id_has_any() {
        let f = RuleFilter {
            id: Some(1),
            ..Default::default()
        };
        assert!(f.has_any());
    }

    #[tokio::test]
    async fn mock_get_returns_programmed_model() {
        let mut mock = MockRuleRepository::new();
        mock.expect_get()
            .withf(|filter: &RuleFilter| filter.id == Some(42))
            .return_once(|_| Ok(fixture(42, 7)));
        let got = mock
            .get(&RuleFilter {
                id: Some(42),
                evaluator_id: None,
            })
            .await
            .unwrap();
        assert_eq!(got.id, 42);
        assert_eq!(got.evaluator_id, 7);
    }

    #[tokio::test]
    async fn mock_list_returns_programmed_models() {
        let mut mock = MockRuleRepository::new();
        mock.expect_list()
            .withf(|filter: &RuleFilter| filter.evaluator_id == Some(7))
            .return_once(|_| Ok(vec![fixture(1, 7), fixture(2, 7)]));
        let got = mock
            .list(&RuleFilter {
                id: None,
                evaluator_id: Some(7),
            })
            .await
            .unwrap();
        assert_eq!(got.len(), 2);
    }
}
