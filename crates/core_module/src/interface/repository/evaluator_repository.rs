use crate::repository::model::evaluator::EvaluatorModel;
use async_trait::async_trait;

/// Filter for listing evaluators.
///
/// Both fields are optional: when neither carries a value the whole
/// (non-deleted, currently-valid) table is returned. The `evaluator`
/// table has no `evaluator_id` column — its primary key `id` is what
/// other tables reference as `evaluator_id` — so `evaluator_id` is
/// accepted here as an alias for `id`, with `id` winning when both
/// are set.
#[derive(Debug, Clone, Default)]
pub struct EvaluatorFilter {
    pub id: Option<i64>,
    pub evaluator_id: Option<i64>,
}

impl EvaluatorFilter {
    /// The single `id` to filter by, or `None` for list-all.
    pub fn effective_id(&self) -> Option<i64> {
        self.id.or(self.evaluator_id)
    }
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait EvaluatorRepository {
    async fn list(&self, filter: &EvaluatorFilter) -> Result<Vec<EvaluatorModel>, sqlx::Error>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(id: i64) -> EvaluatorModel {
        EvaluatorModel {
            id,
            name: "eval".to_string(),
            start_valid_date: None,
            end_valid_date: None,
            is_active: true,
            running_frequency: None,
            created_at: None,
            created_by: None,
            updated_at: None,
            updated_by: None,
            deleted_at: None,
            deleted_by: None,
        }
    }

    #[test]
    fn effective_id_prefers_id_over_evaluator_id() {
        let filter = EvaluatorFilter {
            id: Some(1),
            evaluator_id: Some(2),
        };
        assert_eq!(filter.effective_id(), Some(1));
    }

    #[test]
    fn effective_id_falls_back_to_evaluator_id() {
        let filter = EvaluatorFilter {
            id: None,
            evaluator_id: Some(7),
        };
        assert_eq!(filter.effective_id(), Some(7));
    }

    #[test]
    fn effective_id_is_none_when_both_are_missing() {
        assert_eq!(EvaluatorFilter::default().effective_id(), None);
    }

    #[tokio::test]
    async fn mock_list_returns_programmed_models() {
        let mut mock = MockEvaluatorRepository::new();
        mock.expect_list()
            .withf(|filter: &EvaluatorFilter| filter.effective_id() == Some(3))
            .return_once(|_| Ok(vec![fixture(3)]));
        let got = mock
            .list(&EvaluatorFilter {
                id: Some(3),
                evaluator_id: None,
            })
            .await
            .unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].id, 3);
    }
}
