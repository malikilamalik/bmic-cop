use core_module::interface::repository::{
    evaluator_repository::{EvaluatorFilter, EvaluatorRepository},
    rule_repository::{RuleFilter, RuleRepository},
};
use core_module::repository::sql::{
    evaluator::MySqlEvaluatorRepository, rule::MySqlRuleRepository,
};
use pkg::config::ingestion::IngestionConfig;
use pkg::config::mysql::MysqlConfig;
use pkg::mysql;
use pkg::proto::client::{
    client_client::ClientClient, IngestRequest, Job, JobInput, TimeRange,
};
use std::time::Duration;
use tokio_cron_scheduler::{Job as CronJob, JobScheduler, JobSchedulerError};

fn ingestion_endpoint() -> String {
    format!("http://{}", IngestionConfig::from_env().addr())
}

fn build_time_range_between(start: &str, end: &str) -> TimeRange {
    TimeRange {
        r#type: "between".into(),
        value: "".into(),
        start: start.into(),
        end: end.into(),
    }
}
fn build_time_range_named(value: &str) -> TimeRange {
    TimeRange {
        r#type: "named".into(),
        value: value.into(),
        start: "".into(),
        end: "".into(),
    }
}

fn build_demo_request() -> IngestRequest {
    let jobs = vec![
        Job {
            evaluator_id: 1,
            input: vec![JobInput {
                key: "data_transaction_widow".into(),
                entity: "transaction".into(),
                time_range: Some(build_time_range_between(
                    "2026-09-01T00:00:00",
                    "2026-09-07T23:59:59",
                )),
            }],
        },
        Job {
            evaluator_id: 12,
            input: vec![JobInput {
                key: "data_transaction_today".into(),
                entity: "transaction".into(),
                time_range: Some(build_time_range_named("today")),
            }],
        },
        Job {
            evaluator_id: 13,
            input: vec![JobInput {
                key: "data_customer_widow".into(),
                entity: "customer".into(),
                time_range: Some(build_time_range_between(
                    "2026-09-01T00:00:00",
                    "2026-09-07T23:59:59",
                )),
            }],
        },
        Job {
            evaluator_id: 14,
            input: vec![JobInput {
                key: "data_customer_today".into(),
                entity: "customer".into(),
                time_range: Some(build_time_range_named("today")),
            }],
        },
    ];
    IngestRequest { job: jobs, jobs: vec![] }
}

/// Convert a rule's `input` JSON into Vec<JobInput>
/// Supports:
/// - {"key":"...","entity":"...","time_range":{"type":"...","value":"...","start":"...","end":"..."}}
/// - [{"key":...},...] array of above
/// - {"inputs":[...]} wrapped
fn rule_input_to_job_inputs(input: &serde_json::Value) -> Vec<JobInput> {
    let mut out = Vec::new();
    let values: Vec<&serde_json::Value> = if let Some(arr) = input.as_array() {
        arr.iter().collect()
    } else if let Some(arr) = input.get("inputs").and_then(|v| v.as_array()) {
        arr.iter().collect()
    } else if input.get("key").is_some() || input.get("entity").is_some() {
        vec![input]
    } else {
        // unknown shape – return empty and caller will fallback
        vec![]
    };
    for v in values {
        let key = v
            .get("key")
            .and_then(|x| x.as_str())
            .unwrap_or("default_key")
            .to_string();
        let entity = v
            .get("entity")
            .and_then(|x| x.as_str())
            .unwrap_or("transaction")
            .to_string();
        let tr = v.get("time_range").or_else(|| v.get("timeRange"));
        let time_range = if let Some(trv) = tr {
            let ttype = trv
                .get("type")
                .and_then(|x| x.as_str())
                .unwrap_or("named");
            if ttype == "between" {
                Some(build_time_range_between(
                    trv.get("start").and_then(|x| x.as_str()).unwrap_or(""),
                    trv.get("end").and_then(|x| x.as_str()).unwrap_or(""),
                ))
            } else {
                Some(build_time_range_named(
                    trv.get("value").and_then(|x| x.as_str()).unwrap_or("today"),
                ))
            }
        } else {
            // fallback to today
            Some(build_time_range_named("today"))
        };
        out.push(JobInput {
            key,
            entity,
            time_range,
        });
    }
    out
}

/// Build IngestRequest from active evaluators + their active rules
/// This is the DB-driven path: each evaluator becomes a Job, each rule's input becomes a JobInput
fn build_request_from_evaluators(
    eval_rules: Vec<(core_module::repository::model::evaluator::EvaluatorModel, Vec<core_module::repository::model::rule::RuleModel>)>,
) -> IngestRequest {
    let mut jobs = Vec::new();
    for (eval, rules) in eval_rules {
        let mut inputs = Vec::new();
        for rule in rules {
            let mut jis = rule_input_to_job_inputs(&rule.input);
            if jis.is_empty() {
                // Fallback: use demo transaction today if rule input is empty/invalid
                jis.push(JobInput {
                    key: format!("rule_{}_fallback", rule.id),
                    entity: "transaction".into(),
                    time_range: Some(build_time_range_named("today")),
                });
            }
            inputs.extend(jis);
        }
        if inputs.is_empty() {
            // evaluator with no active rules -> still send one default input so job is not empty
            inputs.push(JobInput {
                key: "default".into(),
                entity: "transaction".into(),
                time_range: Some(build_time_range_named("today")),
            });
        }
        jobs.push(Job {
            evaluator_id: eval.id as u64,
            input: inputs,
        });
    }
    IngestRequest { job: jobs, jobs: vec![] }
}

async fn fetch_active_evaluators_and_rules() -> anyhow::Result<IngestRequest> {
    let cfg = MysqlConfig::from_env();
    let pool = mysql::init(&cfg).database("business_rule").await?;
    let eval_repo = MySqlEvaluatorRepository::new(pool.clone());
    let rule_repo = MySqlRuleRepository::new(pool.clone());

    // Active evaluators: is_active = true, deleted_at IS NULL, within start/end_valid_date (SQL handles dates, we add is_active filter via SQL)
    // The repository's SQL already filters is_active = TRUE, deleted_at IS NULL, and date range
    let evaluators = eval_repo.list(&EvaluatorFilter::default()).await?;
    println!(
        "[scheduler] fetched {} active evaluators (is_active true, within dates, not deleted)",
        evaluators.len()
    );
    let mut eval_rules = Vec::new();
    for eval in evaluators {
        let rules = rule_repo
            .list(&RuleFilter {
                evaluator_id: Some(eval.id),
                ..Default::default()
            })
            .await?;
        println!(
            "[scheduler] evaluator {} ({}) has {} active rules",
            eval.id,
            eval.name,
            rules.len()
        );
        eval_rules.push((eval, rules));
    }
    if eval_rules.is_empty() {
        anyhow::bail!("no active evaluators found, will fallback to demo");
    }
    Ok(build_request_from_evaluators(eval_rules))
}

async fn call_submit_jobs() {
    let endpoint = ingestion_endpoint();
    // Try DB-driven request, fallback to demo on error/empty
    let req = match fetch_active_evaluators_and_rules().await {
        Ok(r) if !r.job.is_empty() => {
            println!(
                "[scheduler] using DB-driven request with {} jobs from business_rule",
                r.job.len()
            );
            r
        }
        Ok(_) => {
            println!("[scheduler] DB returned empty, using demo request");
            build_demo_request()
        }
        Err(e) => {
            eprintln!(
                "[scheduler] DB fetch failed ({}), using demo request",
                e
            );
            build_demo_request()
        }
    };
    println!(
        "[scheduler] calling Client/SubmitJobs at {} with {} jobs",
        endpoint,
        req.job.len()
    );
    for (i, j) in req.job.iter().enumerate() {
        println!(
            "[scheduler]  job[{}] evaluator_id={} inputs={}",
            i,
            j.evaluator_id,
            j.input.len()
        );
        for (k, inp) in j.input.iter().enumerate() {
            println!(
                "[scheduler]    input[{}] key={} entity={} time_range={:?}",
                k, inp.key, inp.entity, inp.time_range
            );
        }
    }
    match ClientClient::connect(endpoint.clone()).await {
        Ok(mut client) => match client.submit_jobs(req).await {
            Ok(resp) => {
                let inner = resp.into_inner();
                println!(
                    "[scheduler] SubmitJobs success job_ids={:?} message={}",
                    inner.job_ids, inner.message
                );
            }
            Err(e) => eprintln!("[scheduler] SubmitJobs RPC failed: {}", e),
        },
        Err(e) => eprintln!("[scheduler] connect failed to {}: {}", endpoint, e),
    }
}

#[tokio::main]
async fn main() -> Result<(), JobSchedulerError> {
    // Load .env for INGESTION_ADDR and MySQL
    let _ = dotenvy::dotenv();
    let sched = JobScheduler::new().await?;

    // Scheduler-driven client: every 10 seconds call Client/SubmitJobs
    sched
        .add(CronJob::new_async("1/10 * * * * *", |_uuid, _l| {
            Box::pin(async move {
                println!("I run every 10 seconds – scheduler -> Client/SubmitJobs");
                call_submit_jobs().await;
            })
        })?)
        .await?;

    sched.start().await?;
    println!(
        "[scheduler] started, will call {} every 10s (DB-driven, fallback demo)",
        ingestion_endpoint()
    );
    tokio::time::sleep(Duration::from_secs(100)).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn scheduler_builds_submitjobs_request_with_4_cases() {
        let req = build_demo_request();
        assert_eq!(req.job.len(), 4);
        assert_eq!(req.job[0].evaluator_id, 1);
        assert_eq!(req.job[0].input[0].entity, "transaction");
        assert_eq!(
            req.job[0].input[0]
                .time_range
                .as_ref()
                .unwrap()
                .r#type,
            "between"
        );
        assert_eq!(
            req.job[1].input[0].time_range.as_ref().unwrap().r#type,
            "named"
        );
        assert_eq!(
            req.job[1].input[0].time_range.as_ref().unwrap().value,
            "today"
        );
        assert_eq!(req.job[2].input[0].entity, "customer");
        assert_eq!(
            req.job[2].input[0].time_range.as_ref().unwrap().r#type,
            "between"
        );
        assert_eq!(req.job[3].input[0].entity, "customer");
        assert_eq!(
            req.job[3].input[0].time_range.as_ref().unwrap().value,
            "today"
        );
        let tr_between = req.job[0].input[0].time_range.as_ref().unwrap();
        assert_eq!(tr_between.start, "2026-09-01T00:00:00");
        assert_eq!(tr_between.end, "2026-09-07T23:59:59");
    }

    #[test]
    fn rule_input_to_job_input_between() {
        let v = json!({"key":"k1","entity":"transaction","time_range":{"type":"between","start":"2026-09-01T00:00:00","end":"2026-09-07T23:59:59"}});
        let jis = rule_input_to_job_inputs(&v);
        assert_eq!(jis.len(), 1);
        assert_eq!(jis[0].key, "k1");
        assert_eq!(jis[0].entity, "transaction");
        assert_eq!(jis[0].time_range.as_ref().unwrap().r#type, "between");
    }

    #[test]
    fn rule_input_to_job_input_named() {
        let v = json!({"key":"k2","entity":"customer","time_range":{"type":"named","value":"today"}});
        let jis = rule_input_to_job_inputs(&v);
        assert_eq!(jis.len(), 1);
        assert_eq!(jis[0].entity, "customer");
        assert_eq!(jis[0].time_range.as_ref().unwrap().value, "today");
    }

    #[test]
    fn rule_input_array_wrapped() {
        let v = json!([{"key":"a","entity":"transaction","time_range":{"type":"named","value":"today"}},{"key":"b","entity":"customer","time_range":{"type":"between","start":"2026-09-01T00:00:00","end":"2026-09-02T00:00:00"}}]);
        let jis = rule_input_to_job_inputs(&v);
        assert_eq!(jis.len(), 2);
    }

    #[test]
    fn build_request_from_evaluators_maps_rules() {
        use chrono::NaiveDateTime;
        let eval = core_module::repository::model::evaluator::EvaluatorModel {
            id: 42,
            name: "test_eval".into(),
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
        };
        let rule = core_module::repository::model::rule::RuleModel {
            id: 1,
            evaluator_id: 42,
            content: json!({}),
            input: json!({"key":"k1","entity":"transaction","time_range":{"type":"named","value":"today"}}),
            version: Some(1),
            description: None,
            is_active: true,
            created_at: None,
            created_by: None,
        };
        let req = build_request_from_evaluators(vec![(eval, vec![rule])]);
        assert_eq!(req.job.len(), 1);
        assert_eq!(req.job[0].evaluator_id, 42);
        assert_eq!(req.job[0].input.len(), 1);
        assert_eq!(req.job[0].input[0].key, "k1");
    }

    #[test]
    fn ingestion_endpoint_uses_ingestion_config() {
        let ep = ingestion_endpoint();
        assert!(ep.starts_with("http://"));
        assert!(ep.contains(":"), "endpoint should be http://host:port");
    }
}
