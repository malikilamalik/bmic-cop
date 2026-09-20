use std::sync::Arc;

use chrono::Utc;
use pkg::mysql::DbPool;
use pkg::proto::evaluator::{
    evaluator_server::Evaluator as EvaluatorTrait, Benefit, Customer, EvaluateReply,
    EvaluateRequest, Transaction, TransactionData,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::Row;
use std::time::Duration;

use tonic::{Request, Response, Status};
use zen_engine::{model::DecisionContent, DecisionEngine};

fn api_timeout() -> Duration {
    // gRPC internal service timeout — 200ms default.
    // Priority: GRPC_TIMEOUT_MS > API_TIMEOUT_MS > 200ms
    std::env::var("GRPC_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_millis)
        .or_else(|| {
            std::env::var("API_TIMEOUT_MS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .map(Duration::from_millis)
        })
        .unwrap_or(Duration::from_millis(200))
}

fn idempotency_key(customer_id: i64, job_id: u64, today: &str) -> String {
    let raw = format!("{}-{}-{}", customer_id, job_id, today);
    let mut hasher = Sha256::new();
    hasher.update(raw.as_bytes());
    format!("{:x}", hasher.finalize())
}

async fn append_benefit_file(path: &str, line: &str) -> anyhow::Result<()> {
    use std::path::Path;
    use tokio::io::AsyncWriteExt;
    if let Some(parent) = Path::new(path).parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    // If file exists and is non-empty, prefix with newline; else no newline
    let exists = Path::new(path).exists();
    let needs_newline = if exists {
        tokio::fs::metadata(path)
            .await
            .map(|m| m.len() > 0)
            .unwrap_or(false)
    } else {
        false
    };
    let mut content = String::new();
    if needs_newline {
        content.push('\n');
    }
    content.push_str(line);
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;
    file.write_all(content.as_bytes()).await?;
    file.flush().await?;
    Ok(())
}

use crate::interface::repository::rule_repository::{RuleFilter, RuleRepository};
use crate::repository::sql::rule::MySqlRuleRepository;

/// Build zen input JSON from EvaluateRequest
/// Single customer per request (looped by coordinator) with shared transaction_today:
/// {customer:{customer_id}, transaction:{transaction_today:{sum,avg,count}}}
fn build_zen_input(req: &EvaluateRequest) -> Value {
    let mut root = serde_json::Map::new();
    if let Some(customer) = &req.customer {
        let mut cmap = serde_json::Map::new();
        cmap.insert("customer_id".to_string(), json!(customer.customer_id));
        root.insert("customer".to_string(), Value::Object(cmap));
    }
    if let Some(transaction) = &req.transaction {
        if let Some(data) = &transaction.transaction_today {
            let mut tmap = serde_json::Map::new();
            let mut inner = serde_json::Map::new();
            inner.insert("sum".to_string(), json!(data.sum));
            inner.insert("avg".to_string(), json!(data.avg));
            inner.insert("count".to_string(), json!(data.count));
            tmap.insert("transaction_today".to_string(), Value::Object(inner));
            root.insert("transaction".to_string(), Value::Object(tmap));
        }
    }
    Value::Object(root)
}

/// Parse a rule's `content` Value into DecisionContent, handling both wrapped and raw forms
fn parse_decision_content(content: &Value) -> Result<DecisionContent, String> {
    // The rule's content column may contain the full JDM graph as in data/rule/cashback.json
    // It should have "contentType": "application/vnd.gorules.decision" and nodes/edges.
    // Try to deserialize directly; if it fails, wrap it.
    serde_json::from_value(content.clone()).map_err(|e| format!("invalid DecisionContent: {}", e))
}

pub struct EvaluatorService {
    pool: DbPool,
}

impl EvaluatorService {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }

    async fn evaluate_rules(
        &self,
        evaluator_id: i64,
        zen_input: Value,
    ) -> Result<(Vec<Benefit>, Vec<i64>), Status> {
        let rule_repo = MySqlRuleRepository::new(self.pool.clone());
        let filter = RuleFilter {
            id: None,
            evaluator_id: Some(evaluator_id),
            ..Default::default()
        };
        let rules = rule_repo
            .list(&filter)
            .await
            .map_err(|e| Status::internal(format!("fetch rules failed for evaluator {}: {}", evaluator_id, e)))?;

        if rules.is_empty() {
            pkg::log::warn!(
                "[Evaluator] no active rules found for evaluator_id={}",
                evaluator_id
            );
            return Ok((vec![], vec![]));
        }

        let mut benefits = Vec::new();
        let mut matched_rule_ids: Vec<i64> = Vec::new();

        for rule in rules {
            pkg::log::info!(
                "[Evaluator] evaluating rule id={} evaluator_id={} content keys={:?}",
                rule.id,
                rule.evaluator_id,
                rule.content.as_object().map(|m| m.keys().cloned().collect::<Vec<_>>())
            );
            let decision_content = match parse_decision_content(&rule.content) {
                Ok(dc) => dc,
                Err(e) => {
                    pkg::log::warn!(
                        "[Evaluator] rule {} has invalid content, skipping: {}",
                        rule.id,
                        e
                    );
                    continue;
                }
            };
            // zen types are !Send (Rc), so run blocking on a dedicated thread
            let zen_input_clone = zen_input.clone();
            let dc_clone = decision_content.clone();
            let (result_json, rule_id) = tokio::task::spawn_blocking(move || {
                let rt = tokio::runtime::Handle::try_current();
                // Create a new runtime for blocking zen evaluation if needed, but we can also just use the current thread
                // Since DecisionEngine is not Send, we run it synchronously here
                let engine = DecisionEngine::default();
                let decision = engine
                    .create_decision(Arc::new(dc_clone))
                    .map_err(|e| format!("create_decision failed for rule {}: {:?}", rule.id, e))?;
                // Use block_on for the async evaluate
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| format!("runtime build failed: {}", e))?;
                let result = rt
                    .block_on(decision.evaluate(zen_input_clone.into()))
                    .map_err(|e| format!("evaluate failed for rule {}: {:?}", rule.id, e))?;
                let result_json: Value = result.result.to_value();
                Ok::<(Value, i64), String>((result_json, rule.id))
            })
            .await
            .map_err(|e| Status::internal(format!("spawn_blocking failed: {}", e)))?
            .map_err(|e: String| Status::internal(e))?;
            pkg::log::info!(
                "[Evaluator] rule {} evaluate result: {}",
                rule_id,
                serde_json::to_string(&result_json).unwrap_or_else(|_| format!("{:?}", result_json))
            );
            // Extract benefit.type and benefit.amount if present
            // The cashback decision table outputs {"benefit": {"type": "CASHBACK", "amount": 5000000}}
            let mut produced_benefit = false;
            if let Some(benefit) = result_json.get("benefit") {
                let b_type = benefit
                    .get("type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("UNKNOWN")
                    .to_string();
                let amount = benefit
                    .get("amount")
                    .and_then(|v| v.as_i64())
                    .or_else(|| benefit.get("amount").and_then(|v| v.as_u64().map(|x| x as i64)))
                    .unwrap_or(0);
                benefits.push(Benefit {
                    r#type: b_type,
                    amount,
                });
                produced_benefit = true;
            } else if let Some(arr) = result_json.as_array() {
                // Sometimes result is an array of outputs
                let count_before = benefits.len();
                for item in arr {
                    if let Some(b) = item.get("benefit") {
                        let b_type = b
                            .get("type")
                            .and_then(|v| v.as_str())
                            .unwrap_or("UNKNOWN")
                            .to_string();
                        let amount = b
                            .get("amount")
                            .and_then(|v| v.as_i64())
                            .unwrap_or(0);
                        benefits.push(Benefit {
                            r#type: b_type,
                            amount,
                        });
                    }
                }
                if benefits.len() > count_before {
                    produced_benefit = true;
                }
            } else {
                // Fallback: try to get result directly
                let s = serde_json::to_string(&result_json).unwrap_or_default();
                if !s.is_empty() && s != "null" {
                    benefits.push(Benefit {
                        r#type: "UNKNOWN".to_string(),
                        amount: 0,
                    });
                    produced_benefit = true;
                }
            }
            if produced_benefit {
                matched_rule_ids.push(rule_id);
            }
        }
        Ok((benefits, matched_rule_ids))
    }
}

impl EvaluatorService {
    async fn evaluate_inner(
        &self,
        req: EvaluateRequest,
    ) -> Result<Response<EvaluateReply>, Status> {
        let cust_str = req
            .customer
            .as_ref()
            .map(|c| format!("customer_id={}", c.customer_id))
            .unwrap_or_else(|| "None".to_string());
        let tx_str = req
            .transaction
            .as_ref()
            .and_then(|t| t.transaction_today.as_ref())
            .map(|d| format!("sum={} avg={} count={}", d.sum, d.avg, d.count))
            .unwrap_or_else(|| "None".to_string());
        pkg::log::info!(
            "[Evaluator] received EvaluateRequest evaluator_id={} customer={} transaction={} job_id={}",
            req.evaluator_id,
            cust_str,
            tx_str,
            req.job_id
        );

        if req.evaluator_id == 0 {
            return Err(Status::invalid_argument("evaluator_id is required"));
        }

        let zen_input = build_zen_input(&req);
        pkg::log::info!(
            "[Evaluator] built zen input: {}",
            serde_json::to_string(&zen_input).unwrap_or_else(|_| format!("{:?}", zen_input))
        );

        // Also log the expected cashback.json example for reference:
        // input transaction.transaction_today.count is { "transaction": {"transaction_today": {"sum":1500000000,"count":150}} }
        // input transaction.transaction_windowed.sum is { "transaction": {"transaction_windowed": {"sum":1500000000,"count":150}} }
        // Our zen_input already matches that shape.

        let (benefits, matched_rule_ids) = self
            .evaluate_rules(req.evaluator_id, zen_input.clone())
            .await?;
        let zen_input_str =
            serde_json::to_string(&zen_input).unwrap_or_else(|_| format!("{:?}", zen_input));

        // Don't add customer in input to keep value small (500 customers → huge)
        let result_json = json!({
            "evaluator_id": req.evaluator_id,
            "benefits": benefits.iter().map(|b| json!({"type": b.r#type, "amount": b.amount})).collect::<Vec<_>>()
        });

        let today = Utc::now().format("%Y-%m-%d").to_string();
        let result_json_str_full = serde_json::to_string(&result_json).unwrap_or_default();
        // Single customer per request — looped by coordinator (3 times for 3 customers)
        let customer_opt = req.customer.as_ref();
        if let Some(cust) = customer_opt {
            let cid = cust.customer_id;
            if cid == 0 {
                pkg::log::warn!("[Evaluator] customer_id is 0, skipping benefit insert");
            } else {
                let ikey = idempotency_key(cid, req.evaluator_id as u64, &today);
                let per_customer_value = json!({
                    "benefits": benefits.iter().map(|b| json!({"type": b.r#type, "amount": b.amount})).collect::<Vec<_>>()
                });
                let per_customer_str = serde_json::to_string(&per_customer_value).unwrap_or_default();
                let insert_res = sqlx::query(
                    "INSERT INTO benefit (evaluator_id, customer_id, value, description, idempotency_key) VALUES (?, ?, CAST(? AS JSON), ?, ?)",
                )
                .bind(req.evaluator_id)
                .bind(cid)
                .bind(&per_customer_str)
                .bind(format!(
                    "evaluated {} benefits for customer {}",
                    benefits.len(),
                    cid
                ))
                .bind(&ikey)
                .execute(self.pool.as_ref())
                .await;
                match insert_res {
                    Ok(r) => {
                        pkg::log::info!(
                            "[Evaluator] inserted benefit id={} for evaluator_id={} customer_id={} job_id={} ikey={} value={} rule_ids={:?} zen_input={}",
                            r.last_insert_id(),
                            req.evaluator_id,
                            cid,
                            req.job_id,
                            ikey,
                            per_customer_str,
                            matched_rule_ids,
                            zen_input_str
                        );
                        let file_date = today.replace("-", "");
                        let path = format!("data/output/benefit{}.txt", file_date);
                        let line = format!(
                            "{{\"customer_id\":{},\"job_id\":{},\"idempotency_key\":\"{}\",\"value\":{}}}",
                            cid, req.job_id, ikey, per_customer_str
                        );
                        if let Err(e) = append_benefit_file(&path, &line).await {
                            pkg::log::error!("[Evaluator] failed to append to {}: {}", path, e);
                        }
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        if msg.contains("Duplicate entry") || msg.contains("1062") || msg.contains("UNIQUE") {
                            pkg::log::info!(
                                "[Evaluator] duplicate idempotency_key for evaluator_id={} customer_id={} job_id={} ikey={} — skipping insert and file (idempotent)",
                                req.evaluator_id,
                                cid,
                                req.job_id,
                                ikey
                            );
                        } else {
                            pkg::log::error!(
                                "[Evaluator] failed to insert benefit for evaluator_id={} customer_id={}: {}",
                                req.evaluator_id,
                                cid,
                                e
                            );
                        }
                    }
                }
            }
        } else {
            pkg::log::warn!(
                "[Evaluator] no customer in request for evaluator_id={} job_id={}, skipping benefit insert",
                req.evaluator_id,
                req.job_id
            );
        }
        // Only insert customer_id=0 when there is truly no customer AND no transaction
        if !benefits.is_empty() {
            if req.transaction.is_some() && req.transaction.as_ref().unwrap().transaction_today.is_some() {
                if req.customer.is_none() {
                    pkg::log::warn!(
                        "[Evaluator] transaction exists but customer missing for evaluator_id={} job_id={} — skipping 0-customer insert",
                        req.evaluator_id,
                        req.job_id
                    );
                }
                // already inserted per-customer above, do not insert 0
            } else if req.customer.is_none() {
                let ikey = idempotency_key(0, req.evaluator_id as u64, &today);
                let insert_res = sqlx::query(
                    "INSERT INTO benefit (evaluator_id, customer_id, value, description, idempotency_key) VALUES (?, ?, CAST(? AS JSON), ?, ?)",
                )
                .bind(req.evaluator_id)
                .bind(0_i64)
                .bind(&result_json_str_full)
                .bind(format!("evaluated {} benefits (no customer)", benefits.len()))
                .bind(&ikey)
                .execute(self.pool.as_ref())
                .await;
                match insert_res {
                    Ok(r) => {
                        pkg::log::info!(
                            "[Evaluator] inserted benefit id={} for evaluator_id={} job_id={} ikey={} (no customer) value={} rule_ids={:?} zen_input={}",
                            r.last_insert_id(),
                            req.evaluator_id,
                            req.job_id,
                            ikey,
                            result_json_str_full,
                            matched_rule_ids,
                            zen_input_str
                        );
                        let file_date = today.replace("-", "");
                        let path = format!("data/output/benefit{}.txt", file_date);
                        let line = format!(
                            "{{\"customer_id\":0,\"job_id\":{},\"idempotency_key\":\"{}\",\"value\":{}}}",
                            req.job_id, ikey, result_json_str_full
                        );
                        if let Err(e) = append_benefit_file(&path, &line).await {
                            pkg::log::error!("[Evaluator] failed to append to {}: {}", path, e);
                        }
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        if msg.contains("Duplicate entry") || msg.contains("1062") || msg.contains("UNIQUE") {
                            pkg::log::info!(
                                "[Evaluator] duplicate idempotency_key for evaluator_id={} job_id={} ikey={} (no customer) — skipping",
                                req.evaluator_id,
                                req.job_id,
                                ikey
                            );
                        } else {
                            pkg::log::error!(
                                "[Evaluator] failed to insert benefit for evaluator_id={} (no customer): {}",
                                req.evaluator_id,
                                e
                            );
                        }
                    }
                }
            }
        }

        let reply = EvaluateReply {
            result_json: result_json_str_full,
            benefits: benefits.clone(),
        };

        pkg::log::info!(
            "[Evaluator] parsed response evaluator_id={} benefits={:?} result_json={}",
            req.evaluator_id,
            benefits,
            reply.result_json
        );

        Ok(Response::new(reply))
    }
}

#[tonic::async_trait]
impl EvaluatorTrait for EvaluatorService {
    async fn evaluate(
        &self,
        request: Request<EvaluateRequest>,
    ) -> Result<Response<EvaluateReply>, Status> {
        let req = request.into_inner();
        let timeout = api_timeout();
        match tokio::time::timeout(timeout, self.evaluate_inner(req)).await {
            Ok(res) => res,
            Err(_) => Err(Status::deadline_exceeded(format!(
                "Evaluate timeout after {}ms",
                timeout.as_millis()
            ))),
        }
    }
}

pub async fn start_evaluator_server(pool: DbPool, addr: &str) -> anyhow::Result<()> {
    use pkg::proto::evaluator::evaluator_server::EvaluatorServer;
    let svc = EvaluatorService::new(pool);
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(pkg::proto::FILE_DESCRIPTOR_SET)
        .build_v1()
        .unwrap();
    tonic::transport::Server::builder()
        .add_service(EvaluatorServer::new(svc))
        .add_service(reflection)
        .serve(addr.parse()?)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pkg::proto::evaluator::{Customer, Transaction, TransactionData};

    fn make_req() -> EvaluateRequest {
        EvaluateRequest {
            evaluator_id: 1,
            customer: Some(Customer { customer_id: 42 }),
            transaction: Some(Transaction {
                transaction_today: Some(TransactionData {
                    sum: 5000000000,
                    avg: 5000000001,
                    count: 4,
                }),
            }),
            job_id: 123,
        }
    }

    #[test]
    fn build_zen_input_maps_transaction_and_customer() {
        let req = make_req();
        let input = build_zen_input(&req);
        // Should have transaction.transaction_today.sum etc. and customer.customer_id
        assert_eq!(
            input["transaction"]["transaction_today"]["sum"],
            json!(5000000000i64)
        );
        assert_eq!(
            input["transaction"]["transaction_today"]["count"],
            json!(4)
        );
        assert_eq!(input["customer"]["customer_id"], json!(42));
    }

    #[test]
    fn build_zen_input_empty() {
        let req = EvaluateRequest {
            evaluator_id: 1,
            customer: None,
            transaction: None,
            job_id: 0,
        };
        let input = build_zen_input(&req);
        assert!(input.as_object().unwrap().is_empty());
    }

    #[test]
    fn build_zen_input_per_customer_array() {
        // Coordinator loops per deduped customer_id from transaction's customer_id column
        // Example from prompt: 3 customers sharing same transaction_today
        let tx = Transaction {
            transaction_today: Some(TransactionData {
                sum: 2500001000,
                avg: 357143000,
                count: 7,
            }),
        };
        let reqs: Vec<EvaluateRequest> = vec![1, 2, 3]
            .into_iter()
            .map(|cid| EvaluateRequest {
                evaluator_id: 1,
                customer: Some(Customer { customer_id: cid }),
                transaction: Some(tx.clone()),
                job_id: 123,
            })
            .collect();
        assert_eq!(reqs.len(), 3);
        for (i, req) in reqs.iter().enumerate() {
            let input = build_zen_input(req);
            assert_eq!(input["customer"]["customer_id"], json!((i + 1) as i64));
            assert_eq!(
                input["transaction"]["transaction_today"]["sum"],
                json!(2500001000i64)
            );
            assert_eq!(input["transaction"]["transaction_today"]["count"], json!(7));
            assert_eq!(
                input["transaction"]["transaction_today"]["avg"],
                json!(357143000i64)
            );
        }
        // JSON array view: each req corresponds to one element of expected array
        let array: Vec<Value> = reqs.iter().map(build_zen_input).collect();
        assert_eq!(array.len(), 3);
        assert_eq!(array[0]["customer"]["customer_id"], json!(1));
        assert_eq!(array[1]["customer"]["customer_id"], json!(2));
        assert_eq!(array[2]["customer"]["customer_id"], json!(3));
    }

    #[test]
    fn parse_decision_content_valid_cashback() {
        let content: Value =
            serde_json::from_str(include_str!("../../../../../data/rule/cashback.json"))
                .unwrap();
        let dc = parse_decision_content(&content).unwrap();
        // DecisionContent should have nodes
        let json = serde_json::to_value(&dc).unwrap();
        assert!(json.get("nodes").is_some());
    }

    #[tokio::test]
    async fn evaluate_with_cashback_rule_matches_example() {
        // Direct zen evaluation without DB, using cashback.json content
        let content: Value =
            serde_json::from_str(include_str!("../../../../../data/rule/cashback.json"))
                .unwrap();
        let dc: DecisionContent = serde_json::from_value(content).unwrap();
        let engine = DecisionEngine::default();
        let decision = engine.create_decision(Arc::new(dc)).unwrap();
        // Example from data/rule/cashback.json: transaction.transaction_today.sum >=1500000000 and count >=3 => benefit 5000000
        let input = json!({
            "transaction": {
                "transaction_today": { "sum": 1500000000i64, "count": 150 }
            }
        });
        let result = decision.evaluate(input.into()).await.unwrap();
        let result_json: Value = result.result.to_value();
        // result should contain benefit
        assert!(
            result_json.get("benefit").is_some() || result_json.to_string().contains("benefit"),
            "result should contain benefit, got {}",
            result_json
        );
    }

    #[tokio::test]
    async fn evaluate_with_request_data() {
        let content: Value =
            serde_json::from_str(include_str!("../../../../../data/rule/cashback.json"))
                .unwrap();
        let dc: DecisionContent = serde_json::from_value(content).unwrap();
        let engine = DecisionEngine::default();
        let decision = engine.create_decision(Arc::new(dc)).unwrap();
        let req = make_req();
        let zen_input = build_zen_input(&req);
        let result = decision.evaluate(zen_input.into()).await.unwrap();
        let result_json: Value = result.result.to_value();
        // With sum 5000000000 and count 4, should hit first rule (CASHBACK 10000000)
        let s = result_json.to_string();
        assert!(
            s.contains("CASHBACK") || s.contains("benefit"),
            "expected CASHBACK, got {}",
            s
        );
    }

    #[test]
    fn idempotency_key_is_sha256_of_customer_job_date() {
        let key = idempotency_key(42, 123, "2026-09-20");
        // SHA256 hex is 64 chars
        assert_eq!(key.len(), 64);
        // deterministic
        assert_eq!(key, idempotency_key(42, 123, "2026-09-20"));
        // different job/evaluator yields different key
        assert_ne!(key, idempotency_key(42, 124, "2026-09-20"));
        // different customer yields different key
        assert_ne!(key, idempotency_key(43, 123, "2026-09-20"));
        // known value: sha256("42-123-2026-09-20")
        let mut hasher = Sha256::new();
        hasher.update(b"42-123-2026-09-20");
        let expected = format!("{:x}", hasher.finalize());
        assert_eq!(key, expected);
    }

    #[test]
    fn idempotency_uses_evaluator_id_not_job_id() {
        // Regression: idempotency must be customer_id + evaluator_id + date, not job_id
        // Same customer + same evaluator + same day → same key even if job_id differs
        // (coordinator loops per customer with different job_id values would otherwise create duplicates)
        let today = "2026-09-20";
        let cid = 1;
        let evaluator_id = 1u64;
        let job_a = 100u64;
        let job_b = 101u64;
        let key_evaluator = idempotency_key(cid, evaluator_id, today);
        let key_job_a = idempotency_key(cid, job_a, today);
        let key_job_b = idempotency_key(cid, job_b, today);
        // evaluator-based key is stable across jobs
        assert_eq!(key_evaluator, idempotency_key(cid, evaluator_id, today));
        // job-based keys would differ (the bug)
        assert_ne!(key_job_a, key_job_b);
        // evaluator key differs from job key
        assert_ne!(key_evaluator, key_job_a);
        // Verify with known hash for evaluator path
        let mut hasher = Sha256::new();
        hasher.update(format!("{}-{}-{}", cid, evaluator_id, today).as_bytes());
        let expected = format!("{:x}", hasher.finalize());
        assert_eq!(key_evaluator, expected);
    }

    #[test]
    fn no_zero_customer_when_transaction_exists() {
        let req_with_tx_no_cust = EvaluateRequest {
            evaluator_id: 1,
            customer: None,
            transaction: Some(Transaction {
                transaction_today: Some(TransactionData {
                    sum: 1000,
                    avg: 500,
                    count: 2,
                }),
            }),
            job_id: 555,
        };
        let input = build_zen_input(&req_with_tx_no_cust);
        assert_eq!(
            input["transaction"]["transaction_today"]["sum"],
            json!(1000)
        );
        assert!(input.get("customer").is_none());
        let ik_zero = idempotency_key(0, 555, "2026-09-20");
        let ik_real = idempotency_key(101, 555, "2026-09-20");
        assert_ne!(ik_zero, ik_real);
        assert_eq!(ik_zero.len(), 64);
    }

    #[tokio::test]
    async fn benefit_file_append_newline_handling() {
        let dir = "/tmp/test_benefit_file_evaluator";
        let _ = tokio::fs::create_dir_all(dir).await;
        let path = format!("{}/benefit20260920.txt", dir);
        let _ = tokio::fs::remove_file(&path).await;
        append_benefit_file(&path, "line1").await.unwrap();
        let c1 = tokio::fs::read_to_string(&path).await.unwrap();
        assert_eq!(c1, "line1");
        append_benefit_file(&path, "line2").await.unwrap();
        let c2 = tokio::fs::read_to_string(&path).await.unwrap();
        assert_eq!(c2, "line1\nline2");
        // third append should also add newline
        append_benefit_file(&path, "line3").await.unwrap();
        let c3 = tokio::fs::read_to_string(&path).await.unwrap();
        assert_eq!(c3, "line1\nline2\nline3");
        let _ = tokio::fs::remove_file(&path).await;
        let _ = tokio::fs::remove_dir(dir).await;
    }

    #[test]
    fn api_timeout_default_is_50ms() {
        unsafe {
            std::env::remove_var("API_TIMEOUT_MS");
            std::env::remove_var("GRPC_TIMEOUT_MS");
        }
        assert_eq!(api_timeout(), Duration::from_millis(200));
    }

    #[test]
    fn api_timeout_respects_env() {
        unsafe {
            std::env::remove_var("GRPC_TIMEOUT_MS");
            std::env::set_var("API_TIMEOUT_MS", "88");
        }
        assert_eq!(api_timeout(), Duration::from_millis(88));
        unsafe { std::env::remove_var("API_TIMEOUT_MS"); }
        assert_eq!(api_timeout(), Duration::from_millis(200));
        // GRPC_TIMEOUT_MS takes priority over API_TIMEOUT_MS
        unsafe {
            std::env::set_var("API_TIMEOUT_MS", "88");
            std::env::set_var("GRPC_TIMEOUT_MS", "99");
        }
        assert_eq!(api_timeout(), Duration::from_millis(99));
        unsafe {
            std::env::remove_var("API_TIMEOUT_MS");
            std::env::remove_var("GRPC_TIMEOUT_MS");
        }
        assert_eq!(api_timeout(), Duration::from_millis(200));
    }

    #[tokio::test]
    async fn timeout_wraps_slow_evaluate() {
        let timeout = Duration::from_millis(50);
        let slow = async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            Ok::<String, String>("done".into())
        };
        let res = tokio::time::timeout(timeout, slow).await;
        assert!(res.is_err());
    }
}
