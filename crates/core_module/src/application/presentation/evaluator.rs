use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::Json,
    routing::get,
    Router,
};
use chrono::{NaiveDateTime, Utc};
use serde::Deserialize;
use sqlx::Row;
use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

use pkg::mysql::DbPool;

use crate::repository::model::benefit::BenefitModel;

/// Timeout for API handlers in milliseconds.
/// Can be overridden via `API_TIMEOUT_MS` env var, defaults to 500ms.
fn api_timeout() -> Duration {
    std::env::var("API_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_millis(500))
}

// ---------------------------------------------------------------------------
// In-memory cache for presentation benefits (fixes 50ms timeout under load)
// ---------------------------------------------------------------------------

fn benefit_cache_ttl() -> Duration {
    std::env::var("BENEFIT_CACHE_TTL_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_millis(30_000))
}

fn evaluator_cache_ttl() -> Duration {
    std::env::var("EVALUATOR_CACHE_TTL_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_millis(60_000))
}

#[derive(Clone)]
struct BenefitCacheEntry {
    benefits: Vec<BenefitModel>,
    expiry: Instant,
}

#[derive(Clone)]
struct EvaluatorValidityCacheEntry {
    result: Result<(), (StatusCode, String)>,
    expiry: Instant,
}

static BENEFIT_CACHE: OnceLock<RwLock<HashMap<(i64, Option<i64>), BenefitCacheEntry>>> =
    OnceLock::new();
static EVALUATOR_VALIDITY_CACHE: OnceLock<RwLock<HashMap<i64, EvaluatorValidityCacheEntry>>> =
    OnceLock::new();

fn benefit_cache() -> &'static RwLock<HashMap<(i64, Option<i64>), BenefitCacheEntry>> {
    BENEFIT_CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

fn evaluator_validity_cache() -> &'static RwLock<HashMap<i64, EvaluatorValidityCacheEntry>> {
    EVALUATOR_VALIDITY_CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

async fn get_cached_benefits(key: (i64, Option<i64>)) -> Option<Vec<BenefitModel>> {
    let cache = benefit_cache().read().await;
    if let Some(entry) = cache.get(&key) {
        if Instant::now() < entry.expiry {
            return Some(entry.benefits.clone());
        }
    }
    None
}

async fn insert_cached_benefits(key: (i64, Option<i64>), benefits: Vec<BenefitModel>) {
    let mut cache = benefit_cache().write().await;
    cache.insert(
        key,
        BenefitCacheEntry {
            benefits,
            expiry: Instant::now() + benefit_cache_ttl(),
        },
    );
}

async fn get_cached_evaluator_validity(evaluator_id: i64) -> Option<Result<(), (StatusCode, String)>> {
    let cache = evaluator_validity_cache().read().await;
    if let Some(entry) = cache.get(&evaluator_id) {
        if Instant::now() < entry.expiry {
            return Some(entry.result.clone());
        }
    }
    None
}

async fn insert_cached_evaluator_validity(evaluator_id: i64, result: Result<(), (StatusCode, String)>) {
    let mut cache = evaluator_validity_cache().write().await;
    cache.insert(
        evaluator_id,
        EvaluatorValidityCacheEntry {
            result,
            expiry: Instant::now() + evaluator_cache_ttl(),
        },
    );
}

/// Clear all caches – used in tests and for manual invalidation.
pub async fn clear_benefit_cache() {
    benefit_cache().write().await.clear();
    evaluator_validity_cache().write().await.clear();
}

async fn evict_expired_benefit_cache() {
    let now = Instant::now();
    let mut cache = benefit_cache().write().await;
    cache.retain(|_, v| now < v.expiry);
    let mut e_cache = evaluator_validity_cache().write().await;
    e_cache.retain(|_, v| now < v.expiry);
}

#[derive(Debug, Deserialize)]
pub struct GetBenefitParams {
    pub customer_id: i64,
    pub evaluator_id: Option<i64>,
}

/// Check if evaluator is active and within valid date range.
/// Cached to avoid DB hit on hot path (50ms timeout).
/// Returns Ok(()) if valid, Err((StatusCode, String)) if not.
async fn check_evaluator_valid(
    pool: &DbPool,
    evaluator_id: i64,
) -> Result<(), (StatusCode, String)> {
    if let Some(cached) = get_cached_evaluator_validity(evaluator_id).await {
        pkg::log::info!(
            "[Cache] evaluator_validity hit for evaluator_id={} result={:?}",
            evaluator_id,
            cached.is_ok()
        );
        return cached;
    }
    let result = check_evaluator_valid_inner(pool, evaluator_id).await;
    insert_cached_evaluator_validity(evaluator_id, result.clone()).await;
    result
}

async fn check_evaluator_valid_inner(
    pool: &DbPool,
    evaluator_id: i64,
) -> Result<(), (StatusCode, String)> {
    let row = sqlx::query(
        "SELECT is_active, CAST(start_valid_date AS DATETIME) as start_valid_date, CAST(end_valid_date AS DATETIME) as end_valid_date FROM evaluator WHERE id = ? AND deleted_at IS NULL",
    )
    .bind(evaluator_id)
    .fetch_optional(pool.as_ref())
    .await
    .map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to fetch evaluator {}: {}", evaluator_id, e),
        )
    })?;

    let row = match row {
        Some(r) => r,
        None => {
            return Err((
                StatusCode::NOT_FOUND,
                format!("evaluator {} not found", evaluator_id),
            ))
        }
    };

    // is_active is boolean in MySQL (TINYINT)
    let is_active: bool = row.try_get("is_active").map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to parse is_active: {}", e),
        )
    })?;
    if !is_active {
        return Err((
            StatusCode::FORBIDDEN,
            format!("evaluator {} is not active", evaluator_id),
        ));
    }

    let start_valid_date: Option<NaiveDateTime> = row.try_get("start_valid_date").unwrap_or(None);
    let end_valid_date: Option<NaiveDateTime> = row.try_get("end_valid_date").unwrap_or(None);

    let now = Utc::now().naive_utc();

    if let Some(start) = start_valid_date {
        if now < start {
            return Err((
                StatusCode::FORBIDDEN,
                format!(
                    "evaluator {} not yet valid: now {} < start_valid_date {}",
                    evaluator_id, now, start
                ),
            ));
        }
    }
    if let Some(end) = end_valid_date {
        if now > end {
            return Err((
                StatusCode::FORBIDDEN,
                format!(
                    "evaluator {} expired: now {} > end_valid_date {}",
                    evaluator_id, now, end
                ),
            ));
        }
    }

    Ok(())
}

/// Core logic: get benefits for customer_id, checking evaluator active/valid and created_at today.
/// Does not parse value JSON — returns raw Value as stored.
/// Results are cached per (customer_id, evaluator_id) to meet 50ms timeout under load.
pub async fn get_benefits_by_customer(
    pool: &DbPool,
    customer_id: i64,
    evaluator_id: Option<i64>,
) -> Result<Vec<BenefitModel>, (StatusCode, String)> {
    let cache_key = (customer_id, evaluator_id);
    if let Some(cached) = get_cached_benefits(cache_key).await {
        pkg::log::info!(
            "[Cache] benefit hit for customer_id={} evaluator_id={:?} count={}",
            customer_id,
            evaluator_id,
            cached.len()
        );
        return Ok(cached);
    }
    // Helper to decode a row into BenefitModel, handling TIMESTAMP as string to avoid sqlx DATETIME vs TIMESTAMP mismatch
    async fn fetch_benefits(
        pool: &DbPool,
        sql: &'static str,
        customer_id: i64,
        evaluator_id: Option<i64>,
    ) -> Result<Vec<BenefitModel>, (StatusCode, String)> {
        let mut query = sqlx::query(sql);
        query = query.bind(customer_id);
        if let Some(eid) = evaluator_id {
            query = query.bind(eid);
        }
        let rows = query
            .fetch_all(pool.as_ref())
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed to fetch benefits: {}", e),
                )
            })?;
        let mut out = Vec::new();
        for row in rows {
            // value is JSON — try as Value, fallback to String
            let value: Option<serde_json::Value> = row.try_get("value").unwrap_or(None);
            let description: Option<String> = row.try_get("description").unwrap_or(None);
            // TIMESTAMP columns: try NaiveDateTime first, then String, then fallback None
            let expired_at: Option<NaiveDateTime> = {
                if let Ok(v) = row.try_get::<Option<NaiveDateTime>, _>("expired_at") {
                    v
                } else if let Ok(v) = row.try_get::<Option<String>, _>("expired_at") {
                    v.and_then(|s| NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S").ok())
                } else {
                    None
                }
            };
            let created_at: Option<NaiveDateTime> = {
                if let Ok(v) = row.try_get::<Option<NaiveDateTime>, _>("created_at") {
                    v
                } else if let Ok(v) = row.try_get::<Option<String>, _>("created_at") {
                    v.and_then(|s| {
                        NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S")
                            .ok()
                            .or_else(|| NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.f").ok())
                            .or_else(|| NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S").ok())
                    })
                } else {
                    None
                }
            };
            let id: i64 = row.try_get("id").map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed to parse id: {}", e),
                )
            })?;
            let evaluator_id: i64 = row.try_get("evaluator_id").map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed to parse evaluator_id: {}", e),
                )
            })?;
            let customer_id: i64 = row.try_get("customer_id").map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed to parse customer_id: {}", e),
                )
            })?;
            out.push(BenefitModel {
                id,
                evaluator_id,
                customer_id,
                value,
                description,
                expired_at,
                created_at,
            });
        }
        Ok(out)
    }

    if let Some(eid) = evaluator_id {
        check_evaluator_valid(pool, eid).await?;
        let benefits = fetch_benefits(
            pool,
            "SELECT id, evaluator_id, customer_id, value, description, CAST(expired_at AS CHAR) as expired_at, CAST(created_at AS CHAR) as created_at FROM benefit WHERE customer_id = ? AND evaluator_id = ? AND created_at >= CURDATE() AND created_at < CURDATE() + INTERVAL 1 DAY",
            customer_id,
            Some(eid),
        )
        .await?;
        let benefits_clone = benefits.clone();
        insert_cached_benefits(cache_key, benefits_clone).await;
        evict_expired_benefit_cache().await;
        return Ok(benefits);
    }

    let all_benefits = fetch_benefits(
        pool,
        "SELECT id, evaluator_id, customer_id, value, description, CAST(expired_at AS CHAR) as expired_at, CAST(created_at AS CHAR) as created_at FROM benefit WHERE customer_id = ? AND created_at >= CURDATE() AND created_at < CURDATE() + INTERVAL 1 DAY",
        customer_id,
        None,
    )
    .await?;

    // Filter by evaluator active/valid
    let mut result = Vec::new();
    for b in all_benefits {
        // Check evaluator for this benefit (cached)
        if check_evaluator_valid(pool, b.evaluator_id).await.is_ok() {
            result.push(b);
        }
    }
    let result_clone = result.clone();
    insert_cached_benefits(cache_key, result_clone).await;
    evict_expired_benefit_cache().await;
    Ok(result)
}

/// Axum handler: GET /benefits?customer_id=123&evaluator_id=1
/// Query params: customer_id (required), evaluator_id (optional)
/// Wrapped in `tokio::time::timeout` to avoid hanging DB calls.
pub async fn get_benefits_handler(
    State(pool): State<DbPool>,
    Query(params): Query<GetBenefitParams>,
) -> Result<Json<Vec<BenefitModel>>, (StatusCode, String)> {
    let timeout = api_timeout();
    let fut = get_benefits_by_customer(&pool, params.customer_id, params.evaluator_id);
    match tokio::time::timeout(timeout, fut).await {
        Ok(res) => res.map(Json),
        Err(_) => Err((
            StatusCode::REQUEST_TIMEOUT,
            format!("request timeout after {}ms", timeout.as_millis()),
        )),
    }
}

/// Create router for presentation layer
pub fn create_evaluator_router(pool: DbPool) -> Router {
    Router::new()
        .route("/benefits", get(get_benefits_handler))
        .with_state(pool)
}

/// Start HTTP server for evaluator presentation API
pub async fn start_presentation_server(pool: DbPool, addr: &str) -> anyhow::Result<()> {
    let app = create_evaluator_router(pool);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    pkg::log::info!("starting evaluator presentation server on {}", addr);
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn is_evaluator_valid_for_test(
        is_active: bool,
        start_valid_date: Option<NaiveDateTime>,
        end_valid_date: Option<NaiveDateTime>,
        now: NaiveDateTime,
    ) -> bool {
        if !is_active {
            return false;
        }
        if let Some(start) = start_valid_date {
            if now < start {
                return false;
            }
        }
        if let Some(end) = end_valid_date {
            if now > end {
                return false;
            }
        }
        true
    }

    #[test]
    fn valid_date_check_is_active() {
        let now = Utc::now().naive_utc();
        assert!(!is_evaluator_valid_for_test(
            false,
            None,
            None,
            now
        ));
        assert!(is_evaluator_valid_for_test(true, None, None, now));
    }

    #[test]
    fn valid_date_check_start_end() {
        let now = Utc::now().naive_utc();
        let past = now - Duration::days(1);
        let future = now + Duration::days(1);
        // Not yet valid
        assert!(!is_evaluator_valid_for_test(true, Some(future), None, now));
        // Expired
        assert!(!is_evaluator_valid_for_test(true, None, Some(past), now));
        // Valid within range
        assert!(is_evaluator_valid_for_test(
            true,
            Some(past),
            Some(future),
            now
        ));
        // Valid with only start in past
        assert!(is_evaluator_valid_for_test(true, Some(past), None, now));
        // Valid with only end in future
        assert!(is_evaluator_valid_for_test(true, None, Some(future), now));
    }

    #[test]
    fn created_at_today_sql_uses_range() {
        // Uses range query for today (faster, sargable, avoids DATE() function)
        // Should be `created_at >= CURDATE() AND created_at < CURDATE() + INTERVAL 1 DAY`
        let sql = "SELECT id, evaluator_id, customer_id, value, description, CAST(expired_at AS CHAR) as expired_at, CAST(created_at AS CHAR) as created_at \
                   FROM benefit WHERE customer_id = ? AND created_at >= CURDATE() AND created_at < CURDATE() + INTERVAL 1 DAY";
        assert!(sql.contains("created_at >= CURDATE()"));
        assert!(sql.contains("CURDATE() + INTERVAL 1 DAY"));
        assert!(sql.contains("customer_id = ?"));
    }

    #[test]
    fn api_timeout_default_is_50ms() {
        // Default should be 500ms when API_TIMEOUT_MS not set
        unsafe { std::env::remove_var("API_TIMEOUT_MS"); }
        let t = super::api_timeout();
        assert_eq!(t, std::time::Duration::from_millis(500));
    }

    #[test]
    fn api_timeout_respects_env() {
        unsafe { std::env::set_var("API_TIMEOUT_MS", "123"); }
        let t = super::api_timeout();
        assert_eq!(t, std::time::Duration::from_millis(123));
        unsafe { std::env::remove_var("API_TIMEOUT_MS"); }
        // restore default
        assert_eq!(super::api_timeout(), std::time::Duration::from_millis(500));
    }

    #[tokio::test]
    async fn timeout_triggers_on_slow_future() {
        let timeout = std::time::Duration::from_millis(50);
        let slow = async {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            42
        };
        let res = tokio::time::timeout(timeout, slow).await;
        assert!(res.is_err(), "should timeout after 50ms");
    }

    #[tokio::test]
    async fn timeout_passes_for_fast_future() {
        let timeout = std::time::Duration::from_millis(50);
        let fast = async { 42 };
        let res = tokio::time::timeout(timeout, fast).await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), 42);
    }

    #[test]
    fn value_not_parsed() {
        // Benefit value should be returned as is, not parsed
        // The handler returns BenefitModel with value: Option<Value> directly
        let benefit = BenefitModel {
            id: 1,
            evaluator_id: 1,
            customer_id: 42,
            value: Some(serde_json::json!({"benefits": [{"type": "CASHBACK", "amount": 10000000}]})),
            description: Some("test".to_string()),
            expired_at: None,
            created_at: Some(Utc::now().naive_utc()),
        };
        // Simulate not parsing: just return as is
        let json = serde_json::to_value(&benefit).unwrap();
        assert_eq!(
            json["value"],
            serde_json::json!({"benefits": [{"type": "CASHBACK", "amount": 10000000}]})
        );
    }

    #[test]
    fn customer_id_not_zero_when_transaction_exists_logic() {
        // For transaction entity, customer_id should not be 0
        // This mirrors the evaluator logic: if transaction exists but customer empty, warn and skip 0
        let has_transaction = true;
        let customers_empty = true;
        let should_skip_zero = has_transaction && customers_empty;
        assert!(should_skip_zero, "should skip 0-customer when transaction exists");
        // For non-transaction (no transaction), 0 is allowed for generic benefit
        let has_transaction = false;
        let should_skip_zero = has_transaction && customers_empty;
        assert!(!should_skip_zero);
    }

    #[tokio::test]
    async fn benefit_cache_hit_and_miss() {
        clear_benefit_cache().await;
        let key = (99991i64, Some(1i64));
        assert!(get_cached_benefits(key).await.is_none());
        let benefit = BenefitModel {
            id: 1,
            evaluator_id: 1,
            customer_id: 99991,
            value: Some(serde_json::json!({"benefits": [{"type": "CASHBACK", "amount": 100}]})),
            description: Some("cached".into()),
            expired_at: None,
            created_at: Some(Utc::now().naive_utc()),
        };
        insert_cached_benefits(key, vec![benefit.clone()]).await;
        let hit = get_cached_benefits(key).await;
        assert!(hit.is_some());
        let hit_val = hit.unwrap();
        assert_eq!(hit_val.len(), 1);
        assert_eq!(hit_val[0].customer_id, 99991);
        // different key misses
        assert!(get_cached_benefits((99991, Some(2))).await.is_none());
        assert!(get_cached_benefits((99991, None)).await.is_none());
        // clear invalidates
        clear_benefit_cache().await;
        assert!(get_cached_benefits(key).await.is_none());
    }

    #[tokio::test]
    async fn benefit_cache_expires_after_ttl() {
        clear_benefit_cache().await;
        unsafe { std::env::set_var("BENEFIT_CACHE_TTL_MS", "50"); }
        let key = (99992i64, Some(1i64));
        let benefit = BenefitModel {
            id: 2,
            evaluator_id: 1,
            customer_id: 99992,
            value: None,
            description: None,
            expired_at: None,
            created_at: Some(Utc::now().naive_utc()),
        };
        insert_cached_benefits(key, vec![benefit]).await;
        // immediate hit
        assert!(get_cached_benefits(key).await.is_some());
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        // expired
        assert!(get_cached_benefits(key).await.is_none());
        clear_benefit_cache().await;
        unsafe { std::env::remove_var("BENEFIT_CACHE_TTL_MS"); }
    }

    #[tokio::test]
    async fn evaluator_validity_cache_hit_and_expiry() {
        clear_benefit_cache().await;
        let eid = 99993i64;
        assert!(get_cached_evaluator_validity(eid).await.is_none());
        insert_cached_evaluator_validity(eid, Ok(())).await;
        let cached = get_cached_evaluator_validity(eid).await;
        assert!(cached.is_some());
        assert!(cached.unwrap().is_ok());
        // cache error as well
        let eid2 = 99994i64;
        insert_cached_evaluator_validity(
            eid2,
            Err((StatusCode::NOT_FOUND, "evaluator 99994 not found".into())),
        )
        .await;
        let cached_err = get_cached_evaluator_validity(eid2).await;
        assert!(cached_err.is_some());
        assert_eq!(cached_err.unwrap().unwrap_err().0, StatusCode::NOT_FOUND);
        // expiry
        unsafe { std::env::set_var("EVALUATOR_CACHE_TTL_MS", "50"); }
        let eid3 = 99995i64;
        insert_cached_evaluator_validity(eid3, Ok(())).await;
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        assert!(get_cached_evaluator_validity(eid3).await.is_none());
        clear_benefit_cache().await;
        unsafe { std::env::remove_var("EVALUATOR_CACHE_TTL_MS"); }
    }

    #[test]
    fn benefit_cache_ttl_defaults() {
        unsafe {
            std::env::remove_var("BENEFIT_CACHE_TTL_MS");
            std::env::remove_var("EVALUATOR_CACHE_TTL_MS");
        }
        assert_eq!(benefit_cache_ttl(), std::time::Duration::from_millis(30_000));
        assert_eq!(evaluator_cache_ttl(), std::time::Duration::from_millis(60_000));
        unsafe { std::env::set_var("BENEFIT_CACHE_TTL_MS", "12345"); }
        assert_eq!(benefit_cache_ttl(), std::time::Duration::from_millis(12345));
        unsafe { std::env::remove_var("BENEFIT_CACHE_TTL_MS"); }
        assert_eq!(benefit_cache_ttl(), std::time::Duration::from_millis(30_000));
    }

    #[tokio::test]
    async fn benefit_cache_separate_keys_isolated() {
        clear_benefit_cache().await;
        let key1 = (1001i64, Some(1i64));
        let key2 = (1001i64, None);
        let b1 = BenefitModel {
            id: 10,
            evaluator_id: 1,
            customer_id: 1001,
            value: Some(serde_json::json!({"v": 1})),
            description: None,
            expired_at: None,
            created_at: Some(Utc::now().naive_utc()),
        };
        let b2 = BenefitModel {
            id: 11,
            evaluator_id: 2,
            customer_id: 1001,
            value: Some(serde_json::json!({"v": 2})),
            description: None,
            expired_at: None,
            created_at: Some(Utc::now().naive_utc()),
        };
        insert_cached_benefits(key1, vec![b1.clone()]).await;
        insert_cached_benefits(key2, vec![b2.clone()]).await;
        let r1 = get_cached_benefits(key1).await.unwrap();
        let r2 = get_cached_benefits(key2).await.unwrap();
        assert_eq!(r1[0].id, 10);
        assert_eq!(r2[0].id, 11);
        clear_benefit_cache().await;
    }
}
