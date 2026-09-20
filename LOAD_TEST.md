# Load Test - BMIC COP

`load_test.js` is a [k6](https://k6.io/) script that hits **all 5 entry points** with **1000 hits each** (`TOTAL_JOBS=1000`). Every handler is wrapped in `tokio::time::timeout(50ms)` (`API_TIMEOUT_MS`).

## 1. What is tested

| # | Service | Proto / HTTP | Address (env) | RPC / Path | Payload | Expected |
|---|---------|--------------|---------------|------------|---------|----------|
| 1 | **Coordinator** | `coordinator.proto` gRPC | `COORDINATOR_ADDR=127.0.0.1:10162` | `coordinator.Coordinator/SubmitJob` | `files:["transaction20260914.txt"] n_reduce:4 entity:transaction` | `jobId` |
| 2 | **Ingestion Ingest** | `client.proto` gRPC | `INGESTION_ADDR=127.0.0.1:50051` | `client.Client/Ingest` | `job:{evaluator_id:1 input:{key:data_transaction_widow entity:transaction timeRange:{between 2026-09-01..07}}}` | `jobIds` or `DEADLINE_EXCEEDED` |
| 3 | **Ingestion SubmitJobs** | `client.proto` gRPC | same | `client.Client/SubmitJobs` (alias) | `job:{data_transaction_today named today}` | `jobIds` or `DEADLINE_EXCEEDED` |
| 4 | **Evaluator** | `evaluator.proto` gRPC | `EVALUATOR_ADDR=127.0.0.1:50052` | `evaluator.Evaluator/Evaluate` | `evaluator_id:1 customer:{101} transaction:{sum:5e9 avg:1.25e9 count:4} jobId:999` | `resultJson` + `benefits` or `DEADLINE_EXCEEDED` |
| 5a | **Presentation** | HTTP | `PRESENTATION_ADDR=127.0.0.1:8081` | `GET /benefits?customer_id=101&evaluator_id=1` | - | `200` with `value` raw |
| 5b | Presentation | HTTP | same | `GET /benefits?customer_id=102` | old date `2020-01-01` | `200 []` |
| 5c | Presentation | HTTP | same | `GET /benefits?customer_id=104&evaluator_id=99` | inactive | `403` |
| 5d | Presentation | HTTP | same | `GET /benefits?customer_id=999` | missing | `200 []` |
| 5e | Presentation | HTTP | same | `GET /benefits?customer_id=101&evaluator_id=99999` | not found | `404` |

`5a-e` are executed **per iteration** (4+1 extra check), so `TOTAL_JOBS=1000` → 1000 × 5 service calls + 5000 HTTP checks.

Implementation: `crates/ingestion_engine/src/application/client/mod.rs::handle_ingest` (Ingest/SubmitJobs), `crates/core_module/src/application/evaluator/mod.rs::evaluate` (Evaluate), `crates/core_module/src/application/presentation/evaluator.rs::get_benefits_handler` – all use `tokio::time::timeout(api_timeout())` where `api_timeout()=Duration::from_millis(API_TIMEOUT_MS || 50)`.

## 2. Config

| Env | Default | Desc |
|-----|---------|------|
| `TOTAL_JOBS` | `1000` | Iterations. Each iteration hits **all** services → 1000 hits each |
| `VUS` | `10` | Virtual users (concurrency) |
| `COORDINATOR_ADDR` | `127.0.0.1:10162` | `mr-coordinator` |
| `INGESTION_ADDR` | `127.0.0.1:50051` | `mr-client` / `ingestion_engine` |
| `EVALUATOR_ADDR` | `127.0.0.1:50052` | `core_module` evaluator |
| `PRESENTATION_ADDR` | `127.0.0.1:8081` | presentation HTTP |
| `API_TIMEOUT_MS` | `50` | Server-side timeout (both Rust services read at request time) |

See `openapi.yaml` for spec and `curl.sh` for manual repro.

## 3. Metrics & Thresholds

```js
grpc_req_duration: ["p(95)<300", "p(99)<500"] // gRPC
http_req_duration: ["p(95)<300", "p(99)<500"] // HTTP (50ms timeout, p95 may hit timeout)
submit_job_errors: ["rate<0.002"]
ingest_errors: ["rate<0.002"]
submit_jobs_errors: ["rate<0.002"]
evaluate_errors: ["rate<0.002"]
benefits_errors: ["rate<0.002"] // 500s
benefits_timeouts: ["rate<0.5"] // 408s allowed up to 50% under 50ms load
```

Custom `Rate`/`Trend`:
- `ingest_duration`, `evaluate_duration`, `benefits_duration` – `Trend` in ms
- `*_errors` – `1` on failure (excluding `DEADLINE_EXCEEDED`/`408` which is expected with 50ms)
- `benefits_timeouts` – `1` on `408`/`DEADLINE_EXCEEDED`

Timeouts are **not** counted as `*_errors` (they are expected with 50ms under load/cold pool).

## 4. How to run

### Prereq
- `docker compose --env-file .env -f infra/database/docker-compose.yml up -d` (MySQL 9.7.2)
- `cargo build` (or `make build`)
- Start services (separate terminals or `setsid -f`):
  ```bash
  ./target/debug/mr-coordinator &
  ./target/debug/mr-worker &
  API_TIMEOUT_MS=50 PRESENTATION_ADDR=127.0.0.1:8081 ./target/debug/presentation &
  API_TIMEOUT_MS=50 INGESTION_ADDR=127.0.0.1:50051 ./target/debug/client &
  API_TIMEOUT_MS=50 EVALUATOR_ADDR=127.0.0.1:50052 ./target/debug/evaluator &
  ```
  Or `make run-ingestion` / `cargo run --bin presentation` etc.

### k6
```bash
# install: https://k6.io/docs/getting-started/installation/
k6 run load_test.js                                  # default 1000 hits each, VUS 10
k6 run --env TOTAL_JOBS=100 --env VUS=1 load_test.js # smoke
k6 run --env TOTAL_JOBS=1000 --env VUS=50 load_test.js # stress

# with custom addrs
k6 run --env COORDINATOR_ADDR=127.0.0.1:10162 --env INGESTION_ADDR=127.0.0.1:50051 load_test.js

# via env file
API_TIMEOUT_MS=50 k6 run load_test.js
```

### Manual (same payloads)
```bash
bash curl.sh # runs curl + grpcurl for all 5 endpoints + timeout demo
# or grpcurl directly:
grpcurl -plaintext -d '{"job":[{"evaluator_id":1,"input":[{"key":"data_transaction_widow","entity":"transaction","time_range":{"type":"between","start":"2026-09-01T00:00:00","end":"2026-09-07T23:59:59"}}]}]}' 127.0.0.1:50051 client.Client/Ingest
grpcurl -plaintext -d '{"job":[{"evaluator_id":1,"input":[{"key":"data_transaction_today","entity":"transaction","time_range":{"type":"named","value":"today"}}]}]}' 127.0.0.1:50051 client.Client/SubmitJobs
grpcurl -plaintext -d '{"evaluator_id":1,"customer":{"customer_id":101},"transaction":{"transaction_today":{"sum":5000000000,"avg":1250000000,"count":4}},"job_id":999}' 127.0.0.1:50052 evaluator.Evaluator/Evaluate
curl --noproxy "*" "http://127.0.0.1:8081/benefits?customer_id=101&evaluator_id=1"
```

## 5. Design notes

- **1000 hits each**: `options.iterations = TOTAL_JOBS` and `default()` calls all 5 tests per iteration → deterministic 1000 hits per RPC regardless of VU.
- **Cold pool**: first presentation request often `408` with 50ms (MySQL pool acquire ~60-80ms). Subsequent reuse <30ms → `200`. `benefits_timeouts` threshold `0.5` allows warmup.
- **Idempotency**: `evaluator` inserts `benefit` with `idempotency_key=SHA256(customer_id-job_id-YYYY-MM-DD)` (`VARCHAR(255) UNIQUE`), duplicate `1062` is logged not failed, file `data/output/benefitYYYYMMDD.txt` appends with `\n` if exists (see `append_benefit_file`).
- **Value raw**: presentation returns `value` as `Option<Value>` without parsing – checked in load test via `JSON.parse`.
- **OpenAPI**: `openapi.yaml` documents `408` for all paths (`API_TIMEOUT_MS=50ms`).

## 6. Troubleshooting

- `Failed to connect 127.0.0.1:10162` → `mr-coordinator` not running (`--noproxy` not needed for gRPC)
- `request timeout after 50ms` / `DEADLINE_EXCEEDED` → expected under load or cold start; increase `API_TIMEOUT_MS=500` for stable run: `API_TIMEOUT_MS=500 ./target/debug/presentation &`
- `grpcurl: command not found` → `go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest`
- `k6: command not found` → `brew install k6` / `snap install k6`
- MySQL `Can't connect` → `docker compose up -d`, check `.env` `MYSQL_*`

## 7. Files

- `load_test.js` – k6 script (this doc)
- `openapi.yaml` + `crates/*/openapi.yaml` – OpenAPI 3.0.3 spec (includes 408)
- `curl.sh` – curl/grpcurl repro for all endpoints
- `crates/*/src/application/*/mod.rs` – `api_timeout()` + `tokio::time::timeout` impl
- `data/output/benefitYYYYMMDD.txt` – evaluator file output (newline handling)

## 8. Example output

```bash
$ k6 run --env TOTAL_JOBS=10 load_test.js
...
  grpc_req_duration..............: avg=45ms p(95)=120ms p(99)=210ms
  http_req_duration..............: avg=28ms p(95)=48ms
  submit_job_errors..............: 0.00%
  ingest_errors..................: 0.00%
  submit_jobs_errors.............: 0.00%
  evaluate_errors................: 0.00%
  benefits_timeouts..............: 10.00%
...
```
