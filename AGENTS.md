## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

# AGENTS.md — Project Structure Guide

> This document is the canonical map for AI agents and new contributors.
> Read it before editing code or scaffolding new features.

## 0. TL;DR

```
bmic-cop = Cargo workspace (resolver v2)
  ├── crates/pkg            # shared library: config, logging, MySQL, proto/gRPC
  ├── crates/ingestion_engine # MapReduce ingestion: coordinator + worker + SQL persistence
  ├── crates/core_module    # Business-rule evaluation domain (stub)
  ├── proto/ingestion_engine # .proto sources (compiled via build.rs → crates/pkg/src/proto)
  ├── data/                 # sample transaction/customer .txt files fed to workers
  ├── infra/database        # docker-compose for MySQL 9.7.2
  ├── migrations/           # SQL migrations (business_rule / datamart)
  └── Makefile / .env        # run helpers and env defaults
```

Two runtime processes do the heavy lifting:
`mr-coordinator` (gRPC server on `127.0.0.1:10162`) ↔ `mr-worker` (gRPC client + heartbeat).

---

## 1. Workspace Root

| Path | Purpose |
|------|---------|
| `Cargo.toml` | Workspace definition: `members = ["crates/*"]`. Centralizes `workspace.dependencies` (tokio, tonic, prost, sqlx, serde, chrono). |
| `Cargo.lock` | Locked dependency tree (check in). |
| `build.rs` | Proto codegen: scans `crates/pkg/proto/ingestion_engine/*.proto`, runs `tonic-prost-build`/`prost-build`, emits to `crates/pkg/src/proto/*.rs`. Re-run on `cargo build`. |
| `Makefile` | Shortcuts: `run-core`, `run-ingestion`, `test-core`, `test-ingestion`, `build`, `db-up/down/restart` (delegates to `infra/database/docker-compose.yml`). |
| `.env` | Default env for MySQL (`MYSQL_*`), Coordinator (`COORDINATOR_HOST/PORT`, `INITIAL_WORKER_ID`, `TASK_TIMEOUT_SECS`), Worker (`BUF_SIZE`, `WAIT_TIME_MS`, `HEARTBEAT_INTERVAL_MS`, `INITIAL_WORKER_PORT`). Consumed via `dotenvy` in `*Config::from_env()`. |
| `infra/database/docker-compose.yml` | Single `db` service `mysql:9.7.2` on `3306`, volume `mysql_data`. |
| `data/` | Input fixtures: `data/transaction/transactionYYYYMMDD.txt` (CSV `id,ref_id,timestamp,amount,type,description`) and `data/customer/customer.txt`. Workers read these after `GetTask`. |
| `target/` | Build artifacts; `target/debug/mr-coordinator`, `mr-worker` binaries. |

---

## 2. Crate: `pkg` — Shared Kernel (`crates/pkg`)

`pkg` is the **only cross-cutting dependency**. Both `ingestion_engine` and `core_module` depend on it via `path = "../pkg"`. No circular deps.

```
crates/pkg/
  Cargo.toml          # depends on tonic, prost, sqlx, tokio, env_logger/log
  src/lib.rs          # re-exports: pub mod config; pub mod log; pub mod mysql; pub mod proto;
  src/config/
    mod.rs            # re-exports
    coordinator.rs    # CoordinatorConfig {host, port, initial_worker_id, initial_job_id, task_timeout_secs, coordinator_startup_ms} + from_env() + addr()
    worker.rs         # WorkerConfig + constants: BUF_SIZE, WAIT_TIME_MS, HEARTBEAT_INTERVAL_MS, INITIAL_WORKER_PORT, etc.
    mysql.rs          # MysqlConfig + DSN
  src/log/mod.rs      # wrapper: `pub use log::{info,warn,error,...}` in prod, `println` in `#[cfg(test)]`; `init_logger()` via env_logger
  src/mysql/mod.rs    # MysqlBuilder (connect_lazy to datamart/business_rule DBs), DbPool = Arc<MySqlPool>, ping()
  src/proto/
    mod.rs            # pub mod coordinator; pub mod worker;
    coordinator.rs    # @generated — Prost messages + CoordinatorClient/Server (Heartbeat, Register, GetTask, FinishTask)
    worker.rs         # @generated — worker service (stub)
  proto/ingestion_engine/
    coordinator.proto # service Coordinator {Heartbeat, Register, GetTask, FinishTask}
    worker.proto
    client.proto
```

**Key design:** `build.rs` at repo root compiles protos into `crates/pkg/src/proto/` — do not hand-edit `coordinator.rs` there; regenerate.

---

## 3. Crate: `ingestion_engine` — MapReduce Ingestion (`crates/ingestion_engine`)

Primary business crate. Implements a coordinator/worker MapReduce pattern for ingesting transaction files into MySQL, plus SQL persistence layer.

```
crates/ingestion_engine/
  Cargo.toml  # bins: [[bin]] mr-coordinator → src/bin/coordinator.rs, mr-worker → src/bin/worker.rs
  src/
    lib.rs                # pub mod application; pub mod interface; pub mod internal;
    lupamain.rs           # legacy entry (unused; use bins)
    bin/
      coordinator.rs      # #[tokio::main] → CoordinatorConfig::from_env() → log::init_logger() → application::coordinator::start()
      worker.rs           # same for Worker (needs both CoordinatorConfig + WorkerConfig); DO NOT MODIFY header comment
      client.rs           # stub client (currently empty main)
    application/
      mod.rs              # pub mod coordinator; pub mod worker;
      coordinator/mod.rs  # Coordinator struct, Inner {jobs, workers, next_job_id}, TaskState/TaskRecord/Job, impl Coordinator for tonic (heartbeat, register, get_task, finish_task), pub async fn start()
      worker/mod.rs       # Worker {id, client, inner:WorkerState, worker_config}, WorkerState {data: HashMap<jobId, HashMap<(map,reduce), Bytes>>}, impl Worker {new(), handle_task(), read_and_log_file(), parse_transaction_line(), run(), start()} + ParsedTransaction + tests
    interface/
      mod.rs              # pub mod repository; pub mod service;
      repository/
        mod.rs / common.rs / job.rs / job_detail.rs / job_file.rs  # async traits: JobRepository, JobDetailRepository, JobFileRepository + Filter structs
      service/mod.rs
    internal/
      mod.rs              # pub mod repository; pub mod service;
      repository/
        mod.rs
        model/             # JobModel, JobDetailModel, JobFileModel (+ .test.rs, ModelsCommon impl)
          mod.rs / job.rs / job_detail.rs / job_file.rs
        sql/
          mod.rs
          job/             # implementation.rs (sqlx queries), list.test.rs, mod.rs
          job_detail/      # init.rs, list.rs, list.test.rs, mod.rs
          job_file/        # implementation.rs, list.test.rs, mod.rs
      service/mod.rs
```

### 3.1 Coordinator Deep Dive (`application/coordinator/mod.rs`)

- State: `Inner { next_job_id, next_worker_id, jobs: HashMap<u32,Job>, job_order: VecDeque<u32>, workers: HashMap<u32,Instant> }` behind `Arc<Mutex<Inner>>`.
- Lifecycle: `Coordinator::new()` → `start()` builds `CoordinatorServer` via `tonic::transport::Server`.
- RPCs:
  - `register()` — assigns `worker_id`, records liveness `Instant::now()`, logs via `pkg::log`.
  - `heartbeat()` — refreshes worker liveness.
  - `get_task()` — ensures worker known, sleeps **1s** (`tokio::time::sleep(Duration::from_secs(1))`) then returns `GetTaskReply { wait:false, job_id:0, file:"../../../data/transaction/transaction20260914.txt", output_dir:"../../../data/output/transaction.txt" }`. Commented-out logic shows original MapReduce scheduling (map_tasks/reduce_tasks, crash recovery via `is_worker_alive/crashed`).
  - `finish_task()` — marks reduce task completed, checks `all_done` → `job.done = true`.
- Config-driven liveness via `task_timeout_secs`.

### 3.2 Worker Deep Dive (`application/worker/mod.rs`)

- State: `WorkerState { data: HashMap<u32, HashMap<(u32,u32), Bytes>> }` (shard cache for MapReduce), `Worker { id, client: CoordinatorClient<Channel>, inner, worker_config }`.
- `Worker::new()` — connects to `http://{coordinator.addr()}`, calls `register()`.
- `handle_task(GetTaskReply)` — if `wait` sleeps `wait_time_ms`; else logs processing, sleeps **1s** (`sleeping 1s before reading file`), then `read_and_log_file(&file)`:
  - Tries `tokio::fs::read_to_string(path)` with fallbacks (`data/transaction/...`, `CARGO_MANIFEST_DIR` relative) to handle cwd variance.
  - Logs raw lines `log::info!("[i] raw: ...")` and parsed via `parse_transaction_line()` → `log::info!("parsed line -> id=..., timestamp=..., ...")` (uses `pkg::log` so console in prod, `println` in tests).
  - Returns line count; on error logs `log::error!`.
  - Always calls `finish_task(worker_id, job_id, task:1)`.
- `parse_transaction_line(line: &str) -> Option<ParsedTransaction>` — `splitn(6, ',')` to preserve commas in description; validates 6 columns.
- `run()` — spawns heartbeat loop (`heartbeat_interval_ms`) + poll loop (`get_task()` → `handle_task()`; on error sleeps `wait_time_ms`).
- `worker_server()` — stub for worker gRPC server (per-worker port `get_port(worker_id, initial_worker_port)`, addr `127.0.0.1:port`), currently no-op.
- `start()` — `Worker::new` + spawn `worker_server` + `run()`.
- Tests (`#[cfg(test)]`) — unit tests for `parse_transaction_line` (valid, comma-in-desc, invalid) and async tests for `read_and_log_file` (counts, empty).

### 3.3 Persistence Layers

Follows **interface → internal/repository** hex pattern:

- `interface/repository/*.rs` — pure async traits + Filter DTOs (no sqlx). Example `JobRepository::list/get`.
- `internal/repository/model/*.rs` — `JobModel {id, evaluator_id, status, created_at, updated_at}` with `FromRow`, `Serialize/Deserialize`, `ModelsCommon` (table name, columns, value mapping). Tests in `*.test.rs`.
- `internal/repository/sql/*` — concrete sqlx implementations (`implementation.rs`, `list.rs`, `init.rs`), tested in `list.test.rs`. Uses `pkg::mysql::DbPool`.

---

## 4. Crate: `core_module` — Rule Evaluation (`crates/core_module`)

Currently a stub/scaffold for business-rule evaluation:

```
crates/core_module/
  Cargo.toml (edition 2024, no deps declared yet — add as needed)
  src/main.rs          # Hello, world! (placeholder binary)
  src/application/
    evaluator/         # (empty)
    scheduler/mod.rs   # scheduler logic (empty)
  src/interface/
    mod.rs
    repository/common.rs, evaluator_repository.rs
  src/repository/
    mod.rs
    model/benefit.rs, evaluator.rs, rule.rs (+ .test.rs)
    sql/benefit/, evaluator/list.rs, rule/
```

Mirrors `ingestion_engine`'s layered pattern but minimal. Extend alongside `ingestion_engine` patterns.

---

## 5. Data Flow & Runtime

```
.env ──► CoordinatorConfig / WorkerConfig (from_env + defaults)
           │
           ├─► mr-coordinator (tonic gRPC server :10162)
           │     heartbeat/register/get_task/finish_task
           │     └── 1s sleep in get_task before returning file job
           │
           └─► mr-worker (tonic client)
                 register → id
                 loop: get_task → 1s sleep → read data/transaction/*.txt
                       → log raw + parsed via pkg::log (env_logger)
                       → finish_task
                 heartbeat every 2s (tokio::spawn)
                 (future: mr-worker gRPC server on 10163+id)
```

File format example (`data/transaction/transaction20260914.txt`):
`1,1,2026-09-14T03:21:15Z,15000050,Debit,Pembayaran merchant menggunakan kartu debit`
→ parsed to `ParsedTransaction {id, ref_id, timestamp, amount, tx_type, description}`.

---

## 6. Build, Run, Test

```bash
cargo build                    # builds all crates + regenerates proto via build.rs
cargo run -p core_module
cargo run --bin mr-coordinator # RUST_LOG=info to see pkg::log output
cargo run --bin mr-worker

make run-core / run-ingestion  # wrappers via .env
make test-core                 # cargo llvm-cov -p core_module
make test-ingestion            # cargo llvm-cov -p ingestion_engine --ignore-filename-regex "internal/repository/sql/(job|job_detail|job_file)/mod.rs"
make db-up / db-down / db-restart

cargo test -p ingestion_engine --lib application::worker -- --nocapture  # focused worker parsing tests
```

MySQL required for `internal/repository/sql/*` tests; `MysqlBuilder::connect_lazy` avoids needing DB at pool creation.

---

## 7. Conventions for Agents

### Think Before Coding
- State assumptions. If interpretations diverge, present options — don't pick silently.
- Prefer simplest solution; push back on over-engineering.

### Simplicity First
- Minimum code for the task. No speculative abstractions or config.
- 200 lines that could be 50 → rewrite.

### Surgical Changes
- Touch only what the request touches. Match existing style.
- Clean up only orphans *your* change created (unused imports/vars).

### Goal-Driven Execution
- Define verifiable success first: e.g. "Add validation → write invalid-input tests, make them pass".
- For multi-step tasks, plan `1. [Step] → verify: [check]` and loop until green.

### Crate-Specific Notes
- **Proto:** edit `crates/pkg/proto/ingestion_engine/*.proto`, then `cargo build` — never edit `crates/pkg/src/proto/*.rs` by hand.
- **Config:** add env vars in `.env` + defaults in `pkg/src/config/*::from_env()`.
- **Logging:** use `pkg::log::info!` (env_logger in prod, `println` in tests) with `RUST_LOG=info`.
- **SQL:** new table → `interface/repository/<table>.rs` (trait) → `internal/repository/model/<table>.rs` → `internal/repository/sql/<table>/implementation.rs` + tests, mirroring existing `job*` pattern.
- **Worker tasks:** keep `handle_task`'s 1s sleep + `read_and_log_file` fallback logic intact; tests live in `application/worker/mod.rs` `#[cfg(test)]`.

---

## 8. Gotchas

- `build.rs` expects `proto/ingestion_engine/` at crate root — path is relative to repo root; moving it breaks codegen.
- Worker file path `../../../data/...` is relative to `target/debug/` — `read_and_log_file` has fallback candidates; keep them when changing paths.
- `mr-worker` `run()` currently has a subtle `self.id` capture in the heartbeat `tokio::spawn(async move { log::info!("Worker {} sending hearbeat", self.id) })` — it compiles but moves `self.id` copy; prefer `let wid = self.id` before spawn if refactoring.
- `Coordinator::finish_task` checks `reduce_tasks` bounds but not `map_tasks` — intentional stub for demo job `job_id=0`.
- `core_module` is edition 2024 while others are 2021 — watch for edition-specific lints.
