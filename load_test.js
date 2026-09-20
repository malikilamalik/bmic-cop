/**
 * BMIC COP Load Test - Detailed
 * ===============================
 * Hits all 5 entry points with 1000 hits each (TOTAL_JOBS=1000):
 *  1. coordinator.Coordinator/SubmitJob (10162)
 *  2. client.Client/Ingest          (50051) - also 50ms timeout
 *  3. client.Client/SubmitJobs      (50051) - alias, 50ms
 *  4. evaluator.Evaluator/Evaluate  (50052) - 50ms, idempotency SHA256(customer-job-date)
 *  5. GET /benefits?customer_id=&evaluator_id= (8081) - 4 cases + 1 extra per iteration, also 50ms
 *
 * Usage:
 *   k6 run load_test.js
 *   k6 run --env TOTAL_JOBS=100 --env VUS=1 load_test.js
 *   k6 run --env TOTAL_JOBS=1000 --env VUS=10 load_test.js  # 1000 hits each, ~5000 reqs
 *   TOTAL_JOBS=1000 k6 run load_test.js
 *
 * Env: TOTAL_JOBS, VUS, COORDINATOR_ADDR, INGESTION_ADDR, EVALUATOR_ADDR, PRESENTATION_ADDR
 * Servers read API_TIMEOUT_MS=50 at request time (tokio::time::timeout -> 408/DEADLINE_EXCEEDED)
 * See LOAD_TEST.md for thresholds, metrics, troubleshooting and openapi.yaml/curl.sh for manual repro.
 * Each iteration calls all 5 services, so iterations=TOTAL_JOBS => 1000 hits per RPC.
 */
import grpc from "k6/net/grpc";
import http from "k6/http";
import { check, sleep } from "k6";
import { Rate, Trend } from "k6/metrics";

const coordinatorClient = new grpc.Client();
const ingestionClient = new grpc.Client();
const evaluatorClient = new grpc.Client();

coordinatorClient.load(["crates/pkg/proto/ingestion_engine"], "coordinator.proto");
ingestionClient.load(["crates/pkg/proto/ingestion_engine"], "client.proto");
evaluatorClient.load(["crates/pkg/proto/core_module"], "evaluator.proto");

// TOTAL_JOBS=1000 means 1000 hits each service (coordinator, ingestion Ingest, ingestion SubmitJobs, evaluator Evaluate, presentation)
const TOTAL_JOBS = Number(__ENV.TOTAL_JOBS || 1000);
const INGESTION_ADDR = __ENV.INGESTION_ADDR || "127.0.0.1:50051";
const COORDINATOR_ADDR = __ENV.COORDINATOR_ADDR || "127.0.0.1:10162";
const EVALUATOR_ADDR = __ENV.EVALUATOR_ADDR || "127.0.0.1:50052";
const PRESENTATION_ADDR = __ENV.PRESENTATION_ADDR || "127.0.0.1:8081";
const PRESENTATION_URL = `http://${PRESENTATION_ADDR}/benefits`;

// Custom metrics
const submitJobErrors = new Rate("submit_job_errors");
const ingestErrors = new Rate("ingest_errors");
const submitJobsErrors = new Rate("submit_jobs_errors");
const evaluateErrors = new Rate("evaluate_errors");
const benefitsErrors = new Rate("benefits_errors");
const benefitsTimeouts = new Rate("benefits_timeouts");
const ingestDuration = new Trend("ingest_duration");
const evaluateDuration = new Trend("evaluate_duration");
const benefitsDuration = new Trend("benefits_duration");

export const options = {
  // 1000 hits each: 1000 iterations * 1 VU, each iteration hits all 5 RPCs
  vus: Number(__ENV.VUS || 10),
  iterations: TOTAL_JOBS,
  thresholds: {
    grpc_req_duration: ["p(95)<300", "p(99)<500"],
    http_req_duration: ["p(95)<300", "p(99)<500"],
    submit_job_errors: ["rate<0.002"],
    ingest_errors: ["rate<0.002"],
    submit_jobs_errors: ["rate<0.002"],
    evaluate_errors: ["rate<0.002"],
    benefits_errors: ["rate<0.002"],
    benefits_timeouts: ["rate<0.5"],
  },
};

function testCoordinator() {
  coordinatorClient.connect(COORDINATOR_ADDR, { plaintext: true });
  const response = coordinatorClient.invoke("coordinator.Coordinator/SubmitJob", {
    files: ["transaction20260914.txt"],
    output_dir: "/data/output",
    app: "benefit-evaluator",
    n_reduce: 4,
    key: "customer_id",
    entity: "transaction",
  });
  const success = response && response.status === grpc.StatusOK && response.message?.jobId !== undefined;
  submitJobErrors.add(!success);
  check(response, {
    "SubmitJob status OK": (r) => r && r.status === grpc.StatusOK,
    "jobId exists": (r) => r && r.message?.jobId !== undefined,
  });
  coordinatorClient.close();
}

function testIngest() {
  ingestionClient.connect(INGESTION_ADDR, { plaintext: true });
  const start = Date.now();
  const payload = {
    job: [
      {
        evaluatorId: 1,
        input: [
          {
            key: "data_transaction_widow",
            entity: "transaction",
            timeRange: { type: "between", start: "2026-09-01T00:00:00", end: "2026-09-07T23:59:59" },
          },
        ],
      },
    ],
    jobs: [],
  };
  const response = ingestionClient.invoke("client.Client/Ingest", payload);
  const duration = Date.now() - start;
  ingestDuration.add(duration);
  const isTimeout = response && response.status === grpc.StatusDeadlineExceeded;
  const success = response && response.status === grpc.StatusOK && Array.isArray(response.message?.jobIds);
  ingestErrors.add(!success && !isTimeout);
  check(response, {
    "Ingest OK or DeadlineExceeded (50ms)": (r) => r && (r.status === grpc.StatusOK || r.status === grpc.StatusDeadlineExceeded),
    "Ingest jobIds or timeout": (r) => r && (r.message?.jobIds !== undefined || r.status === grpc.StatusDeadlineExceeded),
  });
  if (isTimeout) console.log(`Ingest timeout after ${duration}ms`);
  ingestionClient.close();
}

function testSubmitJobs() {
  ingestionClient.connect(INGESTION_ADDR, { plaintext: true });
  const payload = {
    job: [
      {
        evaluatorId: 1,
        input: [
          {
            key: "data_transaction_today",
            entity: "transaction",
            timeRange: { type: "named", value: "today" },
          },
        ],
      },
    ],
    jobs: [],
  };
  const response = ingestionClient.invoke("client.Client/SubmitJobs", payload);
  const isTimeout = response && response.status === grpc.StatusDeadlineExceeded;
  const success = response && response.status === grpc.StatusOK && Array.isArray(response.message?.jobIds);
  submitJobsErrors.add(!success && !isTimeout);
  check(response, {
    "SubmitJobs OK or DeadlineExceeded": (r) => r && (r.status === grpc.StatusOK || r.status === grpc.StatusDeadlineExceeded),
  });
  ingestionClient.close();
}

function testEvaluate() {
  evaluatorClient.connect(EVALUATOR_ADDR, { plaintext: true });
  const start = Date.now();
  // rpc Evaluate(EvaluateRequest) returns (EvaluateReply)
  const payload = {
    evaluatorId: 1,
    customer: { customerId: 101 },
    transaction: {
      transactionToday: { sum: 5000000000, avg: 1250000000, count: 4 },
    },
    jobId: 999,
  };
  const response = evaluatorClient.invoke("evaluator.Evaluator/Evaluate", payload);
  const duration = Date.now() - start;
  evaluateDuration.add(duration);
  const isTimeout = response && response.status === grpc.StatusDeadlineExceeded;
  const success = response && response.status === grpc.StatusOK && response.message?.resultJson !== undefined;
  evaluateErrors.add(!success && !isTimeout);
  check(response, {
    "Evaluate OK or DeadlineExceeded (50ms)": (r) => r && (r.status === grpc.StatusOK || r.status === grpc.StatusDeadlineExceeded),
    "Evaluate resultJson or timeout": (r) => r && (r.message?.resultJson !== undefined || r.status === grpc.StatusDeadlineExceeded),
  });
  if (isTimeout) console.log(`Evaluate timeout after ${duration}ms`);
  evaluatorClient.close();
}

function testPresentation() {
  const cases = [
    { customer_id: 101, evaluator_id: 1, expect: 200, desc: "active today" },
    { customer_id: 102, expect: 200, desc: "old date -> []" },
    { customer_id: 104, evaluator_id: 99, expect: 403, desc: "inactive" },
    { customer_id: 999, expect: 200, desc: "no benefit -> []" },
  ];
  for (const c of cases) {
    let url = `${PRESENTATION_URL}?customer_id=${c.customer_id}`;
    if (c.evaluator_id) url += `&evaluator_id=${c.evaluator_id}`;
    const start = Date.now();
    const res = http.get(url, { timeout: "60s" });
    const duration = Date.now() - start;
    benefitsDuration.add(duration);
    const isTimeout = res.status === 408;
    const isError = res.status >= 500;
    benefitsTimeouts.add(isTimeout ? 1 : 0);
    benefitsErrors.add(isError ? 1 : 0);
    check(res, {
      [`benefits ${c.desc} ${c.expect} or 408`]: (r) => r.status === c.expect || r.status === 408,
      [`benefits ${c.desc} not 500`]: (r) => r.status !== 500,
    });
    if (c.customer_id === 101 && res.status === 200) {
      try {
        const body = JSON.parse(res.body);
        check(body, {
          "benefits array": (b) => Array.isArray(b),
        });
      } catch (e) {}
    }
  }
}

export default function () {
  // Each iteration hits all services => 1000 hits each when TOTAL_JOBS=1000
  testCoordinator();
  testIngest();
  testSubmitJobs();
  testEvaluate();
  testPresentation();
  sleep(0.05);
}

export function handleSummary(data) {
  return {
    stdout: JSON.stringify(
      {
        total_jobs: TOTAL_JOBS,
        coordinator_p95: data.metrics.grpc_req_duration?.values["p(95)"],
        http_p95: data.metrics.http_req_duration?.values["p(95)"],
        ingest_errors: data.metrics.ingest_errors?.values.rate,
        submit_jobs_errors: data.metrics.submit_jobs_errors?.values.rate,
        evaluate_errors: data.metrics.evaluate_errors?.values.rate,
        benefits_errors: data.metrics.benefits_errors?.values.rate,
        timeouts: data.metrics.benefits_timeouts?.values.rate,
      },
      null,
      2
    ),
  };
}
