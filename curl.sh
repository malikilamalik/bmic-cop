#!/usr/bin/env bash
set -e
# BMIC COP - curl / grpcurl examples
# Timeout is 50ms (API_TIMEOUT_MS) via tokio::time::timeout -> HTTP 408 / gRPC DEADLINE_EXCEEDED
# Servers: presentation 127.0.0.1:8081, ingestion 127.0.0.1:50051, evaluator 127.0.0.1:50052
# See openapi.yaml for full spec

PRESENTATION_ADDR=${PRESENTATION_ADDR:-127.0.0.1:8081}
INGESTION_ADDR=${INGESTION_ADDR:-127.0.0.1:50051}
EVALUATOR_ADDR=${EVALUATOR_ADDR:-127.0.0.1:50052}
API_TIMEOUT_MS=${API_TIMEOUT_MS:-50}

echo "=== Presentation GET /benefits (timeout ${API_TIMEOUT_MS}ms) ==="
echo "# 1. active today -> 200 with benefit (value raw)"
curl --noproxy "*" -s -w "\nHTTP %{http_code} time=%{time_total}s\n" \
  "http://${PRESENTATION_ADDR}/benefits?customer_id=101&evaluator_id=1" | head -n 20

echo
echo "# 2. active without evaluator filter -> 200"
curl --noproxy "*" -s -w "\nHTTP %{http_code}\n" \
  "http://${PRESENTATION_ADDR}/benefits?customer_id=101" | head -n 20

echo
echo "# 3. old date (2020-01-01) -> 200 [] (DATE filter)"
curl --noproxy "*" -s -w "\nHTTP %{http_code}\n" \
  "http://${PRESENTATION_ADDR}/benefits?customer_id=102" | head -n 20

echo
echo "# 4. inactive evaluator -> 403"
curl --noproxy "*" -s -i "http://${PRESENTATION_ADDR}/benefits?customer_id=104&evaluator_id=99" | head -n 20

echo
echo "# 5. evaluator not found -> 404"
curl --noproxy "*" -s -i "http://${PRESENTATION_ADDR}/benefits?customer_id=101&evaluator_id=99999" | head -n 20

echo
echo "# 6. missing customer benefit -> 200 []"
curl --noproxy "*" -s -w "\nHTTP %{http_code}\n" \
  "http://${PRESENTATION_ADDR}/benefits?customer_id=999" | head -n 20

echo
echo "# 7. timeout demo (force 1ms) -> 408"
API_TIMEOUT_MS=1 curl --noproxy "*" -s -i "http://${PRESENTATION_ADDR}/benefits?customer_id=101" 2>&1 | head -n 20 || true
# Note: server reads API_TIMEOUT_MS at request time, so restart with API_TIMEOUT_MS=1 to really test:
#   API_TIMEOUT_MS=1 PRESENTATION_ADDR=127.0.0.1:8081 ./target/debug/presentation &

echo
echo "=== Ingestion gRPC (client.Client) ==="
echo "# requires grpcurl: https://github.com/fullstorydev/grpcurl"
if command -v grpcurl >/dev/null 2>&1; then
  echo "# Ingest (primary) - data_transaction_widow between 2026-09-01..07"
  grpcurl -plaintext -d '{
    "job": [{
      "evaluator_id": 1,
      "input": [{
        "key": "data_transaction_widow",
        "entity": "transaction",
        "time_range": {"type": "between", "start": "2026-09-01T00:00:00", "end": "2026-09-07T23:59:59"}
      }]
    }]
  }' ${INGESTION_ADDR} client.Client/Ingest || echo "grpcurl Ingest failed (timeout 50ms may return DEADLINE_EXCEEDED)"

  echo
  echo "# SubmitJobs (alias) - data_transaction_today named today"
  grpcurl -plaintext -d '{
    "job": [{
      "evaluator_id": 1,
      "input": [{
        "key": "data_transaction_today",
        "entity": "transaction",
        "time_range": {"type": "named", "value": "today"}
      }]
    }]
  }' ${INGESTION_ADDR} client.Client/SubmitJobs || echo "grpcurl SubmitJobs failed"

  echo
  echo "# Customer entity -> customer.txt"
  grpcurl -plaintext -d '{
    "job": [{
      "evaluator_id": 2,
      "input": [{
        "key": "data_customer_today",
        "entity": "customer",
        "time_range": {"type": "named", "value": "today"}
      }]
    }]
  }' ${INGESTION_ADDR} client.Client/Ingest || true
else
  echo "grpcurl not found, use:"
  echo "  grpcurl -plaintext -d '{\"job\":[{\"evaluator_id\":1,\"input\":[{\"key\":\"data_transaction_widow\",\"entity\":\"transaction\",\"time_range\":{\"type\":\"between\",\"start\":\"2026-09-01T00:00:00\",\"end\":\"2026-09-07T23:59:59\"}}]}]}' ${INGESTION_ADDR} client.Client/Ingest"
  echo "  grpcurl -plaintext -d '{\"job\":[{\"evaluator_id\":1,\"input\":[{\"key\":\"data_transaction_today\",\"entity\":\"transaction\",\"time_range\":{\"type\":\"named\",\"value\":\"today\"}}]}]}' ${INGESTION_ADDR} client.Client/SubmitJobs"
fi

echo
echo "=== Evaluator gRPC (evaluator.Evaluator) ==="
echo "# rpc Evaluate(EvaluateRequest) returns (EvaluateReply) - timeout 50ms"
if command -v grpcurl >/dev/null 2>&1; then
  grpcurl -plaintext -d '{
    "evaluator_id": 1,
    "customer": {"customer_id": 101},
    "transaction": {"transaction_today": {"sum": 5000000000, "avg": 1250000000, "count": 4}},
    "job_id": 999
  }' ${EVALUATOR_ADDR} evaluator.Evaluator/Evaluate || echo "grpcurl Evaluate failed (may be DEADLINE_EXCEEDED with 50ms)"
else
  echo "grpcurl not found, use:"
  echo "  grpcurl -plaintext -d '{\"evaluator_id\":1,\"customer\":{\"customer_id\":101},\"transaction\":{\"transaction_today\":{\"sum\":5000000000,\"avg\":1250000000,\"count\":4}},\"job_id\":999}' ${EVALUATOR_ADDR} evaluator.Evaluator/Evaluate"
fi

echo
echo "=== Load test (1000 hits each) ==="
echo "k6 run --env TOTAL_JOBS=1000 --env VUS=10 load_test.js"
echo "  hits: SubmitJob 1000, Ingest 1000, SubmitJobs 1000, Evaluate 1000, GET /benefits 1000*4 cases"
echo "  thresholds: grpc p95<300 p99<500, http p95<50 (matches 50ms timeout)"
