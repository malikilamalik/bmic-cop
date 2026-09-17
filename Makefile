.PHONY: run-core run-ingestion build db-restart db-up db-down

include .env
run-core:
	cargo run -p core_module

test-core:
	cargo llvm-cov -p core_module --no-cfg-coverage

test-ingestion:
	cargo llvm-cov -p ingestion_engine --no-cfg-coverage --ignore-filename-regex "internal/repository/sql/(job|job_detail|job_file)/mod\.rs"

run-ingestion:
	cargo run -p ingestion_engine

build:
	cargo build

db-restart:
	docker compose --env-file .env -f infra/database/docker-compose.yml up -d --force-recreate

db-up:
	docker compose --env-file .env -f infra/database/docker-compose.yml up -d

db-down:
	docker compose -f infra/database/docker-compose.yml down
