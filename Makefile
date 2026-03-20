.PHONY: build clean test test-short test-coverage run install check setup-hooks

# Build variables
BINARY_NAME=dmt
VERSION?=$(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME=$(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS=-ldflags "-s -w -X github.com/johndauphine/dmt/internal/version.Version=$(VERSION)"

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod

all: build

build:
	$(GOBUILD) $(LDFLAGS) -o $(BINARY_NAME) ./cmd/migrate

build-linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BINARY_NAME)-linux-amd64 ./cmd/migrate

build-darwin:
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BINARY_NAME)-darwin-amd64 ./cmd/migrate
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 $(GOBUILD) $(LDFLAGS) -o $(BINARY_NAME)-darwin-arm64 ./cmd/migrate

build-windows:
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BINARY_NAME)-windows-amd64.exe ./cmd/migrate

build-all: build-linux build-darwin build-windows

clean:
	rm -f $(BINARY_NAME)
	rm -f $(BINARY_NAME)-*

test:
	$(GOTEST) -v ./...

test-short:
	$(GOTEST) ./... -short

test-coverage:
	$(GOTEST) ./... -coverprofile=coverage.out
	$(GOCMD) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

deps:
	$(GOMOD) download
	$(GOMOD) tidy

install: build
	cp $(BINARY_NAME) $(GOPATH)/bin/

run: build
	./$(BINARY_NAME) run --config config.yaml

# Development helpers
fmt:
	$(GOCMD) fmt ./...

lint:
	golangci-lint run

# Docker test databases
# Data directories persist across container recreations
MSSQL_DATA_DIR=$(HOME)/docker-data/mssql
PG_DATA_DIR=$(HOME)/docker-data/postgres
MSSQL_BENCH_DIR=$(HOME)/docker-data/mssql-bench
PG_BENCH_DIR=$(HOME)/docker-data/postgres-bench
MSSQL_TARGET_DIR=$(HOME)/docker-data/mssql-target
MYSQL_BENCH_DIR=$(HOME)/docker-data/mysql-bench

test-dbs-up:
	@mkdir -p $(MSSQL_DATA_DIR) $(PG_DATA_DIR)
	docker run -d --name mssql-test \
		-e 'ACCEPT_EULA=Y' \
		-e 'SA_PASSWORD=TestPass2024' \
		-v $(MSSQL_DATA_DIR):/var/opt/mssql \
		-p 1433:1433 \
		mcr.microsoft.com/mssql/server:2022-latest
	docker run -d --name pg-test \
		-e 'POSTGRES_PASSWORD=TestPass2024' \
		-v $(PG_DATA_DIR):/var/lib/postgresql/data \
		-p 5432:5432 \
		postgres:16-alpine

# Performance-tuned containers for benchmarking (NOT production-safe: fsync=off)
# Tuned for 8GB Docker RAM (proven optimal on M5 Pro/24GB, same principle here):
#   MSSQL: 4GB buffer pool — matches M5 Pro sweet spot
#   PostgreSQL: 1GB shared_buffers + aggressive WAL settings
#   ~3GB headroom for container OS, WAL files, page cache
# Host retains remaining RAM for DMT pipeline buffers + OS
bench-dbs-up:
	@mkdir -p $(MSSQL_BENCH_DIR) $(PG_BENCH_DIR)
	docker run -d --name mssql-bench \
		-e 'ACCEPT_EULA=Y' \
		-e 'SA_PASSWORD=TestPass2024' \
		-e 'MSSQL_MEMORY_LIMIT_MB=4096' \
		-v $(MSSQL_BENCH_DIR):/var/opt/mssql \
		-p 1433:1433 \
		mcr.microsoft.com/mssql/server:2022-latest
	docker run -d --name pg-bench \
		-e 'POSTGRES_PASSWORD=TestPass2024' \
		-v $(PG_BENCH_DIR):/var/lib/postgresql/data \
		--shm-size=2g \
		-p 5432:5432 \
		postgres:16-alpine \
		-c shared_buffers=1GB \
		-c effective_cache_size=4GB \
		-c work_mem=256MB \
		-c maintenance_work_mem=512MB \
		-c wal_buffers=64MB \
		-c max_wal_size=4GB \
		-c min_wal_size=1GB \
		-c checkpoint_completion_target=0.9 \
		-c checkpoint_timeout=30min \
		-c wal_level=minimal \
		-c max_wal_senders=0 \
		-c synchronous_commit=off \
		-c fsync=off \
		-c max_connections=200 \
		-c random_page_cost=1.1 \
		-c effective_io_concurrency=200 \
		-c huge_pages=off

test-dbs-down:
	docker rm -f mssql-test pg-test 2>/dev/null || true

# Second MSSQL instance for MSSQL→MSSQL testing (port 1434)
mssql-target-up:
	@mkdir -p $(MSSQL_TARGET_DIR)
	docker run -d --name mssql-target \
		-e 'ACCEPT_EULA=Y' \
		-e 'SA_PASSWORD=TestPass2024' \
		-e 'MSSQL_MEMORY_LIMIT_MB=4096' \
		-v $(MSSQL_TARGET_DIR):/var/opt/mssql \
		-p 1434:1433 \
		mcr.microsoft.com/mssql/server:2022-latest

mssql-target-down:
	docker rm -f mssql-target 2>/dev/null || true

# MySQL instance for cross-engine testing (port 3306)
mysql-bench-up:
	@mkdir -p $(MYSQL_BENCH_DIR)
	docker run -d --name mysql-bench \
		-e 'MYSQL_ROOT_PASSWORD=TestPass2024' \
		-v $(MYSQL_BENCH_DIR):/var/lib/mysql \
		--shm-size=1g \
		-p 3306:3306 \
		mysql:8.0 \
		--innodb-buffer-pool-size=256M \
		--innodb-redo-log-capacity=256M \
		--innodb-flush-log-at-trx-commit=0 \
		--innodb-flush-method=O_DIRECT \
		--innodb-doublewrite=0 \
		--max-connections=200 \
		--skip-log-bin

mysql-bench-down:
	docker rm -f mysql-bench 2>/dev/null || true

# All bench containers (MSSQL source + PG + MSSQL target + MySQL)
bench-all-up: bench-dbs-up mssql-target-up mysql-bench-up
bench-all-down: bench-dbs-down mssql-target-down mysql-bench-down

bench-dbs-down:
	docker rm -f mssql-bench pg-bench 2>/dev/null || true

# Pre-commit hooks
setup-hooks:
	git config core.hooksPath .githooks
	chmod +x .githooks/pre-commit
	@echo "Git hooks configured to use .githooks directory"

# Run all checks (useful for CI)
check: fmt test
	@echo "All checks passed"
