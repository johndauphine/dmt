.PHONY: build clean test test-short test-coverage run install check setup-hooks \
        load-fixture-pgbench load-fixture-so2010-minimal test-fixtures-load

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

# Docker test/bench databases
# IMPORTANT (Docker Desktop on macOS/Windows): Use named volumes, not bind mounts.
# Named volumes use VM-internal ext4 (~4.5 GB/s writes).
# Bind mounts go through VirtioFS (~1.5 GB/s) — 3x slower, kills throughput.
# On native Linux, bind mounts use host ext4/xfs directly and don't have this penalty.
# To remove named volumes: docker volume rm mssql-test-data pg-test-data mssql-bench-data pg-bench-data

test-dbs-up:
	docker run -d --name mssql-test \
		--user root \
		-e 'ACCEPT_EULA=Y' \
		-e 'SA_PASSWORD=TestPass2024' \
		-v mssql-test-data:/var/opt/mssql \
		-p 1433:1433 \
		mcr.microsoft.com/mssql/server:2022-latest
	docker run -d --name pg-test \
		-e 'POSTGRES_PASSWORD=TestPass2024' \
		-v pg-test-data:/var/lib/postgresql/data \
		-p 5432:5432 \
		postgres:16-alpine

# Performance-tuned containers for benchmarking (NOT production-safe: fsync=off)
# Tuned for 8GB Docker RAM (proven optimal on M5 Pro/24GB, same principle here):
#   MSSQL: 4GB buffer pool — matches M5 Pro sweet spot
#   PostgreSQL: 1GB shared_buffers + aggressive WAL settings
#   ~3GB headroom for container OS, WAL files, page cache
# Host retains remaining RAM for DMT pipeline buffers + OS
bench-dbs-up:
	docker run -d --name mssql-bench \
		--user root \
		-e 'ACCEPT_EULA=Y' \
		-e 'SA_PASSWORD=TestPass2024' \
		-e 'MSSQL_MEMORY_LIMIT_MB=4096' \
		-v mssql-bench-data:/var/opt/mssql \
		-p 1433:1433 \
		mcr.microsoft.com/mssql/server:2022-latest
	docker run -d --name pg-bench \
		-e 'POSTGRES_PASSWORD=TestPass2024' \
		-v pg-bench-data:/var/lib/postgresql/data \
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
	docker run -d --name mssql-target \
		--user root \
		-e 'ACCEPT_EULA=Y' \
		-e 'SA_PASSWORD=TestPass2024' \
		-e 'MSSQL_MEMORY_LIMIT_MB=4096' \
		-v mssql-target-data:/var/opt/mssql \
		-p 1434:1433 \
		mcr.microsoft.com/mssql/server:2022-latest

mssql-target-down:
	docker rm -f mssql-target 2>/dev/null || true

# MySQL instance for cross-engine testing (port 3306)
mysql-bench-up:
	docker run -d --name mysql-bench \
		-e 'MYSQL_ROOT_PASSWORD=TestPass2024' \
		-v mysql-bench-data:/var/lib/mysql \
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

# ----------------------------------------------------------------------
# Fixture loaders (#178)
# ----------------------------------------------------------------------
# CI-friendly fixtures that populate the running test/bench containers
# with reproducible data. Each loader is idempotent (drops+recreates)
# and completes in seconds. See docs/FIXTURES.md for the full inventory
# (including the full SO2010/SO2013/WWI .bak procedures that aren't
# scriptable here without large downloads).

# pgbench: scale-1 PostgreSQL TPC-B-like fixture (~16 MB, <1 s to load).
# Override scale with FIXTURE_SCALE=10 etc. Targets pg-test by default,
# falls back to pg-bench.
load-fixture-pgbench:
	./scripts/load-fixture-pgbench.sh

# SO2010-minimal: synthesized DDL + tiny seed for the StackOverflow2010
# schema (~30 rows total, 9 tables). Covers the type-mapping surface
# without the 10 GB .bak download.
load-fixture-so2010-minimal:
	./scripts/load-fixture-so2010-minimal.sh

# Convenience: load every CI-friendly fixture in one shot. SO2013 and
# WWI are explicitly excluded — they require a manual .bak restore;
# see docs/FIXTURES.md for the procedure.
test-fixtures-load: load-fixture-pgbench load-fixture-so2010-minimal
	@echo ""
	@echo "All CI-friendly fixtures loaded."
	@echo "For full SO2010/SO2013/WWI bench fixtures, see docs/FIXTURES.md"

# Pre-commit hooks
setup-hooks:
	git config core.hooksPath .githooks
	chmod +x .githooks/pre-commit
	@echo "Git hooks configured to use .githooks directory"

# Run all checks (useful for CI)
check: fmt test
	@echo "All checks passed"
