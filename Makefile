# proxyfarm — Makefile
BINS    := fetcher coordinator api worker
BIN_DIR := bin
GO      ?= go
COMPOSE := docker compose -f deploy/compose/docker-compose.yml

.PHONY: all build test lint vet tidy clean migrate seed compose-up compose-down k8s-build run-worker-dry

all: build

build:
	@mkdir -p $(BIN_DIR)
	@for b in $(BINS); do \
		$(GO) build -trimpath -ldflags "-s -w" -o $(BIN_DIR)/$$b ./cmd/$$b || exit 1; \
	done
	@echo "built: $(BINS)"

test:
	$(GO) test -count=1 ./...

lint:
	@command -v golangci-lint >/dev/null || { echo "golangci-lint not installed (https://golangci-lint.run)"; exit 1; }
	golangci-lint run ./...

vet:
	$(GO) vet ./...

tidy:
	$(GO) mod tidy

clean:
	rm -rf $(BIN_DIR)

# apply migrations + seed sources against PG_DSN (default local compose pg)
migrate:
	PG_DSN="postgres://proxyfarm:proxyfarm@localhost:5432/proxyfarm?sslmode=disable" \
		$(GO) run ./cmd/coordinator --migrate

seed:
	PG_DSN="postgres://proxyfarm:proxyfarm@localhost:5432/proxyfarm?sslmode=disable" \
		$(GO) run ./cmd/coordinator --seed-sql fixtures/subs_seed.sql

compose-up:
	$(COMPOSE) up -d --build

compose-down:
	$(COMPOSE) down -v

k8s-build:
	kustomize build deploy/k8s > /dev/null && echo "kustomize OK"

# M0 DoD: worker checks local configs without NATS/pg
run-worker-dry:
	$(GO) run ./cmd/worker --role=tester --dry-run --nodes fixtures/sample_uris.txt --limit 22
