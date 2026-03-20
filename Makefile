SHELL := /bin/bash
.DEFAULT_GOAL := help

COMPOSE ?= docker compose
API_URL ?= http://localhost:8000
PYTHON ?= python

.PHONY: help infra-up app-up up db-init wait-api sources ingest-markets ingest-quotes runs smoke smoke-clean logs-api logs-writer down reset

help:
	@printf "\nLocal ingestion targets\n\n"
	@printf "  make up             Build app image, start infra, migrate DB, start API and writer\n"
	@printf "  make smoke          Run a local ingestion smoke test end-to-end\n"
	@printf "  make smoke-clean    Reset local data, then run the smoke test from a clean state\n"
	@printf "  make sources        List discovered batch/stream sources\n"
	@printf "  make ingest-markets Trigger the Polymarket markets ingestion job\n"
	@printf "  make ingest-quotes  Trigger the Polymarket quotes ingestion job\n"
	@printf "  make runs           List recorded ingestion runs\n"
	@printf "  make logs-api       Tail API logs\n"
	@printf "  make logs-writer    Tail writer logs\n"
	@printf "  make down           Stop the local stack\n\n"

infra-up:
	$(COMPOSE) up -d postgres redpanda minio minio-init

app-up:
	$(COMPOSE) up -d --build api writer

up: infra-up db-init app-up

db-init:
	$(COMPOSE) up --build db-migrate

wait-api:
	@until curl -fsS "$(API_URL)/healthz" >/dev/null; do \
		echo "waiting for API at $(API_URL)"; \
		sleep 2; \
	done

sources: wait-api
	curl -fsS "$(API_URL)/sources" | $(PYTHON) -m json.tool

ingest-markets: wait-api
	curl -fsS -X POST "$(API_URL)/runs/polymarket_markets" | $(PYTHON) -m json.tool

ingest-quotes: wait-api
	curl -fsS -X POST "$(API_URL)/runs/polymarket_quotes" | $(PYTHON) -m json.tool

runs: wait-api
	curl -fsS "$(API_URL)/runs" | $(PYTHON) -m json.tool

smoke: up wait-api
	@$(MAKE) ingest-markets
	@sleep 2
	@$(MAKE) ingest-quotes
	@sleep 2
	@$(MAKE) runs

smoke-clean: reset smoke

logs-api:
	$(COMPOSE) logs -f api

logs-writer:
	$(COMPOSE) logs -f writer

down:
	$(COMPOSE) down

reset:
	$(COMPOSE) down --remove-orphans
	rm -rf artifacts/postgres artifacts/minio
