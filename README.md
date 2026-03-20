# Poly Arbitrage

Poly Arbitrage is a plugin-oriented ingestion platform for collecting raw market data, persisting it, and exposing simple operational controls over that ingestion flow.

Today the first connector is Polymarket. The system can:

- fetch market snapshots from Polymarket
- derive quote targets from the markets it already knows about
- fetch quote snapshots for those targets
- publish raw records onto Kafka/Redpanda
- persist raw payloads to object storage and metadata to Postgres
- expose HTTP endpoints for triggering runs and inspecting status

The repository is structured as an ingestion platform, not a single script.

## Core Concepts

There are three main data concepts in the codebase:

- `RawRecord`
  This is the raw ingestion event. It keeps the untouched payload fetched from a source plus metadata such as source, entity type, fetch time, mode, and parent relationships.

- `EntityState`
  This is the platform's current known state for an entity. It is derived from raw records and used for dependency-aware ingestion. For example, Polymarket quote ingestion reads active market entity states to know which tokens to request quotes for.

- `SourceSpec`
  This is connector metadata used to describe a source job: its name, what it produces, its dependencies, supported modes, and optional schedule.

In practice:

1. a connector fetches source data and emits `RawRecord` objects
2. the ingestion runtime publishes those records to the event bus
3. the writer consumes the records, stores the raw payload, and projects them into `EntityState`
4. downstream connectors query `EntityState` to know what to ingest next

## Architecture

At a high level the system looks like this:

```text
FastAPI API
  -> ApplicationRuntime
     -> IngestionApplication
     -> WriterApplication
     -> SourceRegistry
     -> EventBus
     -> SqlAlchemyStore
     -> shared httpx client
```

Main components:

- `src/poly_arbitrage/api`
  FastAPI application with health, source discovery, and run-triggering endpoints.

- `src/poly_arbitrage/runtime`
  Runtime wiring, ingestion orchestration, writer service, event bus, and process entrypoints.

- `src/poly_arbitrage/connectors`
  Plugin-style source adapters. Right now this contains the Polymarket connector.

- `src/poly_arbitrage/storage`
  Postgres metadata/index storage and object storage integration.

- `src/poly_arbitrage/dagster_defs`
  Dagster job and schedule definitions derived from the registered batch sources.

## Polymarket Flow

The current Polymarket implementation has three source jobs:

- `polymarket_markets`
  Fetches market snapshots from the Gamma API and emits `polymarket::market` records.

- `polymarket_quotes`
  Reads active market entities, extracts token ids, fetches prices from the CLOB API, and emits `polymarket::quote` records.

- `polymarket_quote_stream`
  Reads active quote entities and consumes quote updates from the websocket stream.

The connector-specific projection logic lives next to the connector, not in the generic runtime. For Polymarket that logic is in `src/poly_arbitrage/connectors/polymarket/entity_projection.py`.

## Local Development

The local stack uses:

- Postgres for metadata
- Redpanda as the Kafka-compatible event bus
- MinIO for object storage
- FastAPI for control endpoints
- a writer process that consumes raw records and persists them

The easiest way to exercise the system locally is through the `Makefile`.

### Start the stack

```bash
make up
```

This will:

- start Postgres, Redpanda, and MinIO
- run database initialization
- build the application image
- start the API and writer services

### Run an end-to-end smoke test

```bash
make smoke-clean
```

This is the recommended first run. It:

- tears down the local stack
- removes local Postgres and MinIO data under `artifacts/`
- rebuilds the stack from a clean state
- triggers markets ingestion
- triggers quotes ingestion
- prints the recorded ingestion runs

Use `make smoke` only when you intentionally want to reuse existing local data.

### Useful commands

```bash
make sources
make ingest-markets
make ingest-quotes
make runs
make logs-api
make logs-writer
make down
```

## Environment

Default local settings live in `.env.example`.

Important variables:

- `DATABASE_URL`
- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_RAW_TOPIC`
- `STORAGE_BUCKET`
- `STORAGE_ENDPOINT_URL`
- `POLYMARKET_GAMMA_BASE_URL`
- `POLYMARKET_CLOB_BASE_URL`

For the docker-compose workflow, the defaults are already wired for local Postgres, Redpanda, and MinIO.

## Current Status

What works today:

- plugin-style source registration
- batch ingestion via the API
- raw record publication to Kafka/Redpanda
- writer persistence to Postgres and object storage
- entity-state projection for dependency-aware ingestion
- local smoke test workflow through `make smoke-clean`

What is still early-stage:

- schema migration strategy beyond `create_all`
- richer failure handling and retries
- stronger observability around writer/stream workers
- broader connector coverage beyond Polymarket

## Tests

Run the full test suite with:

```bash
pytest -q
ruff check src tests
```
