from __future__ import annotations

from dataclasses import dataclass

import httpx

from poly_arbitrage.config import Settings
from poly_arbitrage.connectors.discovery import (
    ConnectorServices,
    SourceRegistry,
    discover_source_registry,
)
from poly_arbitrage.runtime.event_bus import InMemoryEventBus, KafkaEventBus
from poly_arbitrage.runtime.ingestion import IngestionApplication
from poly_arbitrage.runtime.writer import WriterApplication
from poly_arbitrage.storage import S3BlobWriter, SqlAlchemyStore, create_engine_and_session_factory


@dataclass(slots=True)
class ApplicationRuntime:
    """Process-scoped services shared by the API and worker entrypoints."""

    settings: Settings
    http_client: httpx.AsyncClient
    store: SqlAlchemyStore
    bus: KafkaEventBus | InMemoryEventBus
    source_registry: SourceRegistry
    ingestion: IngestionApplication
    writer: WriterApplication

    async def aclose(self) -> None:
        await self.http_client.aclose()


def build_runtime(
    settings: Settings | None = None,
    *,
    bus: KafkaEventBus | InMemoryEventBus | None = None,
    store: SqlAlchemyStore | None = None,
    blob_writer: S3BlobWriter | None = None,
) -> ApplicationRuntime:
    resolved_settings = settings or Settings.from_env()
    _, session_factory = create_engine_and_session_factory(resolved_settings.database_url)
    resolved_store = store or SqlAlchemyStore(session_factory)
    resolved_bus = bus or KafkaEventBus(
        bootstrap_servers=resolved_settings.kafka_bootstrap_servers,
        topic=resolved_settings.kafka_raw_topic,
    )
    resolved_blob_writer = blob_writer or S3BlobWriter(resolved_settings)
    http_client = httpx.AsyncClient(timeout=30.0)
    registry = discover_source_registry(
        ConnectorServices(
            settings=resolved_settings,
            http_client=http_client,
            entity_store=resolved_store,
        )
    )
    ingestion = IngestionApplication(
        run_store=resolved_store,
        checkpoints=resolved_store,
        bus=resolved_bus,
        registry=registry,
    )
    writer = WriterApplication(
        bus=resolved_bus,
        blob_writer=resolved_blob_writer,
        metadata_writer=resolved_store,
        entity_store=resolved_store,
        source_registry=registry,
        storage_prefix=resolved_settings.storage_prefix,
    )
    return ApplicationRuntime(
        settings=resolved_settings,
        http_client=http_client,
        store=resolved_store,
        bus=resolved_bus,
        source_registry=registry,
        ingestion=ingestion,
        writer=writer,
    )
