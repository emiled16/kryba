from __future__ import annotations

from dataclasses import dataclass

from poly_arbitrage.config import Settings
from poly_arbitrage.connectors.discovery import (
    ConnectorServices,
    SourceRegistry,
    discover_source_registry,
)
from poly_arbitrage.connectors.polymarket.client import PolymarketClient
from poly_arbitrage.runtime.event_bus import InMemoryEventBus, KafkaEventBus
from poly_arbitrage.runtime.ingestion import IngestionApplication
from poly_arbitrage.runtime.writer import WriterApplication
from poly_arbitrage.storage import S3BlobWriter, SqlAlchemyStore, create_engine_and_session_factory


@dataclass(slots=True)
class ApplicationContainer:
    settings: Settings
    client: PolymarketClient
    store: SqlAlchemyStore
    bus: KafkaEventBus | InMemoryEventBus
    registry: SourceRegistry
    ingestion: IngestionApplication
    writer: WriterApplication

    async def aclose(self) -> None:
        await self.client.aclose()


def build_container(
    settings: Settings | None = None,
    *,
    bus: KafkaEventBus | InMemoryEventBus | None = None,
    store: SqlAlchemyStore | None = None,
    blob_writer: S3BlobWriter | None = None,
) -> ApplicationContainer:
    resolved_settings = settings or Settings.from_env()
    _, session_factory = create_engine_and_session_factory(resolved_settings.database_url)
    resolved_store = store or SqlAlchemyStore(session_factory)
    resolved_bus = bus or KafkaEventBus(
        bootstrap_servers=resolved_settings.kafka_bootstrap_servers,
        topic=resolved_settings.kafka_raw_topic,
    )
    resolved_blob_writer = blob_writer or S3BlobWriter(resolved_settings)
    client = PolymarketClient(resolved_settings)
    registry = discover_source_registry(
        ConnectorServices(
            settings=resolved_settings,
            client=client,
            catalog=resolved_store,
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
        catalog=resolved_store,
        storage_prefix=resolved_settings.storage_prefix,
    )
    return ApplicationContainer(
        settings=resolved_settings,
        client=client,
        store=resolved_store,
        bus=resolved_bus,
        registry=registry,
        ingestion=ingestion,
        writer=writer,
    )
