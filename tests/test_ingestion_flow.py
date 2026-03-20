from __future__ import annotations

from datetime import UTC, datetime

import pytest

from poly_arbitrage.config import Settings
from poly_arbitrage.connectors.discovery import ConnectorServices, discover_source_registry
from poly_arbitrage.connectors.polymarket.markets import PolymarketMarketsSource
from poly_arbitrage.connectors.polymarket.quotes import PolymarketQuoteSource
from poly_arbitrage.contracts import EntityState
from poly_arbitrage.runtime.event_bus import InMemoryEventBus
from poly_arbitrage.runtime.ingestion import IngestionApplication
from poly_arbitrage.runtime.testing import InMemoryBlobWriter
from poly_arbitrage.runtime.writer import WriterApplication
from poly_arbitrage.storage.db import create_engine_and_session_factory
from poly_arbitrage.storage.models import Base, RawRecordIndexModel
from poly_arbitrage.storage.repositories import SqlAlchemyStore


class StubPolymarketClient:
    async def list_markets(self, *, limit: int = 100, offset: int = 0):
        return [
            {
                "id": "mkt-1",
                "question": "Will it rain?",
                "slug": "will-it-rain",
                "conditionId": "cond-1",
                "active": True,
                "closed": False,
                "clobTokenIds": ["tok-yes", "tok-no"],
                "outcomes": ["Yes", "No"],
            }
        ]

    async def get_market_prices(self, token_ids: list[str]):
        return {token_id: {"BUY": "0.42", "SELL": "0.43"} for token_id in token_ids}


def build_ingestion(bus: InMemoryEventBus, store: SqlAlchemyStore, client: StubPolymarketClient):
    registry = discover_source_registry(
        ConnectorServices(
            settings=Settings.from_env(),
            client=client,
            entity_store=store,
        )
    )
    return IngestionApplication(
        run_store=store,
        checkpoints=store,
        bus=bus,
        registry=registry,
    )


@pytest.mark.asyncio
async def test_markets_then_quotes_end_to_end() -> None:
    engine, session_factory = create_engine_and_session_factory("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    store = SqlAlchemyStore(session_factory)
    bus = InMemoryEventBus()
    client = StubPolymarketClient()
    registry = discover_source_registry(
        ConnectorServices(
            settings=Settings.from_env(),
            client=client,
            entity_store=store,
        )
    )
    ingestion = build_ingestion(bus, store, client)
    writer = WriterApplication(
        bus=bus,
        blob_writer=InMemoryBlobWriter(),
        metadata_writer=store,
        entity_store=store,
        source_registry=registry,
        storage_prefix="raw",
    )

    markets_result = await ingestion.run_job("polymarket_markets")
    await writer.persist_pending()
    quotes_result = await ingestion.run_job("polymarket_quotes")
    await writer.persist_pending()

    markets = await store.list_entities("polymarket", "market", status="active")
    quotes = await store.list_entities("polymarket", "quote", status="active")

    with session_factory() as session:
        raw_count = session.query(RawRecordIndexModel).count()

    assert markets_result.published_count == 1
    assert quotes_result.published_count == 2
    assert len(markets) == 1
    assert len(quotes) == 2
    assert raw_count == 3


@pytest.mark.asyncio
async def test_quote_source_uses_catalog_entities() -> None:
    engine, session_factory = create_engine_and_session_factory("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    store = SqlAlchemyStore(session_factory)
    now = datetime(2026, 3, 20, tzinfo=UTC)
    await store.upsert_entity(
        EntityState(
            source="polymarket",
            entity_type="market",
            entity_id="mkt-1",
            status="active",
            first_seen_at=now,
            last_seen_at=now,
            attributes={"clob_token_ids": ["tok-1"], "outcomes": ["Yes"]},
        )
    )

    records, _ = await PolymarketQuoteSource(StubPolymarketClient(), store).fetch()

    assert len(records) == 1
    assert records[0].parent_entity_id == "mkt-1"


@pytest.mark.asyncio
async def test_duplicate_delivery_skips_extra_blob_and_metadata_write() -> None:
    engine, session_factory = create_engine_and_session_factory("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    store = SqlAlchemyStore(session_factory)
    bus = InMemoryEventBus()
    blob_writer = InMemoryBlobWriter()
    writer = WriterApplication(
        bus=bus,
        blob_writer=blob_writer,
        metadata_writer=store,
        entity_store=store,
        source_registry=discover_source_registry(
            ConnectorServices(
                settings=Settings.from_env(),
                client=StubPolymarketClient(),
                entity_store=store,
            )
        ),
        storage_prefix="raw",
    )
    record, _ = await PolymarketMarketsSource(StubPolymarketClient()).fetch(cursor="0", limit=1)

    await bus.publish(record[0])
    await bus.publish(record[0])
    await writer.persist_pending()

    with session_factory() as session:
        raw_count = session.query(RawRecordIndexModel).count()

    assert raw_count == 1
    assert len(blob_writer.objects) == 1


@pytest.mark.asyncio
async def test_stream_update_preserves_quote_catalog_relationships() -> None:
    engine, session_factory = create_engine_and_session_factory("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    store = SqlAlchemyStore(session_factory)
    now = datetime(2026, 3, 20, tzinfo=UTC)
    await store.upsert_entity(
        EntityState(
            source="polymarket",
            entity_type="quote",
            entity_id="tok-yes",
            status="active",
            first_seen_at=now,
            last_seen_at=now,
            parent_entity_type="market",
            parent_entity_id="mkt-1",
            attributes={"prices": {"BUY": "0.42"}},
            subscription_status="ready",
            subscription_key="tok-yes",
            subscription_target="mkt-1",
            subscription_updated_at=now,
        )
    )
    writer = WriterApplication(
        bus=InMemoryEventBus(),
        blob_writer=InMemoryBlobWriter(),
        metadata_writer=store,
        entity_store=store,
        source_registry=discover_source_registry(
            ConnectorServices(
                settings=Settings.from_env(),
                client=StubPolymarketClient(),
                entity_store=store,
            )
        ),
        storage_prefix="raw",
    )

    from poly_arbitrage.contracts import IngestionMode, RawRecord

    await writer._persist_record(
        RawRecord(
            source="polymarket",
            entity_type="quote",
            entity_id="tok-yes",
            event_type="price_change",
            fetched_at=now.replace(minute=5),
            occurred_at=None,
            mode=IngestionMode.STREAM,
            payload={"asset_id": "tok-yes", "bid": "0.44"},
        )
    )

    quotes = await store.list_entities("polymarket", "quote", status="active")

    assert quotes[0].parent_entity_id == "mkt-1"
    assert quotes[0].subscription_target == "mkt-1"
