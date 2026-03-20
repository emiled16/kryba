from __future__ import annotations

from datetime import UTC, datetime

import pytest

from poly_arbitrage.contracts import EntityState
from poly_arbitrage.storage.db import create_engine_and_session_factory
from poly_arbitrage.storage.models import Base
from poly_arbitrage.storage.repositories import SqlAlchemyStore


@pytest.mark.asyncio
async def test_store_upserts_entities_and_checkpoints() -> None:
    engine, session_factory = create_engine_and_session_factory("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    store = SqlAlchemyStore(session_factory)

    await store.upsert_entity(
        EntityState(
            source="polymarket",
            entity_type="market",
            entity_id="123",
            status="active",
            first_seen_at=datetime(2026, 3, 20, tzinfo=UTC),
            last_seen_at=datetime(2026, 3, 20, tzinfo=UTC),
            attributes={"slug": "rain-market"},
        )
    )
    await store.save("polymarket", "markets", "default", "100")

    entities = await store.list_entities("polymarket", "market", status="active")

    assert len(entities) == 1
    assert entities[0].attributes["slug"] == "rain-market"
    assert await store.get("polymarket", "markets", "default") == "100"
