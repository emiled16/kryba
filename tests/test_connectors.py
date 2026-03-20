from __future__ import annotations

from datetime import UTC, datetime

import pytest

from poly_arbitrage.connectors.polymarket.markets import PolymarketMarketsSource
from poly_arbitrage.connectors.polymarket.quotes import (
    PolymarketQuoteSource,
    PolymarketQuoteStreamSource,
)
from poly_arbitrage.contracts import EntityState


class FakePolymarketClient:
    async def list_markets(self, *, limit: int = 100, offset: int = 0):
        return [
            {
                "id": "mkt-1",
                "question": "Will it rain?",
                "active": True,
                "closed": False,
                "clobTokenIds": ["tok-yes", "tok-no"],
                "outcomes": ["Yes", "No"],
            }
        ]

    async def get_market_prices(self, token_ids: list[str]):
        return {token_id: {"BUY": "0.50", "SELL": "0.51"} for token_id in token_ids}


class FakeEntityStore:
    async def list_entities(self, source: str, entity_type: str, **_: object):
        if entity_type == "market":
            return [
                EntityState(
                    source=source,
                    entity_type=entity_type,
                    entity_id="mkt-1",
                    status="active",
                    first_seen_at=datetime(2026, 3, 20, tzinfo=UTC),
                    last_seen_at=datetime(2026, 3, 20, tzinfo=UTC),
                    attributes={
                        "clob_token_ids": ["tok-yes", "tok-no"],
                        "outcomes": ["Yes", "No"],
                    },
                )
            ]
        return [
            EntityState(
                source=source,
                entity_type=entity_type,
                entity_id="tok-yes",
                status="active",
                first_seen_at=datetime(2026, 3, 20, tzinfo=UTC),
                last_seen_at=datetime(2026, 3, 20, tzinfo=UTC),
                parent_entity_type="market",
                parent_entity_id="mkt-1",
                subscription_status="ready",
                subscription_target="mkt-1",
            )
        ]


class FakeWebSocket:
    def __init__(self, messages: list[str]):
        self.messages = messages
        self.sent_messages: list[str] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def send(self, payload: str):
        self.sent_messages.append(payload)

    async def recv(self):
        return self.messages.pop(0)


@pytest.mark.asyncio
async def test_markets_source_builds_market_records() -> None:
    source = PolymarketMarketsSource(FakePolymarketClient())

    records, next_cursor = await source.fetch(cursor="0", limit=100)

    assert len(records) == 1
    assert records[0].entity_type == "market"
    assert next_cursor == "1"


@pytest.mark.asyncio
async def test_quote_source_builds_quote_records_from_catalog_markets() -> None:
    source = PolymarketQuoteSource(FakePolymarketClient(), FakeEntityStore())

    records, checkpoint = await source.fetch(chunk_size=10)

    assert len(records) == 2
    assert {record.entity_id for record in records} == {"tok-yes", "tok-no"}
    assert checkpoint is not None


@pytest.mark.asyncio
async def test_quote_stream_preserves_parent_market_linkage() -> None:
    published = []
    websocket = FakeWebSocket(['{"asset_id":"tok-yes","event_type":"price_change"}'])
    source = PolymarketQuoteStreamSource(
        FakeEntityStore(),
        websocket_connector=lambda *_args, **_kwargs: websocket,
    )

    async def publish(record):
        published.append(record)

    processed = await source.stream(publish, max_messages=1)

    assert processed == 1
    assert published[0].parent_entity_id == "mkt-1"
    assert published[0].parent_entity_type == "market"
