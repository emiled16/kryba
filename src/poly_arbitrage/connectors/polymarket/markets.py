from __future__ import annotations

from datetime import UTC, datetime

from poly_arbitrage.connectors.polymarket.client import PolymarketClient
from poly_arbitrage.contracts import IngestionMode, RawRecord, SourceSpec


class PolymarketMarketsSource:
    spec = SourceSpec(
        name="polymarket_markets",
        produces="market",
        cron_schedule="*/15 * * * *",
    )

    def __init__(self, client: PolymarketClient):
        self._client = client

    async def fetch(
        self,
        *,
        cursor: str | None = None,
        limit: int = 100,
    ) -> tuple[list[RawRecord], str | None]:
        offset = int(cursor or "0")
        markets = await self._client.list_markets(limit=limit, offset=offset)
        fetched_at = datetime.now(tz=UTC)
        records = [
            RawRecord(
                source="polymarket",
                entity_type="market",
                entity_id=str(market["id"]),
                event_type="snapshot",
                fetched_at=fetched_at,
                occurred_at=None,
                mode=IngestionMode.BATCH,
                payload=market,
                cursor=str(offset),
            )
            for market in markets
        ]
        next_cursor = str(offset + len(markets)) if markets else None
        return records, next_cursor
