from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import httpx

from poly_arbitrage.contracts import IngestionMode, RawRecord, SourceSpec


class PolymarketMarketsSource:
    spec = SourceSpec(
        name="polymarket_markets",
        produces="polymarket::market",
        cron_schedule="*/15 * * * *",
    )

    def __init__(self, http_client: httpx.AsyncClient, *, gamma_base_url: str):
        self._http_client = http_client
        self._gamma_base_url = gamma_base_url.rstrip("/")

    async def fetch(
        self,
        *,
        cursor: str | None = None,
        limit: int = 100,
    ) -> tuple[list[RawRecord], str | None]:
        offset = int(cursor or "0")
        response = await self._http_client.get(
            f"{self._gamma_base_url}/markets",
            params={"limit": limit, "offset": offset, "active": "true", "closed": "false"},
        )
        response.raise_for_status()
        markets = self._parse_markets(response.json())
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

    @staticmethod
    def _parse_markets(payload: object) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict) and isinstance(payload.get("markets"), list):
            return payload["markets"]
        raise ValueError("Unexpected Polymarket markets response shape")
