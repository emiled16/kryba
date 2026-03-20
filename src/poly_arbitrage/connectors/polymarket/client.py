from __future__ import annotations

from typing import Any

import httpx

from poly_arbitrage.config import Settings


class PolymarketClient:
    def __init__(self, settings: Settings, client: httpx.AsyncClient | None = None):
        self._client = client or httpx.AsyncClient(timeout=30.0)
        self._owns_client = client is None
        self._gamma_base_url = settings.polymarket_gamma_base_url.rstrip("/")
        self._clob_base_url = settings.polymarket_clob_base_url.rstrip("/")

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def list_markets(self, *, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        response = await self._client.get(
            f"{self._gamma_base_url}/markets",
            params={"limit": limit, "offset": offset, "active": "true", "closed": "false"},
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict) and isinstance(payload.get("markets"), list):
            return payload["markets"]
        raise ValueError("Unexpected Polymarket markets response shape")

    async def get_market_prices(self, token_ids: list[str]) -> dict[str, dict[str, str]]:
        if not token_ids:
            return {}
        response = await self._client.get(
            f"{self._clob_base_url}/prices",
            params=[("token_ids", token_id) for token_id in token_ids],
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("Unexpected Polymarket prices response shape")
        return payload
