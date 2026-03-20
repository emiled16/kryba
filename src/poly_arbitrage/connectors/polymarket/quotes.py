from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

from poly_arbitrage.connectors.polymarket.client import PolymarketClient
from poly_arbitrage.connectors.polymarket.parsing import parse_string_list
from poly_arbitrage.contracts import EntityCatalog, IngestionMode, RawRecord, SourceSpec


class PolymarketQuoteSource:
    spec = SourceSpec(
        name="polymarket_quotes",
        produces="quote",
        depends_on=["market"],
        cron_schedule="*/5 * * * *",
    )

    def __init__(self, client: PolymarketClient, catalog: EntityCatalog):
        self._client = client
        self._catalog = catalog

    async def fetch(
        self,
        *,
        cursor: str | None = None,
        chunk_size: int = 50,
    ) -> tuple[list[RawRecord], str | None]:
        del cursor
        markets = await self._catalog.list_entities("polymarket", "market", status="active")
        market_tokens: list[tuple[str, str, str | None]] = []
        for market in markets:
            token_ids = parse_string_list(market.attributes.get("clob_token_ids"))
            outcomes = parse_string_list(market.attributes.get("outcomes"))
            for index, token_id in enumerate(token_ids):
                outcome = outcomes[index] if index < len(outcomes) else None
                market_tokens.append((market.entity_id, token_id, outcome))

        fetched_at = datetime.now(tz=UTC)
        records: list[RawRecord] = []
        for start in range(0, len(market_tokens), chunk_size):
            chunk = market_tokens[start : start + chunk_size]
            prices = await self._client.get_market_prices([token_id for _, token_id, _ in chunk])
            for market_id, token_id, outcome in chunk:
                price_payload = {
                    "token_id": token_id,
                    "market_id": market_id,
                    "outcome": outcome,
                    "prices": prices.get(token_id, {}),
                }
                records.append(
                    RawRecord(
                        source="polymarket",
                        entity_type="quote",
                        entity_id=token_id,
                        event_type="snapshot",
                        fetched_at=fetched_at,
                        occurred_at=None,
                        mode=IngestionMode.BATCH,
                        payload=price_payload,
                        parent_entity_type="market",
                        parent_entity_id=market_id,
                    )
                )
        return records, fetched_at.isoformat()


class PolymarketQuoteStreamSource:
    spec = SourceSpec(
        name="polymarket_quote_stream",
        produces="quote",
        depends_on=["market"],
        modes=[IngestionMode.STREAM],
    )

    def __init__(
        self,
        catalog: EntityCatalog,
        websocket_url: str | None = None,
        websocket_connector: Callable[..., Awaitable[object]] | None = None,
    ):
        self._catalog = catalog
        self._websocket_url = websocket_url or "wss://ws-subscriptions-clob.polymarket.com/ws/market"
        self._websocket_connector = websocket_connector

    async def stream(
        self,
        publish: Callable[[RawRecord], Awaitable[None]],
        *,
        max_messages: int | None = None,
    ) -> int:
        quotes = await self._catalog.list_entities("polymarket", "quote", status="active")
        quote_lookup = {
            quote.entity_id: quote for quote in quotes if quote.subscription_status != "disabled"
        }
        token_ids = list(quote_lookup)
        if not token_ids:
            return 0

        if self._websocket_connector is None:
            try:
                import websockets
            except ImportError as exc:
                raise RuntimeError("websockets dependency is required for quote streaming") from exc
            connect = websockets.connect
        else:
            connect = self._websocket_connector
        processed = 0
        async with connect(self._websocket_url) as websocket:
            await websocket.send(
                json.dumps(
                    {
                        "type": "market",
                        "assets_ids": token_ids,
                        "custom_feature_enabled": True,
                    }
                )
            )
            while max_messages is None or processed < max_messages:
                raw_message = await websocket.recv()
                message = json.loads(raw_message)
                items = message if isinstance(message, list) else [message]
                fetched_at = datetime.now(tz=UTC)
                for item in items:
                    asset_id = str(item.get("asset_id", ""))
                    if not asset_id:
                        continue
                    existing_quote = quote_lookup.get(asset_id)
                    await publish(
                        RawRecord(
                            source="polymarket",
                            entity_type="quote",
                            entity_id=asset_id,
                            event_type=str(item.get("event_type", "stream_update")),
                            fetched_at=fetched_at,
                            occurred_at=None,
                            mode=IngestionMode.STREAM,
                            payload=item,
                            parent_entity_type=(
                                existing_quote.parent_entity_type if existing_quote else None
                            ),
                            parent_entity_id=(
                                existing_quote.parent_entity_id if existing_quote else None
                            ),
                        )
                    )
                    processed += 1
                    if max_messages is not None and processed >= max_messages:
                        break
        return processed
