from __future__ import annotations

from poly_arbitrage.connectors.discovery import ConnectorServices, SourceRegistry
from poly_arbitrage.connectors.polymarket.markets import PolymarketMarketsSource
from poly_arbitrage.connectors.polymarket.quotes import (
    PolymarketQuoteSource,
    PolymarketQuoteStreamSource,
)
from poly_arbitrage.contracts import BatchSourceRegistration, StreamSourceRegistration


def register(registry: SourceRegistry, services: ConnectorServices) -> None:
    markets_source = PolymarketMarketsSource(services.client)
    quotes_source = PolymarketQuoteSource(services.client, services.catalog)
    quote_stream_source = PolymarketQuoteStreamSource(services.catalog)

    registry.register_batch(
        BatchSourceRegistration(spec=markets_source.spec, fetch=markets_source.fetch)
    )
    registry.register_batch(
        BatchSourceRegistration(spec=quotes_source.spec, fetch=quotes_source.fetch)
    )
    registry.register_stream(
        StreamSourceRegistration(
            spec=quote_stream_source.spec,
            consume=quote_stream_source.stream,
        )
    )
