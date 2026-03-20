from __future__ import annotations

from datetime import UTC, datetime

from poly_arbitrage.connectors.polymarket.parsing import parse_string_list
from poly_arbitrage.contracts import EntityState, IngestionMode, RawRecord


def project_entities(record: RawRecord) -> list[EntityState]:
    if record.entity_type == "market":
        payload = record.payload
        is_active = payload.get("active", True) and not payload.get("closed", False)
        status = "active" if is_active else "inactive"
        return [
            EntityState(
                source=record.source,
                entity_type="market",
                entity_id=record.entity_id,
                status=status,
                first_seen_at=record.fetched_at,
                last_seen_at=record.fetched_at,
                attributes={
                    "question": payload.get("question"),
                    "slug": payload.get("slug"),
                    "condition_id": payload.get("conditionId"),
                    "clob_token_ids": parse_string_list(payload.get("clobTokenIds")),
                    "outcomes": parse_string_list(payload.get("outcomes")),
                },
            )
        ]

    if record.entity_type == "quote":
        payload = record.payload
        subscription_target = (
            record.parent_entity_id or payload.get("market_id") or payload.get("marketId")
        )
        return [
            EntityState(
                source=record.source,
                entity_type="quote",
                entity_id=record.entity_id,
                status="active",
                first_seen_at=record.fetched_at,
                last_seen_at=record.fetched_at,
                parent_entity_type=record.parent_entity_type,
                parent_entity_id=record.parent_entity_id,
                attributes=payload,
                subscription_status="ready" if record.mode == IngestionMode.BATCH else None,
                subscription_key=record.entity_id,
                subscription_target=subscription_target,
                subscription_updated_at=(
                    datetime.now(tz=UTC) if record.mode == IngestionMode.BATCH else None
                ),
            )
        ]

    return []
