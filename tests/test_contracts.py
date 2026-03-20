from __future__ import annotations

from datetime import UTC, datetime

from poly_arbitrage.contracts import IngestionMode, RawRecord
from poly_arbitrage.hashing import build_idempotency_key, stable_payload_hash


def test_raw_record_round_trip() -> None:
    record = RawRecord(
        source="polymarket",
        entity_type="market",
        entity_id="123",
        event_type="snapshot",
        fetched_at=datetime(2026, 3, 20, 12, 0, tzinfo=UTC),
        occurred_at=None,
        mode=IngestionMode.BATCH,
        payload={"id": "123", "question": "Will it rain?"},
        cursor="0",
    )

    restored = RawRecord.from_dict(record.to_dict())

    assert restored == record


def test_idempotency_key_is_stable() -> None:
    record = RawRecord(
        source="polymarket",
        entity_type="quote",
        entity_id="token-1",
        event_type="snapshot",
        fetched_at=datetime(2026, 3, 20, 12, 0, tzinfo=UTC),
        occurred_at=None,
        mode=IngestionMode.BATCH,
        payload={"prices": {"BUY": "0.42"}},
    )

    payload_hash = stable_payload_hash(record.payload)

    assert build_idempotency_key(record, payload_hash) == build_idempotency_key(
        record,
        payload_hash,
    )


def test_idempotency_key_changes_with_fetch_time() -> None:
    payload = {"prices": {"BUY": "0.42"}}
    first = RawRecord(
        source="polymarket",
        entity_type="quote",
        entity_id="token-1",
        event_type="snapshot",
        fetched_at=datetime(2026, 3, 20, 12, 0, tzinfo=UTC),
        occurred_at=None,
        mode=IngestionMode.BATCH,
        payload=payload,
    )
    second = RawRecord(
        source="polymarket",
        entity_type="quote",
        entity_id="token-1",
        event_type="snapshot",
        fetched_at=datetime(2026, 3, 20, 12, 5, tzinfo=UTC),
        occurred_at=None,
        mode=IngestionMode.BATCH,
        payload=payload,
    )

    payload_hash = stable_payload_hash(payload)

    assert build_idempotency_key(first, payload_hash) != build_idempotency_key(
        second,
        payload_hash,
    )
