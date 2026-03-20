from __future__ import annotations

import hashlib
import json

from poly_arbitrage.contracts import RawRecord


def stable_payload_hash(payload: dict[str, object]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def build_idempotency_key(record: RawRecord, payload_hash: str) -> str:
    occurred = record.occurred_at.isoformat() if record.occurred_at else ""
    fetched = record.fetched_at.isoformat()
    native = (
        f"{record.source}|{record.entity_type}|{record.entity_id}|"
        f"{record.event_type}|{occurred}|{fetched}|{payload_hash}"
    )
    return hashlib.sha256(native.encode("utf-8")).hexdigest()
