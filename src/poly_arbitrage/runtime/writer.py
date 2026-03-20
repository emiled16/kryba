from __future__ import annotations

from datetime import UTC

from poly_arbitrage.contracts import (
    BlobWriter,
    EntityCatalog,
    MetadataWriter,
    PersistedRecord,
    RawRecord,
)
from poly_arbitrage.hashing import build_idempotency_key, stable_payload_hash
from poly_arbitrage.runtime.catalog import project_entity_states
from poly_arbitrage.runtime.event_bus import EventBus


class WriterApplication:
    def __init__(
        self,
        *,
        bus: EventBus,
        blob_writer: BlobWriter,
        metadata_writer: MetadataWriter,
        catalog: EntityCatalog,
        storage_prefix: str,
    ):
        self._bus = bus
        self._blob_writer = blob_writer
        self._metadata_writer = metadata_writer
        self._catalog = catalog
        self._storage_prefix = storage_prefix.strip("/")

    async def persist_pending(self, *, max_messages: int | None = None) -> int:
        return await self._bus.consume(self._persist_record, max_messages=max_messages)

    async def _persist_record(self, record: RawRecord) -> None:
        payload_hash = stable_payload_hash(record.payload)
        idempotency_key = build_idempotency_key(record, payload_hash)
        if await self._metadata_writer.raw_record_exists(idempotency_key):
            return
        blob_key = self._build_blob_key(record, idempotency_key)
        blob_uri = await self._blob_writer.write_json(blob_key, record.payload)
        await self._metadata_writer.record_raw(
            PersistedRecord(
                record=record,
                blob_uri=blob_uri,
                payload_hash=payload_hash,
                idempotency_key=idempotency_key,
            )
        )
        for entity in project_entity_states(record):
            await self._catalog.upsert_entity(entity)

    def _build_blob_key(self, record: RawRecord, idempotency_key: str) -> str:
        fetched_date = record.fetched_at.astimezone(UTC).date().isoformat()
        return (
            f"{self._storage_prefix}/{record.source}/{record.entity_type}/"
            f"dt={fetched_date}/{record.entity_id}/{idempotency_key}.json"
        )
