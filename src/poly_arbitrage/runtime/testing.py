from __future__ import annotations

from poly_arbitrage.contracts import PersistedRecord


class InMemoryBlobWriter:
    def __init__(self):
        self.objects: dict[str, dict[str, object]] = {}

    async def write_json(self, key: str, payload: dict[str, object]) -> str:
        self.objects[key] = payload
        return f"memory://{key}"


class InMemoryMetadataWriter:
    def __init__(self):
        self.records: list[PersistedRecord] = []

    async def raw_record_exists(self, idempotency_key: str) -> bool:
        return any(record.idempotency_key == idempotency_key for record in self.records)

    async def record_raw(self, persisted: PersistedRecord) -> None:
        self.records.append(persisted)
