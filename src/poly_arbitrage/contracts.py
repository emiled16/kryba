from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Protocol


class IngestionMode(StrEnum):
    BATCH = "batch"
    STREAM = "stream"


@dataclass(slots=True)
class RawRecord:
    source: str
    entity_type: str
    entity_id: str
    event_type: str
    fetched_at: datetime
    occurred_at: datetime | None
    mode: IngestionMode
    payload: dict[str, Any]
    cursor: str | None = None
    parent_entity_type: str | None = None
    parent_entity_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["fetched_at"] = self.fetched_at.astimezone(UTC).isoformat()
        data["occurred_at"] = (
            self.occurred_at.astimezone(UTC).isoformat() if self.occurred_at else None
        )
        data["mode"] = self.mode.value
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RawRecord:
        return cls(
            source=data["source"],
            entity_type=data["entity_type"],
            entity_id=data["entity_id"],
            event_type=data["event_type"],
            fetched_at=datetime.fromisoformat(data["fetched_at"]),
            occurred_at=datetime.fromisoformat(data["occurred_at"])
            if data.get("occurred_at")
            else None,
            mode=IngestionMode(data["mode"]),
            payload=data["payload"],
            cursor=data.get("cursor"),
            parent_entity_type=data.get("parent_entity_type"),
            parent_entity_id=data.get("parent_entity_id"),
        )


@dataclass(slots=True)
class SourceSpec:
    name: str
    produces: str
    depends_on: list[str] = field(default_factory=list)
    modes: list[IngestionMode] = field(default_factory=lambda: [IngestionMode.BATCH])
    cron_schedule: str | None = None


@dataclass(slots=True)
class EntityState:
    source: str
    entity_type: str
    entity_id: str
    status: str
    first_seen_at: datetime
    last_seen_at: datetime
    parent_entity_type: str | None = None
    parent_entity_id: str | None = None
    attributes: dict[str, Any] = field(default_factory=dict)
    subscription_status: str | None = None
    subscription_key: str | None = None
    subscription_target: str | None = None
    subscription_updated_at: datetime | None = None


@dataclass(slots=True)
class PersistedRecord:
    record: RawRecord
    blob_uri: str
    payload_hash: str
    idempotency_key: str


class BlobWriter(Protocol):
    async def write_json(self, key: str, payload: dict[str, Any]) -> str:
        """Persist the payload and return its URI."""


class MetadataWriter(Protocol):
    async def raw_record_exists(self, idempotency_key: str) -> bool:
        """Return whether a raw record with the idempotency key already exists."""

    async def record_raw(self, persisted: PersistedRecord) -> None:
        """Persist the raw record index."""


class CheckpointStore(Protocol):
    async def get(self, source: str, job_name: str, partition_key: str) -> str | None:
        """Fetch a saved cursor."""

    async def save(self, source: str, job_name: str, partition_key: str, cursor: str) -> None:
        """Persist a cursor."""


class EntityStore(Protocol):
    async def upsert_entity(self, entity: EntityState) -> None:
        """Create or update an entity row."""

    async def list_entities(
        self,
        source: str,
        entity_type: str,
        *,
        status: str | None = None,
        parent_entity_type: str | None = None,
        parent_entity_id: str | None = None,
    ) -> list[EntityState]:
        """List stored entities matching the provided filters."""


class RunStore(Protocol):
    async def create_run(self, job_name: str, source: str) -> str:
        """Create a run and return its identifier."""

    async def complete_run(self, run_id: str, status: str, error: str | None = None) -> None:
        """Mark a run as completed."""

    async def list_runs(self) -> list[dict[str, Any]]:
        """List recent runs."""

    async def get_run(self, run_id: str) -> dict[str, Any] | None:
        """Return a specific run by identifier."""


BatchFetchFn = Callable[..., Awaitable[tuple[list[RawRecord], str | None]]]
StreamConsumeFn = Callable[..., Awaitable[int]]
EntityProjector = Callable[[RawRecord], list[EntityState]]


@dataclass(slots=True, frozen=True)
class BatchSourceRegistration:
    spec: SourceSpec
    fetch: BatchFetchFn


@dataclass(slots=True, frozen=True)
class StreamSourceRegistration:
    spec: SourceSpec
    consume: StreamConsumeFn
