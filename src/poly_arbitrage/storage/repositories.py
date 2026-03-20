from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from poly_arbitrage.contracts import (
    CheckpointStore,
    EntityCatalog,
    EntityState,
    MetadataWriter,
    PersistedRecord,
    RunStore,
)
from poly_arbitrage.storage.models import (
    CheckpointModel,
    EntityModel,
    IngestionRunModel,
    RawRecordIndexModel,
)


class SqlAlchemyStore(MetadataWriter, CheckpointStore, EntityCatalog, RunStore):
    def __init__(self, session_factory):
        self._session_factory = session_factory

    async def raw_record_exists(self, idempotency_key: str) -> bool:
        with self._session_factory() as session:
            stmt = select(RawRecordIndexModel.id).where(
                RawRecordIndexModel.idempotency_key == idempotency_key
            )
            return session.scalar(stmt) is not None

    async def record_raw(self, persisted: PersistedRecord) -> None:
        with self._session_factory() as session:
            model = RawRecordIndexModel(
                idempotency_key=persisted.idempotency_key,
                source=persisted.record.source,
                entity_type=persisted.record.entity_type,
                entity_id=persisted.record.entity_id,
                event_type=persisted.record.event_type,
                fetched_at=persisted.record.fetched_at,
                occurred_at=persisted.record.occurred_at,
                mode=persisted.record.mode.value,
                cursor=persisted.record.cursor,
                blob_uri=persisted.blob_uri,
                payload_hash=persisted.payload_hash,
            )
            session.add(model)
            try:
                session.commit()
            except IntegrityError:
                session.rollback()

    async def get(self, source: str, job_name: str, partition_key: str) -> str | None:
        with self._session_factory() as session:
            stmt = select(CheckpointModel).where(
                CheckpointModel.source == source,
                CheckpointModel.job_name == job_name,
                CheckpointModel.partition_key == partition_key,
            )
            model = session.scalar(stmt)
            return model.cursor_value if model else None

    async def save(self, source: str, job_name: str, partition_key: str, cursor: str) -> None:
        with self._session_factory() as session:
            stmt = select(CheckpointModel).where(
                CheckpointModel.source == source,
                CheckpointModel.job_name == job_name,
                CheckpointModel.partition_key == partition_key,
            )
            model = session.scalar(stmt)
            if model is None:
                model = CheckpointModel(
                    source=source,
                    job_name=job_name,
                    partition_key=partition_key,
                    cursor_value=cursor,
                )
                session.add(model)
            else:
                model.cursor_value = cursor
                model.updated_at = datetime.now(tz=UTC)
            session.commit()

    async def upsert_entity(self, entity: EntityState) -> None:
        with self._session_factory() as session:
            stmt = select(EntityModel).where(
                EntityModel.source == entity.source,
                EntityModel.entity_type == entity.entity_type,
                EntityModel.entity_id == entity.entity_id,
            )
            model = session.scalar(stmt)
            if model is None:
                model = EntityModel(
                    source=entity.source,
                    entity_type=entity.entity_type,
                    entity_id=entity.entity_id,
                    parent_entity_type=entity.parent_entity_type,
                    parent_entity_id=entity.parent_entity_id,
                    status=entity.status,
                    first_seen_at=entity.first_seen_at,
                    last_seen_at=entity.last_seen_at,
                    attributes=entity.attributes,
                    subscription_status=entity.subscription_status,
                    subscription_key=entity.subscription_key,
                    subscription_target=entity.subscription_target,
                    subscription_updated_at=entity.subscription_updated_at,
                )
                session.add(model)
            else:
                if entity.parent_entity_type is not None:
                    model.parent_entity_type = entity.parent_entity_type
                if entity.parent_entity_id is not None:
                    model.parent_entity_id = entity.parent_entity_id
                model.status = entity.status
                model.last_seen_at = entity.last_seen_at
                model.attributes = {**(model.attributes or {}), **entity.attributes}
                if entity.subscription_status is not None:
                    model.subscription_status = entity.subscription_status
                if entity.subscription_key is not None:
                    model.subscription_key = entity.subscription_key
                if entity.subscription_target is not None:
                    model.subscription_target = entity.subscription_target
                if entity.subscription_updated_at is not None:
                    model.subscription_updated_at = entity.subscription_updated_at
            session.commit()

    async def list_entities(
        self,
        source: str,
        entity_type: str,
        *,
        status: str | None = None,
        parent_entity_type: str | None = None,
        parent_entity_id: str | None = None,
    ) -> list[EntityState]:
        with self._session_factory() as session:
            stmt = select(EntityModel).where(
                EntityModel.source == source,
                EntityModel.entity_type == entity_type,
            )
            if status is not None:
                stmt = stmt.where(EntityModel.status == status)
            if parent_entity_type is not None:
                stmt = stmt.where(EntityModel.parent_entity_type == parent_entity_type)
            if parent_entity_id is not None:
                stmt = stmt.where(EntityModel.parent_entity_id == parent_entity_id)

            entities = []
            for model in session.scalars(stmt):
                entities.append(
                    EntityState(
                        source=model.source,
                        entity_type=model.entity_type,
                        entity_id=model.entity_id,
                        status=model.status,
                        first_seen_at=model.first_seen_at,
                        last_seen_at=model.last_seen_at,
                        parent_entity_type=model.parent_entity_type,
                        parent_entity_id=model.parent_entity_id,
                        attributes=model.attributes or {},
                        subscription_status=model.subscription_status,
                        subscription_key=model.subscription_key,
                        subscription_target=model.subscription_target,
                        subscription_updated_at=model.subscription_updated_at,
                    )
                )
            return entities

    async def create_run(self, job_name: str, source: str) -> str:
        run_id = str(uuid4())
        with self._session_factory() as session:
            session.add(
                IngestionRunModel(
                    id=run_id,
                    source=source,
                    job_name=job_name,
                    status="running",
                )
            )
            session.commit()
        return run_id

    async def complete_run(self, run_id: str, status: str, error: str | None = None) -> None:
        with self._session_factory() as session:
            model = session.get(IngestionRunModel, run_id)
            if model is None:
                return
            model.status = status
            model.error = error
            model.finished_at = datetime.now(tz=UTC)
            session.commit()

    async def list_runs(self) -> list[dict[str, object]]:
        with self._session_factory() as session:
            stmt = select(IngestionRunModel).order_by(IngestionRunModel.started_at.desc())
            return [self._run_to_dict(model) for model in session.scalars(stmt)]

    async def get_run(self, run_id: str) -> dict[str, object] | None:
        with self._session_factory() as session:
            model = session.get(IngestionRunModel, run_id)
            return self._run_to_dict(model) if model else None

    @staticmethod
    def _run_to_dict(model: IngestionRunModel) -> dict[str, object]:
        return {
            "id": model.id,
            "source": model.source,
            "job_name": model.job_name,
            "status": model.status,
            "started_at": model.started_at.isoformat(),
            "finished_at": model.finished_at.isoformat() if model.finished_at else None,
            "error": model.error,
        }
