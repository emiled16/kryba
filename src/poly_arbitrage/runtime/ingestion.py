from __future__ import annotations

from dataclasses import dataclass

from poly_arbitrage.connectors.discovery import SourceRegistry
from poly_arbitrage.contracts import CheckpointStore, RunStore
from poly_arbitrage.runtime.event_bus import EventBus


@dataclass(slots=True)
class IngestionResult:
    run_id: str
    published_count: int
    checkpoint: str | None


class IngestionApplication:
    def __init__(
        self,
        *,
        run_store: RunStore,
        checkpoints: CheckpointStore,
        bus: EventBus,
        registry: SourceRegistry,
    ):
        self._run_store = run_store
        self._checkpoints = checkpoints
        self._bus = bus
        self._registry = registry

    def list_batch_jobs(self) -> list[str]:
        return [registration.spec.name for registration in self._registry.list_batch()]

    def list_stream_jobs(self) -> list[str]:
        return [registration.spec.name for registration in self._registry.list_stream()]

    async def run_job(self, job_name: str, **kwargs: object) -> IngestionResult:
        registration = self._registry.get_batch(job_name)
        run_id = await self._run_store.create_run(job_name, registration.spec.name)
        try:
            cursor = await self._checkpoints.get(registration.spec.name, job_name, "default")
            records, checkpoint = await registration.fetch(cursor=cursor, **kwargs)
            for record in records:
                await self._bus.publish(record)
            if checkpoint:
                await self._checkpoints.save(
                    registration.spec.name,
                    job_name,
                    "default",
                    checkpoint,
                )
            await self._run_store.complete_run(run_id, "succeeded")
            return IngestionResult(
                run_id=run_id,
                published_count=len(records),
                checkpoint=checkpoint,
            )
        except Exception as exc:
            await self._run_store.complete_run(run_id, "failed", error=str(exc))
            raise

    async def run_stream_job(
        self,
        job_name: str,
        *,
        max_messages: int | None = None,
    ) -> IngestionResult:
        registration = self._registry.get_stream(job_name)
        run_id = await self._run_store.create_run(job_name, registration.spec.name)
        try:
            published_count = await registration.consume(
                self._bus.publish,
                max_messages=max_messages,
            )
            await self._run_store.complete_run(run_id, "succeeded")
            return IngestionResult(
                run_id=run_id,
                published_count=published_count,
                checkpoint=None,
            )
        except Exception as exc:
            await self._run_store.complete_run(run_id, "failed", error=str(exc))
            raise


__all__ = ["IngestionApplication", "IngestionResult"]
