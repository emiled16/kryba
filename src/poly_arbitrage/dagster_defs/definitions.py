from __future__ import annotations

import asyncio

try:
    from dagster import Definitions, ScheduleDefinition, job, op
except ImportError:  # pragma: no cover - exercised only when dagster is missing
    defs = None
else:
    from poly_arbitrage.contracts import BatchSourceRegistration
    from poly_arbitrage.runtime.bootstrap import build_runtime

    def _run_batch_job(job_name: str) -> dict[str, object]:
        runtime = build_runtime()
        try:
            result = asyncio.run(runtime.ingestion.run_job(job_name))
            return {"run_id": result.run_id, "published_count": result.published_count}
        finally:
            asyncio.run(runtime.aclose())

    def _build_job(job_name: str):
        @op(name=f"{job_name}_op")
        def _op() -> dict[str, object]:
            return _run_batch_job(job_name)

        @job(name=f"{job_name}_job")
        def _job():
            _op()

        return _job

    def _load_batch_registrations() -> list[BatchSourceRegistration]:
        runtime = build_runtime()
        try:
            return runtime.source_registry.list_batch()
        finally:
            asyncio.run(runtime.aclose())

    def _build_schedules(
        batch_registrations: list[BatchSourceRegistration],
        jobs: list,
    ) -> list[ScheduleDefinition]:
        schedules = []
        for registration, dagster_job in zip(batch_registrations, jobs, strict=True):
            if registration.spec.cron_schedule:
                schedules.append(
                    ScheduleDefinition(
                        job=dagster_job,
                        cron_schedule=registration.spec.cron_schedule,
                        name=f"{registration.spec.name}_schedule",
                    )
                )
        return schedules

    def _build_definitions() -> Definitions:
        batch_registrations = _load_batch_registrations()
        jobs = [_build_job(registration.spec.name) for registration in batch_registrations]
        schedules = _build_schedules(batch_registrations, jobs)
        return Definitions(jobs=jobs, schedules=schedules)

    defs = _build_definitions()
