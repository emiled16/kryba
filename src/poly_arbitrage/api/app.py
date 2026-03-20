from __future__ import annotations

from contextlib import asynccontextmanager
from typing import cast

import uvicorn
from fastapi import FastAPI, HTTPException

from poly_arbitrage.config import Settings
from poly_arbitrage.runtime.bootstrap import ApplicationRuntime, build_runtime


def create_app(runtime: ApplicationRuntime | None = None) -> FastAPI:
    app = FastAPI(title="Poly Arbitrage Control API")

    @asynccontextmanager
    async def lifespan(app_instance: FastAPI):
        app_instance.state.runtime = runtime or build_runtime()
        try:
            yield
        finally:
            await _runtime(app_instance).aclose()

    app.router.lifespan_context = lifespan

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz")
    async def readyz() -> dict[str, str]:
        return {"status": "ready"}

    @app.get("/sources")
    def list_sources() -> dict[str, list[str]]:
        return {
            "batch": _runtime(app).ingestion.list_batch_jobs(),
            "stream": _runtime(app).ingestion.list_stream_jobs(),
        }

    @app.post("/runs/markets")
    async def run_markets() -> dict[str, object]:
        return await run_job("polymarket_markets")

    @app.post("/runs/quotes")
    async def run_quotes() -> dict[str, object]:
        return await run_job("polymarket_quotes")

    @app.post("/runs/{job_name}")
    async def run_job(job_name: str) -> dict[str, object]:
        if job_name not in _runtime(app).ingestion.list_batch_jobs():
            raise HTTPException(status_code=404, detail="job not found")
        result = await _runtime(app).ingestion.run_job(job_name)
        return {
            "run_id": result.run_id,
            "published_count": result.published_count,
            "checkpoint": result.checkpoint,
        }

    @app.get("/runs")
    async def list_runs() -> list[dict[str, object]]:
        return await _runtime(app).store.list_runs()

    @app.get("/runs/{run_id}")
    async def get_run(run_id: str) -> dict[str, object]:
        run = await _runtime(app).store.get_run(run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="run not found")
        return run

    return app


def _runtime(app: FastAPI) -> ApplicationRuntime:
    return cast(ApplicationRuntime, app.state.runtime)


app = create_app()


def run() -> None:
    settings = Settings.from_env()
    uvicorn.run(
        "poly_arbitrage.api.app:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=False,
    )


if __name__ == "__main__":
    run()
