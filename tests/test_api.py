from __future__ import annotations

from dataclasses import dataclass

from fastapi.testclient import TestClient

from poly_arbitrage.api.app import create_app
from poly_arbitrage.runtime.ingestion import IngestionResult


class FakeRunStore:
    async def list_runs(self):
        return [{"id": "run-1", "status": "succeeded"}]

    async def get_run(self, run_id: str):
        if run_id == "run-1":
            return {"id": "run-1", "status": "succeeded"}
        return None


class FakeIngestion:
    def list_batch_jobs(self):
        return ["polymarket_markets", "polymarket_quotes"]

    def list_stream_jobs(self):
        return ["polymarket_quote_stream"]

    async def run_job(self, job_name: str):
        if job_name == "polymarket_markets":
            return IngestionResult(run_id="run-1", published_count=2, checkpoint="2")
        if job_name == "polymarket_quotes":
            return IngestionResult(
                run_id="run-2",
                published_count=4,
                checkpoint="2026-03-20T00:00:00+00:00",
            )
        raise ValueError(job_name)


@dataclass
class FakeContainer:
    ingestion: FakeIngestion
    store: FakeRunStore

    async def aclose(self) -> None:
        return None


def test_api_exposes_run_endpoints() -> None:
    app = create_app(FakeContainer(ingestion=FakeIngestion(), store=FakeRunStore()))

    with TestClient(app) as client:
        assert client.get("/healthz").json() == {"status": "ok"}
        assert client.get("/sources").json() == {
            "batch": ["polymarket_markets", "polymarket_quotes"],
            "stream": ["polymarket_quote_stream"],
        }
        assert client.post("/runs/markets").json()["published_count"] == 2
        assert client.post("/runs/polymarket_quotes").json()["published_count"] == 4
        assert client.get("/runs").json() == [{"id": "run-1", "status": "succeeded"}]
        assert client.get("/runs/run-1").status_code == 200
        assert client.post("/runs/missing").status_code == 404
        assert client.get("/runs/missing").status_code == 404
