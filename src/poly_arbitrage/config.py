from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(slots=True)
class Settings:
    database_url: str
    kafka_bootstrap_servers: str
    kafka_raw_topic: str
    storage_bucket: str
    storage_prefix: str
    storage_endpoint_url: str | None
    storage_region: str
    storage_access_key_id: str | None
    storage_secret_access_key: str | None
    api_host: str
    api_port: int
    log_level: str
    polymarket_gamma_base_url: str
    polymarket_clob_base_url: str
    dagster_home: str

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            database_url=os.getenv(
                "DATABASE_URL",
                "postgresql+psycopg://postgres:postgres@localhost:5432/poly_arbitrage",
            ),
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            kafka_raw_topic=os.getenv("KAFKA_RAW_TOPIC", "raw-records"),
            storage_bucket=os.getenv("STORAGE_BUCKET", "raw-ingestion"),
            storage_prefix=os.getenv("STORAGE_PREFIX", "raw"),
            storage_endpoint_url=os.getenv("STORAGE_ENDPOINT_URL") or None,
            storage_region=os.getenv("STORAGE_REGION", "us-east-1"),
            storage_access_key_id=os.getenv("STORAGE_ACCESS_KEY_ID") or None,
            storage_secret_access_key=os.getenv("STORAGE_SECRET_ACCESS_KEY") or None,
            api_host=os.getenv("API_HOST", "0.0.0.0"),
            api_port=int(os.getenv("API_PORT", "8000")),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            polymarket_gamma_base_url=os.getenv(
                "POLYMARKET_GAMMA_BASE_URL",
                os.getenv("POLYMARKET_BASE_URL", "https://gamma-api.polymarket.com"),
            ),
            polymarket_clob_base_url=os.getenv(
                "POLYMARKET_CLOB_BASE_URL", "https://clob.polymarket.com"
            ),
            dagster_home=os.getenv("DAGSTER_HOME", ".dagster"),
        )
