from __future__ import annotations

import json

import boto3

from poly_arbitrage.config import Settings


class S3BlobWriter:
    def __init__(self, settings: Settings):
        client_kwargs: dict[str, str] = {"region_name": settings.storage_region}
        if settings.storage_endpoint_url:
            client_kwargs["endpoint_url"] = settings.storage_endpoint_url
        if settings.storage_access_key_id:
            client_kwargs["aws_access_key_id"] = settings.storage_access_key_id
        if settings.storage_secret_access_key:
            client_kwargs["aws_secret_access_key"] = settings.storage_secret_access_key
        self._bucket = settings.storage_bucket
        self._client = boto3.client("s3", **client_kwargs)

    async def write_json(self, key: str, payload: dict[str, object]) -> str:
        body = json.dumps(payload, sort_keys=True).encode("utf-8")
        self._client.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=body,
            ContentType="application/json",
        )
        return f"s3://{self._bucket}/{key}"
