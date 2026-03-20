from __future__ import annotations

import json
from collections import deque
from collections.abc import Awaitable, Callable
from typing import Protocol

from poly_arbitrage.contracts import RawRecord


class EventBus(Protocol):
    async def publish(self, record: RawRecord) -> None:
        """Publish a raw record."""

    async def consume(
        self,
        handler: Callable[[RawRecord], Awaitable[None]],
        *,
        max_messages: int | None = None,
    ) -> int:
        """Consume records and pass them to the handler."""


class InMemoryEventBus:
    def __init__(self):
        self._queue: deque[RawRecord] = deque()

    async def publish(self, record: RawRecord) -> None:
        self._queue.append(record)

    async def consume(
        self,
        handler: Callable[[RawRecord], Awaitable[None]],
        *,
        max_messages: int | None = None,
    ) -> int:
        processed = 0
        while self._queue and (max_messages is None or processed < max_messages):
            await handler(self._queue.popleft())
            processed += 1
        return processed


class KafkaEventBus:
    def __init__(self, bootstrap_servers: str, topic: str):
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic

    async def publish(self, record: RawRecord) -> None:
        try:
            from kafka import KafkaProducer
        except ImportError as exc:
            raise RuntimeError("kafka-python-ng dependency is required for KafkaEventBus") from exc

        producer = KafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        )
        try:
            producer.send(self._topic, record.to_dict()).get(timeout=10)
        finally:
            producer.flush()
            producer.close()

    async def consume(
        self,
        handler: Callable[[RawRecord], Awaitable[None]],
        *,
        max_messages: int | None = None,
    ) -> int:
        try:
            from kafka import KafkaConsumer
        except ImportError as exc:
            raise RuntimeError("kafka-python-ng dependency is required for KafkaEventBus") from exc

        consumer = KafkaConsumer(
            self._topic,
            bootstrap_servers=self._bootstrap_servers,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            consumer_timeout_ms=1000,
        )
        processed = 0
        try:
            for message in consumer:
                await handler(RawRecord.from_dict(message.value))
                consumer.commit()
                processed += 1
                if max_messages is not None and processed >= max_messages:
                    break
        finally:
            consumer.close()
        return processed
