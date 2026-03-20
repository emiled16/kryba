from __future__ import annotations

import importlib
import pkgutil
from dataclasses import dataclass
from typing import Any

from poly_arbitrage.config import Settings
from poly_arbitrage.contracts import BatchSourceRegistration, StreamSourceRegistration


@dataclass(slots=True)
class ConnectorServices:
    settings: Settings
    client: Any
    catalog: Any


class SourceRegistry:
    def __init__(self):
        self._batch_sources: dict[str, BatchSourceRegistration] = {}
        self._stream_sources: dict[str, StreamSourceRegistration] = {}

    def register_batch(self, registration: BatchSourceRegistration) -> None:
        self._batch_sources[registration.spec.name] = registration

    def register_stream(self, registration: StreamSourceRegistration) -> None:
        self._stream_sources[registration.spec.name] = registration

    def get_batch(self, job_name: str) -> BatchSourceRegistration:
        return self._batch_sources[job_name]

    def get_stream(self, job_name: str) -> StreamSourceRegistration:
        return self._stream_sources[job_name]

    def list_batch(self) -> list[BatchSourceRegistration]:
        return list(self._batch_sources.values())

    def list_stream(self) -> list[StreamSourceRegistration]:
        return list(self._stream_sources.values())


def discover_source_registry(services: ConnectorServices) -> SourceRegistry:
    registry = SourceRegistry()
    package = importlib.import_module("poly_arbitrage.connectors")
    for module_info in pkgutil.iter_modules(package.__path__):
        plugin_module_name = f"{package.__name__}.{module_info.name}.plugin"
        try:
            plugin_module = importlib.import_module(plugin_module_name)
        except ModuleNotFoundError:
            continue
        register = getattr(plugin_module, "register", None)
        if callable(register):
            register(registry, services)
    return registry
