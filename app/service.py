"""编排层：归一化 -> 存储 -> 下游投递（失败入重试队列）。"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from typing import Any

from .config import Settings, settings as default_settings
from .errors import IngestError
from .models import IngestResult, Liveness
from .normalizer import normalize
from .retry_queue import Processor, RetryQueue
from .store import DeviceStore


class TelemetryService:
    def __init__(
        self,
        settings: Settings = default_settings,
        *,
        time_func: Callable[[], float] = time.time,
        processor: Processor | None = None,
    ) -> None:
        self._settings = settings
        self._time = time_func
        self.store = DeviceStore(settings)
        self.retry_queue = RetryQueue(settings, time_func=time_func)
        self._processor: Processor | None = processor

    def set_processor(self, processor: Processor) -> None:
        self._processor = processor

    async def ingest_one(self, payload: Any) -> IngestResult:
        raise NotImplementedError

    async def ingest_batch(self, payloads: list[Any]) -> list[IngestResult]:
        raise NotImplementedError

    async def retry_pending(self) -> dict[str, int]:
        raise NotImplementedError

    def status(self, device_id: str) -> dict[str, Any]:
        raise NotImplementedError

    def liveness(self, device_id: str) -> Liveness:
        raise NotImplementedError
