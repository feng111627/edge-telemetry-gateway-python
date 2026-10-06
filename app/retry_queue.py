"""下游失败数据的暂存与退避重试队列。"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from .config import Settings
from .models import NormalizedRecord

Processor = Callable[[NormalizedRecord], Awaitable[None]]


@dataclass(order=True)
class _Entry:
    next_visible: float
    seq: int
    record: NormalizedRecord = field(compare=False)
    attempts: int = field(default=0, compare=False)


class RetryQueue:
    def __init__(self, settings: Settings, *, time_func: Callable[[], float]) -> None:
        self._settings = settings
        self._time = time_func
        self._pending: list[_Entry] = []
        self._dead_letter: list[_Entry] = []
        self._seq = 0
        self._enqueued = 0
        self._delivered = 0
        self._failed = 0

    def __len__(self) -> int:
        return len(self._pending)

    def enqueue(self, record: NormalizedRecord) -> None:
        raise NotImplementedError

    def due_entries(self) -> list[_Entry]:
        raise NotImplementedError

    def _delay_for(self, attempts: int) -> float:
        raise NotImplementedError

    async def pump_once(self, processor: Processor) -> dict[str, int]:
        raise NotImplementedError

    def stats(self) -> dict[str, int]:
        raise NotImplementedError

    def dead_letters(self) -> list[NormalizedRecord]:
        raise NotImplementedError
