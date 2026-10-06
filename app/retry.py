"""下游处理失败数据的暂存与退避重试。

- push：失败的数据进入暂存队列。
- due：只返回到期的条目，未到期的继续等待——单条反复失败不会拖住其他数据。
- 退避：第 n 次失败后等待 base_delay * 2**(n-1) 秒，间隔逐步拉长。
- 超过 max_attempts 次失败则放弃（移出队列，计入 dead）。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field

from app.models import TelemetryRecord


@dataclass
class RetryItem:
    record: TelemetryRecord
    attempts: int = 0
    next_attempt_at: float = 0.0


class RetryQueue:
    def __init__(self, base_delay: float = 1.0, max_attempts: int = 5) -> None:
        if base_delay <= 0:
            raise ValueError("base_delay 必须 > 0")
        if max_attempts < 1:
            raise ValueError("max_attempts 必须 >= 1")
        self.base_delay = base_delay
        self.max_attempts = max_attempts
        self._lock = threading.Lock()
        self._items: list[RetryItem] = []
        self.dead: list[TelemetryRecord] = []

    def push(self, record: TelemetryRecord, now: float) -> None:
        with self._lock:
            self._items.append(RetryItem(record=record, next_attempt_at=now))

    def due(self, now: float) -> list[RetryItem]:
        with self._lock:
            return [it for it in self._items if it.next_attempt_at <= now]

    def ack(self, item: RetryItem) -> None:
        with self._lock:
            if item in self._items:
                self._items.remove(item)

    def fail(self, item: RetryItem, now: float) -> bool:
        """记录一次失败；返回是否还会重试（False 表示已放弃）。"""
        with self._lock:
            item.attempts += 1
            if item.attempts >= self.max_attempts:
                if item in self._items:
                    self._items.remove(item)
                self.dead.append(item.record)
                return False
            item.next_attempt_at = now + self.base_delay * (2 ** (item.attempts - 1))
            return True

    def __len__(self) -> int:
        with self._lock:
            return len(self._items)
