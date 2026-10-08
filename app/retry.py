"""下游处理失败数据的暂存与退避重试。

- push：失败的数据进入暂存队列。
- due：只返回到期的条目，未到期的继续等待——单条反复失败不会拖住其他数据。
- 退避：第 n 次失败后等待 min(base_delay * 2**(n-1), max_delay) 秒，
  间隔逐步拉长但有上限，不会无限往后拖。
- 超过 max_attempts 次失败则放弃（移入 dead 集合，可查询）。
- redrive：把 dead 中的某一条或全部重新放回队列等待投递；条目是「移动」
  而非「复制」，同一数据不会被重复送给下游，也不影响接入侧计数。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass

from app.models import TelemetryRecord


@dataclass
class RetryItem:
    record: TelemetryRecord
    attempts: int = 0
    next_attempt_at: float = 0.0


class RetryQueue:
    def __init__(self, base_delay: float = 1.0, max_attempts: int = 5,
                 max_delay: float = 300.0) -> None:
        if base_delay <= 0:
            raise ValueError("base_delay 必须 > 0")
        if max_attempts < 1:
            raise ValueError("max_attempts 必须 >= 1")
        if max_delay < base_delay:
            raise ValueError("max_delay 必须 >= base_delay")
        self.base_delay = base_delay
        self.max_attempts = max_attempts
        self.max_delay = max_delay
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
            delay = min(self.base_delay * (2 ** (item.attempts - 1)), self.max_delay)
            item.next_attempt_at = now + delay
            return True

    def redrive(self, now: float, index: int | None = None) -> int:
        """把 dead 中的记录重新放回队列等待投递，返回重新投递的条数。

        index 为 None 时重新投递全部；否则只重新投递 dead 中的第 index 条。
        记录从 dead 中移除后才入队，同一数据不会重复投递。
        """
        with self._lock:
            if index is not None:
                if index < 0 or index >= len(self.dead):
                    raise IndexError(f"dead 下标越界: {index}")
                chosen = [self.dead.pop(index)]
            else:
                chosen = list(self.dead)
                self.dead.clear()
            for record in chosen:
                self._items.append(RetryItem(record=record, next_attempt_at=now))
            return len(chosen)

    def dead_count_by_device(self) -> dict[str, int]:
        with self._lock:
            counts: dict[str, int] = {}
            for record in self.dead:
                counts[record.device_id] = counts.get(record.device_id, 0) + 1
            return counts

    def pending_count_by_device(self) -> dict[str, int]:
        with self._lock:
            counts: dict[str, int] = {}
            for item in self._items:
                dev = item.record.device_id
                counts[dev] = counts.get(dev, 0) + 1
            return counts

    def __len__(self) -> int:
        with self._lock:
            return len(self._items)
