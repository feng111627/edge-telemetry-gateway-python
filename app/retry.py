"""下游处理失败数据的暂存、退避重试与最终放弃。

- push：失败的数据进入暂存队列，立即到期可重试。
- due：只返回到期的条目，未到期的继续等待——单条反复失败不会拖住其他数据。
- 退避：第 n 次失败后等待 base_delay * 2**(n-1) 秒，间隔逐步拉长，
  但不超过 max_delay（默认 300 秒），不会无限往后拖。
- 超过 max_attempts 次失败则放弃（移出 pending，计入 dead）。
- redeliver：dead 集合可逐条或全部手动重新投递；同一时刻每条数据只会
  被投递一次（_inflight 去重），重新投递不产生重复计数。
- process_due：原子地取走到期条目并逐条独立投递，单条异常/卡死都不
  影响其它条目，也不阻塞新数据接入（队列所有操作均为短临界区）。
"""
from __future__ import annotations

import itertools
import threading
from collections.abc import Callable
from dataclasses import dataclass

from app.models import TelemetryRecord


@dataclass
class RetryItem:
    record: TelemetryRecord
    item_id: int
    attempts: int = 0
    next_attempt_at: float = 0.0


@dataclass
class DeadEntry:
    """最终放弃的条目：保留身份与尝试次数，供查看与手动重新投递。"""

    item_id: int
    record: TelemetryRecord
    attempts: int


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
        self._ids = itertools.count(1)
        self._items: list[RetryItem] = []
        self._inflight: set[int] = set()
        # dead 保留为 record 列表（兼容既有查看方式），_dead_entries 带身份。
        self.dead: list[TelemetryRecord] = []
        self._dead_entries: list[DeadEntry] = []

    def push(self, record: TelemetryRecord, now: float) -> RetryItem:
        with self._lock:
            item = RetryItem(record=record, item_id=next(self._ids),
                             next_attempt_at=now)
            self._items.append(item)
            return item

    def due(self, now: float) -> list[RetryItem]:
        with self._lock:
            return [it for it in self._items if it.next_attempt_at <= now]

    def ack(self, item: RetryItem) -> None:
        with self._lock:
            if item in self._items:
                self._items.remove(item)
            self._inflight.discard(item.item_id)

    def fail(self, item: RetryItem, now: float) -> bool:
        """记录一次失败；返回是否还会重试（False 表示已放弃）。"""
        with self._lock:
            item.attempts += 1
            if item.attempts >= self.max_attempts:
                if item in self._items:
                    self._items.remove(item)
                self._inflight.discard(item.item_id)
                self.dead.append(item.record)
                self._dead_entries.append(
                    DeadEntry(item.item_id, item.record, item.attempts))
                return False
            delay = min(self.max_delay,
                        self.base_delay * (2 ** (item.attempts - 1)))
            item.next_attempt_at = now + delay
            self._inflight.discard(item.item_id)
            return True

    def claim_due(self, now: float) -> list[RetryItem]:
        """原子地取走当前到期且未在投递中的条目；调用方负责 ack/fail。"""
        with self._lock:
            claimed = [it for it in self._items
                       if it.next_attempt_at <= now
                       and it.item_id not in self._inflight]
            self._inflight.update(it.item_id for it in claimed)
            return claimed

    def process_due(self, sender: Callable[[TelemetryRecord], bool],
                    now: float) -> dict[str, int]:
        """投递所有到期条目，逐条独立成败。返回处理统计。"""
        processed = succeeded = 0
        for item in self.claim_due(now):
            processed += 1
            try:
                ok = sender(item.record)
            except Exception:
                ok = False
            if ok:
                self.ack(item)
                succeeded += 1
            else:
                self.fail(item, now)
        return {"processed": processed, "succeeded": succeeded,
                "pending": len(self), "dead": len(self.dead)}

    # ---- 最终放弃集合（dead-letter）的查看与手动重新投递 ----

    def list_dead(self) -> list[DeadEntry]:
        with self._lock:
            return list(self._dead_entries)

    def redeliver(self, now: float,
                  item_id: int | None = None) -> list[int]:
        """把 dead 中的一条（item_id）或全部重新放回 pending。

        尝试次数清零、立即到期；返回重新投递条目的 item_id 列表。
        同一条 dead 只会被重新投递一次（原子移出），不会重复计数。
        """
        with self._lock:
            if item_id is None:
                entries = self._dead_entries
            else:
                entries = [e for e in self._dead_entries if e.item_id == item_id]
            if not entries:
                return []
            self._dead_entries = [e for e in self._dead_entries
                                  if e not in entries]
            self.dead = [r for r in self.dead if not any(
                r is e.record for e in entries)]
            requeued = []
            for entry in entries:
                new_item = RetryItem(record=entry.record,
                                     item_id=next(self._ids),
                                     next_attempt_at=now)
                self._items.append(new_item)
                requeued.append(entry.item_id)
            return requeued

    # ---- 按设备的队列计数（供接入质量汇总） ----

    def counts_by_device(self) -> tuple[dict[str, int], dict[str, int]]:
        with self._lock:
            pending: dict[str, int] = {}
            for it in self._items:
                pending[it.record.device_id] = pending.get(it.record.device_id, 0) + 1
            dead: dict[str, int] = {}
            for record in self.dead:
                dead[record.device_id] = dead.get(record.device_id, 0) + 1
            return pending, dead

    def __len__(self) -> int:
        with self._lock:
            return len(self._items)
