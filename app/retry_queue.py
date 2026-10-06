"""下游失败数据的暂存与指数退避重试队列。

设计要点：

- 按 ``next_visible``（下次可见时间）排序的最小堆，``pump_once`` 只处理
  已到期的条目；反复失败、退避时间较长的条目会被跳过，**不会拖住**
  后续进入队列的数据。
- 重试间隔指数退避：``min(base * backoff**attempts, max_delay)``。
- 超过最大重试次数的条目进入死信队列（dead letter），不再参与重试，
  但仍然可查询、可留痕。
"""

from __future__ import annotations

import heapq
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
        self._heap: list[_Entry] = []
        self._dead_letter: list[_Entry] = []
        self._seq = 0
        self.stats_enqueued = 0
        self.stats_delivered = 0
        self.stats_failed = 0  # 投递失败次数（含重试中的失败）
        self.stats_dead = 0

    def __len__(self) -> int:
        return len(self._heap)

    def enqueue(self, record: NormalizedRecord, *, attempts: int = 0, next_visible: float | None = None) -> None:
        """暂存一条失败数据。attempts=0 表示首次失败，立即可重试。"""
        entry = _Entry(
            next_visible=self._time() if next_visible is None else next_visible,
            seq=self._seq,
            record=record,
            attempts=attempts,
        )
        self._seq += 1
        heapq.heappush(self._heap, entry)
        self.stats_enqueued += 1

    def due_entries(self) -> list[_Entry]:
        """查看（不移除）当前已到期的条目，按到期时间排序。"""
        now = self._time()
        return [e for e in self._heap if e.next_visible <= now]

    def _delay_for(self, attempts: int) -> float:
        # attempts 为“已经失败过的次数”：第一次失败后等 base，之后指数拉长
        delay = self._settings.retry_base_delay * self._settings.retry_backoff ** max(attempts - 1, 0)
        return min(delay, self._settings.retry_max_delay)

    async def pump_once(self, processor: Processor) -> dict[str, int]:
        """处理一轮所有已到期条目，返回本轮统计。

        未到期的条目留在堆中；处理过程中新到期或退避回来的条目本轮不处理，
        保证单轮有界、单条数据反复失败不会阻塞其它条目。
        """
        ready: list[_Entry] = []
        deferred: list[_Entry] = []
        now = self._time()
        while self._heap:
            entry = heapq.heappop(self._heap)
            (ready if entry.next_visible <= now else deferred).append(entry)
        for entry in deferred:
            heapq.heappush(self._heap, entry)

        delivered = failed = dead = 0
        for entry in ready:
            try:
                await processor(entry.record)
            except Exception:
                self.stats_failed += 1
                failed += 1
                entry.attempts += 1
                if entry.attempts >= self._settings.retry_max_attempts:
                    self._dead_letter.append(entry)
                    self.stats_dead += 1
                    dead += 1
                else:
                    entry.next_visible = self._time() + self._delay_for(entry.attempts)
                    heapq.heappush(self._heap, entry)
            else:
                self.stats_delivered += 1
                delivered += 1
        return {"processed": len(ready), "delivered": delivered, "failed": failed, "dead": dead}

    def stats(self) -> dict[str, int]:
        return {
            "pending": len(self._heap),
            "dead_letter": len(self._dead_letter),
            "enqueued": self.stats_enqueued,
            "delivered": self.stats_delivered,
            "failed": self.stats_failed,
        }

    def dead_letters(self) -> list[NormalizedRecord]:
        return [e.record for e in self._dead_letter]
