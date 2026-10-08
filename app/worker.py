"""后台自动补偿：持续把重试队列中到期的数据重新投递给下游。

- RetryWorker 在独立线程里按固定间隔唤醒，调用 drain_due 处理到期条目；
  接入路径只负责把失败数据 push 进队列，两者互不阻塞。
- drain_due 逐条处理：转发在队列锁之外进行，单条一直失败只会拉长自己
  的退避，不会拖住其它数据。
- /retry/run 手动触发与后台线程共用同一个 drain_due，语义完全一致。
"""
from __future__ import annotations

import threading
import time
from collections.abc import Callable

from app.models import TelemetryRecord
from app.retry import RetryQueue


def drain_due(queue: RetryQueue,
              deliver: Callable[[TelemetryRecord], bool],
              now: float) -> tuple[int, int]:
    """处理当前到期的重试条目，返回 (处理条数, 成功条数)。"""
    processed = succeeded = 0
    for item in queue.due(now):
        processed += 1
        try:
            ok = deliver(item.record)
        except Exception:
            ok = False
        if ok:
            queue.ack(item)
            succeeded += 1
        else:
            queue.fail(item, now)
    return processed, succeeded


class RetryWorker:
    """按固定间隔自动补偿失败数据的后台线程。"""

    def __init__(self, queue: RetryQueue,
                 deliver: Callable[[TelemetryRecord], bool],
                 interval: float = 1.0,
                 now_fn: Callable[[], float] = time.time) -> None:
        if interval <= 0:
            raise ValueError("interval 必须 > 0")
        self.queue = queue
        self.deliver = deliver
        self.interval = interval
        self._now = now_fn
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def run_once(self) -> tuple[int, int]:
        """处理一轮到期条目（供测试与手动触发复用）。"""
        return drain_due(self.queue, self.deliver, self._now())

    def _loop(self) -> None:
        while not self._stop.wait(self.interval):
            self.run_once()

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, name="retry-worker", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None
