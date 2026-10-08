"""下游失败数据的后台自动补偿。

RetryWorker 在守护线程中周期扫描 RetryQueue：
- 到期的失败数据自动反复投递，退避间隔由 RetryQueue 决定（指数增长、有上限）；
- 超过最大尝试次数自动进入 dead 集合，不再自动重试；
- 每条数据独立处理，单条一直失败/抛异常只会按退避继续等待，
  不会拖累其他数据，也不会卡住新数据接入（一轮只处理当批到期条目，
  队列操作均为短临界区）；
- 停止后台线程不会丢失数据：条目仍在 RetryQueue 中，可手动 run/重投。
"""
from __future__ import annotations

import threading
import time
from collections.abc import Callable

from app.models import TelemetryRecord
from app.retry import RetryQueue


class RetryWorker:
    def __init__(self, queue: RetryQueue,
                 sender: Callable[[TelemetryRecord], bool],
                 interval: float = 1.0,
                 now_fn: Callable[[], float] = time.time,
                 sleep_fn: Callable[[float], None] = time.sleep) -> None:
        if interval <= 0:
            raise ValueError("interval 必须 > 0")
        self._queue = queue
        self._sender = sender
        self._interval = interval
        self._now = now_fn
        self._sleep = sleep_fn
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self, timeout: float | None = 1.0) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout)

    def run_once(self) -> dict[str, int]:
        """执行一轮补偿（也可脱离线程单独调用）。"""
        return self._queue.process_due(self._sender, self._now())

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.run_once()
            except Exception:
                # 补偿循环本身永不因单轮异常而退出
                pass
            # 可被 stop 立即唤醒
            self._stop.wait(self._interval)
