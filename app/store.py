"""设备状态与历史存储。

- 每个设备独立维护最新值与按时间排序的有界历史，设备之间互不串扰。
- 幂等：同一设备同一 metric 的同一 seq 重复上报返回 DUPLICATE，不重复计数。
- 乱序：时间戳早于当前最新值的记录返回 STALE，仍按序插入历史但不更新最新值。
- 容量：历史超出上限时按时间从旧到新淘汰。
- 线程安全：并发上报通过锁串行化。
"""
from __future__ import annotations

import bisect
import threading
import time
from collections.abc import Callable

from app.models import DeviceState, TelemetryRecord


class IngestOutcome:
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    STALE = "stale"


class TelemetryStore:
    def __init__(self, history_limit: int = 100,
                 now_fn: Callable[[], float] = time.time) -> None:
        if history_limit < 1:
            raise ValueError("history_limit 必须 >= 1")
        self.history_limit = history_limit
        self._now = now_fn
        self._lock = threading.Lock()
        self._states: dict[str, DeviceState] = {}
        self._seen: dict[str, set[tuple[str, int]]] = {}

    def ingest(self, record: TelemetryRecord) -> str:
        with self._lock:
            state = self._states.get(record.device_id)
            if state is None:
                state = self._states[record.device_id] = DeviceState(record.device_id)
                self._seen[record.device_id] = set()

            key = (record.metric, record.seq)
            if key in self._seen[record.device_id]:
                return IngestOutcome.DUPLICATE
            self._seen[record.device_id].add(key)

            state.last_seen = self._now()

            timestamps = [r.timestamp for r in state.history]
            pos = bisect.bisect_right(timestamps, record.timestamp)
            state.history.insert(pos, record)
            while len(state.history) > self.history_limit:
                state.history.pop(0)  # 淘汰最旧的一条

            if state.latest is None or record.timestamp >= state.latest.timestamp:
                state.latest = record
                return IngestOutcome.ACCEPTED
            return IngestOutcome.STALE

    def get_state(self, device_id: str) -> DeviceState | None:
        with self._lock:
            state = self._states.get(device_id)
            if state is None:
                return None
            # 返回拷贝，避免调用方修改内部状态
            return DeviceState(
                device_id=state.device_id,
                latest=state.latest,
                history=list(state.history),
                last_seen=state.last_seen,
            )

    def list_devices(self) -> list[str]:
        with self._lock:
            return sorted(self._states)
