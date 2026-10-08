"""设备状态与历史存储。

- 每个设备独立维护最新值与按时间排序的有界历史，设备之间互不串扰。
- 幂等：同一设备同一 metric 的同一 seq 重复上报返回 DUPLICATE，不重复计数。
- 乱序：时间戳早于当前最新值的记录返回 STALE，仍按序插入历史但不更新最新值。
- 时效：时间戳早于 now - max_past_age 的记录返回 TOO_OLD，晚于
  now + max_future_skew 的记录返回 FUTURE；两者都不改动设备任何状态
  （不更新最新值、不写入历史、不刷新 last_seen），同一条数据重复上报
  结论一致。两者与「时间戳无法解析」（归一化阶段拒绝）是不同情况。
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
    TOO_OLD = "too_old"
    FUTURE = "future"


class TelemetryStore:
    def __init__(self, history_limit: int = 100,
                 now_fn: Callable[[], float] = time.time,
                 max_past_age: float | None = None,
                 max_future_skew: float | None = None) -> None:
        if history_limit < 1:
            raise ValueError("history_limit 必须 >= 1")
        if max_past_age is not None and max_past_age <= 0:
            raise ValueError("max_past_age 必须 > 0")
        if max_future_skew is not None and max_future_skew < 0:
            raise ValueError("max_future_skew 必须 >= 0")
        self.history_limit = history_limit
        self.max_past_age = max_past_age
        self.max_future_skew = max_future_skew
        self._now = now_fn
        self._lock = threading.Lock()
        self._states: dict[str, DeviceState] = {}
        self._seen: dict[str, set[tuple[str, int]]] = {}

    def ingest(self, record: TelemetryRecord) -> str:
        with self._lock:
            state = self._states.get(record.device_id)
            key = (record.metric, record.seq)
            if state is not None and key in self._seen[record.device_id]:
                return IngestOutcome.DUPLICATE

            now = self._now()
            # 时效判定在任何状态变更之前：太旧或未来时间戳的记录
            # 不创建设备、不刷新 last_seen、不写历史，结论确定且可重复。
            if (self.max_past_age is not None
                    and record.timestamp < now - self.max_past_age):
                return IngestOutcome.TOO_OLD
            if (self.max_future_skew is not None
                    and record.timestamp > now + self.max_future_skew):
                return IngestOutcome.FUTURE

            if state is None:
                state = self._states[record.device_id] = DeviceState(record.device_id)
                self._seen[record.device_id] = set()
            self._seen[record.device_id].add(key)

            state.last_seen = now

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
