"""设备状态与历史存储。

- 每个设备独立维护最新值与按时间排序的有界历史，设备之间互不串扰。
- 幂等：同一设备同一 metric 的同一 seq 重复上报返回 DUPLICATE，不重复计数。
- 乱序：时间戳早于当前最新值的记录返回 STALE，仍按序插入历史但不更新最新值。
- 时效：时间戳早于当前超过 max_age 秒返回 TOO_OLD——只按序留历史痕迹，
  不更新最新值、不更新 last_seen（不会把设备判成在线）；时间戳晚于当前
  超过 future_skew 秒返回 FUTURE——不留任何痕迹，重复上报结论始终一致。
  max_age/future_skew 为 None 时不做对应判定，与历史行为完全一致。
  max_age=None 时不做太旧判定，与历史行为完全一致。
- 容量：历史超出上限时按时间从旧到新淘汰。
- 线程安全：并发上报通过锁串行化。
"""
from __future__ import annotations

import bisect
import threading
import time
from collections.abc import Callable

from app.freshness import FUTURE, TOO_OLD, classify_freshness
from app.models import DeviceState, TelemetryRecord


class IngestOutcome:
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    STALE = "stale"
    TOO_OLD = TOO_OLD
    FUTURE = FUTURE


class TelemetryStore:
    def __init__(self, history_limit: int = 100,
                 now_fn: Callable[[], float] = time.time,
                 max_age: float | None = None,
                 future_skew: float | None = None) -> None:
        if history_limit < 1:
            raise ValueError("history_limit 必须 >= 1")
        if max_age is not None and max_age < 0:
            raise ValueError("max_age 必须 >= 0 或 None")
        if future_skew is not None and future_skew < 0:
            raise ValueError("future_skew 必须 >= 0 或 None")
        self.history_limit = history_limit
        self.max_age = max_age
        self.future_skew = future_skew
        self._now = now_fn
        self._lock = threading.Lock()
        self._states: dict[str, DeviceState] = {}
        self._seen: dict[str, set[tuple[str, int]]] = {}

    def ingest(self, record: TelemetryRecord) -> str:
        with self._lock:
            now = self._now()
            freshness = classify_freshness(
                record.timestamp, now, self.max_age, self.future_skew)
            if freshness == FUTURE:
                # 未来时间戳：拒绝且不留任何痕迹（连设备空壳都不建），
                # 重复上报结论始终为 FUTURE
                return IngestOutcome.FUTURE

            state = self._states.get(record.device_id)
            if state is None:
                state = self._states[record.device_id] = DeviceState(record.device_id)
                self._seen[record.device_id] = set()

            key = (record.metric, record.seq)
            if key in self._seen[record.device_id]:
                return IngestOutcome.DUPLICATE

            # 从这里起数据会留下痕迹（正常或太旧），纳入去重
            self._seen[record.device_id].add(key)

            timestamps = [r.timestamp for r in state.history]
            pos = bisect.bisect_right(timestamps, record.timestamp)
            state.history.insert(pos, record)
            while len(state.history) > self.history_limit:
                state.history.pop(0)  # 淘汰最旧的一条

            if freshness == TOO_OLD:
                # 太旧：只留历史痕迹，不更新最新值与 last_seen（不影响在线判定）
                return IngestOutcome.TOO_OLD

            state.last_seen = now

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
