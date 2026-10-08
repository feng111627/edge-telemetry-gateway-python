"""接入质量汇总：按设备统计各类接入结果与补偿积压。

- 计数维度：accepted（正常收下并成为最新值）、stale（乱序收下）、
  duplicate（重复去重）、too_old（太旧）、future（未来时间戳）、
  rejected（归一化阶段被拒绝）。
- pending_retry / dead 不是累计计数，而是读取时从重试队列现算的当前值。
- 所有计数在锁内完成，多设备并发上报时读数不会算乱。
"""
from __future__ import annotations

import threading

from app.retry import RetryQueue
from app.store import IngestOutcome

# 无法从上报中解析出设备时，拒绝计数归入这个桶
UNKNOWN_DEVICE = "<unknown>"

_COUNTER_KEYS = ("accepted", "stale", "duplicate", "too_old", "future", "rejected")

_OUTCOME_TO_KEY = {
    IngestOutcome.ACCEPTED: "accepted",
    IngestOutcome.STALE: "stale",
    IngestOutcome.DUPLICATE: "duplicate",
    IngestOutcome.TOO_OLD: "too_old",
    IngestOutcome.FUTURE: "future",
}


class IngestMetrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[str, dict[str, int]] = {}

    def _bucket(self, device_id: str) -> dict[str, int]:
        bucket = self._counters.get(device_id)
        if bucket is None:
            bucket = self._counters[device_id] = {
                key: 0 for key in _COUNTER_KEYS}
        return bucket

    def record_outcome(self, device_id: str, outcome: str) -> None:
        key = _OUTCOME_TO_KEY.get(outcome)
        if key is None:
            raise ValueError(f"未知接入结果: {outcome!r}")
        with self._lock:
            self._bucket(device_id)[key] += 1

    def record_rejected(self, device_id: str | None) -> None:
        with self._lock:
            self._bucket(device_id or UNKNOWN_DEVICE)["rejected"] += 1

    def summary(self, retry_queue: RetryQueue) -> dict[str, dict[str, int]]:
        """按设备汇总计数，并叠加当前重试队列的积压/放弃数。"""
        with self._lock:
            devices = {dev: dict(bucket) for dev, bucket in self._counters.items()}
        pending = retry_queue.pending_count_by_device()
        dead = retry_queue.dead_count_by_device()
        for dev in set(pending) | set(dead):
            devices.setdefault(dev, {key: 0 for key in _COUNTER_KEYS})
        for dev, bucket in devices.items():
            bucket["pending_retry"] = pending.get(dev, 0)
            bucket["dead"] = dead.get(dev, 0)
        return dict(sorted(devices.items()))
