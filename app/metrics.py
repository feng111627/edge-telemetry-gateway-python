"""按设备的接入质量汇总。

逐设备统计接入结论：
- accepted：正常收下（含乱序 stale，数据本身有效）；
- duplicate：重复上报；
- too_old：时间戳过旧，只留历史痕迹；
- future：时间戳落在未来；
- rejected：归一化阶段被拒（not_an_object/missing_field/bad_type/
  bad_timestamp/unknown_source 等可区分原因统一计入此项）。

等待补偿（pending）与最终放弃（dead）条数不属于接入计数，在出快照时
从重试队列实时并入，避免两处状态各自漂移。

多设备并发上报时通过锁串行化计数，读快照拿到的是一致的瞬时值。
"""
from __future__ import annotations

import threading
from collections.abc import Mapping

# 每个设备维护的计数桶及对外顺序。
COUNTER_KEYS = ("accepted", "duplicate", "too_old", "future", "rejected")

# 接入结果 -> 计数桶。stale 是有效数据，计入 accepted。
_OUTCOME_BUCKET = {
    "accepted": "accepted",
    "stale": "accepted",
    "duplicate": "duplicate",
    "too_old": "too_old",
    "future": "future",
}


class QualityMetrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counts: dict[str, dict[str, int]] = {}

    def _bucket(self, device_id: str) -> dict[str, int]:
        bucket = self._counts.get(device_id)
        if bucket is None:
            bucket = {key: 0 for key in COUNTER_KEYS}
            self._counts[device_id] = bucket
        return bucket

    def record(self, device_id: str, outcome: str) -> None:
        """记录一条已有确定接入结论（store 的 ingest 结果）的上报。"""
        name = _OUTCOME_BUCKET.get(outcome)
        if name is None:
            raise ValueError(f"未知接入结论: {outcome!r}")
        with self._lock:
            self._bucket(device_id)[name] += 1

    def reject(self, device_id: str) -> None:
        """记录一条归一化阶段被拒的上报。"""
        with self._lock:
            self._bucket(device_id)["rejected"] += 1

    def snapshot(self, pending: Mapping[str, int] | None = None,
                 dead: Mapping[str, int] | None = None,
                 devices: list[str] | None = None) -> dict[str, dict[str, int]]:
        """返回按设备汇总的瞬时读数，并实时并入 pending/dead。

        devices 给出需要覆盖的设备全集（如 store 已知设备）；
        重试队列中出现、但从未接入成功的设备也会被补进来。
        """
        pending = pending or {}
        dead = dead or {}
        with self._lock:
            counts = {dev: dict(bucket) for dev, bucket in self._counts.items()}

        for dev in devices or []:
            counts.setdefault(dev, {key: 0 for key in COUNTER_KEYS})
        for dev in set(pending) | set(dead):
            counts.setdefault(dev, {key: 0 for key in COUNTER_KEYS})

        for dev, bucket in counts.items():
            bucket["pending"] = int(pending.get(dev, 0))
            bucket["dead"] = int(dead.get(dev, 0))
        return dict(sorted(counts.items()))
