"""设备状态存储：最新值、有界历史、幂等去重、在线判定。

并发模型：每个设备一把 :class:`asyncio.Lock`，同一设备的写入串行化，
不同设备之间互不阻塞；因此多设备并发上报时状态与历史不会串扰。
"""

from __future__ import annotations

import asyncio
from collections import deque
from typing import Deque

from .config import Settings
from .models import DeviceState, Liveness, NormalizedRecord


class DeviceStore:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._devices: dict[str, DeviceState] = {}
        # 每设备最近见过的幂等键（有界，防止无限增长）
        self._dedup: dict[str, Deque[str]] = {}
        self._dedup_set: dict[str, set[str]] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        # 写入计数，验证“重复不计数”用
        self.accepted_counts: dict[str, int] = {}
        self.duplicate_counts: dict[str, int] = {}

    def _lock_for(self, device_id: str) -> asyncio.Lock:
        lock = self._locks.get(device_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[device_id] = lock
        return lock

    def _ensure(self, device_id: str) -> DeviceState:
        state = self._devices.get(device_id)
        if state is None:
            state = DeviceState(device_id=device_id)
            self._devices[device_id] = state
            self._dedup[device_id] = deque()
            self._dedup_set[device_id] = set()
            self.accepted_counts[device_id] = 0
            self.duplicate_counts[device_id] = 0
        return state

    def _remember_key(self, device_id: str, key: str) -> None:
        keys = self._dedup[device_id]
        seen = self._dedup_set[device_id]
        if key in seen:
            return
        keys.append(key)
        seen.add(key)
        while len(keys) > self._settings.dedup_window:
            seen.discard(keys.popleft())

    async def apply(self, record: NormalizedRecord) -> bool:
        """写入一条已归一记录。

        返回 ``True`` 表示重复数据（幂等命中，不计数、不改状态/历史）；
        返回 ``False`` 表示首次写入。

        乱序的确定性结论：过期 / 未来时间戳在归一化层直接拒绝；
        仍在有效期内但早于最新值的“晚到数据”按时间序插入历史，
        但不刷新最新值。
        """
        async with self._lock_for(record.device_id):
            state = self._ensure(record.device_id)
            seen = self._dedup_set[record.device_id]
            if record.dedup_key in seen:
                self.duplicate_counts[record.device_id] += 1
                return True

            # 历史按时间升序插入（同时间戳排到其后，保持全序稳定）
            history = state.history
            pos = len(history)
            while pos > 0 and history[pos - 1].ts > record.ts:
                pos -= 1
            history.insert(pos, record)
            # 容量淘汰：最旧的先丢
            overflow = len(history) - self._settings.history_capacity
            if overflow > 0:
                del history[:overflow]

            # 只有不早于最新值的数据才刷新“最新值”
            if state.latest is None or record.ts >= state.latest.ts:
                state.latest = record
            self._remember_key(record.device_id, record.dedup_key)
            self.accepted_counts[record.device_id] += 1
            return False

    def get_state(self, device_id: str) -> DeviceState | None:
        return self._devices.get(device_id)

    def history(self, device_id: str, limit: int | None = None) -> list[NormalizedRecord]:
        """返回按时间升序的历史（最新的在末尾）；limit 返回最近的 N 条。"""
        state = self._devices.get(device_id)
        items = list(state.history) if state else []
        if limit is not None:
            items = items[-limit:]
        return items

    def liveness(self, device_id: str, *, now: float) -> Liveness:
        """按最后**接收**时间判定：正常 / 迟滞 / 离线 / 未知。"""
        state = self._devices.get(device_id)
        if state is None or state.latest is None:
            return Liveness.UNKNOWN
        age = now - state.latest.received_at
        if age < self._settings.stale_after_seconds:
            return Liveness.ONLINE
        if age < self._settings.offline_after_seconds:
            return Liveness.STALE
        return Liveness.OFFLINE

    def all_device_ids(self) -> list[str]:
        return sorted(self._devices)
