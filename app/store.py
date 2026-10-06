"""设备状态存储：最新值、有界历史、幂等去重、在线判定。"""

from __future__ import annotations

import asyncio

from .config import Settings
from .models import DeviceState, Liveness, NormalizedRecord
from .errors import IngestError


class DeviceStore:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._devices: dict[str, DeviceState] = {}
        self._dedup: dict[str, set[str]] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    def _lock_for(self, device_id: str) -> asyncio.Lock:
        raise NotImplementedError

    async def apply(self, record: NormalizedRecord) -> bool:
        """写入一条已归一记录，返回是否为重复数据。"""
        raise NotImplementedError

    def get_state(self, device_id: str) -> DeviceState | None:
        raise NotImplementedError

    def history(self, device_id: str, limit: int | None = None) -> list[NormalizedRecord]:
        raise NotImplementedError

    def liveness(self, device_id: str, *, now: float) -> Liveness:
        raise NotImplementedError

    def all_device_ids(self) -> list[str]:
        raise NotImplementedError
