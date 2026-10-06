"""统一的内部数据模型。"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from .errors import RejectReason


class Liveness(str, Enum):
    ONLINE = "online"
    STALE = "stale"
    OFFLINE = "offline"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class Measurement:
    metric: str
    value: float


@dataclass(frozen=True)
class NormalizedRecord:
    device_id: str
    ts: float                      # 设备上报时间（epoch 秒）
    received_at: float             # 网关接收时间（epoch 秒）
    measurements: tuple[Measurement, ...]
    source_format: str
    message_id: str | None
    dedup_key: str                 # 幂等键


@dataclass
class DeviceState:
    device_id: str
    latest: NormalizedRecord | None = None
    history: list[NormalizedRecord] = field(default_factory=list)


@dataclass(frozen=True)
class IngestResult:
    accepted: bool
    dedup_key: str
    device_id: str | None
    duplicate: bool = False
    reason: RejectReason | None = None
    detail: str = ""
