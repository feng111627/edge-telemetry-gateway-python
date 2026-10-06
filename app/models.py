"""内部统一的数据结构定义（骨架）。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field


class RejectReason(enum.Enum):
    """上报被拒绝的可区分原因。"""

    NOT_AN_OBJECT = "not_an_object"
    MISSING_FIELD = "missing_field"
    BAD_TYPE = "bad_type"
    BAD_TIMESTAMP = "bad_timestamp"
    UNKNOWN_SOURCE = "unknown_source"


@dataclass(frozen=True)
class TelemetryRecord:
    """归一化后的内部遥测记录。"""

    device_id: str
    metric: str
    value: float
    timestamp: float
    seq: int
    source: str


@dataclass
class DeviceState:
    """单个设备的最新值与有界历史。"""

    device_id: str
    latest: TelemetryRecord | None = None
    history: list[TelemetryRecord] = field(default_factory=list)
    last_seen: float | None = None


class OnlineStatus(enum.Enum):
    NORMAL = "normal"
    LAGGING = "lagging"
    OFFLINE = "offline"
