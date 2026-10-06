"""把不同来源、不同格式的设备上报归一为 NormalizedRecord。"""

from __future__ import annotations

from typing import Any

from .models import NormalizedRecord

SUPPORTED_FORMATS = ("sensor_v1", "gateway_v2", "generic")


def detect_format(payload: Any) -> str:
    raise NotImplementedError


def normalize(payload: Any, *, now: float) -> NormalizedRecord:
    raise NotImplementedError
