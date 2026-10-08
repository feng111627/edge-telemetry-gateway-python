"""多来源设备上报的归一化。

不同来源的设备上报字段与格式不同，这里统一成 TelemetryRecord。
所有不合法的上报都会得到可区分的 RejectReason，不会被静默丢弃。
"""
from __future__ import annotations

import numbers
from dataclasses import dataclass
from typing import Any

from app.models import RejectReason, TelemetryRecord


@dataclass
class NormalizeResult:
    ok: bool
    record: TelemetryRecord | None = None
    reason: RejectReason | None = None
    detail: str = ""


def _fail(reason: RejectReason, detail: str) -> NormalizeResult:
    return NormalizeResult(ok=False, reason=reason, detail=detail)


# 每种来源一个字段映射与提取规则。
# - "modbus":  {"dev": "d1", "reg": "temp", "val": 21.5, "ts": 1700000000, "seq": 3}
# - "mqtt":    {"device_id": ..., "metric": ..., "value": ..., "timestamp": ..., "seq": ...}
# - "http_raw": {"id": ..., "name": ..., "reading": ..., "time": "ISO8601", "n": ...}
_FIELD_MAP: dict[str, dict[str, str]] = {
    "modbus": {
        "device_id": "dev",
        "metric": "reg",
        "value": "val",
        "timestamp": "ts",
        "seq": "seq",
    },
    "mqtt": {
        "device_id": "device_id",
        "metric": "metric",
        "value": "value",
        "timestamp": "timestamp",
        "seq": "seq",
    },
    "http_raw": {
        "device_id": "id",
        "metric": "name",
        "value": "reading",
        "timestamp": "time",
        "seq": "n",
    },
}


def _parse_timestamp(raw: Any) -> float | None:
    """接受数字（Unix 秒）或 ISO8601 字符串，失败返回 None。"""
    if isinstance(raw, bool):
        return None
    if isinstance(raw, numbers.Real):
        return float(raw)
    if isinstance(raw, str):
        from datetime import datetime

        text = raw.strip()
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(text).timestamp()
        except ValueError:
            return None
    return None


def extract_device_id(source: str, payload: object) -> str | None:
    """尽力从原始上报中提取 device_id（用于被拒绝数据的归类统计）。"""
    fields = _FIELD_MAP.get(source)
    if fields is None or not isinstance(payload, dict):
        return None
    device_id = payload.get(fields["device_id"])
    return device_id if isinstance(device_id, str) and device_id else None


def normalize(source: str, payload: object) -> NormalizeResult:
    """把不同来源的原始上报统一成 TelemetryRecord。"""
    fields = _FIELD_MAP.get(source)
    if fields is None:
        return _fail(RejectReason.UNKNOWN_SOURCE, f"未知来源: {source!r}")

    if not isinstance(payload, dict):
        return _fail(RejectReason.NOT_AN_OBJECT,
                     f"上报必须是 JSON 对象，实际为 {type(payload).__name__}")

    missing = [name for name in fields.values() if name not in payload]
    if missing:
        return _fail(RejectReason.MISSING_FIELD,
                     f"缺少字段: {', '.join(sorted(missing))}")

    device_id = payload[fields["device_id"]]
    metric = payload[fields["metric"]]
    if not isinstance(device_id, str) or not device_id:
        return _fail(RejectReason.BAD_TYPE, "device_id 必须是非空字符串")
    if not isinstance(metric, str) or not metric:
        return _fail(RejectReason.BAD_TYPE, "metric 必须是非空字符串")

    value = payload[fields["value"]]
    if isinstance(value, bool) or not isinstance(value, numbers.Real):
        return _fail(RejectReason.BAD_TYPE, "value 必须是数字")

    seq = payload[fields["seq"]]
    if isinstance(seq, bool) or not isinstance(seq, int):
        return _fail(RejectReason.BAD_TYPE, "seq 必须是整数")

    timestamp = _parse_timestamp(payload[fields["timestamp"]])
    if timestamp is None:
        return _fail(RejectReason.BAD_TIMESTAMP, "timestamp 必须是数字或 ISO8601 字符串")

    return NormalizeResult(ok=True, record=TelemetryRecord(
        device_id=device_id,
        metric=metric,
        value=float(value),
        timestamp=timestamp,
        seq=seq,
        source=source,
    ))
