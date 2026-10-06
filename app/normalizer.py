"""把不同来源、不同格式的设备上报归一为 NormalizedRecord。

支持三种来源格式：

1. ``sensor_v1``（温湿度传感器，时间戳为毫秒 epoch）::

       {"device_id": "s-1", "ts_ms": 1728000000000,
        "temperature": 23.5, "humidity": 60.1, "msg_id": "abc"}

2. ``gateway_v2``（网关批量指标，时间戳为 ISO-8601 字符串）::

       {"device": {"id": "g-1"},
        "timestamp": "2024-10-04T10:00:00Z",
        "metrics": {"cpu": 0.42, "mem": 0.8},
        "seq": 123}

3. ``generic``（通用格式，时间戳为秒 epoch，指标为数组）::

       {"device_id": "x-1", "ts": 1728000000.0,
        "measurements": [{"metric": "v", "value": 1}]}

任何不符合上述结构的上报都会抛出 :class:`IngestError`，reason 可区分
"不是对象 / 缺少设备 ID / 格式无法识别 / 缺字段 / 类型错误 / 时间戳问题"。
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

from .config import Settings, settings as default_settings
from .errors import IngestError, RejectReason
from .models import Measurement, NormalizedRecord

SUPPORTED_FORMATS = ("sensor_v1", "gateway_v2", "generic")


def detect_format(payload: Any) -> str:
    """根据字段特征推断上报格式。"""
    if not isinstance(payload, dict):
        raise IngestError(RejectReason.NOT_A_OBJECT, "上报内容必须是 JSON 对象")
    if "ts_ms" in payload:
        return "sensor_v1"
    if "metrics" in payload or "device" in payload:
        return "gateway_v2"
    if "measurements" in payload:
        return "generic"
    raise IngestError(RejectReason.UNKNOWN_FORMAT, "无法识别上报格式（缺少特征字段）")


def _require(payload: dict, key: str) -> Any:
    if key not in payload:
        raise IngestError(RejectReason.MISSING_FIELD, f"缺少必填字段: {key}")
    return payload[key]


def _as_number(value: Any, field: str) -> float:
    # bool 是 int 的子类型，显式排除
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise IngestError(RejectReason.BAD_TYPE, f"字段 {field} 必须是数值，实际为 {type(value).__name__}")
    return float(value)


def _validate_ts(ts: float, *, now: float, settings: Settings) -> float:
    if ts > now + settings.max_clock_skew_seconds:
        raise IngestError(
            RejectReason.FUTURE_TIMESTAMP,
            f"时间戳 {ts} 超出未来容差 {settings.max_clock_skew_seconds}s",
        )
    if ts < now - settings.expire_after_seconds:
        raise IngestError(
            RejectReason.EXPIRED_TIMESTAMP,
            f"时间戳 {ts} 早于过期窗口 {settings.expire_after_seconds}s",
        )
    return ts


def _parse_iso(value: Any) -> float:
    if not isinstance(value, str):
        raise IngestError(RejectReason.BAD_TYPE, "timestamp 必须是 ISO-8601 字符串")
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError as exc:
        raise IngestError(RejectReason.BAD_TIMESTAMP, f"无法解析时间戳: {value!r}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def _device_id(payload: dict, fmt: str) -> str:
    if fmt == "gateway_v2":
        device = _require(payload, "device")
        if not isinstance(device, dict):
            raise IngestError(RejectReason.BAD_TYPE, "device 必须是对象")
        device_id = _require(device, "id")
    else:
        device_id = _require(payload, "device_id")
    if not isinstance(device_id, str) or not device_id.strip():
        raise IngestError(
            RejectReason.BAD_TYPE if not isinstance(device_id, str) else RejectReason.MISSING_DEVICE_ID,
            "device id 必须是非空字符串",
        )
    return device_id.strip()


def _measurements(payload: dict, fmt: str) -> tuple[Measurement, ...]:
    if fmt == "sensor_v1":
        items: list[Measurement] = []
        for key in ("temperature", "humidity"):
            if key in payload:
                items.append(Measurement(metric=key, value=_as_number(payload[key], key)))
        if not items:
            raise IngestError(RejectReason.MISSING_FIELD, "temperature/humidity 至少需要一个")
        return tuple(sorted(items, key=lambda m: m.metric))

    if fmt == "gateway_v2":
        metrics = _require(payload, "metrics")
        if not isinstance(metrics, dict) or not metrics:
            raise IngestError(RejectReason.BAD_TYPE, "metrics 必须是非空对象")
        items = []
        for key, value in metrics.items():
            if not isinstance(key, str) or not key:
                raise IngestError(RejectReason.BAD_TYPE, "metrics 的键必须是非空字符串")
            items.append(Measurement(metric=key, value=_as_number(value, f"metrics.{key}")))
        return tuple(sorted(items, key=lambda m: m.metric))

    raw = _require(payload, "measurements")
    if not isinstance(raw, list) or not raw:
        raise IngestError(RejectReason.BAD_TYPE, "measurements 必须是非空数组")
    items = []
    for i, item in enumerate(raw):
        if not isinstance(item, dict):
            raise IngestError(RejectReason.BAD_TYPE, f"measurements[{i}] 必须是对象")
        metric = _require(item, "metric")
        if not isinstance(metric, str) or not metric:
            raise IngestError(RejectReason.BAD_TYPE, f"measurements[{i}].metric 必须是非空字符串")
        items.append(Measurement(metric=metric, value=_as_number(_require(item, "value"), f"measurements[{i}].value")))
    return tuple(sorted(items, key=lambda m: (m.metric, m.value)))


def _timestamp(payload: dict, fmt: str) -> float:
    if fmt == "sensor_v1":
        raw = _require(payload, "ts_ms")
        if isinstance(raw, bool) or not isinstance(raw, (int, float)):
            raise IngestError(RejectReason.BAD_TYPE, "ts_ms 必须是毫秒 epoch 数值")
        return float(raw) / 1000.0
    if fmt == "gateway_v2":
        return _parse_iso(_require(payload, "timestamp"))
    raw = _require(payload, "ts")
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise IngestError(RejectReason.BAD_TYPE, "ts 必须是秒 epoch 数值")
    return float(raw)


def _make_dedup_key(device_id: str, message_id: str | None, ts: float, measurements: tuple[Measurement, ...]) -> str:
    if message_id is not None:
        basis = f"id|{message_id}"
    else:
        body = ";".join(f"{m.metric}={m.value!r}" for m in measurements)
        basis = f"content|{ts:.6f}|{body}"
    digest = hashlib.sha256(f"{device_id}|{basis}".encode("utf-8")).hexdigest()[:16]
    return f"{device_id}:{digest}"


def normalize(
    payload: Any,
    *,
    now: float,
    settings: Settings = default_settings,
) -> NormalizedRecord:
    """把一条原始上报归一为 :class:`NormalizedRecord`。

    会先做结构校验（拒绝原因可区分），再校验时间戳窗口。
    """
    fmt = detect_format(payload)
    device_id = _device_id(payload, fmt)
    raw_ts = _timestamp(payload, fmt)
    if raw_ts != raw_ts or raw_ts in (float("inf"), float("-inf")):
        raise IngestError(RejectReason.BAD_TIMESTAMP, "时间戳必须是有限数值")
    ts = _validate_ts(raw_ts, now=now, settings=settings)
    measurements = _measurements(payload, fmt)

    message_id: str | None = None
    if fmt == "sensor_v1" and payload.get("msg_id") is not None:
        message_id = _opt_str(payload["msg_id"], "msg_id")
    elif fmt == "gateway_v2" and "seq" in payload and payload["seq"] is not None:
        message_id = f"seq-{_opt_str(payload['seq'], 'seq')}"
    elif "message_id" in payload and payload["message_id"] is not None:
        message_id = _opt_str(payload["message_id"], "message_id")

    dedup_key = _make_dedup_key(device_id, message_id, ts, measurements)
    return NormalizedRecord(
        device_id=device_id,
        ts=ts,
        received_at=now,
        measurements=measurements,
        source_format=fmt,
        message_id=message_id,
        dedup_key=dedup_key,
    )


def _opt_str(value: Any, field: str) -> str:
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise IngestError(RejectReason.BAD_TYPE, f"字段 {field} 类型不支持")
    return str(value)
