"""归一化层测试：多格式接入与可区分的拒绝原因。"""

from __future__ import annotations

import pytest

from app.config import Settings
from app.errors import IngestError, RejectReason
from app.models import NormalizedRecord
from app.normalizer import normalize

pytestmark = pytest.mark.normalizer

NOW = 1_728_000_000.0
SETTINGS = Settings(expire_after_seconds=3600.0, max_clock_skew_seconds=60.0)


def norm(payload, now: float = NOW):
    return normalize(payload, now=now, settings=SETTINGS)


@pytest.mark.parametrize(
    "payload,fmt,metrics",
    [
        (
            {"device_id": "s-1", "ts_ms": NOW * 1000, "temperature": 23.5, "humidity": 60.1},
            "sensor_v1",
            (("humidity", 60.1), ("temperature", 23.5)),
        ),
        (
            {"device": {"id": "g-1"}, "timestamp": "2024-10-04T00:00:00Z",
             "metrics": {"cpu": 0.42, "mem": 0.8}},
            "gateway_v2",
            (("cpu", 0.42), ("mem", 0.8)),
        ),
        (
            {"device_id": "x-1", "ts": NOW,
             "measurements": [{"metric": "v", "value": 1}, {"metric": "a", "value": 2}]},
            "generic",
            (("a", 2.0), ("v", 1.0)),
        ),
    ],
)
def test_三种合法格式被归一为统一结构(payload, fmt, metrics):
    record = norm(payload)
    assert isinstance(record, NormalizedRecord)
    assert record.device_id
    assert record.source_format == fmt
    assert [(m.metric, m.value) for m in record.measurements] == list(metrics)
    assert record.received_at == NOW
    assert record.dedup_key.startswith(f"{record.device_id}:")


def test_毫秒与ISO时间戳都换算为秒级epoch():
    r1 = norm({"device_id": "s", "ts_ms": NOW * 1000, "temperature": 1})
    assert r1.ts == pytest.approx(NOW)
    r2 = norm({"device": {"id": "s"}, "timestamp": "2024-10-04T00:00:00+00:00",
               "metrics": {"cpu": 1}})
    assert isinstance(r2.ts, float)


def test_非对象上报_拒绝原因为not_an_object():
    with pytest.raises(IngestError) as exc:
        norm([1, 2])
    assert exc.value.reason is RejectReason.NOT_A_OBJECT


def test_缺少设备ID_拒绝原因为missing_field或missing_device_id():
    with pytest.raises(IngestError) as exc:
        norm({"ts_ms": NOW * 1000, "temperature": 1})
    assert exc.value.reason in (RejectReason.MISSING_FIELD, RejectReason.MISSING_DEVICE_ID)


def test_无法识别格式_拒绝原因为unknown_format():
    with pytest.raises(IngestError) as exc:
        norm({"device_id": "s-1", "foo": 1})
    assert exc.value.reason is RejectReason.UNKNOWN_FORMAT


def test_字段类型错误_拒绝原因为bad_type():
    with pytest.raises(IngestError) as exc:
        norm({"device_id": "s-1", "ts_ms": NOW * 1000, "temperature": "热"})
    assert exc.value.reason is RejectReason.BAD_TYPE


def test_布尔值不得充当数值_拒绝原因为bad_type():
    with pytest.raises(IngestError) as exc:
        norm({"device_id": "s-1", "ts_ms": NOW * 1000, "temperature": True})
    assert exc.value.reason is RejectReason.BAD_TYPE


def test_缺少指标字段_拒绝原因为missing_field():
    with pytest.raises(IngestError) as exc:
        norm({"device_id": "s-1", "ts_ms": NOW * 1000})
    assert exc.value.reason is RejectReason.MISSING_FIELD


def test_无法解析的时间戳_拒绝原因为bad_timestamp():
    with pytest.raises(IngestError) as exc:
        norm({"device": {"id": "g"}, "timestamp": "not-a-date", "metrics": {"cpu": 1}})
    assert exc.value.reason is RejectReason.BAD_TIMESTAMP


def test_未来时间戳超容差_拒绝原因为future_timestamp():
    with pytest.raises(IngestError) as exc:
        norm({"device_id": "x", "ts": NOW + 3600,
              "measurements": [{"metric": "v", "value": 1}]})
    assert exc.value.reason is RejectReason.FUTURE_TIMESTAMP


def test_过期时间戳_拒绝原因为expired_timestamp():
    with pytest.raises(IngestError) as exc:
        norm({"device_id": "x", "ts": NOW - 7200,
              "measurements": [{"metric": "v", "value": 1}]})
    assert exc.value.reason is RejectReason.EXPIRED_TIMESTAMP


def test_相同message_id产生稳定dedup_key_无id时按内容指纹():
    p1 = {"device_id": "x", "ts": NOW, "message_id": "m-1",
          "measurements": [{"metric": "v", "value": 1}]}
    p2 = {"device_id": "x", "ts": NOW, "message_id": "m-1",
          "measurements": [{"metric": "v", "value": 2}]}
    assert norm(p1).dedup_key == norm(p2).dedup_key

    c1 = {"device_id": "x", "ts": NOW,
          "measurements": [{"metric": "v", "value": 1}]}
    c2 = {"device_id": "x", "ts": NOW,
          "measurements": [{"metric": "v", "value": 1}]}
    c3 = {"device_id": "x", "ts": NOW,
          "measurements": [{"metric": "v", "value": 9}]}
    assert norm(c1).dedup_key == norm(c2).dedup_key
    assert norm(c1).dedup_key != norm(c3).dedup_key
