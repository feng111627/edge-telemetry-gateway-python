"""归一化：多来源映射、畸形上报的可区分拒绝原因。"""
from app.models import RejectReason
from app.normalizer import normalize


def test_modbus_payload_normalized():
    r = normalize("modbus", {"dev": "d1", "reg": "temp", "val": 21.5,
                             "ts": 1700000000, "seq": 3})
    assert r.ok
    assert r.record.device_id == "d1"
    assert r.record.metric == "temp"
    assert r.record.value == 21.5
    assert r.record.timestamp == 1700000000.0
    assert r.record.seq == 3
    assert r.record.source == "modbus"


def test_http_raw_iso_timestamp():
    r = normalize("http_raw", {"id": "d3", "name": "p", "reading": 1.0,
                               "time": "2026-10-06T08:00:00Z", "n": 7})
    assert r.ok
    assert r.record.timestamp == 1791273600.0
    assert r.record.seq == 7


def test_unknown_source_rejected():
    r = normalize("lora", {"device_id": "d1"})
    assert not r.ok and r.reason is RejectReason.UNKNOWN_SOURCE


def test_non_object_payload_rejected():
    for bad in ([1, 2], "text", 42, None):
        r = normalize("mqtt", bad)
        assert not r.ok and r.reason is RejectReason.NOT_AN_OBJECT


def test_missing_field_rejected():
    r = normalize("mqtt", {"device_id": "d1", "metric": "t"})
    assert not r.ok and r.reason is RejectReason.MISSING_FIELD
    assert "value" in r.detail and "timestamp" in r.detail


def test_bad_value_type_rejected():
    r = normalize("mqtt", {"device_id": "d1", "metric": "t", "value": "hot",
                           "timestamp": 1, "seq": 1})
    assert not r.ok and r.reason is RejectReason.BAD_TYPE


def test_bool_value_rejected_as_bad_type():
    r = normalize("mqtt", {"device_id": "d1", "metric": "t", "value": True,
                           "timestamp": 1, "seq": 1})
    assert not r.ok and r.reason is RejectReason.BAD_TYPE


def test_bad_timestamp_rejected():
    r = normalize("mqtt", {"device_id": "d1", "metric": "t", "value": 1.0,
                           "timestamp": "not-a-time", "seq": 1})
    assert not r.ok and r.reason is RejectReason.BAD_TIMESTAMP


def test_non_integer_seq_rejected():
    r = normalize("mqtt", {"device_id": "d1", "metric": "t", "value": 1.0,
                           "timestamp": 1, "seq": 1.5})
    assert not r.ok and r.reason is RejectReason.BAD_TYPE
