"""时效判定：太旧（TOO_OLD）与未来时间戳（FUTURE）的确定结论。"""
from app.models import TelemetryRecord
from app.store import IngestOutcome, TelemetryStore

NOW = 10_000.0


def rec(device="d1", metric="temp", value=1.0, ts=NOW, seq=0):
    return TelemetryRecord(device, metric, value, ts, seq, "mqtt")


def make_store(now=NOW, max_past_age=300.0, max_future_skew=60.0, limit=10):
    return TelemetryStore(history_limit=limit, now_fn=lambda: now,
                          max_past_age=max_past_age,
                          max_future_skew=max_future_skew)


def test_too_old_record_rejected_with_deterministic_outcome():
    s = make_store()
    assert s.ingest(rec(ts=NOW - 301, seq=1)) == IngestOutcome.TOO_OLD
    assert s.ingest(rec(ts=NOW - 10_000, seq=2)) == IngestOutcome.TOO_OLD


def test_too_old_boundary_inclusive():
    s = make_store()
    # 恰好等于容忍边界的记录仍然正常收下
    assert s.ingest(rec(ts=NOW - 300, seq=1)) == IngestOutcome.ACCEPTED


def test_too_old_does_not_touch_device_state():
    s = make_store()
    s.ingest(rec(ts=NOW - 10, seq=1, value=5.0))
    before = s.get_state("d1")
    assert s.ingest(rec(ts=NOW - 9999, seq=2, value=9.9)) == IngestOutcome.TOO_OLD
    after = s.get_state("d1")
    assert after.latest.value == 5.0          # 最新值不被带偏
    assert after.last_seen == before.last_seen  # 不刷新 last_seen，不影响在线判定
    assert [r.seq for r in after.history] == [1]  # 不留历史痕迹


def test_too_old_from_unknown_device_creates_no_state():
    s = make_store()
    assert s.ingest(rec(device="ghost", ts=NOW - 9999, seq=1)) == IngestOutcome.TOO_OLD
    assert s.get_state("ghost") is None
    assert s.list_devices() == []


def test_too_old_repeat_report_same_conclusion():
    s = make_store()
    first = s.ingest(rec(ts=NOW - 9999, seq=1))
    second = s.ingest(rec(ts=NOW - 9999, seq=1))
    assert first == second == IngestOutcome.TOO_OLD  # 重复上报结论一致


def test_future_timestamp_distinguished_from_too_old():
    s = make_store()
    assert s.ingest(rec(ts=NOW + 61, seq=1)) == IngestOutcome.FUTURE
    assert s.ingest(rec(ts=NOW - 301, seq=2)) == IngestOutcome.TOO_OLD


def test_future_boundary_inclusive():
    s = make_store()
    assert s.ingest(rec(ts=NOW + 60, seq=1)) == IngestOutcome.ACCEPTED


def test_future_record_does_not_touch_device_state():
    s = make_store()
    s.ingest(rec(ts=NOW, seq=1, value=5.0))
    assert s.ingest(rec(ts=NOW + 9999, seq=2, value=9.9)) == IngestOutcome.FUTURE
    state = s.get_state("d1")
    assert state.latest.value == 5.0
    assert state.latest.timestamp == NOW
    assert [r.seq for r in state.history] == [1]


def test_future_repeat_report_same_conclusion():
    s = make_store()
    assert s.ingest(rec(ts=NOW + 9999, seq=1)) == IngestOutcome.FUTURE
    assert s.ingest(rec(ts=NOW + 9999, seq=1)) == IngestOutcome.FUTURE


def test_timeliness_disabled_by_default_keeps_old_behavior():
    # 不配置容忍范围时行为与之前完全一致：任意时间戳都按乱序规则处理
    s = TelemetryStore(history_limit=10, now_fn=lambda: NOW)
    assert s.ingest(rec(ts=NOW - 10_000, seq=1)) == IngestOutcome.ACCEPTED
    assert s.ingest(rec(ts=NOW + 10_000, seq=2)) == IngestOutcome.ACCEPTED


def test_previously_seen_record_still_duplicate_not_too_old():
    # 先正常收下、之后时钟前进导致同一记录落在容忍范围之外：
    # 重复上报仍判 DUPLICATE（幂等优先），不变成 TOO_OLD
    clock = {"now": NOW}
    s = TelemetryStore(history_limit=10, now_fn=lambda: clock["now"],
                       max_past_age=300.0, max_future_skew=60.0)
    assert s.ingest(rec(ts=NOW, seq=1)) == IngestOutcome.ACCEPTED
    clock["now"] = NOW + 10_000
    assert s.ingest(rec(ts=NOW, seq=1)) == IngestOutcome.DUPLICATE
