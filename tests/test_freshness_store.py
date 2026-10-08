"""存储层时效行为：太旧留痕不带偏状态、未来不留痕、重复结论一致。"""
from app.models import TelemetryRecord
from app.status import evaluate
from app.store import IngestOutcome, TelemetryStore
from app.models import OnlineStatus


def rec(ts, seq, device="d1", metric="temp"):
    return TelemetryRecord(device, metric, 1.0, ts, seq, "mqtt")


def make_store(now=1000.0, limit=10, max_age=100.0, future_skew=10.0):
    return TelemetryStore(history_limit=limit, now_fn=lambda: now,
                          max_age=max_age, future_skew=future_skew)


def test_too_old_keeps_history_but_not_latest_or_last_seen():
    s = make_store()
    assert s.ingest(rec(1000, 1)) == IngestOutcome.ACCEPTED
    assert s.ingest(rec(899, 2)) == IngestOutcome.TOO_OLD
    state = s.get_state("d1")
    assert state.latest.seq == 1                 # 最新值不被旧数据顶替
    assert [r.seq for r in state.history] == [2, 1]  # 仍留下历史痕迹
    assert state.last_seen == 1000.0             # last_seen 不被旧数据刷新


def test_too_old_does_not_mark_device_online():
    # 设备早已离线，补传一条很久以前的数据不能把它拉回在线
    s = make_store(now=10_000.0)
    assert s.ingest(rec(1000, 1)) == IngestOutcome.TOO_OLD
    state = s.get_state("d1")
    assert state.last_seen is None
    assert evaluate(state.last_seen, 10_000.0) is OnlineStatus.OFFLINE
    assert state.latest is None


def test_too_old_repeated_report_is_consistent_and_not_double_counted():
    s = make_store()
    s.ingest(rec(1000, 1))
    first = s.ingest(rec(899, 2))
    second = s.ingest(rec(899, 2))
    assert first == IngestOutcome.TOO_OLD
    assert second == IngestOutcome.DUPLICATE     # 重复上报确定、不重复计历史
    assert len(s.get_state("d1").history) == 2


def test_future_timestamp_leaves_no_trace():
    s = make_store()
    assert s.ingest(rec(1000, 1)) == IngestOutcome.ACCEPTED
    assert s.ingest(rec(2000, 2)) == IngestOutcome.FUTURE
    state = s.get_state("d1")
    assert state.latest.seq == 1
    assert [r.seq for r in state.history] == [1]  # 历史也不留
    assert state.last_seen == 1000.0


def test_future_repeated_report_always_future():
    s = make_store()
    for _ in range(3):
        assert s.ingest(rec(2000, 2)) == IngestOutcome.FUTURE
    assert s.get_state("d1") is None             # 设备状态从未被创建


def test_future_distinguished_from_unparseable_and_too_old():
    s = make_store()
    assert s.ingest(rec(1011, 2)) == IngestOutcome.FUTURE
    assert s.ingest(rec(899, 3)) == IngestOutcome.TOO_OLD
    assert s.ingest(rec(1005, 4)) == IngestOutcome.ACCEPTED  # 偏差容忍内


def test_concurrent_freshness_decisions_are_stable():
    import threading

    s = make_store(now=1000.0, limit=1000)
    outcomes: list[str] = []

    def worker():
        for _ in range(20):
            outcomes.append(s.ingest(rec(899, 2)))   # 同一条太旧数据
            outcomes.append(s.ingest(rec(2000, 3)))  # 同一条未来数据

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # 未来数据永远 future；太旧数据恰好一次 too_old，其余 duplicate
    assert outcomes.count("future") == 160
    assert outcomes.count("too_old") == 1
    assert outcomes.count("duplicate") == 159
