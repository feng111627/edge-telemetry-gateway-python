"""状态存储：幂等、乱序、容量淘汰、设备隔离、并发接入。"""
import threading

from app.models import TelemetryRecord
from app.store import IngestOutcome, TelemetryStore


def rec(device="d1", metric="temp", value=1.0, ts=0.0, seq=0):
    return TelemetryRecord(device, metric, value, ts, seq, "mqtt")


def make_store(limit=3, now=1000.0):
    return TelemetryStore(history_limit=limit, now_fn=lambda: now)


def test_first_ingest_accepted_and_updates_latest():
    s = make_store()
    assert s.ingest(rec(ts=10, seq=1)) == IngestOutcome.ACCEPTED
    state = s.get_state("d1")
    assert state.latest.seq == 1
    assert state.last_seen == 1000.0
    assert [r.seq for r in state.history] == [1]


def test_duplicate_report_is_idempotent():
    s = make_store()
    s.ingest(rec(ts=10, seq=1, value=5.0))
    assert s.ingest(rec(ts=10, seq=1, value=9.9)) == IngestOutcome.DUPLICATE
    state = s.get_state("d1")
    assert len(state.history) == 1
    assert state.latest.value == 5.0  # 不重复计数，保留首次值


def test_out_of_order_timestamp_marked_stale():
    s = make_store()
    s.ingest(rec(ts=100, seq=1))
    assert s.ingest(rec(ts=50, seq=2)) == IngestOutcome.STALE
    state = s.get_state("d1")
    assert state.latest.seq == 1  # 最新值不被旧数据覆盖
    assert [r.timestamp for r in state.history] == [50.0, 100.0]  # 历史仍按序


def test_history_evicts_oldest_when_full():
    s = make_store(limit=3)
    for i, ts in enumerate([10, 20, 30, 40], start=1):
        s.ingest(rec(ts=ts, seq=i))
    state = s.get_state("d1")
    assert [r.timestamp for r in state.history] == [20.0, 30.0, 40.0]


def test_stale_record_evicted_immediately_when_older_than_window():
    s = make_store(limit=2)
    s.ingest(rec(ts=100, seq=1))
    s.ingest(rec(ts=200, seq=2))
    # 比窗口内最旧记录还旧的乱序数据：有确定结论（STALE），且不会挤掉有效历史
    assert s.ingest(rec(ts=1, seq=3)) == IngestOutcome.STALE
    state = s.get_state("d1")
    assert [r.timestamp for r in state.history] == [100.0, 200.0]


def test_devices_do_not_interfere():
    s = make_store()
    s.ingest(rec(device="d1", ts=10, seq=1))
    s.ingest(rec(device="d2", ts=20, seq=1))
    s.ingest(rec(device="d1", ts=30, seq=2))
    assert s.get_state("d1").latest.timestamp == 30.0
    assert s.get_state("d2").latest.timestamp == 20.0
    assert len(s.get_state("d2").history) == 1
    assert s.list_devices() == ["d1", "d2"]


def test_same_seq_different_metric_not_duplicate():
    s = make_store()
    s.ingest(rec(metric="temp", ts=10, seq=1))
    assert s.ingest(rec(metric="hum", ts=20, seq=1)) == IngestOutcome.ACCEPTED


def test_concurrent_ingest_no_crosstalk():
    s = TelemetryStore(history_limit=500, now_fn=lambda: 0.0)
    errors = []

    def worker(device, count):
        try:
            for seq in range(count):
                s.ingest(rec(device=device, ts=float(seq), seq=seq))
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(f"dev{i}", 100))
               for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    assert len(s.list_devices()) == 10
    for i in range(10):
        state = s.get_state(f"dev{i}")
        assert len(state.history) == 100
        assert state.latest.seq == 99
        assert all(r.device_id == f"dev{i}" for r in state.history)


def test_concurrent_duplicate_ingest_counted_once():
    s = TelemetryStore(history_limit=10, now_fn=lambda: 0.0)

    def worker():
        for _ in range(50):
            s.ingest(rec(ts=1.0, seq=1))

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(s.get_state("d1").history) == 1
