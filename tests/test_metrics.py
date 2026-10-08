"""接入质量汇总：分类计数、设备归类、并发准确性、并入 pending/dead。"""
import threading

from app.metrics import QualityMetrics


def test_outcomes_bucketed_correctly():
    m = QualityMetrics()
    m.record("d1", "accepted")
    m.record("d1", "stale")        # 乱序仍是有效数据，计入 accepted
    m.record("d1", "duplicate")
    m.record("d1", "too_old")
    m.record("d1", "future")
    m.reject("d1")
    snap = m.snapshot()
    assert snap["d1"] == {
        "accepted": 2, "duplicate": 1, "too_old": 1,
        "future": 1, "rejected": 1, "pending": 0, "dead": 0,
    }


def test_per_device_isolation_and_sorted_keys():
    m = QualityMetrics()
    m.record("b", "accepted")
    m.record("a", "future")
    m.reject("b")
    snap = m.snapshot(devices=["a", "b", "ghost"])
    assert list(snap) == ["a", "b", "ghost"]
    assert snap["ghost"] == {
        "accepted": 0, "duplicate": 0, "too_old": 0,
        "future": 0, "rejected": 0, "pending": 0, "dead": 0,
    }


def test_pending_dead_merged_per_device():
    m = QualityMetrics()
    m.record("d1", "accepted")
    snap = m.snapshot(pending={"d1": 2, "d9": 1}, dead={"d1": 3})
    assert snap["d1"]["pending"] == 2 and snap["d1"]["dead"] == 3
    assert snap["d9"]["pending"] == 1 and snap["d9"]["accepted"] == 0


def test_concurrent_counts_are_accurate():
    m = QualityMetrics()
    outcome_cycle = ["accepted", "stale", "duplicate", "too_old",
                     "future"]

    def worker(device):
        for i in range(1000):
            m.record(device, outcome_cycle[i % len(outcome_cycle)])
        for _ in range(7):
            m.reject(device)

    threads = [threading.Thread(target=worker, args=(f"dev{i}",))
               for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    snap = m.snapshot()
    assert len(snap) == 8
    for i in range(8):
        bucket = snap[f"dev{i}"]
        # accepted/stale 各 200 -> accepted=400；其余各 200；rejected=7
        assert bucket == {
            "accepted": 400, "duplicate": 200, "too_old": 200,
            "future": 200, "rejected": 7, "pending": 0, "dead": 0,
        }
