"""接入质量汇总：分类计数、补偿积压读数、并发准确性。"""
import threading
import time

import pytest
from fastapi.testclient import TestClient

import app.api as api
from app.metrics import UNKNOWN_DEVICE, IngestMetrics
from app.retry import RetryQueue
from app.store import IngestOutcome, TelemetryStore
from main import app


@pytest.fixture()
def client():
    api.store = TelemetryStore(history_limit=100, max_past_age=300.0,
                               max_future_skew=60.0)
    api.retry_queue = RetryQueue(base_delay=1.0, max_attempts=3)
    api.metrics = IngestMetrics()
    api.downstream = lambda record: True
    return TestClient(app)


def mqtt(device="d1", metric="t", value=1.0, ts=None, seq=1):
    return {"device_id": device, "metric": metric, "value": value,
            "timestamp": time.time() if ts is None else ts, "seq": seq}


def test_summary_counts_each_outcome_category(client):
    now = time.time()
    client.post("/ingest/mqtt", json=mqtt(ts=now, seq=1))          # accepted
    client.post("/ingest/mqtt", json=mqtt(ts=now - 10, seq=2))     # stale
    client.post("/ingest/mqtt", json=mqtt(ts=now, seq=1))          # duplicate
    client.post("/ingest/mqtt", json=mqtt(ts=now - 1000, seq=3))   # too_old
    client.post("/ingest/mqtt", json=mqtt(ts=now + 1000, seq=4))   # future
    client.post("/ingest/mqtt", json=mqtt(value="bad", seq=5))     # rejected
    d1 = client.get("/metrics/summary").json()["devices"]["d1"]
    assert d1["accepted"] == 1
    assert d1["stale"] == 1
    assert d1["duplicate"] == 1
    assert d1["too_old"] == 1
    assert d1["future"] == 1
    assert d1["rejected"] == 1


def test_summary_tracks_pending_and_dead_per_device(client):
    api.retry_queue = RetryQueue(base_delay=0.01, max_attempts=1)
    api.downstream = lambda record: record.device_id != "d2"
    client.post("/ingest/mqtt", json=mqtt(device="d1"))  # 成功，无积压
    client.post("/ingest/mqtt", json=mqtt(device="d2"))  # 失败进队列
    summary = client.get("/metrics/summary").json()["devices"]
    assert summary["d2"]["pending_retry"] == 1
    client.post("/retry/run")  # max_attempts=1，一次失败即放弃
    summary = client.get("/metrics/summary").json()["devices"]
    assert summary["d2"]["pending_retry"] == 0
    assert summary["d2"]["dead"] == 1
    assert summary["d1"]["pending_retry"] == 0


def test_rejected_without_device_id_goes_to_unknown_bucket(client):
    client.post("/ingest/mqtt", json={"metric": "t"})  # 缺 device_id
    devices = client.get("/metrics/summary").json()["devices"]
    assert devices[UNKNOWN_DEVICE]["rejected"] == 1


def test_summary_isolated_per_device(client):
    now = time.time()
    client.post("/ingest/mqtt", json=mqtt(device="d1", ts=now, seq=1))
    client.post("/ingest/mqtt", json=mqtt(device="d2", ts=now, seq=1))
    client.post("/ingest/mqtt", json=mqtt(device="d2", ts=now, seq=1))
    devices = client.get("/metrics/summary").json()["devices"]
    assert devices["d1"]["accepted"] == 1 and devices["d1"]["duplicate"] == 0
    assert devices["d2"]["accepted"] == 1 and devices["d2"]["duplicate"] == 1


def test_concurrent_ingest_summary_counts_accurate(client):
    # 多设备并发上报：每设备 50 条正常 + 50 条重复，汇总必须精确
    now = time.time()

    def worker(device):
        for seq in range(50):
            client.post("/ingest/mqtt", json=mqtt(device=device, ts=now, seq=seq))
            client.post("/ingest/mqtt", json=mqtt(device=device, ts=now, seq=seq))

    threads = [threading.Thread(target=worker, args=(f"dev{i}",))
               for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    devices = client.get("/metrics/summary").json()["devices"]
    assert len(devices) == 5
    for i in range(5):
        d = devices[f"dev{i}"]
        assert d["accepted"] == 50
        assert d["duplicate"] == 50
        assert d["stale"] == d["too_old"] == d["future"] == d["rejected"] == 0


def test_metrics_unit_record_and_unknown_outcome():
    m = IngestMetrics()
    m.record_outcome("d1", IngestOutcome.ACCEPTED)
    m.record_rejected(None)
    with pytest.raises(ValueError):
        m.record_outcome("d1", "not_an_outcome")
    q = RetryQueue()
    summary = m.summary(q)
    assert summary["d1"]["accepted"] == 1
    assert summary[UNKNOWN_DEVICE]["rejected"] == 1
