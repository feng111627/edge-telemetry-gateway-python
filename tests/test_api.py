"""端到端：接入、查询、在线状态、下游失败重试。"""
import pytest
from fastapi.testclient import TestClient

import app.api as api
from app.retry import RetryQueue
from app.store import TelemetryStore
from main import app


@pytest.fixture()
def client():
    api.store = TelemetryStore(history_limit=5)
    api.retry_queue = RetryQueue(base_delay=1.0, max_attempts=3)
    api.downstream = lambda record: True
    return TestClient(app)


def mqtt(device="d1", metric="t", value=1.0, ts=100, seq=1):
    return {"device_id": device, "metric": metric, "value": value,
            "timestamp": ts, "seq": seq}


def test_ingest_and_query_latest(client):
    r = client.post("/ingest/mqtt", json=mqtt())
    assert r.status_code == 200
    assert r.json()["outcome"] == "accepted"
    dev = client.get("/devices/d1").json()
    assert dev["latest"]["value"] == 1.0
    assert len(dev["history"]) == 1
    assert client.get("/devices/d1/status").json()["status"] == "normal"


def test_malformed_report_gets_distinct_reason(client):
    r = client.post("/ingest/mqtt", json=mqtt(value="hot")).json()
    assert r["accepted"] is False and r["reason"] == "bad_type"
    r = client.post("/ingest/mqtt", json={"device_id": "d1"}).json()
    assert r["reason"] == "missing_field"
    r = client.post("/ingest/unknown_src", json=mqtt()).json()
    assert r["reason"] == "unknown_source"


def test_duplicate_report_not_counted_twice(client):
    client.post("/ingest/mqtt", json=mqtt())
    r = client.post("/ingest/mqtt", json=mqtt()).json()
    assert r["outcome"] == "duplicate"
    assert len(client.get("/devices/d1").json()["history"]) == 1


def test_batch_ingest_mixed_results(client):
    payload = [mqtt(seq=1), mqtt(seq=2, value="bad"), mqtt(seq=1)]
    results = client.post("/ingest/mqtt", json=payload).json()["results"]
    assert [r.get("outcome") for r in results] == ["accepted", None, "duplicate"]
    assert results[1]["reason"] == "bad_type"
    assert len(client.get("/devices/d1").json()["history"]) == 1


def test_out_of_order_report_keeps_latest(client):
    client.post("/ingest/mqtt", json=mqtt(ts=200, seq=1))
    r = client.post("/ingest/mqtt", json=mqtt(ts=100, seq=2)).json()
    assert r["outcome"] == "stale"
    dev = client.get("/devices/d1").json()
    assert dev["latest"]["timestamp"] == 200.0
    assert [h["timestamp"] for h in dev["history"]] == [100.0, 200.0]


def test_unknown_device_returns_404(client):
    assert client.get("/devices/ghost").status_code == 404
    assert client.get("/devices/ghost/status").status_code == 404


def test_downstream_failure_queued_and_retried(client):
    failures = {"n": 0}

    def flaky(record):
        failures["n"] += 1
        return failures["n"] > 1  # 第一次失败，之后成功

    api.downstream = flaky
    client.post("/ingest/mqtt", json=mqtt())
    assert client.get("/retry").json()["pending"] == 1
    r = client.post("/retry/run").json()
    assert r["succeeded"] == 1 and r["pending"] == 0


def test_persistent_downstream_failure_eventually_dead(client):
    import time

    api.retry_queue = RetryQueue(base_delay=0.01, max_attempts=3)
    api.downstream = lambda record: False
    client.post("/ingest/mqtt", json=mqtt())
    for _ in range(3):
        client.post("/retry/run")
        time.sleep(0.02)  # 等待退避间隔到期
    status = client.get("/retry").json()
    assert status["pending"] == 0 and status["dead"] == 1


def test_failing_record_does_not_block_later_records(client):
    # 只对 seq=1 的记录失败，其余成功
    api.downstream = lambda record: record.seq != 1
    client.post("/ingest/mqtt", json=mqtt(seq=1))
    r = client.post("/ingest/mqtt", json=mqtt(seq=2)).json()
    assert r["forwarded"] is True  # 后续数据正常接入
    assert client.get("/retry").json()["pending"] == 1
