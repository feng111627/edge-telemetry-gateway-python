"""端到端：接入、查询、在线状态、下游失败重试。"""
import pytest
from fastapi.testclient import TestClient

import app.api as api
from app.metrics import IngestMetrics
from app.retry import RetryQueue
from app.store import TelemetryStore
from main import app


@pytest.fixture()
def client():
    api.store = TelemetryStore(history_limit=5)
    api.retry_queue = RetryQueue(base_delay=1.0, max_attempts=3)
    api.metrics = IngestMetrics()
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


# ---- 时效判定（API 层） ----

import time


@pytest.fixture()
def timely_client():
    """带时效判定配置的接入端。"""
    api.store = TelemetryStore(history_limit=5, max_past_age=300.0,
                               max_future_skew=60.0)
    api.retry_queue = RetryQueue(base_delay=1.0, max_attempts=3)
    api.metrics = IngestMetrics()
    api.downstream = lambda record: True
    return TestClient(app)


def test_too_old_report_gets_distinct_outcome(timely_client):
    now = time.time()
    r = timely_client.post("/ingest/mqtt", json=mqtt(ts=now - 1000, seq=1)).json()
    assert r["outcome"] == "too_old"
    assert r["accepted"] is False
    assert timely_client.get("/devices/d1").status_code == 404  # 不创建设备


def test_future_report_gets_distinct_outcome(timely_client):
    now = time.time()
    r = timely_client.post("/ingest/mqtt", json=mqtt(ts=now + 1000, seq=1)).json()
    assert r["outcome"] == "future"
    assert r["accepted"] is False


def test_outcomes_distinguishable_in_batch(timely_client):
    now = time.time()
    payload = [mqtt(ts=now, seq=1), mqtt(ts=now - 1000, seq=2),
               mqtt(ts=now + 1000, seq=3), mqtt(ts=now, seq=1)]
    results = timely_client.post("/ingest/mqtt", json=payload).json()["results"]
    assert [r["outcome"] for r in results] == [
        "accepted", "too_old", "future", "duplicate"]


def test_too_old_report_not_forwarded_downstream(timely_client):
    sent = []
    api.downstream = lambda record: sent.append(record) or True
    now = time.time()
    timely_client.post("/ingest/mqtt", json=mqtt(ts=now - 1000, seq=1))
    timely_client.post("/ingest/mqtt", json=mqtt(ts=now + 1000, seq=2))
    assert sent == []  # 太旧/未来时间戳的数据不送下游


def test_too_old_report_does_not_mark_device_online(timely_client):
    now = time.time()
    timely_client.post("/ingest/mqtt", json=mqtt(ts=now - 1000, seq=1))
    assert timely_client.get("/devices/d1").status_code == 404
    # 正常上报后设备才出现且在线
    timely_client.post("/ingest/mqtt", json=mqtt(ts=now, seq=2))
    assert timely_client.get("/devices/d1/status").json()["status"] == "normal"


# ---- 最终放弃集合：查询与重新投递 ----

def _drive_to_dead(client, seq=1):
    if not isinstance(api.retry_queue, RetryQueue) \
            or api.retry_queue.max_attempts != 2:
        api.retry_queue = RetryQueue(base_delay=0.01, max_attempts=2)
    api.downstream = lambda record: False
    client.post("/ingest/mqtt", json=mqtt(seq=seq))
    for _ in range(2):
        client.post("/retry/run")
        time.sleep(0.02)


def test_dead_set_queryable(client):
    _drive_to_dead(client)
    dead = client.get("/retry/dead").json()["dead"]
    assert len(dead) == 1 and dead[0]["seq"] == 1


def test_redrive_all_from_dead(client):
    _drive_to_dead(client)
    r = client.post("/retry/dead/redrive").json()
    assert r["redriven"] == 1 and r["pending"] == 1 and r["dead"] == 0


def test_redrive_single_by_index(client):
    _drive_to_dead(client, seq=1)
    _drive_to_dead(client, seq=2)
    r = client.post("/retry/dead/redrive", json={"index": 0}).json()
    assert r["redriven"] == 1
    assert client.get("/retry").json()["dead"] == 1


def test_redrive_invalid_index_returns_404(client):
    _drive_to_dead(client)
    assert client.post("/retry/dead/redrive", json={"index": 9}).status_code == 404


def test_redrive_then_downstream_recovers_no_duplicate_delivery(client):
    _drive_to_dead(client)
    delivered = []
    api.downstream = lambda record: delivered.append(record.seq) or True
    client.post("/retry/dead/redrive")
    client.post("/retry/dead/redrive")  # 再次 redrive：dead 已空，不会重复投递
    client.post("/retry/run")
    assert delivered == [1]  # 同一条数据只送给下游一次
    summary = client.get("/metrics/summary").json()["devices"]["d1"]
    assert summary["accepted"] == 1  # 重新投递不影响接入侧计数


# ---- 后台自动补偿（服务生命周期内） ----

def test_background_worker_retries_automatically(client):
    api.retry_queue = RetryQueue(base_delay=0.02, max_attempts=3)
    api.WORKER_INTERVAL = 0.02
    attempts = {"n": 0}

    def flaky(record):
        attempts["n"] += 1
        return attempts["n"] > 1  # 首次失败，补偿时成功

    api.downstream = flaky
    with TestClient(app):  # 进入 lifespan，启动后台补偿线程
        client.post("/ingest/mqtt", json=mqtt())
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            if client.get("/retry").json()["pending"] == 0:
                break
            time.sleep(0.01)
        status = client.get("/retry").json()
        assert status["pending"] == 0 and status["dead"] == 0
        assert attempts["n"] >= 2  # 无需手动 /retry/run 即自动补偿成功
