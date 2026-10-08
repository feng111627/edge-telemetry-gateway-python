"""端到端：时效判定响应、质量汇总视图、dead 集合查看与手动重新投递。"""
import time

import pytest
from fastapi.testclient import TestClient

import app.api as api
from app.metrics import QualityMetrics
from app.retry import RetryQueue
from app.store import TelemetryStore
from main import app


@pytest.fixture()
def client():
    api.store = TelemetryStore(history_limit=10, max_age=3600, future_skew=300)
    api.retry_queue = RetryQueue(base_delay=0.01, max_attempts=3, max_delay=1.0)
    api.metrics = QualityMetrics()
    api.downstream = lambda record: True
    return TestClient(app)


def mqtt(device="d1", metric="t", value=1.0, ts=None, seq=1):
    return {"device_id": device, "metric": metric, "value": value,
            "timestamp": time.time() if ts is None else ts, "seq": seq}


def test_too_old_report_has_clear_outcome_and_keeps_device_state(client):
    now = time.time()
    r = client.post("/ingest/mqtt", json=mqtt(ts=now - 100, seq=1))
    assert r.json()["outcome"] == "accepted"
    r = client.post("/ingest/mqtt", json=mqtt(ts=now - 3601, seq=2)).json()
    assert r["outcome"] == "too_old" and r["accepted"] is False
    assert r["forwarded"] is None                 # 不触发下游
    dev = client.get("/devices/d1").json()
    assert dev["latest"]["seq"] == 1              # 最新值不带偏
    assert [h["seq"] for h in dev["history"]] == [2, 1]  # 历史留痕
    # last_seen 仍由正常上报决定，设备保持 normal
    assert client.get("/devices/d1/status").json()["status"] == "normal"


def test_future_report_distinguished_and_leaves_no_trace(client):
    now = time.time()
    assert client.post("/ingest/mqtt",
                       json=mqtt(ts=now + 301, seq=1)).json()["outcome"] == "future"
    assert client.get("/devices/d1").status_code == 404  # 设备状态从未建立
    # 与“时间戳解析不了”区分
    bad = client.post("/ingest/mqtt", json={
        "device_id": "d1", "metric": "t", "value": 1,
        "timestamp": "garbage", "seq": 2}).json()
    assert bad["accepted"] is False and bad["reason"] == "bad_timestamp"
    assert "outcome" not in bad
    # 重复上报结论一致
    assert client.post("/ingest/mqtt",
                       json=mqtt(ts=now + 301, seq=1)).json()["outcome"] == "future"


def test_quality_summary_counts_per_device(client):
    now = time.time()
    client.post("/ingest/mqtt", json=mqtt(device="d1", ts=now - 10, seq=1))
    client.post("/ingest/mqtt", json=mqtt(device="d1", ts=now - 10, seq=1))  # duplicate
    client.post("/ingest/mqtt", json=mqtt(device="d1", ts=now - 9999, seq=2))  # too_old
    client.post("/ingest/mqtt", json=mqtt(device="d1", ts=now + 9999, seq=3))  # future
    client.post("/ingest/mqtt", json={  # rejected（bad_type），归入 d2
        "device_id": "d2", "metric": "t", "value": "hot",
        "timestamp": now, "seq": 1})
    devices = client.get("/metrics").json()["devices"]
    assert devices["d1"] == {
        "accepted": 1, "duplicate": 1, "too_old": 1, "future": 1,
        "rejected": 0, "pending": 0, "dead": 0}
    assert devices["d2"]["rejected"] == 1


def test_rejected_without_device_id_not_counted(client):
    client.post("/ingest/mqtt", json=[1, 2, 3])  # not_an_object，无法归设备
    client.post("/ingest/mqtt", json={"metric": "t"})  # 缺 device_id
    assert client.get("/metrics").json()["devices"] == {}


def test_dead_letter_listing_and_single_redelivery(client):
    api.downstream = lambda record: False
    client.post("/ingest/mqtt", json=mqtt(seq=1))
    for _ in range(3):
        client.post("/retry/run")
        time.sleep(0.02)
    view = client.get("/retry").json()
    assert view["pending"] == 0 and view["dead"] == 1
    item_id = view["dead_items"][0]["id"]
    assert view["dead_items"][0]["device_id"] == "d1"

    # 让下游恢复；手动重投单条 -> 当场成功，不产生重复
    delivered = []
    api.downstream = lambda r: delivered.append(r.seq) or True
    res = client.post(f"/retry/redeliver?item_id={item_id}").json()
    assert res["redelivered"] == [item_id]
    assert res["dead"] == 0 and delivered == [1]
    assert client.post("/retry/redeliver").json()["redelivered"] == []


def test_redeliver_all_after_giving_up(client):
    api.downstream = lambda record: False
    client.post("/ingest/mqtt", json=mqtt(seq=1))
    client.post("/ingest/mqtt", json=mqtt(device="d2", seq=1))
    for _ in range(3):
        client.post("/retry/run")
        time.sleep(0.02)
    assert client.get("/retry").json()["dead"] == 2
    # 全部重投：第一次尝试仍失败则回到 pending（等待退避），dead 清空
    res = client.post("/retry/redeliver").json()
    assert len(res["redelivered"]) == 2 and res["dead"] == 0
    assert res["pending"] == 2
    # 重复调用不会把同一条重复送入队列或下游
    res2 = client.post("/retry/redeliver").json()
    assert res2["redelivered"] == [] and res2["pending"] == 2


def test_background_worker_eventually_drains_without_manual_run(client):
    # 不调 /retry/run：后台 worker 生命周期由应用管理；这里直接验证队列
    # 能被独立 worker 自动补偿（API 层 worker 仅在 lifespan 下启动）
    from app.worker import RetryWorker

    api.downstream = lambda record: False
    client.post("/ingest/mqtt", json=mqtt(seq=1))
    w = RetryWorker(api.retry_queue, lambda r: False, interval=0.01)
    w.start()
    try:
        deadline = time.time() + 2.0
        while time.time() < deadline and not api.retry_queue.list_dead():
            time.sleep(0.01)
        assert len(api.retry_queue.list_dead()) == 1
    finally:
        w.stop()
