"""HTTP 接口端到端测试。"""

from __future__ import annotations

import concurrent.futures
import time

import pytest
from fastapi.testclient import TestClient

import app.dependencies as deps
from app.config import Settings
from app.models import NormalizedRecord
from app.service import TelemetryService
from main import app

pytestmark = pytest.mark.api


@pytest.fixture()
def client() -> TestClient:
    deps._service = TelemetryService(Settings(
        history_capacity=5, stale_after_seconds=60, offline_after_seconds=300,
        max_clock_skew_seconds=60, expire_after_seconds=3600,
        retry_base_delay=1.0, retry_backoff=2.0, retry_max_delay=8.0, retry_max_attempts=2,
    ))
    return TestClient(app)


def sensor(device: str, ts_ms: int | None = None, **extra) -> dict:
    payload = {"device_id": device, "ts_ms": int((ts_ms or time.time() * 1000)),
               "temperature": 21.5}
    payload.update(extra)
    return payload


def test_根路由健康检查(client):
    assert client.get("/").json()["status"] == "ok"


def test_正常接入三种格式并查询状态与历史(client):
    assert client.post("/ingest", json=sensor("s-1")).json()["accepted"] is True
    now = time.time()
    gw = {"device": {"id": "g-1"}, "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)),
          "metrics": {"cpu": 0.3}}
    assert client.post("/ingest", json=gw).json()["accepted"] is True
    generic = {"device_id": "x-1", "ts": now, "measurements": [{"metric": "v", "value": 7}]}
    assert client.post("/ingest", json=generic).json()["accepted"] is True

    status = client.get("/devices/s-1/status").json()
    assert status["liveness"] == "online"
    assert status["measurements"][0]["metric"] == "temperature"
    history = client.get("/devices/s-1/history").json()["items"]
    assert len(history) == 1 and history[0]["source_format"] == "sensor_v1"


def test_畸形JSON返回400并有区分原因(client):
    resp = client.post("/ingest", content=b"{not json", headers={"content-type": "application/json"})
    assert resp.status_code == 400
    assert resp.json()["reason"] == "malformed_json"


def test_字段缺失与类型错误返回可区分原因(client):
    r1 = client.post("/ingest", json={"device_id": "s-1", "ts_ms": time.time() * 1000}).json()
    assert r1["accepted"] is False and r1["reason"] == "missing_field"
    r2 = client.post("/ingest", json=sensor("s-1", temperature="热")).json()
    assert r2["accepted"] is False and r2["reason"] == "bad_type"
    r3 = client.post("/ingest", json={"device_id": "s-1", "x": 1}).json()
    assert r3["reason"] == "unknown_format"
    r4 = client.post("/ingest", json={"ts_ms": time.time() * 1000, "temperature": 1}).json()
    assert r4["reason"] in ("missing_device_id", "missing_field")


def test_重复上报幂等_计数稳定(client):
    p = sensor("s-9", msg_id="dup-1")
    r1 = client.post("/ingest", json=p).json()
    p2 = dict(p, temperature=99.9)  # 同一 msg_id
    r2 = client.post("/ingest", json=p2).json()
    assert r1["accepted"] and r2["accepted"] and r2["duplicate"]
    assert r1["dedup_key"] == r2["dedup_key"]
    status = client.get("/devices/s-9/status").json()
    assert status["measurements"][0]["value"] == 21.5
    assert status["history_size"] == 1


def test_乱序时间戳进入历史但最新值不变(client):
    base = int(time.time() * 1000)
    client.post("/ingest", json=sensor("s-2", ts_ms=base, temperature=30.0))
    client.post("/ingest", json=sensor("s-2", ts_ms=base - 5000, temperature=10.0))
    history = client.get("/devices/s-2/history").json()["items"]
    assert [h["measurements"][0]["value"] for h in history] == [10.0, 30.0]
    status = client.get("/devices/s-2/status").json()
    assert status["measurements"][0]["value"] == 30.0


def test_过期与未来时间戳被拒绝(client):
    now = time.time()
    old = {"device_id": "x", "ts": now - 7200, "measurements": [{"metric": "v", "value": 1}]}
    fut = {"device_id": "x", "ts": now + 7200, "measurements": [{"metric": "v", "value": 1}]}
    assert client.post("/ingest", json=old).json()["reason"] == "expired_timestamp"
    assert client.post("/ingest", json=fut).json()["reason"] == "future_timestamp"


def test_历史容量上限淘汰最旧记录(client):
    base = int(time.time() * 1000)
    for i in range(8):
        client.post("/ingest", json=sensor("s-cap", ts_ms=base + i * 1000, temperature=float(i)))
    history = client.get("/devices/s-cap/history").json()["items"]
    assert len(history) == 5
    assert [h["measurements"][0]["value"] for h in history] == [3.0, 4.0, 5.0, 6.0, 7.0]


def test_批量接入逐条独立判定(client):
    now = time.time()
    resp = client.post("/ingest/batch", json=[
        sensor("b-1"), "garbage",
        {"device_id": "b-2", "ts": now, "measurements": [{"metric": "v", "value": 1}]},
    ]).json()
    assert resp["total"] == 3 and resp["accepted"] == 2 and resp["rejected"] == 1
    assert resp["results"][1]["reason"] == "not_an_object"
    bad = client.post("/ingest/batch", json={"not": "array"})
    assert bad.status_code == 400


def test_并发接入不同设备不串扰(client):
    base = int(time.time() * 1000)

    def send(dev: str) -> None:
        for i in range(20):
            r = client.post("/ingest", json=sensor(dev, ts_ms=base + i * 1000, temperature=float(i)))
            assert r.json()["accepted"] is True

    t0 = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
        list(pool.map(send, [f"c-{d}" for d in range(5)]))
    assert time.time() - t0 < 30
    for d in range(5):
        dev = f"c-{d}"
        history = client.get(f"/devices/{dev}/history").json()["items"]
        assert len(history) == 5  # 容量淘汰
        assert all(h["source_format"] == "sensor_v1" for h in history)
        status = client.get(f"/devices/{dev}/status").json()
        assert status["measurements"][0]["value"] == 19.0


def test_下游失败可通过retry接口观察并补偿(client):
    state = {"fail": True}

    async def flaky(r: NormalizedRecord) -> None:
        if state["fail"]:
            raise RuntimeError("down")

    deps.get_service().set_processor(flaky)
    client.post("/ingest", json=sensor("r-1"))
    stats = client.get("/retry/stats").json()
    assert stats["pending"] == 1 and stats["enqueued"] == 1
    # 等待首次失败后的退避（>=1s），再恢复下游
    client.post("/retry/pump")
    time.sleep(1.1)
    state["fail"] = False
    pumped = client.post("/retry/pump").json()
    assert pumped["delivered"] == 1
    assert client.get("/retry/stats").json()["pending"] == 0
