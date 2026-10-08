"""应用 lifespan：后台 worker 自动补偿、失败条目不重复入队、关闭优雅停止。"""
import time

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import app.api as api
from app.api import router
from app.metrics import QualityMetrics
from app.retry import RetryQueue
from app.store import TelemetryStore


@pytest.fixture()
def live_client():
    # 独立 app + lifespan；store 关闭时效检查让小 ts 的构造也不被拦
    api.store = TelemetryStore(history_limit=10)
    api.retry_queue = RetryQueue(base_delay=0.01, max_attempts=3, max_delay=1.0)
    api.metrics = QualityMetrics()
    api.downstream = lambda record: True
    app = FastAPI()
    api.install_lifespan(app)
    app.include_router(router)
    with TestClient(app) as client:
        yield client
    api.worker = None


def test_worker_auto_compensates_and_no_duplicate_enqueue(live_client):
    api.downstream = lambda record: False
    live_client.post("/ingest/mqtt", json={
        "device_id": "d1", "metric": "t", "value": 1.0,
        "timestamp": time.time(), "seq": 1})
    # 没有手动 /retry/run，等待后台自动重试到放弃
    deadline = time.time() + 3.0
    while time.time() < deadline:
        view = live_client.get("/retry").json()
        if view["dead"] == 1:
            break
        time.sleep(0.02)
    view = live_client.get("/retry").json()
    assert view["dead"] == 1
    assert view["pending"] == 0          # 关键：没有因重新 push 而堆积
    assert len(api.retry_queue) == 0
