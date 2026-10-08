"""FastAPI 路由：接入、查询、在线状态、重试与质量汇总。

下游转发通过 `downstream` 可调用对象完成，失败的数据进入 RetryQueue，
由后台 RetryWorker 按指数退避（有上限）自动补偿；超过最大尝试次数进入
dead 集合，可查看并手动重新投递。测试可替换 downstream 模拟失败。
"""
from __future__ import annotations

import time
from collections.abc import Callable
from contextlib import asynccontextmanager
from typing import Any

from fastapi import APIRouter, Body, FastAPI
from fastapi.responses import JSONResponse

from app.metrics import QualityMetrics
from app.models import TelemetryRecord
from app.normalizer import extract_device_id, normalize
from app.retry import RetryQueue
from app.status import evaluate
from app.store import IngestOutcome, TelemetryStore
from app.worker import RetryWorker

LAGGING_AFTER = 60.0
OFFLINE_AFTER = 300.0

# 时效判定（秒）：超过 24 小时视为太旧；未来超过 300 秒视为未来时间戳。
MAX_AGE = 24 * 3600.0
FUTURE_SKEW = 300.0

# 自动补偿：退避基数、上限与最大尝试次数，以及后台扫描间隔。
RETRY_BASE_DELAY = 1.0
RETRY_MAX_DELAY = 300.0
RETRY_MAX_ATTEMPTS = 5
WORKER_INTERVAL = 1.0

router = APIRouter()
store = TelemetryStore(history_limit=100, max_age=MAX_AGE,
                       future_skew=FUTURE_SKEW)
retry_queue = RetryQueue(base_delay=RETRY_BASE_DELAY,
                         max_attempts=RETRY_MAX_ATTEMPTS,
                         max_delay=RETRY_MAX_DELAY)
metrics = QualityMetrics()

# 下游转发钩子：返回 True 表示成功，抛异常或返回 False 表示失败。
downstream: Callable[[TelemetryRecord], bool] = lambda record: True
worker: RetryWorker | None = None


def install_lifespan(app: FastAPI) -> None:
    """让应用启动时自动开始后台补偿、关闭时优雅停止。"""

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        global worker
        worker = RetryWorker(retry_queue, _send_downstream,
                             interval=WORKER_INTERVAL)
        worker.start()
        try:
            yield
        finally:
            worker.stop()

    app.router.lifespan_context = lifespan


def _forward(record: TelemetryRecord) -> bool:
    """转发到下游，失败则暂存重试队列。返回是否成功。"""
    if _send_downstream(record):
        return True
    retry_queue.push(record, now=time.time())
    return False


def _send_downstream(record: TelemetryRecord) -> bool:
    """直接投递一次下游：异常或 False 都视为失败，但不重新入队。

    重试/补偿路径使用它——失败后的退避与放弃由 RetryQueue 负责，
    不能再 push，否则同一条会被重复入队。
    """
    try:
        return downstream(record)
    except Exception:
        return False


def _ingest_one(source: str, payload: Any) -> dict[str, Any]:
    result = normalize(source, payload)
    if not result.ok:
        device_id = extract_device_id(source, payload)
        if device_id is not None:
            metrics.reject(device_id)
        return {
            "accepted": False,
            "reason": result.reason.value if result.reason else "unknown",
            "detail": result.detail,
        }
    record = result.record
    assert record is not None
    outcome = store.ingest(record)
    metrics.record(record.device_id, outcome)
    # 重复上报不重复转发；太旧/未来数据不代表当前状态，不触发下游转发
    if outcome == IngestOutcome.DUPLICATE:
        forwarded = True
    elif outcome in (IngestOutcome.TOO_OLD, IngestOutcome.FUTURE):
        forwarded = None
    else:
        forwarded = _forward(record)
    return {
        # 只有真正进入当前状态的 accepted/stale 才算“收下”；
        # duplicate/too_old/future 均为 accepted=false，具体看 outcome
        "accepted": outcome in (IngestOutcome.ACCEPTED, IngestOutcome.STALE),
        "outcome": outcome,
        "forwarded": forwarded,
        "device_id": record.device_id,
        "metric": record.metric,
        "seq": record.seq,
    }


@router.post("/ingest/{source}")
def ingest(source: str, payload: Any = Body(...)) -> Any:
    """接入单条或一批上报。格式错误/时效异常的数据逐条给出确定结论。"""
    if isinstance(payload, list):
        return {"results": [_ingest_one(source, p) for p in payload]}
    return _ingest_one(source, payload)


@router.get("/devices")
def list_devices() -> dict[str, Any]:
    return {"devices": store.list_devices()}


@router.get("/devices/{device_id}")
def get_device(device_id: str) -> Any:
    state = store.get_state(device_id)
    if state is None:
        return JSONResponse(status_code=404, content={"detail": "device not found"})
    return {
        "device_id": state.device_id,
        "latest": state.latest.__dict__ if state.latest else None,
        "last_seen": state.last_seen,
        "history": [r.__dict__ for r in state.history],
    }


@router.get("/devices/{device_id}/status")
def get_device_status(device_id: str) -> Any:
    state = store.get_state(device_id)
    if state is None:
        return JSONResponse(status_code=404, content={"detail": "device not found"})
    return {
        "device_id": device_id,
        "status": evaluate(state.last_seen, time.time(),
                           LAGGING_AFTER, OFFLINE_AFTER).value,
        "last_seen": state.last_seen,
    }


@router.get("/metrics")
def get_metrics() -> dict[str, Any]:
    """按设备给出接入质量汇总，并实时并入等待补偿/最终放弃条数。"""
    pending, dead = retry_queue.counts_by_device()
    return {"devices": metrics.snapshot(
        pending=pending, dead=dead, devices=store.list_devices())}


@router.get("/retry")
def get_retry_queue() -> dict[str, Any]:
    return {
        "pending": len(retry_queue),
        "dead": len(retry_queue.dead),
        "dead_items": [
            {"id": e.item_id, "attempts": e.attempts,
             "device_id": e.record.device_id, "metric": e.record.metric,
             "seq": e.record.seq, "timestamp": e.record.timestamp,
             "source": e.record.source, "value": e.record.value}
            for e in retry_queue.list_dead()
        ],
    }


@router.post("/retry/run")
def run_retries() -> dict[str, Any]:
    """立即处理当前到期的重试条目（后台任务之外的手动触发，语义不变）。"""
    return retry_queue.process_due(_send_downstream, time.time())


@router.post("/retry/redeliver")
def redeliver(item_id: int | None = None) -> dict[str, Any]:
    """把 dead 中的一条（传 item_id）或全部手动重新投递。

    原子移出 dead 后立即重新入队，同一条不会重复计数或重复送给下游；
    随后立即尝试处理一轮。
    """
    now = time.time()
    requeued = retry_queue.redeliver(now, item_id=item_id)
    result = retry_queue.process_due(_send_downstream, now)
    result["redelivered"] = requeued
    return result
