"""FastAPI 路由：接入、查询、在线状态、重试队列。

下游转发通过 `downstream` 可调用对象完成，失败的数据进入 RetryQueue 暂存，
按指数退避重试；测试可替换 downstream 模拟失败。
"""
from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

from fastapi import APIRouter, Body
from fastapi.responses import JSONResponse

from app.models import TelemetryRecord
from app.normalizer import normalize
from app.retry import RetryQueue
from app.status import evaluate
from app.store import IngestOutcome, TelemetryStore

LAGGING_AFTER = 60.0
OFFLINE_AFTER = 300.0

router = APIRouter()
store = TelemetryStore(history_limit=100)
retry_queue = RetryQueue(base_delay=1.0, max_attempts=5)

# 下游转发钩子：返回 True 表示成功，抛异常或返回 False 表示失败。
downstream: Callable[[TelemetryRecord], bool] = lambda record: True


def _forward(record: TelemetryRecord) -> bool:
    """转发到下游，失败则暂存重试队列。返回是否成功。"""
    try:
        ok = downstream(record)
    except Exception:
        ok = False
    if not ok:
        retry_queue.push(record, now=time.time())
    return ok


def _ingest_one(source: str, payload: Any) -> dict[str, Any]:
    result = normalize(source, payload)
    if not result.ok:
        return {
            "accepted": False,
            "reason": result.reason.value if result.reason else "unknown",
            "detail": result.detail,
        }
    record = result.record
    assert record is not None
    outcome = store.ingest(record)
    forwarded = _forward(record) if outcome != IngestOutcome.DUPLICATE else True
    return {
        "accepted": outcome != IngestOutcome.DUPLICATE,
        "outcome": outcome,
        "forwarded": forwarded,
        "device_id": record.device_id,
        "metric": record.metric,
        "seq": record.seq,
    }


@router.post("/ingest/{source}")
def ingest(source: str, payload: Any = Body(...)) -> Any:
    """接入单条或一批上报。格式错误的数据逐条给出拒绝原因。"""
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


@router.get("/retry")
def get_retry_queue() -> dict[str, Any]:
    return {"pending": len(retry_queue), "dead": len(retry_queue.dead)}


@router.post("/retry/run")
def run_retries() -> dict[str, Any]:
    """处理当前到期的重试条目（由后台任务或手动触发）。"""
    now = time.time()
    processed, succeeded = 0, 0
    for item in retry_queue.due(now):
        processed += 1
        try:
            ok = downstream(item.record)
        except Exception:
            ok = False
        if ok:
            retry_queue.ack(item)
            succeeded += 1
        else:
            retry_queue.fail(item, now)
    return {"processed": processed, "succeeded": succeeded,
            "pending": len(retry_queue), "dead": len(retry_queue.dead)}
