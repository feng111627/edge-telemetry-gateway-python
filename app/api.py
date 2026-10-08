"""FastAPI 路由：接入、查询、在线状态、重试补偿、接入质量汇总。

下游转发通过 `downstream` 可调用对象完成，失败的数据进入 RetryQueue 暂存；
后台 RetryWorker 按固定间隔自动补偿（指数退避、有上限、超限放弃入 dead），
也可通过 /retry/run 手动触发同一套处理逻辑。测试可替换 downstream 模拟失败。
"""
from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

from fastapi import APIRouter, Body
from fastapi.responses import JSONResponse

from app.metrics import IngestMetrics
from app.models import TelemetryRecord
from app.normalizer import extract_device_id, normalize
from app.retry import RetryQueue
from app.status import evaluate
from app.store import IngestOutcome, TelemetryStore
from app.worker import RetryWorker, drain_due

LAGGING_AFTER = 60.0
OFFLINE_AFTER = 300.0

# 时效判定容忍范围（秒）：比当前时间早/晚超过该范围的数据不改动设备状态
MAX_PAST_AGE = 24 * 3600.0
MAX_FUTURE_SKEW = 300.0

# 重试补偿参数：指数退避、间隔上限、最大尝试次数、后台扫描间隔
RETRY_BASE_DELAY = 1.0
RETRY_MAX_DELAY = 60.0
RETRY_MAX_ATTEMPTS = 5
WORKER_INTERVAL = 1.0

router = APIRouter()
store = TelemetryStore(history_limit=100,
                       max_past_age=MAX_PAST_AGE,
                       max_future_skew=MAX_FUTURE_SKEW)
retry_queue = RetryQueue(base_delay=RETRY_BASE_DELAY,
                         max_attempts=RETRY_MAX_ATTEMPTS,
                         max_delay=RETRY_MAX_DELAY)
metrics = IngestMetrics()

# 下游转发钩子：返回 True 表示成功，抛异常或返回 False 表示失败。
downstream: Callable[[TelemetryRecord], bool] = lambda record: True


def _deliver(record: TelemetryRecord) -> bool:
    """投递到下游，异常视为失败。"""
    try:
        return bool(downstream(record))
    except Exception:
        return False


worker: RetryWorker | None = None


def start_worker() -> None:
    """启动后台自动补偿线程（按当前 queue/downstream 配置重建）。"""
    global worker
    stop_worker()
    worker = RetryWorker(retry_queue, _deliver, interval=WORKER_INTERVAL)
    worker.start()


def stop_worker() -> None:
    global worker
    if worker is not None:
        worker.stop()
        worker = None


def _forward(record: TelemetryRecord) -> bool:
    """转发到下游，失败则暂存重试队列（由后台自动补偿）。返回是否成功。"""
    ok = _deliver(record)
    if not ok:
        retry_queue.push(record, now=time.time())
    return ok


def _ingest_one(source: str, payload: Any) -> dict[str, Any]:
    result = normalize(source, payload)
    if not result.ok:
        metrics.record_rejected(extract_device_id(source, payload))
        return {
            "accepted": False,
            "reason": result.reason.value if result.reason else "unknown",
            "detail": result.detail,
        }
    record = result.record
    assert record is not None
    outcome = store.ingest(record)
    metrics.record_outcome(record.device_id, outcome)
    # 太旧或未来时间戳的数据不转发下游，避免把异常数据带出去
    forwarded = True
    if outcome in (IngestOutcome.ACCEPTED, IngestOutcome.STALE):
        forwarded = _forward(record)
    return {
        "accepted": outcome in (IngestOutcome.ACCEPTED, IngestOutcome.STALE),
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
        return JSONResponse(status_code=404,
                            content={"detail": "device not found"})
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
        return JSONResponse(status_code=404,
                            content={"detail": "device not found"})
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
    """处理当前到期的重试条目（与后台自动补偿共用同一套逻辑）。"""
    processed, succeeded = drain_due(retry_queue, _deliver, time.time())
    return {"processed": processed, "succeeded": succeeded,
            "pending": len(retry_queue), "dead": len(retry_queue.dead)}


@router.get("/retry/dead")
def list_dead() -> dict[str, Any]:
    """查看最终放弃（超过最大尝试次数）的数据集合。"""
    return {"dead": [r.__dict__ for r in retry_queue.dead]}


@router.post("/retry/dead/redrive")
def redrive_dead(payload: Any = Body(None)) -> dict[str, Any]:
    """把 dead 中的数据重新放回重试队列：默认全部，也可指定 {"index": n}。

    重新投递是「移动」而非「复制」：不会重复计数，同一条数据也不会
    被重复送给下游。
    """
    index = None
    if isinstance(payload, dict) and "index" in payload:
        index = payload["index"]
        if not isinstance(index, int) or isinstance(index, bool):
            return JSONResponse(status_code=422,
                                content={"detail": "index 必须是整数"})
    try:
        count = retry_queue.redrive(now=time.time(), index=index)
    except IndexError:
        return JSONResponse(status_code=404,
                            content={"detail": "dead 下标越界"})
    return {"redriven": count, "pending": len(retry_queue),
            "dead": len(retry_queue.dead)}


@router.get("/metrics/summary")
def metrics_summary() -> dict[str, Any]:
    """按设备汇总接入质量：各类结果计数与当前补偿积压。"""
    return {"devices": metrics.summary(retry_queue)}
