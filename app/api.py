"""HTTP 路由层。"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from .dependencies import get_service
from .schemas import (
    BatchIngestResponse,
    DeviceStatusResponse,
    HistoryEntry,
    HistoryResponse,
    IngestResponse,
    RetryStats,
)
from .service import TelemetryService

router = APIRouter()


def _to_ingest_response(result) -> IngestResponse:
    return IngestResponse(
        accepted=result.accepted,
        duplicate=result.duplicate,
        device_id=result.device_id,
        dedup_key=result.dedup_key or None,
        reason=result.reason.value if result.reason is not None else None,
        detail=result.detail or None,
    )


@router.post("/ingest", response_model=IngestResponse)
async def ingest(
    request: Request, service: TelemetryService = Depends(get_service)
) -> IngestResponse | JSONResponse:
    """接入单条设备上报；畸形 JSON 返回 400 与 malformed_json 原因。"""
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse(
            status_code=400,
            content={"accepted": False, "reason": "malformed_json", "detail": "请求体不是合法 JSON"},
        )
    result = await service.ingest_one(payload)
    return _to_ingest_response(result)


@router.post("/ingest/batch", response_model=BatchIngestResponse)
async def ingest_batch(
    request: Request, service: TelemetryService = Depends(get_service)
) -> BatchIngestResponse | JSONResponse:
    """批量接入。请求体需为 JSON 数组，数组中每条独立判定。"""
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse(
            status_code=400,
            content={"accepted": False, "reason": "malformed_json", "detail": "请求体不是合法 JSON"},
        )
    if not isinstance(payload, list):
        return JSONResponse(
            status_code=400,
            content={"accepted": False, "reason": "not_an_array", "detail": "批量接口请求体必须是数组"},
        )
    results = await service.ingest_batch(payload)
    items = [_to_ingest_response(r) for r in results]
    return BatchIngestResponse(
        total=len(items),
        accepted=sum(1 for r in items if r.accepted and not r.duplicate),
        rejected=sum(1 for r in items if not r.accepted),
        duplicate=sum(1 for r in items if r.duplicate),
        results=items,
    )


@router.get("/devices/{device_id}/status", response_model=DeviceStatusResponse)
async def device_status(
    device_id: str, service: TelemetryService = Depends(get_service)
) -> DeviceStatusResponse:
    return DeviceStatusResponse(**service.status(device_id))


@router.get("/devices/{device_id}/history", response_model=HistoryResponse)
async def device_history(
    device_id: str, limit: int | None = None,
    service: TelemetryService = Depends(get_service),
) -> HistoryResponse:
    rows: list[dict[str, Any]] = service.history(device_id, limit=limit)
    return HistoryResponse(
        device_id=device_id,
        items=[HistoryEntry(**row) for row in rows],
    )


@router.post("/retry/pump")
async def retry_pump(service: TelemetryService = Depends(get_service)) -> dict[str, int]:
    return await service.retry_pending()


@router.get("/retry/stats", response_model=RetryStats)
async def retry_stats(service: TelemetryService = Depends(get_service)) -> RetryStats:
    return RetryStats(**service.retry_queue.stats())
