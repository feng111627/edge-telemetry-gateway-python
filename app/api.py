"""HTTP 路由层。"""

from __future__ import annotations

from fastapi import APIRouter

from .schemas import (
    BatchIngestResponse,
    DeviceStatusResponse,
    HistoryResponse,
    IngestResponse,
    RetryStats,
)

router = APIRouter()


@router.post("/ingest", response_model=IngestResponse)
async def ingest(payload: dict) -> IngestResponse:
    raise NotImplementedError


@router.post("/ingest/batch", response_model=BatchIngestResponse)
async def ingest_batch(payload: dict) -> BatchIngestResponse:
    raise NotImplementedError


@router.get("/devices/{device_id}/status", response_model=DeviceStatusResponse)
async def device_status(device_id: str) -> DeviceStatusResponse:
    raise NotImplementedError


@router.get("/devices/{device_id}/history", response_model=HistoryResponse)
async def device_history(device_id: str, limit: int | None = None) -> HistoryResponse:
    raise NotImplementedError


@router.post("/retry/pump")
async def retry_pump() -> dict:
    raise NotImplementedError


@router.get("/retry/stats", response_model=RetryStats)
async def retry_stats() -> RetryStats:
    raise NotImplementedError
