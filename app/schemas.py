"""HTTP API 的请求 / 响应模型。"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class IngestResponse(BaseModel):
    accepted: bool
    duplicate: bool = False
    device_id: str | None = None
    dedup_key: str | None = None
    reason: str | None = None
    detail: str | None = None


class BatchIngestResponse(BaseModel):
    total: int
    accepted: int
    rejected: int
    duplicate: int
    results: list[IngestResponse]


class MeasurementOut(BaseModel):
    metric: str
    value: float


class DeviceStatusResponse(BaseModel):
    device_id: str
    liveness: str
    last_seen: float | None
    ts: float | None
    measurements: list[MeasurementOut] = Field(default_factory=list)
    history_size: int


class HistoryEntry(BaseModel):
    ts: float
    received_at: float
    source_format: str
    measurements: list[MeasurementOut]


class HistoryResponse(BaseModel):
    device_id: str
    items: list[HistoryEntry]


class RetryStats(BaseModel):
    pending: int
    dead_letter: int
    enqueued: int
    delivered: int
    failed: int
