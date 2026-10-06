"""共享的服务单例与 FastAPI 依赖。"""

from __future__ import annotations

from .service import TelemetryService

_service: TelemetryService | None = None


def get_service() -> TelemetryService:
    global _service
    if _service is None:
        _service = TelemetryService()
    return _service


def reset_service() -> TelemetryService:
    """测试 / 运维场景下重置单例。"""
    global _service
    _service = TelemetryService()
    return _service
