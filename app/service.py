"""编排层：归一化 -> 存储 -> 下游投递（失败入重试队列）。"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Any

from .config import Settings, settings as default_settings
from .errors import IngestError
from .models import IngestResult, Liveness, NormalizedRecord
from .normalizer import normalize as _normalize
from .retry_queue import Processor, RetryQueue
from .store import DeviceStore


class TelemetryService:
    def __init__(
        self,
        settings: Settings = default_settings,
        *,
        time_func: Callable[[], float] = time.time,
        processor: Processor | None = None,
    ) -> None:
        self._settings = settings
        self._time = time_func
        self.store = DeviceStore(settings)
        self.retry_queue = RetryQueue(settings, time_func=time_func)
        self._processor: Processor | None = processor
        self._pump_task: asyncio.Task | None = None

    def set_processor(self, processor: Processor) -> None:
        """注册下游处理器（例如转发到消息队列 / 告警引擎）。"""
        self._processor = processor

    def start_background_pump(self, interval: float = 0.1) -> asyncio.Task:
        """启动后台重试循环；重复调用时返回既有任务。"""
        if self._pump_task is None or self._pump_task.done():
            self._pump_task = asyncio.create_task(self._pump_loop(interval))
        return self._pump_task

    async def stop_background_pump(self) -> None:
        if self._pump_task is not None:
            self._pump_task.cancel()
            try:
                await self._pump_task
            except asyncio.CancelledError:
                pass
            self._pump_task = None

    async def _pump_loop(self, interval: float) -> None:
        while True:
            try:
                await self.retry_pending()
            except Exception:  # pragma: no cover - 后台循环必须永不退出
                pass
            await asyncio.sleep(interval)

    async def ingest_one(self, payload: Any) -> IngestResult:
        """处理一条原始上报。

        流程：归一化（失败带可区分原因拒绝）-> 幂等写入存储 ->
        投递下游（失败只暂存，不影响上报被接受这一结论）。
        """
        now = self._time()
        try:
            record: NormalizedRecord = _normalize(payload, now=now, settings=self._settings)
        except IngestError as exc:
            device_id = payload.get("device_id") if isinstance(payload, dict) else None
            return IngestResult(
                accepted=False, dedup_key="",
                device_id=device_id if isinstance(device_id, str) else None,
                reason=exc.reason, detail=exc.detail,
            )

        duplicate = await self.store.apply(record)
        if not duplicate and self._processor is not None:
            try:
                await self._processor(record)
            except Exception:
                # 下游失败不丢数据：暂存后由退避重试补偿
                self.retry_queue.enqueue(record)

        return IngestResult(
            accepted=True, dedup_key=record.dedup_key,
            device_id=record.device_id, duplicate=duplicate,
        )

    async def ingest_batch(self, payloads: list[Any]) -> list[IngestResult]:
        """批量接入：逐条独立处理，单条畸形不影响其它条目。"""
        return list(await asyncio.gather(*(self.ingest_one(p) for p in payloads)))

    async def retry_pending(self) -> dict[str, int]:
        """触发一轮重试。未注册下游处理器时为空操作。"""
        if self._processor is None:
            return {"processed": 0, "delivered": 0, "failed": 0, "dead": 0}
        return await self.retry_queue.pump_once(self._processor)

    def liveness(self, device_id: str) -> Liveness:
        return self.store.liveness(device_id, now=self._time())

    def status(self, device_id: str) -> dict[str, Any]:
        state = self.store.get_state(device_id)
        latest = state.latest if state else None
        return {
            "device_id": device_id,
            "liveness": self.liveness(device_id).value,
            "last_seen": latest.received_at if latest else None,
            "ts": latest.ts if latest else None,
            "measurements": (
                [{"metric": m.metric, "value": m.value} for m in latest.measurements]
                if latest else []
            ),
            "history_size": len(state.history) if state else 0,
        }

    def history(self, device_id: str, limit: int | None = None) -> list[dict[str, Any]]:
        return [
            {
                "ts": r.ts,
                "received_at": r.received_at,
                "source_format": r.source_format,
                "measurements": [{"metric": m.metric, "value": m.value} for m in r.measurements],
            }
            for r in self.store.history(device_id, limit)
        ]
