"""编排层测试：接入、拒绝、幂等、下游失败暂存、状态查询。"""

from __future__ import annotations

import asyncio

import pytest

from app.config import Settings
from app.errors import RejectReason
from app.models import NormalizedRecord
from app.service import TelemetryService

pytestmark = pytest.mark.service


class FakeClock:
    def __init__(self, start: float = 1_000_000.0) -> None:
        self.now = start

    def time(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture()
def clk() -> FakeClock:
    return FakeClock()


@pytest.fixture()
def svc(clk) -> TelemetryService:
    return TelemetryService(
        Settings(history_capacity=10, stale_after_seconds=60, offline_after_seconds=300,
                 expire_after_seconds=3600, max_clock_skew_seconds=60,
                 retry_base_delay=1.0, retry_backoff=2.0, retry_max_delay=8.0,
                 retry_max_attempts=2),
        time_func=clk.time,
    )


def generic_payload(device: str = "d1", value: float = 1.0, *, ts=None, mid=None) -> dict:
    p = {"device_id": device, "ts": ts if ts is not None else 1_000_000.0,
         "measurements": [{"metric": "v", "value": value}]}
    if mid:
        p["message_id"] = mid
    return p


def test_正常接入返回accepted并可查询最新状态(svc):
    result = asyncio.run(svc.ingest_one(generic_payload(value=42.0)))
    assert result.accepted is True
    status = svc.status("d1")
    assert status["liveness"] == "online"
    assert status["measurements"] == [{"metric": "v", "value": 42.0}]
    assert status["history_size"] == 1


def test_畸形上报带可区分原因且不被静默吞掉(svc, clk):
    cases = [
        ("not-json", RejectReason.NOT_A_OBJECT),
        ({"device_id": "d1", "ts": clk.now, "measurements": []}, RejectReason.BAD_TYPE),
        ({"device_id": "d1", "ts": clk.now, "measurements": [{"metric": "v", "value": "x"}]},
         RejectReason.BAD_TYPE),
        ({"device_id": "d1", "weird": True}, RejectReason.UNKNOWN_FORMAT),
    ]
    for payload, reason in cases:
        result = asyncio.run(svc.ingest_one(payload))
        assert result.accepted is False
        assert result.reason is reason, payload
    assert svc.status("d1")["history_size"] == 0


def test_重复上报稳定_不重复计数_不重复投递(svc, clk):
    delivered: list[NormalizedRecord] = []

    async def proc(r: NormalizedRecord) -> None:
        delivered.append(r)

    svc.set_processor(proc)
    p = generic_payload(mid="m-1", value=1.0)
    r1 = asyncio.run(svc.ingest_one(p))
    r2 = asyncio.run(svc.ingest_one(generic_payload(mid="m-1", value=9.0)))
    assert r1.accepted and r2.accepted and r2.duplicate
    assert r1.dedup_key == r2.dedup_key
    assert svc.store.accepted_counts["d1"] == 1
    assert len(delivered) == 1  # 重复数据不再次触发下游
    assert svc.status("d1")["measurements"][0]["value"] == 1.0


def test_下游失败数据被暂存_退避重试后成功_不影响接入结论(svc, clk):
    flaky = {"fail": True}

    async def processor(r: NormalizedRecord) -> None:
        if flaky["fail"]:
            raise ConnectionError("broker unavailable")

    svc.set_processor(processor)
    result = asyncio.run(svc.ingest_one(generic_payload()))
    assert result.accepted is True  # 接入成功与下游状态解耦
    assert svc.retry_queue.stats()["pending"] == 1

    # 立即重试会立刻真正尝试（首次失败时立即可见），再次失败后进入 1s 退避
    pumped = asyncio.run(svc.retry_pending())
    assert pumped["processed"] == 1 and pumped["failed"] == 1
    # 退避窗口内 -> 无处理
    assert asyncio.run(svc.retry_pending())["processed"] == 0
    # 推进时间超过 1s 退避
    clk.advance(1.1)
    flaky["fail"] = False
    pumped = asyncio.run(svc.retry_pending())
    assert pumped["delivered"] == 1
    assert svc.retry_queue.stats()["pending"] == 0


def test_反复失败单条不拖住后续数据接入(svc, clk):
    async def always_fail(r: NormalizedRecord) -> None:
        raise RuntimeError("down")

    svc.set_processor(always_fail)
    asyncio.run(svc.ingest_one(generic_payload(device="d1", mid="bad")))  # 进入退避
    # 另一个设备紧接着的数据仍然正常接入（也投递失败暂存，但接入不被阻塞）
    r = asyncio.run(svc.ingest_one(generic_payload(device="d2", mid="good")))
    assert r.accepted is True
    assert len(svc.retry_queue) == 2
    due = svc.retry_queue.due_entries()
    # 两条都是刚失败、立即可见？第一条已在 pump 前未 pump，故都到期；
    # pump 一次后第一条退避，第二条可单独成功，互不阻塞
    async def succeed_for_d2(r: NormalizedRecord) -> None:
        if r.device_id == "d1":
            raise RuntimeError("still down")

    svc.set_processor(succeed_for_d2)
    out = asyncio.run(svc.retry_pending())
    assert out["processed"] == 2
    assert out["delivered"] == 1
    assert out["failed"] == 1
    pending = svc.retry_queue.due_entries()
    assert len(pending) == 0  # d1 已进入退避，不再到期
    assert len(svc.retry_queue) == 1


def test_批量接入_单条畸形不影响其它(svc):
    results = asyncio.run(svc.ingest_batch([
        generic_payload(device="a"),
        "garbage",
        generic_payload(device="b"),
    ]))
    assert [r.accepted for r in results] == [True, False, True]
    assert results[1].reason is RejectReason.NOT_A_OBJECT


def test_在线状态随时间迁移(svc, clk):
    asyncio.run(svc.ingest_one(generic_payload()))
    assert svc.liveness("d1").value == "online"
    clk.advance(61)
    assert svc.liveness("d1").value == "stale"
    clk.advance(240)
    assert svc.liveness("d1").value == "offline"
