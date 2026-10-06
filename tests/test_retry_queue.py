"""重试队列测试：退避间隔、跳过未到期、不阻塞后续、死信。"""

from __future__ import annotations

import asyncio

import pytest

from app.config import Settings
from app.models import Measurement, NormalizedRecord
from app.retry_queue import RetryQueue

pytestmark = pytest.mark.retry_queue

SETTINGS = Settings(
    retry_base_delay=1.0, retry_backoff=2.0, retry_max_delay=8.0, retry_max_attempts=3,
)


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def time(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def rec(mid: str) -> NormalizedRecord:
    return NormalizedRecord(
        device_id="d1", ts=100.0, received_at=100.0,
        measurements=(Measurement(metric="v", value=1.0),),
        source_format="generic", message_id=mid, dedup_key=f"d1:{mid}",
    )


def test_首次失败立即重试_退避间隔逐次拉长():
    clk = FakeClock()
    q = RetryQueue(SETTINGS, time_func=clk.time)
    calls: list[str] = []

    async def flaky(r: NormalizedRecord) -> None:
        calls.append(r.message_id)
        if len(calls) <= 2:
            raise RuntimeError("downstream down")

    q.enqueue(rec("a"))
    # 首次失败：attempts=1，1s 后可重试
    r1 = asyncio.run(q.pump_once(flaky))
    assert r1 == {"processed": 1, "delivered": 0, "failed": 1, "dead": 0}
    assert len(q.due_entries()) == 0  # 未到期，被跳过

    clk.advance(0.9)
    assert len(q.due_entries()) == 0
    clk.advance(0.2)  # 1.1s
    due = q.due_entries()
    assert len(due) == 1
    assert due[0].attempts == 1
    # 第二次失败：attempts=2，间隔 2s
    r2 = asyncio.run(q.pump_once(flaky))
    assert r2["failed"] == 1
    assert len(q.due_entries()) == 0
    clk.advance(1.9)
    assert len(q.due_entries()) == 0
    clk.advance(0.2)
    # 第三次成功
    r3 = asyncio.run(q.pump_once(flaky))
    assert r3["delivered"] == 1
    assert len(q) == 0
    s = q.stats()
    assert s["enqueued"] == 1 and s["delivered"] == 1 and s["failed"] == 2 and s["dead_letter"] == 0


def test_超过最大次数进入死信队列():
    clk = FakeClock()
    q = RetryQueue(SETTINGS, time_func=clk.time)

    async def always_fail(r: NormalizedRecord) -> None:
        raise RuntimeError("nope")

    q.enqueue(rec("a"))
    for expected_delay in (1.0, 2.0):
        asyncio.run(q.pump_once(always_fail))
        clk.advance(expected_delay + 0.01)
    result = asyncio.run(q.pump_once(always_fail))  # 第 3 次失败 -> 死信
    assert result["dead"] == 1
    assert len(q) == 0
    assert [r.message_id for r in q.dead_letters()] == ["a"]
    assert q.stats()["dead_letter"] == 1


def test_未到期条目不阻塞其它已到期数据():
    clk = FakeClock()
    q = RetryQueue(SETTINGS, time_func=clk.time)
    delivered: list[str] = []

    async def processor(r: NormalizedRecord) -> None:
        if r.message_id == "bad":
            raise RuntimeError("fail")
        delivered.append(r.message_id)

    q.enqueue(rec("bad"))
    asyncio.run(q.pump_once(processor))   # bad 第一次失败，1s 后才可见
    q.enqueue(rec("good1"))
    q.enqueue(rec("good2"))
    result = asyncio.run(q.pump_once(processor))  # bad 未到期，两条 good 照常处理
    assert result["processed"] == 2
    assert result["delivered"] == 2
    assert delivered == ["good1", "good2"]
    assert len(q) == 1  # 只剩退避中的 bad


def test_退避上限受max_delay约束():
    q = RetryQueue(SETTINGS, time_func=lambda: 0.0)
    assert q._delay_for(1) == 1.0
    assert q._delay_for(2) == 2.0
    assert q._delay_for(3) == 4.0
    assert q._delay_for(10) == 8.0
