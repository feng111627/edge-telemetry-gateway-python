"""设备存储测试：去重、历史淘汰、乱序、并发隔离、在线判定。"""

from __future__ import annotations

import asyncio

import pytest

from app.config import Settings
from app.models import Liveness, Measurement, NormalizedRecord
from app.store import DeviceStore

pytestmark = pytest.mark.store


def make_record(device: str, ts: float, value: float = 1.0, *, received_at: float | None = None,
                mid: str | None = None) -> NormalizedRecord:
    measurements = (Measurement(metric="v", value=value),)
    key = f"{device}:{mid}" if mid else f"{device}:c{ts}-{value}"
    return NormalizedRecord(
        device_id=device, ts=ts, received_at=received_at if received_at is not None else ts,
        measurements=measurements, source_format="generic", message_id=mid, dedup_key=key,
    )


def store(capacity: int = 3) -> DeviceStore:
    return DeviceStore(Settings(
        history_capacity=capacity, stale_after_seconds=60, offline_after_seconds=300,
        max_clock_skew_seconds=0, expire_after_seconds=10**9, dedup_window=8,
    ))


async def apply(rec):
    return await store_obj.apply(rec)


store_obj: DeviceStore = store()


@pytest.fixture(autouse=True)
def _fresh_store():
    global store_obj
    store_obj = store()
    yield


def test_首次写入更新最新值与历史():
    dup = asyncio.run(store_obj.apply(make_record("d1", 100, 1)))
    assert dup is False
    state = store_obj.get_state("d1")
    assert state.latest.ts == 100
    assert store_obj.history("d1")[0].ts == 100


def test_同一数据重复上报结果稳定且不重复计数():
    r1 = make_record("d1", 100, 1, mid="m1")
    assert asyncio.run(store_obj.apply(r1)) is False
    r2 = make_record("d1", 100, 9, mid="m1")  # 即使值不同，同一 message_id 仍是同一条
    assert asyncio.run(store_obj.apply(r2)) is True
    assert store_obj.accepted_counts["d1"] == 1
    assert store_obj.duplicate_counts["d1"] == 1
    # 最新值保持首次写入的内容
    assert store_obj.get_state("d1").latest.measurements[0].value == 1
    assert len(store_obj.history("d1")) == 1


def test_历史按时间升序_乱序数据插入到正确位置():
    for ts in (100, 300, 200):
        asyncio.run(store_obj.apply(make_record("d1", ts)))
    assert [r.ts for r in store_obj.history("d1")] == [100, 200, 300]


def test_晚到数据进入有序历史但不刷新最新值():
    asyncio.run(store_obj.apply(make_record("d1", 200, value=2.0)))
    asyncio.run(store_obj.apply(make_record("d1", 100, value=1.0)))
    # 历史按时间升序
    assert [r.ts for r in store_obj.history("d1")] == [100, 200]
    # 最新值仍是时间戳更大的那条
    assert store_obj.get_state("d1").latest.ts == 200
    assert store_obj.get_state("d1").latest.measurements[0].value == 2.0
    assert store_obj.accepted_counts["d1"] == 2


def test_历史超容量按最旧优先淘汰():
    for ts in (100, 200, 300, 400, 500):
        asyncio.run(store_obj.apply(make_record("d1", ts)))
    assert [r.ts for r in store_obj.history("d1")] == [300, 400, 500]


def test_不同设备的状态与历史互不串扰():
    asyncio.run(store_obj.apply(make_record("a", 100)))
    asyncio.run(store_obj.apply(make_record("b", 200)))
    assert store_obj.get_state("a").latest.device_id == "a"
    assert [r.device_id for r in store_obj.history("a")] == ["a"]
    assert store_obj.liveness("a", now=100) is Liveness.ONLINE
    assert store_obj.liveness("never-seen", now=100) is Liveness.UNKNOWN


def test_在线情况按最后接收时间区分正常迟滞离线():
    asyncio.run(store_obj.apply(make_record("d1", 100, received_at=100)))
    assert store_obj.liveness("d1", now=159) is Liveness.ONLINE
    assert store_obj.liveness("d1", now=161) is Liveness.STALE
    assert store_obj.liveness("d1", now=401) is Liveness.OFFLINE


def test_多设备并发上报互不阻塞不错位():
    async def scenario():
        await asyncio.gather(*[
            store_obj.apply(make_record(f"dev-{d}", 100 + i))
            for d in range(10) for i in range(20)
        ])

    asyncio.run(scenario())
    for d in range(10):
        dev = f"dev-{d}"
        # 每设备 20 条不同 ts，但历史容量为 3
        assert len(store_obj.history(dev)) == 3
        assert store_obj.accepted_counts[dev] == 20
        assert {r.device_id for r in store_obj.history(dev)} == {dev}
        assert store_obj.get_state(dev).latest.ts == 119


def test_同一设备高并发下重复上报只计数一次():
    rec = make_record("d1", 100, mid="only")

    async def scenario():
        results = await asyncio.gather(*[store_obj.apply(rec) for _ in range(50)])
        return results

    results = asyncio.run(scenario())
    assert results.count(False) == 1
    assert results.count(True) == 49
    assert store_obj.accepted_counts["d1"] == 1
