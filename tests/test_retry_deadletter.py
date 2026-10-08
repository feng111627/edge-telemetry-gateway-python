"""退避上限、最终放弃集合（dead-letter）查看与手动重新投递。"""
from app.models import TelemetryRecord
from app.retry import RetryQueue


def rec(seq, device="d1"):
    return TelemetryRecord(device, "temp", 1.0, 0.0, seq, "mqtt")


def exhaust(q, now_points, always_fail=True):
    """在给定时间点反复处理直到进 dead。"""
    n = 0
    sender = (lambda r: False) if always_fail else None
    for t in now_points:
        st = q.process_due(sender, t)
        n += st["processed"]
    return n


def test_backoff_is_capped_by_max_delay():
    q = RetryQueue(base_delay=1.0, max_attempts=10, max_delay=4.0)
    q.push(rec(1), now=0.0)
    item = q.due(0.0)[0]
    q.fail(item, now=0.0)
    assert item.next_attempt_at == 1.0     # delay 1
    q.fail(item, now=1.0)
    assert item.next_attempt_at == 3.0     # delay 2
    q.fail(item, now=3.0)
    assert item.next_attempt_at == 7.0     # delay 4
    q.fail(item, now=7.0)
    assert item.next_attempt_at == 11.0    # delay 封顶 4，不再翻倍


def test_dead_entry_carries_identity_and_attempts():
    q = RetryQueue(base_delay=1.0, max_attempts=3)
    q.push(rec(1), now=0.0)
    exhaust(q, [0.0, 1.0, 3.0])
    assert len(q) == 0
    entries = q.list_dead()
    assert len(entries) == 1
    assert entries[0].record.seq == 1
    assert entries[0].attempts == 3


def test_redeliver_all_does_not_duplicate_count_or_send():
    q = RetryQueue(base_delay=1.0, max_attempts=2)
    q.push(rec(1), now=0.0)
    q.push(rec(2, "d2"), now=0.0)
    exhaust(q, [0.0, 1.0])
    ids = [e.item_id for e in q.list_dead()]
    assert sorted(q.redeliver(0.0)) == sorted(ids)
    assert q.redeliver(0.0) == []          # dead 已空：不会重复重投
    assert len(q) == 2 and q.list_dead() == []
    # 重投后立即被 claim 的仍是各自唯一一条（下游不会收到重复）
    claimed = q.claim_due(0.0)
    assert {it.record.seq for it in claimed} == {1, 2}


def test_redeliver_single_entry():
    q = RetryQueue(base_delay=1.0, max_attempts=2)
    q.push(rec(1), now=0.0)
    q.push(rec(2, "d2"), now=0.0)
    exhaust(q, [0.0, 1.0])
    target = next(e for e in q.list_dead() if e.record.device_id == "d2")
    assert q.redeliver(0.0, item_id=target.item_id) == [target.item_id]
    assert len(q.list_dead()) == 1         # 另一条仍在 dead
    assert len(q) == 1
    assert q.redeliver(0.0, item_id=999_999) == []  # 不存在的 id 安全无操作


def test_claim_due_prevents_double_dispatch():
    q = RetryQueue(base_delay=1.0, max_attempts=3)
    q.push(rec(1), now=0.0)
    first = q.claim_due(0.0)
    assert len(first) == 1
    assert q.claim_due(0.0) == []          # 投递中的条目不被再次送出
    q.fail(first[0], now=0.0)
    assert q.claim_due(0.5) == []          # 退避未到期
    assert q.claim_due(1.0)[0].record.seq == 1


def test_counts_by_device_includes_pending_and_dead():
    q = RetryQueue(base_delay=1.0, max_attempts=2)
    q.push(rec(1, "d1"), now=0.0)
    q.push(rec(2, "d1"), now=0.0)
    q.push(rec(3, "d2"), now=0.0)
    # 让 d1 的第一条放弃
    item = q.due(0.0)[0]
    q.fail(item, 0.0)
    q.fail(item, 1.0)
    pending, dead = q.counts_by_device()
    assert pending == {"d1": 1, "d2": 1}
    assert dead == {"d1": 1}


def test_process_due_isolates_per_record_failure():
    # 下游对 seq=1 始终抛异常，其它正常：单条不拖累其余
    q = RetryQueue(base_delay=1.0, max_attempts=3)
    q.push(rec(1), now=0.0)
    q.push(rec(2), now=0.0)

    def sender(record):
        if record.seq == 1:
            raise RuntimeError("boom")
        return True

    st = q.process_due(sender, 0.0)
    assert st["processed"] == 2 and st["succeeded"] == 1
    assert len(q) == 1                     # 只有失败的那条留下
