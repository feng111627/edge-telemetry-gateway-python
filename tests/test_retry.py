"""失败暂存与指数退避重试。"""
from app.models import TelemetryRecord
from app.retry import RetryQueue


def rec(seq):
    return TelemetryRecord("d1", "temp", 1.0, 0.0, seq, "mqtt")


def test_failed_record_is_queued_and_due_immediately():
    q = RetryQueue(base_delay=1.0, max_attempts=3)
    q.push(rec(1), now=0.0)
    assert len(q) == 1
    assert len(q.due(0.0)) == 1


def test_backoff_interval_grows_exponentially():
    q = RetryQueue(base_delay=2.0, max_attempts=5)
    q.push(rec(1), now=0.0)
    item = q.due(0.0)[0]
    q.fail(item, now=0.0)
    assert item.next_attempt_at == 2.0    # 2 * 2^0
    q.fail(item, now=2.0)
    assert item.next_attempt_at == 6.0    # 2 + 2 * 2^1
    q.fail(item, now=6.0)
    assert item.next_attempt_at == 14.0   # 6 + 2 * 2^2


def test_not_due_before_backoff_elapsed():
    q = RetryQueue(base_delay=10.0, max_attempts=5)
    q.push(rec(1), now=0.0)
    item = q.due(0.0)[0]
    q.fail(item, now=0.0)
    assert q.due(9.9) == []
    assert len(q.due(10.0)) == 1


def test_gives_up_after_max_attempts():
    q = RetryQueue(base_delay=1.0, max_attempts=3)
    q.push(rec(1), now=0.0)
    item = q.due(0.0)[0]
    assert q.fail(item, now=0.0) is True
    assert q.fail(item, now=1.0) is True
    assert q.fail(item, now=2.0) is False  # 放弃
    assert len(q) == 0
    assert q.dead == [rec(1)]


def test_one_failing_record_does_not_block_others():
    q = RetryQueue(base_delay=100.0, max_attempts=5)
    q.push(rec(1), now=0.0)
    q.push(rec(2), now=0.0)
    bad = q.due(0.0)[0]
    q.fail(bad, now=0.0)  # 第一条进入长退避
    due = q.due(1.0)
    assert len(due) == 1 and due[0].record.seq == 2  # 第二条不受影响
    q.ack(due[0])
    assert len(q) == 1


def test_ack_removes_item():
    q = RetryQueue(base_delay=1.0, max_attempts=3)
    q.push(rec(1), now=0.0)
    item = q.due(0.0)[0]
    q.ack(item)
    assert len(q) == 0
