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


def test_backoff_capped_at_max_delay():
    q = RetryQueue(base_delay=10.0, max_attempts=6, max_delay=25.0)
    q.push(rec(1), now=0.0)
    item = q.due(0.0)[0]
    q.fail(item, now=0.0)
    assert item.next_attempt_at == 10.0   # 10 * 2^0
    q.fail(item, now=10.0)
    assert item.next_attempt_at == 30.0   # 10 + 10 * 2^1
    q.fail(item, now=30.0)
    assert item.next_attempt_at == 55.0   # 30 + min(40, 25)：触及上限
    q.fail(item, now=55.0)
    assert item.next_attempt_at == 80.0   # 继续封顶在 25，不再指数增长


def test_max_delay_must_be_at_least_base_delay():
    import pytest

    with pytest.raises(ValueError):
        RetryQueue(base_delay=10.0, max_attempts=3, max_delay=5.0)


def test_redrive_all_moves_dead_back_to_pending():
    q = RetryQueue(base_delay=1.0, max_attempts=1)
    q.push(rec(1), now=0.0)
    q.push(rec(2), now=0.0)
    for item in q.due(0.0):
        q.fail(item, now=0.0)
    assert len(q.dead) == 2 and len(q) == 0

    assert q.redrive(now=10.0) == 2
    assert len(q.dead) == 0 and len(q) == 2
    # 重新投递的条目立即到期、尝试次数清零
    due = q.due(10.0)
    assert len(due) == 2 and all(it.attempts == 0 for it in due)


def test_redrive_single_by_index():
    q = RetryQueue(base_delay=1.0, max_attempts=1)
    q.push(rec(1), now=0.0)
    q.push(rec(2), now=0.0)
    for item in q.due(0.0):
        q.fail(item, now=0.0)

    assert q.redrive(now=5.0, index=0) == 1
    assert len(q) == 1
    assert len(q.dead) == 1 and q.dead[0].seq == 2  # 另一条仍留在 dead


def test_redrive_invalid_index_raises():
    import pytest

    q = RetryQueue(base_delay=1.0, max_attempts=1)
    q.push(rec(1), now=0.0)
    q.fail(q.due(0.0)[0], now=0.0)
    with pytest.raises(IndexError):
        q.redrive(now=0.0, index=5)


def test_redrive_does_not_double_deliver():
    # 重新投递是「移动」：再次 redrive 全部时 dead 已空，不会重复投递
    q = RetryQueue(base_delay=1.0, max_attempts=1)
    q.push(rec(1), now=0.0)
    q.fail(q.due(0.0)[0], now=0.0)
    assert q.redrive(now=1.0) == 1
    assert q.redrive(now=1.0) == 0
    assert len(q) == 1  # 队列里只有一条，不会重复送给下游


def test_redriven_record_can_succeed_and_ack():
    q = RetryQueue(base_delay=1.0, max_attempts=1)
    q.push(rec(1), now=0.0)
    q.fail(q.due(0.0)[0], now=0.0)
    q.redrive(now=1.0)
    item = q.due(1.0)[0]
    q.ack(item)
    assert len(q) == 0 and len(q.dead) == 0


def test_counts_by_device():
    q = RetryQueue(base_delay=1.0, max_attempts=1)
    q.push(TelemetryRecord("d1", "temp", 1.0, 0.0, 1, "mqtt"), now=0.0)
    q.push(TelemetryRecord("d1", "hum", 2.0, 0.0, 2, "mqtt"), now=0.0)
    q.push(TelemetryRecord("d2", "temp", 3.0, 0.0, 1, "mqtt"), now=0.0)
    # d2 的直接放弃进 dead
    item = [it for it in q.due(0.0) if it.record.device_id == "d2"][0]
    q.fail(item, now=0.0)
    assert q.pending_count_by_device() == {"d1": 2}
    assert q.dead_count_by_device() == {"d2": 1}
