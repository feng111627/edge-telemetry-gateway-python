"""后台自动补偿：自动退避重试、最终放弃、并发不阻塞、优雅停止。"""
import threading
import time

from app.models import TelemetryRecord
from app.retry import RetryQueue
from app.worker import RetryWorker


def rec(seq=1):
    return TelemetryRecord("d1", "t", 1.0, 0.0, seq, "mqtt")


def test_worker_retries_until_success():
    q = RetryQueue(base_delay=1.0, max_attempts=5)
    calls = {"n": 0}

    def sender(_):
        calls["n"] += 1
        return calls["n"] >= 3            # 第三次成功

    clock = {"now": 0.0}
    w = RetryWorker(q, sender, now_fn=lambda: clock["now"])
    q.push(rec(), now=0.0)
    w.run_once()                          # 第 1 次失败 -> 1s 后到期
    assert calls["n"] == 1 and len(q) == 1
    clock["now"] = 1.0
    w.run_once()                          # 第 2 次失败 -> 3s 后到期
    assert q.due(2.0) == []
    clock["now"] = 3.0
    st = w.run_once()                     # 第 3 次成功
    assert st["succeeded"] == 1 and len(q) == 0 and q.list_dead() == []


def test_worker_gives_up_into_dead_after_max_attempts():
    q = RetryQueue(base_delay=1.0, max_attempts=3)
    clock = {"now": 0.0}
    w = RetryWorker(q, lambda _: False, now_fn=lambda: clock["now"])
    q.push(rec(), now=0.0)
    for t in (0.0, 1.0, 3.0):
        clock["now"] = t
        w.run_once()
    assert len(q) == 0 and len(q.list_dead()) == 1


def test_background_worker_compensates_without_blocking_ingest():
    q = RetryQueue(base_delay=0.01, max_attempts=2)
    w = RetryWorker(q, lambda _: False, interval=0.01)
    w.start()
    try:
        q.push(rec(1), now=time.time())
        # 后台补偿期间，新数据仍能即时入队
        time.sleep(0.005)
        q.push(rec(2), now=time.time())
        deadline = time.time() + 2.0
        while time.time() < deadline and len(q.list_dead()) < 2:
            time.sleep(0.01)
        assert len(q.list_dead()) == 2
    finally:
        w.stop()
    assert not w._thread.is_alive()


def test_one_persistently_failing_record_does_not_block_others():
    q = RetryQueue(base_delay=100.0, max_attempts=5)  # 失败条目长退避
    lock = threading.Lock()
    succeeded: list[int] = []

    def sender(record):
        if record.seq == 1:
            return False
        with lock:
            succeeded.append(record.seq)
        return True

    w = RetryWorker(q, sender, now_fn=lambda: 0.0)
    q.push(rec(1), now=0.0)
    q.push(rec(2), now=0.0)
    w.run_once()                          # seq=1 退避，seq=2 当场成功
    assert succeeded == [2]
    assert len(q) == 1 and q.due(0.0) == []


def test_worker_loop_survives_sender_exception():
    q = RetryQueue(base_delay=0.001, max_attempts=2)

    def sender(_):
        raise RuntimeError("downstream down")

    w = RetryWorker(q, sender, interval=0.005)
    w.start()
    try:
        q.push(rec(), now=time.time())
        deadline = time.time() + 2.0
        while time.time() < deadline and not q.list_dead():
            time.sleep(0.01)
        assert len(q.list_dead()) == 1    # 循环没有因异常死掉，最终放弃
    finally:
        w.stop()
