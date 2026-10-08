"""后台自动补偿：自动重试、放弃入 dead、重新投递后再自动补偿。"""
import threading
import time

from app.models import TelemetryRecord
from app.retry import RetryQueue
from app.worker import RetryWorker, drain_due


def rec(seq, device="d1"):
    return TelemetryRecord(device, "temp", 1.0, 0.0, seq, "mqtt")


def wait_for(cond, timeout=2.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return True
        time.sleep(0.005)
    return False


def test_worker_retries_automatically_without_manual_trigger():
    q = RetryQueue(base_delay=0.02, max_attempts=5)
    delivered = []
    worker = RetryWorker(q, delivered.append, interval=0.01)
    worker.start()
    try:
        q.push(rec(1), now=time.time())
        assert wait_for(lambda: len(delivered) == 1)  # 无需手动触发即自动补偿
        assert wait_for(lambda: len(q) == 0)
    finally:
        worker.stop()


def test_worker_gives_up_to_dead_after_max_attempts():
    q = RetryQueue(base_delay=0.01, max_attempts=3)
    worker = RetryWorker(q, lambda record: False, interval=0.01)
    worker.start()
    try:
        q.push(rec(1), now=time.time())
        assert wait_for(lambda: len(q.dead) == 1)
        assert len(q) == 0  # 放弃后停止自动重试
    finally:
        worker.stop()


def test_redriven_dead_record_retried_automatically():
    q = RetryQueue(base_delay=0.01, max_attempts=2)
    state = {"fail": True}

    def deliver(record):
        return not state["fail"]

    worker = RetryWorker(q, deliver, interval=0.01)
    worker.start()
    try:
        q.push(rec(1), now=time.time())
        assert wait_for(lambda: len(q.dead) == 1)
        # 手动重新投递后，下游恢复，后台自动补偿成功
        state["fail"] = False
        q.redrive(now=time.time())
        assert wait_for(lambda: len(q) == 0 and len(q.dead) == 0)
    finally:
        worker.stop()


def test_one_failing_record_does_not_block_others_in_worker():
    q = RetryQueue(base_delay=10.0, max_attempts=5)  # 长退避
    delivered = []
    worker = RetryWorker(
        q, lambda record: delivered.append(record.seq) or record.seq != 1,
        interval=0.01)
    worker.start()
    try:
        q.push(rec(1), now=time.time())   # 这条会一直失败，进入长退避
        q.push(rec(2), now=time.time())
        assert wait_for(lambda: 2 in delivered)
        assert len(q) == 1  # 只剩坏数据在等退避，好数据已被处理
    finally:
        worker.stop()


def test_drain_due_counts_processed_and_succeeded():
    q = RetryQueue(base_delay=1.0, max_attempts=3)
    q.push(rec(1), now=0.0)
    q.push(rec(2), now=0.0)
    processed, succeeded = drain_due(
        q, lambda record: record.seq == 1, now=0.0)
    assert (processed, succeeded) == (2, 1)
    assert len(q) == 1  # 失败的那条回到队列等待退避


def test_worker_stop_is_clean_and_idempotent():
    q = RetryQueue(base_delay=1.0, max_attempts=3)
    worker = RetryWorker(q, lambda record: True, interval=0.01)
    worker.start()
    worker.start()  # 重复 start 不报错
    worker.stop()
    worker.stop()  # 重复 stop 也不报错
    assert worker._thread is None


def test_worker_concurrent_push_does_not_block_ingest_path():
    # 后台补偿运行时，接入线程可以无锁竞争地继续 push
    q = RetryQueue(base_delay=0.01, max_attempts=50)
    worker = RetryWorker(q, lambda record: False, interval=0.005)
    worker.start()
    try:
        done = threading.Event()

        def producer():
            for i in range(200):
                q.push(rec(i), now=time.time())
            done.set()

        t = threading.Thread(target=producer)
        t.start()
        t.join(timeout=5.0)
        assert done.is_set()  # 接入路径没有被后台补偿卡死
    finally:
        worker.stop()
