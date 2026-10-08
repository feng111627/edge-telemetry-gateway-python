"""时效判定：太旧 / 未来 / 正常窗口的边界与确定性。"""
from app.freshness import classify_freshness

NOW = 1_000_000.0


def test_fresh_record_is_ok():
    assert classify_freshness(NOW, NOW, max_age=3600, future_skew=60) == "ok"
    assert classify_freshness(NOW - 100, NOW, 3600, 60) == "ok"


def test_too_old_boundary_inclusive():
    # 正好落在容忍窗口边界（含等于）算正常，超出一秒算太旧
    assert classify_freshness(NOW - 3600, NOW, 3600, 60) == "ok"
    assert classify_freshness(NOW - 3600.001, NOW, 3600, 60) == "too_old"


def test_future_distinguished_from_too_old_and_unparseable():
    assert classify_freshness(NOW + 61, NOW, 3600, 60) == "future"
    assert classify_freshness(NOW + 60, NOW, 3600, 60) == "ok"  # 时钟偏差容忍边界
    assert classify_freshness(NOW - 10_000, NOW, 3600, 60) == "too_old"


def test_classification_is_deterministic_for_repeated_report():
    # 同一条数据（同一时间戳）重复判定结论一致
    for _ in range(5):
        assert classify_freshness(NOW - 9999, NOW, 3600, 60) == "too_old"
    for _ in range(5):
        assert classify_freshness(NOW + 9999, NOW, 3600, 60) == "future"


def test_disabled_checks_keep_legacy_behavior():
    # max_age=None：再旧的数据也不判太旧
    assert classify_freshness(-10 ** 12, NOW, None, 60) == "ok"
    # future_skew=None：再未来的数据也不判未来
    assert classify_freshness(NOW + 10 ** 9, NOW, 3600, None) == "ok"
    # 两个都关闭时一律 ok
    assert classify_freshness(NOW - 10 ** 9, NOW, None, None) == "ok"
