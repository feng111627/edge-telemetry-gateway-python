"""在线状态判定：正常 / 迟滞 / 离线 的阈值边界。"""
from app.models import OnlineStatus
from app.status import evaluate

NOW = 10_000.0


def test_never_reported_is_offline():
    assert evaluate(None, NOW) is OnlineStatus.OFFLINE


def test_recent_report_is_normal():
    assert evaluate(NOW - 59, NOW) is OnlineStatus.NORMAL
    assert evaluate(NOW - 60, NOW) is OnlineStatus.NORMAL  # 边界含等于


def test_lagging_between_thresholds():
    assert evaluate(NOW - 61, NOW) is OnlineStatus.LAGGING
    assert evaluate(NOW - 300, NOW) is OnlineStatus.LAGGING


def test_offline_beyond_threshold():
    assert evaluate(NOW - 301, NOW) is OnlineStatus.OFFLINE


def test_future_timestamp_treated_as_normal():
    # 设备时钟异常导致 last_seen 在未来时，不误判为离线
    assert evaluate(NOW + 10, NOW) is OnlineStatus.NORMAL


def test_custom_thresholds():
    assert evaluate(NOW - 5, NOW, lagging_after=10, offline_after=20) is OnlineStatus.NORMAL
    assert evaluate(NOW - 15, NOW, lagging_after=10, offline_after=20) is OnlineStatus.LAGGING
    assert evaluate(NOW - 25, NOW, lagging_after=10, offline_after=20) is OnlineStatus.OFFLINE
