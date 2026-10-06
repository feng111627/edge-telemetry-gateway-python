"""在线状态判定。

按设备最后一次上报距今的时长划分：
- 距 now <= lagging_after      -> NORMAL（正常）
- lagging_after < 距 now <= offline_after -> LAGGING（迟滞）
- 距 now > offline_after 或从未上报 -> OFFLINE（离线）
"""
from __future__ import annotations

from app.models import OnlineStatus


def evaluate(last_seen: float | None, now: float,
             lagging_after: float = 60.0, offline_after: float = 300.0) -> OnlineStatus:
    if last_seen is None or last_seen > now:
        # 从未上报视为离线；时间戳在未来（时钟异常）按正常处理，避免误判
        return OnlineStatus.OFFLINE if last_seen is None else OnlineStatus.NORMAL
    elapsed = now - last_seen
    if elapsed <= lagging_after:
        return OnlineStatus.NORMAL
    if elapsed <= offline_after:
        return OnlineStatus.LAGGING
    return OnlineStatus.OFFLINE
