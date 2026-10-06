"""全局配置：历史容量、在线判定阈值、重试参数等。"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    # 每设备历史记录容量上限
    history_capacity: int = 100
    # 在线判定阈值（秒）
    stale_after_seconds: float = 60.0
    offline_after_seconds: float = 300.0
    # 时间戳容错
    max_clock_skew_seconds: float = 60.0
    expire_after_seconds: float = 3600.0
    # 幂等去重保留窗口（每设备，按内容指纹）
    dedup_window: int = 4096
    # 重试：基础间隔、倍数、最大次数
    retry_base_delay: float = 1.0
    retry_max_delay: float = 60.0
    retry_backoff: float = 2.0
    retry_max_attempts: int = 5


settings = Settings()
