"""上报时间戳的时效判定。

现场设备可能补传很久以前的数据，也可能时钟不准把时间戳写到未来。
接入时对已成功归一化（时间戳可解析）的记录做确定性分类：

- ``Freshness.OK``：时间戳落在可接受窗口内。
- ``Freshness.TOO_OLD``：时间戳比当前时间早超过 ``max_age`` 秒。
  这类数据不能代表设备当前状态，也不能据此把设备判成在线；
  ``max_age=None`` 表示不限制（与历史行为完全一致）。
- ``Freshness.FUTURE``：时间戳比当前时间晚超过 ``future_skew`` 秒
  （容忍少量时钟偏差），与“太旧”以及“时间戳无法解析”严格区分；
  ``future_skew=None`` 表示不做未来判定（与历史行为完全一致）。

同一条数据重复上报时分类结论一致（分类只依赖时间戳与配置）。
"""
from __future__ import annotations

import enum


class Freshness(enum.Enum):
    OK = "ok"
    TOO_OLD = "too_old"
    FUTURE = "future"


# IngestOutcome 中使用的小写结果名，便于直接对外暴露。
FRESH = Freshness.OK.value
TOO_OLD = Freshness.TOO_OLD.value
FUTURE = Freshness.FUTURE.value


def classify_freshness(timestamp: float, now: float,
                       max_age: float | None,
                       future_skew: float | None) -> str:
    """按当前时间对时间戳做时效分类，返回 ok/too_old/future。"""
    if max_age is not None and timestamp < now - max_age:
        return TOO_OLD
    if future_skew is not None and timestamp > now + future_skew:
        return FUTURE
    return FRESH
