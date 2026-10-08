"""pytest 公共配置：按业务模块输出覆盖情况与每条用例结果。"""
from __future__ import annotations

BUSINESS_NAMES = {
    "test_normalizer": "数据归一化与可区分拒绝原因",
    "test_store": "状态存储（幂等/乱序/容量淘汰/设备隔离/并发）",
    "test_status": "在线状态判定（正常/迟滞/离线）",
    "test_freshness": "时间戳时效判定（太旧/未来/容忍窗口）",
    "test_freshness_store": "时效接入行为（留痕/不带偏状态/重复结论）",
    "test_retry": "失败暂存与指数退避重试",
    "test_retry_deadletter": "退避上限、最终放弃集合与手动重新投递",
    "test_worker": "后台自动补偿（自动重试/放弃/不阻塞/容错）",
    "test_metrics": "接入质量汇总（分类计数/并发准确性）",
    "test_api": "端到端接入、查询与重试接口",
    "test_api_quality": "端到端时效响应、质量视图与 dead 重投",
    "test_lifespan_worker": "应用生命周期下的后台自动补偿集成",
}

_config = None
_seen: set[str] = set()


def pytest_configure(config):
    global _config
    _config = config


def _write(line: str, sep: bool = False) -> None:
    tr = _config.pluginmanager.get_plugin("terminalreporter") if _config else None
    if tr is None:
        return
    if sep and hasattr(tr, "write_sep"):
        tr.write_sep("=", line)
    else:
        tr.line(line)


def pytest_runtest_logstart(nodeid):
    module = nodeid.split("::")[0]
    stem = module.rsplit("/", 1)[-1].removesuffix(".py")
    if module not in _seen:
        _seen.add(module)
        _write(f"业务覆盖: {BUSINESS_NAMES.get(stem, stem)}", sep=True)


def pytest_runtest_logreport(report):
    if report.when != "call":
        return
    label = {"passed": "通过", "failed": "失败", "skipped": "跳过"}.get(
        report.outcome, report.outcome)
    case = report.nodeid.split("::", 1)[-1]
    _write(f"  [{label}] {case}")
