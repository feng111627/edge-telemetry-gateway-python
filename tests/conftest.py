"""pytest 公共配置：按业务模块输出覆盖情况与每条用例结果。"""
from __future__ import annotations

BUSINESS_NAMES = {
    "test_normalizer": "数据归一化与可区分拒绝原因",
    "test_store": "状态存储（幂等/乱序/容量淘汰/设备隔离/并发）",
    "test_status": "在线状态判定（正常/迟滞/离线）",
    "test_retry": "失败暂存与指数退避重试",
    "test_api": "端到端接入、查询与重试接口",
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
