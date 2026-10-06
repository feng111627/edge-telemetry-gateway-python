"""pytest 公共夹具与业务化日志输出。

跑测试时：
- 每条用例开始/结束都有 INFO 日志（``pytest -s`` 可实时查看）；
- 结束后终端输出“业务覆盖汇总”，按业务分组列出用例与通过/失败结果。
"""

from __future__ import annotations

import logging

import pytest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s | %(message)s",
)
logger = logging.getLogger("telemetry-tests")

# nodeid -> (业务分组, 用例名)
_cases: dict[str, tuple[str, str]] = {}
_results: dict[str, str] = {}


def pytest_runtest_logstart(nodeid: str, location) -> None:
    logger.info("用例开始: %s", nodeid)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    if report.when == "call":
        business = {"normalizer", "store", "retry_queue", "service", "api"}
        marker = next((m.name for m in item.iter_markers() if m.name in business), "未分组")
        name = item.name
        _cases[item.nodeid] = (marker, name)
        if report.passed:
            _results[item.nodeid] = "passed"
            logger.info("[%s] 用例通过: %s", marker, item.nodeid)
        elif report.failed:
            _results[item.nodeid] = "failed"
            logger.info("[%s] 用例失败: %s", marker, item.nodeid)
        elif report.skipped:
            _results[item.nodeid] = "skipped"


def pytest_terminal_summary(terminalreporter, exitstatus, config) -> None:
    groups: dict[str, list[tuple[str, str]]] = {}
    for nodeid, (marker, name) in _cases.items():
        groups.setdefault(marker, []).append((name, _results.get(nodeid, "?")))

    terminalreporter.write_sep("=", "业务覆盖汇总（按业务分组的用例与结果）")
    total = passed = failed = 0
    for marker in sorted(groups):
        rows = sorted(groups[marker])
        ok = sum(1 for _, r in rows if r == "passed")
        bad = sum(1 for _, r in rows if r == "failed")
        total += len(rows)
        passed += ok
        failed += bad
        terminalreporter.write_line(f"[{marker}] 用例 {len(rows)} 条，通过 {ok}，失败 {bad}")
        for name, result in rows:
            icon = {"passed": "✓", "failed": "✗", "skipped": "⊘"}.get(result, "?")
            terminalreporter.write_line(f"    {icon} {name} [{result}]")
    terminalreporter.write_sep("-", f"合计 {total} 条：通过 {passed}，失败 {failed}")
