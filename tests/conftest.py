"""pytest 公共夹具与业务化日志输出。"""

from __future__ import annotations

import logging

import pytest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s | %(message)s",
)
logger = logging.getLogger("telemetry-tests")


def pytest_runtest_logstart(nodeid: str, location) -> None:
    logger.info("用例开始: %s", nodeid)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    if report.when == "call":
        marker = next((m.name for m in item.iter_markers()), "未分组")
        if report.passed:
            logger.info("[%s] 用例通过: %s", marker, item.nodeid)
        elif report.failed:
            logger.info("[%s] 用例失败: %s", marker, item.nodeid)
