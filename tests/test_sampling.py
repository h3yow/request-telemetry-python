"""采样与开关测试：错误/慢请求始终保留，开关采样前后聚合口径自洽。"""
from __future__ import annotations

import logging

from starlette.testclient import TestClient

from conftest import make_app
from telemetry.config import TelemetryConfig
from telemetry.models import FailureCategory, Outcome

logger = logging.getLogger("tests.sampling")


def _drive(client: TestClient, n_ok: int, n_biz: int, n_framework: int, n_slow: int) -> None:
    for _ in range(n_ok):
        client.get("/ok")
    for _ in range(n_biz):
        client.get("/business-error")
    for _ in range(n_framework):
        client.get("/framework-error")
    for _ in range(n_slow):
        client.get("/slow")


def _bucket_counts(collector) -> dict:
    return {(k.route, k.failure_category): v.count for k, v in collector.snapshot().items()}


def test_sampling_toggle_consistency():
    """采样开关前后：错误与慢请求计数完全一致，仅普通成功请求计数随采样率变化。"""
    inputs = dict(n_ok=40, n_biz=6, n_framework=4, n_slow=3)
    logger.info("输入: %s", inputs)

    # 场景 A：关闭采样（全量保留）
    app_a, col_a = make_app(TelemetryConfig(enabled=True, sample_rate=1.0, slow_threshold_ms=50.0))
    _drive(TestClient(app_a, raise_server_exceptions=False), **inputs)
    counts_a = _bucket_counts(col_a)

    # 场景 B：开启采样（普通请求采样率 0）
    app_b, col_b = make_app(TelemetryConfig(enabled=True, sample_rate=0.0, slow_threshold_ms=50.0))
    _drive(TestClient(app_b, raise_server_exceptions=False), **inputs)
    counts_b = _bucket_counts(col_b)

    logger.info("判定依据: 错误与慢请求始终保留 -> 两场景相应桶计数必须相等")
    logger.info("全量计数: %s", counts_a)
    logger.info("采样计数: %s", counts_b)

    # 错误维度自洽：采样前后完全一致
    assert counts_b[("/business-error", FailureCategory.BUSINESS)] == inputs["n_biz"]
    assert counts_b[("/framework-error", FailureCategory.FRAMEWORK)] == inputs["n_framework"]
    assert counts_a[("/business-error", FailureCategory.BUSINESS)] == counts_b[("/business-error", FailureCategory.BUSINESS)]
    assert counts_a[("/framework-error", FailureCategory.FRAMEWORK)] == counts_b[("/framework-error", FailureCategory.FRAMEWORK)]
    # 慢请求维度自洽：即使采样率为 0 也全部保留
    assert counts_b[("/slow", FailureCategory.NONE)] == inputs["n_slow"]
    # 普通成功请求：全量场景计数 = 输入；采样场景被丢弃
    assert counts_a[("/ok", FailureCategory.NONE)] == inputs["n_ok"]
    assert ("/ok", FailureCategory.NONE) not in counts_b


def test_disabled_collector_zero_overhead_bypass():
    """总开关关闭：不采集任何记录，请求正常响应。"""
    app, collector = make_app(TelemetryConfig(enabled=False, sample_rate=1.0))
    client = TestClient(app, raise_server_exceptions=False)
    for path in ("/ok", "/business-error", "/framework-error", "/slow"):
        resp = client.get(path)
        logger.info("输入: GET %s (enabled=False) -> status=%d", path, resp.status_code)
    logger.info("判定依据: enabled=False 时 record() 立即旁路 -> 快照为空 | 实际: %s",
                collector.snapshot())
    assert collector.snapshot() == {}


def test_partial_sample_rate_keeps_all_errors():
    """部分采样率下错误与慢请求仍 100% 保留。"""
    app, collector = make_app(TelemetryConfig(enabled=True, sample_rate=0.1, slow_threshold_ms=50.0))
    client = TestClient(app, raise_server_exceptions=False)
    _drive(client, n_ok=30, n_biz=5, n_framework=5, n_slow=5)
    counts = _bucket_counts(collector)
    logger.info("输入: sample_rate=0.1, 30 ok + 5 biz + 5 framework + 5 slow | 实际: %s", counts)
    assert counts[("/business-error", FailureCategory.BUSINESS)] == 5
    assert counts[("/framework-error", FailureCategory.FRAMEWORK)] == 5
    assert counts[("/slow", FailureCategory.NONE)] == 5
    # 普通成功请求按概率保留，必然不超过总量
    assert counts.get(("/ok", FailureCategory.NONE), 0) <= 30
