"""中间件端到端测试：终态分类、span 归并、慢请求、开关。"""

from __future__ import annotations

import logging

from fastapi.testclient import TestClient

from telemetry.classification import FailureCategory, Outcome
from telemetry.collector import TelemetryCollector
from telemetry.config import TelemetryConfig

logger = logging.getLogger("telemetry.tests")


def _by_path(collector: TelemetryCollector, path: str):
    recs = [r for r in collector.snapshot() if r.path == path]
    assert len(recs) == 1, f"{path} 应恰好一条记录，实际 {len(recs)}"
    return recs[0]


def test_success_path(client: TestClient, collector: TelemetryCollector) -> None:
    resp = client.get("/")
    logger.debug("输入 GET / -> status=%s body=%s", resp.status_code, resp.json())
    assert resp.status_code == 200
    rec = _by_path(collector, "/")
    logger.debug("判定 outcome=%s category=%s duration=%.3fms",
                 rec.outcome.value, rec.failure_category.value, rec.duration_ms)
    assert rec.outcome is Outcome.SUCCESS
    assert rec.failure_category is FailureCategory.NONE
    assert rec.status_code == 200
    assert rec.trace_id.startswith("trace-")


def test_business_error_distinct_from_framework(
    client: TestClient, collector: TelemetryCollector
) -> None:
    rb = client.get("/business-error")
    rf = client.get("/framework-error")
    logger.debug("输入 /business-error -> %s; /framework-error -> %s",
                 rb.status_code, rf.status_code)
    biz = _by_path(collector, "/business-error")
    fwk = _by_path(collector, "/framework-error")
    logger.debug("判定 business: outcome=%s category=%s reason=%s",
                 biz.outcome.value, biz.failure_category.value, biz.reason)
    logger.debug("判定 framework: outcome=%s category=%s reason=%s",
                 fwk.outcome.value, fwk.failure_category.value, fwk.reason)
    assert (rb.status_code, rf.status_code) == (422, 500)
    assert biz.outcome is Outcome.BUSINESS_ERROR
    assert biz.failure_category is FailureCategory.BUSINESS
    assert fwk.outcome is Outcome.FRAMEWORK_ERROR
    assert fwk.failure_category is FailureCategory.FRAMEWORK
    assert biz.reason != fwk.reason


def test_slow_request_flagged_and_kept(
    client: TestClient, collector: TelemetryCollector
) -> None:
    collector.update_config(TelemetryConfig(sample_rate=0.0))
    resp = client.get("/slow?ms=550")
    logger.debug("输入 /slow?ms=550 (阈值500ms) -> status=%s", resp.status_code)
    rec = _by_path(collector, "/slow")
    logger.debug("判定 slow=%s duration=%.2fms kept=True(慢请求强制保留)",
                 rec.slow, rec.duration_ms)
    assert rec.slow is True
    assert rec.duration_ms >= 500.0


def test_non_slow_request_not_flagged(
    client: TestClient, collector: TelemetryCollector
) -> None:
    resp = client.get("/slow?ms=10")
    logger.debug("输入 /slow?ms=10 (阈值500ms) -> status=%s", resp.status_code)
    rec = _by_path(collector, "/slow")
    logger.debug("判定 slow=%s duration=%.2fms", rec.slow, rec.duration_ms)
    assert rec.slow is False
    assert rec.duration_ms < 500.0


def test_nested_spans_merge_to_same_trace(
    client: TestClient, collector: TelemetryCollector
) -> None:
    resp = client.get("/nested")
    logger.debug("输入 /nested -> status=%s", resp.status_code)
    rec = _by_path(collector, "/nested")
    assert len(rec.spans) == 2
    by_id = {s.span_id: s for s in rec.spans}
    for span in rec.spans:
        logger.debug("span %s trace=%s parent=%s outcome=%s",
                     span.name, span.trace_id, span.parent_span_id,
                     span.outcome.value)
        assert span.trace_id == rec.trace_id  # 归并到请求 trace
    roots = [s for s in rec.spans if s.parent_span_id is None]
    leaves = [s for s in rec.spans if s.parent_span_id is not None]
    assert len(roots) == 1 and len(leaves) == 1
    assert by_id[leaves[0].span_id].parent_span_id == roots[0].span_id
    # 创建顺序外=先、内=后，可核对嵌套结构。
    assert roots[0].name == "outer-call"


def test_nested_error_spans_classified(
    client: TestClient, collector: TelemetryCollector
) -> None:
    resp = client.get("/nested-error")
    logger.debug("输入 /nested-error -> status=%s", resp.status_code)
    rec = _by_path(collector, "/nested-error")
    assert resp.status_code == 400
    assert rec.outcome is Outcome.BUSINESS_ERROR
    assert len(rec.spans) == 2
    for span in rec.spans:
        logger.debug("span %s category=%s reason=%s",
                     span.name, span.failure_category.value, span.reason)
        assert span.failure_category is FailureCategory.BUSINESS


def test_middleware_disabled_passthrough(
    client: TestClient, collector: TelemetryCollector
) -> None:
    collector.disable()
    resp = client.get("/")
    logger.debug("采集关闭后输入 / -> status=%s, records=%s",
                 resp.status_code, len(collector.snapshot()))
    assert resp.status_code == 200
    assert collector.aggregate().total_seen == 0


def test_http_404_fallback_classified_as_business(
    client: TestClient, collector: TelemetryCollector
) -> None:
    """未匹配路由的 404 走 FastAPI 默认处理器，中间件须按状态码兜底为业务类。"""

    resp = client.get("/no-such-path")
    logger.debug("输入 GET /no-such-path -> status=%s", resp.status_code)
    assert resp.status_code == 404
    rec = [r for r in collector.snapshot() if r.path == "/no-such-path"][0]
    logger.debug("判定 outcome=%s category=%s reason=%s",
                 rec.outcome.value, rec.failure_category.value, rec.reason)
    assert rec.outcome is Outcome.BUSINESS_ERROR
    assert rec.failure_category is FailureCategory.BUSINESS


def test_report_endpoint_not_collected(
    client: TestClient, collector: TelemetryCollector
) -> None:
    """内部观测端点不得污染业务采集口径。"""

    client.get("/")
    client.get("/__telemetry__/report")
    paths = {r.path for r in collector.snapshot()}
    logger.debug("访问 / 与 /__telemetry__/report 后采集到的路径=%s", paths)
    assert paths == {"/"}
