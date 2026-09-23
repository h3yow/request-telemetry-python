"""失败原因分类测试：业务异常与框架级错误不得混为一类。"""
from __future__ import annotations

import logging

from starlette.testclient import TestClient

from telemetry.models import FailureCategory, Outcome

from conftest import make_app
from telemetry.config import TelemetryConfig

logger = logging.getLogger("tests.classification")


def _only_bucket(collector, route):
    snap = collector.snapshot()
    matches = [(k, v) for k, v in snap.items() if k.route == route]
    assert len(matches) == 1, f"期望 {route} 只有一个聚合桶，实际: {matches}"
    return matches[0]


def test_success_classified_as_success():
    app, collector = make_app(TelemetryConfig(enabled=True, sample_rate=1.0))
    client = TestClient(app)
    resp = client.get("/ok")
    (key, stats) = _only_bucket(collector, "/ok")
    logger.info(
        "输入: GET /ok -> status=%d | 判定依据: 2xx 无异常 -> outcome=SUCCESS, category=NONE",
        resp.status_code,
    )
    assert resp.status_code == 200
    assert key.outcome is Outcome.SUCCESS
    assert key.failure_category is FailureCategory.NONE
    assert stats.count == 1


def test_business_error_not_mixed_with_framework():
    app, collector = make_app(TelemetryConfig(enabled=True, sample_rate=1.0))
    client = TestClient(app, raise_server_exceptions=False)

    resp_b = client.get("/business-error")
    (key_b, stats_b) = _only_bucket(collector, "/business-error")
    logger.info(
        "输入: GET /business-error (raise BusinessError) -> status=%d | "
        "判定依据: BusinessError 被中间件捕获 -> category=BUSINESS",
        resp_b.status_code,
    )
    assert resp_b.status_code == 409
    assert key_b.outcome is Outcome.FAILURE
    assert key_b.failure_category is FailureCategory.BUSINESS

    resp_f = client.get("/framework-error")
    (key_f, stats_f) = _only_bucket(collector, "/framework-error")
    logger.info(
        "输入: GET /framework-error (raise RuntimeError) -> status=%d | "
        "判定依据: 未捕获异常 -> category=FRAMEWORK",
        resp_f.status_code,
    )
    assert resp_f.status_code == 500
    assert key_f.outcome is Outcome.FAILURE
    assert key_f.failure_category is FailureCategory.FRAMEWORK

    # 两类失败严格落在不同的桶，不混为一类
    assert key_b.failure_category != key_f.failure_category
    assert stats_b.count == 1 and stats_f.count == 1


def test_4xx_is_business_5xx_is_framework():
    from fastapi import FastAPI
    from fastapi.responses import JSONResponse

    from telemetry.collector import Collector
    from telemetry.middleware import TelemetryMiddleware

    collector = Collector(TelemetryConfig(enabled=True, sample_rate=1.0))
    app = FastAPI()
    app.add_middleware(TelemetryMiddleware, collector=collector)

    @app.get("/not-found-ish")
    def not_found():
        return JSONResponse({"detail": "bad input"}, status_code=422)

    @app.get("/server-ish")
    def server_ish():
        return JSONResponse({"detail": "upstream broken"}, status_code=503)

    client = TestClient(app)
    client.get("/not-found-ish")
    client.get("/server-ish")
    snap = collector.snapshot()
    by_route = {k.route: k for k in snap}
    logger.info(
        "输入: 422 与 503 响应 | 判定依据: 4xx->BUSINESS, 5xx->FRAMEWORK | 实际: %s",
        {r: k.failure_category.value for r, k in by_route.items()},
    )
    assert by_route["/not-found-ish"].failure_category is FailureCategory.BUSINESS
    assert by_route["/server-ish"].failure_category is FailureCategory.FRAMEWORK
