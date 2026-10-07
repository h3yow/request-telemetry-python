"""pytest 公共夹具。

- ``collector``：每个用例独立的全新采集器（默认全量采集、低慢请求阈值）；
- ``client``：绑定该采集器的 TestClient，不向测试外抛出 500；
- 所有用例在 caplog 中打印输入与判定依据（见 autouse fixture）。
"""

from __future__ import annotations

import logging

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.endpoints import router as demo_router
from app.errors import register_exception_handlers
from telemetry.collector import TelemetryCollector
from telemetry.config import TelemetryConfig
from telemetry.middleware import TelemetryMiddleware

logger = logging.getLogger("telemetry.tests")


@pytest.fixture
def collector() -> TelemetryCollector:
    # 容量上限在共享夹具中调到足够大，保持原有"无界保留"测试语义；
    # 容量/淘汰边界由专门用例用独立配置覆盖（见 test_capacity.py）。
    coll = TelemetryCollector(
        TelemetryConfig(
            enabled=True,
            sample_rate=1.0,
            slow_threshold_ms=50.0,
            window_seconds=300.0,
            detail_max_records=1_000_000,
        )
    )
    return coll


@pytest.fixture
def app(collector: TelemetryCollector) -> FastAPI:
    # 复用 main.build_app 的装配（含 /__telemetry__/* 观测路由），
    # 但绑定到夹具采集器，便于断言内部状态。
    from main import build_telemetry_router

    application = FastAPI()
    register_exception_handlers(application)
    application.add_middleware(TelemetryMiddleware, collector=collector)
    application.include_router(demo_router)
    application.include_router(build_telemetry_router(collector))
    return application


@pytest.fixture
def client(app: FastAPI) -> TestClient:
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture(autouse=True)
def _log_case_inputs(request: pytest.FixtureRequest, caplog: pytest.LogCaptureFixture):
    """统一在日志中打印用例输入参数与判定依据，满足可解释性要求。"""

    caplog.set_level(logging.DEBUG, logger="telemetry.tests")
    callspec = getattr(request.node, "callspec", None)
    params = (
        ", ".join(f"{k}={v!r}" for k, v in callspec.params.items())
        if callspec is not None
        else "-"
    )
    logger.debug("用例=%s 输入=[%s]", request.node.name, params)
    yield
