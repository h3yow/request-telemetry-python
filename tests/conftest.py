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
from telemetry.middleware import install_telemetry
from telemetry.obsapi import register_obs_api

logger = logging.getLogger("telemetry.tests")


@pytest.fixture
def collector() -> TelemetryCollector:
    coll = TelemetryCollector(
        TelemetryConfig(
            enabled=True,
            sample_rate=1.0,
            slow_threshold_ms=50.0,
            # 旧用例不关心淘汰，夹具显式给足容量，保持原"全量明细"语义；
            # 淘汰边界由 test_retention.py 用独立小容量配置专门覆盖。
            max_records=1_000_000,
        )
    )
    return coll


@pytest.fixture
def app(collector: TelemetryCollector) -> FastAPI:
    application = FastAPI()
    register_exception_handlers(application)
    application.include_router(demo_router)
    # 与生产装配一致：业务开始标记内层 + 遥测采集外层。
    install_telemetry(application, collector)
    register_obs_api(application, collector)
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
