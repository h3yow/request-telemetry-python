"""测试公共装置：按配置构建隔离的应用与采集器。"""
from __future__ import annotations

import logging

import pytest
from fastapi import FastAPI

from telemetry.collector import Collector
from telemetry.config import TelemetryConfig
from telemetry.middleware import BusinessError, TelemetryMiddleware, emit_nested_record
from telemetry.models import Outcome

logger = logging.getLogger("tests")


def make_app(config: TelemetryConfig) -> tuple[FastAPI, Collector]:
    """构建一个带采集能力的独立应用（不依赖 main.py 的全局状态）。"""
    collector = Collector(config)
    app = FastAPI()
    app.add_middleware(TelemetryMiddleware, collector=collector)

    @app.get("/ok")
    def ok():
        return {"status": "ok"}

    @app.get("/business-error")
    def business_error():
        raise BusinessError("库存不足", status_code=409)

    @app.get("/framework-error")
    def framework_error():
        raise RuntimeError("unexpected failure")

    @app.get("/slow")
    async def slow():
        import asyncio

        await asyncio.sleep(config.slow_threshold_ms / 1000.0 + 0.02)
        return {"status": "slow"}

    @app.get("/nested")
    async def nested():
        import asyncio
        import time

        t0 = time.perf_counter()
        await asyncio.sleep(0.001)
        emit_nested_record(collector, name="nested:db", duration_ms=(time.perf_counter() - t0) * 1000)
        return {"status": "nested"}

    return app, collector


@pytest.fixture
def full_sampling() -> tuple[FastAPI, Collector]:
    return make_app(TelemetryConfig(enabled=True, sample_rate=1.0, slow_threshold_ms=100.0))
