"""客户端中断与异常链路测试：失败原因可区分，单次异常不影响整体采集。"""
from __future__ import annotations

import asyncio
import logging

import pytest
from starlette.requests import ClientDisconnect, Request
from starlette.responses import JSONResponse

from conftest import make_app
from telemetry.collector import Collector
from telemetry.config import TelemetryConfig
from telemetry.middleware import TelemetryMiddleware, emit_nested_record
from telemetry.models import FailureCategory, Outcome, RequestRecord

logger = logging.getLogger("tests.disconnect")


def _make_request(path: str = "/disconnect", receive=None) -> Request:
    async def default_receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "headers": [],
            "query_string": b"",
        },
        receive or default_receive,
    )


def test_client_disconnect_classified_and_reraised():
    """客户端中途断开：归类为 CLIENT_DISCONNECT，异常继续上抛，且仍被采集。"""
    app, collector = make_app(TelemetryConfig(enabled=True, sample_rate=1.0))
    middleware = TelemetryMiddleware(app, collector)

    async def call_next(request):
        raise ClientDisconnect()

    async def main():
        with pytest.raises(ClientDisconnect):
            await middleware.dispatch(_make_request(), call_next)

    asyncio.run(main())
    snap = collector.snapshot()
    assert len(snap) == 1
    (key, stats), = snap.items()
    logger.info(
        "输入: call_next 抛出 ClientDisconnect | 判定依据: 中间件捕获后归类并重抛 | "
        "实际: outcome=%s category=%s",
        key.outcome.value, key.failure_category.value,
    )
    assert key.outcome is Outcome.FAILURE
    assert key.failure_category is FailureCategory.CLIENT_DISCONNECT
    assert stats.count == 1


def test_disconnect_detected_after_handler_returns():
    """处理期间客户端断开（receive 通道就绪 http.disconnect）：
    响应照常生成，但记录归类为 CLIENT_DISCONNECT（状态记 499）。"""
    app, collector = make_app(TelemetryConfig(enabled=True, sample_rate=1.0))
    middleware = TelemetryMiddleware(app, collector)

    async def disconnect_receive():
        return {"type": "http.disconnect"}

    async def call_next(request):
        return JSONResponse({"ok": True})

    async def main():
        resp = await middleware.dispatch(_make_request("/ok", disconnect_receive), call_next)
        assert resp.status_code == 200  # 响应对象仍正常生成

    asyncio.run(main())
    (key, stats), = collector.snapshot().items()
    logger.info(
        "输入: 处理完成后 receive 通道已断开 | 判定依据: is_disconnected()=True -> "
        "CLIENT_DISCONNECT | 实际: category=%s", key.failure_category.value,
    )
    assert key.failure_category is FailureCategory.CLIENT_DISCONNECT
    assert key.outcome is Outcome.FAILURE


def test_disconnect_distinct_from_business_and_framework():
    """断开、业务异常、框架错误三类失败原因必须可区分。"""
    app, collector = make_app(TelemetryConfig(enabled=True, sample_rate=1.0))
    middleware = TelemetryMiddleware(app, collector)

    async def disconnect_next(request):
        raise ClientDisconnect()

    async def framework_next(request):
        raise RuntimeError("boom")

    async def business_next(request):
        from telemetry.middleware import BusinessError

        raise BusinessError("余额不足", status_code=409)

    async def main():
        for fn in (disconnect_next, framework_next):
            with pytest.raises(Exception):
                await middleware.dispatch(_make_request(), fn)
        # BusinessError 由中间件转为响应，不上抛
        resp = await middleware.dispatch(_make_request(), business_next)
        assert resp.status_code == 409

    asyncio.run(main())
    categories = {k.failure_category for k in collector.snapshot()}
    logger.info("输入: 依次抛出 ClientDisconnect/RuntimeError/BusinessError | 实际类别: %s",
                sorted(c.value for c in categories))
    assert categories == {
        FailureCategory.CLIENT_DISCONNECT,
        FailureCategory.FRAMEWORK,
        FailureCategory.BUSINESS,
    }


def test_collection_failure_does_not_break_request():
    """采集器自身抛异常时，请求照常返回，整体采集不受影响。"""
    app, collector = make_app(TelemetryConfig(enabled=True, sample_rate=1.0))

    class BrokenCollector(Collector):
        def record(self, record):  # type: ignore[override]
            raise RuntimeError("storage on fire")

    broken = BrokenCollector(TelemetryConfig(enabled=True, sample_rate=1.0))
    middleware = TelemetryMiddleware(app, broken)

    async def call_next(request):
        return JSONResponse({"ok": True})

    async def main():
        resp = await middleware.dispatch(_make_request("/ok"), call_next)
        logger.info("输入: 采集器 record() 抛异常 | 判定依据: 中间件吞掉采集异常 | 响应 status=%d",
                    resp.status_code)
        assert resp.status_code == 200

    asyncio.run(main())


def test_missing_context_behavior_stable():
    """上下文缺失时 emit_nested_record 不抛异常，自动生成 trace_id。"""
    collector = Collector(TelemetryConfig(enabled=True, sample_rate=1.0))
    kept = emit_nested_record(collector, name="orphan-op", duration_ms=1.0)
    snap = collector.snapshot()
    logger.info("输入: 无请求上下文直接补采 | 判定依据: ensure_trace_id 生成新 id | kept=%s", kept)
    assert kept is True
    assert sum(s.count for s in snap.values()) == 1
