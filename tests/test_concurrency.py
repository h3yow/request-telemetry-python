"""并发交错测试：计数不丢失/不重复/不错配，同一请求记录可归并到同一追踪上下文。"""
from __future__ import annotations

import asyncio
import logging

import httpx
import pytest
from starlette.testclient import TestClient

from conftest import make_app
from telemetry.config import TelemetryConfig
from telemetry.models import FailureCategory, Outcome

logger = logging.getLogger("tests.concurrency")

N_REQUESTS = 50


def _run(coro):
    return asyncio.run(coro)


def test_concurrent_requests_counts_exact():
    """并发交错请求 /ok 与 /nested：总计数精确，无丢失无重复。"""
    app, collector = make_app(TelemetryConfig(enabled=True, sample_rate=1.0))

    async def main():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
            tasks = []
            for i in range(N_REQUESTS):
                path = "/nested" if i % 2 else "/ok"
                tasks.append(client.get(path, headers={"x-trace-id": f"trace-{i}"}))
            responses = await asyncio.gather(*tasks)
            assert all(r.status_code == 200 for r in responses)

    _run(main())
    total = sum(s.count for s in collector.snapshot().values())
    # /nested 每请求产生 2 条记录（中间件 + 嵌套调用），/ok 产生 1 条
    expected = (N_REQUESTS // 2) * 1 + (N_REQUESTS // 2) * 2
    logger.info(
        "输入: %d 个并发请求（一半 /ok 一半 /nested）| 判定依据: /ok=1条 /nested=2条 | "
        "期望=%d 实际=%d",
        N_REQUESTS, expected, total,
    )
    assert total == expected


def test_concurrent_traces_no_cross_contamination():
    """并发请求各自携带独立 trace_id：归并结果不得错配。"""
    app, collector = make_app(TelemetryConfig(enabled=True, sample_rate=1.0))

    async def main():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
            await asyncio.gather(*[
                client.get("/nested", headers={"x-trace-id": f"tid-{i}"}) for i in range(20)
            ])

    _run(main())
    for i in range(20):
        records = collector.trace_records(f"tid-{i}")
        routes = sorted(r.route for r in records)
        logger.info("trace_id=tid-%d 归并记录路由=%s", i, routes)
        # 同一请求的两条记录归并到同一追踪上下文，且全部属于该 trace_id
        assert routes == ["/nested", "nested:db"], routes
        assert all(r.trace_id == f"tid-{i}" for r in records)


def test_concurrent_mixed_outcomes_buckets_correct():
    """并发混合成功/业务失败/框架失败：各维度桶计数精确。"""
    app, collector = make_app(TelemetryConfig(enabled=True, sample_rate=1.0))

    async def main():
        transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
            paths = ["/ok", "/business-error", "/framework-error"] * 10
            await asyncio.gather(*[client.get(p) for p in paths])

    _run(main())
    snap = collector.snapshot()
    counts = {(k.route, k.failure_category): v.count for k, v in snap.items()}
    logger.info("输入: 3 类入口各 10 次并发 | 判定依据: 按 入口×类别 分桶 | 实际: %s", counts)
    assert counts[("/ok", FailureCategory.NONE)] == 10
    assert counts[("/business-error", FailureCategory.BUSINESS)] == 10
    assert counts[("/framework-error", FailureCategory.FRAMEWORK)] == 10
