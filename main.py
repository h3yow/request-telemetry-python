"""应用入口：装配采集能力，并提供用于本地验证的演示路由。"""
from __future__ import annotations

import asyncio
import os
import time

from fastapi import FastAPI

from telemetry.collector import Collector
from telemetry.config import TelemetryConfig
from telemetry.middleware import BusinessError, TelemetryMiddleware, emit_nested_record


def load_config() -> TelemetryConfig:
    """从环境变量加载配置（均有默认值，不依赖外部服务）。"""
    return TelemetryConfig(
        enabled=os.getenv("TELEMETRY_ENABLED", "true").lower() == "true",
        sample_rate=float(os.getenv("TELEMETRY_SAMPLE_RATE", "1.0")),
        slow_threshold_ms=float(os.getenv("TELEMETRY_SLOW_THRESHOLD_MS", "500")),
    )


config = load_config()
collector = Collector(config)

app = FastAPI(title="request-telemetry-python")
app.add_middleware(TelemetryMiddleware, collector=collector)


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/ok")
def ok():
    """普通成功请求。"""
    return {"status": "ok"}


@app.get("/business-error")
def business_error():
    """业务异常：归类为 BUSINESS，与框架级错误区分。"""
    raise BusinessError("库存不足", status_code=409)


@app.get("/framework-error")
def framework_error():
    """框架级错误：未捕获异常，归类为 FRAMEWORK。"""
    raise RuntimeError("unexpected null pointer in handler")


@app.get("/slow")
async def slow():
    """慢请求：超过阈值，采样开启时仍始终保留。"""
    await asyncio.sleep(config.slow_threshold_ms / 1000.0 + 0.05)
    return {"status": "slow"}


@app.get("/nested")
async def nested():
    """嵌套调用：同一请求内补采多条记录，可按 trace_id 归并。"""
    t0 = time.perf_counter()
    await asyncio.sleep(0.01)
    emit_nested_record(collector, name="nested:db", duration_ms=(time.perf_counter() - t0) * 1000)
    t1 = time.perf_counter()
    await asyncio.sleep(0.01)
    emit_nested_record(collector, name="nested:cache", duration_ms=(time.perf_counter() - t1) * 1000)
    return {"status": "nested"}


@app.get("/disconnect")
async def disconnect(delay: float = 30.0):
    """客户端中途断开演示端点：响应前等待 delay 秒，便于客户端主动断开。

    用 curl 验证：curl -m 1 http://127.0.0.1:8000/disconnect?delay=2
    客户端 1 秒断开，处理结束后该请求归类为 client_disconnect（状态记 499）。
    """
    await asyncio.sleep(delay)
    return {"status": "never"}


@app.get("/telemetry/stats")
def telemetry_stats():
    """聚合快照：按 入口 × 结果 × 失败类别 维度。"""
    snap = collector.snapshot()
    return {
        "enabled": collector.enabled,
        "buckets": [
            {
                "route": key.route,
                "outcome": key.outcome.value,
                "failure_category": key.failure_category.value,
                "count": stats.count,
                "avg_duration_ms": round(stats.avg_duration_ms, 3),
                "max_duration_ms": round(stats.max_duration_ms, 3),
            }
            for key, stats in sorted(snap.items(), key=lambda kv: (kv[0].route, kv[0].outcome.value))
        ],
    }


@app.get("/telemetry/trace/{trace_id}")
def telemetry_trace(trace_id: str):
    """按 trace_id 归并查询同一请求的多条记录。"""
    records = collector.trace_records(trace_id)
    return {
        "trace_id": trace_id,
        "records": [
            {"route": r.route, "outcome": r.outcome.value, "duration_ms": round(r.duration_ms, 3)}
            for r in records
        ],
    }
