"""服务入口：装配 FastAPI 应用、遥测中间件与本地观测端点。

不依赖任何外部服务或真实凭据。本地启动::

    uv run uvicorn main:app --reload

环境变量见 :mod:`telemetry.config`。
"""

from __future__ import annotations

from fastapi import APIRouter, FastAPI
from fastapi.responses import JSONResponse

from app.endpoints import router as demo_router
from app.errors import register_exception_handlers
from telemetry.collector import TelemetryCollector
from telemetry.config import TelemetryConfig, config_from_env
from telemetry.middleware import TelemetryMiddleware


def build_telemetry_router(target: TelemetryCollector) -> APIRouter:
    """构造绑定到指定采集器的观测端点路由（生产/测试复用）。"""

    router = APIRouter()

    @router.get("/__telemetry__/report", include_in_schema=False)
    def telemetry_report() -> JSONResponse:
        """返回全量 + 采样两套口径的聚合报表（JSON 可序列化）。"""

        report = target.aggregate()

        def dump(entry_map: dict) -> dict:
            return {
                entry: {
                    "total": agg.total,
                    "by_outcome": agg.by_outcome,
                    "by_failure": agg.by_failure,
                    "slow_count": agg.slow_count,
                    "error_count": agg.error_count,
                    "kept_count": agg.kept_count,
                    "total_duration_ms": round(agg.total_duration_ms, 3),
                    "max_duration_ms": round(agg.max_duration_ms, 3),
                    "avg_duration_ms": round(agg.avg_duration_ms, 3),
                }
                for entry, agg in entry_map.items()
            }

        return JSONResponse(
            {
                "enabled": target.enabled(),
                "sample_rate": report.sample_rate,
                "total_seen": report.total_seen,
                "total_kept": report.total_kept,
                "census": dump(report.census),
                "sampled": dump(report.sampled),
            }
        )

    @router.post("/__telemetry__/enabled/{value}", include_in_schema=False)
    def telemetry_set_enabled(value: bool) -> dict[str, bool]:
        """运行期整体开关采集（true/false）。"""

        if value:
            target.enable()
        else:
            target.disable()
        return {"enabled": target.enabled()}

    @router.get("/__telemetry__/window", include_in_schema=False)
    def telemetry_window() -> JSONResponse:
        """滑动窗口内各入口的请求量/失败/慢请求/p50/p95/p99（全量口径）。"""

        stats = target.window_stats()
        return JSONResponse(
            {
                "enabled": target.enabled(),
                "meta": target.window_meta(),
                "entries": {
                    entry: {
                        "count": s.count,
                        "error_count": s.error_count,
                        "slow_count": s.slow_count,
                        "p50_ms": round(s.p50_ms, 3),
                        "p95_ms": round(s.p95_ms, 3),
                        "p99_ms": round(s.p99_ms, 3),
                    }
                    for entry, s in stats.items()
                },
            }
        )

    @router.get("/__telemetry__/slow", include_in_schema=False)
    def telemetry_slow(
        limit: int | None = None, entry: str | None = None
    ) -> JSONResponse:
        """最慢的一批**已保留**慢请求及其耗时归因（耗时降序）。"""

        pairs = target.slowest_attributions(limit=limit, entry=entry)

        def dump_segments(segments) -> list[dict]:
            return [
                {
                    "name": seg.name,
                    "duration_ms": round(seg.duration_ms, 3),
                    "children": dump_segments(seg.children),
                }
                for seg in segments
            ]

        return JSONResponse(
            {
                "sample_rate": target.config.normalized_sample_rate,
                "detail_capacity": target.config.detail_max_records,
                "detail_evicted": target.detail_evicted,
                "items": [
                    {
                        "trace_id": rec.trace_id,
                        "entry": rec.entry,
                        "path": rec.path,
                        "status_code": rec.status_code,
                        "total_ms": round(rec.duration_ms, 3),
                        "failure_category": rec.failure_category.value,
                        "reason": rec.reason,
                        "attribution": {
                            "queue_ms": round(attr.queue_ms, 3),
                            "business_own_ms": round(attr.business_own_ms, 3),
                            "nested_total_ms": round(attr.nested_total_ms, 3),
                            "response_ms": round(attr.response_ms, 3),
                            "other_ms": round(attr.other_ms, 3),
                            "accounted_ms": round(attr.accounted_ms, 3),
                            "segments_clean": attr.segments_clean,
                            "segments": dump_segments(attr.segments),
                        },
                    }
                    for rec, attr in pairs
                ],
            }
        )

    return router


def build_app(config: TelemetryConfig | None = None) -> FastAPI:
    """用给定配置构建独立应用与采集器（生产入口与测试共用）。"""

    local_collector = TelemetryCollector(config or TelemetryConfig())
    application = FastAPI(title="request-telemetry-python", version="0.1.0")
    # 异常处理器先注册；中间件在 ASGI 最外层负责统一计时与终态判定。
    register_exception_handlers(application)
    application.add_middleware(TelemetryMiddleware, collector=local_collector)
    application.include_router(demo_router)
    application.include_router(build_telemetry_router(local_collector))
    application.state.collector = local_collector  # type: ignore[attr-defined]
    return application


# 进程级单例应用；配置来自环境变量（缺省为全量采集）。
app = build_app(config_from_env())
