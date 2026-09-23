"""服务入口：装配 FastAPI 应用、遥测中间件与本地观测端点。

不依赖任何外部服务或真实凭据。本地启动::

    uv run uvicorn main:app --reload

环境变量见 :mod:`telemetry.config`。
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from app.endpoints import router as demo_router
from app.errors import register_exception_handlers
from telemetry.collector import TelemetryCollector
from telemetry.config import TelemetryConfig, config_from_env
from telemetry.middleware import TelemetryMiddleware

# 进程级单例采集器；配置来自环境变量（缺省为全量采集）。
collector = TelemetryCollector(config_from_env())

app = FastAPI(title="request-telemetry-python", version="0.1.0")
# 异常处理器先注册；中间件在 ASGI 最外层负责统一计时与终态判定。
register_exception_handlers(app)
app.add_middleware(TelemetryMiddleware, collector=collector)
app.include_router(demo_router)


@app.get("/__telemetry__/report", include_in_schema=False)
def telemetry_report() -> JSONResponse:
    """返回全量 + 采样两套口径的聚合报表（JSON 可序列化）。"""

    report = collector.aggregate()

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
            "enabled": collector.enabled(),
            "sample_rate": report.sample_rate,
            "total_seen": report.total_seen,
            "total_kept": report.total_kept,
            "census": dump(report.census),
            "sampled": dump(report.sampled),
        }
    )


@app.post("/__telemetry__/enabled/{value}", include_in_schema=False)
def telemetry_set_enabled(value: bool) -> dict[str, bool]:
    """运行期整体开关采集（true/false）。"""

    if value:
        collector.enable()
    else:
        collector.disable()
    return {"enabled": collector.enabled()}


def build_app(config: TelemetryConfig | None = None) -> FastAPI:
    """测试辅助：用自定义配置构建独立应用与采集器。"""

    local_collector = TelemetryCollector(config or TelemetryConfig())
    application = FastAPI()
    register_exception_handlers(application)
    application.add_middleware(TelemetryMiddleware, collector=local_collector)
    application.include_router(demo_router)
    application.state.collector = local_collector  # type: ignore[attr-defined]
    return application
