"""服务入口：装配 FastAPI 应用、遥测中间件与本地观测端点。

不依赖任何外部服务或真实凭据。本地启动::

    uv run uvicorn main:app --reload

环境变量见 :mod:`telemetry.config`。
"""

from __future__ import annotations

from fastapi import FastAPI

from app.endpoints import router as demo_router
from app.errors import register_exception_handlers
from telemetry.collector import TelemetryCollector
from telemetry.config import TelemetryConfig, config_from_env
from telemetry.middleware import install_telemetry
from telemetry.obsapi import register_obs_api

# 进程级单例采集器；配置来自环境变量（缺省为全量采集）。
collector = TelemetryCollector(config_from_env())

app = FastAPI(title="request-telemetry-python", version="0.1.0")
# 异常处理器先注册；中间件在 ASGI 最外层负责统一计时与终态判定。
register_exception_handlers(app)
app.include_router(demo_router)
# 同时安装业务开始标记内层与采集外层中间件（顺序在函数内保证）。
install_telemetry(app, collector)
# 本地观测/控制端点（report / window / slowest / enabled）。
register_obs_api(app, collector)


def build_app(config: TelemetryConfig | None = None) -> FastAPI:
    """测试辅助：用自定义配置构建独立应用与采集器。"""

    from telemetry.middleware import build_telemetry_app

    local_collector = TelemetryCollector(config or TelemetryConfig())
    application = FastAPI()
    build_telemetry_app(application, local_collector)
    register_obs_api(application, local_collector)
    return application
