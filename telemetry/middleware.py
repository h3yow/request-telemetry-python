"""ASGI 遥测中间件：在请求边界统一采集。

职责
----
1. 为每个 HTTP 请求建立唯一 :class:`~telemetry.context.TraceContext`，
   使得请求处理过程中产生的所有 span 归并到同一 ``trace_id``；
2. 用 :func:`time.perf_counter` 测量端到端耗时（毫秒），并记录业务开始
   边界，供慢请求三段归因（等待/业务/嵌套）；
3. 确定终态，优先级：

   - 下游协程被取消 / receive 收到 ``http.disconnect``
     → ``client_disconnect``（客户端中途断开）；
   - 抛出业务异常（或路由处理器在 ``scope['state']`` 中显式标注）
     → ``business_error``；
   - 其他未处理异常 → ``framework_error``；
   - 无异常时按最终响应状态码兜底分类（4xx 业务 / 5xx 框架）；
4. 依据阈值判定慢请求，并调用 :class:`TelemetryCollector` 原子登记；
5. 采集总开关关闭时直接透传，零记录、近乎零开销；
6. 采集自身的任何异常都被隔离，绝不改变业务响应。
"""

from __future__ import annotations

import asyncio
import time

from fastapi import FastAPI
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from .classification import (
    FailureCategory,
    Outcome,
    classify_exception,
    classify_status,
)
from .collector import TelemetryCollector
from .config import TelemetryConfig
from .context import begin_trace, mark_business_start, reset_trace
from .recorder import RequestRecord

# 业务异常处理器可在 scope['state'] 中写入该键，显式传递终态信息。
STATE_OUTCOME_KEY = "telemetry_outcome"
STATE_CATEGORY_KEY = "telemetry_failure_category"
STATE_REASON_KEY = "telemetry_reason"
STATE_STATUS_KEY = "telemetry_status_code"

# 内部观测/控制端点不纳入业务采集口径，避免自污染。
EXCLUDED_PATH_PREFIX = "/__telemetry__/"


class BusinessStartMiddleware:
    """标记业务处理真正开始边界的内层 ASGI 中间件。

    调用链顺序（外 -> 内）：:class:`TelemetryMiddleware`（建 trace/计时）
    -> 本中间件 -> 路由/业务。进入本层即视为框架排队、接收请求体等前置
    工作完成、即将进入业务，在当前 trace 上打一个一次性时间戳。
    相比用第一次 receive，该边界不依赖服务器何时拉取事件（uvicorn 对
    无 body 的 GET 可能根本不调 receive），口径稳定。
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            mark_business_start()
        await self.app(scope, receive, send)


def install_telemetry(app: FastAPI, collector: TelemetryCollector) -> FastAPI:
    """在 FastAPI 应用上安装业务边界标记与遥测中间件。

    Starlette 按后 add 更靠外的顺序包裹，因此先 add 内层标记、
    再 add 外层 :class:`TelemetryMiddleware`，最终顺序为
    Telemetry -> BusinessStart -> 路由。
    """

    app.add_middleware(BusinessStartMiddleware)
    app.add_middleware(TelemetryMiddleware, collector=collector)
    app.state.collector = collector  # type: ignore[attr-defined]
    return app


def build_telemetry_app(
    app: FastAPI,
    collector: TelemetryCollector,
    include_routers: bool = True,
) -> FastAPI:
    """测试/装配辅助：异常处理器 + 演示路由 + 遥测一次性装齐。"""

    from app.endpoints import router as demo_router
    from app.errors import register_exception_handlers

    register_exception_handlers(app)
    if include_routers:
        app.include_router(demo_router)
    install_telemetry(app, collector)
    return app


class TelemetryMiddleware:
    def __init__(
        self,
        app: ASGIApp,
        collector: TelemetryCollector | None = None,
        config: TelemetryConfig | None = None,
    ) -> None:
        self.app = app
        self.collector = collector or TelemetryCollector(config)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        if not self.collector.enabled() or scope.get("path", "").startswith(
            EXCLUDED_PATH_PREFIX
        ):
            await self.app(scope, receive, send)
            return

        method = scope.get("method", "")
        raw_path = scope.get("path", "")
        # 入口维度：METHOD + 路径（不含 query string，避免高基数）。
        entry = f"http:{method} {raw_path}"

        ctx, token = begin_trace(entry)
        trace_id = ctx.trace_id

        status_code = 0
        disconnected = False

        async def receive_wrapper() -> Message:
            nonlocal disconnected
            message = await receive()
            # 记录客户端断开事实；事件仍原样透传给下游，不改变业务语义。
            if message.get("type") == "http.disconnect":
                disconnected = True
            return message

        async def send_wrapper(message: Message) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = int(message.get("status", 0))
            await send(message)

        started_perf = time.perf_counter()
        started_ms = started_perf * 1000.0
        outcome: Outcome | None = None
        category: FailureCategory | None = None
        reason = ""
        cancelled = False

        try:
            await self.app(scope, receive_wrapper, send_wrapper)
        except asyncio.CancelledError:
            cancelled = True
            raise
        except BaseException as exc:  # noqa: BLE001 - 边界处分类后必须重新抛出
            outcome, category, reason = classify_exception(exc)
            raise
        finally:
            ended_perf = time.perf_counter()
            duration_ms = (ended_perf - started_perf) * 1000.0

            state = scope.get("state", {})
            state_status = state.get(STATE_STATUS_KEY)
            if isinstance(state_status, int):
                status_code = state_status

            if cancelled or disconnected:
                # 客户端中途断开：确定结论，不论下游是否已部分写响应。
                outcome = Outcome.CLIENT_DISCONNECT
                category = FailureCategory.CLIENT_DISCONNECT
                if not reason or cancelled:
                    reason = "client disconnected" if disconnected else (
                        "request cancelled"
                    )
            elif outcome is None:
                # 业务异常处理器可显式标注终态，优先采信。
                state_outcome = state.get(STATE_OUTCOME_KEY)
                state_category = state.get(STATE_CATEGORY_KEY)
                state_reason = state.get(STATE_REASON_KEY)
                if state_outcome is not None and state_category is not None:
                    outcome = (
                        state_outcome
                        if isinstance(state_outcome, Outcome)
                        else Outcome(str(state_outcome))
                    )
                    category = (
                        state_category
                        if isinstance(state_category, FailureCategory)
                        else FailureCategory(str(state_category))
                    )
                    reason = str(state_reason or reason)
                else:
                    outcome, category, reason = classify_status(status_code)

            threshold = self.collector.config.slow_threshold_ms
            is_slow = duration_ms > threshold

            business_started_ms = (
                ctx.business_started_ms
                if ctx.business_started_ms is not None
                else 0.0
            )
            record = RequestRecord(
                trace_id=trace_id,
                entry=entry,
                method=method,
                path=raw_path,
                status_code=status_code,
                duration_ms=duration_ms,
                outcome=outcome,
                failure_category=category,
                reason=reason,
                slow=is_slow,
                kept=False,
                spans=tuple(ctx.span_records),
                started_ms=started_ms,
                business_started_ms=business_started_ms,
            )
            # record() 内部已对异常做隔离，不影响主链路。
            self.collector.record(record)
            reset_trace(token)
