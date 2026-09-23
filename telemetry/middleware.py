"""ASGI 遥测中间件：在请求边界统一采集。

职责
----
1. 为每个 HTTP 请求建立唯一 :class:`~telemetry.context.TraceContext`，
   使得请求处理过程中产生的所有 span 归并到同一 ``trace_id``；
2. 用 :func:`time.perf_counter` 测量端到端耗时（毫秒）；
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

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from .classification import (
    FailureCategory,
    Outcome,
    classify_exception,
    classify_status,
)
from .collector import TelemetryCollector
from .config import TelemetryConfig
from .context import begin_trace, reset_trace
from .recorder import RequestRecord

# 业务异常处理器可在 scope['state'] 中写入该键，显式传递终态信息。
STATE_OUTCOME_KEY = "telemetry_outcome"
STATE_CATEGORY_KEY = "telemetry_failure_category"
STATE_REASON_KEY = "telemetry_reason"
STATE_STATUS_KEY = "telemetry_status_code"

# 内部观测/控制端点不纳入业务采集口径，避免自污染。
EXCLUDED_PATH_PREFIX = "/__telemetry__/"


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

        started = time.perf_counter()
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
            duration_ms = (time.perf_counter() - started) * 1000.0

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
            )
            # record() 内部已对异常做隔离，不影响主链路。
            self.collector.record(record)
            reset_trace(token)
