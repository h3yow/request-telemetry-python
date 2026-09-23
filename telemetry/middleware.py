"""FastAPI/Starlette 中间件：统一采集每个请求的耗时、结果与失败原因。"""
from __future__ import annotations

import logging
import time

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import ClientDisconnect, Request
from starlette.responses import JSONResponse, Response

from .collector import Collector
from .context import ensure_trace_id, reset_trace_id, set_trace_id
from .models import FailureCategory, Outcome, RequestRecord

logger = logging.getLogger("telemetry")


class BusinessError(Exception):
    """业务异常：可预期的业务失败，与框架级错误严格区分。"""

    def __init__(self, message: str, status_code: int = 400) -> None:
        super().__init__(message)
        self.status_code = status_code


class TelemetryMiddleware(BaseHTTPMiddleware):
    """请求采集中间件。

    失败归类规则（判定依据）：
    - BusinessError 或 4xx 响应        → BUSINESS（业务异常）
    - 未捕获异常或 5xx 响应            → FRAMEWORK（框架级错误）
    - ClientDisconnect（客户端中断）   → CLIENT_DISCONNECT
    - 其余                              → SUCCESS

    采集本身的异常被吞掉并记日志：单次采集失败不得影响请求与整体采集。
    """

    def __init__(self, app, collector: Collector) -> None:  # type: ignore[no-untyped-def]
        super().__init__(app)
        self._collector = collector

    async def dispatch(self, request: Request, call_next) -> Response:  # type: ignore[no-untyped-def]
        if not self._collector.enabled:
            return await call_next(request)

        trace_id = request.headers.get("x-trace-id") or ensure_trace_id()
        token = set_trace_id(trace_id)
        start = time.perf_counter()
        status_code = 500
        outcome = Outcome.SUCCESS
        category = FailureCategory.NONE
        reason = ""
        try:
            response = await call_next(request)
            status_code = response.status_code
            if await request.is_disconnected():
                # 处理期间客户端已断开：无论响应如何都归类为 CLIENT_DISCONNECT。
                # is_disconnected() 为非阻塞探测，仅在断开消息已就绪时返回 True。
                status_code = 499
                outcome, category = Outcome.FAILURE, FailureCategory.CLIENT_DISCONNECT
                reason = "client disconnected before response completed"
            elif status_code >= 500:
                outcome, category = Outcome.FAILURE, FailureCategory.FRAMEWORK
                reason = f"HTTP {status_code}"
            elif status_code >= 400:
                outcome, category = Outcome.FAILURE, FailureCategory.BUSINESS
                reason = f"HTTP {status_code}"
            return response
        except BusinessError as exc:
            status_code = exc.status_code
            outcome, category = Outcome.FAILURE, FailureCategory.BUSINESS
            reason = f"BusinessError: {exc}"
            return JSONResponse({"detail": str(exc)}, status_code=exc.status_code)
        except ClientDisconnect:
            status_code = 499
            outcome, category = Outcome.FAILURE, FailureCategory.CLIENT_DISCONNECT
            reason = "client disconnected before response completed"
            raise
        except Exception as exc:
            status_code = 500
            outcome, category = Outcome.FAILURE, FailureCategory.FRAMEWORK
            reason = f"{type(exc).__name__}: {exc}"
            raise
        finally:
            duration_ms = (time.perf_counter() - start) * 1000.0
            try:
                self._collector.record(
                    RequestRecord(
                        trace_id=trace_id,
                        route=request.url.path,
                        method=request.method,
                        status_code=status_code,
                        duration_ms=duration_ms,
                        outcome=outcome,
                        failure_category=category,
                        failure_reason=reason,
                    )
                )
            except Exception:  # 采集异常不得影响请求与整体采集
                logger.exception("telemetry record failed, request unaffected")
            reset_trace_id(token)


def emit_nested_record(
    collector: Collector,
    *,
    name: str,
    duration_ms: float,
    outcome: Outcome = Outcome.SUCCESS,
    failure_category: FailureCategory = FailureCategory.NONE,
    failure_reason: str = "",
) -> bool:
    """在请求内嵌套调用处补采一条记录。

    trace_id 取自当前追踪上下文（ensure_trace_id：缺失时生成新 id，
    行为明确且稳定），因此同一请求的多条记录可按 trace_id 归并。
    """
    return collector.record(
        RequestRecord(
            trace_id=ensure_trace_id(),
            route=name,
            method="INTERNAL",
            status_code=0,
            duration_ms=duration_ms,
            outcome=outcome,
            failure_category=failure_category,
            failure_reason=failure_reason,
        )
    )
