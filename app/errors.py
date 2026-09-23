"""业务异常定义与统一异常处理器。

约定：

- :class:`BusinessError` 表示调用方可修复的业务错误（默认 400），遥测归类为
  ``business_error``；
- 其余未处理异常由兜底处理器转为 HTTP 500，遥测归类为
  ``framework_error`` —— 两者在采集口径中始终是不同类别。

处理器在 ``request.scope['state']`` 中写入终态标注，
:class:`~telemetry.middleware.TelemetryMiddleware` 会优先采信。
"""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from telemetry.classification import FailureCategory, Outcome, reason_of
from telemetry.errors import BusinessError
from telemetry.middleware import (
    STATE_CATEGORY_KEY,
    STATE_OUTCOME_KEY,
    STATE_REASON_KEY,
    STATE_STATUS_KEY,
)

__all__ = ["BusinessError", "register_exception_handlers"]


def _mark(
    request: Request,
    status_code: int,
    outcome: Outcome,
    category: FailureCategory,
    reason: str,
) -> None:
    state = request.scope.setdefault("state", {})
    state[STATE_STATUS_KEY] = status_code
    state[STATE_OUTCOME_KEY] = outcome
    state[STATE_CATEGORY_KEY] = category
    state[STATE_REASON_KEY] = reason


async def business_error_handler(request: Request, exc: BusinessError) -> JSONResponse:
    _mark(
        request,
        exc.status_code,
        Outcome.BUSINESS_ERROR,
        FailureCategory.BUSINESS,
        reason_of(exc),
    )
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": "business_error", "detail": exc.message},
    )


async def unhandled_error_handler(request: Request, exc: Exception) -> JSONResponse:
    """兜底：未处理异常一律 500，归类为框架错误。"""

    _mark(
        request,
        500,
        Outcome.FRAMEWORK_ERROR,
        FailureCategory.FRAMEWORK,
        reason_of(exc),
    )
    return JSONResponse(
        status_code=500,
        content={"error": "framework_error", "detail": "internal server error"},
    )


def register_exception_handlers(app: FastAPI) -> None:
    app.add_exception_handler(BusinessError, business_error_handler)
    # 放在最后兜底；FastAPI 自身的 HTTPException 仍走默认处理器（4xx/5xx
    # 由中间件按状态码兜底分类）。
    app.add_exception_handler(Exception, unhandled_error_handler)
