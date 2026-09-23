"""结果与失败原因分类。

关键约定（业务异常与框架级错误绝不混为一类）：

- 业务类：``BusinessError`` 或 HTTP 4xx —— 调用方语义错误，归类为
  :data:`FailureCategory.BUSINESS`；
- 框架类：未处理异常或 HTTP 5xx —— 服务/框架内部错误，归类为
  :data:`FailureCategory.FRAMEWORK`；
- 客户端断开：``asyncio.CancelledError`` / Starlette ``ClientDisconnect``，
  归类为 :data:`FailureCategory.CLIENT_DISCONNECT`；
- 其余无法判定的归 :data:`FailureCategory.UNKNOWN`，不允许静默吞掉。
"""

from __future__ import annotations

import asyncio

from enum import Enum

from fastapi import HTTPException
from starlette.requests import ClientDisconnect

from .errors import BusinessError


class Outcome(str, Enum):
    """请求终态。"""

    SUCCESS = "success"
    BUSINESS_ERROR = "business_error"
    FRAMEWORK_ERROR = "framework_error"
    CLIENT_DISCONNECT = "client_disconnect"


class FailureCategory(str, Enum):
    """失败原因类别（可区分、可聚合）。"""

    NONE = "none"
    BUSINESS = "business"
    FRAMEWORK = "framework"
    CLIENT_DISCONNECT = "client_disconnect"
    UNKNOWN = "unknown"


def reason_of(exc: BaseException) -> str:
    """生成稳定可读的失败原因字符串：``异常类型名: 消息``。"""

    message = str(exc).strip()
    if message:
        return f"{type(exc).__name__}: {message}"
    return type(exc).__name__


def classify_exception(exc: BaseException) -> tuple[Outcome, FailureCategory, str]:
    """按异常类型判定结果、失败类别与原因。

    判定优先级：客户端断开 > 业务异常 > 其他框架错误。
    """

    if isinstance(exc, (asyncio.CancelledError, ClientDisconnect)):
        return (
            Outcome.CLIENT_DISCONNECT,
            FailureCategory.CLIENT_DISCONNECT,
            reason_of(exc),
        )

    if isinstance(exc, BusinessError):
        return Outcome.BUSINESS_ERROR, FailureCategory.BUSINESS, reason_of(exc)

    if isinstance(exc, HTTPException):
        if 400 <= exc.status_code < 500:
            return Outcome.BUSINESS_ERROR, FailureCategory.BUSINESS, reason_of(exc)
        return Outcome.FRAMEWORK_ERROR, FailureCategory.FRAMEWORK, reason_of(exc)

    # 其余未处理异常一律视为框架级错误，绝不与业务错误混类。
    return Outcome.FRAMEWORK_ERROR, FailureCategory.FRAMEWORK, reason_of(exc)


def classify_status(status_code: int) -> tuple[Outcome, FailureCategory, str]:
    """在没有抛出异常时，依据最终 HTTP 状态码兜底分类。"""

    if status_code < 400:
        return Outcome.SUCCESS, FailureCategory.NONE, ""
    if 400 <= status_code < 500:
        return (
            Outcome.BUSINESS_ERROR,
            FailureCategory.BUSINESS,
            f"HTTP {status_code}",
        )
    return (
        Outcome.FRAMEWORK_ERROR,
        FailureCategory.FRAMEWORK,
        f"HTTP {status_code}",
    )
