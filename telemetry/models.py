"""采集数据模型：结果、失败类别与请求记录。"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum


class Outcome(str, Enum):
    """请求结果。"""

    SUCCESS = "success"
    FAILURE = "failure"


class FailureCategory(str, Enum):
    """失败类别：业务异常与框架级错误严格区分。"""

    NONE = "none"                    # 非失败
    BUSINESS = "business"            # 业务异常（可预期，由业务代码显式抛出）
    FRAMEWORK = "framework"          # 框架级错误（未捕获异常、500 等）
    CLIENT_DISCONNECT = "client_disconnect"  # 客户端中途断开
    TIMEOUT = "timeout"              # 超时类失败


@dataclass(frozen=True)
class RequestRecord:
    """单条请求采集记录。"""

    trace_id: str
    route: str
    method: str
    status_code: int
    duration_ms: float
    outcome: Outcome
    failure_category: FailureCategory = FailureCategory.NONE
    failure_reason: str = ""
    timestamp: float = field(default_factory=time.time)


def new_trace_id() -> str:
    return uuid.uuid4().hex
