"""追踪上下文：基于 contextvars，保证并发/嵌套下互不串扰。"""
from __future__ import annotations

from contextvars import ContextVar, Token

_current_trace_id: ContextVar[str | None] = ContextVar("telemetry_trace_id", default=None)


def set_trace_id(trace_id: str) -> Token[str | None]:
    """设置当前上下文的 trace_id，返回可复位的 token。

    contextvars 保证：同一请求内的嵌套调用共享该值；
    并发请求各自持有独立副本，互不覆盖。
    """
    return _current_trace_id.set(trace_id)


def get_trace_id() -> str | None:
    """获取当前上下文 trace_id；缺失时返回 None（行为明确且稳定，不抛异常）。"""
    return _current_trace_id.get()


def reset_trace_id(token: Token[str | None]) -> None:
    """按 token 复位上下文，请求结束时必须调用以避免泄漏。"""
    _current_trace_id.reset(token)


def ensure_trace_id() -> str:
    """获取当前 trace_id；上下文缺失时生成新 id 并写入上下文。

    缺失时的行为明确且稳定：不抛异常、不返回 None，
    保证单次上下文异常不会影响整体采集。
    """
    from .models import new_trace_id

    trace_id = get_trace_id()
    if trace_id is None:
        trace_id = new_trace_id()
        set_trace_id(trace_id)
    return trace_id
