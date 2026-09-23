"""追踪上下文：把同一次请求产生的多条记录归并到同一 trace。

实现要点：

- 使用 :mod:`contextvars` 存储当前 trace。asyncio 为每个并发 Task 复制
  Context，因此并发请求之间天然隔离，不会出现 trace_id 错配；
- 一次请求对应一个 :class:`TraceContext`（一个 ``trace_id``），其下可有多个
  span（如嵌套的内部调用），全部携带同一 ``trace_id``；
- 上下文缺失时 :func:`current_trace` 返回 ``None``，绝不抛异常，也绝不隐式
  创建 trace —— 调用方据此可以选择降级或跳过，保证单次异常不影响整体采集。
"""

from __future__ import annotations

import contextlib
import itertools
import time
from collections.abc import Iterator
from contextvars import ContextVar
from dataclasses import dataclass, field

from .classification import FailureCategory, Outcome
from .recorder import SpanRecord

_trace_counter = itertools.count(1)
_span_counter = itertools.count(1)


def _new_trace_id() -> str:
    return f"trace-{next(_trace_counter):016d}"


def _new_span_id() -> str:
    return f"span-{next(_span_counter):016d}"


@dataclass
class TraceContext:
    """单次请求的追踪上下文。"""

    trace_id: str
    entry: str
    span_ids: list[str] = field(default_factory=list)
    """已登记的 span id，按创建顺序排列（用于核对嵌套结构）。"""

    span_records: list[SpanRecord] = field(default_factory=list)
    """本次 trace 下已结束的全部 span 记录（嵌套/异常均会落入）。"""

    _span_parents: dict[str, str | None] = field(default_factory=dict)

    def register_span(self, parent_span_id: str | None) -> str:
        """登记一个新 span，返回其全局唯一 id。"""

        span_id = _new_span_id()
        self.span_ids.append(span_id)
        self._span_parents[span_id] = parent_span_id
        return span_id

    def parent_of(self, span_id: str) -> str | None:
        return self._span_parents.get(span_id)


_current: ContextVar[TraceContext | None] = ContextVar(
    "telemetry_trace", default=None
)


def begin_trace(entry: str):
    """创建并绑定一个新的 trace 上下文，返回 ``(ctx, reset_token)``。

    典型用法::

        ctx, token = begin_trace("http")
        try:
            ...
        finally:
            reset_trace(token)
    """

    ctx = TraceContext(trace_id=_new_trace_id(), entry=entry)
    token = _current.set(ctx)
    return ctx, token


def current_trace() -> TraceContext | None:
    """返回当前 trace；缺失时稳定返回 ``None``。"""

    return _current.get()


def reset_trace(token: object) -> None:
    """恢复 ContextVar 到绑定前状态；token 非法时静默忽略。"""

    with contextlib.suppress(ValueError):
        _current.reset(token)  # type: ignore[arg-type]


@dataclass
class _SpanHandle:
    ctx: TraceContext
    span_id: str
    parent_span_id: str | None
    name: str
    started: float


@contextlib.contextmanager
def trace_span(
    name: str,
) -> Iterator[_SpanHandle | None]:
    """在当前 trace 下登记一个嵌套 span 的上下文管理器。

    - 有活动 trace 时返回 span 句柄，退出时把结果写入句柄上的
      ``record``（:class:`SpanRecord`），供上层收集；
    - 无活动 trace 时返回 ``None`` 且不报错 —— 上下文缺失行为明确稳定。

    任何在 with 块内抛出的异常都会按既有分类规则标注，随后原样继续抛出，
    绝不在采集层吞掉业务异常。
    """

    ctx = current_trace()
    if ctx is None:
        yield None
        return

    parent = getattr(_span_stack.get(None), "span_id", None)
    span_id = ctx.register_span(parent)
    handle = _SpanHandle(
        ctx=ctx,
        span_id=span_id,
        parent_span_id=parent,
        name=name,
        started=time.perf_counter(),
    )
    token = _span_stack.set(handle)
    record: SpanRecord | None = None
    try:
        yield handle
    except BaseException as exc:  # noqa: BLE001 - 需捕获并分类后重新抛出
        from .classification import classify_exception

        outcome, category, reason = classify_exception(exc)
        record = SpanRecord(
            trace_id=ctx.trace_id,
            span_id=span_id,
            parent_span_id=parent,
            name=name,
            duration_ms=(time.perf_counter() - handle.started) * 1000.0,
            outcome=outcome,
            failure_category=category,
            reason=reason,
        )
        handle.record = record  # type: ignore[attr-defined]
        ctx.span_records.append(record)
        raise
    else:
        record = SpanRecord(
            trace_id=ctx.trace_id,
            span_id=span_id,
            parent_span_id=parent,
            name=name,
            duration_ms=(time.perf_counter() - handle.started) * 1000.0,
            outcome=Outcome.SUCCESS,
            failure_category=FailureCategory.NONE,
            reason="",
        )
        handle.record = record  # type: ignore[attr-defined]
        ctx.span_records.append(record)
    finally:
        _span_stack.reset(token)


_span_stack: ContextVar[_SpanHandle | None] = ContextVar(
    "telemetry_span_stack", default=None
)


def span_record(handle: _SpanHandle | None) -> SpanRecord | None:
    """从句柄读取已生成的 span 记录；句柄为 None 时返回 None。"""

    if handle is None:
        return None
    return getattr(handle, "record", None)
