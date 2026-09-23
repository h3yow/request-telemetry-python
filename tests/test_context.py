"""追踪上下文测试：缺失降级、嵌套归并、异常链路、并发隔离。"""

from __future__ import annotations

import asyncio
import logging

import pytest

from telemetry.classification import FailureCategory
from telemetry.context import (
    begin_trace,
    current_trace,
    reset_trace,
    span_record,
    trace_span,
)

logger = logging.getLogger("telemetry.tests")


def test_missing_context_is_stable_and_safe() -> None:
    """无 trace 时：current_trace 返回 None，span 返回 None，绝不抛异常。"""

    assert current_trace() is None
    with trace_span("orphan") as handle:
        assert handle is None
    assert span_record(handle) is None
    logger.debug("输入: 无活动 trace -> current_trace=None, span 句柄=None（行为稳定）")


def test_nested_spans_share_trace_and_chain() -> None:
    ctx, token = begin_trace("job")
    try:
        with trace_span("outer") as outer:
            with trace_span("inner") as inner:
                pass
    finally:
        reset_trace(token)

    outer_rec = span_record(outer)
    inner_rec = span_record(inner)
    logger.debug(
        "输入嵌套 outer/inner -> trace 相同=%s, inner.parent=%s, outer.id=%s",
        inner_rec.trace_id == ctx.trace_id == outer_rec.trace_id,
        inner_rec.parent_span_id,
        outer_rec.span_id,
    )
    assert inner_rec.trace_id == outer_rec.trace_id == ctx.trace_id
    assert inner_rec.parent_span_id == outer_rec.span_id
    assert outer_rec.parent_span_id is None


@pytest.mark.parametrize(
    "exc_name,exc,expected",
    [
        ("business", __import__("telemetry.errors", fromlist=["BusinessError"]).BusinessError("x"), FailureCategory.BUSINESS),
        ("framework", RuntimeError("boom"), FailureCategory.FRAMEWORK),
        ("cancel", asyncio.CancelledError(), FailureCategory.CLIENT_DISCONNECT),
    ],
)
def test_exception_chain_classified_on_every_span(exc_name, exc, expected) -> None:
    ctx, token = begin_trace("job")
    raised = None
    try:
        with trace_span("outer"):
            with trace_span("inner"):
                raise exc
    except BaseException as caught:  # noqa: BLE001
        raised = caught
    finally:
        reset_trace(token)

    logger.debug("输入异常链 outer>inner 抛 %r -> 各 span 类别=%s",
                 exc, [s.failure_category for s in ctx.span_records])
    assert type(raised) is type(exc) or isinstance(raised, type(exc))
    assert len(ctx.span_records) == 2
    for span in ctx.span_records:
        assert span.failure_category is expected
        assert span.trace_id == ctx.trace_id


def test_trace_context_isolated_between_concurrent_tasks() -> None:
    async def worker(entry: str) -> tuple[str, str]:
        ctx, token = begin_trace(entry)
        await asyncio.sleep(0)
        with trace_span(entry + "-span"):
            await asyncio.sleep(0)
            observed = current_trace().entry
        reset_trace(token)
        return ctx.trace_id, observed

    async def run():
        results = await asyncio.gather(*(worker(f"e{i}") for i in range(50)))
        return results

    results = asyncio.run(run())
    trace_ids = {tid for tid, _ in results}
    observations = {obs for _, obs in results}
    logger.debug("50 并发任务 trace_id 去重数=%s, entry 观测集合大小=%s",
                 len(trace_ids), len(observations))
    assert len(trace_ids) == 50  # 无错配
    assert observations == {f"e{i}" for i in range(50)}  # 无串扰


def test_reset_restores_previous_context() -> None:
    ctx1, t1 = begin_trace("first")
    ctx2, t2 = begin_trace("second")
    assert current_trace() is ctx2
    reset_trace(t2)
    assert current_trace() is ctx1
    reset_trace(t1)
    assert current_trace() is None
    logger.debug("嵌套 begin/reset 后上下文正确弹回 first -> None")
