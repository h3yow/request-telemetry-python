"""慢请求耗时归因：等待 / 业务自身 / 嵌套调用 三段对账。

段的定义（边界时刻全部来自同一个 ``perf_counter`` 单调时钟）：

- **wait_ms（业务前等待）**：请求进入中间件 -> 业务处理真正开始。
  业务开始边界由 ``record.business_started_ms`` 显式标记（依赖注入用的
  ``mark_business_start``）；未标记时退化用"最早根 span 的起点"，
  没有 span 则视为无等待（0）；
- **nested_ms（嵌套调用）**：所有**根 span**（``parent_span_id is None``，
  即业务直接发起的内部调用）耗时之和。只数根 span，嵌套树不会重复计时；
- **business_ms（业务自身）**：``总耗时 - wait - nested``，把测量误差/残差
  （浮点尾差、未闭合 span 等）并入本段，保证三段之和**恰等于**总耗时。
  残差为负时（边界轻微倒挂）钳为 0，段不允许出现负数。

因此归因恒等式：``wait_ms + business_ms + nested_ms == duration_ms``
（同一次浮点加法可复现），明细树里每个 span 也保留独立耗时与状态。
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .recorder import RequestRecord, SpanRecord

# 浮点钳制阈值：小于该量级的负残差视为测量噪声并入业务段。
_EPS_MS = 1e-6


@dataclass(frozen=True)
class SegmentBreakdown:
    """三段耗时分解（毫秒）；三段之和恒等于请求总耗时。"""

    wait_ms: float
    business_ms: float
    nested_ms: float
    accounted_ms: float
    """三段合计（= ``wait + business + nested``，应与总耗时一致）。"""


@dataclass(frozen=True)
class SpanNode:
    """归因明细树中的一个 span 节点。"""

    span_id: str
    name: str
    duration_ms: float
    outcome: str
    failure_category: str
    reason: str
    started_offset_ms: float
    """相对请求起点的偏移（毫秒）；时刻缺失时为 0。"""

    children: tuple["SpanNode", ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class RequestAttribution:
    """单请求归因结果：段分解 + 根 span 明细树。"""

    trace_id: str
    entry: str
    duration_ms: float
    slow: bool
    outcome: str
    failure_category: str
    reason: str
    segments: SegmentBreakdown
    root_spans: tuple[SpanNode, ...]
    boundary_source: str
    """业务开始边界的来源：``mark`` / ``root_span`` / ``none``。"""


def _is_root(span: SpanNode | SpanRecord) -> bool:
    return span.parent_span_id is None  # type: ignore[attr-defined]


def _build_tree(
    spans: tuple[SpanRecord, ...], base_ms: float
) -> tuple[SpanNode, ...]:
    """按 parent 关系构造 span 树，返回有序的根节点元组。"""

    nodes: dict[str, SpanNode] = {}
    children: dict[str | None, list[SpanRecord]] = {}
    for span in spans:
        children.setdefault(span.parent_span_id, []).append(span)

    def node_of(span: SpanRecord) -> SpanNode:
        cached = nodes.get(span.span_id)
        if cached is not None:
            return cached
        kids = tuple(
            node_of(child)
            for child in sorted(
                children.get(span.span_id, []),
                key=lambda s: (s.started_ms, s.span_id),
            )
        )
        node = SpanNode(
            span_id=span.span_id,
            name=span.name,
            duration_ms=span.duration_ms,
            outcome=span.outcome.value,
            failure_category=span.failure_category.value,
            reason=span.reason,
            started_offset_ms=max(0.0, span.started_ms - base_ms)
            if span.started_ms > 0 and base_ms > 0
            else 0.0,
            children=kids,
        )
        nodes[span.span_id] = node
        return node

    roots = sorted(
        (s for s in spans if s.parent_span_id is None),
        key=lambda s: (s.started_ms, s.span_id),
    )
    return tuple(node_of(span) for span in roots)


def attribute(rec: RequestRecord) -> RequestAttribution:
    """对一条请求记录做耗时归因。"""

    total = float(rec.duration_ms)
    spans = rec.spans

    # --- 嵌套段：只累计根 span，避免树状嵌套重复计时。 ---
    root_records = tuple(s for s in spans if s.parent_span_id is None)
    nested_ms = float(sum((s.duration_ms for s in root_records), 0.0))

    # --- 等待段：确定业务开始边界。 ---
    start = float(rec.started_ms)
    if rec.business_started_ms > 0:
        business_start = float(rec.business_started_ms)
        boundary_source = "mark"
    elif root_records:
        business_start = float(min(s.started_ms for s in root_records))
        boundary_source = "root_span"
    else:
        business_start = start
        boundary_source = "none"

    if start > 0 and business_start > 0:
        wait_ms = max(0.0, business_start - start)
    else:
        # 记录缺少时刻信息（如手工构造的测试记录）：无法还原段，全部并入业务。
        wait_ms = 0.0
        nested_ms = 0.0
        boundary_source = "none"

    # --- 业务段：残差并入本段，保证三段之和恰为总耗时。 ---
    business_ms = total - wait_ms - nested_ms
    if business_ms < -_EPS_MS:
        # 边界倒挂超过噪声量级：不允许负段，嵌套段相应钳制（保守归因）。
        overflow = -business_ms
        nested_ms = max(0.0, nested_ms - overflow)
        business_ms = total - wait_ms - nested_ms
    if business_ms < 0.0:
        business_ms = 0.0
    accounted = wait_ms + business_ms + nested_ms

    return RequestAttribution(
        trace_id=rec.trace_id,
        entry=rec.entry,
        duration_ms=total,
        slow=rec.slow,
        outcome=rec.outcome.value,
        failure_category=rec.failure_category.value,
        reason=rec.reason,
        segments=SegmentBreakdown(
            wait_ms=wait_ms,
            business_ms=business_ms,
            nested_ms=nested_ms,
            accounted_ms=accounted,
        ),
        root_spans=_build_tree(spans, start),
        boundary_source=boundary_source,
    )
