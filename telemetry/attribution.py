"""慢请求耗时归因：把端到端耗时拆成可对账的若干段。

分段口径（与 :class:`~telemetry.recorder.RequestRecord` 上的单调时钟
测量点一一对应，见 :mod:`telemetry.middleware`）：

- ``queue``：中间件入口 → 下游首次 receive 返回（拿到请求）之间的等待，
  即真正进入业务处理之前的等待时间（含框架取请求本身的 await 等待）；
- ``business``：业务处理窗口总耗时（业务起点 → 应用协程结束）。归因时再
  拆成"业务自身耗时"与其内部的顶层嵌套调用；
- 嵌套调用：按 span 的 ``parent_span_id`` 组织成树，同一父节点下的子 span
  视为兄弟、**互不重叠**（正常的嵌套上下文语义保证了这一点）；
- ``response``：应用协程结束 → 中间件收尾（响应尾部/清理）；
- ``other``：对账残差，``other = total − queue − business − response``。
  业务自身耗时 ``business_own = business − 顶层 span 耗时之和``。

对账保证：``queue + business_own + 顶层span之和 + response + other`` 恒等于
``total``（浮点残差被归入 ``other``，绝不凭空多出或减少）。当顶层 span
之和大于业务窗口（异常/测量噪声导致的负值残差）时，``other`` 可能为负，
此时 :meth:`RequestAttribution.is_consistent` 仍成立（和严格对账），但
:attr:`RequestAttribution.segments_clean` 为 ``False``，提示数据存在重叠。
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .recorder import RequestRecord

# 扁平分段名称约定（顺序即耗时贡献的时间顺序）。
SEGMENT_QUEUE = "queue"
SEGMENT_BUSINESS = "business"
SEGMENT_RESPONSE = "response"
SEGMENT_OTHER = "other"

# 浮点对账容差：1 纳秒（单位毫秒）。
_CONSISTENCY_TOLERANCE_MS = 1e-6


@dataclass(frozen=True)
class Segment:
    """归因树上的一段耗时；children 为其内部嵌套的子段。

    顶层归因树的每个 :class:`Segment` 代表一个顶层嵌套调用（或业务自身
    段），其 ``children`` 为该调用内部更深一层的嵌套调用。
    """

    name: str
    duration_ms: float
    children: tuple["Segment", ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class RequestAttribution:
    """单个请求的耗时归因结果。

    扁平不变式::

        queue_ms + business_own_ms + sum(顶层嵌套 span) + response_ms + other_ms
        == total_ms
    """

    trace_id: str
    entry: str
    total_ms: float
    queue_ms: float
    business_own_ms: float
    response_ms: float
    other_ms: float
    segments: tuple[Segment, ...] = field(default_factory=tuple)
    """顶层归因段：业务自身段（可能为 0）在前，其后按结束顺序排列顶层
    嵌套调用；嵌套调用内部的子调用挂在各自的 children 上。"""

    nested_total_ms: float = 0.0
    """顶层嵌套调用耗时之和（已计入 segments，扁平对账时用）。"""

    segments_clean: bool = True
    """各段语义是否干净（无重叠）：业务自身耗时非负即视为干净。"""

    @property
    def accounted_ms(self) -> float:
        """扁平各段（含残差段）之和；应与 ``total_ms`` 相等。"""

        return (
            self.queue_ms
            + self.business_own_ms
            + self.nested_total_ms
            + self.response_ms
            + self.other_ms
        )

    def is_consistent(self, tolerance_ms: float = _CONSISTENCY_TOLERANCE_MS) -> bool:
        """各段之和是否与总耗时对得上（容差默认 1 纳秒）。"""

        return abs(self.accounted_ms - self.total_ms) <= tolerance_ms

    def as_flat_dict(self) -> dict[str, float]:
        """扁平口径：每一段的毫秒数（嵌套调用不重复展开，仅顶层各一段）。"""

        parts: dict[str, float] = {
            SEGMENT_QUEUE: self.queue_ms,
            SEGMENT_BUSINESS: self.business_own_ms,
            SEGMENT_RESPONSE: self.response_ms,
            SEGMENT_OTHER: self.other_ms,
        }
        for seg in self.segments:
            if seg.name in (SEGMENT_BUSINESS,):
                continue
            parts[seg.name] = parts.get(seg.name, 0.0) + seg.duration_ms
        return parts


def _build_segment_tree(record: RequestRecord) -> tuple[list[Segment], float]:
    """把记录上的 span 组织成顶层段列表，返回 (顶层段, 顶层耗时之和)。

    组织规则（确定性，避免异常数据导致归因崩溃）：

    - 按 span 出现顺序建树；父 id 未知的 span 当作顶层段（断链不丢失）；
    - 每个父节点的 children 按子 span 的出现顺序排列；
    - 顶层耗时之和只累加顶层段；子段耗时已包含在父段内，不重复入账。
    """

    children_of: dict[str, list[tuple[int, str, str, float]]] = {}
    meta: dict[str, tuple[str, float, str | None, int]] = {}

    for index, span in enumerate(record.spans):
        meta[span.span_id] = (span.name, span.duration_ms, span.parent_span_id, index)

    roots: list[tuple[int, str]] = []
    for index, span in enumerate(record.spans):
        parent = span.parent_span_id
        if parent is None or parent not in meta:
            roots.append((index, span.span_id))
        else:
            children_of.setdefault(parent, []).append(
                (index, span.span_id, span.name, span.duration_ms)
            )

    def build(span_id: str) -> Segment:
        name, duration, _, _ = meta[span_id]
        kids = tuple(
            build(child_id)
            for _, child_id, _, _ in sorted(children_of.get(span_id, []),
                                            key=lambda item: item[0])
        )
        return Segment(name=name, duration_ms=duration, children=kids)

    root_segments: list[Segment] = []
    nested_total = 0.0
    for _, span_id in sorted(roots, key=lambda item: item[0]):
        seg = build(span_id)
        root_segments.append(seg)
        nested_total += seg.duration_ms
    return root_segments, nested_total


def build_attribution(record: RequestRecord) -> RequestAttribution:
    """依据请求记录与其 span 树构建耗时归因。

    未携带中间件计时点的记录（``queue/business/response`` 均为 0）退化为
    全部耗时落在 ``other``，对账恒成立。
    """

    total = float(record.duration_ms)
    queue = max(0.0, float(record.queue_ms))
    business = max(0.0, float(record.business_ms))
    response = max(0.0, float(record.response_ms))

    nested_segments, nested_total = _build_segment_tree(record)

    business_own = business - nested_total
    clean = business_own >= 0.0

    # 先把业务自身段放在顶层（为负也保留真实残差语义；对账仍严格成立）。
    business_seg = Segment(name=SEGMENT_BUSINESS, duration_ms=business_own)
    segments = (business_seg, *nested_segments)

    # 残差段收口全部浮点误差与未解释时间，保证和恒等于 total。
    other = total - queue - business - response

    return RequestAttribution(
        trace_id=record.trace_id,
        entry=record.entry,
        total_ms=total,
        queue_ms=queue,
        business_own_ms=business_own,
        response_ms=response,
        other_ms=other,
        segments=segments,
        nested_total_ms=nested_total,
        segments_clean=clean,
    )
