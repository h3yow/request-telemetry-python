"""慢请求耗时归因测试：分段对账、嵌套树、异常数据与退化场景。

核心不变式（每条用例都验证）::

    queue + business_own + 顶层嵌套span之和 + response + other == total
"""

from __future__ import annotations

import logging

from telemetry.attribution import (
    SEGMENT_BUSINESS,
    SEGMENT_OTHER,
    SEGMENT_QUEUE,
    SEGMENT_RESPONSE,
    build_attribution,
)
from telemetry.classification import FailureCategory, Outcome
from telemetry.recorder import RequestRecord, SpanRecord

logger = logging.getLogger("telemetry.tests")


def _span(span_id: str, parent: str | None, name: str, duration: float,
          outcome: Outcome = Outcome.SUCCESS,
          category: FailureCategory = FailureCategory.NONE) -> SpanRecord:
    return SpanRecord(
        trace_id="t", span_id=span_id, parent_span_id=parent, name=name,
        duration_ms=duration, outcome=outcome, failure_category=category,
        reason="",
    )


def _record(total: float, *, queue: float = 0.0, business: float = 0.0,
            response: float = 0.0,
            spans: tuple[SpanRecord, ...] = ()) -> RequestRecord:
    return RequestRecord(
        trace_id="t", entry="http:GET /", method="GET", path="/",
        status_code=200, duration_ms=total, outcome=Outcome.SUCCESS,
        failure_category=FailureCategory.NONE, reason="", slow=True,
        kept=True, spans=spans, queue_ms=queue, business_ms=business,
        response_ms=response,
    )


def test_segments_sum_exactly_to_total() -> None:
    # total=100: queue=5 + business=70(自身40+outer30) + response=3 -> other=22
    spans = (
        _span("s1", None, "outer", 30.0),
        _span("s2", "s1", "inner", 10.0),
    )
    attr = build_attribution(_record(100.0, queue=5.0, business=70.0,
                                     response=3.0, spans=spans))
    logger.debug(
        "输入 total=100 q=5 business=70(outer30/inner10) response=3 -> "
        "own=%s nested=%s other=%s accounted=%s",
        attr.business_own_ms, attr.nested_total_ms, attr.other_ms,
        attr.accounted_ms,
    )
    assert attr.business_own_ms == 40.0
    assert attr.nested_total_ms == 30.0  # 只算顶层 outer；inner 不重复
    assert attr.other_ms == 22.0
    assert attr.accounted_ms == 100.0
    assert attr.is_consistent()


def test_nested_tree_structure_preserved() -> None:
    spans = (
        _span("a", None, "outer-a", 30.0),
        _span("b", "a", "mid", 20.0),
        _span("c", "b", "inner", 5.0),
        _span("d", None, "outer-b", 8.0),
    )
    attr = build_attribution(_record(100.0, business=38.0, spans=spans))
    logger.debug("输入两层嵌套 + 第二棵顶层树 -> 顶层段=%s",
                 [(s.name, s.duration_ms,
                   [(c.name, c.duration_ms) for c in s.children])
                  for s in attr.segments])
    named = {s.name: s for s in attr.segments}
    assert set(named) == {SEGMENT_BUSINESS, "outer-a", "outer-b"}
    mid = named["outer-a"].children[0]
    assert mid.name == "mid" and mid.children[0].name == "inner"
    # 顶层耗时只累加 outer-a(30)+outer-b(8)=38，中层/内层不重复。
    assert attr.nested_total_ms == 38.0
    assert attr.business_own_ms == 0.0
    assert attr.is_consistent()
    assert attr.other_ms == 62.0  # 100 - 0 - 38 - 0


def test_disconnected_request_without_response_phase() -> None:
    # 断开路径：无 response.start，business 窗口延伸到收尾，response=0。
    attr = build_attribution(_record(60.0, queue=4.0, business=56.0,
                                     response=0.0))
    logger.debug("断开请求 total=60 q=4 business=56 response=0 -> "
                 "own=%s other=%s accounted=%s",
                 attr.business_own_ms, attr.other_ms, attr.accounted_ms)
    assert attr.response_ms == 0.0
    assert attr.business_own_ms == 56.0
    assert attr.accounted_ms == 60.0
    assert attr.is_consistent()


def test_record_without_timing_points_degrades_to_other() -> None:
    # 手工构造的记录没有中间件计时点：全部耗时落入 other，仍严格对账。
    attr = build_attribution(_record(42.0))
    logger.debug("无计时点 total=42 -> q=%s own=%s resp=%s other=%s",
                 attr.queue_ms, attr.business_own_ms, attr.response_ms,
                 attr.other_ms)
    assert (attr.queue_ms, attr.business_own_ms, attr.response_ms) == (0, 0, 0)
    assert attr.other_ms == 42.0
    assert attr.accounted_ms == 42.0


def test_overlapping_spans_yield_negative_own_but_still_consistent() -> None:
    # 顶层两个 span 之和 80 > business 窗口 70（异常重叠）：
    # own=-10 被如实保留（segments_clean=False），残差段保证和仍等于 total。
    spans = (_span("a", None, "a", 50.0), _span("b", None, "b", 30.0))
    attr = build_attribution(_record(100.0, queue=5.0, business=70.0,
                                     response=3.0, spans=spans))
    logger.debug("重叠 span: business=70, 顶层span=80 -> own=%s clean=%s "
                 "other=%s accounted=%s（和仍对账）",
                 attr.business_own_ms, attr.segments_clean, attr.other_ms,
                 attr.accounted_ms)
    assert attr.business_own_ms == -10.0
    assert attr.segments_clean is False
    assert attr.is_consistent()
    assert attr.accounted_ms == 100.0


def test_orphan_span_with_unknown_parent_is_treated_as_root() -> None:
    # parent 指向不存在的 span：断链不丢失，提为顶层段。
    spans = (_span("x", "ghost", "lonely", 12.0),)
    attr = build_attribution(_record(50.0, business=12.0, spans=spans))
    logger.debug("断链 span(parent=未知) -> 顶层段=%s nested=%s",
                 [s.name for s in attr.segments], attr.nested_total_ms)
    assert any(s.name == "lonely" for s in attr.segments)
    assert attr.nested_total_ms == 12.0
    assert attr.is_consistent()


def test_flat_dict_contains_all_phases_in_time_order() -> None:
    spans = (_span("a", None, "call-a", 10.0),
             _span("b", None, "call-b", 6.0))
    attr = build_attribution(_record(100.0, queue=2.0, business=30.0,
                                     response=4.0, spans=spans))
    flat = attr.as_flat_dict()
    total_parts = sum(flat.values())
    logger.debug("扁平口径=%s 之和=%s", flat, total_parts)
    # 判定依据：q2 + 自身14 + call-a10 + call-b6 + resp4 + other64 = 100。
    assert flat[SEGMENT_QUEUE] == 2.0
    assert flat[SEGMENT_BUSINESS] == 14.0
    assert flat[SEGMENT_RESPONSE] == 4.0
    assert flat["call-a"] == 10.0 and flat["call-b"] == 6.0
    assert flat[SEGMENT_OTHER] == 64.0
    assert abs(total_parts - 100.0) < 1e-9
