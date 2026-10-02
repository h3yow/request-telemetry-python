"""慢请求三段归因：段恒等式、根 span 不重复、边界来源退化。"""

from __future__ import annotations

import logging

from telemetry.attribution import attribute
from telemetry.classification import FailureCategory, Outcome
from telemetry.recorder import RequestRecord, SpanRecord

logger = logging.getLogger("telemetry.tests")


def _span(sid, parent, name, dur, st, en):
    return SpanRecord("t", sid, parent, name, dur, Outcome.SUCCESS,
                      FailureCategory.NONE, "", st, en)


def test_three_segments_sum_to_total() -> None:
    """总100 = 等20 + 嵌套(根30+根10=40) + 业务40；子 span 不重复计。"""

    spans = (
        _span("s1", None, "A", 30.0, 120.0, 150.0),
        _span("s2", "s1", "A-child", 10.0, 125.0, 135.0),
        _span("s3", None, "B", 10.0, 160.0, 170.0),
    )
    rec = RequestRecord("t", "e", "GET", "/", 200, 100.0, Outcome.SUCCESS,
                        FailureCategory.NONE, "", True, True, spans,
                        started_ms=100.0, business_started_ms=120.0)
    a = attribute(rec)
    logger.debug(
        "总=100 wait=%s nested=%s(子span不重复) business=%s accounted=%s",
        a.segments.wait_ms, a.segments.nested_ms, a.segments.business_ms,
        a.segments.accounted_ms,
    )
    assert (a.segments.wait_ms, a.segments.nested_ms, a.segments.business_ms) == (
        20.0, 40.0, 40.0,
    )
    assert abs(a.segments.accounted_ms - 100.0) < 1e-9
    assert [n.span_id for n in a.root_spans] == ["s1", "s3"]
    assert [c.span_id for c in a.root_spans[0].children] == ["s2"]


def test_boundary_fallback_to_root_span() -> None:
    """未显式标记 business_start 时，用最早根 span 起点退化，仍能对账。"""

    spans = (_span("s1", None, "A", 30.0, 120.0, 150.0),)
    rec = RequestRecord("t", "e", "GET", "/", 200, 100.0, Outcome.SUCCESS,
                        FailureCategory.NONE, "", True, True, spans,
                        started_ms=100.0)
    a = attribute(rec)
    logger.debug("无标记 -> 来源=%s wait=%s accounted=%s",
                 a.boundary_source, a.segments.wait_ms, a.segments.accounted_ms)
    assert a.boundary_source == "root_span"
    assert a.segments.wait_ms == 20.0
    assert abs(a.segments.accounted_ms - 100.0) < 1e-9


def test_no_spans_all_time_is_business() -> None:
    rec = RequestRecord("t", "e", "GET", "/", 200, 50.0, Outcome.SUCCESS,
                        FailureCategory.NONE, "", False, True, (),
                        started_ms=10.0, business_started_ms=11.0)
    a = attribute(rec)
    # 无 span：nested=0；等待已在 business 前发生 -> wait=1, business=49
    logger.debug("无span wait=%s biz=%s nested=%s", a.segments.wait_ms,
                 a.segments.business_ms, a.segments.nested_ms)
    assert a.boundary_source == "mark"
    assert (a.segments.wait_ms, a.segments.nested_ms,
            round(a.segments.business_ms, 9)) == (1.0, 0.0, 49.0)
    assert abs(a.segments.accounted_ms - 50.0) < 1e-9


def test_negative_residual_clamped() -> None:
    """根 span 合计量异常超过总耗时时，业务段钳为 0 且不出现负段、不超总耗时。"""

    spans = (_span("s1", None, "A", 120.0, 100.0, 220.0),)
    rec = RequestRecord("t", "e", "GET", "/", 200, 100.0, Outcome.SUCCESS,
                        FailureCategory.NONE, "", True, True, spans,
                        started_ms=100.0, business_started_ms=100.0)
    a = attribute(rec)
    logger.debug("嵌套倒挂 nested=%s business=%s accounted=%s",
                 a.segments.nested_ms, a.segments.business_ms,
                 a.segments.accounted_ms)
    assert a.segments.business_ms == 0.0
    assert a.segments.nested_ms <= 100.0
    assert abs(a.segments.accounted_ms - 100.0) < 1e-9
