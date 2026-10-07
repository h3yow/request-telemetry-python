"""容量淘汰、窗口口径与慢请求归因查询的采集器集成测试。

覆盖：

- 明细 FIFO 上限：超限确定性淘汰最旧；census 全量计数不丢不重；
- 滚动窗口为全量口径（与采样、明细淘汰解耦）；
- 关闭采集 / 零采样时各口径的明确行为；
- slowest_attributions 的排序、limit、entry 过滤与对账。
"""

from __future__ import annotations

import logging

import pytest

from telemetry.classification import FailureCategory, Outcome
from telemetry.collector import TelemetryCollector
from telemetry.config import TelemetryConfig
from telemetry.recorder import RequestRecord, SpanRecord
from telemetry.attribution import build_attribution

logger = logging.getLogger("telemetry.tests")


class FakeClock:
    def __init__(self, start: float = 10_000.0) -> None:
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


def _rec(trace_id: str, *, duration_ms: float = 10.0,
         slow: bool = False, outcome: Outcome = Outcome.SUCCESS,
         category: FailureCategory = FailureCategory.NONE,
         entry: str = "http:GET /", status_code: int = 200,
         spans: tuple[SpanRecord, ...] = ()) -> RequestRecord:
    return RequestRecord(
        trace_id=trace_id, entry=entry, method="GET", path="/",
        status_code=status_code, duration_ms=duration_ms, outcome=outcome,
        failure_category=category, reason=("", "boom")[category !=
                                                        FailureCategory.NONE],
        slow=slow, kept=False, spans=spans,
    )


@pytest.fixture
def bounded_collector() -> tuple[TelemetryCollector, FakeClock]:
    clock = FakeClock()
    coll = TelemetryCollector(
        TelemetryConfig(
            enabled=True, sample_rate=1.0, slow_threshold_ms=50.0,
            window_seconds=100.0,
            detail_max_records=3, slow_top_n=2,
        ),
        clock=clock,
    )
    return coll, clock


# ---- 明细 FIFO 淘汰 ----------------------------------------------------

def test_detail_eviction_is_fifo_and_census_stays_full(
    bounded_collector: tuple[TelemetryCollector, FakeClock]
) -> None:
    coll, clock = bounded_collector
    for i in range(5):
        clock.advance(1.0)
        coll.record(_rec(f"t-{i}", duration_ms=10.0 + i))
    kept_ids = [r.trace_id for r in coll.snapshot()]
    logger.debug("明细上限 3，写入 5 条 -> 保留=%s 淘汰=%s",
                 kept_ids, coll.detail_evicted)
    # 判定依据：FIFO 保留最新 3 条（t-2,t-3,t-4），淘汰计数=2。
    assert kept_ids == ["t-2", "t-3", "t-4"]
    assert coll.detail_evicted == 2
    # 全量计数不受任何影响。
    report = coll.aggregate()
    assert report.total_seen == 5
    assert report.census["http:GET /"].total == 5
    # 窗口同样不受明细淘汰影响（全量 5 条）。
    assert coll.window_stats()["http:GET /"].count == 5


def test_evicted_trace_can_not_be_re_counted_but_duplicate_still_deduped(
    bounded_collector: tuple[TelemetryCollector, FakeClock]
) -> None:
    coll, clock = bounded_collector
    first = _rec("t-0")
    coll.record(first)
    coll.record(_rec("t-1"))
    coll.record(_rec("t-2"))
    coll.record(_rec("t-3"))  # t-0 被 FIFO 淘汰出明细
    assert "t-0" not in [r.trace_id for r in coll.snapshot()]
    # t-0 重复提交：不重复计数（去重账本仍在），也不会重新进明细；
    # 返回 False 表示该 trace 当前已不在保留明细中。
    kept_again = coll.record(first)
    report = coll.aggregate()
    logger.debug("已淘汰 trace 重复提交 -> kept=%s total_seen=%s kept=%s",
                 kept_again, report.total_seen, report.total_kept)
    assert kept_again is False
    assert [r.trace_id for r in coll.snapshot()] == ["t-1", "t-2", "t-3"]
    assert report.total_seen == 4


def test_shrinking_capacity_via_config_evicts_immediately(
    bounded_collector: tuple[TelemetryCollector, FakeClock]
) -> None:
    coll, clock = bounded_collector
    for i in range(3):
        clock.advance(1.0)
        coll.record(_rec(f"t-{i}"))
    assert len(coll.snapshot()) == 3
    coll.update_config(
        TelemetryConfig(detail_max_records=1, window_seconds=100.0,
                        slow_top_n=2)
    )
    logger.debug("容量 3->1 热更新后立即淘汰 -> 保留=%s",
                 [r.trace_id for r in coll.snapshot()])
    assert [r.trace_id for r in coll.snapshot()] == ["t-2"]
    assert coll.detail_evicted == 2


# ---- 窗口口径与采样解耦 ------------------------------------------------

def test_window_is_full_census_even_at_zero_sample_rate(
    bounded_collector: tuple[TelemetryCollector, FakeClock]
) -> None:
    coll, clock = bounded_collector
    coll.update_config(TelemetryConfig(
        sample_rate=0.0, detail_max_records=3, window_seconds=100.0,
        slow_top_n=2))
    for i in range(20):
        clock.advance(0.5)
        rec = _rec(
            f"t-{i}", duration_ms=100.0 if i == 19 else 5.0,
            slow=(i == 19),
            outcome=(Outcome.FRAMEWORK_ERROR if i == 7 else Outcome.SUCCESS),
            category=(FailureCategory.FRAMEWORK if i == 7
                      else FailureCategory.NONE),
            status_code=(500 if i == 7 else 200),
        )
        coll.record(rec)
    stats = coll.window_stats()["http:GET /"]
    report = coll.aggregate()
    logger.debug(
        "零采样写入 20 条(1错误1慢): 窗口 count=%s err=%s slow=%s p99=%s; "
        "census total=%s; 明细 kept=%s",
        stats.count, stats.error_count, stats.slow_count, stats.p99_ms,
        report.total_seen, report.total_kept,
    )
    # 判定依据：窗口=全量 20；明细只剩错误+慢（2 条），但窗口数不缩水。
    assert stats.count == 20
    assert stats.error_count == 1 and stats.slow_count == 1
    assert stats.p99_ms == 100.0
    assert report.total_seen == 20
    assert report.total_kept == 2


def test_disabled_collector_writes_nothing_to_window(
    bounded_collector: tuple[TelemetryCollector, FakeClock]
) -> None:
    coll, clock = bounded_collector
    coll.disable()
    coll.record(_rec("x"))
    logger.debug("关闭采集: window=%s census_total=%s",
                 coll.window_stats(), coll.aggregate().total_seen)
    assert coll.window_stats() == {}
    assert coll.aggregate().total_seen == 0


def test_window_scroll_removes_old_samples(
    bounded_collector: tuple[TelemetryCollector, FakeClock]
) -> None:
    coll, clock = bounded_collector
    coll.record(_rec("old-1", duration_ms=10.0))
    coll.record(_rec("old-2", duration_ms=20.0))
    clock.advance(50.0)
    coll.record(_rec("new-1", duration_ms=30.0))
    before = coll.window_stats()["http:GET /"]
    clock.advance(60.0)  # 越过 100s 窗口，旧两条滑出
    after = coll.window_stats()["http:GET /"]
    logger.debug("滚动: 窗口内 count %s -> %s（旧样本滑出）",
                 before.count, after.count)
    assert before.count == 3
    assert after.count == 1
    assert after.p50_ms == 30.0
    # census 不随窗口滑出而变化。
    assert coll.aggregate().census["http:GET /"].total == 3


# ---- 慢请求归因查询 ----------------------------------------------------

def test_slowest_attributions_sorted_and_consistent(
    bounded_collector: tuple[TelemetryCollector, FakeClock]
) -> None:
    coll, clock = bounded_collector
    # 该用例要保留全部明细以纯粹验证排序/Top N：放宽到容量 10。
    coll.update_config(TelemetryConfig(
        sample_rate=1.0, slow_threshold_ms=50.0, window_seconds=100.0,
        detail_max_records=10, slow_top_n=2))
    spans = (_span_root("db", 40.0),)
    data = [("a", 600.0, True), ("b", 900.0, True), ("c", 300.0, True),
            ("ok", 5.0, False)]
    for trace_id, dur, slow in data:
        clock.advance(1.0)
        coll.record(_rec(trace_id, duration_ms=dur, slow=slow,
                         spans=spans if slow else ()))
    pairs = coll.slowest_attributions()
    durations = [rec.duration_ms for rec, _ in pairs]
    logger.debug("Top N(默认 slow_top_n=2) 最慢请求 -> %s", durations)
    # 判定依据：按耗时降序，只返回 slow=True，受 slow_top_n=2 截断。
    assert durations == [900.0, 600.0]
    for rec, attr in pairs:
        assert rec.slow
        assert attr.is_consistent(), attr
        assert build_attribution(rec).trace_id == attr.trace_id


def test_slowest_attributions_limit_and_entry_filter(
    bounded_collector: tuple[TelemetryCollector, FakeClock]
) -> None:
    coll, clock = bounded_collector
    coll.record(_rec("x1", duration_ms=100.0, slow=True,
                     entry="http:GET /a"))
    coll.record(_rec("x2", duration_ms=200.0, slow=True,
                     entry="http:GET /b"))
    only_b = coll.slowest_attributions(entry="http:GET /b")
    explicit_limit = coll.slowest_attributions(limit=1)
    logger.debug("entry 过滤 -> %s; limit=1 -> %s",
                 [r.trace_id for r, _ in only_b],
                 [r.trace_id for r, _ in explicit_limit])
    assert [r.trace_id for r, _ in only_b] == ["x2"]
    assert [r.trace_id for r, _ in explicit_limit] == ["x2"]
    # limit 边界：0/负数返回空列表（确定结果，绝不抛错）。
    assert coll.slowest_attributions(limit=0) == []
    assert coll.slowest_attributions(limit=-3) == []


def test_window_matches_census_at_high_volume() -> None:
    """窗口不设条数上限：高流量下窗口各项与全量累计严格一致。"""

    clock = FakeClock()
    coll = TelemetryCollector(
        TelemetryConfig(
            enabled=True, sample_rate=0.0, slow_threshold_ms=1e9,
            window_seconds=1e9,
            detail_max_records=10, slow_top_n=5,
        ),
        clock=clock,
    )
    total = 12_000  # 超过旧实现的默认窗口样本上限 10000
    for i in range(total):
        clock.advance(0.001)
        coll.record(_rec(f"t-{i}", duration_ms=10.0 * (i + 1)))
    window = coll.window_stats()["http:GET /"]
    meta = coll.window_meta()
    census_total = coll.aggregate().census["http:GET /"].total
    logger.debug("窗口内写入 %s 条 -> 窗口count=%s added=%s expired=%s; "
                 "census=%s（零采样明细=%s）",
                 total, window.count, meta["added"], meta["expired"],
                 census_total, coll.aggregate().total_kept)
    # 判定依据：窗口是全量口径，一条不丢，与 census 完全对得上。
    assert window.count == total
    assert meta["added"] == total and meta["expired"] == 0
    assert census_total == total
    assert coll.aggregate().total_kept == 0


def _span_root(name: str, duration: float) -> SpanRecord:
    return SpanRecord(
        trace_id="", span_id=f"span-{name}", parent_span_id=None,
        name=name, duration_ms=duration, outcome=Outcome.SUCCESS,
        failure_category=FailureCategory.NONE, reason="",
    )
