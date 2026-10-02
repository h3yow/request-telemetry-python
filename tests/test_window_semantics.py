"""采集器层面的窗口口径、采样对账、时长淘汰与慢请求归因查询。"""

from __future__ import annotations

import logging

import pytest

from telemetry.classification import FailureCategory, Outcome
from telemetry.collector import TelemetryCollector
from telemetry.config import TelemetryConfig
from telemetry.recorder import RequestRecord

logger = logging.getLogger("telemetry.tests")


class FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.t = start

    def __call__(self) -> float:
        return self.t


def _rec(trace: str, ms: float, error: bool = False, slow: bool = False,
         entry: str = "e") -> RequestRecord:
    return RequestRecord(
        trace_id=trace, entry=entry, method="GET", path="/",
        status_code=500 if error else 200, duration_ms=ms,
        outcome=Outcome.FRAMEWORK_ERROR if error else Outcome.SUCCESS,
        failure_category=FailureCategory.FRAMEWORK if error
        else FailureCategory.NONE,
        reason="boom" if error else "", slow=slow, kept=False,
    )


def test_window_counts_are_full_census_even_under_sampling() -> None:
    """采样率 0 时普通请求不留明细、没有分位样本，但窗口计数仍为全量。"""

    clk = FakeClock()
    coll = TelemetryCollector(
        TelemetryConfig(enabled=True, sample_rate=0.0, slow_threshold_ms=50.0,
                        window_ms=10_000.0),
        clock_ms=clk,
    )
    for i in range(10):
        coll.record(_rec(f"t{i}", float(i + 1)))
    s = coll.window_stats()["e"]
    agg = coll.aggregate()
    logger.debug(
        "rate=0 写10 -> 窗口total=%s 样本数=%s total_seen=%s total_kept=%s",
        s.total, s.quantile_sample_size, agg.total_seen, agg.total_kept,
    )
    assert s.total == 10
    assert s.quantile_sample_size == 0 and s.p95_ms is None
    assert agg.total_seen == 10 and agg.total_kept == 0


def test_error_and_slow_feed_window_quantiles_under_sampling() -> None:
    """错误/慢请求强制保留：它们既进全量计数也进分位样本，可对账。"""

    clk = FakeClock()
    coll = TelemetryCollector(
        TelemetryConfig(enabled=True, sample_rate=0.0, slow_threshold_ms=50.0,
                        window_ms=10_000.0),
        clock_ms=clk,
    )
    for i in range(10):
        coll.record(_rec(f"t{i}", 10.0))            # 普通 -> 不保留
    coll.record(_rec("err", 20.0, error=True))      # 错误 -> 保留
    coll.record(_rec("slow", 80.0, slow=True))      # 慢 -> 保留
    s = coll.window_stats()["e"]
    logger.debug(
        "10普通+1错误+1慢 -> total=%s errors=%s slow=%s 样本数=%s p50=%s",
        s.total, s.error_count, s.slow_count, s.quantile_sample_size, s.p50_ms,
    )
    assert (s.total, s.error_count, s.slow_count) == (12, 1, 1)
    assert s.quantile_sample_size == 2
    assert s.p50_ms == 50.0  # 样本 [20, 80] 的中位数


def test_window_matches_census_inside_window_and_rolls() -> None:
    """窗口内全量计数 == 窗口内写入量；时钟推进后旧请求退出窗口。"""

    clk = FakeClock(start=0.0)
    coll = TelemetryCollector(
        TelemetryConfig(window_ms=100.0, slow_threshold_ms=50.0,
                        max_records=1_000_000),
        clock_ms=clk,
    )
    for i in range(5):
        clk.t = float(i) * 30  # 0,30,60,90,120
        coll.record(_rec(f"t{i}", 1.0, error=(i == 0), slow=(i == 4)))
    s120 = coll.window_stats()["e"]
    clk.t = 130.0
    s130 = coll.window_stats()["e"]
    logger.debug(
        "now=120 窗口[20,120] total=%s errors=%s slow=%s；now=130 total=%s",
        s120.total, s120.error_count, s120.slow_count, s130.total,
    )
    # 窗口 (20,120]：ts=0 已出窗；其余 4 条
    assert s120.total == 4 and s120.error_count == 0 and s120.slow_count == 1
    # now=130 edge=30：ts=30 为边界值保留，仍只少 ts=0 -> 4 条
    assert s130.total == 4
    # 再推进到 130.1：edge=30.1，ts=30 滑出 -> 3 条
    clk.t = 130.1
    assert coll.window_stats()["e"].total == 3
    # census 不受窗口滚动影响，仍是全量
    assert coll.aggregate().total_seen == 5


def test_age_eviction_drops_old_details_only() -> None:
    """超 max_age 的明细被淘汰；census/窗口事件不受影响。"""

    clk = FakeClock(start=0.0)
    coll = TelemetryCollector(
        TelemetryConfig(window_ms=1_000.0, slow_threshold_ms=50.0,
                        max_records=100, max_age_ms=100.0),
        clock_ms=clk,
    )
    clk.t = 0.0
    coll.record(_rec("old", 1.0))
    clk.t = 50.0
    coll.record(_rec("mid", 2.0))
    clk.t = 100.0
    # 新写入触发淘汰：edge=0，ts=0 边界保留
    coll.record(_rec("edge", 3.0))
    kept_edge = [r.trace_id for r in coll.snapshot()]
    clk.t = 100.0 + 1e-6
    coll.record(_rec("new", 4.0))  # edge≈0 -> ts=0 过期
    kept_after = [r.trace_id for r in coll.snapshot()]
    census = coll.aggregate().total_seen
    logger.debug("边界保留=%s；再推进后保留=%s；census=%s",
                 kept_edge, kept_after, census)
    assert kept_edge == ["old", "mid", "edge"]
    assert kept_after == ["mid", "edge", "new"]
    assert coll.evicted_details == 1 and census == 4
    assert coll.window_stats()["e"].total == 4  # 窗口比保留期长，事件仍在


def test_slowest_and_attribute_trace() -> None:
    """最慢一批按耗时降序；按 trace 可查归因，被淘汰的 trace 查不到。"""

    clk = FakeClock()
    coll = TelemetryCollector(
        TelemetryConfig(slow_threshold_ms=5.0, max_records=3, max_age_ms=0.0),
        clock_ms=clk,
    )
    for i, ms in enumerate([10.0, 50.0, 30.0, 90.0, 5.0]):
        coll.record(_rec(f"t{i}", ms))
    top = coll.slowest(2)
    attrs = coll.slowest_attributions(2)
    logger.debug("5条 cap=3 保留t2,t3,t4 最慢两条=%s（期望 [t3=90,t2=30]）",
                 [(r.trace_id, r.duration_ms) for r in top])
    assert [r.trace_id for r in top] == ["t3", "t2"]
    assert all(
        abs(a.segments.accounted_ms - a.duration_ms) < 1e-6 for a in attrs
    )
    found = coll.attribute_trace("t3")
    assert found is not None
    assert coll.attribute_trace("t0") is None  # 已淘汰
    assert coll.attribute_trace("t1") is None  # 被容量淘汰


def test_disabled_collector_feeds_nothing() -> None:
    """整体关闭：无计数、无窗口事件、无明细、无分位。"""

    clk = FakeClock()
    coll = TelemetryCollector(
        TelemetryConfig(enabled=False, window_ms=10_000.0), clock_ms=clk
    )
    for i in range(3):
        assert coll.record(_rec(f"t{i}", 1.0)) is False
    stats = coll.window_stats()
    logger.debug("关闭后 窗口入口=%s total_seen=%s kept=%s",
                 stats, coll.aggregate().total_seen, coll.aggregate().total_kept)
    assert stats == {}
    assert coll.aggregate().total_seen == 0


def test_config_toggle_preserves_window_and_retention_settings() -> None:
    """运行期 enable/disable 不重置窗口长度与容量配置。"""

    coll = TelemetryCollector(
        TelemetryConfig(window_ms=1234.0, max_records=7, max_age_ms=42.0)
    )
    coll.disable()
    cfg = coll.config
    logger.debug("disable 后 window=%s max_records=%s max_age=%s enabled=%s",
                 cfg.window_ms, cfg.max_records, cfg.max_age_ms, cfg.enabled)
    assert (cfg.enabled, cfg.window_ms, cfg.max_records, cfg.max_age_ms) == (
        False, 1234.0, 7, 42.0,
    )
    coll.enable()
    assert coll.config.enabled is True
    assert coll.config.window_ms == 1234.0
