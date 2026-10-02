"""明细保留边界：条数上限（FIFO）+ 时长上限，确定性淘汰。"""

from __future__ import annotations

import logging

import pytest

from telemetry.config import TelemetryConfig
from telemetry.retention import RetentionPolicy, eviction_cut

logger = logging.getLogger("telemetry.tests")


def test_capacity_fifo() -> None:
    """容量 3、5 条记录 -> 最旧 2 条淘汰，保留恒为后缀。"""

    ts = [1.0, 2.0, 3.0, 4.0, 5.0]
    cut = eviction_cut(ts, 5.0, RetentionPolicy(max_records=3, max_age_ms=0))
    logger.debug("输入 ts=%s cap=3 -> cut=%s（保留 %s）", ts, cut, ts[cut:])
    assert cut == 2 and ts[cut:] == [3.0, 4.0, 5.0]


def test_age_boundary_inclusive() -> None:
    """边界值保留：ts >= now-max_age 不淘汰；早一刻淘汰。"""

    ts = [1.0, 2.0, 10.0]
    p = RetentionPolicy(max_records=100, max_age_ms=10.0)
    cut_edge = eviction_cut(ts, 11.0, p)
    cut_after = eviction_cut(ts, 11.0 + 1e-9, p)
    logger.debug(
        "now=11(edge=1,边界保留) cut=%s；now=11+eps cut=%s", cut_edge, cut_after
    )
    assert cut_edge == 0
    assert cut_after == 1 and ts[cut_after:] == [2.0, 10.0]


def test_capacity_then_age() -> None:
    """容量先切，再继续跳过过期最旧记录，结果仍是后缀。"""

    ts = [1.0, 2.0, 5.0, 9.0, 10.0]
    cut = eviction_cut(ts, 20.0, RetentionPolicy(max_records=3, max_age_ms=10.0))
    logger.debug("ts=%s now=20 cap=3 age=10 -> cut=%s 保留=%s", ts, cut, ts[cut:])
    assert cut == 4 and ts[cut:] == [10.0]


def test_zero_capacity_keeps_nothing() -> None:
    """max_records=0：一条明细也不留（全量计数仍在采集器侧保留）。"""

    ts = [1.0, 2.0]
    cut = eviction_cut(ts, 2.0, RetentionPolicy(max_records=0, max_age_ms=0))
    logger.debug("cap=0 -> cut=%s（期望 %s）", cut, len(ts))
    assert cut == len(ts)


@pytest.mark.parametrize(
    "kwargs",
    [{"max_records": -1}, {"max_records": 1.0}, {"max_age_ms": -1.0}],
)
def test_invalid_policy_rejected(kwargs: dict) -> None:
    logger.debug("非法策略输入=%s -> 期望 ValueError/TypeError", kwargs)
    with pytest.raises((ValueError, TypeError)):
        RetentionPolicy(**kwargs)  # type: ignore[arg-type]


def test_collector_capacity_keeps_census_intact(collector) -> None:
    """采集器内：明细被容量淘汰后，census 全量计数与窗口不丢不重。"""

    from telemetry.classification import FailureCategory, Outcome
    from telemetry.recorder import RequestRecord

    coll = collector
    coll.update_config(TelemetryConfig(max_records=3, max_age_ms=0.0,
                                       slow_threshold_ms=50.0))
    for i in range(10):
        coll.record(
            RequestRecord(
                trace_id=f"t{i}", entry="e", method="GET", path="/",
                status_code=200, duration_ms=float(i + 1),
                outcome=Outcome.SUCCESS, failure_category=FailureCategory.NONE,
                reason="", slow=False, kept=False,
            )
        )
    kept = coll.snapshot()
    census = coll.aggregate()
    logger.debug(
        "写入10 cap=3 -> 保留明细=%s（应为3且为最新3条），census.total=%s，淘汰=%s",
        [r.trace_id for r in kept], census.total_seen, coll.evicted_details,
    )
    assert len(kept) == 3
    assert [r.trace_id for r in kept] == ["t7", "t8", "t9"]
    assert census.total_seen == 10 and census.census["e"].total == 10
    assert coll.evicted_details == 7


def test_evicted_trace_replay_does_not_double_count(collector) -> None:
    """淘汰不删去重集合：已淘汰 trace 重放不会让 census 重复计数。"""

    from telemetry.classification import FailureCategory, Outcome
    from telemetry.recorder import RequestRecord

    coll = collector
    coll.update_config(TelemetryConfig(max_records=1, max_age_ms=0.0,
                                       slow_threshold_ms=50.0))

    def make(i: int) -> RequestRecord:
        return RequestRecord(
            trace_id="same", entry="e", method="GET", path="/",
            status_code=200, duration_ms=1.0, outcome=Outcome.SUCCESS,
            failure_category=FailureCategory.NONE, reason="", slow=False,
            kept=False,
        )

    for i in range(3):
        coll.record(make(i))
    total = coll.aggregate().total_seen
    logger.debug("同一 trace 连续 record 3 次 -> census.total_seen=%s（期望1）", total)
    assert total == 1
    # 同一 trace 只保留一次，不会重复追加 -> 没有新的淘汰发生。
    assert len(coll.snapshot()) == 1 and coll.evicted_details == 0
