"""采集器测试：采样、强制保留、去重、聚合与开关。"""

from __future__ import annotations

import logging

from telemetry.classification import FailureCategory, Outcome
from telemetry.collector import TelemetryCollector, _stable_unit_interval
from telemetry.config import TelemetryConfig
from telemetry.recorder import RequestRecord

logger = logging.getLogger("telemetry.tests")


def _rec(
    trace_id: str,
    *,
    entry: str = "http:GET /",
    outcome: Outcome = Outcome.SUCCESS,
    category: FailureCategory = FailureCategory.NONE,
    slow: bool = False,
    duration_ms: float = 10.0,
    status_code: int = 200,
    reason: str = "",
) -> RequestRecord:
    return RequestRecord(
        trace_id=trace_id,
        entry=entry,
        method="GET",
        path="/",
        status_code=status_code,
        duration_ms=duration_ms,
        outcome=outcome,
        failure_category=category,
        reason=reason,
        slow=slow,
        kept=False,
    )


def test_stable_hash_is_deterministic_and_bounded() -> None:
    values = [_stable_unit_interval(f"trace-{i}") for i in range(1000)]
    assert all(0.0 <= v < 1.0 for v in values)
    # 同一输入跨调用结论一致。
    assert _stable_unit_interval("trace-42") == _stable_unit_interval("trace-42")
    logger.debug("输入 trace-42 -> 采样分桶值=%s", _stable_unit_interval("trace-42"))


def test_full_rate_keeps_everything(collector: TelemetryCollector) -> None:
    for i in range(50):
        kept = collector.record(_rec(f"t-{i}"))
        assert kept is True
    rep = collector.aggregate()
    logger.debug("输入 50 条成功请求, 采样率=1 -> seen=%s kept=%s",
                 rep.total_seen, rep.total_kept)
    assert rep.total_seen == 50
    assert rep.total_kept == 50


def test_zero_rate_still_keeps_errors_and_slow(collector: TelemetryCollector) -> None:
    collector.update_config(TelemetryConfig(sample_rate=0.0))
    for i in range(100):
        rec = _rec(
            f"t-{i}",
            outcome=(
                Outcome.FRAMEWORK_ERROR if i % 10 == 0 else Outcome.SUCCESS
            ),
            category=(
                FailureCategory.FRAMEWORK if i % 10 == 0 else FailureCategory.NONE
            ),
            status_code=500 if i % 10 == 0 else 200,
            reason=("boom" if i % 10 == 0 else ""),
            slow=(i % 20 == 5),
            duration_ms=900 if i % 20 == 5 else 5,
        )
        kept = collector.record(rec)
        if i % 10 == 0 or i % 20 == 5:
            assert kept is True
        else:
            assert kept is False
    rep = collector.aggregate()
    logger.debug(
        "采样率=0: 全量 total_seen=%s, 明细 total_kept=%s (10 错误 + 5 慢，无重叠)",
        rep.total_seen,
        rep.total_kept,
    )
    # 全量口径不受采样影响。
    assert rep.total_seen == 100
    assert rep.census["http:GET /"].total == 100
    assert rep.census["http:GET /"].error_count == 10
    assert rep.census["http:GET /"].slow_count == 5
    # 明细仅保留错误与慢请求。
    assert rep.total_kept == 15


def test_duplicate_trace_counted_once(collector: TelemetryCollector) -> None:
    rec = _rec("dup-1")
    collector.record(rec)
    collector.record(rec)
    collector.record(rec)
    rep = collector.aggregate()
    logger.debug("同一 trace_id 重复提交 3 次 -> total_seen=%s", rep.total_seen)
    assert rep.total_seen == 1
    # 明细列表也不应重复（同一 trace 只保留一条）。
    assert len(collector.snapshot()) == 1


def test_partial_rate_deterministic_for_trace(collector: TelemetryCollector) -> None:
    collector.update_config(TelemetryConfig(sample_rate=0.3))
    decisions = {
        f"stable-{i}": collector.should_sample(f"stable-{i}", False, False)
        for i in range(200)
    }
    # 同一 trace 多次判定结论恒定。
    for trace_id, first in decisions.items():
        assert collector.should_sample(trace_id, False, False) is first
    kept_ratio = sum(decisions.values()) / len(decisions)
    logger.debug("采样率=0.3 经验保留比例=%.3f（应接近 0.3）", kept_ratio)
    assert 0.15 < kept_ratio < 0.45


def test_aggregation_grouped_by_entry_outcome_failure(collector: TelemetryCollector) -> None:
    collector.record(_rec("a1", entry="http:GET /a"))
    collector.record(
        _rec(
            "a2",
            entry="http:GET /a",
            outcome=Outcome.BUSINESS_ERROR,
            category=FailureCategory.BUSINESS,
            status_code=400,
            reason="BusinessError: x",
        )
    )
    collector.record(
        _rec(
            "b1",
            entry="http:POST /b",
            outcome=Outcome.FRAMEWORK_ERROR,
            category=FailureCategory.FRAMEWORK,
            status_code=500,
            reason="RuntimeError: y",
        )
    )
    census = collector.aggregate().census
    logger.debug(
        "按入口聚合: /a=%s /b=%s",
        census["http:GET /a"].by_outcome,
        census["http:POST /b"].by_outcome,
    )
    assert census["http:GET /a"].total == 2
    assert census["http:GET /a"].by_outcome == {
        "success": 1,
        "business_error": 1,
    }
    assert census["http:GET /a"].by_failure == {"none": 1, "business": 1}
    assert census["http:POST /b"].by_failure == {"framework": 1}


def test_disable_collector_records_nothing(collector: TelemetryCollector) -> None:
    collector.disable()
    assert collector.enabled() is False
    assert collector.record(_rec("x")) is False
    assert collector.aggregate().total_seen == 0
    logger.debug("关闭采集 -> seen=0 kept=0 enabled=%s", collector.enabled())
    collector.enable()
    assert collector.record(_rec("y")) is True
    assert collector.aggregate().total_seen == 1


def test_record_isolates_internal_errors(collector: TelemetryCollector) -> None:
    # 传入非法对象触发内部异常，record 必须吞掉而不外抛。
    result = collector.record(object())  # type: ignore[arg-type]
    assert result is False
    assert collector.internal_failures >= 1
    # 采集器仍可正常工作。
    assert collector.record(_rec("after")) is True
    logger.debug("内部异常被隔离, failures=%s, 后续请求仍计数",
                 collector.internal_failures)


def test_snapshot_is_independent_copy(collector: TelemetryCollector) -> None:
    collector.record(_rec("s1"))
    snap1 = collector.snapshot()
    collector.record(_rec("s2"))
    assert len(snap1) == 1 and len(collector.snapshot()) == 2
