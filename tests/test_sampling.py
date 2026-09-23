"""采样与开关的口径一致性测试。

核心不变量：

1. 无论采样率如何，**全量口径(census)** 每个请求恰好计数一次；
2. 错误请求与慢请求在任意采样率下都保留明细；
3. 同一 trace_id 的采样判定恒定；
4. 关闭采集 → 无记录；重新开启 → 恢复计数；
5. 采样明细(sampled)的 outcome/failure 计数是全量的子集，且错误/慢数与全量相等。
"""

from __future__ import annotations

import logging

from fastapi.testclient import TestClient

from telemetry.classification import FailureCategory
from telemetry.collector import TelemetryCollector
from telemetry.config import TelemetryConfig

logger = logging.getLogger("telemetry.tests")


def _drive_mixed_load(client: TestClient, rounds: int = 3) -> dict[str, int]:
    """打固定配比的混合请求，返回本轮各类路径命中次数。"""

    for _ in range(rounds):
        client.get("/ping")
        client.get("/ping")
        client.get("/business-error")
        client.get("/framework-error")
        client.get("/slow?ms=60")
    return {"ping": 2 * rounds, "business": rounds, "framework": rounds, "slow": rounds}


def test_census_consistent_across_rates(client: TestClient, collector: TelemetryCollector) -> None:
    baseline_census = None
    for rate in (1.0, 0.5, 0.1, 0.0):
        collector.reset()
        collector.update_config(
            TelemetryConfig(enabled=True, sample_rate=rate, slow_threshold_ms=50.0)
        )
        load = _drive_mixed_load(client, rounds=5)
        rep = collector.aggregate()
        census = rep.census
        totals = {
            entry: (agg.total, agg.error_count, agg.slow_count, dict(agg.by_outcome))
            for entry, agg in census.items()
        }
        logger.debug("采样率=%s 全量口径=%s seen=%s kept=%s",
                     rate, {k: v[0] for k, v in totals.items()},
                     rep.total_seen, rep.total_kept)
        if baseline_census is None:
            baseline_census = totals
        else:
            # 全量口径在不同采样率下逐项完全一致。
            assert totals == baseline_census, (rate, totals, baseline_census)

    total_requests = sum(load.values())
    assert rep.total_seen == total_requests
    # 采样率 0 时，明细仍必须包含全部错误(business+framework=10)与慢(5)。
    assert rep.total_kept == load["business"] + load["framework"] + load["slow"]
    logger.debug("采样率=0 时强制保留: %s 条（错误+慢）", rep.total_kept)


def test_error_and_slow_always_kept_under_low_rate(
    client: TestClient, collector: TelemetryCollector
) -> None:
    collector.update_config(TelemetryConfig(sample_rate=0.0, slow_threshold_ms=50.0))
    _drive_mixed_load(client, rounds=4)
    kept = collector.snapshot()
    kept_paths = sorted(r.path for r in kept)
    logger.debug("采样率=0 保留明细路径=%s", kept_paths)
    # 全部保留项必须是错误或慢请求，绝不允许出现普通成功请求。
    for rec in kept:
        assert (
            rec.failure_category is not FailureCategory.NONE or rec.slow
        ), f"{rec.path} 不应被保留"
    # 每条错误/慢路径各 4 次。
    assert kept_paths.count("/business-error") == 4
    assert kept_paths.count("/framework-error") == 4
    assert kept_paths.count("/slow") == 4
    assert kept_paths.count("/ping") == 0


def test_sampled_subset_reconciles_with_census(
    client: TestClient, collector: TelemetryCollector
) -> None:
    collector.update_config(TelemetryConfig(sample_rate=0.3, slow_threshold_ms=50.0))
    _drive_mixed_load(client, rounds=10)
    rep = collector.aggregate()
    census = rep.census
    sampled = rep.sampled
    for entry, cagg in census.items():
        sagg = sampled.get(entry)
        # 明细每个 outcome 计数不得超过全量；总数不超过全量。
        if sagg is None:
            continue
        for outcome, count in sagg.by_outcome.items():
            assert count <= cagg.by_outcome.get(outcome, 0)
        assert sagg.total <= cagg.total
        logger.debug("入口=%s 全量=%s 明细=%s 全量outcome=%s 明细outcome=%s",
                     entry, cagg.total, sagg.total,
                     cagg.by_outcome, sagg.by_outcome)
    # 错误与慢请求：明细数必须等于全量数（强制保留）。
    for entry in census:
        assert sampled[entry].error_count == census[entry].error_count
        assert sampled[entry].slow_count == census[entry].slow_count


def test_toggle_switch_changes_collection(
    client: TestClient, collector: TelemetryCollector
) -> None:
    client.get("/ping")
    assert collector.aggregate().total_seen == 1
    collector.disable()
    for _ in range(5):
        client.get("/ping")
    assert collector.aggregate().total_seen == 1
    logger.debug("关闭后再发 5 次请求，seen 仍=1")
    collector.enable()
    client.get("/ping")
    assert collector.aggregate().total_seen == 2
    logger.debug("重新开启后 seen=2，开关行为确定")
