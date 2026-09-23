"""采集器：线程安全地计数、保留明细并聚合。

并发正确性：

- 所有可变状态（全量计数器、保留明细列表、trace 去重集合）只在一把
  :class:`threading.RLock` 下变更，"判定是否保留 + 计数 + 追加" 是同一个
  临界区，因此在多线程/多协程交错调用下不会出现计数丢失、重复或错配；
- 采样判定基于 ``trace_id`` 的稳定哈希（``hash()`` 会受 ``PYTHONHASHSEED``
  影响，故使用自实现的 FNV-1a），同一 trace 永远得到同一判定，且不同进程
  结论一致；
- 错误请求与慢请求（``is_error`` / ``is_slow``）无条件保留；
- 全量计数（census）与是否采样完全无关 —— 每个请求必被计数一次。
"""

from __future__ import annotations

import threading
from collections import defaultdict

from .config import TelemetryConfig
from .recorder import AggregationReport, EntryAggregation, RequestRecord


def _stable_unit_interval(key: str) -> float:
    """把字符串键映射到 [0, 1) 的确定值。

    先做 FNV-1a 64 位累加，再用 MurmurHash3 的 fmix64 雪崩混合打散
    —— 直接取 FNV 高位会让 ``stable-0..N`` 这类相近短字符串分布成块、
    采样率失真。纯整数运算，跨进程/跨机器结论一致。
    """

    h = 0xCBF29CE484222325
    for ch in key.encode("utf-8"):
        h ^= ch
        h = (h * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    h ^= h >> 33
    h = (h * 0xFF51AFD7ED558CCD) & 0xFFFFFFFFFFFFFFFF
    h ^= h >> 33
    h = (h * 0xC4CEB9FE1A85EC53) & 0xFFFFFFFFFFFFFFFF
    h ^= h >> 33
    return h / float(1 << 64)


class TelemetryCollector:
    """进程内采集器（不依赖任何外部服务）。"""

    def __init__(self, config: TelemetryConfig | None = None) -> None:
        self._lock = threading.RLock()
        self._records: list[RequestRecord] = []
        self._seen_trace_ids: set[str] = set()
        self._kept_trace_ids: set[str] = set()
        self._census: dict[str, dict[str, object]] = defaultdict(
            lambda: {
                "total": 0,
                "slow": 0,
                "errors": 0,
                "outcome": defaultdict(int),
                "failure": defaultdict(int),
            }
        )
        self._config: TelemetryConfig = config or TelemetryConfig()
        self._total_seen = 0
        self._record_calls_failed = 0

    # ---- 配置与开关 -------------------------------------------------

    @property
    def config(self) -> TelemetryConfig:
        with self._lock:
            return self._config

    def update_config(self, config: TelemetryConfig) -> None:
        """热更新配置（如运行期关闭采集）。"""

        with self._lock:
            self._config = config

    def enable(self) -> None:
        self.update_config(
            TelemetryConfig(
                enabled=True,
                sample_rate=self._config.sample_rate,
                slow_threshold_ms=self._config.slow_threshold_ms,
            )
        )

    def disable(self) -> None:
        self.update_config(
            TelemetryConfig(
                enabled=False,
                sample_rate=self._config.sample_rate,
                slow_threshold_ms=self._config.slow_threshold_ms,
            )
        )

    def enabled(self) -> bool:
        with self._lock:
            return self._config.enabled

    # ---- 采样判定 ---------------------------------------------------

    def should_sample(
        self, trace_id: str, is_error: bool, is_slow: bool
    ) -> bool:
        """返回该请求是否保留明细。

        规则（优先级从高到低）：错误必留、慢请求必留、关闭时不留、
        其余按 ``sample_rate`` 对 trace_id 做确定性抽样。
        """

        with self._lock:
            config = self._config
            if not config.enabled:
                return False
            if is_error or is_slow:
                return True
            rate = config.normalized_sample_rate
            if rate <= 0.0:
                return False
            if rate >= 1.0:
                return True
            return _stable_unit_interval(trace_id) < rate

    # ---- 写入 -------------------------------------------------------

    def record(self, rec: RequestRecord) -> bool:
        """原子地登记一次请求。

        - 全量计数始终 +1（去重：同一 ``trace_id`` 只计一次，防止重复）；
        - 返回该记录是否被保留；
        - 任何意外异常都被隔离，只累加内部失败计数，不波及请求主链路。
        """

        try:
            with self._lock:
                if not self._config.enabled:
                    return False

                is_new = rec.trace_id not in self._seen_trace_ids
                if is_new:
                    self._seen_trace_ids.add(rec.trace_id)
                    self._total_seen += 1
                    bucket = self._census[rec.entry]
                    bucket["total"] = int(bucket["total"]) + 1
                    outcome_counts = bucket["outcome"]  # type: ignore[assignment]
                    failure_counts = bucket["failure"]  # type: ignore[assignment]
                    outcome_counts[rec.outcome.value] += 1
                    failure_counts[rec.failure_category.value] += 1
                    if rec.slow:
                        bucket["slow"] = int(bucket["slow"]) + 1
                    if rec.failure_category.value != "none":
                        bucket["errors"] = int(bucket["errors"]) + 1

                kept = self.should_sample(
                    rec.trace_id,
                    is_error=rec.failure_category.value != "none",
                    is_slow=rec.slow,
                )
                if kept and rec.trace_id not in self._kept_trace_ids:
                    self._kept_trace_ids.add(rec.trace_id)
                    stored = rec if rec.kept else self._mark_kept(rec)
                    self._records.append(stored)
                return kept
        except Exception:  # noqa: BLE001 - 采集绝不能拖垮业务
            self._record_calls_failed += 1
            return False

    @staticmethod
    def _mark_kept(rec: RequestRecord) -> RequestRecord:
        return RequestRecord(
            trace_id=rec.trace_id,
            entry=rec.entry,
            method=rec.method,
            path=rec.path,
            status_code=rec.status_code,
            duration_ms=rec.duration_ms,
            outcome=rec.outcome,
            failure_category=rec.failure_category,
            reason=rec.reason,
            slow=rec.slow,
            kept=True,
            spans=rec.spans,
        )

    # ---- 读取 -------------------------------------------------------

    def snapshot(self) -> list[RequestRecord]:
        """返回当前保留明细的快照副本。"""

        with self._lock:
            return list(self._records)

    def census_snapshot(self) -> dict[str, dict[str, object]]:
        """返回全量计数的深拷贝。"""

        with self._lock:
            return {
                entry: {
                    "total": int(counts["total"]),
                    "slow": int(counts["slow"]),
                    "errors": int(counts["errors"]),
                    "outcome": dict(counts["outcome"]),  # type: ignore[arg-type]
                    "failure": dict(counts["failure"]),  # type: ignore[arg-type]
                }
                for entry, counts in self._census.items()
            }

    def aggregate(self) -> AggregationReport:
        """生成全量 + 采样两套口径的聚合报表。"""

        with self._lock:
            census = self._aggregate_census()
            sampled = self._aggregate_records(self._records)
            return AggregationReport(
                census=census,
                sampled=sampled,
                total_seen=self._total_seen,
                total_kept=len(self._records),
                sample_rate=self._config.normalized_sample_rate,
            )

    def _aggregate_census(self) -> dict[str, EntryAggregation]:
        result: dict[str, EntryAggregation] = {}
        for entry, counts in self._census.items():
            result[entry] = EntryAggregation(
                entry=entry,
                total=int(counts["total"]),
                by_outcome=dict(counts["outcome"]),  # type: ignore[arg-type]
                by_failure=dict(counts["failure"]),  # type: ignore[arg-type]
                slow_count=int(counts["slow"]),
                error_count=int(counts["errors"]),
                kept_count=0,
            )
        return result

    @staticmethod
    def _aggregate_records(
        records: list[RequestRecord],
    ) -> dict[str, EntryAggregation]:
        grouped: dict[str, list[RequestRecord]] = defaultdict(list)
        for rec in records:
            grouped[rec.entry].append(rec)

        report: dict[str, EntryAggregation] = {}
        for entry, recs in grouped.items():
            by_outcome: dict[str, int] = defaultdict(int)
            by_failure: dict[str, int] = defaultdict(int)
            total_duration = 0.0
            max_duration = 0.0
            slow = 0
            errors = 0
            for rec in recs:
                by_outcome[rec.outcome.value] += 1
                by_failure[rec.failure_category.value] += 1
                total_duration += rec.duration_ms
                max_duration = max(max_duration, rec.duration_ms)
                if rec.slow:
                    slow += 1
                if rec.failure_category.value != "none":
                    errors += 1
            report[entry] = EntryAggregation(
                entry=entry,
                total=len(recs),
                by_outcome=dict(by_outcome),
                by_failure=dict(by_failure),
                slow_count=slow,
                error_count=errors,
                kept_count=len(recs),
                total_duration_ms=total_duration,
                max_duration_ms=max_duration,
            )
        return report

    # ---- 测试/运维辅助 ----------------------------------------------

    def reset(self) -> None:
        """清空全部状态（主要供测试使用）。"""

        with self._lock:
            self._records.clear()
            self._seen_trace_ids.clear()
            self._kept_trace_ids.clear()
            self._census.clear()
            self._total_seen = 0
            self._record_calls_failed = 0

    @property
    def internal_failures(self) -> int:
        with self._lock:
            return self._record_calls_failed
