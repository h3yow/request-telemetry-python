"""采集器：线程安全地计数、保留明细、维护滑动窗口并聚合。

并发正确性：

- 所有可变状态（全量计数器、保留明细列表、trace 去重集合、滑动窗口）只在一把
  :class:`threading.RLock` 下变更，"判定是否保留 + 计数 + 追加 + 写窗口 +
  淘汰明细" 是同一个临界区，因此在多线程/多协程交错调用下不会出现计数丢失、
  重复、错配或窗口/明细口径互相打架；
- 采样判定基于 ``trace_id`` 的稳定哈希（``hash()`` 会受 ``PYTHONHASHSEED``
  影响，故使用自实现的 FNV-1a），同一 trace 永远得到同一判定，且不同进程
  结论一致；
- 错误请求与慢请求（``is_error`` / ``is_slow``）无条件保留；
- 全量计数（census）与是否采样完全无关 —— 每个请求必被计数一次；
- 明细淘汰（条数/时长上限）只删除 ``_records`` 中的明细，
  既不改 census，也不动滑动窗口内的全量事件流 —— 全量计数永不因淘汰丢失；
- 时钟通过 ``clock_ms`` 注入（默认 ``perf_counter`` 毫秒），
  窗口滚动与时长淘汰共用同一时钟，测试可换成可控时钟。
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict

from .attribution import RequestAttribution, attribute
from .config import TelemetryConfig
from .recorder import AggregationReport, EntryAggregation, RequestRecord
from .retention import RetentionPolicy, eviction_cut
from .window import SlidingWindowStore, WindowEntryStats


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

    def __init__(
        self,
        config: TelemetryConfig | None = None,
        clock_ms=None,
    ) -> None:
        self._lock = threading.RLock()
        self._records: list[RequestRecord] = []
        self._record_ts_ms: list[float] = []
        """与 ``_records`` 等长、同序：每条保留明细的登记时刻。"""

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
        self._evicted_details = 0
        self._clock_ms = clock_ms
        self._window = SlidingWindowStore(
            self._config.window_ms, clock_ms=self._now
        )

    def _now(self) -> float:
        if self._clock_ms is not None:
            return float(self._clock_ms())
        return time.perf_counter() * 1000.0

    # ---- 配置与开关 -------------------------------------------------

    @property
    def config(self) -> TelemetryConfig:
        with self._lock:
            return self._config

    def update_config(self, config: TelemetryConfig) -> None:
        """热更新配置（如运行期关闭采集）。

        窗口长度变化立即生效（后续滚动按新窗口计算）；容量变化在下次写入时
        触发淘汰。全量计数不受任何配置变化影响。
        """

        with self._lock:
            self._config = config
            self._window.set_window_ms(config.window_ms)

    def enable(self) -> None:
        self.update_config(self._config.with_enabled(True))

    def disable(self) -> None:
        self.update_config(self._config.with_enabled(False))

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

                now_ms = self._now()
                is_error = rec.failure_category.value != "none"
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
                    if is_error:
                        bucket["errors"] = int(bucket["errors"]) + 1

                kept = self.should_sample(rec.trace_id, is_error, rec.slow)
                if kept and rec.trace_id not in self._kept_trace_ids:
                    self._kept_trace_ids.add(rec.trace_id)
                    stored = rec if rec.kept else self._mark_kept(rec)
                    ts = stored.recorded_ms if stored.recorded_ms > 0 else now_ms
                    self._records.append(stored)
                    self._record_ts_ms.append(ts)
                    self._evict_locked(now_ms)

                # 窗口事件流对每个新请求恰好写一次（与 census 对齐）；
                # 耗时样本仅在该请求保留明细时附带，避免样本数超过事件数。
                if is_new:
                    self._window.add(
                        rec.entry,
                        duration_ms=rec.duration_ms if kept else None,
                        is_error=is_error,
                        is_slow=rec.slow,
                        ts_ms=now_ms,
                    )
                return kept
        except Exception:  # noqa: BLE001 - 采集绝不能拖垮业务
            self._record_calls_failed += 1
            return False

    def _evict_locked(self, now_ms: float) -> None:
        """按条数/时长上限淘汰最旧明细（调用方持锁）。

        只动 ``_records`` / ``_record_ts_ms``；淘汰的 trace_id 不从去重集合
        删除（否则同一 trace 重放会造成 census 重复计数），全量计数与窗口
        均不参与淘汰。
        """

        policy = RetentionPolicy(
            max_records=self._config.max_records,
            max_age_ms=self._config.max_age_ms,
        )
        cut = eviction_cut(self._record_ts_ms, now_ms, policy)
        if cut > 0:
            del self._records[:cut]
            del self._record_ts_ms[:cut]
            self._evicted_details += cut

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
            started_ms=rec.started_ms,
            business_started_ms=rec.business_started_ms,
            recorded_ms=rec.recorded_ms,
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
            self._record_ts_ms.clear()
            self._seen_trace_ids.clear()
            self._kept_trace_ids.clear()
            self._census.clear()
            self._window.reset()
            self._total_seen = 0
            self._record_calls_failed = 0
            self._evicted_details = 0

    # ---- 滑动窗口 / 慢请求归因 --------------------------------------

    def window_stats(
        self, entry: str | None = None
    ) -> dict[str, WindowEntryStats]:
        """返回窗口内各入口统计；``entry`` 非空时只返回该入口。"""

        with self._lock:
            if entry is not None:
                return {entry: self._window.stats(entry)}
            return self._window.stats_all()

    def slowest(self, limit: int = 10, entry: str | None = None) -> list[RequestRecord]:
        """返回当前保留明细中最慢的至多 ``limit`` 条（耗时降序）。

        查询在锁内完成拷贝与排序，明细淘汰不会让分页/排序中途变化；
        ``limit<=0`` 返回空列表。
        """

        if limit <= 0:
            return []
        with self._lock:
            records = list(self._records)
        if entry is not None:
            records = [r for r in records if r.entry == entry]
        records.sort(key=lambda r: r.duration_ms, reverse=True)
        return records[:limit]

    def slowest_attributions(
        self, limit: int = 10, entry: str | None = None
    ) -> list[RequestAttribution]:
        """最慢一批请求的耗时归因明细（等待/业务/嵌套三段 + span 树）。"""

        return [attribute(r) for r in self.slowest(limit, entry)]

    def attribute_trace(self, trace_id: str) -> RequestAttribution | None:
        """按 trace_id 查单条归因；明细已被淘汰或不存在时返回 None。"""

        with self._lock:
            for rec in self._records:
                if rec.trace_id == trace_id:
                    return attribute(rec)
        return None

    @property
    def evicted_details(self) -> int:
        """累计因容量/时长上限被淘汰的明细条数。"""

        with self._lock:
            return self._evicted_details

    @property
    def internal_failures(self) -> int:
        with self._lock:
            return self._record_calls_failed
