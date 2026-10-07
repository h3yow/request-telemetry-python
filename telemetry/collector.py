"""采集器：线程安全地计数、滚动窗口聚合、保留明细并做慢请求归因。

并发正确性：

- 所有可变状态（全量计数器、窗口样本、保留明细、trace 去重账本）只在一把
  :class:`threading.RLock` 下变更，"判定是否保留 + 计数 + 窗口写入 + 追加
  明细 + 容量淘汰" 是同一个临界区，因此在多线程/多协程交错调用下不会出现
  计数丢失、重复或口径互相打架（请求量/失败数/慢请求数来自同一份样本）；
- 采样判定基于 ``trace_id`` 的稳定哈希（``hash()`` 会受 ``PYTHONHASHSEED``
  影响，故使用自实现的 FNV-1a + fmix64），同一 trace 永远得到同一判定；
- 错误请求与慢请求（``is_error`` / ``is_slow``）无条件保留；
- **全量计数（census）与是否采样完全无关** —— 每个请求必被计数一次；
- **滚动窗口也是全量口径** —— 每个请求必写一份轻量窗口样本，与采样无关。

容量与淘汰边界（确定规则，绝不影响全量计数）：

- 明细队列上限为 ``config.detail_max_records``，超出时按 FIFO 淘汰最旧
  明细（错误/慢请求也遵循同一上限，淘汰规则不因请求类别而变）；
- 窗口样本**不设条数上限**：窗口内发生的请求一条不少，只在滑出窗口
  时间下界时退去，因此任何流量下窗口各项都与窗口内全量累计对得上；
  内存上界为 ``窗口长度 × 窗口内峰值速率`` 条轻量样本，不随进程生命
  周期累积；
- 明细被淘汰只影响"保留明细"口径与慢请求归因查询，census 计数与窗口样本
  互不牵连；trace 去重账本保留全部 trace_id（保证重复提交绝不重复计数，
  是"全量计数不丢失/不重复"这条对账承诺的前提）；
- 查询接口只对当前明细做内存内排序，耗时受明细上限约束，不会被流量卡住。
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict, deque
from collections.abc import Callable

from .attribution import RequestAttribution, build_attribution
from .config import TelemetryConfig
from .recorder import AggregationReport, EntryAggregation, RequestRecord
from .window import SlidingWindow, WindowEntryStats


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
        clock: Callable[[], float] = time.perf_counter,
    ) -> None:
        self._lock = threading.RLock()
        # 明细改为 FIFO 队列，容量超限时确定性淘汰最旧记录。
        self._records: deque[RequestRecord] = deque()
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
        self._clock = clock
        self._window = SlidingWindow(
            self._config.window_seconds,
            clock=clock,
        )
        self._total_seen = 0
        self._record_calls_failed = 0
        self._detail_evicted = 0

    # ---- 配置与开关 -------------------------------------------------

    @property
    def config(self) -> TelemetryConfig:
        with self._lock:
            return self._config

    def update_config(self, config: TelemetryConfig) -> None:
        """热更新配置（如运行期关闭采集或调整窗口/容量）。

        窗口参数热更新时保留现存样本，只按新边界即时收敛；明细容量缩小时
        立即按 FIFO 收敛到新上限。
        """

        with self._lock:
            self._config = config
            self._window.reconfigure(
                config.window_seconds,
                now=self._clock(),
            )
            self._enforce_detail_capacity_locked()

    def enable(self) -> None:
        self.update_config(self._config_with(enabled=True))

    def disable(self) -> None:
        self.update_config(self._config_with(enabled=False))

    def _config_with(
        self, *, enabled: bool | None = None, **overrides: object
    ) -> TelemetryConfig:
        current = self._config
        return TelemetryConfig(
            enabled=current.enabled if enabled is None else enabled,
            sample_rate=overrides.get("sample_rate", current.sample_rate),  # type: ignore[arg-type]
            slow_threshold_ms=overrides.get(  # type: ignore[arg-type]
                "slow_threshold_ms", current.slow_threshold_ms
            ),
            window_seconds=overrides.get(  # type: ignore[arg-type]
                "window_seconds", current.window_seconds
            ),
            detail_max_records=overrides.get(  # type: ignore[arg-type]
                "detail_max_records", current.detail_max_records
            ),
            slow_top_n=overrides.get("slow_top_n", current.slow_top_n),  # type: ignore[arg-type]
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
        - 窗口样本始终写一份（与采样、明细保留无关）；
        - 明细按采样决定保留，并按 FIFO 容量上限淘汰；
        - 返回该记录是否被保留；
        - 任何意外异常都被隔离，只累加内部失败计数，不波及请求主链路。
        """

        try:
            with self._lock:
                if not self._config.enabled:
                    return False

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

                    # 全量口径：每个新请求即刻进入滚动窗口。
                    self._window.add(
                        rec.entry,
                        self._clock(),
                        rec.duration_ms,
                        is_error,
                        rec.slow,
                    )

                kept = self.should_sample(
                    rec.trace_id,
                    is_error=is_error,
                    is_slow=rec.slow,
                )
                if not is_new:
                    # 同一 trace 重复提交：既不重复计数，也不把已被容量
                    # 淘汰的明细"复活"。返回它当前是否仍在明细队列中。
                    return rec.trace_id in self._kept_trace_ids
                if kept:
                    self._kept_trace_ids.add(rec.trace_id)
                    stored = rec if rec.kept else self._mark_kept(rec)
                    self._records.append(stored)
                    self._enforce_detail_capacity_locked()
                return kept
        except Exception:  # noqa: BLE001 - 采集绝不能拖垮业务
            self._record_calls_failed += 1
            return False

    def _enforce_detail_capacity_locked(self) -> None:
        """把明细队列收敛到配置上限（FIFO 淘汰最旧记录）。

        被淘汰记录的 trace 仍留在去重账本中：重复提交不会因此重复计数。
        """

        cap = self._config.detail_max_records
        while len(self._records) > cap:
            evicted = self._records.popleft()
            self._kept_trace_ids.discard(evicted.trace_id)
            self._detail_evicted += 1

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
            queue_ms=rec.queue_ms,
            business_ms=rec.business_ms,
            response_ms=rec.response_ms,
        )

    # ---- 读取：明细 / 全量 / 聚合 -----------------------------------

    def snapshot(self) -> list[RequestRecord]:
        """返回当前保留明细的快照副本（FIFO 顺序）。"""

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
            sampled = self._aggregate_records(list(self._records))
            return AggregationReport(
                census=census,
                sampled=sampled,
                total_seen=self._total_seen,
                total_kept=len(self._records),
                sample_rate=self._config.normalized_sample_rate,
            )

    # ---- 读取：滚动窗口 ---------------------------------------------

    def window_stats(
        self, now: float | None = None
    ) -> dict[str, WindowEntryStats]:
        """返回各入口在当前滑动窗口内的请求量/失败/慢请求/分位数。"""

        with self._lock:
            return self._window.stats(
                self._clock() if now is None else now
            )

    def window_meta(self) -> dict[str, object]:
        """窗口自身的口径与淘汰计数，供对账与容量观测。"""

        with self._lock:
            return {
                "window_seconds": self._window.window_seconds,
                "size": self._window.size,
                "added": self._window.added,
                "expired": self._window.expired,
            }

    # ---- 读取：慢请求归因 -------------------------------------------

    def slowest_attributions(
        self, limit: int | None = None, entry: str | None = None
    ) -> list[tuple[RequestRecord, RequestAttribution]]:
        """返回最慢的一批保留明细及其耗时归因。

        - 排序：耗时降序；耗时相同按 trace_id 升序（结论确定、可复现）；
        - ``limit`` 缺省取 ``config.slow_top_n``；``limit <= 0`` 时确定地
          返回空列表（不报错）；``limit`` 大于候选数时返回全部候选；
          非 ``int``（含 ``bool``）属于编程错误，抛 :class:`TypeError`；
        - 仅返回**已保留**的慢请求明细（采样/容量淘汰后的口径，见模块文档）；
        - 全程在锁内做有界排序，明细上限即工作量上限。
        """

        if limit is not None and (
            not isinstance(limit, int) or isinstance(limit, bool)
        ):
            raise TypeError("limit 必须为 int 或 None")
        with self._lock:
            top_n = self._config.slow_top_n if limit is None else limit
            if top_n <= 0:
                # 边界输入的确定行为：0/负数 -> 空结果，而不是 500。
                return []
            candidates = [
                rec
                for rec in self._records
                if rec.slow and (entry is None or rec.entry == entry)
            ]
            candidates.sort(key=lambda r: (-r.duration_ms, r.trace_id))
            chosen = candidates[:top_n]
            return [(rec, build_attribution(rec)) for rec in chosen]

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
            self._window.reset()
            self._total_seen = 0
            self._record_calls_failed = 0
            self._detail_evicted = 0

    @property
    def internal_failures(self) -> int:
        with self._lock:
            return self._record_calls_failed

    @property
    def detail_evicted(self) -> int:
        """因容量上限被 FIFO 淘汰的明细累计条数。"""

        with self._lock:
            return self._detail_evicted
