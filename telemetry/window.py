"""按入口维护的滑动时间窗口：计数 + 耗时分位数。

窗口口径（两条同源流，保证"数不打架"）：

- **事件流**：每个进入采集器的请求都写入一条 ``(时间, 是否失败, 是否慢)``，
  与采样率/保留与否无关 —— 所以窗口内的请求量、失败数、慢请求数是
  **全量口径**，可与 census 直接对账；
- **耗时样本流**：只有实际保留了明细的请求才写入 ``(时间, 耗时)``，
  p50/p95/p99 基于窗口内该流计算，并显式给出 ``quantile_sample_size``，
  不把样本数伪装成全量数。采样率为 1 时两条流条数完全相等。

滚动规则：查询/写入时按注入的单调时钟 ``now_ms`` 滑掉
``ts < now - window_ms`` 的旧数据（边界值保留），于是"新请求立刻算进来、
滑出窗口的自然退掉"。全部状态在一把锁内变更，每条事件与其耗时样本
（若有）在同一临界区写入，多线程交错下计数与样本不会错配。
"""

from __future__ import annotations

import threading
from collections import defaultdict, deque
from dataclasses import dataclass

from .quantile import percentile


@dataclass(frozen=True)
class WindowEntryStats:
    """单个入口在当前窗口内的统计。"""

    entry: str
    total: int
    """窗口内请求量（全量口径）。"""

    error_count: int
    """窗口内失败数（全量口径，failure != none）。"""

    slow_count: int
    """窗口内慢请求数（全量口径）。"""

    quantile_sample_size: int
    """参与分位数计算的样本条数（保留明细口径，``<= total``）。"""

    p50_ms: float | None
    p95_ms: float | None
    p99_ms: float | None

    window_ms: float
    sampled_at_ms: float
    """生成该快照时的时钟读数。"""


class SlidingWindowStore:
    """每个入口一份滚动窗口数据。

    ``clock_ms`` 注入单调毫秒时钟（测试可传入可控时钟）；事件结构只存
    数值元组，内存占用有界（由窗口长度决定，与历史总量无关）。
    """

    def __init__(self, window_ms: float, clock_ms=None) -> None:
        if not isinstance(window_ms, (int, float)) or isinstance(window_ms, bool):
            raise TypeError("window_ms 必须是数值")
        if window_ms <= 0:
            raise ValueError("window_ms 必须为正数")
        self._window_ms = float(window_ms)
        self._clock_ms = clock_ms
        self._lock = threading.Lock()
        # 事件流：(ts_ms, is_error, is_slow)
        self._events: dict[str, deque[tuple[float, bool, bool]]] = defaultdict(deque)
        # 耗时样本流：(ts_ms, duration_ms)，仅保留明细的请求
        self._durations: dict[str, deque[tuple[float, float]]] = defaultdict(deque)

    @property
    def window_ms(self) -> float:
        return self._window_ms

    def set_window_ms(self, window_ms: float) -> None:
        """热更新窗口长度（必须为正）；下次读写按新窗口滚动。"""

        if not isinstance(window_ms, (int, float)) or isinstance(window_ms, bool):
            raise TypeError("window_ms 必须是数值")
        if window_ms <= 0:
            raise ValueError("window_ms 必须为正数")
        self._window_ms = float(window_ms)

    def _now(self) -> float:
        if self._clock_ms is not None:
            return float(self._clock_ms())
        import time

        return time.perf_counter() * 1000.0

    # ---- 写入 -------------------------------------------------------

    def add(
        self,
        entry: str,
        duration_ms: float | None,
        is_error: bool,
        is_slow: bool,
        ts_ms: float | None = None,
    ) -> None:
        """登记一次请求；``duration_ms=None`` 表示该请求未保留明细。

        事件与样本在同一临界区写入，保证两条流对同一次请求要么都在、
        要么只有事件（未采样），绝不会出现样本多于事件等错配。
        """

        ts = self._now() if ts_ms is None else float(ts_ms)
        with self._lock:
            self._roll_locked(entry, ts)
            self._events[entry].append((ts, bool(is_error), bool(is_slow)))
            if duration_ms is not None:
                self._durations[entry].append((ts, float(duration_ms)))

    def _roll_locked(self, entry: str, now_ms: float) -> None:
        edge = now_ms - self._window_ms
        events = self._events.get(entry)
        if events is not None:
            while events and events[0][0] < edge:
                events.popleft()
        durations = self._durations.get(entry)
        if durations is not None:
            while durations and durations[0][0] < edge:
                durations.popleft()

    # ---- 读取 -------------------------------------------------------

    def stats(self, entry: str, now_ms: float | None = None) -> WindowEntryStats:
        now = self._now() if now_ms is None else float(now_ms)
        with self._lock:
            self._roll_locked(entry, now)
            events = self._events.get(entry)
            total = 0
            errors = 0
            slow = 0
            if events is not None:
                for _ts, is_error, is_slow in events:
                    total += 1
                    errors += 1 if is_error else 0
                    slow += 1 if is_slow else 0
            values: list[float] = []
            durations = self._durations.get(entry)
            if durations is not None:
                values = [d for _ts, d in durations]
            return WindowEntryStats(
                entry=entry,
                total=total,
                error_count=errors,
                slow_count=slow,
                quantile_sample_size=len(values),
                p50_ms=percentile(values, 50),
                p95_ms=percentile(values, 95),
                p99_ms=percentile(values, 99),
                window_ms=self._window_ms,
                sampled_at_ms=now,
            )

    def stats_all(self, now_ms: float | None = None) -> dict[str, WindowEntryStats]:
        now = self._now() if now_ms is None else float(now_ms)
        with self._lock:
            entries = list(self._events.keys())
        return {entry: self.stats(entry, now) for entry in entries}

    def reset(self) -> None:
        with self._lock:
            self._events.clear()
            self._durations.clear()
