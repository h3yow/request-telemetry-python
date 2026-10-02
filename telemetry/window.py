"""滑动时间窗口：滚动统计最近一段时间内各入口的请求指标。

口径与边界（务必与全量计数、明细保留区分开理解）：

- 窗口只保存聚合所需的**轻量样本**：完成时刻（单调时钟秒）、入口、
  端到端耗时（毫秒）、是否失败、是否慢。样本按完成时刻单调排列；
- 新请求在完成的同一临界区内写入，**立刻**可被下一次查询看到；
- 早于窗口下界（``now - window_seconds``）的样本在写入与查询时惰性
  淘汰，自然滑出。保留边界为左闭：``ts >= now - window_seconds``；
- 窗口是**全量口径**：每个进入采集器的请求都写一份窗口样本，与采样率、
  明细保留完全无关。关闭采集总开关后不写入；
- 样本条数有硬上限 ``max_samples``：正常情况下它远大于窗口内的请求量；
  若极端高峰把它撑满，则按 FIFO 丢弃**最旧**样本（确定规则），丢弃条数
  通过 :attr:`SlidingWindow.overflow_dropped` 可观测，避免内存被撑爆。
  注意这只影响窗口口径，全量 census 计数不受任何影响。

线程安全：本类**自身不加锁**。所有调用（写入与查询）都必须在采集器的
同一把锁下进行，从而与计数、明细淘汰保持一致的原子视图。

分位数采用 nearest-rank 定义（``ceil(p*n)``，1 基），无需近似结构、
结论可复算；空入口分位数为 ``0.0``。
"""

from __future__ import annotations

import math
import threading
import time
from collections import defaultdict, deque
from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True)
class WindowEntryStats:
    """单个入口在当前窗口内的统计结果。"""

    entry: str
    count: int
    error_count: int
    slow_count: int
    p50_ms: float
    p95_ms: float
    p99_ms: float


@dataclass(frozen=True)
class _Sample:
    ts: float
    entry: str
    duration_ms: float
    is_error: bool
    is_slow: bool


_QUANTILES = (0.50, 0.95, 0.99)


def nearest_rank(sorted_values: list[float], q: float) -> float:
    """nearest-rank 分位数：``sorted_values`` 必须升序且非空。"""

    n = len(sorted_values)
    rank = max(1, math.ceil(q * n))
    return sorted_values[rank - 1]


class SlidingWindow:
    """进程内滑动窗口（锁由外部持有）。

    :param window_seconds: 窗口长度（秒），必须严格为正；
    :param max_samples: 窗口样本条数硬上限，必须严格为正；
    :param clock: 返回单调时间（秒）的可调用对象，便于测试注入。
    """

    def __init__(
        self,
        window_seconds: float,
        max_samples: int = 10_000,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if not isinstance(window_seconds, (int, float)) or isinstance(
            window_seconds, bool
        ):
            raise TypeError("window_seconds 必须是数值")
        if window_seconds <= 0:
            raise ValueError("window_seconds 必须严格为正")
        if not isinstance(max_samples, int) or isinstance(max_samples, bool):
            raise TypeError("max_samples 必须是 int")
        if max_samples <= 0:
            raise ValueError("max_samples 必须严格为正")
        self._window = float(window_seconds)
        self._max_samples = max_samples
        self._clock = clock
        self._samples: deque[_Sample] = deque()
        self._added = 0
        self._expired = 0
        self._overflow_dropped = 0

    # ---- 属性 -------------------------------------------------------

    @property
    def window_seconds(self) -> float:
        return self._window

    @property
    def max_samples(self) -> int:
        return self._max_samples

    @property
    def size(self) -> int:
        """当前窗口内（未惰性淘汰）的样本数。"""

        return len(self._samples)

    @property
    def added(self) -> int:
        """累计写入样本数。"""

        return self._added

    @property
    def expired(self) -> int:
        """因滑出窗口被淘汰的样本累计数。"""

        return self._expired

    @property
    def overflow_dropped(self) -> int:
        """因撞上限被 FIFO 丢弃的最旧样本累计数。"""

        return self._overflow_dropped

    # ---- 写入 / 淘汰 ------------------------------------------------

    def add(
        self,
        entry: str,
        ts: float,
        duration_ms: float,
        is_error: bool,
        is_slow: bool,
    ) -> bool:
        """写入一个完成样本。

        返回是否保留在窗口内：样本时刻已在窗口之外（迟到且过期）时返回
        ``False`` 且不计数；正常写入返回 ``True``。撞容量上限时挤掉最旧
        样本，新样本仍立即计入。
        """

        cutoff = self._clock() - self._window
        if ts < cutoff:
            # 迟到样本（理论上不会发生：完成即写），明确丢弃且不入账。
            return False

        self._samples.append(
            _Sample(
                ts=float(ts),
                entry=entry,
                duration_ms=float(duration_ms),
                is_error=bool(is_error),
                is_slow=bool(is_slow),
            )
        )
        self._added += 1

        overflow = len(self._samples) - self._max_samples
        while overflow > 0:
            self._samples.popleft()
            self._overflow_dropped += 1
            overflow -= 1
        return True

    def evict(self, now: float | None = None) -> int:
        """淘汰早于窗口下界的样本，返回淘汰条数。"""

        if now is None:
            now = self._clock()
        cutoff = now - self._window
        removed = 0
        while self._samples and self._samples[0].ts < cutoff:
            self._samples.popleft()
            removed += 1
        self._expired += removed
        return removed

    # ---- 读取 -------------------------------------------------------

    def stats(self, now: float | None = None) -> dict[str, WindowEntryStats]:
        """返回各入口在当前窗口内的请求量、失败数、慢请求数与分位数。"""

        self.evict(now)
        durations: dict[str, list[float]] = defaultdict(list)
        errors: dict[str, int] = defaultdict(int)
        slows: dict[str, int] = defaultdict(int)
        for sample in self._samples:
            durations[sample.entry].append(sample.duration_ms)
            if sample.is_error:
                errors[sample.entry] += 1
            if sample.is_slow:
                slows[sample.entry] += 1

        result: dict[str, WindowEntryStats] = {}
        for entry, values in durations.items():
            values.sort()
            result[entry] = WindowEntryStats(
                entry=entry,
                count=len(values),
                error_count=errors[entry],
                slow_count=slows[entry],
                p50_ms=nearest_rank(values, _QUANTILES[0]),
                p95_ms=nearest_rank(values, _QUANTILES[1]),
                p99_ms=nearest_rank(values, _QUANTILES[2]),
            )
        return result

    def reset(self) -> None:
        """清空窗口状态（主要供测试使用）。"""

        self._samples.clear()
        self._added = 0
        self._expired = 0
        self._overflow_dropped = 0

    def reconfigure(
        self, window_seconds: float, max_samples: int, now: float | None = None
    ) -> None:
        """热更新窗口长度与容量：保留现存样本，仅按新边界即时收敛。

        - 新长度更短时，立刻淘汰落在新窗口之外的旧样本（计入 expired）；
        - 新容量更小时，按 FIFO 丢弃最旧样本（计入 overflow_dropped）。
        """

        if (
            not isinstance(window_seconds, (int, float))
            or isinstance(window_seconds, bool)
            or window_seconds <= 0
        ):
            raise ValueError("window_seconds 必须为正数")
        if (
            not isinstance(max_samples, int)
            or isinstance(max_samples, bool)
            or max_samples <= 0
        ):
            raise ValueError("max_samples 必须为正整数")

        self._window = float(window_seconds)
        self._max_samples = max_samples
        self.evict(now)
        overflow = len(self._samples) - self._max_samples
        while overflow > 0:
            self._samples.popleft()
            self._overflow_dropped += 1
            overflow -= 1
