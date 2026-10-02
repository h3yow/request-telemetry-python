"""滑动窗口：滚动、分位数、事件流/样本流两条口径不打架。"""

from __future__ import annotations

import logging

import pytest

from telemetry.window import SlidingWindowStore

logger = logging.getLogger("telemetry.tests")


class FakeClock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


def test_window_counts_and_quantiles() -> None:
    """5 个事件（1 失败、1 慢）、3 条耗时样本 -> 计数与分位数各归各流。"""

    clk = FakeClock()
    w = SlidingWindowStore(10.0, clock_ms=clk)
    for i in range(5):
        w.add("e", duration_ms=float(i + 1) if i < 3 else None,
              is_error=(i == 1), is_slow=(i == 2), ts_ms=float(i))
    s = w.stats("e", now_ms=4.0)
    logger.debug(
        "total=%s errors=%s slow=%s 样本数=%s p50=%s（样本[1,2,3]期望2.0）",
        s.total, s.error_count, s.slow_count, s.quantile_sample_size, s.p50_ms,
    )
    assert (s.total, s.error_count, s.slow_count) == (5, 1, 1)
    assert s.quantile_sample_size == 3
    assert s.p50_ms == 2.0 and abs(s.p99_ms - 2.98) < 1e-9


def test_window_rolls_out() -> None:
    """新请求立即入窗；早于 now-window 的事件与样本自然滑出（边界保留）。"""

    w = SlidingWindowStore(10.0, clock_ms=lambda: 0.0)
    w.add("e", duration_ms=1.0, is_error=False, is_slow=False, ts_ms=0.0)
    w.add("e", duration_ms=2.0, is_error=True, is_slow=False, ts_ms=1.0)
    at_edge = w.stats("e", now_ms=10.0)
    after_edge = w.stats("e", now_ms=10.1)
    out = w.stats("e", now_ms=11.1)
    logger.debug(
        "边界 now=10 total=%s；now=10.1 total=%s 样本=%s；now=11.1 total=%s 失败=%s",
        at_edge.total, after_edge.total, after_edge.quantile_sample_size,
        out.total, out.error_count,
    )
    assert at_edge.total == 2
    assert after_edge.total == 1 and after_edge.quantile_sample_size == 1
    assert out.total == 0 and out.error_count == 0 and out.p95_ms is None


def test_sample_stream_never_exceeds_event_stream() -> None:
    """高并发/未采样混入时：样本数 <= 事件数，且失败/慢计数恒为全量。"""

    import threading

    w = SlidingWindowStore(1_000_000.0, clock_ms=lambda: 0.0)

    def worker(worker_id: int) -> None:
        for i in range(200):
            ts = float(worker_id * 1000 + i)
            # 偶数保留耗时样本，奇数不保留（模拟采样/未保留）
            w.add("e", duration_ms=ts if i % 2 == 0 else None,
                  is_error=(i % 50 == 0), is_slow=(i % 40 == 0), ts_ms=ts)

    threads = [threading.Thread(target=worker, args=(k,)) for k in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    s = w.stats("e", now_ms=1_000_000.0)
    logger.debug(
        "8x200 并发 -> 事件=%s（期望1600）样本=%s（期望800）失败=%s 慢=%s",
        s.total, s.quantile_sample_size, s.error_count, s.slow_count,
    )
    assert s.total == 1600
    assert s.quantile_sample_size == 800
    assert s.quantile_sample_size <= s.total
    assert s.error_count == 8 * 4
    assert s.slow_count == 8 * 5


def test_window_bad_length() -> None:
    logger.debug("窗口长度 0/-1 -> 期望 ValueError")
    with pytest.raises(ValueError):
        SlidingWindowStore(0)
