"""滑动窗口测试：滚动计入/滑出、p50/p95/p99、多入口与容量上限。

所有用例注入确定性的假时钟，判定依据通过日志打印。
"""

from __future__ import annotations

import logging

import pytest

from telemetry.window import SlidingWindow, nearest_rank

logger = logging.getLogger("telemetry.tests")


class FakeClock:
    """可手动推进的单调时钟。"""

    def __init__(self, start: float = 1000.0) -> None:
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


def _seed(window: SlidingWindow, clock: FakeClock, values: list[float],
          entry: str = "http:GET /") -> None:
    """每个样本间隔 1 秒写入一批耗时样本。"""

    for v in values:
        clock.advance(1.0)
        window.add(entry, clock(), v, is_error=False, is_slow=v >= 90.0)


# ---- 分位数定义 -------------------------------------------------------

@pytest.mark.parametrize(
    "n,q,expected_rank",
    [(1, 0.5, 1), (10, 0.5, 5), (10, 0.95, 10), (10, 0.99, 10),
     (20, 0.95, 19), (20, 0.99, 20), (100, 0.99, 99)],
)
def test_nearest_rank_matches_ceil_definition(n: int, q: float,
                                              expected_rank: int) -> None:
    values = [float(i) for i in range(1, n + 1)]  # 1..n
    result = nearest_rank(values, q)
    logger.debug("输入 n=%s q=%s -> rank=ceil(%s)=%s, 值=%s",
                 n, q, q * n, expected_rank, result)
    assert result == float(expected_rank)


def test_quantiles_on_100_uniform_samples() -> None:
    clock = FakeClock()
    w = SlidingWindow(window_seconds=10_000.0, max_samples=1000, clock=clock)
    _seed(w, clock, [float(i) for i in range(1, 101)])
    s = w.stats()["http:GET /"]
    logger.debug("输入 1..100 各 1 条 -> p50=%s p95=%s p99=%s",
                 s.p50_ms, s.p95_ms, s.p99_ms)
    # 判定依据：nearest-rank, p50=第50个=50, p95=第95个=95, p99=第99个=99。
    assert (s.p50_ms, s.p95_ms, s.p99_ms) == (50.0, 95.0, 99.0)
    assert s.count == 100


# ---- 滚动计入 / 滑出 ---------------------------------------------------

def test_samples_enter_immediately_and_expire_by_time() -> None:
    clock = FakeClock()
    w = SlidingWindow(window_seconds=10.0, max_samples=1000, clock=clock)
    _seed(w, clock, [10.0, 20.0, 30.0])  # 写入完成于 1001/1002/1003

    stats_now = w.stats(now=1005.0)["http:GET /"]
    logger.debug("窗口 [995,1005] 内 3 条 -> count=%s p50=%s",
                 stats_now.count, stats_now.p50_ms)
    assert stats_now.count == 3

    # 推进到 1012：下界 1002，ts=1001 的样本滑出（左开），剩 2 条。
    stats_later = w.stats(now=1012.0)["http:GET /"]
    logger.debug("窗口 [1002,1012]: ts=1001 滑出 -> count=%s 保留耗时=%s/%s",
                 stats_later.count, stats_later.p50_ms, stats_later.p99_ms)
    assert stats_later.count == 2
    assert stats_later.p50_ms == 20.0  # 剩 20,30，nearest-rank p50=20
    assert w.expired == 1

    # 全部滑出后入口消失，且不报错。
    gone = w.stats(now=1020.0)
    assert gone == {}
    assert w.expired == 3


def test_new_request_visible_before_explicit_eviction() -> None:
    clock = FakeClock()
    w = SlidingWindow(window_seconds=10.0, max_samples=1000, clock=clock)
    assert w.add("e", clock(), 5.0, False, False) is True
    # 写入后不调用 evict，直接 stats 也必须立刻可见。
    assert w.stats()["e"].count == 1
    logger.debug("新样本写入即刻可见 -> size=%s", w.size)


def test_late_sample_older_than_window_is_rejected() -> None:
    clock = FakeClock()
    w = SlidingWindow(window_seconds=10.0, max_samples=1000, clock=clock)
    clock.advance(100.0)
    accepted = w.add("e", clock() - 11.0, 1.0, False, False)
    logger.debug("迟到样本 ts=now-11s（窗口 10s）-> 保留=%s, size=%s",
                 accepted, w.size)
    assert accepted is False
    assert w.size == 0
    assert w.added == 0


# ---- 失败数 / 慢请求数与请求量同口径 -----------------------------------

def test_error_and_slow_counts_match_count_under_mix() -> None:
    clock = FakeClock()
    w = SlidingWindow(window_seconds=1000.0, max_samples=1000, clock=clock)
    cases = [
        ("a", 10.0, False, False),
        ("a", 95.0, False, True),
        ("a", 20.0, True, False),
        ("b", 5.0, False, False),
    ]
    for entry, dur, err, slow in cases:
        clock.advance(0.1)
        w.add(entry, clock(), dur, err, slow)
    stats = w.stats()
    a, b = stats["a"], stats["b"]
    logger.debug("入口 a: count=%s errors=%s slow=%s; 入口 b: count=%s",
                 a.count, a.error_count, a.slow_count, b.count)
    # 判定依据：4 条样本同进同出，三个计数来自同一份样本，必然对得上。
    assert a.count == 3 and a.error_count == 1 and a.slow_count == 1
    assert b.count == 1 and b.error_count == 0 and b.slow_count == 0


# ---- 容量上限（FIFO 丢最旧，绝不丢计数语义）----------------------------

def test_overflow_drops_oldest_fifo() -> None:
    clock = FakeClock()
    w = SlidingWindow(window_seconds=1e9, max_samples=3, clock=clock)
    for dur in (10.0, 20.0, 30.0, 40.0):
        clock.advance(1.0)
        w.add("e", clock(), dur, False, False)
        logger.debug("写入 %s 后 size=%s dropped=%s",
                     dur, w.size, w.overflow_dropped)
    s = w.stats()["e"]
    logger.debug("上限 3，写入 4 条 -> count=%s 最小保留耗时=%s",
                 s.count, s.p50_ms)
    # 判定依据：FIFO 淘汰最旧（10），保留 20/30/40；added=4 不丢账。
    assert w.size == 3
    assert w.overflow_dropped == 1
    assert w.added == 4
    assert s.p50_ms == 30.0  # 20,30,40 的 p50
    assert s.count == 3


def test_invalid_construction_arguments_rejected() -> None:
    with pytest.raises(ValueError):
        SlidingWindow(window_seconds=0.0)
    with pytest.raises(ValueError):
        SlidingWindow(window_seconds=1.0, max_samples=0)
    with pytest.raises(TypeError):
        SlidingWindow(window_seconds=True)  # type: ignore[arg-type]
    logger.debug("非法构造参数（0 窗口/0 容量/bool）均被拒绝")


def test_reconfigure_shrinks_window_and_capacity() -> None:
    clock = FakeClock()
    w = SlidingWindow(window_seconds=100.0, max_samples=10, clock=clock)
    _seed(w, clock, [1.0, 2.0, 3.0])  # 完成于 1001/1002/1003
    # 窗口缩到 1s：现在 1003，只剩 ts>=1002... 实际下界 1002，1001 滑出。
    w.reconfigure(window_seconds=1.0, max_samples=10, now=1003.0)
    assert w.size == 2 and w.expired == 1
    # 容量缩到 1：FIFO 再丢 1 条最旧。
    w.reconfigure(window_seconds=100.0, max_samples=1, now=1003.0)
    logger.debug("热更新: 窗口缩短过期=%s, 容量收缩丢弃=%s, size=%s",
                 w.expired, w.overflow_dropped, w.size)
    assert w.size == 1
    assert w.overflow_dropped == 1
