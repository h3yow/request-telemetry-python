"""窗口全量口径回归：高流量（含并发）下窗口与全量累计一致、滑出自然退去、
慢请求明细接口的 limit 边界行为。

背景：旧实现窗口样本有 10000 条硬上限，窗口内请求量超过上限后按 FIFO 丢
最旧样本，导致窗口 count/error/slow/分位数比全量口径小一截；慢请求明细
接口在 limit<=0 时抛 ValueError 表现为 500。本文件固化修复后的契约：

- 窗口不设条数上限，窗口时间内发生的请求一条不少；
- 高并发、错误/慢请求混杂时，窗口三项计数与 census 逐项相等；
- 只有滑出窗口时间下界的样本才退去，census 不受窗口滚动影响；
- ``GET /__telemetry__/slow`` 的 limit 为 0/负数/超大值时都返回 200 与
  确定结果（空列表 / 空列表 / 全部候选）。
"""

from __future__ import annotations

import logging
import threading

from fastapi.testclient import TestClient

from telemetry.classification import FailureCategory, Outcome
from telemetry.collector import TelemetryCollector
from telemetry.config import TelemetryConfig
from telemetry.recorder import RequestRecord

logger = logging.getLogger("telemetry.tests")


class FakeClock:
    def __init__(self, start: float = 10_000.0) -> None:
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


def _rec(trace_id: str, *, entry: str = "http:GET /", duration_ms: float = 10.0,
         is_error: bool = False, is_slow: bool = False) -> RequestRecord:
    return RequestRecord(
        trace_id=trace_id, entry=entry, method="GET", path="/",
        status_code=500 if is_error else 200, duration_ms=duration_ms,
        outcome=(Outcome.FRAMEWORK_ERROR if is_error else Outcome.SUCCESS),
        failure_category=(FailureCategory.FRAMEWORK if is_error
                          else FailureCategory.NONE),
        reason=("boom" if is_error else ""),
        slow=is_slow, kept=False,
    )


def _make_collector(clock: FakeClock, **overrides: object) -> TelemetryCollector:
    return TelemetryCollector(TelemetryConfig(**overrides), clock=clock)  # type: ignore[arg-type]


# ---- 高流量：窗口与全量累计一致 -------------------------------------------

def test_window_matches_census_under_high_volume() -> None:
    """单线程窗口内写入 15000 条（超旧上限 10000），窗口各项 == census。"""

    clock = FakeClock()
    coll = _make_collector(clock, window_seconds=300.0, sample_rate=0.0)
    total = 15_000
    for i in range(total):
        clock.advance(0.001)  # 全部落在 300s 窗口内
        coll.record(_rec(
            f"t-{i}",
            entry=("http:GET /a" if i % 2 == 0 else "http:GET /b"),
            duration_ms=float(i % 100),
            is_error=(i % 7 == 0),
            is_slow=(i % 5 == 0),
        ))

    stats = coll.window_stats()
    census = coll.aggregate().census
    meta = coll.window_meta()
    for entry in ("http:GET /a", "http:GET /b"):
        w, c = stats[entry], census[entry]
        logger.debug(
            "入口 %s: 窗口 count=%s err=%s slow=%s p50=%s p95=%s p99=%s | "
            "census total=%s err=%s slow=%s",
            entry, w.count, w.error_count, w.slow_count,
            w.p50_ms, w.p95_ms, w.p99_ms,
            c.total, c.error_count, c.slow_count,
        )
        # 判定依据：窗口是全量口径，任何流量下与 census 逐项相等。
        assert w.count == c.total
        assert w.error_count == c.error_count
        assert w.slow_count == c.slow_count
        assert 0.0 <= w.p50_ms <= w.p95_ms <= w.p99_ms <= 99.0
    assert stats["http:GET /a"].count + stats["http:GET /b"].count == total
    assert meta["added"] == total and meta["expired"] == 0


def test_window_matches_census_under_concurrent_high_volume() -> None:
    """10 线程 x 1500 并发写入（总量超旧上限），窗口与 census 不丢不重。"""

    clock = FakeClock()
    coll = _make_collector(clock, window_seconds=300.0, sample_rate=0.0)
    threads_n, per_thread = 10, 1500
    total = threads_n * per_thread
    barrier = threading.Barrier(threads_n)

    def write(worker_id: int) -> None:
        barrier.wait()
        for i in range(per_thread):
            seq = worker_id * per_thread + i
            coll.record(_rec(
                f"w{worker_id}-{i}",
                entry=("job-a" if worker_id % 2 == 0 else "job-b"),
                duration_ms=float(seq % 50),
                is_error=(seq % 11 == 0),
                is_slow=(seq % 6 == 0),
            ))

    threads = [threading.Thread(target=write, args=(w,)) for w in range(threads_n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    stats = coll.window_stats()
    census = coll.aggregate().census
    meta = coll.window_meta()
    logger.debug("并发 %s 线程 x %s -> meta=%s", threads_n, per_thread, meta)
    for entry in ("job-a", "job-b"):
        w, c = stats[entry], census[entry]
        logger.debug("入口 %s: 窗口 count=%s err=%s slow=%s | census %s/%s/%s",
                     entry, w.count, w.error_count, w.slow_count,
                     c.total, c.error_count, c.slow_count)
        # 判定依据：并发下同一临界区写入，两口径逐项相等、不丢样本不重复。
        assert w.count == c.total
        assert w.error_count == c.error_count
        assert w.slow_count == c.slow_count
    assert sum(s.count for s in stats.values()) == total
    assert meta["added"] == total and meta["expired"] == 0


# ---- 滑出窗口自然退去 ------------------------------------------------------

def test_requests_expire_as_window_scrolls() -> None:
    """样本只随时间滑出：部分滑出精确减少，全部滑出入口消失，census 不变。"""

    clock = FakeClock()
    coll = _make_collector(clock, window_seconds=10.0, sample_rate=0.0)
    for i in range(3):  # 完成于 t=10001/10002/10003
        clock.advance(1.0)
        coll.record(_rec(f"old-{i}", duration_ms=10.0 * (i + 1),
                         is_error=(i == 0), is_slow=(i == 2)))

    before = coll.window_stats()["http:GET /"]
    assert before.count == 3

    clock.advance(9.0)  # now=10012，下界 10002：ts=10001 滑出（左闭保留边界）
    middle = coll.window_stats()["http:GET /"]
    logger.debug("部分滑出: count %s -> %s, err %s -> %s, slow %s -> %s",
                 before.count, middle.count,
                 before.error_count, middle.error_count,
                 before.slow_count, middle.slow_count)
    # 判定依据：滑出的恰是最早那条（err=1 的那条），其余原样保留。
    assert middle.count == 2
    assert middle.error_count == 0 and middle.slow_count == 1
    assert middle.p50_ms == 20.0  # 剩 20/30

    clock.advance(5.0)  # now=10017，下界 10007：全部滑出
    gone = coll.window_stats()
    meta = coll.window_meta()
    census_total = coll.aggregate().census["http:GET /"].total
    logger.debug("全部滑出: window=%s meta=%s census_total=%s",
                 gone, meta, census_total)
    assert gone == {}
    assert meta["added"] == 3 and meta["expired"] == 3
    # census 是进程生命周期累计，不随窗口滚动变化。
    assert census_total == 3


# ---- 慢请求明细接口 limit 边界 ---------------------------------------------

def test_slow_endpoint_limit_boundary_values(
    client: TestClient, collector: TelemetryCollector
) -> None:
    """limit=0 / 负数 / 超大值都返回 200 与确定结果，不再 500。"""

    for i, dur in enumerate((300.0, 100.0, 200.0)):
        collector.record(_rec(f"s-{i}", duration_ms=dur, is_slow=True))

    resp_zero = client.get("/__telemetry__/slow?limit=0")
    resp_negative = client.get("/__telemetry__/slow?limit=-7")
    resp_huge = client.get("/__telemetry__/slow?limit=1000000000")
    resp_default = client.get("/__telemetry__/slow")
    logger.debug("limit=0 -> %s; limit=-7 -> %s; limit=1e9 -> %s; 默认 -> %s",
                 resp_zero.status_code, resp_negative.status_code,
                 resp_huge.status_code, resp_default.status_code)
    # 判定依据：边界输入一律 200，结果确定。
    assert resp_zero.status_code == 200
    assert resp_negative.status_code == 200
    assert resp_huge.status_code == 200
    assert resp_default.status_code == 200

    zero_items = resp_zero.json()["items"]
    negative_items = resp_negative.json()["items"]
    huge_items = resp_huge.json()["items"]
    logger.debug("items: limit=0 -> %s 条; limit=-7 -> %s 条; limit=1e9 -> %s",
                 len(zero_items), len(negative_items),
                 [i["trace_id"] for i in huge_items])
    assert zero_items == []
    assert negative_items == []
    # 超大 limit 返回全部候选，仍按耗时降序。
    assert [i["trace_id"] for i in huge_items] == ["s-0", "s-2", "s-1"]
    assert [i["total_ms"] for i in huge_items] == [300.0, 200.0, 100.0]
