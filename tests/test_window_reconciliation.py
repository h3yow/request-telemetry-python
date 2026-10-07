"""窗口全量覆盖对账与慢请求明细接口边界测试。

覆盖三条业务承诺：

1. 窗口口径：默认配置下窗口时间内持续写入一万多条请求（超过旧实现的
   窗口样本上限），窗口的请求量/失败数/慢请求数/分位数与全量累计严格
   一致——新请求立即计入，只有真正滑出窗口的才退去，任何流量下都不丢；
2. 并发口径：多线程同时完成、错误/慢请求与正常请求混杂时，窗口各项
   与全量累计一致，不丢样本、不重复计数、互不串账；
3. 观测接口边界：``/__telemetry__/slow`` 的 ``limit`` 传 0、负数、超大
   值时都返回确定结果（HTTP 200），绝不因一个参数返回 500。

判定依据均通过日志打印。
"""

from __future__ import annotations

import logging
import threading

from fastapi.testclient import TestClient

from telemetry.classification import FailureCategory, Outcome
from telemetry.collector import TelemetryCollector
from telemetry.config import TelemetryConfig
from telemetry.recorder import RequestRecord
from telemetry.window import nearest_rank

logger = logging.getLogger("telemetry.tests")


class FakeClock:
    """可手动推进的单调时钟。"""

    def __init__(self, start: float = 10_000.0) -> None:
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


def _rec(trace_id: str, *, duration_ms: float = 10.0, slow: bool = False,
         error: bool = False,
         entry: str = "http:GET /") -> RequestRecord:
    return RequestRecord(
        trace_id=trace_id, entry=entry, method="GET", path="/",
        status_code=500 if error else 200, duration_ms=duration_ms,
        outcome=Outcome.FRAMEWORK_ERROR if error else Outcome.SUCCESS,
        failure_category=(FailureCategory.FRAMEWORK if error
                          else FailureCategory.NONE),
        reason="boom" if error else "", slow=slow, kept=False,
    )


# ---- 1. 高流量下窗口与全量累计一致（默认配置） ----------------------------

def test_window_matches_census_beyond_legacy_sample_cap() -> None:
    """默认配置、窗口时间内写入 12000 条：窗口各项与全量累计严格一致。"""

    clock = FakeClock()
    coll = TelemetryCollector(TelemetryConfig(), clock=clock)  # 全部默认
    total = 12_000  # 超过旧实现默认窗口样本上限 10000
    durations: list[float] = []
    expect_errors = expect_slows = 0
    for i in range(total):
        clock.advance(0.001)  # 12 秒内打完，远在 300s 窗口内
        dur = float(i % 500)
        err = i % 25 == 0
        slow = i % 40 == 0
        expect_errors += err
        expect_slows += slow
        durations.append(dur)
        coll.record(_rec(f"t-{i}", duration_ms=dur, slow=slow, error=err))

    window = coll.window_stats()["http:GET /"]
    census = coll.aggregate().census["http:GET /"]
    meta = coll.window_meta()
    ordered = sorted(durations)
    logger.debug(
        "默认配置写入 %s 条 -> 窗口 count=%s err=%s slow=%s p50=%s p95=%s "
        "p99=%s | census total=%s err=%s slow=%s | meta=%s",
        total, window.count, window.error_count, window.slow_count,
        window.p50_ms, window.p95_ms, window.p99_ms,
        census.total, census.error_count, census.slow_count, meta,
    )
    # 判定依据：窗口一条不丢，各项与全量累计、与全量耗时序列的
    # nearest-rank 分位数完全一致。
    assert window.count == census.total == total
    assert window.error_count == census.error_count == expect_errors
    assert window.slow_count == census.slow_count == expect_slows
    assert window.p50_ms == nearest_rank(ordered, 0.50)
    assert window.p95_ms == nearest_rank(ordered, 0.95)
    assert window.p99_ms == nearest_rank(ordered, 0.99)
    assert meta["added"] == total and meta["expired"] == 0


def test_window_matches_census_under_concurrent_flood() -> None:
    """多线程并发写入 12800 条（错误/慢/正常混杂）：窗口与全量一致。"""

    clock = FakeClock()
    coll = TelemetryCollector(TelemetryConfig(), clock=clock)
    workers, per_worker = 16, 800
    total = workers * per_worker
    barrier = threading.Barrier(workers)

    def write(worker_id: int) -> None:
        barrier.wait()
        for i in range(per_worker):
            seq = worker_id * per_worker + i
            coll.record(_rec(
                f"w{worker_id}-{i}",
                duration_ms=float(seq % 500),
                slow=seq % 40 == 0,
                error=seq % 25 == 0,
            ))

    threads = [threading.Thread(target=write, args=(w,)) for w in range(workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    window = coll.window_stats()["http:GET /"]
    census = coll.aggregate().census["http:GET /"]
    logger.debug(
        "%s 线程 x %s 并发 -> 窗口 count=%s err=%s slow=%s | "
        "census total=%s err=%s slow=%s",
        workers, per_worker, window.count, window.error_count,
        window.slow_count, census.total, census.error_count,
        census.slow_count,
    )
    # 判定依据：并发下不丢样本、不重复计数；三口径同临界区，必然相等。
    assert window.count == census.total == total
    assert window.error_count == census.error_count == total // 25
    assert window.slow_count == census.slow_count == total // 40


# ---- 2. 滑出窗口后自然退去 -------------------------------------------------

def test_requests_fade_out_after_leaving_window() -> None:
    """窗口 60s：旧请求滑出后窗口退去，全量累计不受影响。"""

    clock = FakeClock()
    coll = TelemetryCollector(
        TelemetryConfig(window_seconds=60.0), clock=clock
    )
    for i in range(5):
        clock.advance(1.0)
        coll.record(_rec(f"old-{i}", duration_ms=10.0 + i))
    clock.advance(30.0)
    coll.record(_rec("new-0", duration_ms=99.0))
    before = coll.window_stats()["http:GET /"]
    logger.debug("窗口 [t-60, t]: 5 旧 + 1 新 -> count=%s", before.count)
    assert before.count == 6

    clock.advance(40.0)  # t=10075，下界 10015：旧样本(<=10005)全部滑出
    after = coll.window_stats()["http:GET /"]
    meta = coll.window_meta()
    census_total = coll.aggregate().census["http:GET /"].total
    logger.debug("滑出后 -> 窗口 count=%s p50=%s; expired=%s; census=%s",
                 after.count, after.p50_ms, meta["expired"], census_total)
    # 判定依据：窗口只剩仍在窗内的 new-0；census 全量 6 条不随窗口退去。
    assert after.count == 1
    assert after.p50_ms == 99.0
    assert meta["expired"] == 5
    assert census_total == 6

    clock.advance(61.0)  # new-0 也滑出，入口从窗口消失
    assert coll.window_stats() == {}
    assert coll.aggregate().census["http:GET /"].total == 6


# ---- 3. 慢请求明细接口的 limit 边界 ----------------------------------------

def test_slow_endpoint_limit_edge_cases(client: TestClient,
                                        collector: TelemetryCollector) -> None:
    """limit=0 / 负数 / 超大值都返回确定的 200 结果，绝不 500。"""

    for i in range(3):
        resp = client.get(f"/slow?ms={55 + i}")  # 夹具阈值 50ms，记为慢
        assert resp.status_code == 200
    total_slow = len(collector.slowest_attributions(limit=10**9))
    logger.debug("预置慢请求 %s 条", total_slow)
    assert total_slow == 3

    cases = [
        ("limit=0", 0),
        ("limit=-5", 0),
        ("limit=1000000000", total_slow),  # 超大：返回全部候选
        ("", min(collector.config.slow_top_n, total_slow)),  # 缺省 top_n
    ]
    for query, expected in cases:
        url = f"/__telemetry__/slow?{query}" if query else "/__telemetry__/slow"
        resp = client.get(url)
        body = resp.json() if resp.status_code == 200 else {}
        logger.debug("输入 GET %s -> status=%s items=%s（期望 %s）",
                     url, resp.status_code, len(body.get("items", [])),
                     expected)
        # 判定依据：边界输入一律 200，条数确定。
        assert resp.status_code == 200, url
        assert len(body["items"]) == expected, url
    # 超大 limit 的结果按耗时降序、确定可复现。
    items = client.get("/__telemetry__/slow?limit=1000000000").json()["items"]
    durations = [i["total_ms"] for i in items]
    assert durations == sorted(durations, reverse=True)


def test_slowest_attributions_limit_edges_direct(
    collector: TelemetryCollector,
) -> None:
    """采集器直调：limit 边界与非整数输入都有确定行为。"""

    from telemetry.recorder import RequestRecord as RR  # noqa: F401 - 明示依赖

    collector.record(_rec("s1", duration_ms=100.0, slow=True))
    collector.record(_rec("s2", duration_ms=200.0, slow=True))

    assert collector.slowest_attributions(limit=0) == []
    assert collector.slowest_attributions(limit=-1) == []
    huge = collector.slowest_attributions(limit=10**12)
    logger.debug("limit=0/-1 -> 空; limit=1e12 -> %s 条", len(huge))
    assert [r.trace_id for r, _ in huge] == ["s2", "s1"]
    # 非整数（bool/float/字符串）按缺省 slow_top_n 处理，不抛错。
    defaulted = collector.slowest_attributions(limit="abc")  # type: ignore[arg-type]
    assert [r.trace_id for r, _ in defaulted] == ["s2", "s1"]
    assert collector.slowest_attributions(limit=True) == defaulted  # type: ignore[arg-type]
