"""并发交错测试：计数无丢失/重复/错配，trace 上下文隔离。"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor

from fastapi.testclient import TestClient

from telemetry.collector import TelemetryCollector
from telemetry.classification import FailureCategory

logger = logging.getLogger("telemetry.tests")


def test_concurrent_requests_counts_exact(
    client: TestClient, collector: TelemetryCollector
) -> None:
    """10 线程 x 每线程 40 次混合请求，全量计数必须精确。"""

    def hit(seq: int) -> str:
        path = ("/ping", "/business-error", "/framework-error", "/ping")[seq % 4]
        resp = client.get(path)
        return path

    total_per_thread = 40
    threads_n = 10
    total = total_per_thread * threads_n

    with ThreadPoolExecutor(max_workers=threads_n) as pool:
        paths = list(pool.map(hit, range(total)))

    rep = collector.aggregate()
    logger.debug("并发输入: %s 次, 线程数=%s", total, threads_n)
    logger.debug("全量 total_seen=%s（期望 %s）", rep.total_seen, total)
    assert rep.total_seen == total
    assert rep.total_kept == total  # 默认采样率 1.0

    ping = paths.count("/ping")
    biz = paths.count("/business-error")
    fwk = paths.count("/framework-error")
    census = rep.census
    got_ping = census["http:GET /ping"].total
    got_biz = census["http:GET /business-error"].total
    got_fwk = census["http:GET /framework-error"].total
    logger.debug("按入口: ping=%s business=%s framework=%s", got_ping, got_biz, got_fwk)
    assert (got_ping, got_biz, got_fwk) == (ping, biz, fwk)
    # 失败原因不串类。
    assert census["http:GET /business-error"].by_failure == {"business": biz}
    assert census["http:GET /framework-error"].by_failure == {"framework": fwk}
    assert census["http:GET /ping"].by_outcome == {"success": ping}


def test_trace_ids_unique_under_concurrency(
    client: TestClient, collector: TelemetryCollector
) -> None:
    def hit(_: int) -> None:
        client.get("/ping")

    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(hit, range(200)))

    trace_ids = [r.trace_id for r in collector.snapshot()]
    logger.debug("200 并发请求, 去重后 trace_id 数=%s", len(set(trace_ids)))
    assert len(trace_ids) == 200
    assert len(set(trace_ids)) == 200  # 无重复、无错配


def test_collector_direct_concurrent_writes(collector: TelemetryCollector) -> None:
    """绕过 HTTP，直接对采集器并发写，压测临界区正确性。"""

    from telemetry.classification import Outcome
    from telemetry.recorder import RequestRecord

    def write(worker_id: int) -> None:
        for i in range(250):
            rec = RequestRecord(
                trace_id=f"w{worker_id}-{i}",
                entry="job",
                method="GET",
                path="/job",
                status_code=200,
                duration_ms=1.0,
                outcome=Outcome.SUCCESS,
                failure_category=FailureCategory.NONE,
                reason="",
                slow=False,
                kept=False,
            )
            collector.record(rec)

    threads = [threading.Thread(target=write, args=(w,)) for w in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    rep = collector.aggregate()
    logger.debug("8 线程 x 250 直写 -> total_seen=%s（期望 2000）", rep.total_seen)
    assert rep.total_seen == 2000
    assert rep.census["job"].total == 2000
    assert len(collector.snapshot()) == 2000
