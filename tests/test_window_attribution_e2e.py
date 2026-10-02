"""端到端：经中间件实测的分段计时、嵌套归因，以及高并发下窗口/全量对账。"""

from __future__ import annotations

import logging
import threading

from fastapi.testclient import TestClient

from telemetry.attribution import build_attribution
from telemetry.classification import FailureCategory
from telemetry.collector import TelemetryCollector
from telemetry.config import TelemetryConfig

logger = logging.getLogger("telemetry.tests")


def test_middleware_segments_and_nested_attribution(client: TestClient,
                                                    collector: TelemetryCollector) -> None:
    resp = client.get("/nested")
    logger.debug("输入 GET /nested -> status=%s", resp.status_code)
    assert resp.status_code == 200
    rec = next(r for r in collector.snapshot() if r.path == "/nested")
    attr = build_attribution(rec)
    logger.debug(
        "归因 total=%.3f queue=%.3f business=%.3f response=%.3f; "
        "顶层段=%s 嵌套层=%s",
        attr.total_ms, attr.queue_ms, attr.business_own_ms, attr.response_ms,
        [(s.name, round(s.duration_ms, 3)) for s in attr.segments],
        [(s.name, [c.name for c in s.children]) for s in attr.segments],
    )
    # 判定依据：业务窗口内有两个 span（outer 含 inner），顶层仅 outer；
    # outer 覆盖两次 sleep（>=20ms），故业务自身耗时接近 0。
    assert attr.is_consistent()
    names = [s.name for s in attr.segments]
    assert "outer-call" in names
    outer = next(s for s in attr.segments if s.name == "outer-call")
    assert [c.name for c in outer.children] == ["inner-call"]
    assert attr.queue_ms >= 0.0 and attr.response_ms >= 0.0
    # 对账：各段和与端到端耗时一致（容差 1ns）。
    assert abs(attr.accounted_ms - rec.duration_ms) <= 1e-6


def test_slow_endpoint_returns_reconcilable_attribution(client: TestClient) -> None:
    client.get("/slow?ms=55")  # 夹具阈值 50ms，记为慢
    resp = client.get("/__telemetry__/slow?limit=5")
    assert resp.status_code == 200
    payload = resp.json()
    slow_items = [i for i in payload["items"] if i["path"] == "/slow"]
    logger.debug("输入 GET /slow?ms=55 -> 慢归因条数=%s", len(slow_items))
    assert len(slow_items) == 1
    item = slow_items[0]
    a = item["attribution"]
    logger.debug("HTTP 归因 total=%s accounted=%s segments=%s",
                 item["total_ms"], a["accounted_ms"],
                 [s["name"] for s in a["segments"]])
    assert abs(a["accounted_ms"] - item["total_ms"]) <= 1e-6
    assert a["business_own_ms"] > 0.0  # /slow 无嵌套 span，等待全在业务自身


def test_window_endpoint_reports_quantiles(client: TestClient) -> None:
    for _ in range(5):
        client.get("/ping")
    body = client.get("/__telemetry__/window").json()
    entry = body["entries"]["http:GET /ping"]
    logger.debug("输入 5 次 GET /ping -> count=%s p50=%s p95=%s p99=%s",
                 entry["count"], entry["p50_ms"], entry["p95_ms"],
                 entry["p99_ms"])
    assert entry["count"] == 5
    assert 0.0 < entry["p50_ms"] <= entry["p95_ms"] <= entry["p99_ms"]


def test_concurrent_window_matches_census(collector: TelemetryCollector) -> None:
    """高并发 + 断开/慢请求混杂时，窗口 count 与 census total 必须对得上。"""

    from telemetry.classification import Outcome
    from telemetry.recorder import RequestRecord

    barrier = threading.Barrier(8)

    def write(worker_id: int) -> None:
        barrier.wait()
        for i in range(250):
            # 每 worker：错误 10 条(i%25)、慢 7 条(i%40)，其中 2 条重叠
            # (i%200)，用于验证错误数与慢请求数彼此独立、互不串账。
            is_err = i % 25 == 0
            is_slow = i % 40 == 0
            rec = RequestRecord(
                trace_id=f"w{worker_id}-{i}", entry="job", method="GET",
                path="/job", status_code=500 if is_err else 200,
                duration_ms=900.0 if is_slow else 1.0,
                outcome=(Outcome.FRAMEWORK_ERROR if is_err
                         else Outcome.SUCCESS),
                failure_category=(FailureCategory.FRAMEWORK if is_err
                                  else FailureCategory.NONE),
                reason=("boom" if is_err else ""),
                slow=is_slow, kept=False,
            )
            collector.record(rec)

    threads = [threading.Thread(target=write, args=(w,)) for w in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    window = collector.window_stats()["job"]
    census = collector.aggregate().census["job"]
    logger.debug(
        "8x250 并发: 窗口 count=%s errors=%s slow=%s | census total=%s "
        "errors=%s slow=%s（错误∩慢=%s）",
        window.count, window.error_count, window.slow_count,
        census.total, census.error_count, census.slow_count, 8 * 2,
    )
    # 判定依据：窗口足够长（夹具 300s），全部样本仍在窗内；
    # 错误 8*10=80、慢 8*7=56（其中 16 条重叠），两口径同临界区必须相等。
    assert window.count == census.total == 2000
    assert window.error_count == census.error_count == 80
    assert window.slow_count == census.slow_count == 56
