"""通过 HTTP 验证窗口报表端点、慢请求归因端点与真实 span 对账。"""

from __future__ import annotations

import logging

from fastapi.testclient import TestClient

logger = logging.getLogger("telemetry.tests")


def test_window_endpoint_reports_quantiles(client: TestClient) -> None:
    """打若干入口后，/__telemetry__/window 给出每入口计数与分位数。"""

    for _ in range(5):
        client.get("/ping")
    client.get("/slow?ms=60")  # 超阈值 50ms -> 慢请求
    resp = client.get("/__telemetry__/window")
    data = resp.json()
    ping = data["entries"]["http:GET /ping"]
    slow_entry = data["entries"]["http:GET /slow"]
    logger.debug("窗口 ping=%s slow=%s window_ms=%s",
                 ping, slow_entry, data["window_ms"])
    assert resp.status_code == 200
    assert ping["total"] == 5 and ping["quantile_sample_size"] == 5
    assert ping["p50_ms"] is not None
    assert slow_entry["total"] == 1 and slow_entry["slow_count"] == 1
    # 全量口径下样本数与事件数一致（夹具采样率=1）
    assert ping["quantile_sample_size"] == ping["total"]


def test_slowest_endpoint_segments_reconcile(client: TestClient) -> None:
    """/nested 的慢归因三段之和等于总耗时，span 树含 outer/inner。"""

    client.get("/nested")
    resp = client.get("/__telemetry__/slowest?limit=5")
    data = resp.json()
    assert resp.status_code == 200
    nested = next(
        x for x in data["items"] if x["entry"] == "http:GET /nested"
    )
    seg = nested["segments"]
    logger.debug(
        "/nested 总=%s wait=%s business=%s nested=%s accounted=%s span树=%s",
        nested["duration_ms"], seg["wait_ms"], seg["business_ms"],
        seg["nested_ms"], seg["accounted_ms"],
        [s["name"] for s in nested["spans"]],
    )
    # JSON 展示统一保留 3 位小数，段之和与总耗时只差四舍五入误差
    assert abs(seg["accounted_ms"] - nested["duration_ms"]) < 1e-6
    assert abs(
        seg["wait_ms"] + seg["business_ms"] + seg["nested_ms"]
        - seg["accounted_ms"]
    ) < 1e-3
    root_names = [s["name"] for s in nested["spans"]]
    assert root_names == ["outer-call"]
    assert nested["spans"][0]["children"][0]["name"] == "inner-call"
    # 嵌套段占了 20ms 左右的两次 sleep
    assert seg["nested_ms"] >= 15.0
    assert nested["boundary_source"] == "mark"


def test_slowest_entry_filter_and_limit(client: TestClient) -> None:
    """entry 过滤与 limit 生效，limit 被钳制在 [0,100]。"""

    client.get("/ping")
    client.get("/nested")
    resp = client.get("/__telemetry__/slowest", params={
        "limit": 1, "entry": "http:GET /ping"
    })
    data = resp.json()
    logger.debug("过滤 entry=/ping limit=1 -> count=%s entries=%s",
                 data["count"], [x["entry"] for x in data["items"]])
    assert data["count"] == 1
    assert all(x["entry"] == "http:GET /ping" for x in data["items"])
    big = client.get("/__telemetry__/slowest?limit=99999").json()
    assert len(big["items"]) <= 100


def test_window_empty_entry_returns_zero_stats(client: TestClient) -> None:
    """从未出现过的入口：计数为 0、分位为 null（不臆造 0）。"""

    data = client.get("/__telemetry__/window", params={"entry": "http:GET /nope"})
    entry = data.json()["entries"]["http:GET /nope"]
    logger.debug("空入口窗口统计=%s", entry)
    assert entry["total"] == 0 and entry["p95_ms"] is None
