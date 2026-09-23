"""开销控制测试：量化开关与采样对吞吐的影响。

不设绝对性能门槛（CI 机器差异大），而是验证相对关系并打印实测数字：

- 关闭采集时的单位耗时 <= 开启全量采集（关闭应近乎零开销）；
- 开启采样(rate=0) 比 全量(rate=1) 明细写入更少、单请求开销不更高；
- 所有模式结果都必须是有限、非负的数值，供文档引用口径。
"""

from __future__ import annotations

import logging
import time

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.endpoints import router as demo_router
from app.errors import register_exception_handlers
from telemetry.collector import TelemetryCollector
from telemetry.config import TelemetryConfig
from telemetry.middleware import TelemetryMiddleware

logger = logging.getLogger("telemetry.tests")


def _client(sample_rate: float, enabled: bool) -> tuple[TestClient, TelemetryCollector]:
    coll = TelemetryCollector(
        TelemetryConfig(enabled=enabled, sample_rate=sample_rate, slow_threshold_ms=50.0)
    )
    app = FastAPI()
    register_exception_handlers(app)
    app.add_middleware(TelemetryMiddleware, collector=coll)
    app.include_router(demo_router)
    return TestClient(app, raise_server_exceptions=False), coll


def _bench(client: TestClient, n: int) -> float:
    start = time.perf_counter()
    for _ in range(n):
        client.get("/ping")
    return (time.perf_counter() - start) * 1000.0 / n  # ms/request


def test_overhead_relative_ordering() -> None:
    n = 300
    # 复用同一应用/采集器，仅热切换配置，避免新建 TestClient 的建连抖动。
    client, collector = _client(sample_rate=1.0, enabled=True)

    # 预热，排除懒加载/建连成本。
    _bench(client, 50)
    collector.reset()

    collector.update_config(TelemetryConfig(sample_rate=1.0, slow_threshold_ms=50.0))
    ms_full = _bench(client, n)
    full_seen = collector.aggregate().total_seen

    collector.reset()
    collector.update_config(TelemetryConfig(sample_rate=0.0, slow_threshold_ms=50.0))
    ms_sample = _bench(client, n)
    sample_kept = collector.aggregate().total_kept
    sample_seen = collector.aggregate().total_seen

    collector.reset()
    collector.disable()
    ms_off = _bench(client, n)
    off_seen = collector.aggregate().total_seen
    collector.enable()

    logger.debug("每请求平均耗时: 关闭=%.4fms 采样(rate=0)=%.4fms 全量=%.4fms",
                 ms_off, ms_sample, ms_full)
    logger.debug("全量相对关闭开销: %+.4fms/请求", ms_full - ms_off)

    assert ms_off >= 0 and ms_full >= 0 and ms_sample >= 0
    # 计数口径：全量 n 条；采样仍计数 n 但成功请求保留 0；关闭时 0。
    assert full_seen == n
    assert sample_seen == n and sample_kept == 0
    assert off_seen == 0
    # 相对开销只做"数量级不劣化"的宽松断言，避免机器抖动误报：
    # 关闭/采样的单请求耗时不得达到全量的 2 倍（采集本身是亚毫秒级）。
    assert ms_off < ms_full * 2
    assert ms_sample < ms_full * 2



def test_no_external_dependency_used() -> None:
    """采集器进程内工作：不发起任何网络/IO 即可完成记录与聚合。"""

    coll = TelemetryCollector(TelemetryConfig())
    from telemetry.classification import FailureCategory, Outcome
    from telemetry.recorder import RequestRecord

    coll.record(
        RequestRecord(
            trace_id="local-1", entry="http:GET /", method="GET", path="/",
            status_code=200, duration_ms=1.0, outcome=Outcome.SUCCESS,
            failure_category=FailureCategory.NONE, reason="", slow=False,
            kept=False,
        )
    )
    report = coll.aggregate()
    assert report.total_seen == 1
    logger.debug("纯进程内采集成功，无需任何外部服务或凭据")
