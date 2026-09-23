"""客户端中途断开测试。

TestClient 的同步接口无法真实地在服务端等待期间断开，因此这里直接在 ASGI
层面构造场景，结论必须是确定的 ``client_disconnect``：

1. 服务端等待时，receive 返回 ``http.disconnect``；
2. 服务端任务被外层取消（``CancelledError``）—— 模拟客户端断开导致 uvicorn
   取消请求任务；
3. 断开已发生但下游"平静返回"（未消费 receive）—— 仍应判为断开；
4. 采集记录必须落库，且不干扰其他请求。
"""

from __future__ import annotations

import asyncio
import logging

import pytest

from telemetry.classification import FailureCategory, Outcome
from telemetry.collector import TelemetryCollector
from telemetry.middleware import TelemetryMiddleware

logger = logging.getLogger("telemetry.tests")


def _make_app_stack(collector: TelemetryCollector):
    """构造一个最小下游 ASGI 应用：感知到 http.disconnect 后平静结束。"""

    async def downstream(scope, receive, send):
        message = await receive()
        if message["type"] == "http.disconnect":
            # 模拟感知到断开后结束（不发送响应）。
            return
        # 正常情况下不会走到这。
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    return TelemetryMiddleware(downstream, collector=collector)


async def _invoke(app, receive_messages):
    sent: list[dict] = []
    queue = asyncio.Queue()
    for m in receive_messages:
        queue.put_nowait(m)

    async def receive():
        item = await queue.get()
        if isinstance(item, BaseException):
            raise item
        return item

    async def send(message):
        sent.append(message)

    await app(
        {"type": "http", "method": "GET", "path": "/wait", "state": {},
         "headers": [], "query_string": b""},
        receive,
        send,
    )
    return sent


def test_disconnect_event_recorded_as_client_disconnect(
    collector: TelemetryCollector,
) -> None:
    app = _make_app_stack(collector)
    sent = asyncio.run(_invoke(app, [{"type": "http.disconnect"}]))
    logger.debug("输入 receive=[http.disconnect] -> 发送响应数=%d（期望 0）", len(sent))
    assert sent == []
    recs = collector.snapshot()
    assert len(recs) == 1
    rec = recs[0]
    logger.debug("判定 outcome=%s category=%s reason=%s",
                 rec.outcome.value, rec.failure_category.value, rec.reason)
    assert rec.outcome is Outcome.CLIENT_DISCONNECT
    assert rec.failure_category is FailureCategory.CLIENT_DISCONNECT
    assert "disconnect" in rec.reason


def test_cancelled_error_recorded_as_client_disconnect(
    collector: TelemetryCollector,
) -> None:
    """下游协程在等待时被取消（CancelledError），结论同样为客户端断开。"""

    async def downstream(scope, receive, send):
        await asyncio.sleep(10)  # 模拟长时间等待客户端

    app = TelemetryMiddleware(downstream, collector=collector)

    async def scenario():
        sent: list[dict] = []

        async def receive():
            await asyncio.Event().wait()  # 永远等不到

        async def send(message):
            sent.append(message)

        scope = {"type": "http", "method": "GET", "path": "/wait",
                 "state": {}, "headers": [], "query_string": b""}
        task = asyncio.create_task(app(scope, receive, send))
        await asyncio.sleep(0.02)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        return sent

    sent = asyncio.run(scenario())
    logger.debug("输入 task.cancel() -> 发送响应数=%d", len(sent))
    assert sent == []
    rec = collector.snapshot()[0]
    logger.debug("判定 outcome=%s category=%s reason=%s",
                 rec.outcome.value, rec.failure_category.value, rec.reason)
    assert rec.outcome is Outcome.CLIENT_DISCONNECT
    assert rec.failure_category is FailureCategory.CLIENT_DISCONNECT


def test_disconnect_does_not_corrupt_other_requests(
    collector: TelemetryCollector,
) -> None:
    """断开请求之后，正常请求的采集仍准确（整体采集不受单次异常影响）。"""

    app = _make_app_stack(collector)
    asyncio.run(_invoke(app, [{"type": "http.disconnect"}]))

    # 再来一个正常下游。
    async def healthy(scope, receive, send):
        await receive()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    healthy_app = TelemetryMiddleware(healthy, collector=collector)
    sent = asyncio.run(
        _invoke(
            healthy_app,
            [
                {"type": "http.request", "body": b"", "more_body": False},
            ],
        )
    )
    assert any(m.get("status") == 200 for m in sent if m["type"] == "http.response.start")
    rep = collector.aggregate()
    logger.debug(
        "断开后正常请求 -> seen=%s, 各 outcome=%s",
        rep.total_seen,
        {e: a.by_outcome for e, a in rep.census.items()},
    )
    assert rep.total_seen == 2
    by_cat = {
        e: a.by_failure for e, a in rep.census.items()
    }
    all_failures = {k: v for m in by_cat.values() for k, v in m.items()}
    assert all_failures.get("client_disconnect") == 1
