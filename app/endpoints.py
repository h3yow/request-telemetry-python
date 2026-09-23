"""演示入口：覆盖成功 / 业务异常 / 框架错误 / 慢请求 / 嵌套 span / 断开。"""

from __future__ import annotations

import asyncio

from fastapi import APIRouter

from app.errors import BusinessError
from telemetry.context import trace_span

router = APIRouter()


@router.get("/")
async def read_root() -> dict[str, str]:
    return {"Hello": "World"}


@router.get("/ping")
async def ping() -> dict[str, str]:
    return {"pong": "ok"}


@router.get("/business-error")
async def business_error() -> None:
    """显式抛出业务异常（4xx）。"""

    raise BusinessError("余额不足", status_code=422)


@router.get("/framework-error")
async def framework_error() -> None:
    """未处理异常，被兜底为 500（框架级错误）。"""

    raise RuntimeError("数据库连接失败(模拟)")


@router.get("/slow")
async def slow(ms: int = 600) -> dict[str, int]:
    """耗时超过阈值的慢请求（阈值默认 500ms）。"""

    await asyncio.sleep(ms / 1000.0)
    return {"slept_ms": ms}


@router.get("/nested")
async def nested() -> dict[str, object]:
    """产生两条嵌套 span，归并到同一 trace。"""

    with trace_span("outer-call") as outer:
        await asyncio.sleep(0.01)
        with trace_span("inner-call"):
            await asyncio.sleep(0.01)
    return {"trace_spans": 2, "outer_span": outer.span_id if outer else None}


@router.get("/nested-error")
async def nested_error() -> None:
    """嵌套调用内部抛业务异常，span 与请求终态都应可归因。"""

    with trace_span("outer-call"):
        with trace_span("inner-call"):
            raise BusinessError("内部业务校验失败", status_code=400)


@router.get("/wait-then-respond")
async def wait_then_respond(ms: int = 5000) -> dict[str, str]:
    """用于客户端中途断开测试：等待期间客户端断开会取消该请求。"""

    try:
        await asyncio.sleep(ms / 1000.0)
    except asyncio.CancelledError:
        # 模拟服务端在清理后继续向上抛出，中间件仍应记录为断开。
        raise
    return {"status": "done"}
