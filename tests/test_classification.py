"""失败原因分类测试：业务异常与框架级错误必须可区分。"""

from __future__ import annotations

import asyncio
import logging

import pytest
from fastapi import HTTPException
from starlette.requests import ClientDisconnect

from telemetry.classification import (
    FailureCategory,
    Outcome,
    classify_exception,
    classify_status,
    reason_of,
)
from telemetry.errors import BusinessError

logger = logging.getLogger("telemetry.tests")


@pytest.mark.parametrize(
    "exc",
    [
        pytest.param(BusinessError("余额不足", 422), id="business-422"),
        pytest.param(BusinessError("参数缺失"), id="business-default-400"),
        pytest.param(HTTPException(404, "not found"), id="http-404"),
        pytest.param(HTTPException(403, "forbidden"), id="http-403"),
        pytest.param(RuntimeError("boom"), id="runtime-error"),
        pytest.param(ValueError("bad value"), id="value-error"),
        pytest.param(HTTPException(503, "unavailable"), id="http-503"),
        pytest.param(asyncio.CancelledError(), id="cancelled"),
        pytest.param(ClientDisconnect(), id="client-disconnect"),
    ],
)
def test_exception_categories_are_distinct(exc: BaseException) -> None:
    outcome, category, reason = classify_exception(exc)
    logger.debug(
        "输入异常=%r -> 判定: outcome=%s category=%s reason=%s",
        exc,
        outcome.value,
        category.value,
        reason,
    )

    if isinstance(exc, (asyncio.CancelledError, ClientDisconnect)):
        assert outcome is Outcome.CLIENT_DISCONNECT
        assert category is FailureCategory.CLIENT_DISCONNECT
    elif isinstance(exc, BusinessError):
        assert outcome is Outcome.BUSINESS_ERROR
        assert category is FailureCategory.BUSINESS
    elif isinstance(exc, HTTPException) and exc.status_code < 500:
        assert outcome is Outcome.BUSINESS_ERROR
        assert category is FailureCategory.BUSINESS
    else:
        assert outcome is Outcome.FRAMEWORK_ERROR
        assert category is FailureCategory.FRAMEWORK

    # 业务与框架类别绝不相等，保证可聚合、可区分。
    assert FailureCategory.BUSINESS != FailureCategory.FRAMEWORK
    assert reason  # 所有被分类的异常都必须留下可读原因


def test_business_never_classified_as_framework() -> None:
    """任意 BusinessError（含全部合法 4xx）都不能落入框架类。"""

    for status in range(400, 452):
        exc = BusinessError("x", status)
        _, category, _ = classify_exception(exc)
        logger.debug("输入 status=%s -> category=%s", status, category.value)
        assert category is FailureCategory.BUSINESS


@pytest.mark.parametrize(
    "status,expected",
    [
        (200, Outcome.SUCCESS),
        (204, Outcome.SUCCESS),
        (301, Outcome.SUCCESS),
        (400, Outcome.BUSINESS_ERROR),
        (404, Outcome.BUSINESS_ERROR),
        (500, Outcome.FRAMEWORK_ERROR),
        (502, Outcome.FRAMEWORK_ERROR),
    ],
)
def test_classify_status_fallback(status: int, expected: Outcome) -> None:
    outcome, category, reason = classify_status(status)
    logger.debug(
        "输入 status=%s -> 判定 outcome=%s category=%s reason=%s",
        status,
        outcome.value,
        category.value,
        reason,
    )
    assert outcome is expected


def test_reason_is_stable_and_readable() -> None:
    assert reason_of(BusinessError("坏")) == "BusinessError: 坏"
    assert reason_of(RuntimeError()) == "RuntimeError"
