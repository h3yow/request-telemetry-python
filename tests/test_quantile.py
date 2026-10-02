"""分位数计算的口径用例（线性插值，确定性）。"""

from __future__ import annotations

import logging

import pytest

from telemetry.quantile import percentile

logger = logging.getLogger("telemetry.tests")


def test_empty_and_single_sample() -> None:
    """输入=[] 与 单样本时的确定性结论。"""

    logger.debug("判定依据: 空样本->None；单样本->其自身（任何 p）")
    assert percentile([], 50) is None
    assert percentile([7.0], 50) == 7.0
    assert percentile([7.0], 99) == 7.0


@pytest.mark.parametrize(
    "p,expected",
    [(0.1, 1.099), (50, 50.5), (95, 95.05), (99, 99.01), (100, 100.0)],
)
def test_linear_interpolation_1_to_100(p: float, expected: float) -> None:
    """样本 1..100 时 rank=(p/100)*(n-1) 线性插值。"""

    values = [float(i) for i in range(1, 101)]
    got = percentile(values, p)
    logger.debug("输入=1..100 p=%s -> %s（期望 %s）", p, got, expected)
    assert got is not None and abs(got - expected) < 1e-9


def test_input_not_mutated() -> None:
    """排序使用副本，调用方列表顺序不变（并发只读安全前提）。"""

    data = [3.0, 1.0, 2.0]
    percentile(data, 95)
    logger.debug("输入=%s 调用后=%s（应保持原序）", [3, 1, 2], data)
    assert data == [3.0, 1.0, 2.0]


@pytest.mark.parametrize("bad_p", [0.0, -1.0, 101.0])
def test_invalid_quantile_rejected(bad_p: float) -> None:
    logger.debug("输入 p=%s -> 期望 ValueError", bad_p)
    with pytest.raises(ValueError):
        percentile([1.0, 2.0], bad_p)
