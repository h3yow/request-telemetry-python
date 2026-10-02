"""耗时分位数计算（纯函数、确定性、无第三方依赖）。

口径：对样本做升序排序后使用"线性插值"分位数（与 numpy 默认的 Type-7 一致，
也是大多数监控系统的通用口径）：

    rank = (p / 100) * (n - 1)      # 0-based 位置
    q = x[lo] + (rank - lo) * (x[hi] - x[lo])

- 单样本时所有分位数都等于该样本本身；
- 空样本返回 ``None``，由调用方决定如何展示（不臆造 0，避免与"真实 0ms"混淆）；
- 输入不会被原地修改（内部排序副本），并发只读安全。
"""

from __future__ import annotations


def percentile(values: list[float] | tuple[float, ...], p: float) -> float | None:
    """返回 ``p`` 分位数；``p`` 必须位于 (0, 100]，空输入返回 ``None``。"""

    if not (0.0 < p <= 100.0):
        raise ValueError("分位数 p 必须位于 (0, 100]")
    if not values:
        return None

    ordered = sorted(values)
    n = len(ordered)
    if n == 1:
        return float(ordered[0])

    rank = (p / 100.0) * (n - 1)
    lo = int(rank)
    hi = min(lo + 1, n - 1)
    fraction = rank - lo
    return float(ordered[lo]) + fraction * (float(ordered[hi]) - float(ordered[lo]))
