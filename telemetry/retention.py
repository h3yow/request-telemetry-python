"""明细保留边界：条数上限 + 时长上限的确定性淘汰。

淘汰规则（完全确定，仅作用于"保留明细"，不触碰全量计数）：

1. 记录按到达顺序（时间单调不减）排列；
2. **条数上限**：超出 ``max_records`` 时，最旧的记录先被淘汰（FIFO）；
3. **时长上限**：早于 ``now - max_age_ms`` 的记录被淘汰；
4. 两个上限叠加后，保留下来的一定是原序列的一个**后缀**，边界稳定、
   与数据内容无关，不会出现"同批记录有的留有的删"的抖动。

``max_records <= 0`` 表示一条明细也不留（只有全量计数）；
``max_age_ms <= 0`` 表示不做时长淘汰。
"""

from __future__ import annotations

from dataclasses import dataclass

DEFAULT_MAX_RECORDS = 1000
DEFAULT_MAX_AGE_MS = 60 * 60 * 1000.0  # 1 小时


@dataclass(frozen=True)
class RetentionPolicy:
    """明细保留策略（不可变）。"""

    max_records: int = DEFAULT_MAX_RECORDS
    """明细条数上限；``<= 0`` 表示完全不保留明细。"""

    max_age_ms: float = DEFAULT_MAX_AGE_MS
    """明细时长上限（毫秒）；``<= 0`` 表示不按时间淘汰。"""

    def __post_init__(self) -> None:
        if not isinstance(self.max_records, int) or isinstance(self.max_records, bool):
            raise TypeError("max_records 必须是 int")
        if self.max_records < 0:
            raise ValueError("max_records 不能为负")
        if not isinstance(self.max_age_ms, (int, float)) or isinstance(
            self.max_age_ms, bool
        ):
            raise TypeError("max_age_ms 必须是数值")
        if float(self.max_age_ms) < 0:
            raise ValueError("max_age_ms 不能为负")


def eviction_cut(
    timestamps_ms: list[float], now_ms: float, policy: RetentionPolicy
) -> int:
    """返回应从序列头部删除的条数。

    ``timestamps_ms`` 必须按记录到达顺序排列（单调不减）。保留结果恒为
    ``timestamps_ms[cut:]`` 这样的后缀。

    判定边界：``now_ms - ts <= max_age_ms`` 视为仍在保留期内（边界值保留）。
    """

    n = len(timestamps_ms)

    # 1) 条数淘汰：只保留最后 max_records 条。
    if policy.max_records <= 0:
        cut = n
    else:
        cut = max(0, n - policy.max_records)

    # 2) 时长淘汰：在条数淘汰之后继续跳过已过期的最旧记录。
    if policy.max_age_ms > 0:
        edge = now_ms - float(policy.max_age_ms)
        ordered = timestamps_ms
        while cut < n and ordered[cut] < edge:
            cut += 1
    return cut
