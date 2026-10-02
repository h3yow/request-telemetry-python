"""记录与聚合模型。

- :class:`RequestRecord`：一次请求一条不可变记录（含其全部 span）；
- :class:`EntryAggregation`：单个入口在某个结果维度上的聚合计数；
- :class:`AggregationReport`：整张聚合报表，明确区分

  * ``census`` —— 全量口径：每个经过中间件的请求都计入（无论是否采样保留）；
  * ``sampled`` —— 明细口径：仅统计实际保留下来的记录。

两套口径分开给出，开启采样后聚合结论仍可解释、可对账。
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .classification import FailureCategory, Outcome


@dataclass(frozen=True)
class SpanRecord:
    """一次内部（可嵌套）调用的记录。"""

    trace_id: str
    span_id: str
    parent_span_id: str | None
    name: str
    duration_ms: float
    outcome: Outcome
    failure_category: FailureCategory
    reason: str
    started_ms: float = 0.0
    """span 起始时刻（采集器单调时钟，毫秒；相对任意固定原点）。0 表示缺失。"""

    ended_ms: float = 0.0
    """span 结束时刻（同上）；``ended - started`` 与 ``duration_ms`` 同源。"""


@dataclass(frozen=True)
class RequestRecord:
    """一次请求的完整记录。"""

    trace_id: str
    entry: str
    method: str
    path: str
    status_code: int
    duration_ms: float
    outcome: Outcome
    failure_category: FailureCategory
    reason: str
    slow: bool
    kept: bool
    """该记录是否被采样保留。全量计数不受此字段影响。"""

    spans: tuple[SpanRecord, ...] = field(default_factory=tuple)

    started_ms: float = 0.0
    """请求进入中间件的时刻（单调时钟，毫秒）。"""

    business_started_ms: float = 0.0
    """业务处理真正开始的时刻（等待结束边界）；0 表示未标记。"""

    recorded_ms: float = 0.0
    """登记到采集器的时刻；供滑动窗口与时长淘汰使用。0 表示由采集器补打。"""


@dataclass(frozen=True)
class EntryAggregation:
    """单个入口的聚合结果。"""

    entry: str
    total: int = 0
    by_outcome: dict[str, int] = field(default_factory=dict)
    by_failure: dict[str, int] = field(default_factory=dict)
    slow_count: int = 0
    error_count: int = 0
    kept_count: int = 0
    total_duration_ms: float = 0.0
    max_duration_ms: float = 0.0

    @property
    def avg_duration_ms(self) -> float:
        return self.total_duration_ms / self.total if self.total else 0.0


@dataclass(frozen=True)
class AggregationReport:
    """聚合报表：全量口径 + 保留明细口径 + 全局计数。"""

    census: dict[str, EntryAggregation]
    """全量口径：按入口聚合，所有请求均计入。"""

    sampled: dict[str, EntryAggregation]
    """明细口径：仅统计 ``kept=True`` 的记录。"""

    total_seen: int
    """经过采集器的请求总数（全量）。"""

    total_kept: int
    """实际保留明细的请求数（``kept=True``）。"""

    sample_rate: float
    """生成报表时生效的普通请求采样率。"""
