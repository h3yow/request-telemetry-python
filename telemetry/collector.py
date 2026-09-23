"""采集器：串联采样与聚合，对外提供统一入口。"""
from __future__ import annotations

import threading
from collections import OrderedDict

from .aggregator import AggregateKey, AggregateStats, Aggregator
from .config import TelemetryConfig
from .models import RequestRecord
from .sampler import Sampler

_TRACE_STORE_CAPACITY = 10_000


class Collector:
    """请求采集器。

    - enabled=False 时 record() 立即返回，为零开销旁路，
      不进入采样与聚合路径，对吞吐的影响仅为一次布尔判断。
    - 聚合口径：只统计被保留的记录；错误与慢请求始终被保留，
      因此开关采样前后，错误/慢请求维度的计数完全自洽，
      仅普通成功请求的计数随采样率变化。
    - 归并：被保留的记录同时按 trace_id 归入追踪存储，
      同一请求的多条记录（中间件记录 + 嵌套调用记录）
      可通过 trace_id 归并到同一追踪上下文。
    """

    def __init__(self, config: TelemetryConfig, sampler: Sampler | None = None) -> None:
        self._config = config
        self._sampler = sampler or Sampler(config)
        self._aggregator = Aggregator()
        self._trace_lock = threading.Lock()
        self._traces: OrderedDict[str, list[RequestRecord]] = OrderedDict()

    @property
    def enabled(self) -> bool:
        return self._config.enabled

    def record(self, record: RequestRecord) -> bool:
        """记录一条请求。返回是否被保留（采样命中）。"""
        if not self._config.enabled:
            return False
        if not self._sampler.should_keep(record):
            return False
        self._aggregator.add(record)
        with self._trace_lock:
            bucket = self._traces.setdefault(record.trace_id, [])
            bucket.append(record)
            self._traces.move_to_end(record.trace_id)
            while len(self._traces) > _TRACE_STORE_CAPACITY:
                self._traces.popitem(last=False)
        return True

    def snapshot(self) -> dict[AggregateKey, AggregateStats]:
        return self._aggregator.snapshot()

    def trace_records(self, trace_id: str) -> list[RequestRecord]:
        """按 trace_id 取回归并后的记录列表；不存在时返回空列表。"""
        with self._trace_lock:
            return list(self._traces.get(trace_id, []))
