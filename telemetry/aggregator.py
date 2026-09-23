"""聚合器：按入口与结果维度聚合，线程安全。"""
from __future__ import annotations

import threading
from dataclasses import dataclass

from .models import FailureCategory, Outcome, RequestRecord


@dataclass(frozen=True)
class AggregateKey:
    """聚合维度：入口（路由）× 结果 × 失败类别。"""

    route: str
    outcome: Outcome
    failure_category: FailureCategory


@dataclass
class AggregateStats:
    count: int = 0
    total_duration_ms: float = 0.0
    max_duration_ms: float = 0.0

    @property
    def avg_duration_ms(self) -> float:
        return self.total_duration_ms / self.count if self.count else 0.0


class Aggregator:
    """线程安全聚合器。

    所有计数更新都在同一把锁内完成，保证并发交错时
    计数不丢失、不重复、不错配（记录只按自身维度归桶）。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._stats: dict[AggregateKey, AggregateStats] = {}

    def add(self, record: RequestRecord) -> None:
        key = AggregateKey(
            route=record.route,
            outcome=record.outcome,
            failure_category=record.failure_category,
        )
        with self._lock:
            stats = self._stats.setdefault(key, AggregateStats())
            stats.count += 1
            stats.total_duration_ms += record.duration_ms
            if record.duration_ms > stats.max_duration_ms:
                stats.max_duration_ms = record.duration_ms

    def snapshot(self) -> dict[AggregateKey, AggregateStats]:
        """返回聚合结果的一致性快照（拷贝，调用方可安全遍历）。"""
        with self._lock:
            return {
                key: AggregateStats(
                    count=s.count,
                    total_duration_ms=s.total_duration_ms,
                    max_duration_ms=s.max_duration_ms,
                )
                for key, s in self._stats.items()
            }

    def total_count(self) -> int:
        with self._lock:
            return sum(s.count for s in self._stats.values())
