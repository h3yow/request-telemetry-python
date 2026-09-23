"""采样决策：错误与慢请求始终保留，普通请求按采样率。"""
from __future__ import annotations

import random
from typing import Callable

from .config import TelemetryConfig
from .models import FailureCategory, RequestRecord


class Sampler:
    """采样器。

    判定优先级（判定依据按此顺序）：
    1. 失败请求（failure_category != NONE）→ 始终保留；
    2. 慢请求（duration_ms >= slow_threshold_ms）→ 始终保留；
    3. 普通成功请求 → 以 sample_rate 概率保留。

    rng 可注入以便测试确定性。
    """

    def __init__(self, config: TelemetryConfig, rng: Callable[[], float] | None = None) -> None:
        self._config = config
        self._rng = rng or random.random

    def should_keep(self, record: RequestRecord) -> bool:
        if record.failure_category is not FailureCategory.NONE:
            return True
        if record.duration_ms >= self._config.slow_threshold_ms:
            return True
        return self._rng() < self._config.sample_rate
