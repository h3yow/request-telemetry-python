"""采集配置：开关、采样率、慢请求阈值。"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TelemetryConfig:
    """采集配置。

    - enabled: 采集总开关，关闭后采集为零开销旁路。
    - sample_rate: 普通成功请求的采样率 [0.0, 1.0]；错误与慢请求始终保留。
    - slow_threshold_ms: 慢请求阈值（毫秒），超过即视为慢请求并始终保留。
    """

    enabled: bool = True
    sample_rate: float = 1.0
    slow_threshold_ms: float = 500.0

    def __post_init__(self) -> None:
        if not 0.0 <= self.sample_rate <= 1.0:
            raise ValueError("sample_rate 必须在 [0.0, 1.0] 区间内")
        if self.slow_threshold_ms < 0:
            raise ValueError("slow_threshold_ms 不能为负")
