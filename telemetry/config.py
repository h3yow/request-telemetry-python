"""遥测配置：总开关、采样率、慢请求阈值。

可通过代码构造，也可用 :func:`config_from_env` 从环境变量读取：

- ``TELEMETRY_ENABLED``：``"1"/"true"/"yes"/"on"`` 视为开启，默认开启；
- ``TELEMETRY_SAMPLE_RATE``：``0.0~1.0`` 的普通请求采样率，默认 ``1.0``；
- ``TELEMETRY_SLOW_THRESHOLD_MS``：慢请求阈值（毫秒），默认 ``500.0``。
"""

from __future__ import annotations

import os
from dataclasses import dataclass

_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
_FALSE_VALUES = frozenset({"0", "false", "no", "off"})


@dataclass(frozen=True)
class TelemetryConfig:
    """遥测运行配置（不可变，便于并发下安全读取）。"""

    enabled: bool = True
    """采集总开关。关闭后中间件直接放行，不产生任何记录。"""

    sample_rate: float = 1.0
    """普通（非错误、非慢）请求的保留概率，区间 [0, 1]。"""

    slow_threshold_ms: float = 500.0
    """超过该耗时（严格大于）即判定为慢请求，慢请求始终保留。"""

    def __post_init__(self) -> None:
        if not isinstance(self.enabled, bool):
            raise TypeError("enabled 必须是 bool")
        if not isinstance(self.sample_rate, (int, float)) or isinstance(
            self.sample_rate, bool
        ):
            raise TypeError("sample_rate 必须是数值")
        if not 0.0 <= float(self.sample_rate) <= 1.0:
            raise ValueError("sample_rate 必须位于 [0, 1]")
        if float(self.slow_threshold_ms) < 0:
            raise ValueError("slow_threshold_ms 不能为负")

    @property
    def normalized_sample_rate(self) -> float:
        """钳制到 [0, 1] 后的采样率。"""

        rate = float(self.sample_rate)
        if rate <= 0.0:
            return 0.0
        if rate >= 1.0:
            return 1.0
        return rate


def _parse_bool(raw: str, default: bool) -> bool:
    value = raw.strip().lower()
    if value in _TRUE_VALUES:
        return True
    if value in _FALSE_VALUES:
        return False
    return default


def config_from_env() -> TelemetryConfig:
    """从环境变量构造配置；变量缺失或非法时回退默认值。"""

    enabled = _parse_bool(os.environ.get("TELEMETRY_ENABLED", "1"), True)

    raw_rate = os.environ.get("TELEMETRY_SAMPLE_RATE")
    if raw_rate is None or not raw_rate.strip():
        sample_rate = 1.0
    else:
        try:
            sample_rate = float(raw_rate)
        except ValueError:
            sample_rate = 1.0

    raw_threshold = os.environ.get("TELEMETRY_SLOW_THRESHOLD_MS")
    if raw_threshold is None or not raw_threshold.strip():
        slow_threshold_ms = 500.0
    else:
        try:
            slow_threshold_ms = float(raw_threshold)
        except ValueError:
            slow_threshold_ms = 500.0

    return TelemetryConfig(
        enabled=enabled,
        sample_rate=sample_rate,
        slow_threshold_ms=max(slow_threshold_ms, 0.0),
    )
