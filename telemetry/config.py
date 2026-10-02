"""遥测配置：总开关、采样率、慢请求阈值、窗口长度、明细容量与保留时长。

可通过代码构造，也可用 :func:`config_from_env` 从环境变量读取：

- ``TELEMETRY_ENABLED``：``"1"/"true"/"yes"/"on"`` 视为开启，默认开启；
- ``TELEMETRY_SAMPLE_RATE``：``0.0~1.0`` 的普通请求采样率，默认 ``1.0``；
- ``TELEMETRY_SLOW_THRESHOLD_MS``：慢请求阈值（毫秒），默认 ``500.0``；
- ``TELEMETRY_WINDOW_MS``：滑动时间窗口长度（毫秒），默认 ``60000``；
- ``TELEMETRY_MAX_RECORDS``：保留明细条数上限，默认 ``1000``（0=不保留明细）；
- ``TELEMETRY_MAX_AGE_MS``：保留明细时长上限（毫秒），默认 ``3600000``
  （0=不按时间淘汰）。

窗口与淘汰只影响"明细/窗口"类指标，全量计数（census）不受容量影响。
"""

from __future__ import annotations

import os
from dataclasses import dataclass, replace

_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
_FALSE_VALUES = frozenset({"0", "false", "no", "off"})

DEFAULT_WINDOW_MS = 60_000.0
DEFAULT_MAX_RECORDS = 1000
DEFAULT_MAX_AGE_MS = 60 * 60 * 1000.0


@dataclass(frozen=True)
class TelemetryConfig:
    """遥测运行配置（不可变，便于并发下安全读取）。"""

    enabled: bool = True
    """采集总开关。关闭后中间件直接放行，不产生任何记录。"""

    sample_rate: float = 1.0
    """普通（非错误、非慢）请求的保留概率，区间 [0, 1]。"""

    slow_threshold_ms: float = 500.0
    """超过该耗时（严格大于）即判定为慢请求，慢请求始终保留。"""

    window_ms: float = DEFAULT_WINDOW_MS
    """滑动时间窗口长度（毫秒，必须为正）。"""

    max_records: int = DEFAULT_MAX_RECORDS
    """保留明细条数上限（>=0；0 表示不保留任何明细）。"""

    max_age_ms: float = DEFAULT_MAX_AGE_MS
    """保留明细时长上限（毫秒，>=0；0 表示不按时间淘汰）。"""

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
        if not isinstance(self.window_ms, (int, float)) or isinstance(
            self.window_ms, bool
        ):
            raise TypeError("window_ms 必须是数值")
        if float(self.window_ms) <= 0:
            raise ValueError("window_ms 必须为正数")
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

    @property
    def normalized_sample_rate(self) -> float:
        """钳制到 [0, 1] 后的采样率。"""

        rate = float(self.sample_rate)
        if rate <= 0.0:
            return 0.0
        if rate >= 1.0:
            return 1.0
        return rate

    def with_enabled(self, enabled: bool) -> "TelemetryConfig":
        """只切换总开关，其余配置（窗口/容量/采样）原样保留。"""

        return replace(self, enabled=enabled)


def _parse_bool(raw: str, default: bool) -> bool:
    value = raw.strip().lower()
    if value in _TRUE_VALUES:
        return True
    if value in _FALSE_VALUES:
        return False
    return default


def _parse_float(raw: str | None, default: float) -> float:
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _parse_int(raw: str | None, default: int) -> int:
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def config_from_env() -> TelemetryConfig:
    """从环境变量构造配置；变量缺失或非法时回退默认值。"""

    enabled = _parse_bool(os.environ.get("TELEMETRY_ENABLED", "1"), True)
    sample_rate = _parse_float(os.environ.get("TELEMETRY_SAMPLE_RATE"), 1.0)
    slow_threshold_ms = _parse_float(
        os.environ.get("TELEMETRY_SLOW_THRESHOLD_MS"), 500.0
    )
    window_ms = _parse_float(os.environ.get("TELEMETRY_WINDOW_MS"), DEFAULT_WINDOW_MS)
    max_records = _parse_int(os.environ.get("TELEMETRY_MAX_RECORDS"), DEFAULT_MAX_RECORDS)
    max_age_ms = _parse_float(os.environ.get("TELEMETRY_MAX_AGE_MS"), DEFAULT_MAX_AGE_MS)

    return TelemetryConfig(
        enabled=enabled,
        sample_rate=max(0.0, min(1.0, sample_rate)),
        slow_threshold_ms=max(slow_threshold_ms, 0.0),
        window_ms=max(window_ms, 1e-9),
        max_records=max(max_records, 0),
        max_age_ms=max(max_age_ms, 0.0),
    )
