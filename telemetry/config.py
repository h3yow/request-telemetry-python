"""遥测配置：总开关、采样率、慢请求阈值、窗口与明细容量。

可通过代码构造，也可用 :func:`config_from_env` 从环境变量读取：

- ``TELEMETRY_ENABLED``：``"1"/"true"/"yes"/"on"`` 视为开启，默认开启；
- ``TELEMETRY_SAMPLE_RATE``：``0.0~1.0`` 的普通请求采样率，默认 ``1.0``；
- ``TELEMETRY_SLOW_THRESHOLD_MS``：慢请求阈值（毫秒），默认 ``500.0``；
- ``TELEMETRY_WINDOW_SECONDS``：滑动窗口长度（秒），默认 ``300.0``；
- ``TELEMETRY_WINDOW_MAX_SAMPLES``：窗口轻量样本条数硬上限，默认 ``10000``；
- ``TELEMETRY_DETAIL_MAX_RECORDS``：保留明细条数硬上限，默认 ``1000``；
- ``TELEMETRY_SLOW_TOP_N``：慢请求归因查询默认返回条数，默认 ``20``。

非法环境变量一律回退默认值，绝不阻断启动。
"""

from __future__ import annotations

import os
from dataclasses import dataclass

_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
_FALSE_VALUES = frozenset({"0", "false", "no", "off"})

# 默认值集中定义，供采集器与环境变量解析共用。
DEFAULT_WINDOW_SECONDS = 300.0
DEFAULT_WINDOW_MAX_SAMPLES = 10_000
DEFAULT_DETAIL_MAX_RECORDS = 1_000
DEFAULT_SLOW_TOP_N = 20


@dataclass(frozen=True)
class TelemetryConfig:
    """遥测运行配置（不可变，便于并发下安全读取）。"""

    enabled: bool = True
    """采集总开关。关闭后中间件直接放行，不产生任何记录。"""

    sample_rate: float = 1.0
    """普通（非错误、非慢）请求的保留概率，区间 [0, 1]。"""

    slow_threshold_ms: float = 500.0
    """超过该耗时（严格大于）即判定为慢请求，慢请求始终保留。"""

    window_seconds: float = DEFAULT_WINDOW_SECONDS
    """滑动窗口长度（秒），必须严格为正。"""

    window_max_samples: int = DEFAULT_WINDOW_MAX_SAMPLES
    """窗口轻量样本条数硬上限（FIFO 淘汰），必须严格为正。"""

    detail_max_records: int = DEFAULT_DETAIL_MAX_RECORDS
    """保留明细条数硬上限（FIFO 淘汰），必须严格为正。"""

    slow_top_n: int = DEFAULT_SLOW_TOP_N
    """慢请求归因查询默认返回条数，必须严格为正。"""

    def __post_init__(init_self) -> None:  # noqa: N805 - dataclass 方法名
        if not isinstance(init_self.enabled, bool):
            raise TypeError("enabled 必须是 bool")
        if not isinstance(init_self.sample_rate, (int, float)) or isinstance(
            init_self.sample_rate, bool
        ):
            raise TypeError("sample_rate 必须是数值")
        if not 0.0 <= float(init_self.sample_rate) <= 1.0:
            raise ValueError("sample_rate 必须位于 [0, 1]")
        if float(init_self.slow_threshold_ms) < 0:
            raise ValueError("slow_threshold_ms 不能为负")
        if (
            not isinstance(init_self.window_seconds, (int, float))
            or isinstance(init_self.window_seconds, bool)
            or init_self.window_seconds <= 0
        ):
            raise ValueError("window_seconds 必须为正数")
        for name in ("window_max_samples", "detail_max_records", "slow_top_n"):
            value = getattr(init_self, name)
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise ValueError(f"{name} 必须为正整数")

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


def _parse_float(raw: str | None, default: float) -> float:
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _parse_positive_int(raw: str | None, default: int) -> int:
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def config_from_env() -> TelemetryConfig:
    """从环境变量构造配置；变量缺失或非法时回退默认值。"""

    enabled = _parse_bool(os.environ.get("TELEMETRY_ENABLED", "1"), True)
    sample_rate = _parse_float(os.environ.get("TELEMETRY_SAMPLE_RATE"), 1.0)
    slow_threshold_ms = max(
        _parse_float(os.environ.get("TELEMETRY_SLOW_THRESHOLD_MS"), 500.0), 0.0
    )
    window_seconds = _parse_float(
        os.environ.get("TELEMETRY_WINDOW_SECONDS"), DEFAULT_WINDOW_SECONDS
    )
    if window_seconds <= 0:
        window_seconds = DEFAULT_WINDOW_SECONDS
    window_max_samples = _parse_positive_int(
        os.environ.get("TELEMETRY_WINDOW_MAX_SAMPLES"), DEFAULT_WINDOW_MAX_SAMPLES
    )
    detail_max_records = _parse_positive_int(
        os.environ.get("TELEMETRY_DETAIL_MAX_RECORDS"), DEFAULT_DETAIL_MAX_RECORDS
    )
    slow_top_n = _parse_positive_int(
        os.environ.get("TELEMETRY_SLOW_TOP_N"), DEFAULT_SLOW_TOP_N
    )

    # 采样率非法（越界）时回退默认，避免环境变量直接打爆构造器。
    if not 0.0 <= sample_rate <= 1.0:
        sample_rate = 1.0

    return TelemetryConfig(
        enabled=enabled,
        sample_rate=sample_rate,
        slow_threshold_ms=slow_threshold_ms,
        window_seconds=window_seconds,
        window_max_samples=window_max_samples,
        detail_max_records=detail_max_records,
        slow_top_n=slow_top_n,
    )
