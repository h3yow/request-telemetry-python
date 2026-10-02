"""本地观测/控制端点：聚合报表、滑动窗口、慢请求归因、运行期开关。

这些端点挂在 ``/__telemetry__/`` 前缀下，中间件不采集对该前缀的请求，
避免自污染。通过 :func:`register_obs_api` 可装到任意 FastAPI 应用上
（生产应用与测试应用共用同一套端点，避免口径分叉）。
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from .collector import TelemetryCollector


def register_obs_api(app: FastAPI, collector: TelemetryCollector) -> None:
    @app.get("/__telemetry__/report", include_in_schema=False)
    def telemetry_report() -> JSONResponse:
        """返回全量 + 采样两套口径的聚合报表（JSON 可序列化）。"""

        report = collector.aggregate()

        def dump(entry_map: dict) -> dict:
            return {
                entry: {
                    "total": agg.total,
                    "by_outcome": agg.by_outcome,
                    "by_failure": agg.by_failure,
                    "slow_count": agg.slow_count,
                    "error_count": agg.error_count,
                    "kept_count": agg.kept_count,
                    "total_duration_ms": round(agg.total_duration_ms, 3),
                    "max_duration_ms": round(agg.max_duration_ms, 3),
                    "avg_duration_ms": round(agg.avg_duration_ms, 3),
                }
                for entry, agg in entry_map.items()
            }

        return JSONResponse(
            {
                "enabled": collector.enabled(),
                "sample_rate": report.sample_rate,
                "total_seen": report.total_seen,
                "total_kept": report.total_kept,
                "census": dump(report.census),
                "sampled": dump(report.sampled),
            }
        )

    @app.post("/__telemetry__/enabled/{value}", include_in_schema=False)
    def telemetry_set_enabled(value: bool) -> dict[str, bool]:
        """运行期整体开关采集（true/false）。"""

        if value:
            collector.enable()
        else:
            collector.disable()
        return {"enabled": collector.enabled()}

    @app.get("/__telemetry__/window", include_in_schema=False)
    def telemetry_window(entry: str | None = None) -> JSONResponse:
        """滑动窗口内各入口的请求量/失败/慢请求数与 p50/p95/p99。"""

        stats = collector.window_stats(entry)
        return JSONResponse(
            {
                "window_ms": collector.config.window_ms,
                "sample_rate": collector.config.normalized_sample_rate,
                "entries": {
                    name: {
                        "total": s.total,
                        "error_count": s.error_count,
                        "slow_count": s.slow_count,
                        "quantile_sample_size": s.quantile_sample_size,
                        "p50_ms": None if s.p50_ms is None else round(s.p50_ms, 3),
                        "p95_ms": None if s.p95_ms is None else round(s.p95_ms, 3),
                        "p99_ms": None if s.p99_ms is None else round(s.p99_ms, 3),
                        "sampled_at_ms": round(s.sampled_at_ms, 3),
                    }
                    for name, s in stats.items()
                },
            }
        )

    def _dump_span(node, depth: int = 0) -> dict:
        return {
            "span_id": node.span_id,
            "name": node.name,
            "depth": depth,
            "duration_ms": round(node.duration_ms, 3),
            "started_offset_ms": round(node.started_offset_ms, 3),
            "outcome": node.outcome,
            "failure_category": node.failure_category,
            "reason": node.reason,
            "children": [_dump_span(ch, depth + 1) for ch in node.children],
        }

    @app.get("/__telemetry__/slowest", include_in_schema=False)
    def telemetry_slowest(
        limit: int = 10, entry: str | None = None
    ) -> JSONResponse:
        """最慢的一批保留请求及其三段耗时归因明细。"""

        limit = max(0, min(limit, 100))
        items = collector.slowest_attributions(limit=limit, entry=entry)
        return JSONResponse(
            {
                "count": len(items),
                "evicted_details": collector.evicted_details,
                "items": [
                    {
                        "trace_id": a.trace_id,
                        "entry": a.entry,
                        "duration_ms": round(a.duration_ms, 3),
                        "slow": a.slow,
                        "outcome": a.outcome,
                        "failure_category": a.failure_category,
                        "reason": a.reason,
                        "boundary_source": a.boundary_source,
                        "segments": {
                            "wait_ms": round(a.segments.wait_ms, 3),
                            "business_ms": round(a.segments.business_ms, 3),
                            "nested_ms": round(a.segments.nested_ms, 3),
                            "accounted_ms": round(a.segments.accounted_ms, 3),
                        },
                        "spans": [_dump_span(n) for n in a.root_spans],
                    }
                    for a in items
                ],
            }
        )
