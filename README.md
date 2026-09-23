# request-telemetry-python

基于 FastAPI 的请求级可观测性采集与慢请求归因服务。不依赖任何外部服务或真实凭据，全部能力在进程内完成。

## 采集口径

每个请求由 `TelemetryMiddleware` 统一采集一条记录（`RequestRecord`），字段包括：

| 字段 | 含义 |
| --- | --- |
| `trace_id` | 追踪标识：优先取请求头 `x-trace-id`，否则自动生成；同一请求的多条记录以此归并 |
| `route` / `method` | 入口（路径）与方法 |
| `status_code` | 响应状态码（客户端断开记为 499） |
| `duration_ms` | 请求耗时（毫秒） |
| `outcome` | `success` / `failure` |
| `failure_category` | 失败类别，见下表 |
| `failure_reason` | 失败原因描述 |

### 失败分类（业务异常与框架级错误严格区分）

| 判定依据 | 类别 |
| --- | --- |
| 抛出 `BusinessError`，或响应状态码 4xx | `business`（业务异常） |
| 未捕获异常，或响应状态码 5xx | `framework`（框架级错误） |
| 客户端中途断开 | `client_disconnect` |
| 其余 | `none`（成功） |

客户端断开有两条确定性的检测路径，任一命中即归类为 `client_disconnect`（状态记 499）：

1. 应用读取请求体时抛出 `ClientDisconnect`（如上传中途断开），中间件归类后重抛；
2. 处理期间客户端断开：响应生成后中间件用 `request.is_disconnected()` 非阻塞探测
   （uvicorn 在连接关闭后会让 `receive()` 立即返回 `http.disconnect`），命中即归类。

### 追踪上下文与归并

- 基于 `contextvars`：同一请求内的嵌套调用共享同一 `trace_id`；并发请求各自持有独立副本，互不串扰。
- 请求内可通过 `emit_nested_record(collector, ...)` 补采嵌套调用记录，这些记录与中间件记录按 `trace_id` 归并，可通过 `GET /telemetry/trace/{trace_id}` 查询。
- 上下文缺失时行为明确且稳定：`ensure_trace_id()` 自动生成新 id，不抛异常。
- 采集本身的异常被中间件吞掉并记日志：单次采集失败不影响请求，也不影响整体采集。

### 聚合

聚合维度为 **入口 × 结果 × 失败类别**（`AggregateKey`），统计 `count` / `avg_duration_ms` / `max_duration_ms`。所有计数更新在同一把锁内完成，并发交错时不丢失、不重复、不错配。查询入口：`GET /telemetry/stats`。

## 采样与开关配置

`TelemetryConfig`（`main.py` 中通过环境变量加载）：

| 配置项 | 环境变量 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `enabled` | `TELEMETRY_ENABLED` | `true` | 采集总开关 |
| `sample_rate` | `TELEMETRY_SAMPLE_RATE` | `1.0` | 普通成功请求采样率 `[0.0, 1.0]` |
| `slow_threshold_ms` | `TELEMETRY_SLOW_THRESHOLD_MS` | `500` | 慢请求阈值（毫秒） |

采样判定优先级：

1. **失败请求始终保留**（任何 `failure_category != none`）；
2. **慢请求始终保留**（`duration_ms >= slow_threshold_ms`）；
3. 普通成功请求按 `sample_rate` 概率保留。

因此开启采样后聚合口径仍自洽：错误与慢请求维度的计数与全量采集完全一致，仅普通成功请求的计数随采样率变化。

### 开关对吞吐的影响

`enabled=false` 时中间件直接旁路（`record()` 为一次布尔判断即返回），不进入采样与聚合路径，对吞吐的影响可忽略；开启时每个请求的开销为一次采样判定加一次加锁计数（O(1)）。

## 本地验证

```bash
uv sync                 # 安装依赖
uv run pytest tests/ -q # 运行测试（日志打印输入与判定依据）
```

启动服务并手工验证：

```bash
uv run uvicorn main:app --port 8000

curl http://127.0.0.1:8000/ok                # 成功
curl http://127.0.0.1:8000/business-error    # 业务异常 -> business
curl http://127.0.0.1:8000/framework-error   # 框架错误 -> framework
curl -H "x-trace-id: t1" http://127.0.0.1:8000/nested   # 嵌套调用归并
curl http://127.0.0.1:8000/slow              # 慢请求（始终保留）
curl -m 1 "http://127.0.0.1:8000/disconnect?delay=2"   # 客户端 1 秒断开，处理结束后
                                             # 归类为 client_disconnect（状态记 499）

curl http://127.0.0.1:8000/telemetry/stats        # 聚合快照
curl http://127.0.0.1:8000/telemetry/trace/t1     # 按 trace_id 归并查询
```

## 测试覆盖

- `tests/test_classification.py`：成功 / 业务异常 / 框架错误 / 4xx·5xx 的分类口径；
- `tests/test_concurrency.py`：并发交错下计数精确、trace 归并不错配、混合结果分桶正确；
- `tests/test_disconnect.py`：客户端断开分类与重抛、三类失败可区分、采集异常不影响请求、上下文缺失行为稳定；
- `tests/test_sampling.py`：采样开关前后错误/慢请求计数一致、总开关关闭零采集、部分采样率下错误仍 100% 保留。

## 目录结构

```
telemetry/
  config.py       # 配置（开关 / 采样率 / 慢请求阈值）
  models.py       # RequestRecord、Outcome、FailureCategory
  context.py      # 基于 contextvars 的追踪上下文
  sampler.py      # 采样判定
  aggregator.py   # 线程安全聚合
  collector.py    # 采集器（采样 + 聚合 + trace 归并存储）
  middleware.py   # FastAPI 中间件与 BusinessError
main.py           # 应用装配与演示路由
tests/            # 测试
```
