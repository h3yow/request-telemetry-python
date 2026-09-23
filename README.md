# 请求级可观测性采集与慢请求归因（Python / FastAPI）

在不依赖任何外部服务或真实凭据的前提下，为每个请求统一采集**耗时、结果、
失败原因**，支持按入口与结果维度聚合、确定性采样、整体开关，并保证并发交错、
嵌套调用与客户端中途断开时结论一致且可解释。

## 1. 能力概览

| 能力 | 说明 |
| --- | --- |
| 统一采集 | 每个 HTTP 请求一条 `RequestRecord`：trace、入口、方法、路径、状态码、耗时、结果、失败类别、原因、是否慢请求、是否保留 |
| 失败归类 | `business`（业务异常/4xx）与 `framework`（未处理异常/5xx）严格分开，另有 `client_disconnect`、`none`、`unknown` |
| 追踪归并 | 一次请求一个 `trace_id`，嵌套内部调用产生多个 span，全部携带同一 trace；基于 `contextvars`，并发请求互不串扰 |
| 维度聚合 | 按 **入口 × 结果(outcome) × 失败类别(failure)** 聚合；提供全量与采样两套口径 |
| 采样 | 错误请求与慢请求**始终保留**；普通请求按配置概率、以 trace_id 做确定性采样 |
| 总开关 | 可整体关闭（环境变量或运行期接口），关闭后中间件直接透传 |
| 容错 | 采集内部异常被隔离，不改变业务响应；trace 上下文缺失时稳定降级为 no-op |

## 2. 结果与失败类别（采集口径）

`Outcome`（请求终态）与 `FailureCategory`（失败原因）：

| 场景 | Outcome | FailureCategory | 来源 |
| --- | --- | --- | --- |
| 2xx / 3xx | `success` | `none` | 响应状态码 |
| `BusinessError` 或 4xx | `business_error` | `business` | 业务异常处理器标注 / 状态码兜底 |
| 未处理异常或 5xx | `framework_error` | `framework` | 异常分类 / 状态码兜底 |
| 客户端中途断开 | `client_disconnect` | `client_disconnect` | `http.disconnect` 或任务被 `CancelledError` 取消 |

判定优先级：**客户端断开 > 业务异常 > 框架错误 > 按状态码兜底**。
业务异常与框架级错误永远是不同类别，不会混为一类。

慢请求：`duration_ms > slow_threshold_ms`（严格大于）。

## 3. 两套聚合口径（开启采样后仍自洽）

- **census（全量口径）**：每个经过中间件的请求都恰好计数一次，与采样率无关。
  用于回答"总共发生了什么"。
- **sampled（保留明细口径）**：只统计实际保留下来的记录，用于耗时分位数等
  需要明细的分析。

自洽规则（有测试固化）：

1. 任意采样率下，census 每个入口的 `total / by_outcome / by_failure /
   slow_count / error_count` 完全一致；
2. sampled 中每个结果的计数都是 census 的子集（`<=`）；
3. 错误与慢请求强制保留，因此 sampled 的 `error_count / slow_count`
   **等于** census 对应计数；
4. 同一 `trace_id` 只计数、只保留一次（幂等，防重复/错配）。

采样判定对普通请求生效：`stable_unit(trace_id) < sample_rate`。哈希为
FNV-1a + fmix64 雪崩混合（纯整数、跨进程一致），所以同一 trace 的结论恒定，
且相近 trace_id 也不会分布成块。

## 4. 配置项

### 4.1 环境变量（启动时生效）

| 变量 | 默认 | 含义 |
| --- | --- | --- |
| `TELEMETRY_ENABLED` | `1` | 总开关，`1/true/yes/on` 开，`0/false/no/off` 关 |
| `TELEMETRY_SAMPLE_RATE` | `1.0` | 普通请求保留概率，范围 `[0,1]`；不影响错误/慢请求 |
| `TELEMETRY_SLOW_THRESHOLD_MS` | `500.0` | 慢请求阈值（毫秒），严格大于即判慢 |

示例：

```bash
TELEMETRY_ENABLED=1 TELEMETRY_SAMPLE_RATE=0.1 TELEMETRY_SLOW_THRESHOLD_MS=200 \
  uv run uvicorn main:app --reload
```

### 4.2 运行期开关

- `POST /__telemetry__/enabled/true`：开启采集
- `POST /__telemetry__/enabled/false`：关闭采集
- `GET  /__telemetry__/report`：查看聚合报表（含两套口径、总计数、采样率）

也可在代码中 `collector.enable()` / `collector.disable()` /
`collector.update_config(TelemetryConfig(...))`。

## 5. 对吞吐的影响

采集逻辑全部在进程内完成（内存计数 + 少量 Python 对象），无网络/IO：

- **关闭时**：中间件仅做一次 `enabled()` 判断即透传，近乎零开销，且不产生任何记录；
- **开启全量**：每请求额外做一次 perf_counter 计时、一次终态分类、一次加锁写入；
- **开启采样(rate<1)**：普通请求不构造/保留明细对象，写入更少，开销不高于全量。

`tests/test_overhead.py` 会实测并在日志中打印三种模式的每请求平均耗时
（`关闭 / 采样 / 全量`），并断言"关闭不慢于全量、采样不慢于全量"。
具体数值随机器而异，可用该用例在本机量化。

> 注：当前为进程内内存实现，报表反映单进程数据；重启即清空。设计上聚合器是
> 唯一写入点，后续如需导出到外部后端，只需在 `collector.record` 后追加一个
> 异步 sink，不影响上述口径与并发保证。

## 6. 本地验证

### 安装与启动

```bash
uv sync
uv run uvicorn main:app --reload
```

### 手动打各类入口

```bash
curl -s localhost:8000/                      # success
curl -s localhost:8000/ping                  # success
curl -s localhost:8000/business-error        # 422 business_error / business
curl -s localhost:8000/framework-error       # 500 framework_error / framework
curl -s "localhost:8000/slow?ms=600"         # 慢请求（默认阈值 500ms）
curl -s localhost:8000/nested                # 两个嵌套 span 归并到同一 trace
curl -s localhost:8000/nested-error          # 嵌套内业务异常，逐层可归因
curl -s localhost:8000/__telemetry__/report  # 查看两套口径聚合
```

客户端中途断开（另一个终端）：

```bash
# 请求会等待 5s；立即 Ctrl-C 即触发 http.disconnect / 取消
curl -s "localhost:8000/wait-then-respond?ms=5000"
# 随后查看报表，会出现 client_disconnect 计数
curl -s localhost:8000/__telemetry__/report
```

### 运行测试

```bash
uv run python -m pytest                      # 全部 53 个用例
uv run python -m pytest -q --log-cli-level=DEBUG   # 同时打印每个用例的输入与判定依据
```

测试覆盖：

- `test_classification.py`：业务/框架/断开/状态码兜底分类可区分；
- `test_collector.py`：确定性采样、错误与慢请求强制保留、幂等去重、维度聚合、
  开关、内部异常隔离；
- `test_context.py`：上下文缺失降级、嵌套归并、三类异常链路、并发隔离；
- `test_middleware.py`：各入口终态、span 归并、慢请求、开关透传；
- `test_sampling.py`：不同采样率下 census 完全一致、sampled 可对账、开关切换；
- `test_concurrency.py`：多线程交错下计数无丢失/重复/错配、trace 唯一；
- `test_disconnect.py`：ASGI 层模拟 `http.disconnect` 与 `CancelledError`；
- `test_overhead.py`：三种模式吞吐对比与"无外部依赖"验证。

## 7. 代码结构

```
telemetry/
  config.py         # 配置 + 环境变量加载
  errors.py         # 框架中立的 BusinessError
  classification.py # outcome / failure 分类
  context.py        # TraceContext + contextvars + 嵌套 span
  recorder.py       # RequestRecord / SpanRecord / 聚合报表模型
  collector.py      # 线程安全采集器：计数、采样、聚合、去重
  middleware.py     # ASGI 中间件：计时、建 trace、终态判定、登记
app/
  errors.py         # BusinessError 再导出 + 统一异常处理器
  endpoints.py      # 演示入口
main.py             # 装配应用 + 本地观测端点
tests/              # 上述测试
```
