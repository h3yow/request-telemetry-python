# 请求级可观测性采集与慢请求归因（Python / FastAPI）

在不依赖任何外部服务或真实凭据的前提下，为每个请求统一采集**耗时、结果、
失败原因**，支持按入口与结果维度聚合、确定性采样、整体开关，并保证并发交错、
嵌套调用与客户端中途断开时结论一致且可解释。

在"按入口汇总"的基础上，本轮新增：

- **滑动时间窗口指标**：窗口长度可配置，滚动查看每个入口最近一段时间的
  请求量、失败数、慢请求数与 p50/p95/p99；
- **慢请求耗时归因**：把一次慢请求拆成"业务前等待 / 业务自身 / 嵌套调用"
  三段，提供 span 树明细，可查最慢的一批请求；
- **明细容量与保留边界**：保留明细同时有条数上限与时长上限，超了按确定
  规则淘汰，全量计数不受淘汰影响。

## 1. 能力概览

| 能力 | 说明 |
| --- | --- |
| 统一采集 | 每个 HTTP 请求一条 `RequestRecord`：trace、入口、方法、路径、状态码、耗时、结果、失败类别、原因、是否慢请求、是否保留 |
| 失败归类 | `business`（业务异常/4xx）与 `framework`（未处理异常/5xx）严格分开，另有 `client_disconnect`、`none`、`unknown` |
| 追踪归并 | 一次请求一个 `trace_id`，嵌套内部调用产生多个 span，全部携带同一 trace；基于 `contextvars`，并发请求互不串扰 |
| 维度聚合 | 按 **入口 × 结果(outcome) × 失败类别(failure)** 聚合；提供全量与采样两套口径 |
| 滑动窗口 | 每个入口一份滚动窗口：请求量/失败数/慢请求数（全量）+ p50/p95/p99（保留样本） |
| 慢请求归因 | 等待 / 业务自身 / 嵌套调用三段 + span 树；可查最慢 N 条与按 trace 查明细 |
| 容量边界 | 明细按 `max_records`（条数，FIFO）+ `max_age_ms`（时长）淘汰；只影响明细 |
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

## 4. 滑动时间窗口指标

`TelemetryCollector.window_stats()` / `GET /__telemetry__/window`
返回每个入口在最近 `window_ms`（默认 60 秒，可配置）内的：

| 字段 | 口径 |
| --- | --- |
| `total` | 窗口内请求量，**全量口径**（与采样率无关，可与 census 对账） |
| `error_count` | 窗口内失败数，**全量口径**（`failure != none`） |
| `slow_count` | 窗口内慢请求数，**全量口径** |
| `quantile_sample_size` | 参与分位数计算的样本条数（**保留明细口径**，恒 `<= total`） |
| `p50_ms / p95_ms / p99_ms` | 基于窗口内保留样本的线性插值分位数（Type-7）；无样本为 `null` |

滚动规则：

- 窗口是**滑动而非分桶**：新请求登记即入窗；查询/写入时按当前单调时钟
  滑掉 `ts < now - window_ms` 的旧事件（边界值保留，即 `>=` 仍在窗内）；
- 内部为每个入口维护两条同源流：**事件流**（全量计数）与**耗时样本流**
  （仅保留明细的请求）。两条流在同一把锁、同一临界区写入，因此高并发或
  客户端断开混入时，计数与样本不会错配，且样本数永远不超过事件数；
- 采样率为 1 时两条流等长；采样率为 0 时 `total/error/slow` 仍准确，
  分位数为 `null`（此时 `quantile_sample_size=0`，口径不会假装是全量）；
- 窗口只在内存中保留窗口长度内的数据，内存占用随窗口长度、而非历史总量增长。

分位数定义：样本升序排序后 `rank = (p/100)*(n-1)`，在相邻两值间线性插值
（与 numpy 默认 `percentile` 一致）。单样本时所有分位数等于其自身。

## 5. 慢请求耗时归因

`slowest_attributions(limit)` / `attribute_trace(trace_id)` /
`GET /__telemetry__/slowest` 返回最慢一批请求的三段分解与 span 树。

| 段 | 含义 |
| --- | --- |
| `wait_ms` | 请求进入中间件 → 业务真正开始之间的等待（排队/框架前置处理） |
| `nested_ms` | 业务直接发起的**根 span**（`parent_span_id is None`）耗时之和；子 span 不重复计 |
| `business_ms` | 总耗时 − wait − nested（含测量残差） |

关键保证：

- **对账恒等式** `wait + business + nested == duration_ms`：业务段吸收
  浮点尾差与未闭合残差（倒挂超过 `1e-6ms` 时嵌套段保守钳减，不允许负段）。
  测试逐条断言该恒等式，不会凭空多出或少掉时间；
- **业务开始边界**由内层 ASGI 中间件 `BusinessStartMiddleware` 在进入业务
  前一次性标记（`contextvars` 内的时间戳）。不依赖"第一次 receive"
  （uvicorn 对无 body 的 GET 可能不调 receive），口径稳定；
- 标记缺失时按"最早根 span 起点"退化（`boundary_source=root_span`），
  完全无 span 时全部计入业务段（`boundary_source=none`），仍满足恒等式；
- span 树保留每个内部调用的名称、相对起点偏移、耗时、终态与原因，
  嵌套业务异常（如 `/nested-error`）也能逐层归因。

端点返回里的数值统一保留 3 位小数，因此 JSON 上段之和与总耗时可能有
至多约 `1e-3ms` 的四舍五入差异；采集器内部用全精度，恒等式严格成立。

## 6. 明细容量与淘汰规则

保留明细（`_records`）同时受两个上限约束，二者叠加生效：

1. **条数上限** `max_records`（默认 `1000`）：超出时最旧的记录先淘汰
   （FIFO）；`0` 表示一条明细都不留；
2. **时长上限** `max_age_ms`（默认 `3600000`，1 小时）：早于
   `now - max_age_ms` 的记录淘汰（边界值保留）；`0` 表示不按时间淘汰。

淘汰特性（有测试固化）：

- 结果恒为原序列的一个**后缀**，规则只取决于时间与位置，与数据内容无关；
- 淘汰在写入临界区内完成，查询接口不会看到半淘汰状态，也不会被淘汰拖住；
- **淘汰只删明细**：census 全量计数、trace 去重集合、窗口事件流都不参与
  淘汰。因此高峰把内存压到上限后，全量计数仍不丢失、不重复
  （已淘汰 trace 重放也不会让 census 多加一次）；
- `evicted_details` 暴露累计淘汰条数，`/slowest` 响应中也会带上。

口径与开关/采样的关系：

- **关闭采集**：不计数、不入窗、不留明细，`window_stats()` 为空；
- **采样率 < 1**：census 与窗口的 `total/error/slow` 是全量；分位数与
  慢请求明细只覆盖保留集合（错误、慢请求必在其中）；
- **`max_records=0`**：census 与窗口计数照常，仅无任何明细/分位样本。

## 7. 配置项

### 7.1 环境变量（启动时生效）

| 变量 | 默认 | 含义 |
| --- | --- | --- |
| `TELEMETRY_ENABLED` | `1` | 总开关，`1/true/yes/on` 开，`0/false/no/off` 关 |
| `TELEMETRY_SAMPLE_RATE` | `1.0` | 普通请求保留概率，范围 `[0,1]`；不影响错误/慢请求 |
| `TELEMETRY_SLOW_THRESHOLD_MS` | `500.0` | 慢请求阈值（毫秒），严格大于即判慢 |
| `TELEMETRY_WINDOW_MS` | `60000` | 滑动窗口长度（毫秒，必须为正） |
| `TELEMETRY_MAX_RECORDS` | `1000` | 保留明细条数上限；`0`=不保留明细 |
| `TELEMETRY_MAX_AGE_MS` | `3600000` | 保留明细时长上限（毫秒）；`0`=不按时间淘汰 |

示例：

```bash
TELEMETRY_ENABLED=1 TELEMETRY_SAMPLE_RATE=0.1 \
TELEMETRY_WINDOW_MS=30000 TELEMETRY_MAX_RECORDS=5000 \
TELEMETRY_MAX_AGE_MS=600000 \
  uv run uvicorn main:app --reload
```

### 7.2 运行期接口

- `POST /__telemetry__/enabled/true`、`/enabled/false`：开关采集
  （只切开关，窗口长度/容量/采样率原样保留）
- `GET  /__telemetry__/report`：全量 + 采样两套口径聚合
- `GET  /__telemetry__/window?entry=...`：滑动窗口指标（可选入口过滤）
- `GET  /__telemetry__/slowest?limit=10&entry=...`：最慢请求归因明细
  （`limit` 服务端钳制在 `[0,100]`）

代码侧可用 `collector.window_stats()`、`collector.slowest_attributions()`、
`collector.attribute_trace(trace_id)`、`collector.evicted_details`。
时钟可在构造采集器时以 `clock_ms=` 注入（测试用可控时钟）。

## 8. 对吞吐的影响

采集逻辑全部在进程内完成（内存计数 + 少量 Python 对象），无网络/IO：

- **关闭时**：中间件仅做一次 `enabled()` 判断即透传，近乎零开销，且不产生任何记录；
- **开启全量**：每请求额外做一次 perf_counter 计时、一次终态分类、一次加锁写入
  （含一次窗口追加与按需淘汰，均为 deque/列表 O(1) 操作）；
- **开启采样(rate<1)**：普通请求只写窗口事件、不构造/保留明细对象，开销不高于全量。

`tests/test_overhead.py` 会实测并在日志中打印三种模式的每请求平均耗时
（`关闭 / 采样 / 全量`），并断言"关闭不慢于全量、采样不慢于全量"。
具体数值随机器而异，可用该用例在本机量化。

> 注：当前为进程内内存实现，报表反映单进程数据；重启即清空。聚合器是
> 唯一写入点，后续如需导出到外部后端，只需在 `collector.record` 后追加一个
> 异步 sink，不影响上述口径与并发保证。

## 9. 本地验证

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
curl -s localhost:8000/__telemetry__/report  # 两套口径聚合
curl -s localhost:8000/__telemetry__/window  # 滑动窗口 p50/p95/p99
curl -s "localhost:8000/__telemetry__/slowest?limit=5"   # 最慢请求三段归因
```

客户端中途断开（另一个终端）：

```bash
curl -s "localhost:8000/wait-then-respond?ms=5000"   # 立即 Ctrl-C
curl -s localhost:8000/__telemetry__/window
```

### 运行测试

```bash
uv run python -m pytest                      # 全部用例（原有用例 + 新增）
uv run python -m pytest -q --log-cli-level=DEBUG   # 同时打印每个用例的输入与判定依据
```

新增/相关测试覆盖：

- `test_quantile.py`：空样本/单样本、线性插值口径、入参不被修改、非法 p 拒绝；
- `test_window.py`：窗口滚动（边界保留）、计数与分位各归其流、8 线程并发下
  样本数永不超过事件数；
- `test_window_semantics.py`：采样率 0 下窗口计数仍为全量、错误/慢强制进
  分位样本、窗口与 census 对账及滚动、时长淘汰只删明细、最慢查询与按 trace
  归因、关闭时口径为空、开关不重置窗口/容量配置；
- `test_retention.py`：FIFO 条数淘汰、时长边界（边界保留）、容量+时长叠加、
  `max_records=0`、非法策略拒绝、淘汰后 census 不丢不重；
- `test_attribution.py`：三段恒等式、只计根 span 不重复、边界退化、
  残差倒挂钳制；
- `test_window_http.py`：`/window`、`/slowest` 端到端、真实 `/nested`
  span 树与三段对账、过滤/limit/空入口；
- 原有 `test_classification / test_collector / test_context / test_middleware
  / test_sampling / test_concurrency / test_disconnect / test_overhead`
  全部保留不回归。

## 10. 代码结构

```
telemetry/
  config.py         # 配置 + 环境变量加载（开关/采样/阈值/窗口/容量）
  errors.py         # 框架中立的 BusinessError
  classification.py # outcome / failure 分类
  context.py        # TraceContext + contextvars + 嵌套 span + 业务开始标记
  recorder.py       # RequestRecord / SpanRecord / 聚合报表模型
  quantile.py       # 确定性线性插值分位数（纯函数）
  window.py         # SlidingWindowStore：事件流 + 耗时样本流
  retention.py      # RetentionPolicy + 条数/时长淘汰边界
  attribution.py    # 三段归因 + span 树（恒等式可对账）
  collector.py      # 线程安全采集器：计数、采样、窗口、淘汰、聚合、查询
  middleware.py     # ASGI 中间件：计时、业务边界、终态判定、登记
  obsapi.py         # /__telemetry__/report|window|slowest|enabled 端点
app/
  errors.py         # BusinessError 再导出 + 统一异常处理器
  endpoints.py      # 演示入口
main.py             # 装配应用 + 本地观测端点
tests/              # 上述测试
```

## 11. 兼容范围

- 新增配置项（窗口长度、条数/时长上限）均有默认值；旧的
  `TelemetryConfig(enabled=..., sample_rate=..., slow_threshold_ms=...)`
  构造方式无需修改即可继续工作；
- `RequestRecord` / `SpanRecord` 仅新增**带默认值**的可选时间戳字段，
  旧的关键字构造调用不受影响；
- 新增能力均为增量（新模块、新端点、新采集器方法），`/report`、
  开关接口与既有采集口径保持不变；
- 明细默认容量 `1000` 条：依赖"无限保留明细"的旧测试/脚本如需保留更多，
  请显式调大 `TELEMETRY_MAX_RECORDS` 或 `max_records`（全量计数不受影响）。
