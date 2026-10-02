# 请求级可观测性采集与慢请求归因（Python / FastAPI）

在不依赖任何外部服务或真实凭据的前提下，为每个请求统一采集**耗时、结果、
失败原因**，支持按入口与结果维度聚合、确定性采样、整体开关，并保证并发交错、
嵌套调用与客户端中途断开时结论一致且可解释。

本轮在此之上新增"把慢看得更细"的能力：

- **滑动时间窗口**：窗口长度可配置，滚动统计最近一段时间内每个入口的请求量、
  失败数、慢请求数与耗时分位数（p50/p95/p99）；新请求即刻计入、滑出窗口
  自然退去；
- **慢请求耗时归因**：把一个慢请求的时间拆成"业务前等待 / 业务处理本身 /
  内部嵌套调用 / 回响应 / 对账残差"几段，并支持查询最慢的一批请求的归因明细，
  各段之和与总耗时严格对得上；
- **明确的容量与淘汰边界**：明细与窗口样本都有硬上限，超限按 FIFO 确定性
  淘汰，高峰不撑爆内存、查询不被卡住；淘汰只影响明细/窗口，全量计数不丢不重。

## 1. 能力概览

| 能力 | 说明 |
| --- | --- |
| 统一采集 | 每个 HTTP 请求一条 `RequestRecord`：trace、入口、方法、路径、状态码、耗时、结果、失败类别、原因、是否慢请求、是否保留 |
| 失败归类 | `business`（业务异常/4xx）与 `framework`（未处理异常/5xx）严格分开，另有 `client_disconnect`、`none`、`unknown` |
| 追踪归并 | 一次请求一个 `trace_id`，嵌套内部调用产生多个 span，全部携带同一 trace；基于 `contextvars`，并发请求互不串扰 |
| 维度聚合 | 按 **入口 × 结果(outcome) × 失败类别(failure)** 聚合；提供全量与采样两套口径 |
| 滑动窗口 | 可配置窗口长度，滚动给出每个入口的 count/error/slow 与 p50/p95/p99；**全量口径**，与采样无关 |
| 耗时归因 | 端到端耗时拆成 queue/business（自身+嵌套调用树）/response/other，各段之和严格等于总耗时；可查最慢 Top N |
| 容量淘汰 | 明细 `detail_max_records`、窗口样本 `window_max_samples` 双硬上限，超限 FIFO 淘汰，规则确定、可观测 |
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

## 3A. 滑动时间窗口（滚动统计）

`SlidingWindow`（`telemetry/window.py`）以请求**完成时刻**（单调时钟
`perf_counter`，秒）为时间轴：

- 每个进入采集器的请求在完成的同一临界区内写入一份**轻量样本**
  （仅含完成时刻、入口、耗时、是否失败、是否慢），写入后下一次查询**立即可见**；
- 查询时惰性淘汰早于 `now - window_seconds` 的样本（边界左闭：
  `ts >= now - window_seconds`），旧请求自然滑出，无需后台线程；
- 输出每个入口的 `count / error_count / slow_count` 与 `p50_ms / p95_ms /
  p99_ms`。

**口径：窗口是全量口径。** 窗口样本的写入与采样率、明细保留完全无关——即使
`sample_rate=0`，窗口里的 count/error/slow/分位数仍覆盖全部请求。窗口滑出
只影响窗口自身，不影响 census（census 是进程生命周期累计值）。

**分位数定义：nearest-rank**，即升序后取第 `ceil(p*n)` 个（1 基），空窗口
为 `0.0`。该定义无需近似结构、结果可复算；例如 `n=100` 时 p50/p95/p99
分别是第 50/95/99 个值。

**并发一致性**：窗口样本写入与 census 计数在同一把锁、同一个临界区完成，
高并发或错误/慢请求重叠时，同一批请求的 `count` 恒等于窗口内 census 增量，
`error_count + 其他 = count` 不会互相打架（有 8×250 并发用例固化）。

## 3B. 慢请求耗时归因

`build_attribution()`（`telemetry/attribution.py`）把端到端总耗时拆成可对账
的几段。计时点全部在 ASGI 中间件用 `perf_counter` 实测：

| 计时点 | 含义 |
| --- | --- |
| `t0` | 进入中间件 |
| `t_business` | 下游**首次 receive 返回**（框架拿到请求、进入业务）；应用在取请求前就失败时回退为 `t0`（await 等待本身计入 queue） |
| `t_response` | 首条 `http.response.start` 发出（业务处理结束、开始回响应） |
| `t_end` | 中间件收尾 |

于是分段为：

- **queue**（业务前等待）= `t_business - t0`；
- **business**（业务窗口）= `t_response - t_business`；没有响应首字节的异常/
  断开路径则为 `t_end - t_business`（业务窗口延伸到收尾，response=0）；
- **response**（回响应/清理）= `t_end - t_response`，异常路径为 0；
- **嵌套调用**：span 按 `parent_span_id` 组成树，顶层 span 作为 business
  内部的兄弟段、互不重叠；`business_own`（业务自身耗时）=
  `business − 顶层 span 耗时之和`；子 span 耗时已包含在父 span 内，不重复计；
- **other**（对账残差）= `total − queue − business − response`，收口未解释
  时间与全部浮点误差。

**对账不变式（有测试逐条固化）：**

```
queue + business_own + 顶层嵌套span之和 + response + other == total
```

- 顶层 span 之和大于业务窗口（异常重叠）时，`business_own` 如实为负、
  `segments_clean=false`，但和仍严格等于 total，不凭空增多或减少；
- span 的 parent 指向不存在的 id（断链）时，该 span 提为顶层段，耗时不丢失；
- 未经中间件计时点构造的记录（三段都为 0）退化为全部耗时落入 `other`，对账
  恒成立。

最慢请求查询：`collector.slowest_attributions(limit=None, entry=None)`，
按耗时降序、同耗时按 trace_id 升序（确定可复现），返回 `(RequestRecord,
RequestAttribution)`；`limit` 缺省取 `slow_top_n`。

## 3C. 容量与淘汰规则

| 状态 | 默认上限 | 超限规则 | 淘汰影响范围 |
| --- | --- | --- | --- |
| 保留明细 `_records` | `detail_max_records=1000` | FIFO 丢最旧明细（错误/慢请求同样适用，规则不因类别而异） | 只影响 sampled 聚合、`snapshot()` 与慢请求归因查询 |
| 窗口样本 `_window` | `window_max_samples=10000` | FIFO 丢最旧样本，`overflow_dropped` 计数 | 只影响窗口统计 |
| 全量计数 census | 无条数上限（按入口维度累计） | 不淘汰 | —— |
| trace 去重账本 | 保存全部 trace_id | 不淘汰 | 保证重复提交绝不重复计数 |

设计取舍：

1. 正常情况下窗口上限远大于窗口内请求量，淘汰只在极端高峰兜底，避免内存
   被撑爆；`window_meta()` 暴露 `size/added/expired/overflow_dropped`，
   `detail_evicted` 暴露明细淘汰数，容量行为可观测、可告警；
2. **淘汰绝不触碰 census**：明细被 FIFO 淘汰后，该 trace 仍留在去重账本中，
   重复提交既不会重复计数、也不会让旧明细"复活"；
3. 慢请求归因查询只对当前明细做有界内存排序（工作量 ≤ 明细上限），不会被
   流量峰值卡住；
4. 热更新配置（`update_config`）会按新窗口长度即时淘汰过期样本、按新容量
   FIFO 收敛，不丢账。

**开采样 / 关采集时的口径：**

- `sample_rate<1`：census 与窗口仍为全量；sampled 与慢请求归因只覆盖**保留
  明细**（错误/慢必留 + 普通按采样），明细被容量淘汰后也会从查询中消失——
  这是"明细口径"，不代表全量；
- 整体关闭（`enabled=false`）：不计数、不写窗口、不留明细，三套口径都为空。

## 4. 配置项

### 4.1 环境变量（启动时生效）

| 变量 | 默认 | 含义 |
| --- | --- | --- |
| `TELEMETRY_ENABLED` | `1` | 总开关，`1/true/yes/on` 开，`0/false/no/off` 关 |
| `TELEMETRY_SAMPLE_RATE` | `1.0` | 普通请求保留概率，范围 `[0,1]`；不影响错误/慢请求 |
| `TELEMETRY_SLOW_THRESHOLD_MS` | `500.0` | 慢请求阈值（毫秒），严格大于即判慢 |
| `TELEMETRY_WINDOW_SECONDS` | `300.0` | 滑动窗口长度（秒），必须为正 |
| `TELEMETRY_WINDOW_MAX_SAMPLES` | `10000` | 窗口轻量样本条数硬上限（FIFO），正整数 |
| `TELEMETRY_DETAIL_MAX_RECORDS` | `1000` | 保留明细条数硬上限（FIFO），正整数 |
| `TELEMETRY_SLOW_TOP_N` | `20` | 慢请求归因查询默认返回条数，正整数 |

非法或越界的环境变量一律回退默认值，不阻断启动。

示例：

```bash
TELEMETRY_ENABLED=1 TELEMETRY_SAMPLE_RATE=0.1 TELEMETRY_SLOW_THRESHOLD_MS=200 \
TELEMETRY_WINDOW_SECONDS=60 TELEMETRY_DETAIL_MAX_RECORDS=500 \
  uv run uvicorn main:app --reload
```

### 4.2 运行期开关与观测端点

- `POST /__telemetry__/enabled/true`：开启采集
- `POST /__telemetry__/enabled/false`：关闭采集
- `GET  /__telemetry__/report`：查看聚合报表（含两套口径、总计数、采样率）
- `GET  /__telemetry__/window`：当前滑动窗口各入口的
  `count/error_count/slow_count/p50_ms/p95_ms/p99_ms`，并带 `meta`
  （窗口长度、容量、当前样本数、累计滑出/撞上限丢弃数）
- `GET  /__telemetry__/slow?limit=20&entry=http:GET%20/slow`：最慢的一批
  **已保留**慢请求及归因明细（每段毫秒数 + 嵌套段树 + `accounted_ms` 对账值）

也可在代码中 `collector.enable()` / `collector.disable()` /
`collector.update_config(TelemetryConfig(...))` / `collector.window_stats()` /
`collector.slowest_attributions()`。

> 兼容范围：观测端点路径仍在 `/__telemetry__/` 前缀下（中间件不采集自身，
> 避免自污染）。`RequestRecord` 新增的 `queue_ms/business_ms/response_ms`
> 均带默认值 `0.0`，旧的直接构造代码无需修改；新增配置项也都有默认值，
> 原有构造方式保持兼容。

## 5. 对吞吐的影响

采集逻辑全部在进程内完成（内存计数 + 少量 Python 对象），无网络/IO：

- **关闭时**：中间件仅做一次 `enabled()` 判断即透传，近乎零开销，且不产生任何记录；
- **开启全量**：每请求额外做几次 perf_counter 计时（含分段点）、一次终态分类、
  一次加锁写入（census 计数 + 一份轻量窗口样本 + 可能的一条明细）；
- **开启采样(rate<1)**：普通请求不构造/保留明细对象，写入更少，开销不高于全量；
  窗口样本与 census 计数仍按全量写入（一个很小的对象）；
- **查询成本**：窗口查询每次对窗内耗时做一次排序（O(n log n)，n 为窗内样本，
  受 `window_max_samples` 约束）；慢请求归因查询只对 ≤ `detail_max_records`
  条明细排序，均在锁内完成、有界，不随累计流量增长。

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
curl -s localhost:8000/__telemetry__/window  # 滑动窗口 count/分位数
curl -s "localhost:8000/__telemetry__/slow?limit=5"  # 最慢 5 条 + 归因明细
```

可在响应里核对归因：`attribution.accounted_ms` 必须等于该请求的 `total_ms`
（差值 ≤ 1e-6ms）；嵌套调用以 `segments[].children` 成树出现，耗时不重复累加。

客户端中途断开（另一个终端）：

```bash
# 请求会等待 5s；立即 Ctrl-C 即触发 http.disconnect / 取消
curl -s "localhost:8000/wait-then-respond?ms=5000"
# 随后查看报表，会出现 client_disconnect 计数
curl -s localhost:8000/__telemetry__/report
```

### 运行测试

```bash
uv run python -m pytest                      # 全部 90 个用例
uv run python -m pytest -o log_cli=true -o log_cli_level=DEBUG   # 打印每个用例的输入与判定依据
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
- `test_overhead.py`：三种模式吞吐对比与"无外部依赖"验证；
- `test_window.py`：窗口滚动计入/滑出、nearest-rank 分位数、多入口计数一致、
  容量 FIFO、迟到样本拒绝、参数热更新（15 例）；
- `test_attribution.py`：分段和严格等于总耗时、嵌套树结构、断开路径、
  无计时点退化、重叠 span 负残差、断链 span 不丢、扁平口径（7 例）；
- `test_capacity.py`：明细 FIFO 淘汰边界、淘汰 trace 重复提交不复活/不重计、
  容量热更新、零采样下窗口仍全量、关采集三口径皆空、窗口滚动与 census 对账、
  窗口撞上限不影响 census、慢请求 Top N 排序/过滤/对账（9 例）；
- `test_window_attribution_e2e.py`：经中间件实测三段计时与嵌套归因、HTTP
  观测端点、8×250 并发下窗口与 census 逐项相等（4 例）。

## 7. 代码结构

```
telemetry/
  config.py         # 配置 + 环境变量加载（窗口/容量/TopN）
  errors.py         # 框架中立的 BusinessError
  classification.py # outcome / failure 分类
  context.py        # TraceContext + contextvars + 嵌套 span
  recorder.py       # RequestRecord / SpanRecord / 聚合报表模型
  window.py         # SlidingWindow：滚动窗口 + nearest-rank 分位数
  attribution.py    # 慢请求耗时归因（段拆分 + span 树 + 对账残差）
  collector.py      # 线程安全采集器：计数、窗口、采样、容量淘汰、归因查询、聚合
  middleware.py     # ASGI 中间件：分段计时点、建 trace、终态判定、登记
app/
  errors.py         # BusinessError 再导出 + 统一异常处理器
  endpoints.py      # 演示入口
main.py             # 装配应用 + 本地观测端点（report/window/slow）
tests/              # 上述测试
```
