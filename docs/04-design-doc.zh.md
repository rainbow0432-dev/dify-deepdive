# Dify App Trace 参考目录 — 设计文档

**项目**: `dify-deepdive` · **包**: `traceset/` (`dify-trace-catalog`)
**分支**: `main`（从 `feat/dify-trace-catalog` 合并，17 个 commit）
**Dify 源码版本**: `b33e8f0ddb1189427548b0e1206cedcdc17d9bb6`
**Langfuse SDK 版本**: `>=4.2.0,<5.0.0`（v4 ingestion wire format）
**日期**: 2026-06-29

---

## 1. 执行摘要

`dify-deepdive` 项目调查了为什么 Dify 原生的 Langfuse 集成在结构上比可达成的延迟慢约 10 倍（~11s 下限 vs. ~1.5s），随后从诊断转向具体交付物：一个**合成的但贴近真实的 Dify 应用 trace 参考目录**，以 Langfuse wire event 形式捕获。

该目录位于 `traceset/`，包含：

- **14 个场景**，覆盖全部 5 种 Dify 应用模式（`chat`、`completion`、`agent-chat`、`workflow`、`advanced-chat`）和全部 7 种 Dify trace 类型（`Message`、`Workflow`、`Moderation`、`DatasetRetrieval`、`Tool`、`GenerateName`、`SuggestedQuestion`）。
- **88 个 Langfuse wire event**（`trace-create` / `span-create` / `generation-create`）—— Dify 逐个 POST 到 `/api/public/ingestion` 的精确 JSON body。
- **6 个边界场景**：大节点 workflow（17 个事件）、moderation 拦截、空 RAG、tool 失败、suggested-questions 错误、streaming。
- **43 个测试**全部通过，**每个场景 6 项自检**，**2 项目录级覆盖断言**。
- 一个确定性生成脚本（`generate_traceset.py`），可从源码逐字节重建整个目录。

该包运行时**零依赖**（仅需 CPython）；`langfuse` SDK 仅作为开发可选依赖，用于权威 schema 校验。

---

## 2. 背景与动机

### 2.1 延迟问题

记录于 [`docs/01-research-report.md`](./01-research-report.md)、[`docs/02-dify-trace-flow.md`](./02-dify-trace-flow.md) 和 [`docs/03-langfuse-staging-tables.md`](./03-langfuse-staging-tables.md) 的研究表明，Dify 原生的 Langfuse 集成无法仅通过配置实现低延迟，原因在于三个叠加的结构性因素：

1. **REST ingestion（5s 服务端队列）**—— Dify 使用 Langfuse 的 REST ingestion 端点，该端点带有硬编码的 5 秒服务端队列延迟（`processEventBatch.ts:66-91` 中的 `getDelay()`）。OTLP 路径的延迟为 0s。Dify 无法使用 OTLP，因为它不使用 SDK 的 OpenTelemetry span 路径。

2. **Dify 的三阶段解耦（Timer → Celery → 同步 HTTP）**—— Dify 通过以下方式解耦 trace 发射：(a) 进程内 `threading.Timer`，默认 5 秒（`TRACE_QUEUE_MANAGER_INTERVAL`）；(b) Celery 任务队列（`ops_trace` 队列，5s 重试退避，最多 60 次重试）；(c) Celery worker 内的同步逐事件 HTTP POST。每次 `add_trace`/`add_span`/`add_generation` 调用都是**一次 HTTP POST**（`api.ingestion.batch(batch=[event])`）。

3. **缺少 v4 direct-write 信号（UI 延迟最高 10 分钟）**—— Dify 从不发送 `x-langfuse-ingestion-version: 4`，且无法通过 REST 发送。没有 v4 信号，Langfuse 会将 trace 路由到 `observations_batch_staging` ClickHouse 表 + 周期性的 `handleEventPropagationJob`（BullMQ cron），然后才到达最终的 `events` 表——在传输延迟之上额外增加最高 10 分钟的 UI 延迟。

**关键失败模式**：Langfuse HTTP 失败在 Dify 的 `LangFuseDataTrace` provider 中抛出 `ValueError`（而非 `RetryableTraceDispatchError`），因此**从不重试**——静默的永久数据丢失，仅有一个 Redis 计数器递增。

**被证伪的调参项**：`LANGFUSE_FLUSH_AT` / `LANGFUSE_FLUSH_INTERVAL`、`OTEL_BSP_*` 和 `langfuse.flush()` 在 Dify 的代码路径上都是空操作—— Dify 从不触碰 SDK 的 `BatchSpanProcessor`。认为调这些参数能帮助 Dify 的普遍看法是一种误解。

### 2.2 为什么要做参考目录？

延迟研究需要一个具体的、可检查的制品来代表**Dify 实际发射了什么**——不是 Langfuse UI 展示的内容（staging 之后），也不是 Dify 内部 `TraceInfo` Pydantic 模型持有的内容（序列化之前）。该目录捕获的是 **wire event**：到达 `/api/public/ingestion` 的 JSON body，在任何服务端处理之前。

这个中间格式是任何延迟分析、回放工具或替代 ingestion 路径的承重层——然而它在 Dify 源码中（仅构造它）和 Langfuse UI 中（仅展示存储后的内容）都是不可见的。该目录将其提升为一等公民。

---

## 3. 目标与非目标

### 目标

1. **覆盖全部 7 种 Dify trace 类型**（`MessageTraceInfo`、`WorkflowTraceInfo`、`ModerationTraceInfo`、`DatasetRetrievalTraceInfo`、`ToolTraceInfo`、`GenerateNameTraceInfo`、`SuggestedQuestionTraceInfo`）。
2. **覆盖全部 5 种 Dify 应用模式**（`chat`、`completion`、`agent-chat`、`workflow`、`advanced-chat`）。
3. **演练 6 种边界场景**：大节点 workflow、moderation 拦截、空 RAG、tool 失败、suggested-questions 错误、streaming。
4. **Dify 填充的每个字段**都有真实值表示（真实模型 ID、合理的 token 计数/成本、有效 UUID、ISO 8601 时间戳、真实文本内容）。
5. **可复现生成**——确定性 UUID（v5）和固定基准时间戳，使重新运行生成器产生逐字节相同的输出。
6. **Schema 校验**——每个事件都通过真实的 langfuse SDK Pydantic 模型校验（带本地兜底校验器）。

### 非目标

- 从运行的 Dify 实例捕获真实 trace（目录是合成的）。
- Dify 内部 `TraceInfo` JSON 格式（序列化前）。
- Langfuse ingestion 后的存储格式（staging 后的 ClickHouse 行）。
- 延迟场景回放（目录是结构性的，非时间性的）。
- 内部 trace 类型（`PromptGenerationTraceInfo`、`DraftNodeExecutionTrace`）——这些不对用户暴露。
- OTLP / OpenTelemetry 格式的 trace——目录仅包含 REST wire event。

---

## 4. 架构概览

### 4.1 分层

```
generate_traceset.py        orchestrator (build → validate → self-check → write → coverage-assert)
        │
        ├── scenarios/__init__.py    explicit ordered registry (SCENARIOS list)
        │       │
        │       └── scenarios/sNN_*.py    14 flat modules: constants + build_events/build_meta
        │               │
        │               └── helpers.py    make_event_id/timestamp/*_create/wrap_event (pure)
        │
        └── schema.py    validate_event (local + optional langfuse SDK)
```

**自上而下**：orchestrator 导入 registry，registry 导入 14 个场景模块，场景模块导入 helpers。Schema 校验器由 orchestrator 对每个事件调用。测试位于旁边，独立演练每一层。

### 4.2 依赖关系

| 层 | 运行时依赖 | 备注 |
|---|---|---|
| `helpers.py` | 仅标准库（`uuid`、`datetime`） | UUID v5、ISO 8601 运算、camelCase 转换 |
| `schema.py`（本地层） | 仅标准库 | Envelope + 必填字段 + snake_case 守卫 |
| `schema.py`（SDK 层） | `langfuse>=4.2.0,<5.0.0`（可选） | Pydantic `model_validate` 对 `IngestionEvent_*Create` |
| `generate_traceset.py` | 仅标准库（`json`、`os`、`sys`） | 构建 + 校验 + 写入 + 断言 |
| `scenarios/sNN_*.py` | 仅标准库（经 `helpers`） | 纯数据 + 构造逻辑 |
| `tests/` | `pytest>=7.0`（开发） | `tmp_path` 隔离，无网络 |

**运行时**：零依赖。**仅开发**：`langfuse`（权威校验）+ `pytest`。`<5.0.0` 上界是承重的——它锁定到项目延迟研究所依赖的 v4 ingestion wire format。

### 4.3 每层强制执行的契约

六项不变量在 helper、schema、scenario、generator 和 test 层冗余检查：

1. **Wire envelope** `{id, timestamp, type, body}`—— `helpers.wrap_event` 构造；`schema._local_validate` 检查；generator 自检 + 测试复查。
2. **camelCase body key**—— `helpers.to_camel_case` 生成；`schema` 拒绝 body key 中任何 `_`；generator 自检 + 测试复查。
3. **事件计数**—— `scenario.EXPECTED_EVENT_COUNT` 是唯一事实来源；generator 自检、场景测试和 pipeline 测试全部断言。
4. **单调时间戳**—— `helpers.make_timestamp(_BASE, increasing_offset)` 生成；generator 自检 + 测试复查。
5. **meta/events 对偶不变量**—— `build_meta().events_in_order` 必须在长度、顺序和类型上镜像 `build_events()`；generator 自检 + 测试复查。
6. **目录覆盖**—— 全部 7 种 Dify trace 类型 + 全部 5 种应用模式都有代表；在 `main()` 中强制执行。

这种冗余意味着回归首先在最具体的层被捕获，并产生最清晰的失败信息。

---

## 5. 组件设计

### 5.1 `helpers.py` — 事件构造（102 行）

纯函数工厂层。零状态、零 I/O、除标准库外零依赖。

| 函数 | 职责 |
|---|---|
| `make_event_id(seed) -> str` | 从种子字符串生成确定性 UUID **v5**（NAMESPACE_URL）。相同种子 → 相同 ID。实现可复现 trace。 |
| `make_timestamp(base, offset_seconds=0.0) -> str` | 解析 ISO-8601 基准时间，加浮点偏移，返回微秒精度 ISO-8601。 |
| `to_camel_case(snake) -> str` | 按 `_` 分割，除第一段外全部首字母大写。（`user_id` → `userId`，`name` → `name`。） |
| `make_trace_create(trace_id, name, user_id=None, **kwargs) -> dict` | 构造 `trace-create` body。`id`/`name` 总是设置；`userId` 仅在非 None 时设置；kwargs 转 camelCase。 |
| `make_span_create(span_id, trace_id, name, start_time, end_time, **kwargs) -> dict` | 构造 `span-create` body，含 5 个必填 camelCase 字段 + camelCase kwargs。 |
| `make_generation_create(gen_id, trace_id, name, model, start_time, end_time, usage, **kwargs) -> dict` | 构造 `generation-create` body。将 `usage` 映射为 `usageDetails`（唯一的非平凡 wire 名称）。 |
| `wrap_event(event_id, timestamp, event_type, body) -> dict` | 将 body 包装进 4 字段 envelope `{id, timestamp, type, body}`。 |

**关键设计模式**：调用者以惯用的 Python snake_case 编写字段（`parent_observation_id`、`completion_start_time`、`model_parameters`），helper 透明地序列化为 Langfuse 的 wire camelCase。这消除了"字段名拼对了吗？"这一整类 bug。

### 5.2 `schema.py` — Wire Schema 校验器（76 行）

双层校验器，优雅降级。

**第一层 — 本地校验器（`_local_validate`）**：总是运行。检查：
- Envelope 包含全部 4 个必填字段：`{id, timestamp, type, body}`。
- `type` ∈ `{trace-create, span-create, generation-create}`。
- `body` 是 dict。
- 按类型检查必填 body 字段：
  - `trace-create`：`id`、`name`
  - `span-create`：`id`、`traceId`、`name`、`startTime`、`endTime`
  - `generation-create`：`id`、`traceId`、`name`、`startTime`、`endTime`、`model`、`usageDetails`
- **无 snake_case 泄漏**：任何包含 `_` 的 body key 抛出 `ValueError`—— 端到端强制 camelCase 的规范化守卫。

**第二层 — SDK 校验器（`_validate_with_sdk`）**：仅在 `langfuse` 可导入时运行。导入 `IngestionEvent_TraceCreate` / `IngestionEvent_SpanCreate` / `IngestionEvent_GenerationCreate`（来自 `langfuse.api` 的 Pydantic 模型）并调用 `.model_validate(event)`。如果 `langfuse` 不存在，`ImportError` 被静默吞掉。

**快速失败带描述性信息**：每个分支抛出 `ValueError`，消息指出违反的具体字段/类型（如 `"span-create body missing required field: traceId"`、`"snake_case key in generation-create body: status_message"`）。测试通过 `pytest.raises(ValueError, match=...)` 匹配这些子串。

### 5.3 `scenarios/` — 场景模块与注册表

#### 注册表（`scenarios/__init__.py`，34 行）

一个**显式的、有序的、手工维护的注册表**——不是基于装饰器的自动注册：

```python
from . import s01_chat_basic, s02_chat_rag, ... s14_message_streaming

SCENARIOS = [
    s01_chat_basic, s02_chat_rag, ... s14_message_streaming,
]
```

**为什么选择显式而非自动发现**：Fail-closed（缺少 import 意味着场景不在目录中——不会因 glob 未匹配而静默跳过）。确定性排序不依赖文件系统排序。添加场景是两次编辑操作：创建 `sNN_slug.py`，然后加一行 import 和一个列表条目。测试 `test_scenarios_registry_has_14` 和 `test_all_scenario_ids_unique` 锁定基数和唯一性。

#### 场景模块契约

每个场景是一个扁平 Python 模块（无类），导出**模块级常量**和**两个函数**：

| 常量 | 示例 | 用途 |
|---|---|---|
| `SCENARIO_ID` | `"01-chat-basic"` | 目录名 + catalog key |
| `SCENARIO_DESCRIPTION` | `"Basic chatbot, single-turn Q&A"` | 人类可读描述 |
| `APP_TYPE` | `"chatbot"` | 高层应用类别 |
| `DIFY_APP_MODE` | `"chat"` | Dify 5 种应用模式之一（覆盖断言目标） |
| `EDGE_CASE` | `None` 或 `"high-n"` / `"moderation-blocked"` / ... | `None` 为基线；slug 为边界场景 |
| `TRACE_TYPES_EMITTED` | `["MessageTraceInfo", "GenerateNameTraceInfo"]` | 7 种 Dify `*TraceInfo` 类型中的哪些（覆盖断言目标） |
| `EXPECTED_EVENT_COUNT` | `4` | 事件计数的唯一事实来源 |

| 函数 | 返回 | 职责 |
|---|---|---|
| `build_events()` | `list[dict]` | 通过 `helpers.make_*` + `wrap_event` 构造有序 wire event dict 列表。时间戳来自单一 `_BASE` + 递增偏移。 |
| `build_meta()` | `dict` | `meta.json` 载荷：所有常量 + `events_in_order`（1 起索引的清单，映射每个事件到其 `source_trace_type` 和 `dify_handler`）+ provenance pin（`dify_commit`、`langfuse_sdk_version`）+ `notes`。 |

**编写规范**（全部 14 个场景一致）：
- 事件 ID 遵循 `sNN-eNN` 种子约定（`make_event_id("s01-e01")`, ...）——可 grep 回场景。
- 每个场景单一 `_BASE` 时间戳；不同场景使用不同挂钟窗口。
- 模块 docstring = 事件清单（枚举"Events (N):"列表，命名每个事件的类型 + 来源 `*TraceInfo`）。
- `s01_chat_basic.py` 是声明的**参考模板**（所有字段值显式展示；其他场景是变体）。
- 边界场景在 docstring 和 `meta.json` notes 中携带 `VERIFY:` 标记——将研究不确定性直接编码在目录中。

### 5.4 `generate_traceset.py` — 生成 pipeline（335 行）

编排整个目录的构建。

| 函数 | 职责 |
|---|---|
| `generate_scenario(scenario, base_dir) -> dict` | 构建一个场景的 `NN-slug/` 目录：调用 `build_events()`，通过 `schema.validate_event` 校验每个事件，运行 6 项自检，写入 `events.jsonl` + `meta.json`，返回 catalog 条目 dict。 |
| `generate_catalog(scenarios, base_dir)` | 写入 `catalog.json`——按场景摘要条目的 JSON 数组。 |
| `generate_readme(scenarios, base_dir)` | 写入 `README.md`——面向人类的目录索引，含场景表和 provenance。 |
| `generate_schema_doc(base_dir)` | 写入 `schema.md`——wire envelope 和 body 类型的静态字段参考文档 + Dify TraceInfo → wire event 映射。 |
| `main()` | 入口：遍历 `SCENARIOS`，对每个调用 `generate_scenario`，然后调用三个根生成器，打印进度，运行 2 项覆盖断言。 |

**6 项每场景自检**（在任何文件写入之前运行——失败的场景中止构建）：

1. 事件计数：`len(events) == scenario.EXPECTED_EVENT_COUNT`。
2. 有效类型：每个事件 `type` ∈ 3 种有效 wire 类型。
3. 无 snake_case body key。
4. 单调时间戳：`timestamps == sorted(timestamps)`。
5. `meta.events_in_order` 长度匹配事件计数。
6. `meta.events_in_order` 每索引：1 起顺序，每个条目的 `type` 匹配对应事件的 `type`。

**2 项目录级覆盖断言**（在 `main()` 中）：
1. 目录中覆盖全部 7 种 Dify trace 类型。
2. 目录中覆盖全部 5 种 Dify 应用模式。

**幂等/可复现**：确定性事件 ID（UUID v5）+ 确定性 JSON 序列化（`ensure_ascii=False`、`indent=2`）→ 重新运行产生逐字节相同的输出。生成的制品已提交（`.gitignore` 仅排除 `__pycache__`、`*.egg-info`、`.pytest_cache`），因此目录既是构建*输出*也是版本化*交付物*。

### 5.5 `tests/` — 测试套件（435 行，4 个文件）

| 文件 | 行数 | 覆盖 |
|---|---|---|
| `test_helpers.py` | 113 | 全部 7 个 helper：确定性、camelCase、body 形状、envelope。12 个测试。 |
| `test_schema.py` | 96 | 校验器正常/拒绝路径，带 pinned error message 子串。8 个测试。 |
| `test_scenarios.py` | 106 | `_check_scenario(module)` 应用于全部 14 个模块——在测试时重新实现 6 项自检。14 个测试。 |
| `test_generate_traceset.py` | 120 | Pipeline 和制品测试，带 `tmp_path` 隔离 + 端到端 `main()` 冒烟测试。9 个测试。 |

**规范**：核心套件无需网络、无需 SDK（仅 `test_schema` 受益于 SDK，SDK 不存在时静默跳过该层）。`tmp_path` 隔离——没有测试写入真实目录。`gt._BASE_DIR` monkeypatch 是端到端测试的唯一逃生口。冗余是有意的——相同不变量在多层检查，使回归首先在最具体的层被捕获。

---

## 6. Wire Event 格式

### 6.1 Envelope

每个事件是一个 JSON 对象，含 4 个字段：

```json
{
  "id": "<uuid>",
  "timestamp": "<ISO 8601 UTC>",
  "metadata": null,
  "type": "trace-create | span-create | generation-create",
  "body": { ... }
}
```

### 6.2 Body 类型

| 类型 | 必填 body 字段 | 可选字段 |
|---|---|---|
| `trace-create` | `id`、`name` | `userId`、`input`、`output`、`sessionId`、`metadata`、`version`、`release`、`tags`、`public` |
| `span-create` | `id`、`traceId`、`name`、`startTime`、`endTime` | `input`、`output`、`metadata`、`level`、`statusMessage`、`parentObservationId`、`version` |
| `generation-create` | `id`、`traceId`、`name`、`startTime`、`endTime`、`model`、`usageDetails` | `modelParameters`、`input`、`output`、`metadata`、`level`、`statusMessage`、`parentObservationId`、`version`、`completionStartTime` |

### 6.3 序列化规则

- **camelCase** body key（本地 schema 校验器拒绝任何包含 `_` 的 key）。
- **`exclude_unset + exclude_none`**：只有已填充的字段出现在 wire 上。留为默认 `None` 或显式设为 `None` 的字段被剥离。
- **每次 HTTP POST 1 个事件**：Dify 向 `/api/public/ingestion` 发送单元素 `batch=[event]`——每次 `add_trace`/`add_span`/`add_generation` 调用一次 POST。

### 6.4 Dify TraceInfo → Wire Event 映射

| Dify TraceInfo | Wire event | Handler | 典型场景 |
|---|---|---|---|
| `MessageTraceInfo` | 1 trace-create + 1 generation-create | `message_trace` | chat、completion、chatflow |
| `WorkflowTraceInfo` | 1 trace-create + 1 span-create + K 个节点事件 | `workflow_trace` | workflow、chatflow |
| `ModerationTraceInfo` | 1 span-create | `moderation_trace` | 启用 moderation 的 chat |
| `DatasetRetrievalTraceInfo` | 1 span-create | `dataset_retrieval_trace` | 启用 RAG 的 chat |
| `ToolTraceInfo` | 每次 tool 调用 1 span-create | `tool_trace` | agent 应用 |
| `GenerateNameTraceInfo` | 1 trace-create（upsert）+ 1 span-create | `generate_name_trace` | 所有 chat/completion/agent 应用 |
| `SuggestedQuestionTraceInfo` | 1 generation-create | `suggested_question_trace` | 启用建议问题的 chat |

**一次运行内的发射顺序**（当多种 trace 类型触发时）：
```
ModerationTraceInfo → MessageTraceInfo → DatasetRetrievalTraceInfo → ToolTraceInfo → SuggestedQuestionTraceInfo → GenerateNameTraceInfo
```

该顺序遵循 Dify 的 `add_trace_task()` 调用序列：moderation 首先触发（输入检查），然后 message pipeline 完成（LLM 响应），然后 RAG/tool trace 被发射（它们在 message 期间运行），然后 suggested questions（响应后），最后 generate name（最后）。

---

## 7. 场景目录

### 7.1 覆盖矩阵

| # | 场景 | 应用模式 | 边界? | 事件数 | trace-create | span-create | generation-create | Trace 类型 |
|---|---|---|---|---|---|---|---|---|
| 01 | `01-chat-basic` | chat | — | 4 | 2 | 1 | 1 | Message, GenerateName |
| 02 | `02-chat-rag` | chat | — | 6 | 2 | 2 | 2 | Message, DatasetRetrieval, SuggestedQuestion, GenerateName |
| 03 | `03-completion-basic` | completion | — | 4 | 2 | 1 | 1 | Message, GenerateName |
| 04 | `04-agent-single-tool` | agent-chat | — | 5 | 2 | 2 | 1 | Message, Tool, GenerateName |
| 05 | `05-agent-multi-tool` | agent-chat | — | 7 | 2 | 4 | 1 | Message, Tool, GenerateName |
| 06 | `06-workflow-5node` | workflow | — | 7 | 1 | 4 | 2 | Workflow |
| 07 | `07-workflow-15node` | workflow | **high-n** | 17 | 1 | 12 | 4 | Workflow |
| 08 | `08-chatflow-basic` | advanced-chat | — | 11 | 3 | 5 | 3 | Workflow, Message, GenerateName |
| 09 | `09-moderation-blocked` | chat | **moderation-blocked** | 3 | 1 | 1 | 1 | Moderation, Message |
| 10 | `10-moderation-pass-through` | chat | — | 5 | 2 | 2 | 1 | Moderation, Message, GenerateName |
| 11 | `11-rag-empty-results` | chat | **empty-rag** | 5 | 2 | 2 | 1 | DatasetRetrieval, Message, GenerateName |
| 12 | `12-tool-failure` | agent-chat | **tool-error** | 5 | 2 | 2 | 1 | Tool, Message, GenerateName |
| 13 | `13-suggested-questions-error` | chat | **suggested-questions-error** | 5 | 2 | 1 | 2 | Message, GenerateName, SuggestedQuestion |
| 14 | `14-message-streaming` | chat | **streaming** | 4 | 2 | 1 | 1 | Message, GenerateName |
| | **合计** | | **6 边界 / 8 正常** | **88** | **26** | **40** | **22** | |

### 7.2 场景详情

#### 01 — `01-chat-basic`（正常，4 个事件）
最简单的 chat 运行。单轮 Q&A（"Redis vs Memcached"）使用 `gpt-4o-mini`（输入 42 / 输出 156）。GenerateName 的 `trace-create`（事件 3）**复用与 Message `trace-create`（事件 1）相同的 trace ID**——Langfuse upsert 语义。声明为所有其他场景的**参考模板**。

#### 02 — `02-chat-rag`（正常，6 个事件）
RAG 检索返回 3 个文档（分数 0.95/0.89/0.82）。两个不同的 `generation-create` 事件：message 答案和 suggested 后续问题（`output.questions` 数组含 3 项）。演练了所有 chatbot 场景中最多的 trace 类型（4 种）。

#### 03 — `03-completion-basic`（正常，4 个事件）
结构与 01 **相同，除了** `trace-create` body **没有 `sessionId`**——completion 应用没有对话。`input` 是 `{"prompt": ...}`（而非 `{"query": ...}`）。确认 `sessionId` 是条件性发射的。

#### 04 — `04-agent-single-tool`（正常，5 个事件）
`weather_api` tool span，然后合成 `generation-create`，其 `input.messages` 包含 `{"role":"tool", ...}` 条目。Tool `span-create` 发射在 Message `trace-create` 和 `generation-create` **之间**——tool 运行并被 trace 记录先于 LLM 合成被记录。

#### 05 — `05-agent-multi-tool`（正常，7 个事件）
三个顺序 tool（`web_search`、`data_fetch`、`calculator`），然后用 **`gpt-4o`**（非 mini）合成。`input.messages` 携带三个 `role:tool` 条目。演示 agent 路径上的 N-tool 扩展。

#### 06 — `06-workflow-5node`（正常，7 个事件）
Pipeline：`Start → KnowledgeRetrieval → LLM(Generate) → LLM(Refine) → End`。所有子节点携带 `parentObservationId` 指向根 "workflow" span。两个 `generation-create` 事件（Generate + Refine），均为 `gpt-4o`。纯 `WorkflowTraceInfo`（无 Message/GenerateName）。

#### 07 — `07-workflow-15node`（边界：high-N，17 个事件）
**大节点压力场景**——单个 `WorkflowTraceInfo` 任务产生 17 个顺序 HTTP POST。4 个 LLM generation 跨**两个 provider**（3× `gpt-4o`、1× `claude-3-5-sonnet-20241022`）。Tool 命名 span（`web_search`、`data_fetch`）和控制流 span（`Check Confidence`、`validate_output`、`Quality Check`）。最大场景（约占所有目录事件的 19%）。唯一使用非 OpenAI 模型的场景。

#### 08 — `08-chatflow-basic`（正常，11 个事件）
唯一的 `advanced-chat` 场景——**组合案例**：chatflow = workflow + message。前 7 个事件是 workflow（镜像场景 06 的形状）；然后第二个 `trace-create`（Message）和 `generation-create` 记录对话 message；然后 GenerateName。三个 `trace-create` 事件**共享相同 trace ID**（upsert）。所有场景中 `trace-create` 计数最多（3）。

#### 09 — `09-moderation-blocked`（边界：moderation-blocked，3 个事件）
最短场景。Moderation span 有 `level: "WARNING"`、`statusMessage: "Input blocked by moderation: flagged categories [violence, hate]"`。`generation-create` 发射**预设响应**（`metadata.preset_response: true`、`temperature: 0.0`）。**无 GenerateName**——拦截路径跳过对话名称生成。`meta.json` note 标记了 VERIFY——wire **确认 generation-create 会触发**。

#### 10 — `10-moderation-pass-through`（正常，5 个事件）
09 的对照——相同的 Moderation+Message+GenerateName 骨架但走 happy path。Moderation span `output.flagged: false`、`output.action: "pass"`，**无 `level` 字段**（隐式 DEFAULT，在 `exclude_unset` 下省略）。确认通过的 moderation 不发射 `level`/`statusMessage`。

#### 11 — `11-rag-empty-results`（边界：empty-rag，5 个事件）
`dataset_retrieval` span `output: {"documents": [], "result_count": 0}`。LLM 在无知识情况下回答（"I don't have specific information…"）。DatasetRetrieval span 发射在 Message 事件**之前**（检索先运行，然后打开 message trace）。空结果边界—— span 仍以 0 计数输出触发；LLM 仍回答（优雅降级）。

#### 12 — `12-tool-failure`（边界：tool-error，5 个事件）
`stock_api` tool span 有 `level: "ERROR"`、`statusMessage: "ConnectionError: ... timeout after 5000ms"`。Agent 仍完成——合成 `generation-create` 的 `input.messages` 包含 `{"role":"tool", "content":"ConnectionError: ..."}` 且 LLM 道歉。失败被记录，非致命。注意：Tool span 领先（idx 1），与正常 agent 路径（04/05）中 Message `trace-create` 领先不同。

#### 13 — `13-suggested-questions-error`（边界：suggested-questions-error，5 个事件）
失败的 SuggestedQuestion `generation-create`（事件 5）有 `level: "ERROR"`、`statusMessage: "RateLimitError: ... 429"`，且 **`usageDetails.output: 0`**（输入 85 token 已消费，0 输出产出）。失败的 generation **消费了输入 token 但产出零输出**——部分失败的计费特征。发射在**最后**（GenerateName 之后），与场景 02 中 SuggestedQuestion 先于 GenerateName 不同。

#### 14 — `14-message-streaming`（边界：streaming，4 个事件）
结构与 01 相同（相同 4 事件形状）——**唯一**区别是 `generation-create` 上的 streaming 延迟元数据：`metadata: {"streaming": true, "gen_ai_server_time_to_first_token": 320, "llm_streaming_time_to_generate": 4150}` 加 `completionStartTime`。`meta.json` note 标记了 VERIFY 确切元数据 key 名——wire **确认**它们是 `gen_ai_server_time_to_first_token`（320ms TTFT）和 `llm_streaming_time_to_generate`（4150ms TTG）。这是实时 trace 论点的延迟相关场景。

### 7.3 横切模式

1. **Trace-ID upsert 语义**：在所有有 GenerateName 的场景中（chatflow 08 更是如此），多个 `trace-create` 事件复用*相同* `id`。Langfuse 将重复 trace ID 视为 upsert/merge，而非冲突。

2. **事件顺序是发射顺序，非逻辑顺序**：Wire 时间戳是 Dify 的 `LangFuseDataTrace.*` handler *触发*的时间，不是底层工作发生的时间。事件聚集在完成时，而非发生时——直接关系到延迟论点。

3. **`sessionId` 是条件性的**：chat/agent-chat/advanced-chat（会话型）存在；completion（03）和纯 workflow（06、07）不存在。

4. **失败路径仍发射完整事件序列**：09（预设响应）、12（道歉合成）、13（限速 suggested questions）都完成了它们的 Message/GenerateName trace——失败以 `level=ERROR`/`WARNING` + `statusMessage` 记录在出错的 span/generation 上，而非缺失事件。09 是例外，拦截路径完全丢弃 GenerateName。

---

## 8. 确定性与 Provenance

### 确定性

目录从源码**逐字节可复现**：

- **UUID v5**（非 v4）用于事件 ID—— `make_event_id("sNN-eNN")` 每次为相同种子产生相同 ID。
- **固定基准时间戳**每个场景（`_BASE` ISO-8601 常量）—— `make_timestamp(_BASE, offset)` 产生确定性 ISO-8601 字符串。
- **确定性 JSON 序列化**—— 所有 `json.dump` 调用使用 `ensure_ascii=False`、`indent=2`。
- **幂等文件操作**—— `os.makedirs(..., exist_ok=True)`，写入时覆盖。

重新运行 `python3 -m traceset.generate_traceset` 产生与已提交制品逐字节相同的输出。这通过生成后干净的工作树验证。

### Provenance

每个 `meta.json` 锁定两个上游版本：

- `dify_commit`：`b33e8f0ddb1189427548b0e1206cedcdc17d9bb6`—— wire event 字段赋值所验证的 Dify 源码版本。
- `langfuse_sdk_version`：`>=4.2.0,<5.0.0`—— Pydantic 模型定义权威 wire schema 的 Langfuse Python SDK 版本范围。

这些 pin 将合成 trace 绑定到具体上游版本，使目录成为可验证的制品而非自由漂浮的数据集。

---

## 9. 校验策略

### 9.1 双层校验器

`schema.validate_event(event)` 顺序运行两层：

1. **本地校验器**（总是运行）：Envelope 结构、类型有效性、按类型的必填 body 字段、无 snake_case 守卫。仅标准库。
2. **SDK 校验器**（`langfuse` 可导入时运行）：Pydantic `model_validate` 对 `IngestionEvent_TraceCreate` / `IngestionEvent_SpanCreate` / `IngestionEvent_GenerationCreate`—— Langfuse 服务端使用的相同模型。SDK 不存在时静默跳过。

这种拆分实现了**零运行时依赖生成**加 **CI/开发环境的可选权威校验**。

### 9.2 每场景自检（6 项）

由 `generate_scenario()` 在写入任何文件之前运行——失败的场景中止构建：

1. 事件计数匹配 `EXPECTED_EVENT_COUNT`。
2. 所有事件 `type` 值有效。
3. 无 body key 包含 `_`（强制 camelCase）。
4. 时间戳单调非递减。
5. `meta.events_in_order` 长度匹配事件计数。
6. `meta.events_in_order` 每索引类型匹配实际事件类型。

### 9.3 目录级覆盖断言（2 项）

由 `main()` 在所有场景生成后运行：

1. **全部 7 种 Dify trace 类型**在目录的 `TRACE_TYPES_EMITTED` 字段中有代表。
2. **全部 5 种 Dify 应用模式**在目录的 `DIFY_APP_MODE` 字段中有代表。

这些保证目录是*完整的*覆盖矩阵，而非少数示例。

---

## 10. 待解决问题

两个问题在场景 `meta.json` `notes` 字段和设计 spec（[`docs/superpowers/specs/2026-06-26-dify-trace-catalog-design.md`](./superpowers/specs/2026-06-26-dify-trace-catalog-design.md)）中标记：

1. **场景 09（moderation-blocked）**—— 当 moderation 拦截输入（无真实 LLM 调用）时，`MessageTraceInfo` 是否仍发射 `generation-create`？目录**假设是**（3 个事件，带预设响应 `generation-create`）。Wire 确认了这一假设—— `generation-create` 以 `metadata.preset_response: true` 和小 token 用量（输入 15 / 输出 40）触发。**状态**：假设由目录自身 wire 输出验证；仍需对照 Dify 源码（`message_service.py` / `input_moderation.py`）确认生产保真度。

2. **场景 14（streaming）**—— `gen_ai_server_time_to_first_token`（TTFT）和 `llm_streaming_time_to_generate`（TTG）的确切元数据 key 名。目录在 `generation-create` body 的 `metadata` 中使用这些 key 名。**状态**：Wire 输出一致使用这些名称；仍需对照 `LangFuseDataTrace.message_trace` 源码验证生产保真度。

3. **Agent 发射顺序（较软项）**—— 对于 agent 场景（04、05、12），`ToolTraceInfo` 与 `MessageTraceInfo` 的确切相对发射顺序取决于 Dify agent 执行循环中回调触发的时机。目录假设 tool span 在 message `generation-create` 之前发射。这与 Dify 的 `add_trace_task()` 调用序列一致，但尚未对照运行中的 agent 验证。

---

## 11. 实现状态

**已完成。** 实现计划（[`docs/superpowers/plans/2026-06-26-dify-trace-catalog.md`](./superpowers/plans/2026-06-26-dify-trace-catalog.md)）的全部 15 个任务已完成，通过 `--no-ff` 合并 commit `3b83cc4` 合并到 `main`。

| 指标 | 值 |
|---|---|
| 场景 | 14 |
| Wire event | 88（26 trace-create + 40 span-create + 22 generation-create） |
| 测试 | 43 通过（8 schema + 12 helper + 14 scenario + 9 generation） |
| 每场景自检 | 6 |
| 目录级断言 | 2 |
| 边界场景 | 6（high-N、moderation-blocked、empty-RAG、tool-error、suggested-questions-error、streaming） |
| 覆盖 Dify trace 类型 | 7 / 7 |
| 覆盖 Dify 应用模式 | 5 / 5 |
| 运行时依赖 | 0 |
| 开发依赖 | `langfuse>=4.2.0,<5.0.0`、`pytest>=7.0` |
| Python 版本 | >=3.10 |
| 总源码行数 | ~3,480（27 个 Python/TOML 文件） |

**复现**：`python3 -m traceset.generate_traceset`（从仓库根目录）。**测试**：`python3 -m pytest traceset/`。

---

## 12. 参考

### 项目文档

- [`docs/01-research-report.md`](./01-research-report.md) — 延迟研究综合（5 层分解、按方法的延迟预算、证伪的调参项、7 个选项、关键陷阱）。
- [`docs/02-dify-trace-flow.md`](./02-dify-trace-flow.md) — Dify trace 发射深入（Step 0-5 带代码引用、12 个延迟场景 A-L）。
- [`docs/03-langfuse-staging-tables.md`](./03-langfuse-staging-tables.md) — Langfuse 服务端写入路径内部（staging 表、v4 direct-write 路径、服务端调参项）。
- [`docs/superpowers/specs/2026-06-26-dify-trace-catalog-design.md`](./superpowers/specs/2026-06-26-dify-trace-catalog-design.md) — 目录设计 spec（10 节：概览、场景目录、目录结构、wire 格式、meta schema、真实感规范、生成方法、范围边界、参考）。
- [`docs/superpowers/plans/2026-06-26-dify-trace-catalog.md`](./superpowers/plans/2026-06-26-dify-trace-catalog.md) — 15 任务 TDD 实现计划，含完整代码。

### Dify 源码（锁定于 `b33e8f0ddb1189427548b0e1206cedcdc17d9bb6`）

- `api/core/ops/ops_trace_manager.py:1485-1568` — `OpsTraceManager.add_trace_task()`、`run()`、`collect_tasks()`、`send_to_celery()`。
- `api/core/ops/trace_entity.py` — `TraceTask` dataclass、`trace_info_info_map`（10 条目分发表）。
- `api/providers/trace/trace-langfuse/src/dify_trace_langfuse/langfuse_trace.py` — `LangFuseDataTrace` provider，含 `message_trace`、`workflow_trace`、`moderation_trace`、`dataset_retrieval_trace`、`tool_trace`、`generate_name_trace`、`suggested_question_trace` handler。
- `api/core/app/easy_ui_based_generate_task_pipeline.py:419` — `MessageTraceInfo` 调用点。
- `api/core/app/workflow/layers/persistence.py:439` — `WorkflowTraceInfo` 调用点。
- `api/core/moderation/input_moderation.py:52` — `ModerationTraceInfo` 调用点。
- `api/core/rag/retrieval/dataset_retrieval.py:998` — `DatasetRetrievalTraceInfo` 调用点。
- `api/core/callback_handler/agent_tool_callback_handler.py:74` — `ToolTraceInfo` 调用点。
- `api/core/llm_generator/llm_generator.py:140` — `GenerateNameTraceInfo` + `SuggestedQuestionTraceInfo` 调用点。

### Langfuse 源码（锁定于 `216d422635f5634bfaa8a295041f92c81a1c2aed`）

- `src/server/ingestion.ts` — REST ingestion 端点。
- `src/server/otel/v1/traces/index.ts` — OTLP ingestion 端点。
- `src/server/services/EventService.ts:66-91` — `getDelay()`（REST 5s、OTLP 0s）。
- `src/server/queues/otelIngestionQueue.ts:358-459` — v4 direct-write 路由决策。
- `src/server/env.ts:97-108` — `LANGFUSE_INGESTION_QUEUE_DELAY_MS` 及相关环境变量。

### Langfuse Python SDK（锁定于 `>=4.2.0,<5.0.0`）

- `langfuse/api.py` — `IngestionEvent_TraceCreate`、`IngestionEvent_SpanCreate`、`IngestionEvent_GenerationCreate` Pydantic 模型（`schema.py` SDK 层使用）。
- `langfuse/span_processor.py` — `BatchSpanProcessor`（Dify 不使用的 OTel 路径）。
