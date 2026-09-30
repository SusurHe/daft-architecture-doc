# Daft 统一接入口设计（零破坏版）

> 目标：在**不改变任何现有前端入口**（`daft.read_*`、`df.write_*`、`daft.read_table`、`write_sink`、catalog 注册）的前提下，
> 引入一层统一的、能力可声明、可校验、可扩展的后端接入协议。
>
> 基准：`Daft` 仓库 `main` @ `dadd8a0`（2026-09-25，约 v0.7.25）。文中所有现状描述均带 `文件:行号` 证据。
>
> **v2 修正（重要）**：v1 把"后端"简化成单一 `scheme → provider` 注册表，并要求 `sink()` 必须返回 `DataSink`。
> 这两点对**内置格式（Parquet/CSV/JSON/Avro/Text/Mcap/Warc）与数据湖（Iceberg/Delta/Hudi/Paimon/Lance）都是错的**：
> 真实读路径是**存储轴（`StorageConfig`）× 格式轴（`FileFormatConfig`）两个独立参数**（`daft/io/common.py:20-49`、
> `src/daft-scan/src/file_format_config.rs:21-29`）；写路径已有**三种形态**（`SinkInfo::{OutputFileInfo, CatalogInfo, DataSinkInfo}`，
> `src/daft-logical-plan/src/sink_info.rs:17-23`）。v2 引入**双轴解析**、**`SinkSpec` 联合类型**、**内置后端能力矩阵**（见 §3.6、§7.6）与 §8 阶段调整。

---

## 1. 问题陈述

### 1.1 现状：扩展点是"三个，彼此独立"

| 轴 | 现有抽象 | 位置 | 谁在用 |
|---|---|---|---|
| 读 | `ScanOperator`（trait，6 个能力方法） | `src/daft-scan/src/scan_operator.rs:14-70` | 所有内置 reader |
| 读（Python） | `DataSource` → `ScanOperatorHandle.from_data_source` → `LogicalPlanBuilder.from_tabular_scan` | `daft/io/source.py:103-111`、`daft/logical/builder.py:122-124` | 自定义源、表格式 |
| 写 | `DataSink`（`name/schema/start/write/finalize`） | `daft/io/sink.py:31-75` | `write_sink` 及 5 个专用 sink |
| 命名 | `Catalog` / `Table` | `src/daft-catalog/src/catalog.rs:12-42`、`daft/catalog/__init__.py:880-1108` | SQL、`daft.read_table`、Session |

它们之间**没有绑定关系**：同一个后端要分别写三处代码，且能力差异无处声明。典型后果：

- **写侧没有任何能力声明位**：`DataSink.schema()` 是"`finalize()` 输出统计的 schema"（`daft/io/sink.py:53-58`），不是数据表 schema；因此"这个 sink 接受哪些 dtype"只能各自为政——`SQLDataSink` 用私有方法显式检测（`daft/io/_sql.py:177-192`），`ClickHouseDataSink` 什么都不做直接 `to_pandas()` + `insert_df`（`daft/io/clickhouse/clickhouse_data_sink.py:61-68`）。
- **类型映射没有归属层**：Postgres 的 vector 列读回来是 list，需要在 catalog 里手写 cast 回 embedding（`daft/catalog/__postgres.py:633-634`）；DDL 类型映射也写在 catalog 里（`:144-154`）。
- **同一后端两套实现**：`PostgresTable.append` 自己用 psycopg `COPY ... BINARY`，源码注释写明 **单节点串行**，并挂着 TODO 要换成 `write_sql`（`daft/catalog/__postgres.py:660-690`）；而 `df.write_sql` 走 `SQLDataSink` 的 `to_sql`，按 micropartition 并行/分布式（`daft/io/_sql.py:325-364`）。对照：Iceberg catalog 只是转发 `read_iceberg` / `write_iceberg`（`daft/catalog/__iceberg.py:286-305`），因此**不存在**漂移。
- **入口是硬编码 import**：`daft/io/__init__.py:18-40` 逐个手写 `read_*`；全仓无 `entry_points` / `importlib.metadata` 插件发现机制，第三方无法"注册一个后端"。
- **传输选择散落在字符串解析里**：是否走 ConnectorX 由 `SQLConnection._should_use_connectorx()` 从 URL 的 dialect/driver 推断（`daft/sql/sql_connection.py:118-131`），而不是后端自己声明。

### 1.2 约束（硬性）

1. **零破坏**：`daft.read_parquet/csv/json/avro/text/sql/iceberg/...`、`df.write_parquet/csv/json/iceberg/delta_lake/lance/clickhouse/bigtable/turbopuffer/huggingface/sink`、`daft.read_table`、`Session.attach_catalog/attach_table` 的**签名、返回类型、语义、异常类型**全部保持不变。
2. **不引入第二套引擎**：新层只做"工厂 + 能力声明 + 校验"，不参与数据传输路径，不产生额外拷贝。
3. **渐进可落地**：每一阶段都能独立合并、可回滚，且都带测试。

---

## 2. 设计目标 / 非目标

**目标**

| # | 目标 | 衡量方式 |
|---|---|---|
| G1 | 后端只需实现**一处** scan/sink/catalog 逻辑 | 同一后端的 catalog 与连接器入口走同一实现（一致性测试断言） |
| G2 | **能力可声明、可查询、可校验** | `provider.capabilities` + 写前 dtype 预检 + 可行动报错 |
| G3 | **类型映射显式化** | 每个后端声明 dtype → 后端类型的三态映射（native / serialize / reject） |
| G4 | 第三方可接入而无需改 Daft 源码 | 注册表 + entry point 发现；不装依赖时给出清晰报错 |
| G5 | 统一入口（新增）覆盖"按 URI 打开"与"按名字打开" | `daft.open(uri)` 与 `daft.read_table(name)` 语义对齐 |

**非目标**

- 不改 Rust 侧的 `ScanOperator` trait 语义（它继续是读能力的最终承载者）；
- 不把 catalog 变成传输层（catalog 仍然只做命名与解析）；
- 不统一"所有后端的行为"（例如列式块 vs 行式 INSERT 的差异是真实存在的，目标是把差异**显式化**，而不是抹平）；
- 不在第一阶段做 Rust 侧 provider（Python 先行，Rust 通过既有 FFI 桥接入）。

---

## 3. 核心抽象

### 3.1 Capabilities：后端能力声明

```python
# daft/storage/capabilities.py（新增）
from dataclasses import dataclass
from enum import Enum

class DTypeSupport(str, Enum):
    NATIVE = "native"        # 原样映射，双向无损
    SERIALIZE = "serialize"  # 可写，但会序列化（JSON/文本/字节）
    REJECT = "reject"        # 明确不支持

@dataclass(frozen=True)
class Capabilities:
    # 方向
    read: bool = False
    write: bool = False
    named_tables: bool = False          # 能否以"名字"访问（可提供 catalog）

    # 读侧
    pushdown_filters: bool = False
    pushdown_projection: bool = True
    pushdown_limit: bool = False
    statistics: bool = False            # 能在 scan 前提供行数/大小
    partitioned_scan: bool = False      # 支持按列分片并行读
    count_pushdown: bool = False

    # 写侧
    distributed_write: bool = True      # sink 可分发到多 worker（False = 单节点）
    bulk_columnar_insert: bool = False  # 列式块/批量协议（而非语句级 INSERT）
    server_side_settings: bool = False  # 可透传服务端设置（如 ClickHouse async_insert）
    write_modes: tuple[str, ...] = ("append",)   # append / overwrite / create

    # 依赖
    requires: tuple[str, ...] = ()      # 缺失时用于生成可行动报错
```

**字段最小化原则**：只声明"会改变用户决策或错误信息"的能力。`distributed_write` 与 `bulk_columnar_insert` 这两个字段直接对应我们讨论过的 Postgres 案例——它们让"catalog 写入是单节点串行"这种事实**可被查询**，而不是藏在实现里。

### 3.2 TypeMapping：类型映射显式化

```python
# daft/storage/typemap.py（新增）
from dataclasses import dataclass
from typing import Any, Callable, Literal
from daft.datatype import DataType

@dataclass(frozen=True)
class TypeMapping:
    """后端类型映射：三态 + 双向转换 + 非原生列策略。"""
    support: Callable[[DataType], DTypeSupport]
    to_backend: Callable[[DataType], Any] | None = None       # Daft dtype → 后端 DDL/字段类型
    from_backend: Callable[[Any], DataType] | None = None     # 后端类型 → Daft dtype
    non_primitive: Literal["error", "str", "bytes"] = "error" # 与 write_sql 既有语义对齐
```

- 读侧缺口修在**这里**：Postgres 的 `vector → list → embedding` 应该写成 `from_backend` 的一个规则，而不是 catalog 里的一句 `cast`；
- 写侧缺口修在**这里**：`image/tensor/embedding` 到底是 `SERIALIZE` 还是 `REJECT`，由后端声明，报错由引擎统一生成；
- 这个对象天然可测试：一张 dtype 矩阵 × 每个后端 = conformance 用例（见 §6）。

### 3.3 Provider：唯一的后端契约

```python
# daft/storage/provider.py（新增）
from typing import Protocol, runtime_checkable

@runtime_checkable
class Provider(Protocol):
    name: str                                  # "clickhouse" / "parquet" / "iceberg"
    kind: ProviderKind                         # STORAGE | FORMAT | LAKE | TABLE | VIRTUAL（见 §3.6）
    keys: tuple[str, ...]                      # STORAGE: ("s3","gs",…)；FORMAT: ("parquet","pq")；TABLE: ("clickhouse",)
    capabilities: Capabilities
    type_mapping: TypeMapping

    # 按 kind 实现其中若干项
    def scan(self, uri: str, **options) -> "DataSource | ScanOperator": ...
    def sink(self, uri: str, **options) -> "SinkSpec": ...     # ★ v2：联合类型，不是 DataSink
    def catalog(self, uri: str, **options) -> "Catalog": ...
```

**`SinkSpec`：写路径必须保留 Daft 既有的三种形态**（v1 的错误在于强行统一成 `DataSink`）：

```python
# daft/storage/sink_spec.py（新增）
SinkSpec = Union[
    NativeTabularSink,   # → LogicalPlanBuilder.write_tabular(...) → SinkInfo::OutputFileInfo（Rust writer，最快路径）
    CatalogSink,         # → SinkInfo::CatalogInfo（Iceberg/Delta/Paimon/Lance 的表格写出）
    PythonDataSink,      # → DataFrame.write_sink(...) → SinkInfo::DataSinkInfo（ClickHouse/Bigtable/Turbopuffer/HF）
]
```

依据：`SinkInfo` 的三变体定义在 `src/daft-logical-plan/src/sink_info.rs:17-23`；`OutputFileInfo{root_dir, write_mode, file_format, format_option}` 承载原生文件写出，`write_parquet/csv/json/avro` 经 `LogicalPlanBuilder.write_tabular`（`daft/dataframe/dataframe.py:1085` 等 4 处）落到这里。**内置格式绝不能被迫走 Python sink**——那是实打实的性能回归。

关键约定（写进 provider 基类文档 + 注册时校验）：

1. `sink()` 返回 `SinkSpec`，由引擎按形态路由到对应的既有分发机制；**不新开任何写通道**；
2. `scan()` 返回的 `DataSource` 最终经 `DataSource.read()` 变成 DataFrame（`daft/io/source.py:103-111`），读路径也不新开通道；
3. `catalog()` 返回的 `Table.read/append/overwrite` **只允许转发**到本 provider 的 `scan()/sink()`（单一实现原则，见 §5）；
4. provider 只做工厂与声明，**不得**在 `scan/sink` 之外持有连接或缓存数据。

### 3.4 Registry 与解析

```python
# daft/storage/registry.py（新增）
import importlib.metadata as md

_REGISTRY: dict[str, Provider] = {}      # scheme -> provider
ENTRY_POINT_GROUP = "daft.providers"

def register(provider: Provider, *, override: bool = False) -> None:
    for scheme in provider.schemes:
        if scheme in _REGISTRY and not override:
            raise ValueError(f"scheme {scheme!r} 已被 {_REGISTRY[scheme].name} 注册；如需覆盖请显式 override=True")
        _REGISTRY[scheme] = provider

def discover() -> None:
    """懒发现第三方 provider（无缓存、失败不致命）。"""
    for ep in md.entry_points(group=ENTRY_POINT_GROUP):
        try:
            register(ep.load()())
        except Exception as e:      # 第三方依赖缺失不应让 import daft 失败
            _warn_unavailable(ep.name, e)

def resolve(uri: str) -> Provider:
    scheme = uri.split("://", 1)[0].lower()
    if scheme not in _REGISTRY:
        discover()
    if scheme not in _REGISTRY:
        raise NotImplementedError(
            f"没有可处理 {scheme!r} 的 provider。已注册：{sorted(_REGISTRY)}\n"
            f"如果你的后端是 ClickHouse，请安装 'daft[clickhouse]'；自研后端可实现 Provider 并调用 daft.storage.register()。"
        )
    return _REGISTRY[scheme]
```

### 3.5 Handle：统一入口的返回对象

```python
class StorageHandle:
    def __init__(self, uri: str, provider: Provider, **options): ...

    @property
    def capabilities(self) -> Capabilities: ...

    def read(self) -> "DataFrame":                       # = provider.scan(uri).read()
    def sink(self, **options) -> "DataSink":             # 供 write_sink 使用
    def write(self, df, mode: str = "append", **options) -> "DataFrame":
        precheck_dtypes(df.schema(), self.provider)      # ★ 写前预检（见 §6.2）
        if mode == "append":  return df.write_sink(self.sink(**options))
        return self.as_catalog().write(df, mode=mode)     # overwrite/create 走表格语义
    def as_catalog(self) -> "Catalog": ...               # capabilities.named_tables 为真时可用
    def explain(self) -> str: ...                        # 打印将走哪条实现（便于排查"两条路径"）
```

`explain()` 是刻意加的：**把"这一次写的到底是快路径还是慢路径"直接暴露给用户**，正是当前最缺的信息。

---

### 3.6 双轴模型：内置格式与数据湖如何纳入（v2 新增，修正 v1 的模型错误）

**v1 的错误**：把 `parquet/csv/iceberg` 当成"和 `clickhouse` 一样的 scheme 型后端"。实际上内置路径是**两个独立参数的组合**：

| 轴 | 承载者 | 证据 |
|---|---|---|
| **存储（transport）** | `StorageConfig` + `IOClient`/`SourceType` 9 种后端 | `src/daft-io/src/lib.rs:517-527` |
| **格式（format）** | `FileFormatConfig::{Parquet, Csv, Json, Warc, Text, Avro, Mcap}` | `src/daft-scan/src/file_format_config.rs:21-29` |
| 二者如何拼装 | `get_tabular_files_scan(path, …, file_format_config, storage_config, …)` → `ScanOperatorHandle.glob_scan(...)` | `daft/io/common.py:20-49` |
| **数据湖** | Python `DataSource`（自持快照/分区/字段 ID），再进同一个 scan 通道 | `daft/io/iceberg/_iceberg.py:183-200` |

因此 `s3://bucket/x.parquet` 需要解析出"**存储 = s3**"和"**格式 = parquet**"两件事，单一 `scheme → provider` 注册表无法表达。

**修正后的模型：四类 provider + 解析流水线**

```
URI ──► [STORAGE provider]   key = scheme（local / s3 / gs / az / http / hf / unity …）
    ──► [FORMAT provider]    key = 显式 format=  >  扩展名推断  >  报错（不猜）
    ──► scan() / sink()，能力 = meet(storage.capabilities, format.capabilities)

并行入口：
  [TABLE provider]    按名字：daft.read_table("cat.ns.t")、Session catalog（不变）
  [VIRTUAL provider]  无 URI：from_pydict / read_generator / _range / in-memory（只读）
```

- **LAKE provider**（Iceberg/Delta/Hudi/Paimon/Lance）视作"带元数据的格式"：key 是 scheme+format 组合，
  其 `scan()` 内部**复用 FORMAT provider 的文件读取实现**（例如 Iceberg 的文件读取最终仍落到 parquet 的 scan），
  自身只负责快照、分区裁剪、字段 ID 映射这类元数据逻辑——与现状（`IcebergDataSource` 委托文件扫描）一致。
- **能力必须组合（meet）而不是各自声明**：这是 v2 新增的硬规则。

| 目标 URI | 存储能力 | 格式能力 | 合成后的有效能力 |
|---|---|---|---|
| `s3://b/x.parquet` | range GET、并发连接、重试 | projection/filter/limit 下推、metadata 统计、行组切分 | 全部可用（当前最快路径） |
| `s3://b/x.csv` | 同上 | 分块 + 并行解析、limit | **无统计、无谓词下推**（声明里必须为 false） |
| `file:///x.parquet` | pread + 64 KiB 合并 | 同 parquet | 本地最快路径 |
| `iceberg://…` | 由 catalog 给出文件清单 | parquet 能力 + 分区裁剪 + 字段 ID 映射 | 分区裁剪叠加在 parquet 能力之上 |

**URI 解析规则（显式化，避免歧义）**

1. 显式 `format=` 参数优先级最高；
2. 无 scheme ⇒ 视为本地文件（保持现状语义）；
3. 扩展名推断表：`.parquet/.pq`、`.csv`、`.json/.jsonl/.ndjson`、`.avro`、`.txt`、`.mcap`、`.warc`、`.lance`；
4. 无法推断且未显式指定 ⇒ **报错并列出可用格式**，不允许"猜一个"；
5. 同一 scheme 被两个 provider 注册 ⇒ 注册即失败（§3.4）。

**这条修正带来的一个额外好处**：能力声明不一致的现状可以被顺带修掉。当前 `GlobScanOperator` 的
`can_absorb_filter/select/limit/shard` **全部返回 `false`**（`src/daft-scan/src/glob.rs:599-611`），
只有 `supports_count_pushdown` 依赖格式（Parquet 且非 `ignore_corrupt_files`，`:613-621`）；
但 Parquet 的谓词/列下推实际是生效的——因为下推走 `Pushdowns` 通道，只有 limit 真正检查了 `can_absorb_limit()`
（`push_down_limit.rs:134`），而 `can_absorb_filter/select` 目前仅用于 Python scan operator 桥接
（`src/daft-scan/src/python.rs:527-594`）。v2 要求**把"声明"变成唯一事实来源**：声明为 true 就必须真的下推（由 §6.1 的一致性测试断言）。

## 4. 统一入口 API（纯新增，不动老入口）

```python
import daft

# ① 按 URI 打开（新）
h = daft.open("clickhouse://user:pass@host:8123/analytics/events")
h.capabilities.bulk_columnar_insert      # True
df = h.read()                            # 读：转调 read_sql（ConnectorX）
df.write_sink(h.sink())                  # 写：转调 ClickHouseDataSink

# ② 按名字打开（已有，语义对齐）
df = daft.read_table("my_catalog.my_ns.my_table")

# ③ 探测与自省（新）
daft.storage.list_providers()            # [ProviderInfo(name, schemes, capabilities, requires, available)]
daft.storage.dtype_matrix("clickhouse")  # 该后端对 dtype 的支持矩阵（native/serialize/reject）

# ④ 注册自有后端（新）
daft.storage.register(MyInternalProvider())          # 显式注册
# 或打包时声明 entry point： [project.entry-points."daft.providers"] my = "my_pkg:MyProvider"
```

**为什么这样设计不破坏老入口**：老入口是函数（`read_*`）与方法（`write_*`），新入口是"函数 + 对象"，两者不共享命名空间；老入口在 Phase 1 之后内部改为 `provider.scan()/sink()` 转发，签名与返回值不变。

---

## 5. 兼容映射与单一实现原则

### 5.1 老入口 → provider 转发对照表

| 现有入口 | 内部改为 | 兼容性保证 |
|---|---|---|
| `daft.read_parquet(path, io_config=...)` | FORMAT provider("parquet").scan(**STORAGE provider(path).resolve(path)**) —— 等价于把现有 `get_tabular_files_scan(..., file_format_config, storage_config, ...)` 显式化成两次解析 | 签名与返回类型不变；底层仍是 `ScanOperatorHandle.glob_scan` |
| `daft.read_csv/json/avro/text/mcap/warc(...)` | 同上，仅 format key 不同 | 同上 |
| `daft.read_sql(sql, conn, partition_col=...)` | 由 URL scheme 解析出 provider（`clickhouse://` → ClickHouseProvider，`postgres://` → PostgresProvider…）；连接工厂场景回退到内置 `SQLProvider` | 方言/驱动推断逻辑从 `_should_connectorx` 迁到 provider 的 `options` 声明，行为保持一致 |
| `daft.read_iceberg/delta_lake/hudi/paimon/lance(...)` | LAKE provider（内部复用 parquet 的 scan 实现 + 自身元数据逻辑） | 现为 Python `DataSource`，迁移后仍是同一条 `from_data_source` 通道 |
| `daft.from_pydict / read_generator / _range` | VIRTUAL provider（只读，无 URI） | 不变 |
| `df.write_parquet/csv/json/avro(...)` | `open(uri, format=...).sink()` 返回 **`NativeTabularSink`** → 仍走 `LogicalPlanBuilder.write_tabular` → `SinkInfo::OutputFileInfo` | **性能零变化**（不放宽到 Python sink） |
| `df.write_iceberg/deltalake/paimon/lance(...)` | `open(uri, format=...).sink()` 返回 **`CatalogSink`** → `SinkInfo::CatalogInfo` | 不变 |
| `df.write_clickhouse/bigtable/turbopuffer/huggingface(...)` | `resolve(scheme).sink(...)` 返回 **`PythonDataSink`** → `write_sink` → `SinkInfo::DataSinkInfo` | 参数逐字保留；旧参数与 URI 双通道合并 |
| `df.write_sql(table_name, conn, ...)` | 内置 `SQLProvider`（保留 `non_primitive_handling` 等既有语义） | 不变 |
| `df.write_sink(my_sink)` | **不变**（`DataSink` 仍是稳定扩展点；`PythonDataSink` 只是 `SinkSpec` 的一种） | 第三方自定义 sink 零迁移 |
| `daft.read_table("cat.ns.t")` | Session catalog 解析不变；catalog 由 provider 提供 | 不变 |
| `Session.attach_catalog(...)` / `daft.attach_catalog` | 不变 | 不变 |

### 5.2 单一实现原则（防漂移的核心规约）

> **`Provider.catalog()` 返回的 `Table`，其 `read/append/overwrite` 必须且只能转发到同一个 provider 的 `scan()/sink()`。**

- ✅ 合规样例：Iceberg（`daft/catalog/__iceberg.py:286-305` 三行转发）
- ⚠️ 违规样例：Postgres（`daft/catalog/__postgres.py:660-690` 自建 psycopg COPY 单节点实现）

如何**强制**而不是靠自觉：

1. 注册时校验：`catalog()` 返回的 Table 必须带 `provider_name` 标记，`Table.append` 的实现若不在白名单内则在 CI 报告（可用一个简单的 AST/调用栈检查或 `unittest.mock` 断言"append 期间调用了 `provider.sink`"）；
2. 一致性测试：同一份数据经 `df.write_clickhouse()` 与 `catalog.get_table(...).append(df)` 写入后，读回结果与**计划形状**都必须一致（§6.3）；
3. 代码评审规约写进 `CONTRIBUTING.md`（新增后端 checklist）。

---

## 6. 一致性保障机制

### 6.1 Conformance Test Kit（新增 `daft/storage/conformance.py`）

第三方后端只要引入这个 kit，就能自证合规：

```python
# 用户侧
from daft.storage.conformance import run_conformance

run_conformance(MyProvider(), throwaway_uri="my-scheme://localhost/test")
```

kit 内置用例（每个 provider 都必须通过）：

| 用例 | 断言 |
|---|---|
| dtype 往返 | 20+ dtype 的矩阵 DataFrame 写入后读回，语义相等；`REJECT` 的 dtype 必须在**写法上**报错（不是写到一半失败） |
| 空表 / 全 null / 超长字符串 / 负数时间戳 | 不崩溃、语义正确 |
| 入口一致性 | `provider.scan().read()` 与 `catalog().get_table().read()` 结果一致；`sink().write()` 与 `Table.append()` 结果一致 |
| 能力诚实性 | 声明 `pushdown_filters=True` 时，带 `where` 的计划必须真的把谓词下推到 scan（比对优化后计划文本） |
| 依赖缺失 | 卸载可选依赖后，入口报错必须包含安装指令（`capabilities.requires`） |
| 分布式写 | 声明 `distributed_write=False` 时，`handle.explain()` 必须明确提示"单节点串行写" |

### 6.2 写前 dtype 预检（把"静默失败"变成"可行动报错"）

```python
def precheck_dtypes(schema, provider) -> None:
    bad = [(f.name, f.dtype) for f in schema if provider.type_mapping.support(f.dtype) is DTypeSupport.REJECT]
    if not bad:
        return
    cols = ", ".join(f"{n}:{d}" for n, d in bad)
    raise ValueError(
        f"{provider.name} 的写入不支持以下列：{cols}\n"
        f"可选方案：\n"
        f"  ① 先转换：image_encode(col(...)) / col(...).cast(DataType.binary())\n"
        f"  ② 序列化写入：df.write_sql(..., non_primitive_handling='str')\n"
        f"  ③ 查看支持矩阵：daft.storage.dtype_matrix('{provider.name}')\n"
        f"  ④ 若应支持，请给后端提 issue 并附上 capabilities.write_modes"
    )
```

这就是把 `SQLDataSink._detect_non_primitive_columns()`（`daft/io/_sql.py:177-192`）的经验**上提为引擎级契约**，让每个后端免费获得同样的保护。

### 6.3 计划一致性断言（可加进 CI）

```python
def assert_paths_equivalent(df_builder, provider):
    plan_a = df_builder().write_sink(provider.sink(...)).explain(show_all=True)
    plan_b = provider.catalog(...).get_table("t").append(df_builder()) and last_written_plan()
    assert normalize(plan_a) == normalize(plan_b)
```

---

## 7. 示例：ClickHouse Provider（最小实现）

```python
# daft/io/clickhouse/provider.py（新增）
from daft.datatype import DataType
from daft.io.clickhouse.clickhouse_data_sink import ClickHouseDataSink
from daft.io._sql import read_sql
from daft.storage import Capabilities, DTypeSupport, Provider, TypeMapping, register

_NATIVE = {DataType.int8(), DataType.int16(), DataType.int32(), DataType.int64(),
           DataType.uint8(), DataType.uint16(), DataType.uint32(), DataType.uint64(),
           DataType.float32(), DataType.float64(), DataType.bool(), DataType.string(),
           DataType.binary(), DataType.date(), DataType.timestamp("us")}
_SERIALIZE = {DataType.list(DataType.int64()), DataType.struct({}), DataType.map(...)}  # 序列化为 JSON 文本

class ClickHouseTypeMapping(TypeMapping):
    def __init__(self):
        super().__init__(
            support=lambda dt: (DTypeSupport.NATIVE if dt in _NATIVE else
                                DTypeSupport.SERIALIZE if dt in _SERIALIZE else
                                DTypeSupport.REJECT),      # image/tensor/embedding → REJECT（显式！）
            non_primitive="str",
        )

class ClickHouseProvider(Provider):
    name = "clickhouse"
    schemes = ("clickhouse", "ch")
    capabilities = Capabilities(
        read=True, write=True, named_tables=True,
        pushdown_filters=True, pushdown_projection=True, pushdown_limit=True,
        statistics=True, count_pushdown=True,        # 由 read_sql/ConnectorX 提供
        distributed_write=True, bulk_columnar_insert=True, server_side_settings=True,
        write_modes=("append",),
        requires=("clickhouse_connect",),            # 写侧依赖；读侧需要 daft[sql]
    )
    type_mapping = ClickHouseTypeMapping()

    def scan(self, uri, *, query=None, partition_col=None, num_partitions=None, **options):
        # 单一实现：读就是 read_sql（ConnectorX 路径）
        sql = query or f"SELECT * FROM {_table_of(uri)}"
        return _ReadSqlSource(sql, uri, partition_col=partition_col, num_partitions=num_partitions, **options)

    def sink(self, uri, *, table=None, client_kwargs=None, write_kwargs=None, **options):
        # 单一实现：写就是既有的专用 sink（不重复实现协议）
        return ClickHouseDataSink(
            table=_table_of(uri, table),
            host=uri.host, port=uri.port, user=uri.user, password=uri.password, database=uri.database,
            client_kwargs=client_kwargs, write_kwargs=write_kwargs,
        )

    def catalog(self, uri, **options):
        return ClickHouseCatalog(uri, self)          # 见下：只做转发

register(ClickHouseProvider())
```

配套的 catalog（**三行转发，复刻 Iceberg 模式**）：

```python
class ClickHouseTable(Table):
    def __init__(self, provider, uri, ident): ...
    def schema(self):   return self._provider.scan(self._uri).schema()
    def read(self, **o):        return self._provider.scan(self._uri, **o).read()
    def append(self, df, **o):  df.write_sink(self._provider.sink(self._uri, **o))
    def overwrite(self, df, **o):
        if "overwrite" not in self._provider.capabilities.write_modes:
            raise NotImplementedError("ClickHouse provider 未声明 overwrite；请用 DROP + append 或写入新表")
        ...
```

改造后的结果（对用户可见的变化）：

| 场景 | 现在 | 改造后 |
|---|---|---|
| 读 | `read_sql`（要自己知道 URL 形态） | `daft.read_clickhouse(...)` 别名 + `daft.open("clickhouse://...").read()` |
| 写 | `write_clickhouse`（无校验、多模态列静默失败） | 同一入口，但**写前预检** + 可行动报错 |
| SQL | 需手动 attach catalog（若存在） | `daft.open(...).as_catalog()` / session attach |
| 能力差异 | 靠读源码或踩坑 | `daft.storage.dtype_matrix("clickhouse")` / `handle.explain()` |

---

### 7.6 内置后端的能力矩阵（v2 新增：它们必须第一批纳入，而不是被绕开）

FORMA/STORAGE/LAKE 三类内置 provider 的声明应当**直接来自现有实现的能力**，并由测试反向校验（§6.1）。草案：

| Provider | kind | key | 读能力（声明） | 写形态 | 现状依据 |
|---|---|---|---|---|---|
| `file`/`s3`/`gs`/`az`/`http`/`hf`/`unity`… | STORAGE | scheme | 传输/range/重试/并发；**不单独对外 scan** | — | `SourceType` 9 种（`src/daft-io/src/lib.rs:517-527`） |
| `parquet` | FORMAT | `parquet`,`pq` | projection ✅ filter ✅ limit ✅ stats ✅（metadata）count ✅（非 ignore_corrupt）hive 分区 ✅ | `NativeTabularSink` | `FileFormatConfig`（`file_format_config.rs:21-29`）；`supports_count_pushdown`（`glob.rs:613-621`） |
| `csv` | FORMAT | `csv` | limit ✅；**pushdown ❌ stats ❌** | `NativeTabularSink` | 整文件一 task，文件内 4 MiB slab + rayon |
| `json` | FORMAT | `json`,`jsonl`,`ndjson` | limit ✅；JSONL 支持字节范围切分 | `NativeTabularSink` | `simd-json`；字符串字段无统计 |
| `avro` | FORMAT | `avro` | 整文件 + range 读；无范围切分 | `NativeTabularSink` | `arrow-avro` |
| `text` | FORMAT | `txt`,`text` | 单列 Utf8，仅 UTF-8 | `NativeTabularSink` | `src/daft-text/src/read.rs:50-63` |
| `mcap` / `warc` | FORMAT | `mcap`,`warc` | Rust crate 解析，只读 | — | `src/daft-mcap`、`src/daft-warc` |
| `iceberg` | LAKE | `iceberg` | parquet 能力 **+** 快照/分区裁剪/字段 ID 映射 | `CatalogSink` | `IcebergDataSource`（`daft/io/iceberg/_iceberg.py:183-200`）+ `metadata.rs:131-251` |
| `delta` / `hudi` / `paimon` / `lance` | LAKE | 各自 | 同型：元数据逻辑 + 底层文件能力 | `CatalogSink`（Lance 写走 Rust writer） | `daft/io/{delta_lake,hudi,paimon,lance}/` |
| `python-sql` | TABLE | `postgres`,`mysql`,… | 由 `read_sql` 引擎决定（ConnectorX/SQLAlchemy/未来的 ADBC） | `PythonDataSink` | `sql_connection.py:118-131`、`_sql.py:142` |

这张表有两个作用：

1. **用户可见**：`daft.storage.dtype_matrix("csv")` / `capabilities` 能提前告诉用户"Csv 不支持谓词下推"，而不是让用户在计划里发现 filter 停在半空；
2. **开发可见**：新加格式时，能力声明是 checklist，"声明为 true 但实现没做" 会被一致性测试抓住（这是当前 `can_absorb_*` 全 false 却仍能下推这类不一致的根治办法）。

**迁移三步（内置后端）**

| 步 | 动作 | 风险 |
|---|---|---|
| 1 | 把 `get_tabular_files_scan` 的两个参数抽成 `(storage_provider, format_provider)` 两个对象，但**行为不变** | 低（纯重构，`tests/io/**` 覆盖充分） |
| 2 | 让 `read_*` / `write_*` 走 `daft.open(uri, format=...)`，`SinkSpec` 分别产出 `NativeTabularSink` | 低（计划形状与现在逐字一致） |
| 3 | 数据湖 provider 复用 FORMAT provider（Iceberg 的文件读取调 parquet 的 scan），删掉重复解析 | 中（需要对照 pyiceberg 的行为） |

## 8. 分阶段落地（每阶段可独立合并、可回滚）

| 阶段 | 内容 | 破坏性 | 测试 |
|---|---|---|---|
| **P0**（~1 周） | 新增 `daft/storage/`（Provider/Capabilities/TypeMapping/SinkSpec/Registry/Handle）+ `daft.open()` + `list_providers()`；注册 **STORAGE 与 FORMAT 两类内置 provider 的声明**（含 §7.6 能力矩阵），**不改任何老入口实现** | 无（纯新增） | 单测：双轴解析、扩展名推断与"无法推断则报错"、注册冲突、`daft.open` 解析 |
| **P1**（~1–2 周） | **先用内置文件后端验证双轴模型**：`read_parquet/csv/json/avro/text` 改为 `STORAGE × FORMAT` 两次解析（行为逐字等价），`write_parquet/csv/json/avro` 产出 `NativeTabularSink` | 无 | 直接复用现有 `tests/io/**` 作等价性护栏；断言计划形状不变 |
| **P2**（~2 周） | **数据湖 provider**：Iceberg 试点（`LAKE` 复用 `FORMAT("parquet")` 的 scan + 自身快照/分区裁剪），随后 Delta/Hudi/Paimon/Lance | 无 API 变更 | 与现有 `tests/io/iceberg/**` 对照；断言字段 ID 映射与分区裁剪行为不变 |
| **P3**（~2 周） | **数据库 provider**：ClickHouse 完整落地（`PythonDataSink` 形态 + `read_clickhouse` 别名 + dtype 预检）；修 Postgres catalog 内联实现（改走 `write_sql`） | 无 API 变更；行为改善（PG 写入由单节点串行变为可分布式） | conformance kit + 入口一致性断言 + dtype 矩阵 |
| **P4** | 扩展点收敛：新后端只接受 provider 形式；`write_*` 逐个标 `@deprecated`（保留 ≥2 个 minor）；docs 自动生成能力矩阵 | 无（弃用而非删除） | conformance kit 进 CI，未通过不允许合并 |

> 与 v1 的差别：**P1 从"拿 read_parquet 顺手改一下"升级为"用内置文件后端验证双轴模型"**。
> 理由：内置格式覆盖了绝大多数实际查询，且 `tests/io/**` 是最完整的回归护栏；双轴模型若在这里立不住，后面数据湖与数据库的抽象都会歪。

---

## 9. 风险与取舍

| 风险 | 说明 | 缓解 |
|---|---|---|
| **抽象膨胀** | Capabilities 字段会被不断加，最后变成 feature flag 垃圾场 | 字段准入标准："是否改变用户决策或错误信息"；每季度清理一次未使用字段 |
| **兼容层双份代码** | 老入口转发期间可能同时存在新旧实现 | P1 只做逐字等价转发，禁止顺手改行为；用现有测试作为护栏 |
| **provider 与 extras 的依赖矩阵** | 缺依赖时报错必须清晰 | `capabilities.requires` + 注册时惰性探测（`available` 字段） |
| **Rust/Python 不对称** | Rust 侧 provider 暂不做，可能造成两套写法 | 明确 P0–P2 只覆盖 Python；Rust 通过既有 `daft-scan/src/python/wrappers.rs` 桥接，未来再做 Rust 侧 trait 对齐 |
| **性能回归** | 多一层工厂调用 | provider 只在计划构建期被调用（不在数据路径）；用"同一查询的 plan + 执行时间与现状无差异"作为验收 |
| **规范化成本** | 规约（单一实现原则）靠自觉 | 用 P3 的 conformance kit 与 CI 断言把它变成**机器可检查**的事 |

---

## 10. 与现有扩展点的关系（不废弃任何东西）

| 现有 | 新定位 |
|---|---|
| `StorageConfig` + `IOClient` / `SourceType` | **存储轴的实现**：成为 `STORAGE` provider 的内部实现；9 种后端与凭据模型完全不变（`src/daft-io/src/lib.rs:517-527`） |
| `FileFormatConfig`（Parquet/Csv/Json/…） | **格式轴的实现**：成为 `FORMAT` provider 的内部实现；`get_tabular_files_scan` 的两个参数变为两个 provider 对象（`daft/io/common.py:20-49`） |
| `ScanOperator` / `DataSource` | **读能力的实现细节**：provider.scan() 返回它们；能力声明上提到 `Capabilities`，但 trait 本身不变 |
| `SinkInfo::{OutputFileInfo, CatalogInfo, DataSinkInfo}` + `daft-writers` | **写路径的三种形态**：由 `SinkSpec` 分别对应，`NativeTabularSink` 直接复用最快的原生 writer（`src/daft-logical-plan/src/sink_info.rs:17-23`） |
| `DataSink` / `write_sink` | **Python 写扩展点**：`SinkSpec` 的一种；第三方自定义 sink 仍可直接 `write_sink`（零迁移） |
| `Catalog` / `Table` | **命名层**：由 provider 提供，且只允许转发；SQL / `daft.read_table` / Session 全部不变 |
| 数据湖的 Python `DataSource`（Iceberg/Delta/Hudi/Paimon/Lance） | **LAKE provider**：保留其元数据逻辑，文件读取委托给 `FORMAT` provider |
| `daft/io/__init__.py` 的手写 import | 逐步改为"内置 provider 注册表 + 兼容别名"，`read_*` 名字全部保留 |

一句话总结这套设计：**把"存储 × 格式 × 命名"三个正交轴用 `Provider` 组装成后端，把"能力"和"类型映射"从实现里提到声明里，把"统一"做成加法而不是替换。**

**v2 之后的完整心智模型**（推荐放在文档首页给用户看）：

```
读：  URI ─► STORAGE(scheme) ─► FORMAT(显式/扩展名) ─► scan() ─► DataFrame
     名字 ─► TABLE(catalog) ──────────────────────────► Table.read() ─► DataFrame
写：  df ─► SinkSpec ─┬─ NativeTabularSink → write_tabular → SinkInfo::OutputFileInfo（Parquet/CSV/JSON/Avro）
                     ├─ CatalogSink       → SinkInfo::CatalogInfo（Iceberg/Delta/Paimon/Lance）
                     └─ PythonDataSink    → write_sink → SinkInfo::DataSinkInfo（ClickHouse/Bigtable/…）
```

---

## 附：能力字段最小集（建议 v1 冻结这些）

```
轴与身份：kind(STORAGE|FORMAT|LAKE|TABLE|VIRTUAL), keys, requires
方向：read, write, named_tables
读侧：pushdown_filters, pushdown_projection, pushdown_limit, statistics, count_pushdown, partitioned_scan
写侧：sink_kinds(native_tabular|catalog|python_datasink), distributed_write, bulk_columnar_insert,
      server_side_settings, write_modes
类型：type_mapping（三态 native/serialize/reject）
```

共 18 个字段（v2 相比 v1 增加 `kind`/`keys`/`sink_kinds`，用于表达双轴与三种写形态）。
任何新增字段都需要在 PR 里回答："它会让用户的哪一次决策或哪一条错误信息发生变化？"

---

## 变更记录

| 版本 | 变更 |
|---|---|
| v1 | 初版：Provider/Capabilities/TypeMapping/Handle + 单一 scheme 注册表；`sink()` 返回 `DataSink` |
| **v2** | 修正为**双轴模型**（STORAGE × FORMAT + LAKE/TABLE/VIRTUAL），新增 `SinkSpec` 三种写形态、内置后端能力矩阵（§7.6）、能力 meet 组合规则、URI 解析规则；P1 改为"用内置文件后端验证双轴模型" |
