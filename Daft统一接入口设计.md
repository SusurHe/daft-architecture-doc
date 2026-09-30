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

> **v3 边界说明**：本节描述的 `STORAGE × FORMAT` 是**文件 / 数据湖族**的解析路径，**不适用于 DB 类后端**
> （ClickHouse/Postgres 等既无 scheme 也无文件格式，数据文件由数据库自己管理）。完整的四层模型与"location 只在需要时引入"的规则见 §3.7。

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

### 3.7 四层模型（v3）：FS / 文件格式 / 表格式 / Catalog，location 只在需要时引入

v2 把"存储 × 格式"说成了通用解析流水线，这会把 DB 类后端错误地也套进文件体系。正确的划分是**四个相互独立的层**，
依赖关系**只在"需要 location"的那一支向上拉**（与 Spark / Flink 的分层一致）：

```
L3  Catalog（命名解析）        catalog > database/schema > table
      ├── 纯注册表：无 location、无连接（Daft 现状：TableSource::View(LogicalPlan)，table.rs:14-21）
      ├── DB-backed：只持连接串；数据文件由数据库自己管理（Postgres/ClickHouse/MongoDB…）
      └── Location-backed：持 warehouse / LOCATION 根 → **需要 L0/L1/L2**（Hive/Iceberg/Paimon/Glue/Unity/Tables）
L2  Table format（表协议）     快照 · 清单 · 分区规范 · 提交协议（Iceberg/Delta/Hudi/Paimon/Lance）
L1  File format（字节编码）    Parquet / CSV / JSON / Avro / Text / MCAP / WARC
L0  Filesystem（字节存取）     scheme → FS 实现（local / s3 / **oss** / hdfs / gcs / az / http / hf + OpenDAL）
```

**关键结论：`location` 是"可选字段"，不是"必经之路"。**

- DB-backed 表：解析在 L3 就结束，**不涉及 L0/L1/L2**；写数据也交给数据库自己的存储引擎（事务、WAL、compaction 都是它的事），
  Daft 只负责"把 Arrow 批次交出去 + 处理提交语义"——这正是 `SinkSpec::PythonDataSink` / `CatalogSink` 的职责；
- Location-backed 表（Hive / 数据湖）：L3 解析出的表**携带 location + file format + table protocol**，于是才会向下拉 L2→L1→L0；
- 路径式访问（`read_parquet("s3://…")`）：没有 L3，直接 L0→L1。

#### 3.7.1 但"需要 location" ≠ "用户需要指定 location"（v3 补充）

Iceberg 是最典型的边界案例：**它必然有 location**（目录式表：数据文件 + `metadata.json` + manifest 都在文件系统上），
所以 L0/L1/L2 必然参与；但**谁来指定这个路径**是三态可变的，Daft 两种接入都已实现：

| location 来源 | 典型场景 | 用户是否给路径 | Daft 现状证据 |
|---|---|---|---|
| `LOCATION_USER` | 直接读 `metadata.json`（Hadoop 表）、`write_parquet("s3://…")`、`CREATE TABLE … LOCATION` | **是** | `StaticTable.from_metadata(metadata_location=...)`（`daft/io/iceberg/_iceberg.py:186`） |
| `LOCATION_CATALOG` | HMS / Glue / Unity / REST / SQL catalog 里的 Iceberg/Paimon/Hive 表 | **否**（catalog 或 warehouse 配置给） | `IcebergCatalog._load_catalog(name, **options)` → pyiceberg `load_catalog`，选项全部透传（`daft/catalog/__iceberg.py:64-67`）；写侧 `write_iceberg(table: pyiceberg.table.Table, …)` 收的是**表对象**，路径已被解析（`dataframe.py:1403-1412`） |
| `LOCATION_NONE` | Postgres / ClickHouse / MongoDB、纯注册表（`TableSource::View`） | 不适用 | `PostgresCatalog` 只持连接串（`__postgres.py:38`） |

**还有第二层来源：凭据。** Iceberg REST 的 `loadTable` 会在响应中下发文件系统配置（如 S3 凭据、endpoint），
因此即使 location 由 catalog 给出，**FS 凭据也可能由 catalog 下发**。Daft 的语义已经写死在文档字符串里：

> `read_iceberg(..., io_config=...)`：*"If provided, configurations set in `table` are ignored."*（`daft/io/iceberg/_iceberg.py:151`）

即 **显式 `IOConfig` 覆盖表/catalog 下发的配置**。设计里必须把这条优先级固定下来，否则会出现"目录下发的临时凭据"与"本地静态凭据"互相打架：

```
凭据解析优先级：显式 IOConfig  >  catalog / 表下发（credential vending）  >  环境与默认链
```

**修正后的判据（取代上一节的粗粒度说法）：**

> 1. **"后端是否需要 location"** 决定层链长度 —— DB 类在 L3 终止，文件/数据湖类向下拉 L0/L1/L2；
> 2. **"location 由谁给"** 决定配置来源 —— 用户参数 vs catalog 元数据 vs warehouse 配置；
> 3. 两者**不可混为一谈**：Iceberg 属于"必须有 location、但通常不由用户指定"。
>
> 相应地，`TableRef.location` 应当是"**解析后的结果**"，并额外带上 `location_source: LOCATION_USER | LOCATION_CATALOG | LOCATION_NONE`，
> 这样 `handle.explain()` 能明确回答"这次读的路径是谁给的、凭据从哪来"。


**Daft 现状已经部分符合这个分层，不需要新造轮子：**

| 层 | 现状承载者 | 证据 | 缺口 |
|---|---|---|---|
| L0 Filesystem | `IOConfig` 按 scheme 分区 + `opendal_backends: BTreeMap<scheme, kv>` + **`protocol_aliases`（自定义 scheme → 已有 scheme）** | `src/common/io-config/src/config.rs:12-32`；`SourceType` 9 种（`src/daft-io/src/lib.rs:517-527`） | scheme 能力未声明化（range/multipart/并发上限只在实现里）；第三方加 FS 需改 Rust 枚举 |
| L1 File format | `FileFormatConfig::{Parquet, Csv, Json, Warc, Text, Avro, Mcap}` | `src/daft-scan/src/file_format_config.rs:21-29` | 能力未声明化（§3.6 末尾的 `can_absorb_*` 不一致问题） |
| L2 Table format | Iceberg/Delta/Hudi/Paimon/Lance 的 Python `DataSource` + `daft-writers` | `daft/io/iceberg/_iceberg.py:183-200` | 未声明"我需要 FS+FileFormat"这一依赖 |
| L3 Catalog | `Catalog`/`Table`；`IcebergCatalog._inner`（有 warehouse）、`PostgresCatalog`（**只有连接串**） | `daft/catalog/__iceberg.py:49-66`、`__postgres.py:38` | 未声明属于哪一族（location-backed / DB-backed / 纯注册表） |

**落到接口上的三处修改（相对 v2）**

1. **`TableRef` 增加可选字段**，让"要不要 FS"由数据决定而不是由调用方猜：

```python
@dataclass(frozen=True)
class TableRef:
    schema: Schema
    location: Location | None = None        # 仅 location-backed 表非空（fs scheme + path + io_config）
    file_format: str | None = None          # "parquet" / "csv" / …
    table_protocol: str | None = None       # "iceberg" / "delta" / "hive" / None
    scan: ScanSpec | None = None            # DB-backed 直接给 DB 扫描（无 location/格式）
    # 层链：谁参与了这次解析，便于 explain() 与错误信息
    layers: tuple[Literal["catalog","table_format","file_format","filesystem"], ...] = ()
```

2. **能力不再是"通用 meet"，而是"只对参与该表的层求 meet"**：

| 表 | 参与的层 | 有效能力 |
|---|---|---|
| ClickHouse 表 | catalog(DB) | 由 DB 驱动声明（并发写、事务、无文件语义） |
| Hive/Iceberg 表 | catalog(warehouse) → L2 → L1 → L0 | 快照/分区裁剪 ∩ Parquet 下推 ∩ S3 range GET |
| `s3://b/x.parquet` | L1 → L0 | Parquet 能力 ∩ S3 传输能力 |
| 内存表（`TableSource::View`） | catalog(纯注册表) | 无 IO 能力，只有命名 |

3. **写路径按"谁拥有文件"分派**（对应你说的"DB 交给数据库自己写入文件"）：

| 场景 | 谁写文件 | `SinkSpec` 形态 | 需要 location 吗 |
|---|---|---|---|
| `write_parquet("s3://…")` | Daft（`daft-writers`） | `NativeTabularSink` | **需要**（显式给出） |
| Hive / 数据湖表写入 | Daft 写文件 + 元数据提交 | `CatalogSink` | **需要**（由 catalog 的 warehouse/LOCATION 提供） |
| `write_clickhouse(...)` / `write_sql(...)` | **数据库**（Daft 只交数据） | `PythonDataSink` | **不需要**，且不应暴露格式/路径参数 |
| `write_sink(my_sink)` | 用户自定义 | `PythonDataSink` | 由 sink 自己决定 |

**这也是一个 UX 判据**：如果一个 API 让用户为 ClickHouse 指定"格式/路径"，那就是分层泄漏；
反过来，写 Iceberg 时只给表名不给仓库位置，才应该由 catalog 补上。

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

**按 v3 的四层模型补齐"层链"列**（谁参与决定了需要哪些能力声明）：

| 后端族 | 层链 | 需要的声明 |
|---|---|---|
| 本地/对象存储（local/s3/oss/hdfs/gcs/az/http/hf） | **L0** | scheme 能力：range GET、multipart、并发上限、重试、protocol_aliases |
| 文件格式（parquet/csv/json/avro/text/mcap/warc） | **L1 → L0** | 下推能力、统计、切分粒度 |
| 数据湖（iceberg/delta/hudi/paimon/lance） | **L2 → L1 → L0** | 快照/时间旅行、分区裁剪、提交协议、schema 演进 |
| Hive / 元数据服务（glue/unity/hms） | **L3(location-backed) → L2? → L1 → L0** | 命名空间、warehouse/LOCATION 解析、凭据下发 |
| DB（postgres/clickhouse/mongodb…） | **L3(DB-backed)** | 连接、事务/写入模式、类型映射；**无 FS、无格式** |
| 内存/生成器（`TableSource::View`、generator、range） | **L3(纯注册表)** | 只有命名与 schema |

> 一句话判据：**一条后端链上"是否出现 L0/L1"由数据源本身决定，不由 API 决定。**
> 出现"给 DB 指定文件格式"或"给 Iceberg 指定 parquet 压缩"这类要求时，先检查是不是分层泄漏。

**迁移三步（内置后端）**

| 步 | 动作 | 风险 |
|---|---|---|
| 1 | 把 `get_tabular_files_scan` 的两个参数抽成 `(storage_provider, format_provider)` 两个对象，但**行为不变** | 低（纯重构，`tests/io/**` 覆盖充分） |
| 2 | 让 `read_*` / `write_*` 走 `daft.open(uri, format=...)`，`SinkSpec` 分别产出 `NativeTabularSink` | 低（计划形状与现在逐字一致） |
| 3 | 数据湖 provider 复用 FORMAT provider（Iceberg 的文件读取调 parquet 的 scan），删掉重复解析 | 中（需要对照 pyiceberg 的行为） |

## 8. 分阶段落地（每阶段可独立合并、可回滚）

| 阶段 | 内容 | 破坏性 | 测试 |
|---|---|---|---|
| **P0**（~1 周） | 新增 `daft/storage/`（Provider/Capabilities/TypeMapping/SinkSpec/Registry/Handle）+ `daft.open()` + `list_providers()`；注册 **STORAGE 与 FORMAT 两类内置 provider 的声明**（含 §7.6 能力矩阵），**不改任何老入口实现**。**v4 扩容**：把协商协议（`Residual`）、选项契约、`V1_FALLBACK` 标记一并定型（见 §11.11） | 无（纯新增） | 单测：双轴解析、扩展名推断与"无法推断则报错"、注册冲突、`daft.open` 解析、**未知选项报错与拼写建议**、协商残余语义 |
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

**v3 之后的完整心智模型**（推荐放在文档首页给用户看）：

```
L3 Catalog（命名）  catalog > schema > table
     ├─ location-backed（Hive/Iceberg/Paimon/Glue/Unity）──► 需要 L0/L1/L2
     ├─ DB-backed（Postgres/ClickHouse/MongoDB）───────────► 到此结束：数据文件由 DB 自己管
     └─ 纯注册表（View/内存）───────────────────────────────► 到此结束：只有命名与 schema

路径式访问（无 L3）：
读：  URI ─► L0 Filesystem(scheme) ─► L1 FileFormat(显式/扩展名) ─► scan() ─► DataFrame
写：  URI ─► L0 + L1 ─► NativeTabularSink ─► write_tabular ─► SinkInfo::OutputFileInfo

写完分派（谁拥有文件）：
写：  df ─► SinkSpec ─┬─ NativeTabularSink（Daft 写文件，需 location）
                     ├─ CatalogSink       （Daft 写文件 + 表协议提交，location 来自 catalog）
                     └─ PythonDataSink    （DB 自己写文件，Daft 只交数据；无 location/格式参数）
```

---

## 11. 对照 Spark DataSource V2 与 Flink Connector：评估与吸收（v4）

### 11.0 评估速览：v3.1 的缺口，以及谁已经解决过

| v3.1 的做法 | 缺口 | 工业界解法 | 是否吸收 |
|---|---|---|---|
| 单个 `Capabilities` dataclass 同时承担"规划信号"和"协商结果" | 语义混淆：静态声明无法表达"这次实际推下去多少" | Spark：粗粒度 `Table.capabilities()` + 细粒度 `Supports*` mixin 两层 | ✅ §11.1 |
| `pushdown_filters: bool` 等布尔声明 | 声明与实现容易不一致（Daft 现状：`can_absorb_*` 全 false 却仍能下推） | Spark `pushFilters` 返回未处理 filter；Flink `FilterPushdownResult(accepted, remaining)` | ✅ §11.2（**Daft 内部已有同款**） |
| Provider 直接产出 scan/sink | 元数据与协商状态混在一起 | Spark：`Table` → `ScanBuilder` → `Scan` → `Batch` 四段 | ✅ §11.3 |
| `DataSink.start/write/finalize` | **没有 commit 阶段**——只有并行写 + driver 聚合 | Flink SinkV2：`SinkWriter` / `Committer` / `GlobalCommitter` 三段式 | ✅ §11.4（最大借鉴项） |
| `**options` 全收 | 拼写错误被静默吞掉（`write_clickhouse(hostt=…)` 今天不报错） | Flink：`requiredOptions/optionalOptions/forwardOptions` + 未知选项报错 | ✅ §11.5 |
| "所有后端都要迁到 provider" | 迁移成本高、风险大 | Spark：`TableCapability.V1_FALLBACK` 显式共存 | ✅ §11.6 |
| `capabilities.statistics: bool` | 只有"能不能"，没有"报什么" | Spark `SupportsReportStatistics`/`SupportsReportPartitioning`；Flink `SupportsStatisticReport` | ✅ §11.7 |
| 未涉及元数据列/hive 分区列 | Daft 已有 `generated_fields`/`file_path_column`，但未契约化 | Spark `SupportsMetadataColumns`；Flink `SupportsReadingMetadata` | ✅ §11.8 |
| 零散异常 | 用户不知道怎么绕过 | Spark `unsupportedOperation` + 明确列出替代操作 | ✅ §11.9 |

### 11.1 两层能力模型（吸收 Spark 的 `TableCapability` + `Supports*` mixin）

问题：v3.1 用同一个 `Capabilities` 对象回答两个不同的问题——"**能不能规划这类查询**"（规划期、粗粒度、决定快速失败）与"**这次实际推下去多少**"（协商期、细粒度、返回残余）。Spark 用两层解决：

```python
class TableCapability(Enum):        # 粗粒度：规划信号 + 快速失败
    BATCH_READ; BATCH_WRITE; STREAMING_READ; STREAMING_WRITE
    ACCEPT_ANY_SCHEMA; TRUNCATE; V1_FALLBACK

# 细粒度：能力 mixin，按需实现（未实现即不支持，不靠布尔声明）
class SupportsPushdownFilters(Protocol):   def push_filters(self, f: list[Expr]) -> Residual[Expr]: ...
class SupportsPushdownProjection(Protocol):def push_projection(self, cols: list[str]) -> None: ...
class SupportsPushdownLimit(Protocol):     def push_limit(self, n: int) -> bool: ...
class SupportsPushdownAggregates(Protocol):def push_aggregation(self, agg: Expr) -> bool: ...
class SupportsReportStatistics(Protocol):  def report_statistics(self) -> TableStatistics | None: ...
class SupportsReportPartitioning(Protocol):def report_partitioning(self) -> Partitioning | None: ...
class SupportsMetadataColumns(Protocol):   def metadata_columns(self) -> list[MetadataColumn]: ...
```

**规则**：`Capabilities` 只保留"决策与报错需要的少数几个字段"（§附录），其余全部改为 mixin；**未实现某个 mixin = 不支持**，`isinstance(provider, SupportsX)` 就是能力判定，不需要维护两份事实。

### 11.2 协商协议：返回**残余**（吸收 Spark / Flink，并推广 Daft 已有做法）

Spark 的 `ScanBuilder` 文档明确规定了下推顺序：**sample → filter → aggregate → limit/topN → offset → 列裁剪**；每个 `SupportsPushDownX` 负责"接受一部分、把剩下的还回去"。Flink 同构：`applyFilters(...) -> FilterPushdownResult(accepted, remaining)`。

**关键发现：Daft 内部早就是这个协议**，只是只用于 filter：

```rust
// src/daft-scan/src/pushdowns.rs:10-13
pub trait SupportsPushdownFilters {
    /// Applies filters to the scan operator and returns the pushable filters and the remaining filters.
    fn push_filters(&self, filter: &[ExprRef]) -> (Vec<ExprRef>, Vec<ExprRef>);
}
```

配合 `PredicateGroups` 的三路拆分（`src/daft-scan/src/expr_rewriter.rs:56-67`：`partition_only_filter` / `data_only_filter` / `needing_filter_op`），以及 `ScanOperator::as_pushdown_filter()`（`src/daft-scan/src/scan_operator.rs:67-69`）这个暴露口。

**v4 吸收方式**：把这一套从"filter 专有"提升为**所有下推的统一契约**，并规定协商顺序：

```python
@dataclass
class Residual(Generic[T]):
    accepted: list[T]      # 已由后端承担
    remaining: list[T]     # 必须由上层重新求值（正确性由引擎兜底）

class ScanBuilder(Protocol):
    def push_sample(self, spec) -> Residual: ...
    def push_filters(self, filters: list[Expr]) -> Residual[Expr]: ...
    def push_aggregation(self, aggs: list[Expr]) -> Residual[Expr]: ...
    def push_limit(self, n: int, offset: int) -> Residual: ...
    def prune_columns(self, required: list[str]) -> None: ...
    def build(self) -> "Scan": ...
```

`can_absorb_*` 退化为"**预筛选提示**"（优化器可据此早停），而**事实来源是协商返回的残余**。这一条直接消灭 §3.6 末尾那个"声明 false 却能下推"的不一致——因为声明不再是事实来源。

### 11.3 四段式构建：TableRef → ScanBuilder → Scan → Batch（吸收 Spark）

Spark 的分段（`Table` 不可变元数据 → `newScanBuilder(options)` 可变协商 → `build()` → `Scan.toBatch()` → `Batch.planInputPartitions()` + `createReaderFactory()`）解决两个问题：元数据可缓存/可序列化不被协商污染；同一张表可以被多次以不同列集合扫描。

v4 对齐：

| 段 | 职责 | Daft 现有映射 |
|---|---|---|
| `TableRef` | 不可变元数据：schema / location / file_format / table_protocol / layers / location_source | 新增（v3 已定义字段） |
| `ScanBuilder` | 协商下推、列裁剪、切分策略（scan task 大小） | 现散落在 `PushDownFilter/Projection/Limit` + `ScanOperator::to_scan_tasks` |
| `Scan` | 物理计划片段：要读哪些文件/行组、谓词、列 | `Pushdowns` + `ScanTask`（`src/daft-scan/src/lib.rs`） |
| `Batch` | 每分区读取器 | `ScanTaskSource` + `read_scan_task` |

### 11.4 写侧三段式提交协议（**Flink SinkV2 的最大借鉴**）

Flink 把"写"拆成三个角色：`SinkWriter`（每个并行子任务写，产出 `Committable`）→ `Committer`（每个子任务在 checkpoint 时提交）→ `GlobalCommitter`（全局合并与提交）。Daft 现状只有：

```python
class DataSink:                     # daft/io/sink.py:31-75
    def start(self) -> None: ...     # driver
    def write(self, mps) -> Iterator[WriteResult]: ...   # worker 并行
    def finalize(self, results) -> MicroPartition: ...   # driver 聚合统计
```

**缺的正是中间那一段**：没有"提交"这个独立阶段。这解释了 Postgres catalog 为什么退化成单节点串行 `COPY`（`__postgres.py:660-690`）——因为没有 committer 抽象，驱动端只能自己串行写。

v4 引入：

```python
class WriteProtocol(Enum):
    APPEND_ONLY       # 失败后重试可能重复（要求用户接受或后端幂等）
    ATOMIC_COMMIT     # 有原子提交点（文件成功标记 / 快照提交）
    TWO_PHASE         # 支持 prepare/commit/abort
    IDEMPOTENT_UPSERT # 以主键去重

class DataWriter(Protocol):          # worker 侧，可并行
    def write(self, mp: MicroPartition) -> None: ...
    def prepare_commit(self) -> Committable: ...
    def abort(self) -> None: ...

class Committer(Protocol):           # 每个 task / 分区
    def commit(self, cs: list[Committable]) -> list[Committable]: ...

class GlobalCommitter(Protocol):     # driver 侧
    def combine(self, cs: list[Committable]) -> list[Committable]: ...
    def commit(self, cs: list[Committable]) -> None: ...
    def abort(self, cs: list[Committable]) -> None: ...
```

**与 Daft 既有写路径的对应关系**（不需要新引擎，只是把隐含语义显式化）：

| 现有写路径 | 隐含协议 | 显式化后应声明 |
|---|---|---|
| `write_parquet`（`SinkInfo::OutputFileInfo`） | 文件写完 + `_SUCCESS` 标记 | `ATOMIC_COMMIT` |
| Iceberg / Delta / Paimon（`SinkInfo::CatalogInfo`） | 快照/事务日志提交 | `ATOMIC_COMMIT`（或 `TWO_PHASE` 若支持） |
| ClickHouse（`insert_df`） | 直接追加，无事务 | `APPEND_ONLY`（若带 token 则 `IDEMPOTENT_UPSERT`） |
| `write_sql`（SQLAlchemy `to_sql`） | 每 micropartition 一次 `commit()` | `APPEND_ONLY` |
| Postgres catalog 的 `COPY`（现状） | 单节点串行 | **应被替换**：改为 writer+committer 两段 |

声明 `WriteProtocol` 的收益是**自动生成正确行为**：`APPEND_ONLY` 的重试语义必须提示用户可能重复；`ATOMIC_COMMIT` 失败后必须 abort 清理；`TWO_PHASE` 才允许在 distributed 上做 exactly-once 承诺。

### 11.5 工厂与选项契约（吸收 Flink，顺带修掉一个真实 UX bug）

Flink 的 `DynamicTableFactory` 要求工厂声明 `factoryIdentifier()` / `requiredOptions()` / `optionalOptions()` / `forwardOptions()`，未知选项报错；并区分"影响拓扑的选项"与"可恢复时覆盖的选项（enrichment）"。

Daft 现状：`**options` 全收，`write_clickhouse(hostt="…")` 这类拼写错误**静默失效**——这是今天就能修的实际问题。

v4 吸收（并做减法：不引入 Flink 的 enrichment 概念，只保留 `forward_options`）：

```python
class Provider(Protocol):
    @classmethod
    def required_options(cls) -> list[Option]: ...     # 缺失即报错
    @classmethod
    def optional_options(cls) -> list[Option]: ...     # 声明即校验类型/枚举
    @classmethod
    def forward_options(cls) -> list[Option]: ...      # 透传给底层客户端，不做校验
```

未知选项的报错要带**拼写建议**与**合法选项清单**（`did you mean host?`），错误文本由引擎统一生成。

### 11.6 V1 兜底：不强制全量迁移（吸收 Spark `V1_FALLBACK`）

```python
class ApiLevel(Enum): V2 = "v2"; V1_FALLBACK = "v1"
```

`DataSource` / `ScanOperator` / `DataSink` 作为 **V1 路径永久可用**；providers 可声明自己是 `V2`（走新协商协议）或 `V1_FALLBACK`（引擎自动降级到老路径并记录一条 plan hint）。这条让第三方与内置大后端可以**分批迁移**，也让"零破坏"从口号变成机制。

### 11.7 统计与分区上报（吸收 Spark/Flink）

```python
class SupportsReportStatistics(Protocol):
    def report_statistics(self) -> TableStatistics | None: ...   # rowCount / sizeInBytes / 列级区间
class SupportsReportPartitioning(Protocol):
    def report_partitioning(self) -> Partitioning | None: ...     # clustered(keys) / sorted(keys) / unknown
```

价值：`report_statistics()` 正好喂给 Daft 的 `ApproxStats`（`src/daft-logical-plan/src/stats.rs:105-111`），让 Join 策略与分区数决策拿到**来源侧**而非估算的数字；`report_partitioning()` 则直接服务 `can_skip_hash_repartition`（`pipeline_node/translate.rs:96-118`）——源数据已经按 join key 聚簇时跳过一次 shuffle。Daft 已有 `ScanOperator::statistics()`（`scan_operator.rs:51-57`），这一步是把它契约化并补上分区上报。

### 11.8 元数据列与生成列（吸收 Spark `SupportsMetadataColumns` / Flink `SupportsReadingMetadata`）

Daft 已有三样东西但没契约化：`ScanOperator::generated_fields()`、`file_path_column`、Hive 分区字段（`src/daft-scan/src/glob.rs:518-525`）。

```python
@dataclass
class MetadataColumn:
    name: str
    dtype: DataType
    readable: bool = True
    cost: Literal["free", "cheap", "expensive"] = "free"   # 文件路径列=free，行号列=expensive
```

`cost` 是 Daft 特有的补充：它让优化器知道"加一个元数据列"是否值得（Spark/Flink 没有这个概念，因为它们不做多模态与 IO 成本建模）。

### 11.9 错误分类（吸收 Spark `unsupportedOperation`）

```python
class UnsupportedOperationError(DaftError):
    op: str                                   # "overwrite" / "read_streaming" / "push_filter"
    provider: str
    reason: str
    alternatives: list[str]                   # 必须非空：可替代的 API 或操作路径
```

承接 §6.2 的"可行动报错"：错误对象化之后，CLI/Python/计划 hint 三处可以复用同一份替代建议。

### 11.10 明确**不吸收**的部分（避免盲目照抄）

| 机制 | 不吸收的理由 |
|---|---|
| Spark DSv2 的表达式体系（`Filter`/`Expression`/`SupportsPushDownV2Filters`） | Daft 已有 `Expr` + Arrow 内核 + `PredicateGroups`；再引入一套 IR 只会多一层翻译与语义漂移 |
| Flink 的 changelog/upsert 流式表语义（`ChangelogMode`、`DynamicTableSink` upsert） | Daft 的流式能力是 Kafka source 级的，不是变更日志表模型；引入会牵动整个执行层与状态管理 |
| Flink 的 watermark / computed column pushdown | 与 Daft 定位（批式多模态 ETL + AI 推理）无关 |
| Spark 的 `StagedTable`（两阶段 DDL） | Daft 的 catalog DDL 面仍小（`CREATE TABLE` 等），收益低于复杂度 |
| Flink 的 enrichment options（作业恢复时覆盖选项） | Daft 无长跑作业恢复语义；`forward_options` 已覆盖"不影响能力的透传"这一需求 |
| Spark 的 `SupportsPushDownVariantExtractions` 等新算子级下推 | 交给 Daft 优化器规则处理（`push_down_*` 系列），不进入 provider 契约 |

### 11.11 对落地阶段的影响（P0 扩容）

**协商协议、选项契约、V1 标记必须在 P0 定型**——它们是 API 形状，晚改的代价远大于实现成本。更新后的 P0：

| P0 交付物 | 说明 |
|---|---|
| `daft/storage/` 骨架 | `TableCapability`（粗）+ `Supports*` mixin（细）+ `Residual` |
| `ScanBuilder` / `Scan` / `Batch` 三段接口 | 先只接内置 parquet/csv（V1_FALLBACK 兜底其余后端） |
| `WriteProtocol` + `DataWriter/Committer/GlobalCommitter` | 先只声明不启用（内置 writer 标注 `ATOMIC_COMMIT`） |
| 选项契约 | `required/optional/forward_options` + 未知选项报错（可独立合并，收益立刻可见） |
| `daft.open()` / `list_providers()` / `describe(uri)` | 自省入口 |
| V1 兜底 | `ApiLevel.V1_FALLBACK` + plan hint |

### 11.12 相对 v3.1 的净收益

1. **能力不再靠"声明+信任"**，而是靠"协商+返回残余"——声明与实现不可能再漂移；
2. **写路径补齐 commit 阶段**，直接给出 Postgres catalog 那类问题的结构性修法（而不是打补丁）；
3. **迁移风险显著下降**：V1 兜底让内置后端与第三方可以分批走；
4. **错误与选项质量提升**：未知选项报错 + 可行动替代建议，是用户立刻能感知的改进；
5. **优化器拿到更多真实信息**：统计与分区上报直接服务 Join 策略、shuffle 跳过与分区数决策。

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

## 12. 实现状态（v4.1）

> 跟踪方式：本节与仓库根目录的 [`PROGRESS.md`](PROGRESS.md) 同步维护；代码在 fork 分支
> `SusurHe/Daft:feat/unified-storage-provider-api`（基线 `dadd8a0b2`）。

| 设计条目 | 状态 | 实现位置 | 验证 |
|---|---|---|---|
| 四层模型（L0–L3）与 `location_source` | ✅ P0 | `daft/storage/contracts.py`（`Layer`/`Location`/`TableRef`/`LocationSource`） | `tests/storage/test_registry.py::test_resolve_uri_splits_storage_and_format_axes` |
| 双轴解析（STORAGE × FORMAT）+ 扩展名推断 | ✅ P0 | `registry.py:parse_uri/infer_format/resolve_uri` | `test_infer_format_uses_extensions`、`test_unknown_extension_raises_ambiguous_format_error` |
| 注册表 + 第三方入口点发现 + 依赖提示 | ✅ P0 | `registry.py:register/resolve/discover`、入口点组 `daft.storage.providers` | `test_registry_populates_itself_in_a_fresh_interpreter`、`test_known_dependency_hint_is_attached` |
| 两层能力模型（粗 `TableCapability` + 细 `Supports*` mixin） | ✅ P0 | `contracts.py`、`negotiation.py` | `test_scan_sources_only_advertise_what_they_do` |
| 协商返回残余（`Residual`） | ✅ P0 | `residual.py`、`negotiation.py:negotiate` | `test_negotiation_never_loses_a_filter` 等 6 例 |
| 三种写形态 `SinkSpec` | ✅ P0（声明 + 路由） | `contracts.py`、`handle.py:write_with_spec` | `test_sink_spec_uses_the_native_writer`、`test_native_sink_writes_through_the_handle` |
| `WriteProtocol` 提交语义 | ✅ P0（声明） | `contracts.py:WriteProtocol` | `test_write_protocol_semantics` |
| 选项契约 + 拼写建议 + 未知选项报错 | ✅ P0 | `options.py` | `test_options_contract.py`（7 例） |
| `V1_FALLBACK` 兜底 | ✅ P0 | `contracts.py:ApiLevel`、`providers.py:LegacyDataSourceProvider` | `test_v1_fallback_backend_skips_negotiation_and_records_a_hint` |
| 调试/自省入口 | ✅ P0 | `handle.py:describe/plan`、`registry.py:describe_registry`、`negotiation.py:describe_source`、`dtype_matrix()` | `test_describe_explains_layers_location_and_protocol` |
| `daft.open` 公开入口 | ✅ P1-a | `daft/__init__.py`（+3 行）、`daft/storage/handle.py:open_uri` | `tests/storage/test_public_api.py`（3 例） |
| 统计/分区上报、元数据列 | ⚠️ 部分 | mixin 已在 `negotiation.py` 定义；Parquet 源实现 `metadata_columns()` | `describe_source()`/`dtype_matrix()` 可观察；`report_statistics`/`report_partitioning` 待接入 |
| `DataWriter`/`Committer`/`GlobalCommitter` | ⚠️ 仅协议 | `contracts.py` | 未接入执行层（计划随 P3 的数据库 sink 一起落地） |
| 现有读入口改为双轴解析 | ✅ P1-b（试点 `read_parquet`） | `daft/storage/legacy.py`、`providers.py:legacy_file_format_config`、`daft/io/_parquet.py` | 计划与结果逐字节对拍 + `tests/storage/test_legacy_bridge.py` |
| 现有写入口改为 `handle.sink()` | ⏳ P1-c | — | 同上（建议同样先做 `write_parquet` 试点） |
| 数据湖 provider 复用 FORMAT provider | ⏳ P2 | — | — |
| ClickHouse provider（读 `read_sql` / 写既有 sink / `APPEND_ONLY`） | ✅ P3-a | `daft/io/clickhouse/provider.py`、`registry.py:resolve_uri`（DB 类在 L3 终止） | `tests/storage/test_clickhouse_provider.py`（16 例） |
| MongoDB provider、修 Postgres catalog 内联实现 | ⏳ P3-b | — | — |
| 一致性套件（capability 诚实性、选项契约、协商不丢项、类型映射自洽） | ✅ P0.5 | `daft/storage/conformance.py` | `tests/storage/test_conformance.py`（8 例） |
| catalog「单一实现原则」的机器校验 | ⏳ P3-b | — | 计划：在上述套件中断言 `Table.append` 期间调用了 `provider.sink` |

**当前可用能力**（P0 + P1-a 完成后）：

```python
import daft
from daft import col
from daft.storage import ScanRequest, describe_uri, dtype_matrix, list_providers

h = daft.open("/data/events.parquet")        # 或 daft.storage.open_uri(...)
h.provider_info.name                          # 'parquet'
h.write_protocol                              # WriteProtocol.ATOMIC_COMMIT
df = h.read(filters=[col("a") > 1], limit=10)
df.write_parquet ...                          # 或 h.write(df) / df.write_sink(h.sink())
print(h.describe())                           # 层链、location 来源、能力、上次协商结果
print(h.plan(ScanRequest(filters=[col("a") > 1], limit=10)).describe())
```

## 变更记录

| 版本 | 变更 |
|---|---|
| v1 | 初版：Provider/Capabilities/TypeMapping/Handle + 单一 scheme 注册表；`sink()` 返回 `DataSink` |
| **v2** | 修正为**双轴模型**（STORAGE × FORMAT + LAKE/TABLE/VIRTUAL），新增 `SinkSpec` 三种写形态、内置后端能力矩阵（§7.6）、能力 meet 组合规则、URI 解析规则；P1 改为"用内置文件后端验证双轴模型" |
| **v3** | 再把"双轴"收敛为**四层模型**（§3.7）：L0 Filesystem / L1 FileFormat / L2 TableFormat / L3 Catalog，四者**相互独立**；`location` 改为**可选字段**，只有 location-backed 的 catalog 才向下拉 L0/L1/L2；DB-backed 与纯注册表在 L3 终止；写路径按"谁拥有文件"分派（Daft 写 vs DB 写）；`TableRef` 增加 `location/file_format/table_protocol/layers` |
| **v3.1** | 补充 §3.7.1：区分"**表需要 location**"与"**用户需要指定 location**"——`location_source` 三态（USER / CATALOG / NONE），并用 Iceberg 的两种接入（`StaticTable.from_metadata` vs pyiceberg `load_catalog`）作为实证；新增**凭据来源优先级**（显式 IOConfig > catalog 下发 > 环境链），依据 `read_iceberg` 文档字符串 |
| **v4** | 对照 **Spark DataSource V2** 与 **Flink Connector** 做系统评估（§11）：吸收两层能力模型（`TableCapability` + `Supports*` mixin）、**协商返回残余**协议、`TableRef→ScanBuilder→Scan→Batch` 四段式、**写侧三段式提交协议**（`DataWriter/Committer/GlobalCommitter` + `WriteProtocol`）、工厂选项契约（`required/optional/forward_options` + 未知选项报错）、`V1_FALLBACK` 兜底、统计与分区上报、元数据列、错误分类；并明确列出**不吸收**的六项（DSv2 表达式体系、changelog 流式语义、watermark、StagedTable、enrichment options、算子级下推）；P0 扩容以容纳协商协议与选项契约 |
| **v4.4** | P1-b 试点落地：读入口双轴解析（`tabular_scan_configs` 桥接 + provider 拥有 reader 配置映射），用"固定数据目录 + 计划/结果逐字节对拍"证明行为不变；同时记录 Windows-GNU 本地构建的四处阻塞（含 `tikv-jemalloc-sys` 打包缺陷） |
| **v4.3** | P0.5 落地：一致性套件 `conformance.py`（身份/选项契约/能力诚实性/类型映射自洽/协商不丢项），内置 provider 全部通过 |
| **v4.2** | P3-a 落地：ClickHouse provider（`DATABASE` + `APPEND_ONLY` + 三态类型映射）、DB 类 URI 在 L3 终止（`resolve_uri` 不再要求格式轴）、`handle` 改为按协议选 provider；期间发现并修复 4 个缺陷（URI authority 解析、sink 连接参数、表名限定、依赖提示用例） |
| **v4.1** | 增加 §12「实现状态」：把设计条目映射到已实现的模块与测试（P0 完成、P1-a 完成，P1-b/c、P2、P3 待做），并链接 `PROGRESS.md` 与 fork 分支；代码侧新增 `daft/storage/` 契约层与 `daft.open` 导出 |
