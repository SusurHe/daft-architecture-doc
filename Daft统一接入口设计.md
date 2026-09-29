# Daft 统一接入口设计（零破坏版）

> 目标：在**不改变任何现有前端入口**（`daft.read_*`、`df.write_*`、`daft.read_table`、`write_sink`、catalog 注册）的前提下，
> 引入一层统一的、能力可声明、可校验、可扩展的后端接入协议。
>
> 基准：`Daft` 仓库 `main` @ `dadd8a0`（2026-09-25，约 v0.7.25）。文中所有现状描述均带 `文件:行号` 证据。

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
    name: str                                  # "clickhouse"
    schemes: tuple[str, ...]                   # ("clickhouse", "ch")
    capabilities: Capabilities
    type_mapping: TypeMapping

    # 三选一或多选，按 capabilities 声明
    def scan(self, uri: str, **options) -> "DataSource | ScanOperator": ...
    def sink(self, uri: str, **options) -> "DataSink": ...
    def catalog(self, uri: str, **options) -> "Catalog": ...
```

关键约定（写进 provider 基类文档 + 注册时校验）：

1. `sink()` **必须返回 `DataSink`**——写路径继续走既有的 `write_sink` 分发机制（`DataFrame.write_sink`，`daft/dataframe/dataframe.py:2393`），不新开写通道；
2. `scan()` 返回的 `DataSource` 最终经 `DataSource.read()` 变成 DataFrame（`daft/io/source.py:103-111`），因此读路径也不新开通道；
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
| `daft.read_parquet(path, io_config=...)` | `resolve_scheme(path).scan(path, io_config=...)` 包成 DataFrame | 签名与返回类型不变 |
| `daft.read_sql(sql, conn, partition_col=...)` | 由 URL scheme 解析出 provider（`clickhouse://` → ClickHouseProvider，`postgres://` → PostgresProvider…）；连接工厂场景回退到内置 `SQLProvider` | 方言/驱动推断逻辑从 `_should_use_connectorx` 迁到 provider 的 `options` 声明，行为保持一致 |
| `df.write_clickhouse(table=..., host=...)` | `resolve("clickhouse://").sink(uri, ...)` → `self.write_sink(sink)` | 参数逐字保留；旧参数与 URI 双通道合并 |
| `df.write_sql(table_name, conn, ...)` | 内置 `SQLProvider`（保留 `non_primitive_handling` 等既有语义） | 不变 |
| `df.write_sink(my_sink)` | **不变**（`DataSink` 仍是稳定扩展点，provider.sink 只是它的工厂） | 第三方自定义 sink 零迁移 |
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

## 8. 分阶段落地（每阶段可独立合并、可回滚）

| 阶段 | 内容 | 破坏性 | 测试 |
|---|---|---|---|
| **P0**（~1 周） | 新增 `daft/storage/`（Provider/Capabilities/TypeMapping/Registry/Handle）+ `daft.open()` + `list_providers()`；**只注册内置 provider 的声明，不改任何老入口实现** | 无（纯新增） | 单测：注册冲突、未注册 scheme 报错、`daft.open` 解析 |
| **P1**（~1–2 周） | 把 3–5 个老入口改为转发（`read_parquet`、`read_sql`、`write_clickhouse`、`write_parquet`、`write_sql`），实现逐字等价 | 无 | 复用现有 test suite（`tests/io/**`）作为等价性护栏 |
| **P2**（~2 周） | ClickHouse provider 完整落地（含 catalog 转发 + `read_clickhouse` 别名 + dtype 预检）；修 Postgres catalog 的内联实现（改走 `write_sql`） | 无 API 变更；**行为改善**（PG 写入从单节点串行变为可分布式） | conformance kit + 入口一致性断言 |
| **P3** | 扩展点收敛：新后端只接受 provider 形式；`write_*` 逐个标 `@deprecated`（保留 ≥2 个 minor）；docs 自动生成能力矩阵 | 无（弃用而非删除） | conformance kit 进 CI，未通过不允许合并 |

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
| `ScanOperator` / `DataSource` | **读能力的实现细节**：provider.scan() 返回它们；能力声明上提到 `Capabilities`，但 trait 本身不变 |
| `DataSink` / `write_sink` | **写能力的实现细节 + 稳定扩展点**：provider.sink() 是它的工厂；第三方自定义 sink 仍可直接 `write_sink` |
| `Catalog` / `Table` | **命名层**：由 provider 提供，且只允许转发；SQL / `daft.read_table` / Session 全部不变 |
| `daft/io/__init__.py` 的手写 import | 逐步改为"内置 provider 注册表 + 兼容别名"，`read_*` 名字全部保留 |

一句话总结这套设计：**把"读/写/命名"三个正交轴用 `Provider` 绑成一个后端，把"能力"和"类型映射"从实现里提到声明里，把"统一"做成加法而不是替换。**

---

## 附：能力字段最小集（建议 v1 冻结这些）

```
方向：read, write, named_tables
读侧：pushdown_filters, pushdown_projection, pushdown_limit, statistics, count_pushdown, partitioned_scan
写侧：distributed_write, bulk_columnar_insert, server_side_settings, write_modes
工程：requires
```

共 14 个字段。任何新增字段都需要在 PR 里回答："它会让用户的哪一次决策或哪一条错误信息发生变化？"
