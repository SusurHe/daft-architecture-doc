# Issue 草稿：统一存储接入口（Provider 契约 + 能力协商 + 类型映射）

> 用途：按 `CONTRIBUTING.md:34`（非平凡 PR 必须关联一个已被 maintainer 批准的 issue），先以此文开 issue 拿 approve，再提 PR。
> 建议标题：**RFC: unified provider contract for connectors (capabilities, type mapping, negotiation)**
> 建议标签：`Data Sources & Sinks`、`enhancement`

---

## 背景

Daft 目前有三套彼此独立的扩展点：读（`ScanOperator` / `DataSource`）、写（`DataSink` / `write_sink`）、命名（`Catalog` / `Table`）。同一个后端要分别写三处代码，而"能力"没有统一声明位，导致若干可复现的问题。

## 问题（均附代码位置）

1. **写侧没有能力声明位**：`DataSink.schema()` 是 `finalize()` 输出统计的 schema，不是数据 schema（`daft/io/sink.py:53-58`）。于是 `SQLDataSink` 用私有方法检测非原生列（`daft/io/_sql.py:177-192`），而 `ClickHouseDataSink` 直接 `to_pandas()` + `insert_df`（`daft/io/clickhouse/clickhouse_data_sink.py:61-68`）——含 image/tensor 列时行为未定义且未文档化。
2. **声明与实现会漂移**：`GlobScanOperator` 的 `can_absorb_filter/select/limit/shard` 全部返回 `false`（`src/daft-scan/src/glob.rs:599-611`），但 Parquet 的谓词/列下推实际生效（走 `Pushdowns` 通道）；只有 limit 真正检查了 `can_absorb_limit()`（`push_down_limit.rs:134`）。声明不是事实来源。
3. **同一后端两套实现、性能语义不同**：`PostgresCatalog.append` 自建 psycopg `COPY ... BINARY`，源码注明 **单节点串行**并挂 TODO 待换 `write_sql`（`daft/catalog/__postgres.py:660-690`）；而 `df.write_sql` 走按 micropartition 并行的 `to_sql`（`daft/io/_sql.py:325-364`）。对照 Iceberg catalog 是纯转发（`daft/catalog/__iceberg.py:286-305`），因此没有漂移——说明问题出在"catalog 是否允许内置第二套实现"。
4. **选项拼写错误被静默吞掉**：入口普遍是 `**options`。
5. **第三方无法注册后端**：`daft/io/__init__.py:18-40` 逐个硬编码 import，仓库内无 entry point 发现机制。

## 提案（分阶段、零破坏）

新增 `daft/storage/` 契约层，**不动现有入口**：

1. **四层模型**：L0 filesystem（scheme）/ L1 file format / L2 table format / L3 catalog 相互独立；`TableRef` 增加可选 `location` 与 `location_source`（USER / CATALOG / NONE），表达"表需要 location"与"用户需要指定 location"的区别（Iceberg 的 `StaticTable.from_metadata` vs pyiceberg `load_catalog` 两种接入正好对应）。
2. **两层能力**：粗粒度 `TableCapability` + 细粒度 `Supports*` mixin（借鉴 Spark DataSource V2）。未实现 mixin 即不支持，无需维护第二份事实。
3. **协商返回残余**：`push_filters(...) -> Residual(accepted, remaining)`，引擎重算残余算子。这只是把 Daft 内部既有的 `SupportsPushdownFilters::push_filters -> (pushable, remaining)`（`src/daft-scan/src/pushdowns.rs:10-13`）推广到全部下推类型。
4. **写形态显式化**：`SinkSpec = NativeTabularSink | CatalogSink | PythonDataSink`，分别对应 `SinkInfo::{OutputFileInfo, CatalogInfo, DataSinkInfo}`（`src/daft-logical-plan/src/sink_info.rs:17-23`），并引入 `WriteProtocol` 声明提交语义（借鉴 Flink `SinkV2` 的 writer/committer 拆分）。
5. **选项契约**：`required_options` / `optional_options` / `forward_options`（借鉴 Flink table factory），未知选项报错并给拼写建议。
6. **`V1_FALLBACK`**：老 `DataSource`/`ScanOperator`/`DataSink` 永久可用，可分批迁移。
7. **自省**：`describe_uri()`、`handle.describe()`、`dtype_matrix()`、`list_providers()`。

## 现状（已实现的 P0）

分支 `SusurHe/Daft:feat/unified-storage-provider-api`（基线 `dadd8a0b2`）：新增 `daft/storage/` 9 个模块 + `tests/storage/` 6 个文件（50 用例通过，`ruff check` / `ruff format --check` 全绿），并导出 `daft.open` / `daft.storage`。**未修改任何现有入口**。

## 希望 maintainer 确认的点

1. 是否认可"**catalog 的 `Table.read/append/overwrite` 只允许转发到同一个 provider 的 scan/sink**"作为强制规约（并用一致性测试校验），以消除 Postgres 那类双实现漂移？
2. 能力协商改为"返回残余"后，`can_absorb_*` 是保留为优化器早停提示，还是直接废弃？
3. `SinkSpec` 三形态与 `WriteProtocol` 的引入是否可以作为 P1 的目标形态（涉及 `write_parquet/csv/json/avro` 与 `write_sql` 的内部改写，行为逐字等价）？
4. 是否有偏好：`daft.open()` 命名（会遮蔽内置名，仅限 `daft` 命名空间）还是 `daft.open_uri()`？

## 与我方相关的上游讨论

- #7347（ADBC 统一数据库读写）：本提案可承载其"引擎选择应由后端声明而非 URL 解析"的部分；
- #7260 / #7292（MongoDB 读连接器）：可优先作为 provider 试点；
- #5979（`write_sql` + `SQLDataSink`）：写侧统一的前置工作。
