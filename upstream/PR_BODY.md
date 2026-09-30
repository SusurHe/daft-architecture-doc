# PR: feat(storage) — unified provider entry point (P0)

> 用途：这是给上游 `Eventual-Inc/Daft` 提 PR 用的描述草稿。
> 注意 `CONTRIBUTING.md:34` 要求：**非平凡 PR 必须关联一个已被 maintainer 批准的 issue**，所以建议先按此文的 "Motivation" 开 issue 拿到 approve，再提本 PR。

---

## Motivation

Daft 目前有三套彼此独立的扩展点，同一个后端要分别写三处代码，且能力差异无处声明：

| 轴 | 现有抽象 | 位置 |
|---|---|---|
| 读 | `ScanOperator` / `DataSource` | `src/daft-scan/src/scan_operator.rs:14-70`、`daft/io/source.py:103-111` |
| 写 | `DataSink` / `write_sink` | `daft/io/sink.py:31-75` |
| 命名 | `Catalog` / `Table` | `src/daft-catalog/src/catalog.rs:12-42`、`daft/catalog/__init__.py:880-1108` |

由此产生的具体问题（均可在当前代码中复现）：

1. **写侧没有任何能力声明位**：`DataSink.schema()` 是 `finalize()` 输出统计的 schema，不是数据 schema；因此"这个 sink 接受哪些 dtype"只能各自为政——`SQLDataSink` 用私有方法检测（`daft/io/_sql.py:177-192`），`ClickHouseDataSink` 直接 `to_pandas()` + `insert_df` 静默失败（`daft/io/clickhouse/clickhouse_data_sink.py:61-68`）。
2. **声明与实现会漂移**：`GlobScanOperator` 的 `can_absorb_filter/select/limit/shard` 全部返回 `false`（`src/daft-scan/src/glob.rs:599-611`），但 Parquet 的谓词/列下推实际是生效的（走 `Pushdowns` 通道），只有 limit 真正检查了 `can_absorb_limit()`（`push_down_limit.rs:134`）。
3. **同一后端两套实现**：`PostgresCatalog.append` 自建 psycopg `COPY ... BINARY` 且源码注明单节点串行（`daft/catalog/__postgres.py:660-690`），而 `df.write_sql` 走可并行的 `to_sql`；Iceberg catalog 则是纯转发（`__iceberg.py:286-305`），因此没有漂移。
4. **选项拼写错误被静默吞掉**：入口普遍是 `**options`。
5. **第三方无法注册后端**：`daft/io/__init__.py:18-40` 逐个硬编码 import，仓库内无 entry point 发现机制。

## What this PR adds

纯新增，**不改任何现有入口**：新增 `daft/storage/` 包（15 个文件，约 2.8k 行，含测试）。

### 1. 四层模型（L0–L3）

```
L3 Catalog（命名）      catalog > schema > table
      ├─ location-backed（Hive/Iceberg/Paimon/...）──► 需要 L0/L1/L2
      ├─ DB-backed（Postgres/ClickHouse/...）────────► 到此结束：数据文件由数据库自己管理
      └─ 纯注册表（View/内存）────────────────────────► 到此结束
L2 Table format（Iceberg/Delta/...）
L1 File format（Parquet/CSV/JSON/...）
L0 Filesystem（local/s3/gs/az/http/...）
```

`TableRef` 携带 `location`、`file_format`、`table_protocol`、`layers`，以及新增的 **`location_source`**（`USER` / `CATALOG` / `NONE`）——回答"这次读的路径是谁给的、凭据从哪来"。Iceberg 的两种接入在现状中就已存在（`read_iceberg(".../metadata.json")` 走 `StaticTable.from_metadata`，`daft/catalog/__iceberg.py:64-67` 走 pyiceberg `load_catalog`），本 PR 把它们统一表达出来。

### 2. 协商式下推（吸收 Spark DataSource V2 / Flink connector）

能力不再靠布尔声明，而是**协商并返回残余**：

```python
plan = handle.plan(ScanRequest(filters=[...], columns=[...], limit=10))
plan.filters            # 后端吸收的
plan.filters_residual   # 引擎需要自己重算的
```

这不是引入外来概念：Daft 内部早就是这个协议（`src/daft-scan/src/pushdowns.rs:10-13` 的 `push_filters -> (pushable, remaining)`，配合 `PredicateGroups` 三路拆分 `expr_rewriter.rs:56-67`），本 PR 把它从 filter 专有**推广为统一契约**，并规定 Spark 文档中的协商顺序（filters → aggregation → limit → offset → projection）。

### 3. 三种写形态，保留既有快路径

```python
SinkSpec = NativeTabularSink | CatalogSink | PythonDataSink
```

分别路由到 `LogicalPlanBuilder.write_tabular`（`SinkInfo::OutputFileInfo`）、表协议提交（`SinkInfo::CatalogInfo`）、以及 `write_sink`（`SinkInfo::DataSinkInfo`，`src/daft-logical-plan/src/sink_info.rs:17-23`）。同时引入 `WriteProtocol{APPEND_ONLY, ATOMIC_COMMIT, TWO_PHASE, IDEMPOTENT_UPSERT}`（借鉴 Flink `SinkV2` 的 writer/committer 拆分），让"失败后能否重试、是否可能重复"变成声明式契约。

### 4. 选项契约与可行动报错

Provider 声明 `required_options` / `optional_options` / `forward_options`，未知选项报错并给出拼写建议：

```
Invalid options for provider 'parquet': unknown option(s) ['compresion']
  - Accepted options: ['file_path_column', 'hive_partitioning', 'infer_schema', 'io_config']
  Did you mean 'compression' instead of 'compresion'?
```

### 5. V1 兜底与可发现性

- `ApiLevel.V1_FALLBACK`（对应 Spark `TableCapability.V1_FALLBACK`）：老 `DataSource`/`ScanOperator`/`DataSink` 永久可用，可分批迁移；
- `daft.storage` 入口点组 `daft.storage.providers` 支持第三方注册；
- 调试入口：`describe_uri()`、`handle.describe()`（输出层链、location 来源、能力、写协议、解析轨迹与上次协商结果）、`describe_source()`、`dtype_matrix()`。

```text
$ daft.storage.describe_uri("/tmp/demo.parquet")
uri: /tmp/demo.parquet
location: file:///tmp/demo.parquet (source=user)
layers: ['filesystem', 'file_format']
provider: parquet (kind=file_format, api=v2)
capabilities: ['batch_read', 'batch_write']
write_protocol: atomic_commit
storage: 'file' -> local
format: 'parquet' -> parquet (inferred from extension)
```

## Tests

`tests/storage/`（47 个用例，全部通过）：

| 文件 | 覆盖 |
|---|---|
| `test_registry.py` | URI 解析（含 Windows 盘符）、双轴解析、扩展名推断、未知 scheme/format 的可行动报错、依赖提示、重复注册语义、**全新解释器自举** |
| `test_options_contract.py` | 必填/未知/类型/枚举校验、拼写建议、forwarded 透传 |
| `test_negotiation.py` | residual 语义（含"永不丢项"性质）、协商顺序、parquet 吸收 filter/csv 不吸收的对照 |
| `test_providers.py` | dtype 三态矩阵、能力 mixin 判定、写协议语义、sink 形态、V1 provider |
| `test_handle_end_to_end.py` | 经 handle 读写 parquet/csv 并与既有权 API 结果比对、算子在未下推时仍正确、V1 兜底路径、`describe()` 内容 |

本地执行：

```bash
pytest tests/storage -q     # 47 passed
ruff check daft/storage tests/storage && ruff format --check daft/storage tests/storage
```

> 本地环境说明：该 checkout 未构建 Rust 扩展（本机无 cargo/maturin），因此测试是在 PyPI `daft==0.7.25` 运行时上、把 `daft/storage/` 装入其 site-packages 后执行的；测试只依赖稳定的公开 API（`read_parquet` / `read_csv` / `write_*` / `DataFrame.where/select/limit/offset`），在按 `CONTRIBUTING.md` 构建的仓库环境中可直接运行。

## Follow-ups (not in this PR)

1. P1：把内置文件入口改为 `STORAGE × FORMAT` 两次解析（行为逐字等价，用 `tests/io/**` 作护栏）；
2. P2：数据湖 provider 复用 FORMAT provider（Iceberg 的快照/分区裁剪 + parquet 的读取实现）；
3. P3：数据库 provider（ClickHouse/MongoDB）走 `PythonDataSink` 形态；修 `PostgresCatalog.append` 的单节点串行实现；
4. 补 `docs/` 页面与 mkdocs 导航（本 PR 只提供模块级 docstring 与 `describe()` 输出）。
