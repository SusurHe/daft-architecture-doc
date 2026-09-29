# Daft 源码调研 06 — Python API 层、IO、Scan、Parquet 读取器与运行时

仓库 `D:\DOC\daft-src`，commit `dadd8a0b290be148d92acb6f9e6fc4b6e36f221f`（2026-09-25，约 v0.7.25）。行号相对仓库根，全部来自真实源码；未核实处显式标注「未确认」。

---

## 1. Python API 层

**模块组织**：`daft/dataframe/`（`dataframe.py` 6690 行，`DataFrame`/`GroupedDataFrame`；`preview.py`、`display.py`、`to_torch.py`、`_checkpoint_commit.py`；`__init__.py:3` 只导出两个类）；`daft/expressions/expressions.py:138-139`（`Expression` 持 `_expr: _PyExpr`）；`daft/functions/`（纯 Python 函数门面 `str/list/agg/window/numeric/image/ai/...`）；`daft/io/`（`_parquet/_csv/_json/_avro/_text/_blob/_mcap/_warc/_kafka/_sql/_range/_generator/_files.py` + `av/ aws_config/ bigtable/ clickhouse/ delta_lake/ hudi/ huggingface/ iceberg/ lance/ paimon/ turbopuffer/ webdataset/`）；`daft/runners/`（`runner.py`(ABC) `native_runner.py` `ray_runner.py` `flotilla.py` `partitioning.py` `progress_bar.py`）；`daft/udf/`（`udf_v2.py` `udaf.py` `execution.py` `agg_execution.py` `legacy.py`）；`daft/logical/builder.py:55-59`（包 Rust `PyLogicalPlanBuilder`）；`daft/session.py:96-98`、`daft/context.py`、`daft/catalog/`、`daft/file/`、`daft/subscribers/`、`daft/checkpoint.py`。

**pyo3 绑定：单一扩展模块 `daft.daft`**

- 编译产物只有一个 cdylib：`Cargo.toml:117-119`（`[lib] crate-type=["cdylib"] name="daft"`）；maturin 配置 `pyproject.toml:197-199`（`features=["python"]`）。
- `#[pymodule] fn daft`：**`src/lib.rs:114-199`**。逐个调用各 crate 的 `register_modules`（`src/lib.rs:119-148`，含 `daft_context`/`daft_runners`/`daft_io`/`daft_parquet`/`daft_scan`/`daft_session`/`daft_catalog`/`daft_file::python`），`:161-162` 注册 dashboard/cli，`:165-196` 汇总函数注册表。全仓共 36 处 `pub fn register_modules`。
- `daft/daft/` 目录下**只有 `.pyi` 桩**（`__init__.pyi` 3021 行、`dashboard.pyi`、`testing.pyi`），运行期由编译模块填充。
- 例：`pyclass(name="LogicalPlanBuilder") pub struct PyLogicalPlanBuilder`（`src/daft-logical-plan/src/builder/mod.rs:1242-1249`），注册 `src/daft-logical-plan/src/lib.rs:45-61`。

**惰性语义与执行触发点**

```python
# daft/dataframe/dataframe.py:187-211（节选）
def __init__(self, builder: LogicalPlanBuilder) -> None:
    self.__builder = builder                 # 只存逻辑计划，不执行
    self._result_cache: PartitionCacheEntry | None = None
    self._preview = Preview(partition=None, total_rows=None)
```

- 所有变换只返回新 `DataFrame`；**唯一执行入口** `_materialize_results()`（`dataframe.py:5631-5647`：`get_or_create_runner().run(self._builder)` 后 `result.wait()`）。
- 触发点：`collect()`(`:5681`)；`to_pydict()`(`:5970`)、`to_pylist()`(`:6006`) 先调 `self.collect()`。
- `show(n)` **不整体物化**：`self._builder.limit(n, eager=True)` + `run_iter_tables(builder, results_buffer_size=1)` 迭代到够 n 行即 break（`dataframe.py:5703-5712`），结果缓存在 `self._preview`。
- 所有 `write_*` 先 `write_tabular(...)` 建计划再 `write_df.collect()`：`write_parquet` `:1085-1098`、`write_csv` `:1214`、`write_json` `:1315`、`write_avro` `:1389`、`write_iceberg` `:1574`、`write_sink` `:2409`、`write_lance` `:2494`。
- `iter_rows()`(`:528`)/`to_arrow_iter()`(`:616`) 为流式，不整体物化。

**结果缓存的 Python 侧实现**

- 字段 `DataFrame._result_cache: PartitionCacheEntry`（`dataframe.py:207`）；`_builder` 属性一旦有缓存即替换为 `from_in_memory_scan(self._result_cache, ...)`（`:213-233`），后续算子作用于内存分区而非重算。
- `PartitionCacheEntry{key: str, value: PartitionSet|None}`（`daft/runners/partitioning.py:371-399`），`__getstate__` 只序列化 `key`（`:385-390`）。
- 缓存池是**弱引用**字典 `PartitionSetCache.__uuid_to_partition_set: weakref.WeakValueDictionary`（`partitioning.py:402-423`），DataFrame 回收即释放。
- 执行时把所有既有分区集喂给物理计划（`daft/runners/native_runner.py:148-152`）；写出后回填缓存但保留原 builder 以便 `explain()`（`dataframe.py:1105-1108`）。
- Rust 侧 `PartitionCacheEntry::Python(Arc<PyAny>)`（`src/daft-logical-plan/src/builder/mod.rs:1276`）；分区数/字节/行数由 `src/daft-context/src/partition_cache.rs:36-46` 取出。

---

## 2. Session / 配置体系

**配置读取链**：Python `DaftContext`（`daft/context.py:29-33`）包 `PyDaftContext`，`get_context()` 调 Rust 单例（`:111-113`）。Rust `static DAFT_CONTEXT: OnceLock<DaftContext>`（`src/daft-context/src/lib.rs:262-285`），首次创建时 `Config::from_env()`（`:36-43`）即三份配置各取 `from_env()`；内部 `Arc<RwLock<ContextState>>`（`:55-60`），提供 `execution_config()/planning_config()/io_config()/event_log_config()`（`:91-122`）与订阅者表（`:124-146`）；pyo3 注册 `:309-320`。

**DaftExecutionConfig 主要项与默认值**（`src/common/daft-config/src/lib.rs`，默认值在 `:164-203`）

| 配置项 | 默认 | 行 | 配置项 | 默认 | 行 |
|---|---|---|---|---|---|
| `enable_scan_task_split_and_merge` | false | :167 | `shuffle_aggregation_default_partitions` | 200 | :184 |
| `scan_tasks_min_size_bytes` | 96 MiB | :168 | `partial_aggregation_threshold` | 10000 | :185 |
| `scan_tasks_max_size_bytes` | 384 MiB | :169 | `high_cardinality_aggregation_threshold` | 0.8 | :186 |
| `max_sources_per_scan_task` | 10 | :170 | `read_sql_partition_size_bytes` | 512 MiB | :187 |
| `parquet_split_row_groups_max_files` | 10 | :171 | `default_morsel_size` | 128K 行 | :188 |
| `broadcast_join_size_bytes_threshold` | 10 MiB | :172 | `shuffle_algorithm` | "auto" | :189 |
| `hash_join_partition_size_leniency` | 0.5 | :173 | `pre_shuffle_merge_threshold` / `_partition_threshold` | 1 GiB / 200 | :190-191 |
| `sample_size_for_sort` / `num_preview_rows` | 20 / 8 | :174-175 | `scantask_max_parallel` | 8（0=全 CPU） | :192 |
| `parquet_target_filesize` / `_row_group_size` | 512 / 128 MiB | :176-177 | `native_parquet_writer` | true | :193 |
| `parquet_inflation_factor` | 3.0 | :178 | `actor_udf_ready_timeout` | 120 s | :194 |
| `csv_target_filesize` / `_inflation_factor` | 512 MiB / 0.5 | :179-180 | `maintain_order` | true | :195 |
| `json_target_filesize` / `_inflation_factor` | 512 MiB / 0.25 | :181-182 | `enable_dynamic_batching` / `dynamic_batching_strategy` | false / "auto" | :196-197 |
| `text_inflation_factor` | 1.0 | :183 | `flight_shuffle_dirs` / `_compression` | ["/tmp"] / Some("lz4") | :198-199 |
| — | — | — | `enable_multi_glob_path_tasks` | false | :200 |

Python 入口 `daft.set_execution_config(...)`：`daft/context.py:210-338`（参数与默认值说明 `:252-293`；`with_config_values` 后写回 `ctx._ctx._daft_execution_config` `:300-337`）；上下文管理器 `execution_config_ctx` `:199-207`。

**Planning / EventLog**：`DaftPlanningConfig{default_io_config, disable_join_reordering, enable_strict_filter_pushdown, enable_dp_ccp_join_ordering}`（`:64-73`），env `DAFT_DEV_DISABLE_JOIN_REORDERING`/`DAFT_DEV_ENABLE_STRICT_FILTER_PUSHDOWN`/`DAFT_DEV_ENABLE_DP_CCP_JOIN_ORDERING`（`:83-103`）；`DaftEventLogConfig{enabled:false, path:"~/.daft/events"}`（`:282-294`），env `DAFT_EVENT_LOG_ENABLED`/`DAFT_EVENT_LOG_DIR`（`:297-310`）。Python：`set_planning_config`（`context.py:172-196`）、`set_event_log_config`（`:341-368`）。

**`DAFT_*` 如何被 Rust 读取**：helper `parse_bool_from_env` 仅 `"1"`/`"true"` 为真（`:8-14`），`parse_number_from_env*` 失败时 `eprintln!` 并回落默认（`:48-52`）。ExecutionConfig **实际只读 9 个 env**（`:206-214`、`:217-275`）：`DAFT_SHUFFLE_ALGORITHM`、`DAFT_SCANTASK_MAX_PARALLEL`（`"auto"`→0，`:224-232`）、`DAFT_NATIVE_PARQUET_WRITER`、`DAFT_ACTOR_UDF_READY_TIMEOUT`、`DAFT_PARQUET_/CSV_/JSON_/TEXT_INFLATION_FACTOR`、`DAFT_MAINTAIN_ORDER`。
**注意**：`scan_tasks_min/max_size_bytes`、`max_sources_per_scan_task`、`default_morsel_size` **无对应 env**，只能经 Python `set_execution_config` 设置。其它：`DAFT_LOG`（`src/lib.rs:73-75`）、`DAFT_RUNNER`/`RAY_ADDRESS`/`DAFT_RAY_ADDRESS`/`DAFT_RAY_FORCE_CLIENT_MODE`/`DAFT_RAY_WORKER_STARTUP_TIMEOUT`（`src/daft-runners/src/runners.rs:231-247`）、`DAFT_DASHBOARD(_URL)`（`src/daft-dashboard/src/lib.rs:391`、`python.rs:99-101`）。

**Runner / RunnerConfig**：`RunnerConfig` 枚举与 `create_runner()` 在 `src/daft-runners/src/runners.rs:165-193`（`RayRunner` `:16`、`NativeRunner` `:57`）。推断顺序（`get_runner_config_from_env` `:264-288`）：`DAFT_RUNNER=native|ray`；`=py` 明确报错「PyRunner was removed from Daft from v0.5.0 onwards」（`:268-273`）；空 → `detect_ray_state()`（`:198-206`，回调 Python `daft.utils.detect_ray_state`）→ Ray 环境用 Ray 否则 Native（`:274-282`）。全局 runner 为 `OnceLock`，进程内只能设置一次（`:294`、`:299-308`）。pyo3 在 `src/daft-runners/src/python.rs:9-115`，注册 `lib.rs:19`。Python 门面 `daft/runners/__init__.py:22-36`（get_or_create）、`:53-64`（set_runner_native）、`:67-110`（set_runner_ray + 自动伸缩参数）。Native 执行链 `daft/runners/native_runner.py:131-159`（`builder.optimize(execution_config)` → `LocalPhysicalPlan.from_logical_plan_builder` → `NativeExecutor().run(...)`，executor 建于 `:77`）。

**Session**：Python `Session` 持 `PySession.empty()`（`daft/session.py:96-98`），用 `ContextVar` 维护当前 session（`:84`），可作上下文管理器（`:114-116`）。Rust `Session` = `Arc<RwLock<SessionState>>`（`src/daft-session/src/session.rs:25-28`）；`SessionState` 含 `options: Options`（curr_catalog/curr_schema）、`catalogs: Bindings<CatalogRef>`、`providers`、`tables`、`functions`、`agg_functions`（`:33-48`）。附加第一个 catalog 自动设 current（`attach_catalog` `:103-113`）；`current_catalog()` `:185`、`set_catalog()` `:471`。模块级同名函数（`daft/session.py:19-75` 的 `__all__`）代理到当前 session。

**daft.File**：Python `File`（`daft/file/file.py:27`）持 `PyFileReference`（`:54-59`），`open()`→`PyDaftFile`(`:89`)、`path`(`:110`)、`size()`(`:168`)、`exists()`(`:172`)、`mime_type()`(`:176`)、`to_tempfile()`(`:191`)、`is_*/as_*`(`:219-293`)；`io.RawIOBase` 适配器 `DaftFileIO`（`daft/file/file_io.py:20-100`）+ `open_file()`(`:103-120`)。Rust `DaftFile`（`src/daft-file/src/file.rs:14`）：`load_blocking` `:93`、`from_path` `:103`、`from_bytes` `:111`、`size()` `:119`、`guess_mime_type()` `:128`；pyo3 注册 `src/daft-file/src/python.rs:439-443`；另注册为表达式函数 `src/lib.rs:186-194`（`File/FilePath/FileExists/Size/VideoFile/AudioFile/ImageFile/Hdf5File/GuessMimeType`）。

**Catalog**：Rust `Catalog` trait `src/daft-catalog/src/catalog.rs:12-42`（`name/create_function/create_namespace/create_table/drop_*/get_*/has_*/list_*` + `to_py`；`:46-76` 为 python 变体）；`Table` trait `src/daft-catalog/src/table.rs:42-60+`；`TableSource` 只有 `Schema`/`View`（`:13-20`）；内置实现仅内存版 `src/daft-catalog/src/impls/memory.rs`，Python 实现经 `src/daft-catalog/src/python/{mod.rs,wrappers.rs}` 桥接。Python ABC：`daft/catalog/__init__.py:89`(`Catalog`)、`:696`(`Identifier`)、`:814`(`Function`)、`:880`(`Table`)，`Table.read/write/append/overwrite` 在 `:1049/:1085/:1099/:1108`。

**IOConfig 默认值**（`src/common/io-config/`）：字段 `s3/azure/gcs/http/unity/gravitino/hf/tos/cos/goosefs/hdfs` + `disable_suffix_range` + `opendal_backends: BTreeMap<String,BTreeMap<String,String>>` + `protocol_aliases`（`config.rs:14-32`）。S3（字段 `s3.rs:20-44`，默认 `:239-269`）：`max_connections_per_io_thread=8`(:249)、`retry_initial_backoff_ms=1000`(:250)、`connect_timeout_ms=read_timeout_ms=30000`(:251-252)、`num_tries=25`(:253-255，注释称对齐 AWS EMR AIMD)、`retry_mode=Some("adaptive")`(:256)、`multipart_size=8MiB`/`multipart_max_concurrency=100`(:264-265)。HTTP（`http.rs:8-27`）：`user_agent="daft/0.0.1"`、backoff 1000、两个 timeout 30000、`num_tries=5`。GCS/Azure 的 `max_connections_per_io_thread` 同为 8（`gcs.rs:22,36`；`azure.rs:20,37`）；Cos/GooseFS 为 50。

---

## 3. IO 抽象

**底层是 OpenDAL，但核心后端自研**

```toml
# src/daft-io/Cargo.toml:26,60 —— opendal 0.58（default-features=false）；:66 hdfs = ["opendal/services-hdfs"]
opendal = {version = "0.58", default-features = false, features = [...]}
```

`SourceType` 共 9 种（`src/daft-io/src/lib.rs:517-527`）：`File / Http / S3 / AzureBlob / GCS / HF / Unity / Gravitino / OpenDAL{scheme}`；`supports_native_writer()` 仅对 `File | S3 | Gravitino | OpenDAL{..}` 为真（`:548-552`）。后端文件：`local.rs`、`http.rs`、`s3_like.rs`、`azure_blob.rs`、`google_cloud.rs`、`huggingface/`、`unity.rs`、`gravitino.rs`、`opendal_source.rs`（承载通用 OpenDAL 后端，消费 `opendal_backends` 配置）。

**IOClient / ObjectSource**

```rust
// src/daft-io/src/lib.rs:208-211
pub struct IOClient {
    source_type_to_store: tokio::sync::RwLock<HashMap<SourceType, Arc<dyn ObjectSource>>>,
    config: Arc<IOConfig>,
}
```

方法（同行文件）：`new`(:214)、`support_suffix_range`(:221)、`get_source_and_path`(:226，`resolve_url_alias` + 按 SourceType 惰性缓存 client)、`get_source`(:362)、`glob`(:368)、`single_url_get`(:391)、`single_url_put`(:435)、`single_url_get_size`(:447)、`single_url_download`(:458)、`single_url_upload`(:488)、`supports_native_writer`(:548)。导出 `object_io::{FileMetadata, FileType, GetResult, ObjectSource}`(:52)、`range::GetRange`(:61)、`stats::{IOStatsContext, IOStatsRef}`(:57)。`single_url_get(uri, Option<GetRange>, io_stats)` 是唯一数据读取入口。

**重试/超时/并发/缓存**：重试独立成模块 `src/daft-io/src/retry.rs`；退避与次数来自各后端 `IOConfig`。`max_connections_per_io_thread` 在 `s3_like.rs`/`google_cloud.rs`/`azure_blob.rs`/`http.rs` 初始化时消费。辅助模块 `range.rs`、`range_expansion.rs`、`multipart.rs`、`stream_utils.rs`、`counting_reader.rs`(`CountingReader`)、`stats.rs`、`object_store_glob.rs`(glob 展开)。缓存：`ls src/daft-io/src` **无 cache 目录**，未见独立字节范围缓存；跨任务去重依赖按 `SourceType` 缓存 client 对象（`:233-238`）与 `FileMetadata`。「相邻 range 合并」不在 daft-io，而在 Parquet 读取器内（§5.2）。

---

## 4. Scan 与下推

**ScanOperator trait**（`src/daft-scan/src/scan_operator.rs:14-70`）

```
name() / schema() / partitioning_keys() / clustering_keys()(默认 None)
file_path_column() / generated_fields()
can_absorb_filter() / can_absorb_select() / can_absorb_limit() / can_absorb_shard()
multiline_display()
supports_count_pushdown() -> false（默认）          // :47-49
statistics() -> Option<Statistics>（默认 None）      // :51-57，优化器可直接用，跳过逐任务聚合
supported_count_modes() -> Vec<CountMode>（默认空）   // :59-61
to_scan_tasks(pushdowns: Pushdowns) -> Vec<ScanTaskRef>              // :65，核心
as_pushdown_filter() -> Option<&dyn SupportsPushdownFilters>（默认 None） // :67-69
```

**Pushdowns 与下推 trait**

```rust
// src/daft-scan/src/pushdowns.rs:16-36
pub struct Pushdowns {
    pub filters: Option<ExprRef>,            // 兼容字段，等价全部当前过滤
    pub partition_filters: Option<ExprRef>,
    pub columns: Option<Arc<Vec<String>>>,
    pub limit: Option<usize>,
    pub sharder: Option<Sharder>,
    pub pushed_filters: Option<Vec<ExprRef>>,
    pub aggregation: Option<ExprRef>,        // 用于 count 等下推
}
```

`SupportsPushdownFilters` 定义于同文件 `pushdowns.rs:10`；算子经 `as_pushdown_filter()` 暴露，能力由 `can_absorb_*`（`scan_operator.rs:41-44`）声明。`Sharder` 见 `src/daft-scan/src/sharder.rs`；分区/列统计剪枝见 `src/daft-scan/src/statistics.rs`。

**GlobScanOperator 与任务结构**：`GlobScanOperator`（`src/daft-scan/src/glob.rs:28-34`）字段含 `file_path_column`、`hive_partitioning`，构造 `:159-160`；Schema 推断读第一个文件（`:490`），Hive 分区字段由 `parse_hive_partitioning`+`hive_partitions_to_fields` 生成（`:518-520`），`file_path_column` 注入 `:525`，`to_scan_tasks` 在 `:658`（`:691-714` 注入列与分区值）。`ScanSourceKind::File { parquet_metadata, chunk_spec }`（`src/daft-scan/src/lib.rs:176-182`）；`ChunkSpec` 只有 `Parquet(indices)` 与 `Bytes{start,end}`（`lib.rs:143-148`）；`DataSource` 在 `src/daft-scan/src/source.rs`。

**切分与合并算法**（入口 `src/daft-scan/src/scan_task_iters/mod.rs`，由 `cfg.enable_scan_task_split_and_merge` 控制）：

- **合并** `merge_by_sizes`(`:33-79`) → `MergeByFileSize`(`:81-94`)：无 LIMIT 时上下界取 `cfg.scan_tasks_max_size_bytes`/`min_size_bytes`，`max_source_count = cfg.max_sources_per_scan_task`（`:70-77`）；有 LIMIT 时先用首任务估算 limit 行字节数，取 `×1.5`/`÷2` 为新界，估算失败则**不合并**（`:38-68`）。就绪条件：文件数 ≥ `max_source_count` 或累计 `estimate_in_memory_size_bytes()` ≥ 下界（`:101-111`）。
- **Parquet 行组切分** `scan_task_iters/mod.rs:254-321`：累加 `rg.compressed_size()` 与 `column_materialized_sizes()`，达 `scan_tasks_min_size_bytes` 即产出 ScanTask，写入 `ChunkSpec::Parquet(indices)`（`:288`）与仅含相关 RG 的 metadata（`:285-286`）；是否切分受 `parquet_split_row_groups_max_files`（默认 10）限制。
- **JSONL 字节范围切分** `scan_task_iters/split_jsonl/mod.rs:20-136`：仅未压缩 `.jsonl/.ndjson` 单 source 生效，按 `scan_tasks_max_size_bytes` 切并右对齐到换行，产出 `ChunkSpec::Bytes`（`:113`）。
- CSV / Avro **不做**字节范围切分（CSV 文件内部分块并行；Avro 整文件一个任务）。

---

## 5. Parquet 读取器（v0.7.14 重写版）

**文件与依赖**：`src/daft-parquet/src/` 下 `lib.rs`(170) `read.rs`(824) `metadata.rs`(612) `metadata_adapter.rs`(365) `schema_inference.rs`(349) `python.rs`(364) `helpers.rs`(314) `statistics/{mod,column_range,table_stats,utils}.rs` `reader/{mod,chunk_source,field_reader,rg_processor,util}.rs`。`src/daft-parquet/Cargo.toml:19` → `parquet = {workspace=true, features=["async","experimental"]}`，版本 `Cargo.toml:346` = `parquet = "59.0.0"`。**`parquet2` 已彻底移除**：全仓 `grep -rn parquet2`（含 `Cargo.lock`）**0 命中**，仅残留历史 `arrow2` 注释（如 `schema_inference.rs:7`）。

**IO 模型：本地 pread coalescing + 远端 RG range GET**。仅用 arrow-rs **底层件**（不用 `ParquetRecordBatchReaderBuilder`）：

```rust
// src/daft-parquet/src/reader/field_reader.rs:4-19（节选）
use parquet::{
    arrow::{
        array_reader::{
            ArrayReader, FixedSizeListArrayReader, ListArrayReader, MapArrayReader,
            NullArrayReader, PrimitiveArrayReader, StructArrayReader, make_byte_array_reader,
            make_byte_view_array_reader, make_fixed_len_byte_array_reader,
        },
        arrow_reader::RowSelection,
    },
    file::{metadata::ParquetMetaData, serialized_reader::SerializedPageReader},
};
```

- 自建 `ArrayReader` 树 + `SerializedPageReader`（`field_reader.rs:85`，page 位置取自 offset index `:78-82`）。
- 本地：unix `FileExt::read_at` / windows `seek_read`（`reader/chunk_source.rs:428-442`、`:444-459`），在 `MAX_COALESCE_GAP = 64*1024`（`:396`，调用点 `:418`）内合并小 range；`coalesce_ranges`(`:21-40`) 判据 `entry.start <= group.end + max_gap`。
- 远端：按 RG 的 `column(col_idx).byte_range()`（`:550`）→ `coalesce_and_split`（`:619-663`；`MAX_COALESCE_GAP = 1MiB`、`SPLIT_THRESHOLD = 24MiB`、`MAX_REQUEST_SIZE = 16MiB`，常量 `:527-529`）→ 每个合并组立即 spawn `single_url_get(GetRange::Bounded(range))`（`:576-586`）。
- **先剪枝再预取**：`ChunkSourceBuilder::build` 注释要求仅在谓词剪枝后调用（`:277-297`），实际调用 `reader/mod.rs:622`，紧随 `prune_row_groups`（`:593`）。远端只预取 `active_col_indices`（`:139-152`，列集合 = 用户列 ∪ 谓词列）。
- footer：默认尾读 128 KiB（`metadata.rs:452`），大 footer 两段读（`:489-501`），`PAR1` 校验（`:383-400`）。

**并发模型：JoinSet + 有界 channel(1) + 按 RG 顺序回放**

```rust
// src/daft-parquet/src/reader/mod.rs:494-525（节选，行号已核实）
let (senders, receivers): (Vec<_>, Vec<_>) = (0..rg_inputs.len())
    .map(|_| tokio::sync::mpsc::channel::<DaftResult<RecordBatch>>(1))   // 容量 1
    .unzip();
let mut joinset: JoinSet<DaftResult<()>> = JoinSet::new();              // common_runtime::JoinSet
for (rg_pos, (sender, inputs)) in senders.into_iter().zip(rg_inputs).enumerate() {
    joinset.spawn_on(async move { /* process_rg_* → sender.send(item) */ }, &compute);
}
let inner_streams = receivers.into_iter().map(ReceiverStream::new);
let merged: BoxStream<'static, DaftResult<RecordBatch>> =
    Box::pin(futures::stream::iter(inner_streams).flatten());           // 按 RG 顺序回放
common_runtime::combine_stream(merged, async move { joinset.join_all().await }).boxed()
```

`JoinSet` 来自 `common_runtime`（`reader/mod.rs:16`，实现 `src/common/runtime/src/joinset.rs:14`，保序版 `OrderedJoinSet` `:131`）。列解码层再叠一层 `JoinSet` + `mpsc::channel::<DaftResult<ArrayRef>>(1)`（`reader/rg_processor.rs:67-69`）。文件内**始终按 RG 顺序**输出，`maintain_order=false` 只在 scan task 之间重排（注释 `reader/mod.rs:482-485`）；跨 RG LIMIT 由 `apply_cross_rg_limit` 提前终止（`:546-572`）。

**两阶段谓词下推与列复用**

- **阶段 A（RG 级统计）**：`helpers.rs::prune_row_groups`（`:241-314`），顺序为用户 `row_groups` → `start_offset` → `num_rows` → 谓词统计；统计缺失/转换失败时**保守保留**（`:295-304`）。统计链路 `statistics/table_stats.rs:11-58` → `statistics/column_range.rs:89+`。
- **阶段 B（行级）**：不用 arrow-rs 的 `RowFilter`/`ArrowPredicate`（全仓 0 命中），改为解码谓词列 → `eval_predicate_mask`（`reader/util.rs:43-51`）→ `filter_arrays_by_mask`（`:53-69`）→ `bool_array_to_row_selection`（`helpers.rs:128-156`，bool mask RLE 成 `RowSelection`）→ `refine_selection` 与 offset/delete 基础选择合并（`helpers.rs:161-200`，调用点 `reader/mod.rs:452-455`）。数据列直接消费 `RowSelection`：`skip_records`/`read_records`/`consume_batch`（`field_reader.rs:752`/`:759`/`:763,772`）。
- **列复用**：谓词列在阶段 A 已过滤成 `state.filtered_pred`，阶段 B 组装 batch 时直接 slice：

```rust
// src/daft-parquet/src/reader/rg_processor.rs:160-172（节选）
for &col_idx in &plan.read_col_indices {
    if let Some(pp) = plan.pred_col_indices.iter().position(|&i| i == col_idx) {
        arrays.push(state.filtered_pred[pp].slice(state.offset, chunk_rows)); // 复用阶段1结果
    } else {
        let dp = plan.data_col_indices.iter().position(|&i| i == col_idx).expect("...");
        arrays.push(data_chunks[dp].clone());
    }
}
```

- 列计划 `ColumnPlan`（`reader/mod.rs:135-148`：`read_col_indices` = 投影 ∪ 谓词、`pred_col_indices`、`data_col_indices`、`predicate_pushed`），构造 `resolve_column_plan`（`:150-220`）。物理 leaf 映射 `leaves_for_top_fields`（`field_reader.rs:38-60`）按顶层字段粒度读（投影 struct 任一子列会读该字段全部 leaf）。
- 空投影走 `count_only_stream`（`reader/mod.rs:528-544`）；RG 全被剪掉返回**空流**而非空 batch（`:602-611`）。布隆过滤器仅元数据透传（`metadata.rs:209-210`），**从未用于剪枝**。读取器中**无任何 `DAFT_*` 环境变量**（`grep -rn "DAFT_" src/daft-parquet` 0 命中）。

**metadata / schema_inference / pyo3**：`metadata.rs::read_parquet_metadata`（`:518-554`，`ParquetMetaDataReader::decode_metadata` 在 `spawn_blocking` 内 `:538`）；Iceberg field-id 重写 `apply_field_ids_to_arrowrs_parquet_metadata`（`:131-251`，逐字段重建 chunk，**丢弃内嵌 ARROW:schema** `:242-250`，缺 field id 硬失败 `:147-156`）；`strip_string_types_from_parquet_metadata`（`:284-330`）。`metadata_adapter.rs`：`DaftParquetMetadata{inner, original_indices}`(`:23-27`)、`from_arrowrs`(`:31`)、自定义 serde(`:97-119`/`:122-168`)、`DaftRowGroupMetaData::column_materialized_sizes()`(`:219-244`，按物理类型估算**解码后**字节数，被扫描任务切分直接使用)。`schema_inference.rs::infer_schema_from_parquet_metadata_arrowrs`(`:8-70`)：`parquet_to_arrow_schema`(`:19`) → INT96 单位改写(`:22-32`) → raw string→Binary(`:35-41`) → 递归去 Dictionary(`:51`/`:74-111`) → Utf8/Binary 提升为 Large(`:63-67`)。`python.rs:353-364` 注册 6 函数：`read_parquet`(`:47`)、`read_parquet_into_pyarrow`(`:133`)、`read_parquet_into_pyarrow_bulk`(`:245`)、`read_parquet_bulk`(`:188`，`num_parallel_tasks` 默认 128 `:217`)、`read_parquet_schema`(`:298`)、`read_parquet_statistics`(`:328`)，均 `py.detach(...)`。

**CSV / JSON / Avro 要点**

| | CSV | JSON | Avro |
|---|---|---|---|
| 解析库 | `csv` + `csv-async`（`src/daft-csv/Cargo.toml:9-10`），列反序列化用自家 `daft-decoding`(`:13`) | `serde_json`(raw_value) + `simd-json`（`src/daft-json/Cargo.toml:21,23`） | `arrow-avro` 59（`src/daft-avro/Cargo.toml:4`） |
| 分片/并行 | 整文件一个 task；文件内按 `SLABSIZE=4MiB` 切块 + rayon（`daft-csv/src/local/pool.rs:9`、`local.rs:187`、设计注释 `:28-129`）；远端 `csv_async::AsyncReader` 流式 | JSONL 支持字节范围切分（§4.4）；本地 mmap + rayon（`daft-json/src/local.rs:70,210-256`），`total_rows<=128` 退化单线程（`:306-308`） | 整文件、无 range 切分；远端用自实现 `arrow_avro::reader::AsyncFileReader` 做 range 读（`daft-avro/src/read.rs:18-45,118-124`） |
| schema 推断 | `read_csv_schema_single`(`daft-csv/src/metadata.rs:108-140`)，采样上限 **1 MiB**（`read.rs:559-560`、`metadata.rs:196`） | `read_json_schema_single`(`daft-json/src/schema.rs:109-141`)，默认 `max_bytes=1MiB`(`schema.rs:64`) | `read_avro_schema`(`daft-avro/src/schema.rs:8-55`，需整文件下载) |
| 配置 | CSV/JSON 各有 SourceConfig | `JsonParseOptions.sample_size` 默认 1024（`daft-json/src/options.rs:126-134`） | `AvroSourceConfig {}` 空结构（`daft-avro/src/options.rs:44-56`）；**不存在** `AvroReadOptions` |

`arrow-csv`/`arrow-json` 只用于**写**（`src/daft-writers/src/csv_writer.rs:10`、`json_writer.rs:7`）。

---

## 6. 写出与表格式

**daft-writers**（`src/daft-writers/src/`：`lib.rs physical.rs file.rs batch.rs batch_file_writer.rs partition.rs sink.rs storage_backend.rs utils.rs catalog.rs pyarrow.rs test.rs` + `parquet_writer.rs csv_writer.rs json_writer.rs ipc.rs avro_writer.rs lance.rs`）：

- 工厂导出 `pub use lance::make_lance_writer_factory`(`lib.rs:48`)、`pub use sink::make_data_sink_writer_factory`(`:54`)。
- `DataSinkWriter`（`sink.rs:12`，构造 `:20`）实现 `write(&mut self, data)`(`:35`) 与 `close()`(`:89`)；`DataSinkWriterFactory`(`:101`)。`PhysicalWriterFactory`（`physical.rs:23`，构造 `:30`）是格式无关物理写工厂；pyarrow 回退 `create_pyarrow_file_writer`(`:151`，对应 `native_parquet_writer=false`)。
- `file.rs` 按目标大小滚动切文件：`write_and_update_bytes`(`:55`)、`rotate_writer_and_update_estimates`(`:69`)、`write`(`:100`/`:212`)、`close`(`:168`/`:234`)；测试覆盖单/多文件与边界（`:327-383`），对应 `*_target_filesize`/`*_inflation_factor` 配置。单文件模式 `single_file=True` 仅 native runner 支持（`daft/dataframe/dataframe.py:1068-1071`），与 `partition_cols`/`overwrite-partitions` 互斥（`:1062-1067`）。
- Parquet 写出属性（`src/daft-writers/src/parquet_writer.rs:64-72`）：

```rust
let mut builder = WriterProperties::builder()
    .set_writer_version(WriterVersion::PARQUET_1_0)   // parquet_writer.rs:68
    .set_compression(default_compression);            // :69
for (parts, compression) in ... { builder = builder.set_column_compression(...); } // :72
```

  默认压缩 `SNAPPY`（`:95`、`:130`）；支持 `none/uncompressed/snappy/gzip/lzo/brotli/lz4/lz4_raw/zstd`（`parse_compression` `:42-52`）。**未设置** row group 大小与 statistics/bloom filter/page index/dictionary 属性（同文件无 `set_max_row_group_size`/`set_statistics_enabled`/`set_column_bloom_filter_enabled` 命中），写出统计依赖 arrow-rs 默认行为。
- Python→Rust 统一路径 `LogicalPlanBuilder.write_tabular(root_dir, partition_cols, write_mode, write_success_file, file_format, file_format_option, compression, io_config, single_file)`（`dataframe.py:1085-1095`）后 `collect()`。

**表格式集成：全部在 Python 层**

- 根 `Cargo.toml` **无** `iceberg`/`deltalake`/`hudi`/`paimon`/`lance` 依赖（grep 0 命中），`src/` 下也无这些格式的 Rust 实现目录。
- Python 依赖（`pyproject.toml`）：`iceberg = ["pyiceberg >= 0.7.0, != 0.9.1, != 0.10.0, <= 0.11.1"]`(`:45`)、`deltalake = ["deltalake >= 1.6.0,< 1.7.0"]`(`:37`)、`lance = ["daft-lance>=0.5.0,<0.6.0"]`(`:47`)、`unity = ["httpx…","unitycatalog<0.2.0","deltalake…"]`(`:63`)、`hudi = []`(`:43`，**空 extras**)。
- 读取入口在 `daft/io/__init__.py:25-28` 导出：`iceberg/_iceberg.py`、`delta_lake/_deltalake.py`、`hudi/_hudi.py`、`paimon/_paimon.py`。Hudi 自实现 `daft/io/hudi/{hudi_scan.py,pyhudi/}`，`read_hudi` 用 `HudiDataSource`（`_hudi.py:47`）；Paimon 依赖外部 `pypaimon`（`_paimon.py:134`），用 `PaimonDataSource`（`:138`）。Catalog 后端同样在 Python：`daft/catalog/{__glue.py,__iceberg.py,__paimon.py,__postgres.py,__s3tables.py,__unity/,__gravitino/}`。
- Iceberg field-id → Daft schema 的映射在 Rust 侧有辅助（`src/daft-parquet/src/metadata.rs:131-251`），但快照/分区裁剪的**决策**在 Python。**未确认**：`src/daft-scan/src/expr_rewriter.rs` 在分区谓词重写中的参与度。

---

## 7. 其他连接器

统一机制（`daft/io/source.py`）：`DataSource`（`:27`）提供 `name()/schema()/get_partition_fields()/get_clustering_keys()/supports_count_pushdown()` 与核心 `async def get_tasks(pushdowns) -> AsyncIterator[DataSourceTask]`（`:96`）、`read()`（`:103`）；`DataSourceTask`（`:114`）提供 `schema()`(`:123`) + `async def read() -> AsyncIterator[RecordBatch]`(`:127`)，旧接口 `get_micro_partitions()` 保留但**已 deprecated**（`:141`）。Rust 桥接 `src/daft-scan/src/python/wrappers.rs`（`source.py:261-270`）；Python `ScanOperator` ABC 在 `daft/io/scan.py:31-83`。

| 连接器 | 实现层 | 证据 |
|---|---|---|
| Kafka | **Python**（`confluent_kafka`）：`KafkaSource(DataSource)` `daft/io/_kafka.py:259`，导入 `:11`；无 Rust kafka crate | — |
| Hugging Face datasets | Python，复用 `read_parquet`/`read_webdataset`（`daft/io/huggingface/__init__.py:7-8`），失败回退 `datasets` 库（`:15-27`）；Rust 另有 `src/daft-io/src/huggingface/`（`SourceType::HF`） | 双路径 |
| WebDataset | Python：`daft/io/webdataset/{__init__.py,_webdataset.py}`，被 HF 读取器复用 | — |
| MCAP | Rust crate `src/daft-mcap/` + Python 入口 `daft/io/_mcap.py`（`daft/io/__init__.py:35`） | — |
| WARC | Rust crate `src/daft-warc/` + `daft/io/_warc.py`（`:33`） | — |
| ClickHouse | Python：`daft/io/clickhouse/` + `DataFrame.write_clickhouse`（`dataframe.py:2709`） | — |
| Postgres / SQL | Python：`daft/io/_sql.py` + `daft/sql/{sql,sql_connection,sql_scan}.py`；`read_sql_partition_size_bytes` 默认 512 MiB（`daft-config/src/lib.rs:187`） | — |
| Bigtable | Python：`daft/io/bigtable/`；`write_bigtable`（`dataframe.py:2791`） | — |
| Turbopuffer | Python：`daft/io/turbopuffer/`；`write_turbopuffer`（`dataframe.py:2660`） | — |
| Avro/Text/Blob/Range/Generator | Python 入口 `daft/io/{_avro,_text,_blob,_range,_generator,_files}.py`，数据读取在 Rust（`src/daft-avro/`、`src/daft-text/`） | — |
| Lance | 读：外部包 `daft-lance`（`pyproject.toml:47`）；写：Rust `src/daft-writers/src/lance.rs`（`lib.rs:48`） | 读 Python / 写 Rust |

---

## 8. 可观测性与运维

**进度条**：`daft/runners/progress_bar.py` —— `get_tqdm(use_ray_tqdm)`（`:8-39`，Ray 用 `ray.experimental.tqdm_ray.tqdm` `:11`，否则 `tqdm.auto` `:13` 并处理 notebook 检测 `:23-37`）；`ProgressBar`（`:42`）含 `_make_new_bar`(`:57`)、`make_bar_or_update_total`(`:70`)、`update_bar`(`:92`)、`close`(`:99`)；注释说明所有 tqdm 写操作集中在单一长驻线程（`:107`）。

**Dashboard**：Rust `src/daft-dashboard/`（`lib.rs` axum Router+静态资源、`assets.rs`、`client.rs`、`engine.rs`、`events.rs`、`import.rs`、`state.rs`、`python.rs`）。默认监听 `DEFAULT_SERVER_ADDR = Ipv4Addr::UNSPECIFIED`、`DEFAULT_SERVER_PORT = 3238`（`lib.rs:43-44`，并注明监听全部 IPv4 有生产安全风险 `:42`）。Python API：`launch(noop_if_initialized=False, port=None) -> ConnectionHandle`、`ConnectionHandle.get_port()/shutdown()`、`register_dataframe_for_display`、`generate_interactive_html`（桩 `daft/daft/dashboard.pyi:5-11`）；实现 `src/daft-dashboard/src/python.rs`（`RUNNING_PORT` `:17`、`launch` `:81`、端口回落 `:96`、`std::thread::spawn` `:110-114`、注入 `DAFT_DASHBOARD_URL` `:99-101`）。常量 `DAFT_DASHBOARD` 在 `lib.rs:391`，随 `register_modules`(`:390-397`) 暴露，由 `src/lib.rs:161` 注册。订阅者侧 `daft/subscribers/{abc,dashboard,event_log,event_log_sink,events}.py`；Rust 事件总线 `src/daft-context/src/lib.rs:148-259`（`QueryStart/QueryHeartbeat/QueryEnd/OptimizationStart/OptimizationComplete/ExecStart/ExecEnd/OperatorStart/OperatorEnd/Stats`，`:18-25`），`dispatch_event` 逐订阅者容错（`:245-259`）。

**OpenTelemetry**：`src/common/tracing/`（依赖 `opentelemetry`/`_otlp`/`_sdk`，`lib.rs:7-9`）。显式对齐标准 OTEL 变量（注释 `config.rs:4-5`）：`OTEL_EXPORTER_OTLP_ENDPOINT`(`:48`)、`OTEL_EXPORTER_OTLP_PROTOCOL`(`:66`)、`OTEL_EXPORTER_OTLP_{METRICS,LOGS,TRACES}_ENDPOINT`(`:71-73`)、`OTEL_METRIC_EXPORT_INTERVAL`(`:17`)、`OTEL_SERVICE_NAME`/`OTEL_RESOURCE_ATTRIBUTES`(`:166-167,180-202`)；历史兼容 `DAFT_DEV_OTEL_EXPORTER_OTLP_ENDPOINT`(`:14`)。初始化在 pyo3 模块加载时 `init_tracing()`（`src/lib.rs:117`）。

**Telemetry（Scarf）**：`daft/scarf_telemetry.py` POST 到 `https://daft.gateway.scarf.sh/<endpoint>`（`:100`）；端点 `daft-runner`（带 `runner` 字段，`:132-136`）与 `daft-import`(`:139-141`)。退出开关 `opted_out()`(`:32-37`)：`SCARF_NO_ANALYTICS in ("true","1")` 或 `DO_NOT_TRACK in ("true","1")` 或 `DAFT_ANALYTICS_ENABLED in ("0","false")`。

**Checkpointing**：Rust `src/daft-checkpoint/`（`lib.rs` 中 `register_modules` `:21`；`builder.rs config.rs error.rs scan.rs store.rs types.rs impls/`）。类型 `CheckpointStatus`(`types.rs:11`)、`Checkpoint`(`:34`)、`FileFormat`(`:69`)、`FileMetadata`(`:82`)、`pub trait CheckpointStore`(`store.rs:54`)；pyo3 包装 `PyCheckpointStore`(`config.rs:29`)、`PyCheckpointStatus`(`:112`)、`PyCheckpoint`(`:129`)、`PyCheckpointFileFormat`(`:160`)、`PyFileMetadata`(`:181`)。Python：`CheckpointStore(path, io_config)`（`daft/checkpoint.py:25-45`）、`CheckpointConfig`(`:79`)、`IdempotentCommit`(`:160`)；查询侧 `LogicalPlanBuilder.with_checkpoint(config)`（绑定 `src/daft-logical-plan/src/builder/mod.rs:1293-1298`），源侧 `daft/io/_checkpoint.py::attach_checkpoint`（Hudi 读取器使用，`daft/io/hudi/_hudi.py:10,47`）。幂等协议：`list_checkpoints()`/`get_checkpointed_files()`/`mark_committed(ids)`（`checkpoint.py:66-73`）；配置结构体在 `src/common/checkpoint-config/`。

---

## 9. 源码地图

| 路径 | 职责 | 关键类型 |
|---|---|---|
| `daft/dataframe/dataframe.py` | DataFrame 门面，惰性计划 + 结果缓存 | `DataFrame`, `GroupedDataFrame` |
| `daft/logical/builder.py` | Python 计划构建器 | `LogicalPlanBuilder` |
| `daft/context.py` / `daft/session.py` | 全局配置 / 会话与 catalog 绑定 | `DaftContext`, `Session` |
| `daft/catalog/__init__.py` | Catalog ABC + 各后端 | `Catalog`, `Table`, `Identifier` |
| `daft/runners/{runner,native_runner,ray_runner}.py` | Runner 抽象与实现 | `Runner`, `NativeRunner`, `RayRunner` |
| `daft/runners/partitioning.py` | 分区集与结果缓存 | `PartitionSet`, `PartitionCacheEntry`, `PartitionSetCache` |
| `daft/runners/progress_bar.py` | tqdm 进度条 | `ProgressBar` |
| `daft/io/{scan,source,sink,writer}.py` | 扫描/源/写抽象（Python） | `ScanOperator`, `DataSource`, `DataSourceTask`, `DataSink` |
| `daft/file/{file,file_io}.py` | 统一文件抽象与 raw IO | `File`, `DaftFileIO` |
| `daft/checkpoint.py` | 幂等 checkpoint 提交 | `CheckpointStore`, `IdempotentCommit` |
| `src/lib.rs` | pyo3 唯一模块 `daft`，汇总注册 | `#[pymodule] daft` |
| `src/common/daft-config/src/lib.rs` | 执行/规划/事件日志配置 + env | `DaftExecutionConfig`, `DaftPlanningConfig` |
| `src/common/io-config/src/*.rs` | 各后端 IO 配置与凭据 | `IOConfig`, `S3Config`, `HTTPConfig` |
| `src/daft-context/src/lib.rs` | Rust 全局 Context 单例 + 事件总线 | `DaftContext`, `Config` |
| `src/daft-session/src/session.rs` | Session 状态与 catalog 绑定 | `Session`, `SessionState` |
| `src/daft-runners/src/runners.rs` | Runner 单例与配置推断 | `Runner`, `RunnerConfig` |
| `src/daft-catalog/src/{catalog,table}.rs` | Catalog/Table trait | `Catalog`, `Table`, `TableSource` |
| `src/daft-io/src/lib.rs` | IO 入口与后端分派 | `IOClient`, `SourceType` |
| `src/daft-io/src/object_io.rs` | 对象存储抽象 | `ObjectSource`, `GetResult`, `FileMetadata` |
| `src/daft-io/src/{retry,range,multipart,object_store_glob}.rs` | 重试/范围/分片上传/glob | `GetRange` |
| `src/daft-scan/src/scan_operator.rs` / `pushdowns.rs` | 扫描算子 trait / 下推载体 | `ScanOperator`, `Pushdowns`, `SupportsPushdownFilters` |
| `src/daft-scan/src/glob.rs` | 文件 glob 扫描 | `GlobScanOperator` |
| `src/daft-scan/src/scan_task_iters/mod.rs` | 扫描任务切分/合并 | `MergeByFileSize` |
| `src/daft-scan/src/python/wrappers.rs` | Python 数据源桥接 | `PyDataSourceTask` |
| `src/daft-parquet/src/reader/mod.rs` | Parquet 读取编排 | `ColumnPlan`, `build_rg_stream` |
| `src/daft-parquet/src/reader/chunk_source.rs` | 字节来源/pread/range 预取 | `LocalChunkSource`, `RemoteChunkSource`, `coalesce_ranges` |
| `src/daft-parquet/src/reader/field_reader.rs` | 自建 arrow `ArrayReader` 树 | `FieldReaderBuilder` |
| `src/daft-parquet/src/helpers.rs` | RG 剪枝 + `RowSelection` 构造 | `prune_row_groups`, `bool_array_to_row_selection` |
| `src/daft-parquet/src/metadata{,_adapter}.rs` | footer 读取与自有元数据 | `DaftParquetMetadata`, `DaftRowGroupMetaData` |
| `src/daft-writers/src/{sink,physical,file,parquet_writer}.rs` | 写出工厂/滚动切文件/Parquet 属性 | `DataSinkWriter`, `PhysicalWriterFactory` |
| `src/daft-dashboard/src/{lib,python}.rs` | Dashboard HTTP 服务（axum，端口 3238） | `ServerOptions`, `launch` |
| `src/daft-checkpoint/src/{store,types}.rs` | Checkpoint 存储与类型 | `CheckpointStore`, `Checkpoint` |

### 本次未核实 / 存疑

1. `src/daft-io/src/object_io.rs` 中 `ObjectSource` trait 的完整方法清单未逐行列出（仅确认导出与 `GetRange` 用法）。
2. daft-io 是否存在独立 range/文件元数据缓存：`ls src/daft-io/src` 无 cache 目录，结论为「未见」而非「确认不存在于其它 crate」。
3. `daft-io/src/retry.rs` 的重试次数/退避算法细节未展开（仅确认配置项与默认值）。
4. `src/daft-writers/src/{catalog.rs,pyarrow.rs,batch.rs}` 的职责未逐一核实。
5. Iceberg/Delta 的 time travel 与分区裁剪确认实现层为 Python（pyiceberg/deltalake），但 Rust `expr_rewriter.rs` 的具体参与度未核实。
6. CSV/JSON/Avro 在 Ray runner 下的任务粒度、Avro `AsyncAvroFileReader::builder` 第三参数 `1024` 的语义（外部 crate）未确认。
