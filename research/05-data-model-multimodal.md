# 05 · 数据模型（daft-core）与多模态能力实现

> 仓库 `D:\DOC\daft-src`，commit `dadd8a0b290be148d92acb6f9e6fc4b6e36f221f`（2026-09-25）。证据为相对仓库根的 `路径:行号`；**未找到**的结论已显式标注并附搜索关键词。

**七个反直觉结论**：① 底层是 **arrow-rs 59.0.0** 不是 arrow2（全仓 `arrow2` 仅 1 处过期注释 `src/daft-core/src/datatypes/python.rs:15`）；② Rust 侧**没有 `Table` 类型**了，已被 `RecordBatch` 取代（`src/daft-recordbatch/src/lib.rs:68`）；③ `MaterializedResult` **只在 Python**（`daft/runners/partitioning.py:180`），Rust 对应物是 `PartitionSet` + `MicroPartitionSet`；④ `daft-decoding` **不是视频解码**，是 CSV/JSON 字节→Arrow 反序列化 + 类型推断（全仓唯一 SIMD 热点）；⑤ `daft-text` **不 tokenize**（是文本文件读取），tokenize 在 `daft-functions-tokenize` 用 `tiktoken-rs`；⑥ **AI 函数 100% 是 Python 实现**，`src/daft-ai` 仅 63 行的 provider 句柄桥；⑦ `daft-functions-serde` **不是计划序列化**而是数据标量函数，**`src/daft-serde` 这个 crate 不存在**。

---

## 1. 数据模型层次

### 1.1 类型层次与结构定义

`DataFrame(Py) → MicroPartition{schema, chunks: Vec<RecordBatch>, metadata, statistics} → RecordBatch{schema: SchemaRef, columns: Arc<Vec<Column>>, num_rows} → Column → Series{inner: Arc<dyn SeriesLike>} → ArrayWrapper<A: DaftArrayType>`。最底层数组三族：`DataArray<T>`（物理，包 `ArrayRef` + 独立 `NullBuffer`）、`LogicalArray<L>`（逻辑，内含 PhysicalType 数组）、`FixedSizeListArray`/`ListArray`/`StructArray`/`UnionArray`（嵌套）、`PythonArray`（feature 门控）。

- `RecordBatch`：`src/daft-recordbatch/src/lib.rs:67-72`，schema↔列校验 `:84-102`；`Series`：`src/daft-core/src/series/mod.rs:30-34`。
- `SeriesLike` trait（**DataType 分发的核心抽象**）：`src/daft-core/src/series/series_like.rs:13-42`，25 个方法（`to_arrow/as_any/with_nulls/nulls/cast/filter/get_lit/...`）。

```rust
// src/daft-core/src/array/mod.rs:40-46
pub struct DataArray<T> {
    pub field: Arc<Field>,
    data: ArrayRef,               // arrow-rs ArrayRef（= Arc<dyn Array>）
    nulls: Option<NullBuffer>,    // validity bitmap 单独持一份引用
    marker_: PhantomData<T>,
}
```

### 1.2 DataType 分发机制：宏单态化 + `Arc<dyn Trait>`，无 enum 大 match

全靠 `with_match_daft_types!`（`src/daft-core/src/datatypes/matching.rs:2-65`）把 `DataType` 单态化到类型级 `$T`，三步：

1. **建 Series**：`with_match_daft_types!(field.dtype, |$T| <<$T as DaftDataType>::ArrayType as FromArrow>::from_arrow(...)?.into_series())` — `src/daft-core/src/series/mod.rs:111-116`。
2. **建 trait object**：`impl_series_like_for_data_array!` / `_logical_array!` / `_nested_arrays!` 为每种数组生成 `impl SeriesLike for ArrayWrapper<A>`（`src/daft-core/src/series/array_impl/data_array.rs:29-37`；logical 版 `logical_array.rs:26`；nested 版 `nested_array.rs:18`）。
3. **取回具体数组**：`Series::downcast::<Arr>()` 走 `Any::downcast_ref`（`src/daft-core/src/series/ops/downcast.rs:21-30`），配 `Series::i64()/u8()/embedding()` 语法糖。

```rust
// src/daft-core/src/series/array_impl/data_array.rs:18-27
impl<T: DaftArrowBackedType> IntoSeries for DataArray<T>
where ArrayWrapper<Self>: SeriesLike,
{ fn into_series(self) -> Series { Series { inner: Arc::new(ArrayWrapper(self)) } } }
```

宏实例化清单（可据此枚举全部数组类型）：`data_array.rs:158-176`（20 个：Null/Boolean/Binary/FixedSizeBinary/Int8..Float64/Utf8/Interval/Decimal128/Extension）、`logical_array.rs:188-201`（13 个：Date/Time/Duration/Uuid/Timestamp/Image/FixedShapeImage/Tensor/Embedding/FixedShapeTensor/SparseTensor/FixedShapeSparseTensor/Map）、`nested_array.rs:165-168`（FixedSizeList/Struct/List/Union）；包装类型 `ArrayWrapper<T>(pub T)` 在 `series/array_impl/mod.rs:9-14`。
**注意：`impl_series_like!` / `impl_array!` 这两个宏名不存在**（`grep -rn "macro_rules! impl_array\|macro_rules! impl_series_like"` 只命中上述三个 `_for_*` 宏）。

### 1.3 DataType 枚举全量清单

定义 `src/daft-schema/src/dtype.rs:16-152`，`#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]`：

| 分组 | 变体（行号） |
|---|---|
| Arrow 原生 | `Null`(20) `Boolean`(23) `Int8/16/32/64`(26-35) `UInt8/16/32/64`(38-47) `Float16/32/64`(50-56) `Decimal128(usize,usize)`(59) |
| 时间 | `Timestamp(TimeUnit,Option<String>)`(75) `Date`(79) `Time(TimeUnit)`(83) `Duration(TimeUnit)`(86) `Interval`(91) |
| 二进制/文本 | `Binary`(94) `FixedSizeBinary(usize)`(97) `Uuid`(100) `Utf8`(103) |
| 嵌套 | `FixedSizeList(Box<Self>,usize)`(106) `List(Box<Self>)`(109) `Struct(Vec<Field>)`(112) `Map{key,value}`(115) `Union(Vec<Field>,Vec<i8>,UnionMode)`(151) |
| Arrow 扩展 | `Extension(String, Box<Self>, Option<String>)`(121) |
| **Daft 逻辑扩展** | `Embedding(Box<Self>,usize)`(125) `Image(Option<ImageMode>)`(128) `FixedShapeImage(ImageMode,u32,u32)`(131) `Tensor(Box<Self>)`(134) `FixedShapeTensor(Box<Self>,Vec<u64>)`(137) `SparseTensor(Box<Self>,bool)`(140) `FixedShapeSparseTensor(Box<Self>,Vec<u64>,bool)`(143) |
| 其他 | `Python`(146，feature 门控) `Unknown`(148) `File(MediaType)`(149) |

分发口径不止一个，`matching.rs` 共 10 个匹配宏：`with_match_daft_types`（全量，无 default 分支）、`_physical_daft_types`(`:68`)、`_arrow_daft_types`(`:111`)、`_comparable/iterable/hashable/numeric/primitive/integer/float_and_null_daft_types`(`:148/183/216/254/283/313/339`)、`_daft_logical_primitive_types`(`:361`)、`_file_types`(`:383`)。

### 1.4 Null 处理与标量表示

- `NullArray = DataArray<NullType>`（`src/daft-core/src/datatypes/mod.rs:461`），`impl_daft_arrow_datatype!(NullType, Null)`（`:188`），Arrow 侧即 `arrow_schema::DataType::Null`（`dtype.rs:283`）。
- **validity bitmap 独立持有**：`DataArray.nulls: Option<NullBuffer>`（`array/mod.rs:44`）；覆写走 `with_nulls()` 重建 ArrayData（`:200-219`），`slice()` 重新 `from_arrow`（`:225-234`）；`null_count()` = `logical_null_count()`（`:177-179`）。
- 全 null/空构造：`FullNull` trait `src/daft-core/src/array/ops/full.rs:18-19`；嵌套类型用 `NullBuffer::from_iter(repeat_n(false, length))`（`:81/:112/:145`）；`Series::full_null/empty` 分发 `series/mod.rs:119-130`。
- **NULL 被显式跳过**：`Series::build_probe_table_without_nulls` 对 `DataType::Null` 提前返回空表，且注释声明 NULL 不计入 unique 计数（`series/mod.rs:64-104`）。
- 标量：`Literal` 枚举 `src/daft-core/src/lit/mod.rs:36-...`（`Null/Boolean/Utf8/Binary/Uuid/Int8..UInt64/Timestamp/Date/Time/Duration/Interval/Float16/32/64/Decimal(i128,u8,i8)/List(Series)/Python`）；逐行取值 `SeriesLike::get_lit(&self, idx) -> Literal`（`series_like.rs:41`），实现宏 `impl_array_get_lit!`（`src/daft-core/src/array/ops/get_lit.rs:227`）。`Literal`↔`Series` 在 `src/daft-core/src/series/from_lit.rs`（638 行），Python 绑定 `src/daft-core/src/lit/python.rs`（798 行）。

### 1.5 Micropartition 与"分区集"语义

`MicroPartition` 是**执行期最小调度单元**：n 个 `RecordBatch` + schema + 元数据 + 统计，**本身是物化的**（惰性发生在 plan/scan 层，不在 Micropartition）。

字段与不变量：`schema: SchemaRef`、`chunks: Arc<Vec<RecordBatch>>`、`metadata: TableMetadata`（仅 length + column_sizes）、`statistics: Option<TableStatistics>`（每列 `ColumnRangeStatistics` min/max）—— 定义 `src/daft-micropartition/src/micropartition.rs:34-53`。

- 构造强制不变量：`new_loaded` 断言每个 batch schema 与自身、stats schema 均一致（`:62-102`）；`from_arrow` `:104-109`；`empty` `:111-115`；`concat_or_get`（0/1/n batch 合并策略）`:144-157`。`TableMetadata`：`src/daft-stats/src/table_metadata.rs:6-14`；`TableStatistics`：`src/daft-stats/src/table_stats.rs:21-25`。分片算子目录 `src/daft-micropartition/src/ops/`（agg/cast_to_schema/concat/eval_expressions/filter/join/partition/pivot/slice/sort/take/unpivot）。
- 关系链：`Partition` trait（仅 `as_any/size_bytes/num_rows`，`src/common/partitioning/src/lib.rs:15-19`）← `PartitionRef = Arc<dyn Partition>`（`:38`）← `PartitionSet<T: Partition>` trait（12 方法，含 `to_partition_stream`、`get_merged_partitions`，`:58-83`）← 实现 `MicroPartitionSet`（`src/daft-micropartition/src/partitioning.rs:29-33`，内部 `Arc<RwLock<BTreeMap<PartitionId, MicroPartitionRef>>>`，用 BTreeMap 保证输出有序）。缓存层 `PartitionSetCache`（`common/partitioning/src/lib.rs:137-141`）+ `src/daft-context/src/partition_cache.rs:28`。
- **`MaterializedResult` 在 Rust 中不存在**（`grep -rn "MaterializedResult" src/` → 0）。只在 Python：`daft/runners/partitioning.py:180 MaterializedResult`、`:286 LocalPartitionSet`、`:349 LocalMaterializedResult`。Rust↔Python 桥见 `src/daft-context/src/partition_cache.rs:53-73`（`LocalPartitionSet` 包 `PyMicroPartitionSet`）。

---

## 2. 零拷贝与 Arrow 互操作

### 2.1 arrow-rs 而非 arrow2（Cargo 证据）

`Cargo.toml:253-263`（workspace）声明 `arrow = "59.0.0"`、`arrow-array 59.0.0`(features `chrono-tz`)、`arrow-buffer/arrow-data/arrow-flight/arrow-row/arrow-schema/arrow-select 59.0.0`、`arrow-ipc 59.0.0`(features `lz4,zstd`)。`src/daft-core/Cargo.toml:2-3,9` 直接依赖 `arrow`/`arrow-row`/`common-arrow-ffi`(optional)。

迁移残留仅两处、且都不是依赖：`src/daft-core/src/datatypes/python.rs:15` 注释提到 `arrow2::buffer::Buffer<Arc<Py<PyAny>>>`；`src/daft-functions-serde/src/lib.rs:1` 的 `#![allow(deprecated, reason = "arrow2 migration")]` 是历史 lint 名。`grep -rn "arrow2" --include=*.toml src/ Cargo.toml` → **0 命中**。

### 2.2 FFI：PyCapsule 协议为主、`_export_to_c` 兜底

`common-arrow-ffi` 为自研 crate（**不用 `arrow-pyarrow`**，原因见 `src/common/arrow-ffi/src/lib.rs:1-5`：上游锁的 pyo3 版本不兼容）。

- trait：`FromPyArrow::from_pyarrow_bound`(`:83-88`)、`ToPyArrow::to_pyarrow`(`:91-94`)、`IntoPyArrow::into_pyarrow`(`:97-100`)。
- 导出走 **Arrow PyCapsule Interface**：`array_to_pycapsules`(`:418-429`) 用 `PyCapsule::new_with_value(py, ffi_schema, c"arrow_schema")` + `c"arrow_array"`；RecordBatch 按规范以 **StructArray** 形式导出（`:434-445`，字段名 `""`）；reader 走 `FFI_ArrowArrayStream`（`:448-455`）。
- 导入优先 `__arrow_c_array__`（`:306-346`），失败才回退 `validate_class("Array")` + `_export_to_c`（`:348-365`）。
- Python 协议实现：`PySeries.__arrow_c_schema__`/`__arrow_c_array__`（`src/daft-core/src/python/series.rs:168-192`，含 `requested_schema` 时 `arrow::compute::cast` 分支 `:185-189`）；`PyRecordBatch` 对应 `src/daft-recordbatch/src/python.rs:554-573`；Python 转发 `daft/series.py:228-229`。

### 2.3 零拷贝成立条件与例外（关键）

```rust
// src/daft-core/src/python/series.rs:575-592（节选）
let mut data = self.to_arrow()?.to_data();
data.align_buffers();                 // ← 未对齐时会拷贝（64B 对齐）
let array_ptr = &raw const *Box::new(arrow::ffi::FFI_ArrowArray::new(&data));
let schema_ptr = &raw const *Box::new(arrow::ffi::FFI_ArrowSchema::try_from(self.field().to_arrow()?)?);
// pyarrow.Array._import_from_c(array_ptr, schema_ptr)
```

1. **类型必须一致**。`DataArray::from_arrow` 校验 Arrow 类型，仅对"arrow→Daft→arrow 往返一致"的自动 `cast`（Utf8→LargeUtf8、Binary→LargeBinary），否则 `TypeError`（`src/daft-core/src/array/mod.rs:122-162`）。
2. **Extension 需重包装**：PyArrow `_import_from_c` 会丢未注册扩展类型的 metadata，故重新用 `DaftExtension` + `ExtensionArray.from_storage` 包（`series.rs:600-622`）。
3. **FixedShapeTensor 改为 PyArrow canonical 类型**（`series.rs:625-629`）。
4. **两条 RecordBatch 导出路径代价不同**：`record_batch_to_arrow` 逐列 `to_pyarrow` 后 `pyarrow.RecordBatch.from_arrays`（`src/daft-recordbatch/src/ffi.rs:42-63`）；`record_batch_to_arrow_rs` 是纯 Rust、无 Python 路径（`ffi.rs:65-87`）；单次 Struct 导出走 `__arrow_c_array__`（`src/daft-recordbatch/src/python.rs:559-573`）。
5. `PyRecordBatch.from_arrow_record_batches` 做 schema 转换 + `concat`（`ffi.rs:12-39`），是显式拷贝点。

### 2.4 Python 侧封装

- `daft/recordbatch/recordbatch.py:86-114`：`from_arrow_table` 先查是否含 `python()/tensor/sparse_tensor` 等非原生字段，**有则退回 `from_pydict` 慢路径**（`:91-104`），否则走 `_PyRecordBatch.from_arrow_record_batches` 快路径（`:107`）。
- 导出：`to_arrow_record_batch`/`to_arrow_table` = `pa.RecordBatch.from_pydict({name: column.to_arrow()})`（`:170-176`，**逐列**）；`to_pydict`/`to_pylist` 纯 Python 逐值物化（`:178-192`）。
- `daft/series.py:41-60`：`Series.from_arrow` 同样先试原生类型，命中 `FixedShapeTensorType` 时改 `array.storage` 再 `cast`，否则退回 `from_pylist(pyobj="force")`。

---

## 3. 扩展类型与多模态

### 3.1 Arrow 扩展类型：`Field` 承载 metadata，`to_physical()` 承载存储

`DataType::Extension(name, storage, metadata)` 本身**无** Arrow 映射（`dtype.rs:443-451` 明确注释），映射发生在 `Field::to_arrow`：

- `Extension` → 物理字段 + `ARROW:extension:name`/`ARROW:extension:metadata`（`src/daft-schema/src/field.rs:99-116`），并查 `EXTENSION_TYPE_REGISTRY` 还原 **coercion 前**的原始 storage 类型（`:17-20, :102-107`）；`Uuid` → 规范 `arrow.uuid`（`:117-125`，常量 `:14`）。
- **8 个 Daft 逻辑类型统一用 `daft.super_extension` + 完整 dtype JSON**：`Embedding/Image/FixedShapeImage/Tensor/FixedShapeTensor/SparseTensor/FixedShapeSparseTensor/File`（`:126-145`；常量 `DAFT_SUPER_EXTENSION_NAME` 在 `dtype.rs:266`）。`Python` 类型物理为 `Binary`，同样标 `daft.super_extension`（`:146-161`）。
- Python 对偶实现：`daft/extension_type.py:11-32`（`DaftExtension(pa.ExtensionType)`，name 固定 `"daft.super_extension"`）；注册/反注册 `daft/datatype.py:1585-1607`（双检锁 + `atexit.unregister_extension_type`）。

### 3.2 各扩展类型的物理存储（`dtype.rs:361-435`）

| 逻辑类型 | 物理表示 |
|---|---|
| `Embedding(inner,size)` | `FixedSizeList(inner.to_physical(), size)`（`:377`） |
| `Image(mode)` | `Struct{data: List<UInt8 或 mode.get_dtype()>, channel: UInt16, height: UInt32, width: UInt32, mode: UInt8}`（`:378-387`） |
| `FixedShapeImage(mode,h,w)` | `FixedSizeList(mode.get_dtype(), num_channels*h*w)`（`:388-391`） |
| `Tensor(inner)` | `Struct{data: List<inner>, shape: List<UInt64>}`（`:392-395`） |
| `FixedShapeTensor(inner,shape)` | `FixedSizeList(inner, product(shape))`（`:396-399`） |
| `SparseTensor(inner,_)` | `Struct{values: List, indices: List<UInt64>, shape: List<UInt64>}`（`:400-404`） |
| `FixedShapeSparseTensor(inner,shape,_)` | `Struct{values, indices: List<按最大下标自适应 U8/U16/U32/U64>, shape}`（`:405-422`） |
| `File(..)` | `Struct{url: Utf8, io_config: Binary, position: Int64, size: Int64}`（`:424-429`） |
| `Extension(_,storage,_)` / `Uuid` / `Map` / 时间 | `storage.to_physical()`（`:423`）/ `FixedSizeBinary(16)` / `List<Struct<key,value>>` / `Int64 或 Int32`（`:364-376`） |

类型 marker：`src/daft-core/src/datatypes/mod.rs:251-260`（`ImageType/TensorType/SparseTensorType` → `PhysicalType = StructType`；`EmbeddingType/FixedShapeImageType/FixedShapeTensorType` → `FixedSizeListType`；`FileType<T>` 手工 impl `:262-279`）。数组别名 `src/daft-core/src/datatypes/logical.rs:189-196`。列索引常量：`src/daft-core/src/array/image_array.rs:21-25`（data=0/channel=1/height=2/width=3/mode=4）、`tensor.rs:4-14`（data=0/shape=1）。图片零拷贝读取 `src/daft-core/src/array/ops/image.rs:26-52 as_image_obj`（按 offsets 造 `Cow::Borrowed`）。

### 3.3 图片：`daft-image` + `image` crate + rayon，无 SIMD、无 GPU

- 依赖 `src/daft-image/Cargo.toml:9,14,18,19` → `common-image`/`image`/`rayon`/`rustfft`；workspace `Cargo.toml:326 image = "0.25.10"`、`:353 rayon`、`:387 rustfft = "6"`。`fast_image_resize`/`imageproc`/`nvjpeg`/`turbojpeg`/`mozjpeg` 全部 **未找到**。
- 编解码**全部委托 `image` crate**：`src/common/image/src/cow_image.rs:109-113`（`image::load_from_memory`）、`:115-132`（`image::write_buffer_with_format`），无自研 codec。
- kernel 用 `image::imageops::*`：resize `cow_image.rs:147-172`（`FilterType::Triangle`，`:170` 其余模式 `unimplemented!()`）；crop `:174-201`（`crop_imm(...).to_image()`）。rayon 并行只在**元素级**、非像素级：`src/daft-image/src/ops.rs:316-325`(resize)、`:327-343`(crop)、`:94-100`(to_mode)、`:990-1000`(hash_images)。
- `ImageMode` 定义在 **daft-schema**：`src/daft-schema/src/image_mode.rs:38-49`（L/LA/RGB/RGBA/L16/LA16/RGB16/RGBA16/RGB32F/RGBA32F），`from_pil_mode_str` `:71-93`、`try_from_num_channels` `:94-112`、`num_channels` `:114-123`；`ImageFormat` 5 种（PNG/JPEG/TIFF/GIF/BMP）在 `src/daft-schema/src/image_format.rs:17-23`。
- 能力边界：**DataFrame 只能存 8-bit 四模式**（`cow_image.rs:48-56`；`ops/image.rs:119` 对非 L/LA/RGB/RGBA 直接 `assert!`），与 `image_mode.rs:15-18` warning 一致；**无 `ImageError`**，统一 `DaftError`。
- 对外 10 个函数：`src/daft-image/src/functions/mod.rs:17-29`（crop、image_decode、decode_image_file、image_encode、image_file_metadata、image_hash、image_resize、to_tensor、to_mode、image_attribute）。
- **rotate / downsample / SIMD / GPU decode：均未找到**（搜索词 `rotate`、`downsample`、`simd`、`target_feature`、`std::arch`、`cuda`、`nvjpeg`、`npp`）。

### 3.4 张量

- 表示见 3.2：`Tensor` 是 `Struct<data, shape>`，仅 `FixedShapeTensor` 落 `FixedSizeList`。`TensorArray::data_array()/shape_array()`：`src/daft-core/src/array/ops/tensor.rs:3-15`；sparse↔dense 互转测试 `:26-77`。
- **没有 `matmul`**（`grep -rn matmul src/ daft/` → 0）；**没有张量归一化/累加算子**；**没有 torch 张量互操作算子**。张量对外 API 实质只有 `image_to_tensor`（`daft/functions/image.py`，重导出 `daft/functions/__init__.py:131,459`）、`as_tensor` 类型转换（`daft/expressions/expressions.py:573`）、cast 到 sparse。
- 真正的数值算子落在**向量距离**：`src/daft-functions/src/distance/{cosine,dot_product,euclidean}.rs`（67-73 行/个）；入口校验 `src/daft-functions/src/vector_utils.rs:11-55` —— 同时接受 `FixedSizeList` 与 `Embedding`，inner dtype 限 `Int8|Float32|Float64`、输出 `Float64`；实现是 `FixedSizeListArray` 迭代 + `try_as_slice::<T>()`（`:61-80`），**标量循环，无 BLAS/SIMD**。
- numpy/pandas/torch 互操作走 Python 列表或 Arrow：`daft/dataframe/to_torch.py:97`、`daft/recordbatch/recordbatch.py:117-132`（`from_pandas` 先 `pa.Table.from_pandas`）。

### 3.5 文本 / 音频 / 视频 / 文档

- **`daft-text` 是文本文件读取，不是 tokenizer**：`src/daft-text/src/lib.rs:1-4` 仅 `options`/`read`；入口 `src/daft-text/src/read.rs:50-56 stream_text`，只接受 UTF-8（`:57-63`），产出单列 Utf8。**tokenize 在 `daft-functions-tokenize`**：`src/daft-functions-tokenize/Cargo.toml:15 tiktoken-rs`（workspace `Cargo.toml:369 tiktoken-rs = "0.9.1"`）；`src/daft-functions-tokenize/src/bpe.rs:9 use tiktoken_rs::CoreBPE;`，内建词表 `:91-100`（cl100k_base/o200k_base），`encode()` `:218`；**Arrow 表示为 `List<UInt32>`**（`encode.rs:76-79`），落盘 `UInt32Builder`+`OffsetBuffer`（`:93-114`）。
- **音频/视频/PDF：Rust 侧无任何编解码实现**。`grep -rn --include=*.rs -iE "ffmpeg|decord|whisper|pypdf|pdfium|mutool|librosa|torchcodec|pyav" src/` → **0 命中**；Rust 只有类型与 MIME 嗅探：`src/daft-schema/src/media_type.rs:9-15`（`MediaType{Unknown,Video,Audio,Image,Hdf5}`）、`src/daft-file/src/file.rs:406`(`PDF_MAGIC`)、`:462-463`(PDF MIME)、`:467-476`(audio/video MIME)。
- 实际能力在 Python：视频 `daft/io/av/_read_video_frames.py:10 import av`、`:139 av.open`、`:159 container.decode(stream)`，入口 `daft/io/av/__init__.py:25 read_video_frames`（缺依赖抛 `ImportError` `:92`）；音频 `daft/file/audio.py:6 from daft.dependencies import librosa, np, sf`、`:94 librosa.resample`，元数据 `daft/functions/audio.py:15-18`。**PDF 只能当字节喂模型**（`daft/functions/ai/__init__.py:468`），无解析器。
- **`daft-decoding` 真实职责 = 字节→Arrow 反序列化 + schema 推断**（`src/daft-decoding/src/lib.rs:1-3`），服务 CSV/JSON/scan。依赖 `atoi_simd 0.16.1`、`fast-float2 0.2.3`、`simdutf8`、`csv`、`csv-async`、`chrono`（`src/daft-decoding/Cargo.toml`）——**全仓唯一 SIMD 热点**：`deserialize.rs:57-59 simdutf8::basic::from_utf8`、`:93` 起多处 `atoi_simd::parse_skipped`、`:323-326 fast_float2::parse`；推断入口 `inference.rs:21-37`（null→Boolean→Int64→Float64→Utf8/Binary）。消费方仅 `daft-csv`、`daft-json`、`daft-scan`。
- `src/daft-mcap`（机器人 MCAP 容器，依赖 `mcap` crate，`McapReader` 在 `src/daft-mcap/src/read.rs:164`）只解析消息结构，**不解码视频帧**。

### 3.6 Embedding 与向量检索（Lance）

- `Embedding` = **无 shape 元数据的定长扁平向量**（物理 `fixed_size_list<f32>` 语义），与 `Tensor`（带 shape）的区别即在此。渲染为 sparkline（`src/daft-core/src/array/ops/repr.rs:354-368`），**排序未实现**：`repr.rs:672-674 todo!("impl sort for EmbeddingArray")`；Python 构造 `daft/datatype.py:623-632`。
- 距离算子见 3.4。**Daft 自身没有向量索引/ANN/最近邻 API**：`src/**/*.rs` 中 `hnsw`、`vector_index` 未找到；`nearest` 的 Rust 命中全是 asof join（`src/daft-core/src/join.rs:196`、`src/daft-local-execution/src/join/asof_join.rs:266`）。
- **daft-io 里没有任何 lance 代码**（`grep -rni lance src/daft-io/` 只命中 `src/daft-io/src/opendal_source.rs:67` 注释中的单词 "balance"）；唯一声明是可选依赖 `pyproject.toml:47 lance = ["daft-lance>=0.5.0,<0.6.0"]`。
- Lance 能力 = **格式读写 + 参数透传**：读 `daft/io/lance/_lance.py:12 _daft_lance = LazyImport("daft_lance._lance")`、`:20-37 read_lance`，文档 `:70-76` 写明 `default_scan_options` "accepts the same arguments described in `lance.LanceDataset.scanner`"；写 `daft/dataframe/dataframe.py:2424 write_lance` → `daft/recordbatch/recordbatch_io.py:478-496`（直接 `import lance` + `lance.fragment.write_fragments`）。
- 索引与查询都由外部包完成，测试即证据：`tests/io/lancedb/test_lancedb_vector_search.py:76-81`（`default_scan_options={"nearest": {"column","q","k"}}`）、`:118-128`（`metric="cosine"`）、`:159-164`（直接调 `ds.create_index("vector","IVF_PQ",...)`）。

---

## 4. AI Functions

### 4.1 `src/daft-ai` 只是一个 63 行 provider 句柄桥

- 全 crate 3 文件：`src/daft-ai/src/lib.rs`(7 行)、`provider.rs`(14)、`python.rs`(42)。`src/daft-ai/Cargo.toml:1-9` 唯一依赖 `pyo3`(optional)，feature `python = ["dep:pyo3"]` —— **无 tokio/reqwest/async-openai/daft-ext**。
- Rust trait 只有 2 个方法且自述仅 session 使用（`src/daft-ai/src/provider.rs:3-14`）：`pub type ProviderRef = Arc<dyn Provider>;` / `pub trait Provider: Sync + Send + std::fmt::Debug { fn name(&self) -> String; fn to_py(&self, py: Python<'_>) -> PyResult<Py<PyAny>>; }`。
- 唯一消费方是 daft-session：`src/daft-session/src/session.rs:8,41,136`（`providers: Bindings<ProviderRef>`、`attach_provider`）；Python 注入 `src/daft-session/src/python.rs:38 PyProviderWrapper::from(provider).arced()`，用户 API `daft/session.py:219-231`。
- **`embed_text`/`prompt`/`classify` 在 Rust 侧不存在**（`grep -rn "embed_text" --include=*.rs src/` → 0）。**`daft-ai` 不使用 `daft-ext`**：`daft-ext-macros` 实际导出 `#[daft_extension]`(`src/daft-ext-macros/src/lib.rs:28`)、`#[daft_func_batch]`(`:97`)、`#[daft_func]`(`:276`)；`daft-ext`/`daft-ext-internal` 面向**第三方 ABI 扩展**（`src/daft-ext/src/{abi,ffi,session}.rs`），内建函数库不用。

### 4.2 真正的 provider 抽象在 Python

真正可扩展的 provider 抽象全在 Python：`class Provider(ABC)` 定义 `get_text_embedder/get_image_embedder/get_image_classifier/get_text_classifier/get_prompter`（`daft/ai/provider.py:104`，各方法 `:123/129/135/141/147`）；`PROVIDERS` 注册表 + `load_provider` 在 `daft/ai/provider.py:84-97`；`TextEmbedder`/`ImageEmbedder`/`Prompter` 等 `Protocol` 在 `daft/ai/protocols.py:15/18、35/38、82/85`；`Descriptor`（`instantiate`/`get_udf_options`/`get_dimensions`）在 `daft/ai/typing.py:128-153`、`:167-184`。

Provider 清单：`openai`（`daft/ai/openai/provider.py:20`，默认 `text-embedding-3-small`/`gpt-4o-mini` `:23-24`）、`transformers`（`daft/ai/transformers/provider.py:33`，默认 `sentence-transformers/all-MiniLM-L6-v2` `:37`）、`google`（`daft/ai/google/provider.py:19`）、`lm_studio`、`vllm-prefix-caching`（`daft/ai/vllm/provider.py:20`）。**Anthropic provider 未找到**（`grep -rn anthropic daft/ src/` 仅命中 `daft/session.py:561` docstring）。

### 4.3 实现结构：`daft.udf.cls` + `daft.method` 的类 UDF

全部在 `daft/functions/ai/__init__.py`：`embed_text:72`、`embed_image:157`、`classify_text:250`、`classify_image:329`、`prompt:453`（默认 provider OpenAI `:580`）。

```python
# daft/functions/ai/__init__.py:130-143（节选）
text_embedder = _resolve_provider(provider, "transformers").get_text_embedder(model, dimensions, **options)
udf_options = text_embedder.get_udf_options()
is_async = text_embedder.is_async()
call_impl = _TextEmbedderExpression._call_async if is_async else _TextEmbedderExpression._call_sync
_TextEmbedderExpression.__call__ = method.batch(
    method=call_impl,
    return_dtype=text_embedder.get_dimensions().as_dtype(),
    batch_size=udf_options.batch_size,
)
```

- 数据接口：入参 Python `Series` 经 `to_pylist()` → `list[str]`，出参 `list[Embedding]`（numpy 数组），见 `daft/ai/_expressions.py:25-39`（5 个 `_*Expression` 在 `:25/50/75/90/108`）；Python `Embedding` 仅类型别名（`daft/ai/typing.py:156-164`）。
- provider 解析优先级（显式实例→session 注册名→session 当前→默认）在 `daft/functions/ai/__init__.py:43-64`；API key 作 provider 构造参数透传（`daft/ai/utils.py:64-76 merge_provider_and_api_options` → `AsyncOpenAI(**merged)` `daft/ai/openai/protocols/text_embedder.py:220`）。**Rust 侧读 `OPENAI_API_KEY` 的代码未找到。**

### 4.4 批处理与并发

- Rust 侧无并发设施（依赖表已证）。并发全在 Python UDF 层：`UDFOptions.concurrency`（`daft/ai/typing.py:176-184`）→ `daft_cls(max_concurrency=..., max_retries=3)`（`daft/functions/ai/__init__.py:144-151`）。**`Semaphore` 未找到**（`grep -rn Semaphore daft/ai/` → 0）。
- batch size：OpenAI text embedder `64 / max_retries=3 / on_error="raise"`（`daft/ai/openai/protocols/text_embedder.py:94-96`），**token 预算攒批 + 超长单条切分** `batch_token_limit=300_000`（`:163`、`:240-267`）；transformers 下 text `64`、image `16`、text classifier `64`、image classifier `16`。
- 异步：基类 `is_async() -> False`（`daft/ai/protocols.py:29-31`），OpenAI 覆写 True（`daft/ai/openai/protocols/text_embedder.py:158-159`）；命中 `RateLimitError` 降级为 `asyncio.gather` 并发单条（`:282-287`）。GPU：`get_gpu_udf_options()` 按可见 GPU 数设 concurrency/num_gpus（`daft/ai/utils.py:35-55`）。
- **唯一非 UDF 的 Rust 通路是 vLLM prompt**：直接构造 Rust 表达式 `messages._expr.vllm(...)`（`daft/functions/ai/__init__.py:590-604` ↔ `src/daft-dsl/src/python.rs:811`、`src/daft-dsl/src/expr/mod.rs:305`）。token 用量指标 `daft/ai/metrics.py:6-26`。

---

## 5. 函数库组织

### 5.1 注册机制：`FunctionRegistry`(HashMap) + `FunctionModule`，**显式集中注册**

```rust
// src/daft-dsl/src/functions/mod.rs:131-142, 158-164, 191-192
pub struct FunctionRegistry { map: HashMap<String, Arc<dyn ScalarFunctionFactory>> }
pub trait FunctionModule { fn register(_parent: &mut FunctionRegistry); }
pub fn add_fn_factory(&mut self, function: impl ScalarFunctionFactory + 'static) {
    let function = Arc::new(function);
    for alias in function.aliases() { self.map.insert((*alias).to_string(), function.clone()); }
    self.map.insert(function.name().to_string(), function);
}
pub static FUNCTION_REGISTRY: LazyLock<RwLock<FunctionRegistry>> = LazyLock::new(|| RwLock::new(FunctionRegistry::new()));
```

另有 `add_fn`（同步单态化 `:167-173`）、`add_async_fn`（`:175-180`），分别包成 `BuiltinScalarFnVariant::Sync/Async`（`src/daft-dsl/src/functions/scalar.rs:39-43`）。
**没有 inventory/linkme/ctor 自动注册**（`grep -rn "inventory\|linkme" src/ --include=*.rs` 无相关命中）。唯一注册点是 `src/lib.rs:164-196`，注释直言 *"We need to do this here because it's the only point in the rust codebase that we have access to all crates"*，其中 `register::<XFunctions>()` 共 16 次（`:168-183`：numeric/float/uri/image/binary/list/utf8/json/serde/temporal/Misc/distance/similarity/tokenize/random/geo）、`add_fn`/`add_async_fn` 共 12 次（`:185-196`：coalesce、file/file_path/file_exists/size/video_file/audio_file/image_file/hdf5_file/guess_mime_type、monotonically_increasing_id）。

### 5.2 单函数写法：`#[typetag::serde] impl ScalarUDF` + `FunctionArgs`（**无 `#[daft_function]` 宏**）

对 `utf8/list/json/temporal/binary/uri/tokenize` 等内建函数，宏名 `#[daft_function]` **不存在**（`grep -rn "daft_function" src/ --include=*.rs` → 0）。实际三步：手写 struct → `#[typetag::serde] impl ScalarUDF`（`typetag` 提供 trait-object 的序列化/反序列化）→ 自由函数包成 `ExprRef`。

```rust
// src/daft-functions-utf8/src/chr.rs:20-32、:95-97（节选）
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Chr;
#[typetag::serde]
impl ScalarUDF for Chr {
    fn name(&self) -> &'static str { "chr" }
    fn call(&self, inputs: FunctionArgs<Series>, _ctx: &EvalContext) -> DaftResult<Series> { ... }
    fn get_return_field(&self, inputs: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> { ... }
    fn docstring(&self) -> &'static str { ... }
}
#[must_use] pub fn chr(input: ExprRef) -> ExprRef { ScalarFn::builtin(Chr {}, vec![input]).into() }
```

`FunctionArgs` 是 `common_macros` 的 derive（`src/daft-dsl/src/functions/function_args.rs:4`，说明 `:75-81`），支持命名/位置参数混用。`use daft_dsl::functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn}` 见 `chr.rs:11-14`。

### 5.3 子 crate 职责（同一模板：`struct XFunctions; impl FunctionModule`）

| crate | 职责 | 注册点 |
|---|---|---|
| `daft-functions` | 杂项核心：coalesce、concat_ws、slice、to_struct、uuid、hash、minhash、simhash、distance、similarity、numeric、float、random、length、monotonically_increasing_id | `src/daft-functions/src/lib.rs:4-26`、`:70` |
| `daft-functions-utf8` | 字符串算子（regexp_*、levenshtein、soundex、like/ilike、normalize，40+ 子模块） | `src/daft-functions-utf8/src/lib.rs:1-40+` |
| `daft-functions-list` / `-json` / `-temporal` / `-binary` / `-uri` / `-tokenize` | 列表算子 / JSON 读写 / 日期时间构造与算术 / encode-decode-compress / URL download-upload-parse / tiktoken 编解码（见 3.5） | `list/src/lib.rs:56`、`json/src/lib.rs:13`、`temporal/src/lib.rs:1-6`、`binary/src/lib.rs:13`、`uri/src/lib.rs:1-8`、`tokenize/src/lib.rs:10-13` |
| `daft-functions-serde` | **数据** serialize/deserialize（非计划序列化） | `src/daft-functions-serde/src/lib.rs:10-16` |
| `daft-geo` | 地理算子（great_circle_distance） | `src/daft-geo/src/lib.rs:1-6`、`:184` |
| `daft-minhash` / `daft-sketch` / `daft-hash` / `daft-algebra` | MinHash 相似度（`#![feature(portable_simd)]`）/ DDSketch 草图↔Arrow（`ARROW_DDSKETCH_DTYPE`）/ 可插拔 hasher / 表达式布尔代数化简 | `daft-minhash/src/lib.rs:1-5`、`daft-sketch/src/lib.rs:1-2`、`daft-hash/src/lib.rs:1-6`、`daft-algebra/src/lib.rs:1-4` |
| `daft-ext*` | 第三方 ABI 扩展（`#[daft_extension]`/`#[daft_func]`/`#[daft_func_batch]`），内建函数不用 | `src/daft-ext-macros/src/lib.rs:28/97/276` |

### 5.4 Python 侧映射：**手写**，按名字查 Rust 注册表

`daft/functions/__init__.py`（643 行）是手写 import 汇总，**无 codegen**（无 "DO NOT EDIT" 标记；`ls tools/` 无生成器脚本）。每条 Python 函数体只做一件事：把函数**名字符串** + 已转成表达式的参数交给 Rust 查表。

```python
# daft/expressions/expressions.py:445-449
def _call_builtin_scalar_fn(cls, func_name: builtins.str, *args: Any, **kwargs: Any) -> Expression:
    expr_args = [cls._to_expression(v)._expr for v in args]
    expr_kwargs = {k: cls._to_expression(v)._expr for k, v in kwargs.items() if v is not None}
    f = native.get_function_from_registry(func_name)
    return cls._from_pyexpr(f(*expr_args, **expr_kwargs))
```

实例：`daft/functions/binary.py:32 Expression._call_builtin_scalar_fn("encode", expr, codec=charset)`。`daft/functions/*.py` 中该调用点共 **221 处**，另有 `_call_builtin_agg_fn` 与 `_eval_expressions`（`expressions.py:451-455`）变体。Python 模块与 Rust crate 划分基本对齐（`daft/functions/{str,list,datetime,binary,url,spatial,image,...}.py`）。

---

## 6. 序列化与数据交换

**结论：`src/daft-serde` 不存在**（`ls src/` 无此目录）；`daft-functions-serde` 是数据算子（见 5.3）。序列化分五条互不相干的通路：

1. **类型/字面量层（serde derive）**：`DataType`、`Field`、`Schema`、`Literal`、`TableMetadata`、`TableStatistics`、`ScalarFn` 普遍 `#[derive(Serialize, Deserialize)]`（如 `src/daft-schema/src/dtype.rs:16`）。`DataType` 另有自定义 JSON `to_json()`/`from_json()`（`dtype.rs:877/882`），正是 `daft.super_extension` metadata 的载荷（`field.rs:142`）。
2. **Python 对象跨进程：vendored cloudpickle**。`pickle_dumps/pickle_loads` 调 `daft.pickle.dumps/loads`（`src/common/py-serde/src/python.rs:17-30`），该模块是**仓库内自带的 cloudpickle 副本**（`daft/pickle/{__init__,pickle,cloudpickle,cloudpickle_fast}.py`；`pyproject.toml:206` 显式把它排除在 lint 外）。
3. **物理计划/分区句柄跨语言：bincode**。宏 `impl_bincode_py_state_serialization!`（`src/common/py-serde/src/python.rs:93-...`）用 `bincode::serde::encode_to_vec(&self, config::legacy())` 生成 `__reduce__`（配 `_from_serialized`）。应用点：`PyDistributedPhysicalPlan`（`src/daft-distributed/src/python/mod.rs:234`）、`FlightPartitionRef/FlightPartitions`（`src/daft-partition-refs/src/flight.rs:37+`）、`ImageMode`（`src/daft-schema/src/image_mode.rs:4`）。
4. **分区数据跨进程**：Ray 用 `RayPartitionRef{object_ref: Arc<Py<PyAny>>, num_rows, size_bytes}`（`src/daft-partition-refs/src/ray.rs:13-17`）；Arrow Flight shuffle 用 `FlightPartitionRef{shuffle_id, server_address, partition_ref_id, num_rows, size_bytes}` + `FlightPartitions`（`src/daft-partition-refs/src/flight.rs:6-23`），依赖 `arrow-flight`（`src/daft-shuffles/Cargo.toml:4`，实现 `src/daft-shuffles/src/{client,server,oneshot_writer,shuffle_cache}.rs`）。
5. **UDF/actor 对象**：`PyObjectWrapper`（`src/common/py-serde/src/lib.rs:7`）+ `serialize_py_object`/`deserialize_py_object`（`python.rs:32/85`），用于 actor 句柄、placement group、limit counter：`src/daft-distributed/src/pipeline_node/actor_udf.rs:8,35,89`、`.../join/key_filtering_join.rs:46-48`、`.../limit.rs:30-41`、`src/daft-local-plan/src/plan.rs:529,555,2040,2055`。
6. 计划的**人类可读**表达走 `serde_json`（`src/daft-distributed/src/python/mod.rs:231 repr_json()`；`src/daft-logical-plan/src/display/json.rs`）。

---

## 7. 源码地图

| 文件 / 目录 | 职责 | 关键类型 |
|---|---|---|
| `src/daft-schema/src/dtype.rs` | Daft 类型系统唯一真源 | `DataType`(全量变体)、`to_arrow()`、`to_physical()`、`to_json()/from_json()` |
| `src/daft-schema/src/{field,schema}.rs` | 字段/列集合 + Arrow 扩展 metadata 落地 | `Field`、`FieldRef`、`EXTENSION_TYPE_REGISTRY`、`Schema`、`SchemaRef` |
| `src/daft-schema/src/{image_mode,image_format,media_type,time_unit,union_mode}.rs` | 类型参数枚举 | `ImageMode`、`ImageFormat`、`MediaType`、`TimeUnit`、`UnionMode` |
| `src/daft-core/src/datatypes/{mod,matching}.rs` | 类型级 marker + 数组别名 + **DataType 分发中枢** | `DaftDataType`/`DaftPhysicalType`/`DaftArrowBackedType`/`DaftLogicalType`/`NumericNative`、`NullArray`、10 个 `with_match_*_daft_types!` |
| `src/daft-core/src/array/mod.rs` + `{list,fixed_size_list,struct,union,extension,uuid}_array.rs` | 物理/嵌套/扩展数组 | `DataArray<T>`、`with_nulls`、`from_arrow`(含 coercion)、`ListArray`、`FixedSizeListArray`、`StructArray`、`UnionArray`、`ExtensionArray` |
| `src/daft-core/src/array/{image,tensor,file}_array.rs` + `array/ops/*` | 多模态数组视图 + 60+ 算子文件 | `ImageArray`+`ImageArraySidecarData`、`TensorArray`、`AsImageObj`(ops/image.rs)、`DaftListAggable`、`full.rs`、`repr.rs` |
| `src/daft-core/src/series/{mod,series_like}.rs` + `array_impl/*` + `ops/downcast.rs` | Series 抽象与宏批量实现 | `Series{inner: Arc<dyn SeriesLike>}`、`SeriesLike`(25 方法)、`ArrayWrapper<T>`、`IntoSeries`、`Series::downcast<Arr>()` |
| `src/daft-core/src/lit/` | 标量 | `Literal`、`from_lit.rs`、`python.rs` |
| `src/daft-recordbatch/src/lib.rs` + `ffi.rs` | 列式批（旧 `Table`）与 Arrow/PyArrow 互转 | `RecordBatch{schema,columns,num_rows}`、`GrowableRecordBatch`、`record_batch_to_arrow_rs` |
| `src/daft-micropartition/src/{micropartition,partitioning}.rs` | **执行期最小单元** + 分区集实现 | `MicroPartition{schema,chunks,metadata,statistics}`、`MicroPartitionRef`、`MicroPartitionSet` |
| `src/common/partitioning/src/lib.rs` | 分区抽象（避免循环依赖） | `Partition`、`PartitionRef`、`PartitionSet`、`PartitionSetRef`、`PartitionMetadata` |
| `src/daft-stats/src/{table_metadata,table_stats,partition_spec}.rs` | 元数据与统计 | `TableMetadata`、`TableStatistics`、`ColumnRangeStatistics`、`PartitionSpec` |
| `src/common/arrow-ffi/src/lib.rs` | **零拷贝 FFI 唯一入口** | `FromPyArrow`/`ToPyArrow`/`IntoPyArrow`、`array_to_pycapsules`、`record_batch_to_pycapsules` |
| `daft/datatype.py` / `daft/extension_type.py` / `daft/recordbatch/*.py` | Python 类型、`daft.super_extension` 注册、列式封装 | `DataType`、`DaftExtension`、`_ensure_registered_super_ext_type`、`RecordBatch`、`MicroPartition` |
| `src/daft-image/src/{lib,ops,series}.rs` + `functions/` / `src/common/image/src/cow_image.rs` | 10 个图片算子 / **图片编解码唯一实现** | `ImageFunctions`、rayon 元素级并行、`CowImage`（委托 `image` crate） |
| `src/daft-decoding/src/{deserialize,inference}.rs` | 字节→Arrow + 类型推断（SIMD） | `ByteRecordGeneric`、`deserialize_column`、`infer` |
| `src/daft-functions-tokenize/src/bpe.rs` / `src/daft-text/src/read.rs` / `src/daft-mcap/src/read.rs` | tiktoken 封装 / 文本文件扫描 / MCAP 容器解析 | `CoreBPE`、`stream_text`、`McapReader` |
| `src/daft-ai/src/{provider,python}.rs` / `daft/ai/**` / `daft/functions/ai/__init__.py` | provider 句柄桥（63 行）/ **AI 能力真正实现** / AI 用户入口 | `Provider`、`ProviderRef`、`PyProviderWrapper`、`Provider(ABC)`、`Descriptor`、`embed_text`/`prompt`/`classify_*` |
| `src/daft-dsl/src/functions/mod.rs` + `src/lib.rs:164-196` | **函数注册表** + 全仓唯一注册点 | `FunctionRegistry`、`FunctionModule`、`FUNCTION_REGISTRY`；16 × `register::<XFunctions>()` + 12 × `add_fn`/`add_async_fn` |
| `src/daft-ext-macros/src/lib.rs` | 第三方扩展宏（非内建路径） | `#[daft_extension]`、`#[daft_func]`、`#[daft_func_batch]` |
| `daft/expressions/expressions.py:445-455` | Python→Rust 函数调用桥 | `_call_builtin_scalar_fn`、`native.get_function_from_registry` |
| `src/common/py-serde/src/python.rs` + `daft/pickle/` | Python 对象序列化 + vendored cloudpickle | `pickle_dumps/loads`、`serialize_py_object`、`impl_bincode_py_state_serialization!` |
| `src/daft-partition-refs/src/{ray,flight}.rs` + `src/daft-shuffles/` | 跨进程分区句柄与 Arrow Flight shuffle | `RayPartitionRef`、`FlightPartitionRef`、`FlightPartitions` |
| `daft/io/lance/_lance.py` / `daft/recordbatch/recordbatch_io.py` | Lance 读写（外部包） | `read_lance`、`default_scan_options` 透传 |

### 能力缺口（源码级"未找到"汇总）

| 能力 | 状态 | 搜索关键词 |
|---|---|---|
| arrow2 / `Table` 类型 / `MaterializedResult`(Rust) / `daft-serde` crate | 均不存在（已迁 arrow-rs 59，旧 `Table`→`RecordBatch`） | `arrow2` in `*.toml`、`pub struct Table`、`MaterializedResult` in `src/`、`ls src/` |
| GPU 图片解码 / SIMD 图像 kernel / rotate / downsample | 未找到 | `nvjpeg`、`npp`、`cuda`、`simd`、`target_feature`、`std::arch`、`rotate`、`downsample` |
| 张量 matmul / 归一化 / torch 张量互操作 | 未找到（仅向量距离算子） | `matmul`、`normalize`（仅命中 `numeric/pmod.rs` 的 dtype 名） |
| PDF 解析 | 未找到（仅 MIME 嗅探 + 当字节喂模型） | `pdf`、`pypdf`、`pdfium`、`mutool`、`fitz` |
| Rust 侧音视频解码 | 未找到（Python: PyAV / soundfile / librosa） | `ffmpeg`、`decord`、`whisper`、`torchcodec`、`av` |
| 向量索引 / ANN 查询 | 未找到（外委 `daft-lance`） | `hnsw`、`vector_index`、`ivf`、`nearest`（Rust 侧仅 asof join） |
| Anthropic provider / Rust 侧 AI 并发原语 / Rust 侧 `OPENAI_API_KEY` | 未找到（并发全在 Python UDF 层） | `anthropic`、`Semaphore`、`OPENAI_API_KEY` in `*.rs` |
