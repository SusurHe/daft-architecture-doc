# AI Functions 与嵌入/向量检索（Daft @ dadd8a0）

> 证据相对 `D:\DOC\daft-src`。核心结论：**AI 函数全部是纯 Python 实现**（`daft/functions/ai/`），`src/daft-ai` 只是 63 行的 provider 句柄桥；
> **Lance 向量检索不在 Rust 侧**，是把参数透传给外部 `daft-lance` 包。

## daft-ai 结构与 provider 抽象

`src/daft-ai` 全部内容仅 3 个文件：`src/lib.rs`(7 行)、`src/provider.rs`(14)、`src/python.rs`(42)。
`src/daft-ai/Cargo.toml:1-7` 依赖只有 `pyo3 = {workspace = true, optional = true}`，feature 为 `python = ["dep:pyo3"]`
—— **无 tokio / reqwest / async-openai / daft-ext**。`src/daft-ai/src/lib.rs:1-7` 只导出 `provider` 与 `python` 两个模块。

`src/daft-ai/src/provider.rs:3-14` 的 Rust trait 只有 2 个方法，且自述"currently only used in the session"：

```rust
/// Provider implementation reference.
pub type ProviderRef = Arc<dyn Provider>;

/// Provider trait for interacting with providers in rust, which is currently only used in the session.
pub trait Provider: Sync + Send + std::fmt::Debug {
    /// Returns the provider name.
    fn name(&self) -> String;

    /// Creates (or extracts) a Python object that subclasses the Provider ABC.
    #[cfg(feature = "python")]
    fn to_py(&self, py: pyo3::Python<'_>) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>>;
}
```

`src/daft-ai/src/python.rs:9-38` 的 `PyProviderWrapper` 把 Python `Provider` ABC 实例包成 Rust trait object；唯一消费方是 daft-session：
`src/daft-session/src/session.rs:8` `use daft_ai::provider::ProviderRef;`、`:41` `providers: Bindings<ProviderRef>`、
`:136` `pub fn attach_provider(&self, provider: ProviderRef, alias: String)`、`src/daft-session/src/python.rs:38` `PyProviderWrapper::from(provider).arced()`。

**`daft-ext` / `#[daft_function]` 未找到**：`daft-ai` 不依赖 `daft-ext`（见上 Cargo.toml）。`daft-ext-macros` 的宏是
`#[daft_extension]`(`src/daft-ext-macros/src/lib.rs:28`)、`#[daft_func_batch]`(`:97`)、`#[daft_func]`(`:275`)，`daft-ai` 均未使用。
搜索关键词：`daft_function`、`daft_func`、`embed_text` 于 `*.rs`（0 命中）。

**真正的 provider 抽象是 Python 的 ABC + Protocol：**

| 抽象 | 位置 |
|---|---|
| `class Provider(ABC)`，方法 `get_text_embedder`/`get_image_embedder`/`get_image_classifier`/`get_text_classifier`/`get_prompter` | `daft/ai/provider.py:104`，`:123/:129/:135/:141/:147` |
| `PROVIDERS` 注册表 + `load_provider` | `daft/ai/provider.py:85-91`、`:94-97` |
| `TextEmbedder`/`ImageEmbedder`/`Prompter` 等 `Protocol` | `daft/ai/protocols.py:15`/`:18`、`:35`/`:38`、`:82`/`:85` |
| `Descriptor`（`get_provider`/`get_model`/`get_options`/`instantiate`/`get_udf_options`） | `daft/ai/typing.py:128-153` |

```python
ProviderType = Literal["google", "lm_studio", "openai", "transformers", "vllm-prefix-caching"]
PROVIDERS: dict[ProviderType, Callable[..., Provider]] = {
    "google": load_google,
    "lm_studio": load_lm_studio,
    "openai": load_openai,
    "transformers": load_transformers,
    "vllm-prefix-caching": load_vllm_prefix_caching,
}
```
（`daft/ai/provider.py:84-91`；解析顺序「显式实例→session 注册名→session 当前 provider→默认」见 `daft/functions/ai/__init__.py:43-64`）

**provider 实现文件清单**：`daft/ai/openai/provider.py:20`（`OpenAIProvider`，默认 `text-embedding-3-small`/`gpt-4o-mini`，`:23-24`）
+ `protocols/text_embedder.py`、`protocols/prompter.py`；`daft/ai/transformers/provider.py:33`（`TransformersProvider`，默认
`sentence-transformers/all-MiniLM-L6-v2`，`:37`）+ `protocols/` 下 5 个文件；`daft/ai/google/provider.py:19`（`GoogleProvider`，默认
`models/text-embedding-004`，`:22`）；`daft/ai/lm_studio/provider.py`（`LMStudioProvider`）+ `protocols/text_embedder.py`；
`daft/ai/vllm/provider.py:20`（`VLLMPrefixCachingProvider`）+ `protocols/prompter.py`。
**Anthropic provider 未找到**（`grep -rn anthropic daft/ src/` 仅命中 `daft/session.py:561` 的 docstring 举例）。

**API key 传入**：作为 provider 构造参数 `api_key`（`daft/ai/openai/typing.py:9-12`，即 OpenAI client 构造参数透传），
与 per-call 选项合并（`daft/ai/utils.py:64-76` `merge_provider_and_api_options`），最终 `AsyncOpenAI(**merged_provider_options)`
（`daft/ai/openai/protocols/text_embedder.py:220`）；provider 可用 `Session.attach_provider` 注入（`daft/session.py:219-231`）。
**Rust 侧读取 `OPENAI_API_KEY` 的代码未找到。**

## AI 函数实现（embed_text/prompt/classify）

全部在 `daft/functions/ai/__init__.py`，是**基于类 UDF（`daft.udf.cls` + `daft.method`）的纯 Python 实现**，无 Rust 实现体：
`embed_text` `:72`（签名 `(text, *, provider=None, model=None, dimensions=None, **options) -> Expression`，默认 provider `transformers` `:130`、
返回 dtype 取自 descriptor `:141`、`name_override="embed_text"` `:150`）；`embed_image` `:157`；`classify_text` `:250`；`classify_image` `:329`；
`prompt` `:453`（默认 provider `openai`，`:580`）。

```python
    text_embedder = _resolve_provider(provider, "transformers").get_text_embedder(model, dimensions, **options)

    udf_options = text_embedder.get_udf_options()

    # Choose synchronous or asynchronous call implementation based on the embedder
    is_async = text_embedder.is_async()
    call_impl = _TextEmbedderExpression._call_async if is_async else _TextEmbedderExpression._call_sync

    # Decorate the selected call method with @daft.method to specify return_dtype
    _TextEmbedderExpression.__call__ = method.batch(  # type: ignore[method-assign]
        method=call_impl,
        return_dtype=text_embedder.get_dimensions().as_dtype(),
        batch_size=udf_options.batch_size,
    )
```
（`daft/functions/ai/__init__.py:130-143`）

**与 daft-core Series/Array 的接口**：UDF 入参是 Python `Series`，经 `to_pylist()` 变为 `list[str]`/图像列表，输出 `list[Embedding]`（numpy 数组）：

```python
class _TextEmbedderExpression:
    def __init__(self, text_embedder: TextEmbedderDescriptor):
        self.text_embedder = text_embedder.instantiate()

    def _call_sync(self, text_series: Series) -> list[Embedding]:
        text = text_series.to_pylist()
        if not text:
            return []
        result = self.text_embedder.embed_text(text)
        assert isinstance(result, list)
        return result
```
（`daft/ai/_expressions.py:25-39`；5 个 `_*Expression` 分别在 `:25`、`:50`、`:75`、`:90`、`:108`）

Python 侧 `Embedding` 只是类型别名（`daft/ai/typing.py:156-164`，`TYPE_CHECKING` 下为 `np.typing.NDArray[Any]`）；
Rust dtype 由 `EmbeddingDimensions.as_dtype()` 构造（`daft/ai/typing.py:167-173`）。

## 并发与批处理

Rust 侧无任何并发设施（`src/daft-ai/Cargo.toml` 仅 pyo3），全部由 Python UDF 层控制。

- **batch size 常量**：OpenAI text embedder 默认 `batch_size=64, max_retries=3, on_error="raise"`
  （`daft/ai/openai/protocols/text_embedder.py:94-96`），token 预算 `batch_token_limit` 默认 `300_000`（`:163`）；
  transformers text `64`（`daft/ai/transformers/protocols/text_embedder.py:29`）、image `16`（`.../image_embedder.py:28`）、
  text classifier `64`（`.../text_classifier.py:38`）、image classifier `16`（`.../image_classifier.py:34`）。
- **并发度**：`UDFOptions.concurrency`（`daft/ai/typing.py:176-184`，`max_retries=3`）→ `daft_cls(max_concurrency=udf_options.concurrency, ...)`
  （`daft/functions/ai/__init__.py:144-151`）。**`Semaphore` 未找到**（`grep -rn Semaphore daft/ai/` 0 命中）。
- **async**：`is_async()` 基类默认 False（`daft/ai/protocols.py:29-31`），OpenAI 覆写为 True（`daft/ai/openai/protocols/text_embedder.py:158-159`），
  据此选择 `_call_async`/`_call_sync`（`daft/functions/ai/__init__.py:135-136`、`daft/ai/_expressions.py:41-47`）。
- **按 token 预算攒批 + 超长单条切分**（`daft/ai/openai/protocols/text_embedder.py:240-267`）；限流降级为并发单条：

```python
        except RateLimitError:
            # fall back to individual calls when rate limited
            # consider sleeping or other backoff mechanisms
            return await asyncio.gather(*(self._embed_text(text) for text in input_batch))
        except OpenAIError as ex:
            raise ValueError("The `embed_text` method encountered an OpenAI error.") from ex
```
（`daft/ai/openai/protocols/text_embedder.py:282-287`；`import asyncio` 见 `:3`；`AsyncOpenAI` 客户端 `:220`）

- **GPU 并发**：`get_gpu_udf_options()` 按可见 GPU 数设置 concurrency/num_gpus（`daft/ai/utils.py:35-55`）。

## Embedding 类型与物理表示

`DataType::Embedding(Box<Self>, usize)` 定义于 `src/daft-schema/src/dtype.rs:123-125`，**物理类型就是 FixedSizeList**（`dtype.rs:377`）：

```rust
    // Non-ArrowTypes:
    /// A logical type for embeddings.
    Embedding(Box<Self>, usize),
```
（`src/daft-schema/src/dtype.rs:123-125`）

```rust
            Embedding(dtype, size) => FixedSizeList(Box::new(dtype.to_physical()), *size),
```
（`src/daft-schema/src/dtype.rs:377`）

`src/daft-core/src/datatypes/mod.rs:257` `impl_daft_logical_fixed_size_list_datatype!(EmbeddingType, Unknown);`，宏体设置
`type PhysicalType = FixedSizeListType;`（同文件 `:131-149`）；数组别名 `EmbeddingArray = LogicalArray<EmbeddingType>`
（`src/daft-core/src/datatypes/logical.rs:192`）。显示名 `Embedding[{dtype}; {size}]`（`dtype.rs:205`）；Python 构造
`DataType.embedding(dtype, size)`（`daft/datatype.py:623-632`）；落地 Arrow 走 physical + extension metadata（`src/daft-schema/src/field.rs:126-137`）。

**与 Tensor 的区别**（`src/daft-schema/src/dtype.rs`）：

```rust
    Tensor(Box<Self>),                                  // :134 变长
    FixedShapeTensor(Box<Self>, Vec<u64>),              // :137
...
            Tensor(dtype) => Struct(vec![
                Field::new("data", List(Box::new(*dtype.clone()))),
                Field::new("shape", List(Box::new(Self::UInt64))),
            ]),
            FixedShapeTensor(dtype, shape) => FixedSizeList(
                Box::new(*dtype.clone()),
                usize::try_from(shape.iter().product::<u64>()).unwrap(),
            ),
```
（`src/daft-schema/src/dtype.rs:392-399`；`TensorType` 物理为 Struct，见 `src/daft-core/src/datatypes/mod.rs:252`）

即 `Embedding` = 无 shape 元数据的定长扁平向量（`fixed_size_list<f32>` 语义）；`Tensor` = 带 `shape` 字段的 Struct，仅 `FixedShapeTensor` 才落 FixedSizeList。
`EmbeddingArray` 渲染为 sparkline（`src/daft-core/src/array/ops/repr.rs:354-368`），排序未实现：`:672-674` `todo!("impl sort for EmbeddingArray")`。
Rust 原生向量算子 `cosine_distance`（`src/daft-functions/src/distance/cosine.rs:37-39`，另有 `dot_product.rs`/`euclidean.rs`），
输入校验同时接受 `FixedSizeList` 与 `Embedding`（`src/daft-functions/src/vector_utils.rs:17-22`），inner dtype 限 `Int8|Float32|Float64`（`:29-37`）。

## Lance 集成与向量检索能力边界

**daft-io 里没有 lance 相关代码**：`grep -rni lance src/daft-io/` 仅命中 `src/daft-io/src/opendal_source.rs:67` 注释中的单词 "balance"（无关）。
Rust 侧无 lance 依赖；唯一声明在 `pyproject.toml:47` `lance = ["daft-lance>=0.5.0,<0.6.0"]`。

- **读**：`daft/io/lance/_lance.py:12` `_daft_lance = LazyImport("daft_lance._lance")`，`read_lance` 定义 `:20-37`，实现委托 `:124-140`。
- **写**：`daft/dataframe/dataframe.py:2424` `def write_lance`（`:2522-2524` `from daft_lance import ... write_lance as _write_lance`）；
  底层 `daft/recordbatch/recordbatch_io.py:478-496` 直接 `import lance` 并调 `lance.fragment.write_fragments(...)`。

**向量检索能力边界**：daft **自身没有**向量索引/最近邻 API（自有算子中无 `nearest`/`search`/`vector_index`/`hnsw`/`ivf`）。
能力来自把参数**透传给 Lance 的 scanner** —— `default_scan_options` 文档写明 "This accepts the same arguments described in
`lance.LanceDataset.scanner`"（`daft/io/lance/_lance.py:70-76`）。用法证据：

```python
    # Query is closest to [1.0, 0.0] (id=1).
    query = pa.array([1.0, 0.0], type=pa.float32())
    nearest = {"column": "vector", "q": query, "k": k}

    df = daft.read_lance(dataset_path, default_scan_options={"nearest": nearest})
    result = df.select("id").to_pydict()
```
（`tests/io/lancedb/test_lancedb_vector_search.py:76-81`；同文件 `:118-128` `metric="cosine"`、`:131-146` `nprobes`/`refine_factor`、`:233-252` `prefilter`）

- **索引创建也不在 daft**：测试直接调用 Lance Python API `ds.create_index("vector", "IVF_PQ", num_partitions=2, num_sub_vectors=1)`
  （`tests/io/lancedb/test_lancedb_vector_search.py:159-164`）。
- Rust 侧 `nearest` 命中全部属于 asof join（`src/daft-core/src/join.rs:196`、`src/daft-local-execution/src/join/asof_join.rs:266`）；
  `hnsw`/`vector_index` 在 `src/**/*.rs` **未找到**；`ivf` 仅出现在 `daft/io/lance/_lance.py:63-68` 的 index cache 文档字符串（描述 Lance 的 IVF_PQ 页大小）。

**边界结论**：Daft 提供的是「`Embedding` 逻辑类型 + Rust 距离算子 + Lance 格式读写 + `default_scan_options` 参数透传」。
ANN 索引构建与近邻查询由外部 `daft-lance`/`lance` 执行，Daft 不做索引管理，也没有跨格式（如 parquet）的向量检索 API。
Embedding 的 Lance 往返读写有测试覆盖（`tests/io/test_roundtrip_embeddings.py:15` `FMT = Literal["parquet", "lance"]`，`:48` 参数化）。

## Python 侧 daft/ai

文件清单（行数）：`__init__.py`(10)、`_expressions.py`(117)、`metrics.py`(26)、`protocols.py`(91)、`provider.py`(155)、`typing.py`(187)、`utils.py`(120)；
子包：`google/`（`provider.py`+`protocols/prompter.py`）、`lm_studio/`（`provider.py`+`protocols/text_embedder.py`）、
`openai/`（`provider.py`、`typing.py`+`protocols/prompter.py`、`protocols/text_embedder.py`）、
`transformers/`（`provider.py`+`protocols/` 下 `image_classifier.py`、`image_embedder.py`、`prompter.py`、`text_classifier.py`、`text_embedder.py`）、
`vllm/`（`provider.py`+`protocols/prompter.py`）。

**是纯 Python 实现，不是调用 Rust 暴露的 AI 函数**（Rust 侧不存在 `embed_text`/`prompt`/`classify` 符号）。
`daft/ai/__init__.py:1-10` 只导出 `Embedding`、`Provider` 两个名字；面向用户入口是 `daft/functions/ai/__init__.py`，
并在 `daft/functions/__init__.py:3` 重导出 `classify_image, classify_text, embed_text, embed_image, prompt`。

**唯一的 Rust 交互路径**是 vLLM prompt：不走 UDF，直接构造 Rust 表达式（`daft/functions/ai/__init__.py:590-604`）：

```python
    if isinstance(prompter_descriptor, VLLMPrefixCachingPrompterDescriptor):
        if return_format is not None:
            raise ValueError("return_format is not supported for vLLM provider")

        if system_message is not None:
            raise ValueError("system_message is not supported for vLLM provider")

        if isinstance(messages, list):
            raise ValueError("vLLM provider does not support multiple messages")

        vllm_options = prompter_descriptor.get_options()
        return Expression._from_pyexpr(
            messages._expr.vllm(
                prompter_descriptor.model_name,
                vllm_options["concurrency"],
                vllm_options["gpus_per_actor"],
```
（对应 Rust 侧 `src/daft-dsl/src/python.rs:811` `pub fn vllm(`、`src/daft-dsl/src/expr/mod.rs:305`；vLLM 选项默认值见 `daft/ai/vllm/protocols/prompter.py:49-56`）

token 用量指标由 `daft/ai/metrics.py:6-26` 经 `daft.udf.metrics.increment_counter` 记录。
