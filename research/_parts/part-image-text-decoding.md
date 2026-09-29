# Daft 图片/文本/解码 源码调研（D:\DOC\daft-src, commit dadd8a0）

## 图片 (daft-image)

**模块**：`src/daft-image/src/lib.rs:1-6` 仅 6 项；`ls src/daft-image/src/` = `counting_writer.rs / functions/ / iters.rs / lib.rs / ops.rs / series.rs`。
`image_decoders.rs`、`image_formats.rs` **未找到**（`ls` 与 `grep -rn "image_decoders\|image_formats" src/` 均无匹配）；格式枚举在 `daft-schema`。
**依赖**：`src/daft-image/Cargo.toml:9,14,18,19` → `common-image`、`image`、`rayon`、`rustfft`（无其他图像库）。
workspace：`Cargo.toml:326 image = "0.25.10"`、`:353 rayon = "1.12.0"`、`:387 rustfft = "6"`；`image` 特性 `src/common/image/Cargo.toml:3-13`（gif/jpeg/ico/png/tiff/webp/bmp/hdr）。
`grep -rn -E "fast_image_resize|imageproc|nvjpeg|turbojpeg|libjpeg|mozjpeg" .` → **无匹配**。
**ImageFormat（5 种）**：`src/daft-schema/src/image_format.rs:17-23` = PNG / JPEG / TIFF / GIF / BMP（迭代器 `:45-50`）。
**GPU 解码：未找到。** `grep -rn --include=*.rs -E "cuda|nvjpeg|npp|gpu|Cuda|NvJpeg" src/` 命中 193 处，全为资源调度/vLLM
（`src/common/resource-request/src/lib.rs:23`、`src/daft-dsl/src/expr/mod.rs:315 gpus_per_actor` 等）；`grep -rn -iE "nvjpeg|\.cuda\(|torchvision" daft/` 仅 `daft/ai/transformers/provider.py:60`（模型推理）。
**解码/编码统一走 `image` crate**：`src/common/image/src/cow_image.rs:109-113` `decode` 调用 `image::load_from_memory(bytes)`（`:110`）；
编码 `cow_image.rs:115-132` 调用 `image::write_buffer_with_format(...)`（`:119-126`）。无自研 codec。
**kernel = `image::imageops::*` + rayon 行级并行**（非自研逐像素）：
- resize `src/common/image/src/cow_image.rs:147-172`，`image::imageops::resize(..., FilterType::Triangle)`（`:152,157,162,167`），仅 L/LA/RGB/RGBA，其余 `_ => unimplemented!`（`:170`）。
- crop `cow_image.rs:174-201`，`image::imageops::crop_imm(...).to_image()`（`:181`；`:177` 用 `unsafe transmute` 规避生命周期）。
- rayon 并行点 `src/daft-image/src/ops.rs:316-325`（resize，`:322`）、`:327-343`（crop，`:336-337`）、`:94-100`（to_mode）、`:990-1000`（hash_images）。

```rust
// src/daft-image/src/ops.rs:321-324
    (0..images.len())
        .into_par_iter()
        .map(|i| images.as_image_obj(i).map(|img| img.resize(w, h)))
        .collect()
```

**SIMD：未找到**（`grep -rn -i "simd\|target_feature\|std::arch" src/daft-image/src/ src/common/image/src/` → 无匹配）。
**rotate：未找到**（`grep -rn "rotate\|rotation" src/ --include=*.rs` 只命中 writer 轮转 `src/daft-writers/src/file.rs:11,69` 与 optimizer）。
**downsample：无独立 kernel**，phash 内部用 Triangle 缩放代替（`src/daft-image/src/ops.rs:433-435`）；`rustfft` 只服务 phash 的 DCT（`ops.rs:3-8` thread_local `FftPlanner`、`ops.rs:951 dct1d_fft`）。
**UDF 清单**：`src/daft-image/src/functions/mod.rs:17-29` = crop、image_decode、decode_image_file、image_encode、image_file_metadata、image_hash、image_resize、to_tensor、to_mode、image_attribute。

## ImageMode 与物理表示

**定义位置**：`src/daft-schema/src/image_mode.rs:38-49`（既不在 daft-image 也不在 daft-core）；`Display`/`FromStr` 在 `:137-161`，`from_pil_mode_str` `:71-93`，`num_channels` `:114-123`，`iterator` `:124-130`。

```rust
// src/daft-schema/src/image_mode.rs:38-49
pub enum ImageMode {
    L = 1,
    LA = 2,
    RGB = 3,
    RGBA = 4,
    L16 = 5,
    LA16 = 6,
    RGB16 = 7,
    RGBA16 = 8,
    RGB32F = 9,
    RGBA32F = 10,
}
```

**与 DataType::Image 的关系**：mode 是 dtype 的类型参数 —— `src/daft-schema/src/dtype.rs:128 Image(Option<ImageMode>)`、`:131 FixedShapeImage(ImageMode, u32, u32)`；
`dtype.rs:936-939 image_mode()` 取回 mode；`image_mode.rs:94-112 try_from_num_channels` 由 (通道数, dtype) 反推。
**物理类型：`Image` → Struct，`FixedShapeImage` → FixedSizeList**（`dtype.rs:378-391`，节选）：

```rust
// src/daft-schema/src/dtype.rs:378-387
            Image(mode) => Struct(vec![
                Field::new(
                    "data",
                    List(Box::new(mode.map_or(Self::UInt8, |m| m.get_dtype()))),
                ),
                Field::new("channel", UInt16),
                Field::new("height", UInt32),
                Field::new("width", UInt32),
                Field::new("mode", UInt8),
            ]),
```

`FixedShapeImage` 在 `dtype.rs:388-391` 定义为 `FixedSizeList(mode.get_dtype(), num_channels*height*width)`。
**daft-core 表示**：`src/daft-core/src/datatypes/logical.rs:189 pub type ImageArray = LogicalArray<ImageType>;`、`:196 FixedShapeImageArray`；
`:183-184 LogicalArrayImpl<L, PhysicalArray>` 即逻辑数组包物理 StructArray；`src/daft-core/src/datatypes/mod.rs:251`
`impl_daft_logical_data_array_datatype!(ImageType, Unknown, StructType)`、`:258 FixedShapeImageType`。
列索引常量 `src/daft-core/src/array/image_array.rs:21-25`（data=0/channel=1/height=2/width=3/mode=4），sidecar `:12-18`，构造 `:59-84 from_list_array`（`:80-83` 按 `to_physical()` 建 StructArray）。
**ops 接口**：`src/daft-core/src/array/ops/image.rs:11-15 pub trait AsImageObj`；`:26-52 as_image_obj` 按 offsets 零拷贝构造 `CowImage`（`:40 Cow::Borrowed`、`:45 ImageMode::from_u8`）；
回写 `ops/image.rs:96-155 image_array_from_img_buffers`、`:157-195 fixed_image_array_from_img_buffers`。
**错误处理：没有 `ImageError`**（`grep -rn "ImageError\|ImageDecodeError" src/` → 无匹配），统一 `common_error::DaftError`（`src/common/error/src/error.rs:7`）：
解码 `cow_image.rs:112 DaftError::ValueError`、编码 `cow_image.rs:127-131`、参数校验 `src/daft-image/src/functions/decode.rs:57,84-87 TypeError`。
**能力边界**：`cow_image.rs:48-56 from_raw` 只支持 4 种 8-bit 模式（`:51-54` unwrap），`ops/image.rs:119` 对非 L/LA/RGB/RGBA 直接 `assert!` → DataFrame 只能存 8-bit 四模式，与 `image_mode.rs:15-18` 注释一致。

## 文本 (daft-text)

**daft-text 不做 tokenize**，它是文本**文件读取** crate：`src/daft-text/src/lib.rs:1-4` 仅 `options`/`read`；`src/daft-text/Cargo.toml` 无 tokenizer 依赖。
入口 `src/daft-text/src/read.rs:50-56 stream_text`，产出单列 Utf8（`:69`），只接受 UTF-8（`:57-63`）；调用点 `src/daft-local-execution/src/sources/scan_task_reader.rs:14,365`。
**真正的 tokenize crate = tiktoken-rs**，在 `daft-functions-tokenize`：`src/daft-functions-tokenize/Cargo.toml:15 tiktoken-rs`（workspace `Cargo.toml:369 tiktoken-rs = "0.9.1"`）；
`src/daft-functions-tokenize/src/bpe.rs:9 use tiktoken_rs::CoreBPE;`，内建词表 `bpe.rs:91-100`（cl100k_base / o200k_base），文件加载 `bpe.rs:146`，`CoreBPE::new` `bpe.rs:174`，
编码 `bpe.rs:218 pub fn encode(&self, s: &str, use_special: bool) -> Vec<u32>`；注册 `src/daft-functions-tokenize/src/lib.rs:10-13`（tokenize_encode / tokenize_decode）。
**Arrow 表示 = `list<u32>`**（不是 list<u16>）：

```rust
// src/daft-functions-tokenize/src/encode.rs:76-79
        Ok(Field::new(
            input.name,
            DataType::List(Box::new(DataType::UInt32)),
        ))
```

落盘构造 `encode.rs:93-114`（`UInt32Builder` + `OffsetBuffer` → `ListArray`）；反向 decode 接受任意整型 list 并 cast 到 UInt32（`decode.rs:63-67`、`:79-89`），返回 `DataType::Utf8`（`decode.rs:67`）。

## daft-decoding

**职责：把字节/字符串反序列化为 Arrow 数组 + schema 推断，服务 CSV/JSON/scan**；与 HuggingFace datasets、视频帧无关。
自述 `src/daft-decoding/src/lib.rs:1-3`；依赖 `src/daft-decoding/Cargo.toml`：`atoi_simd 0.16.1`、`fast-float2 0.2.3`、`simdutf8`、`csv`、`csv-async`、`chrono`（无 datasets/video）。
关键 API：`deserialize.rs:38 pub trait ByteRecordGeneric`、`:277 deserialize_bytes_to_array`、`:483 deserialize_column`、`:499 deserialize_single_value_to_arrow`、时间格式常量 `:13-30`；
SIMD UTF-8 校验 `deserialize.rs:57-59 simdutf8::basic::from_utf8`；推断入口 `inference.rs:23`。

```rust
// src/daft-decoding/src/inference.rs:23-31
pub fn infer(bytes: &[u8]) -> DataType {
    if is_null(bytes) {
        DataType::Null
    } else if is_boolean(bytes) {
        DataType::Boolean
    } else if is_integer(bytes) {
        DataType::Int64
    } else if is_float(bytes) {
        DataType::Float64
```

（其余分支 `inference.rs:32-37`：utf8 → `infer_string`，否则 `DataType::Binary`。）
消费方（`grep -rln "daft_decoding" src/`）：`src/daft-csv/src/{local.rs,metadata.rs,read.rs}`、`src/daft-json/src/{decoding.rs,inference.rs}`、`src/daft-scan/src/hive.rs` —— 无 video/audio 消费方。

## 音频/视频/文档能力边界

**结论：Rust 侧仅有类型与 MIME 识别，无任何编解码实现；实际能力全由 Python 外部库（PyAV / soundfile / librosa）提供。**
`grep -rn --include=*.rs -iE "ffmpeg|decord|whisper|pypdf|pdfium|mutool|librosa|torchcodec|pyav" src/` → **无匹配**。
Rust 侧证据：`src/daft-schema/src/media_type.rs:9-15`、`src/daft-file/src/file.rs:406 const PDF_MAGIC`、`:462-463` PDF MIME、`:467-476` audio/video MIME。

```rust
// src/daft-schema/src/media_type.rs:9-15
pub enum MediaType {
    Unknown,
    Video,
    Audio,
    Image,
    Hdf5,
}
```

**视频 = Python + PyAV**：`daft/io/av/_read_video_frames.py:10 import av`、`:60` 提示 `pip install av`、`:139 av.open(file)`、`:159 container.decode(stream)`；
入口 `daft/io/av/__init__.py:25 def read_video_frames(...)`，缺依赖抛 `ImportError`（`__init__.py:92`）；属 Python DataSource（`_read_video_frames.py:16 from daft.io import DataSource`）。
**音频 = Python + soundfile / librosa**：`daft/file/audio.py:6 from daft.dependencies import librosa, np, sf`、`:94 librosa.resample(...)`；
`daft/functions/audio.py:15-18 get_metadata_impl`（`file.metadata()`）、`:94/:166` 要求 soundfile、`:171-173` 要求 librosa；`daft/dependencies.py:12,34` LazyImport。
**PDF：完全没有解析能力。** `grep -rn --include=*.rs -i "pdf" src/` 仅 4 处，全在 `src/daft-file/src/file.rs` 的 MIME 嗅探（`:406,462-463,525`）；
`grep -rn --include=*.py -i "pdf" daft/` 命中多为 pandas DataFrame 变量名（如 `daft/io/_sql.py:224,346`），语义相关仅 MIME 说明 `daft/functions/file_.py:137` 与把 PDF 当**字节**喂模型 `daft/functions/ai/__init__.py:468`。
**未找到** pypdf/pdfium/mutool/PyMuPDF 任何引用 → 抽取 PDF 文本只能靠 Python UDF 外挂。
