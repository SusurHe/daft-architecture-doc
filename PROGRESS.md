# 开发进度记录（Daft 统一接入口）

> 目标：把 [`Daft统一接入口设计.md`](Daft统一接入口设计.md) 落地为可评审的补丁——**零破坏、可分批合并**。
> 代码仓库：fork `SusurHe/Daft`，分支 `feat/unified-storage-provider-api`（基线 `dadd8a0b2`，上游 `main` 未被触碰）。
> 维护约定：**每完成一个阶段追加一条记录**，写明 commit、验证命令、发现的问题与遗留项。

## 状态总览

| 阶段 | 内容 | 状态 | 证据 |
|---|---|---|---|
| **P0** | 契约层（能力/类型映射/写形态/提交协议）、注册表、内置 provider、统一 handle、选项契约、残余协商 | ✅ 完成 | `a6faef98`、`f529e910`、`0cf89f64`；47 用例 |
| **P1-a** | 公开入口：导出 `daft.open` / `daft.storage` | ✅ 完成 | `280f0ae6`；50 用例 |
| **P1-b** | 内置读入口（parquet/csv/json/avro/text）改为 `STORAGE × FORMAT` 两次解析 | ⏳ 待做 | — |
| **P1-c** | 内置写入口改为 `handle.sink()` → `NativeTabularSink` | ⏳ 待做 | — |
| **P2** | 数据湖 provider：Iceberg 复用 parquet 的 FORMAT 实现 + 自身元数据逻辑 | ⏳ 待做 | — |
| **P0.5** | 一致性套件（capability 诚实性、选项契约、协商不丢项、类型映射自洽） | ✅ 完成 | `68ec1c27`；74 用例 |
| **P3-a** | ClickHouse provider（读 `read_sql` / 写既有 sink / `APPEND_ONLY` 声明）+ DB 类 URI 在 L3 终止 | ✅ 完成 | `32c76b3b`；66 用例 |
| **P3-b** | MongoDB provider、修 `PostgresCatalog.append` 的内联 COPY、`DataWriter/Committer` 接入执行层 | ⏳ 待做 | — |
| **P4** | 收敛：新后端只接受 provider 形式、`write_*` 弃用、能力矩阵文档自动化 | ⏳ 待做 | — |

图例：✅ 完成 · 🔄 进行中 · ⏳ 待做 · ⚠️ 部分实现

---

## 阶段记录

### P0 · 契约层与内置 provider（完成）

**提交**

| commit | 内容 | 规模 |
|---|---|---|
| `a6faef98` | `feat(storage): add provider contract layer` | 5 文件 / +992 |
| `f529e910` | `feat(storage): add provider registry, built-in providers and unified handle` | 4 文件 / +1304 |
| `0cf89f64` | `test(storage): cover the provider layer end to end` | 6 文件 / +530 |

**新增文件**：`daft/storage/`（`contracts.py`、`options.py`、`residual.py`、`negotiation.py`、`registry.py`、`providers.py`、`handle.py`、`errors.py`、`__init__.py`）+ `tests/storage/`（6 个测试文件）。**未修改任何现有文件**。

**验证**

```bash
pytest tests/storage -q                    # 47 passed
ruff check daft/storage tests/storage      # All checks passed
ruff format --check daft/storage tests/storage   # 15 files already formatted
```

**关键决策**

1. **能力分两层**：粗粒度 `TableCapability`（规划与快速失败）+ 细粒度 `Supports*` mixin（协商，`isinstance` 即能力判定），避免"声明与实现漂移"。
2. **下推返回残余**：`Residual(accepted, remaining)`，引擎负责重算残余算子——把 Daft 内部既有的 filter 协商协议（`src/daft-scan/src/pushdowns.rs`）推广到全部下推类型。
3. **写形态保留三条既有路径**：`NativeTabularSink` → `write_tabular`、`CatalogSink` → 表协议提交、`PythonDataSink` → `write_sink`；不新开写通道，避免把 Parquet/CSV 的原生 writer 挤到 Python sink 造成性能回归。
4. **`location_source` 显式化**：区分"表需要 location"与"用户需要指定 location"（Iceberg 属后者为否的典型）。
5. **内置注册懒加载**：`ensure_builtins()` 在首次查询时注册，保证全新解释器可用；`reset(load_builtins=False)` 的显式清空不会被懒加载覆盖。

**过程中发现并修复的缺陷**

| # | 问题 | 影响 | 修复 |
|---|---|---|---|
| 1 | `OptionsContract.required` 与 `Option.required` 冗余 | 必填校验形同失效 | 契约侧权威：`declared` 会把 `required` 里的选项标记为必填 |
| 2 | `Option.type` 默认 `str` | 对象型选项（如 `source=DataSource()`）被误拒 | `type=None` 表示不做类型校验 |
| 3 | 注册表在全新进程中为空 | `import daft; daft.open(...)` 直接抛 `ProviderNotFoundError` | `ensure_builtins()` 懒加载 + 子进程回归测试 |

**遗留项**：`report_statistics` / `report_partitioning` 仅定义协议未接入；`DataWriter/Committer/GlobalCommitter` 仅协议未接入执行层（随 P3 落地）。

### P1-a · 公开入口导出（完成）

**提交**：`280f0ae6` — `feat(storage): expose daft.open and daft.storage`

**变更**

- `daft/__init__.py`：`from daft import storage` + `from daft.storage import open_uri as open`（与文件内既有的 `range = _range` 重绑定风格一致），并把 `open`、`storage` 加入 `__all__`；
- `tests/storage/test_public_api.py`：别名一致性、`daft.open` 端到端读写、未知格式的可行动报错（3 用例）。

**验证**

```bash
pytest tests/storage -q                    # 50 passed
ruff check daft/storage tests/storage      # All checks passed
```

**说明**：`daft/__init__.py:16` 存在一条**既有** `BLIND-EXCEPT` 告警（对改动前的版本执行同一检查同样报出），不在本阶段范围内，未做修改以免扩大 PR 差异。

### P3-a · ClickHouse provider 与 DB 类后端的分层（完成）

**提交**：`32c76b3b` — `feat(storage): add the ClickHouse provider and let database URIs resolve at the catalog layer`

**变更**

- `daft/storage/registry.py`：注册为 `DATABASE` / `TABLE_FORMAT` 的 scheme **直接在 L3 终止**（无文件系统、无格式、`location_source=NONE`）；`ResolvedSource` 增加可选 `direct_provider` 与可空 `format_name`/`storage_key`/`location`；
- `daft/storage/handle.py`：provider 选择改为按协议（`_provider_for`），使单个数据库 provider 同时承载 scan 与 sink；`describe()` 支持"无 location"；
- `daft/io/clickhouse/provider.py`（新增）：`ClickHouseProvider`（`DATABASE`、`APPEND_ONLY`、类型映射三态）+ `read_clickhouse()` / `write_clickhouse()` 便捷入口 + URI 解析（host/port/user/password/database/table，选项覆盖 URI）；
- `daft/io/clickhouse/__init__.py`：改为惰性导入驱动（`__getattr__`），使 provider 注册**不依赖** `clickhouse-connect` 是否安装；公开名 `ClickHouseDataSink` 保持不变；
- `daft/storage/providers.py`：注册 ClickHouse provider。

**设计验证**：这次改动顺带证伪了 v3 的一处早期写法——DB 类后端**不该走"格式轴"解析**（`clickhouse://host/db/table` 没有扩展名，旧逻辑会抛 `AmbiguousFormatError`）。四层模型的"DB 类在 L3 终止"由此从文档变成代码约束。

**验证**

```bash
pytest tests/storage -q                    # 66 passed（新增 16 例）
ruff check daft/storage daft/io/clickhouse tests/storage   # All checks passed
ruff format --check ...                    # 全绿
```

**过程中发现并修复的缺陷**

| # | 问题 | 影响 | 修复 |
|---|---|---|---|
| 4 | `parse_uri` 把 `user:pass@host:8123` 当成路径 | 表名解析成 `user:pass@host:8123.analytics.events` | 新增 `parse_target()`，用 `urlsplit` 分离 authority 与 path |
| 5 | sink 构造缺 `host` 参数 | 即使 URI 里带 host 也会 `TypeError` | 从 URI 解析连接参数，选项优先，并在导入可选驱动**之前**校验 |
| 6 | 表名与 database 重复限定 | `insert_df(table="analytics.events", database="analytics")` 语义不确定 | sink 传裸表名 + database；SQL 查询侧才用 `database.table` |
| 7 | 缺省依赖提示测试与实现冲突 | 注册 ClickHouse 后旧用例仍断言"未注册" | 该用例改用仍未注册的 `iceberg://` scheme |


### P0.5 · 一致性套件（完成）

**提交**：`68ec1c27` — `feat(storage): add a conformance kit for third party providers`

**目的**：把设计里"声明必须是事实来源"从口头规约变成**机器可检查**：第三方 provider 可以在自己的 CI 里跑同一套检查。

**内容**（`daft/storage/conformance.py` + 8 个用例）

| 检查 | 断言 |
|---|---|
| 身份 | `info.name`/`keys` 非空、key 唯一且小写 |
| 选项契约 | 无重复声明、每个选项必须有 doc |
| 能力诚实性 | 声明 `BATCH_WRITE` ⇒ 必须有 `write_protocol` 且实现 `sink()`；声明 `BATCH_READ` ⇒ 必须实现 `scan()`（V1 fallback 除外） |
| 类型映射自洽 | 支持值必须是 `DTypeSupport`；出现 `SERIALIZE` 时不得声明 `non_primitive="error"` |
| 协商契约 | 一轮协商**不得丢项**（`accounts_for_all`）；实现了 `push_projection` 就必须记录列 |

**验证**

```bash
pytest tests/storage -q     # 74 passed
ruff check / format --check # 全绿
```

内置 provider（parquet / csv / clickhouse）全部通过；测试里另有 4 个"故意违规"的 provider 用于证明检查会真的报错。


---

## 环境与阻塞

| 项 | 情况 | 影响与对策 |
|---|---|---|
| Rust 工具链 | 本机无 `cargo`/`rustc`/`maturin`/`uv`，checkout 未构建扩展 | 无法直接 `import daft`（仓库源码比 PyPI 0.7.25 新，扩展缺 `AvroSourceConfig` 等符号） |
| 可用运行时 | PyPI `daft==0.7.25`（有 Windows wheel）；nightly 索引无 win 产物 | 本地验证方式：把 `daft/storage/` 覆盖进 0.7.25 运行时的 site-packages，并把 `daft.open` 导出补丁同步到已安装包，然后跑 `tests/storage` |
| 结论 | 新层只依赖稳定公开 API（`read_parquet`/`read_csv`/`write_*`/`DataFrame.where/select/limit/offset`） | 在标准构建环境可直接 `pytest tests/storage -q`；但 **P1-b/P1-c 改的是现有入口，必须先能本机构建**，否则无法用 `tests/io/**` 做逐字等价回归 |
| 网络 | 该链路对 SSH 压缩不友好，大包推送会 `send-pack: unexpected disconnect` | 仓库本地已配置 `core.sshCommand "ssh -o Compression=no -o ServerAliveInterval=15"`；必要时按提交逐个推送 |

## 推送与链接

- 分支：<https://github.com/SusurHe/Daft/tree/feat/unified-storage-provider-api>
- 开 PR：<https://github.com/Eventual-Inc/Daft/compare/main...SusurHe:feat/unified-storage-provider-api?expand=1>
- PR 描述草稿：[`upstream/PR_BODY.md`](upstream/PR_BODY.md)
- issue 草稿（上游要求先有被批准的 issue，见 `CONTRIBUTING.md:34`）：[`upstream/ISSUE_DRAFT.md`](upstream/ISSUE_DRAFT.md)

## 下一步

1. **P1-b/P1-c**（需要可构建环境）：读入口改双轴解析、写入口改 `handle.sink()`，用 `tests/io/**` 与计划形状做逐字对拍；
2. **P3-a**：`ClickHouseProvider`（读走 `read_sql`、写走既有 `ClickHouseDataSink`、声明 `APPEND_ONLY`）+ `read_clickhouse` 别名；无需真实 CH 即可单测 sink 构造与选项契约；
3. **一致性套件**：把"协商永不丢项""未知选项必须报错""声明 true 就必须真的下推"固化为可被第三方复用的 conformance 检查。
