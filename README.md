# Daft 架构深度剖析

面向 **Daft**（Eventual 开源的多模态数据引擎）的源码级架构剖析与设计讨论。

- 基准版本：`Eventual-Inc/Daft` `main` @ `dadd8a0`（2026-09-25，对应 v0.7.25 线）
- 方法：按子系统分头做源码调研并保留 `文件:行号` 证据，再据此撰写正文与绘图
- 交付：1 篇架构剖析（Markdown + 自包含 HTML）、14 张矢量插图、6 份源码调研笔记、1 篇统一接入口设计

## 内容清单

| 文件 | 说明 |
|---|---|
| [`Daft架构深度剖析.html`](Daft架构深度剖析.html) | **主文档**：自包含单文件（内联 14 张 SVG、侧边目录、代码高亮），可离线阅读与打印 |
| [`Daft架构深度剖析.md`](Daft架构深度剖析.md) | 同一内容的 Markdown 源（16 章 + 4 附录，含 Mermaid 图） |
| [`Daft统一接入口设计.md`](Daft统一接入口设计.md) | 设计提案：零破坏的统一 Provider 抽象（能力声明 + 类型映射 + 注册表），含分阶段落地与一致性测试 |
| `diagrams/` | 14 张 SVG 插图（分层架构、crate 地图、数据模型、生命周期、优化器、计划改写、Swordfish pipeline、UDF 路径、Flotilla、shuffle、Parquet 读取器、内存与背压、能力全景…） |
| `research/` | 6 份源码调研笔记（逻辑计划与表达式 / 优化器与物理计划 / Swordfish / Flotilla / 数据模型与多模态 / Python API 与 IO 运行时），所有结论带 `文件:行号` |
| `svgkit.py` · `diagrams_gen.py` | 插图生成：统一的 SVG 设计系统 + 全部插图定义 |
| `build_doc.py` | Markdown → HTML 构建：把 `<!-- diagram:ID caption="..." -->` 标记替换为内联 SVG，并生成侧边目录 |

## 文档涵盖的主题

- 三层架构（接口 / 计划 / 执行）、50+ crate 的职责分层、三种 Tokio 运行时
- 数据模型：DataType 与扩展类型、Series/Array 的分发机制、Micropartition、Arrow 零拷贝的成立条件与例外
- 逻辑计划（30 个算子）、表达式系统与函数注册表、SQL 前端（sqlparser → LogicalPlan）
- 优化器：32 条规则的分批执行、固定点与环检测、统计信息来源、UDF 为何必须隔离成独立节点
- Swordfish：pipeline 四类节点、morsel 驱动 Push、三道背压、动态批、关键算子实现、UDF 的三条执行路径
- Flotilla：pipeline node DAG 与物化、调度器与任务生命周期、流式 Limit、三种 shuffle 算法、Ray/K8s 集成
- IO 与 Parquet 读取器：ObjectStore 抽象、ScanTask 切分与下推、v0.7.14 重写的读取器（远端读取最高 17×）
- 多模态与 AI 能力的真实落点（哪些在 Rust、哪些在 Python、哪些外委生态）
- 性能与调优手册、二次开发指南、能力缺口与选型建议

## 重新构建

```bash
pip install markdown pygments
python diagrams_gen.py     # 生成 diagrams/*.svg
python build_doc.py        # 生成 Daft架构深度剖析.html
```

在 Markdown 中用如下标记插入插图（`build_doc.py` 会替换为对应的 `<figure>`）：

```markdown
<!-- diagram:08-swordfish-pipeline caption="图 8 · Swordfish pipeline 图" -->
```

标记后可跟一个 ` ```mermaid ` 代码块：Markdown 阅读器可渲染它，构建 HTML 时会被矢量图替换。

## 来源与归属

- Daft 由 **Eventual（Eventual Computing, Inc.）** 开发并以 **Apache-2.0** 许可开源：<https://github.com/Eventual-Inc/Daft>
- 本项目中的源码引用、行号与结论均来自上述仓库的公开代码与官方文档/博客；引用片段仅用于分析与评论。
- 官方资料：[文档](https://docs.daft.ai) · [Swordfish 引擎解析](https://www.eventual.ai/blog/exploring-daft-swordfish-execution-mechanism) · [Flotilla 发布](https://www.eventual.ai/blog/introducing-flotilla-simplifying-multimodal-data-processing-at-scale) · [v0.7.14 发布说明](https://www.eventual.ai/blog/daft-v0-7-14)
- 详见 [NOTICE.md](NOTICE.md)。本仓库为独立第三方分析，与 Eventual 无隶属关系。

## 说明

- 版本迭代很快，正文结论以所标注的 commit 为准；若与你的版本不符，欢迎以 `文件:行号` 纠正。
- 调研笔记中明确列有"否定性结论"与未核实项（例如某 crate 不存在、某能力未找到），正文对这类结论均采用审慎表述。
