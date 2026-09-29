#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""生成 Daft 文档所需的全部 SVG 插图。"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from svgkit import ACCENTS, C, MONO, Svg, tw, write  # noqa: E402

W = 1240


# ---------------------------------------------------------------- 图 1 分层架构
def d01_layers() -> Svg:
    s = Svg(W, 812, "Daft 端到端架构")
    s.title_block(28, 40, "Daft 端到端架构：三层抽象 + 两种 Runner",
                  "用户只写声明式的 DataFrame / SQL；计划、优化、执行全部由 Rust 引擎接管")

    # ---- API 层 ----
    s.band(24, 70, 1192, 150, "① 接口层 API Layer", "accent=violet",
           sub="Python 进程内，惰性构建计划")
    api = [
        ("Python DataFrame", ["daft.read_parquet(...)", "df.filter().select()", "惰性：不触发计算"], "violet", "Python"),
        ("SQL 接口", ['daft.sql("SELECT ...")', "catalog + 表注册", "与 DataFrame 同计划"], "violet", "SQL"),
        ("表达式系统", ["col(\"a\") + 1", "Expr 树 / 函数注册表", "类型推导 + 列解析"], "pink", "Expr"),
        ("UDF / AI 函数", ["@daft.func / @daft.cls", "embed_text / prompt", "资源请求 num_gpus"], "pink", "UDF"),
        ("Session / Catalog", ["Runner 配置", "IO 配置 / 执行配置", "表与命名空间"], "cyan", "Config"),
    ]
    x, y, cw, ch, gap = 44, 104, 224, 100, 12
    for t, ls, a, tag in api:
        s.card(x, y, cw, ch, t, ls, accent=a, tag=tag, title_size=14.5, line_size=11.8)
        x += cw + gap
    s.arrow(620, 222, 620, 258, color=C["violet"], label="LogicalPlanBuilder 构造逻辑计划树")

    # ---- 计划层 ----
    s.band(24, 248, 1192, 210, "② 计划层 Plan Layer", "accent=cyan", sub="纯 Rust，编译期已知 schema")
    s.card(44, 284, 250, 152, "LogicalPlan 算子树",
           ["Source / Project / Filter", "Aggregate / Join / Sort", "Repartition / UDFProject", "描述 what，不描述 how"],
           accent="cyan", title_size=15, line_size=12.2)
    s.card(316, 284, 262, 152, "Optimizer 优化器",
           ["规则式：下推 / 折叠 / 消除", "代价式：Join 重排", "多模态：UDF/下载单独成节点", "统计信息来自 Scan"],
           accent="pink", title_size=15, line_size=12.2)
    s.card(600, 284, 262, 152, "LocalPhysicalPlan",
           ["单机算子图", "Offset→Limit 等改写", "→ Swordfish Pipeline"], accent="green",
           title_size=15, line_size=12.2)
    s.card(884, 284, 288, 152, "DistributedPhysicalPlan",
           ["按 shuffle 边界切 Stage", "任务 = 分区 = 一个 ScanTask", "→ Flotilla 调度"], accent="amber",
           title_size=15, line_size=12.2)
    s.arrow(294, 360, 314, 360, color=C["pink"])
    s.arrow(578, 360, 598, 360, color=C["green"])
    s.arrow(862, 360, 882, 360, color=C["amber"])
    s.text(1010, 462, "Runner 选择：DAFT_RUNNER=native | ray", size=12, fill=C["muted"], anchor="middle")

    # ---- 执行层 ----
    s.band(24, 478, 1192, 236, "③ 执行层 Execution Layer", "accent=green", sub="Rust + Tokio，morsel 驱动的流水线")
    s.card(44, 514, 560, 180, "Swordfish（native runner，单机）", accent="green", tag="tokio", title_top=True)
    s.text(62, 560, "Each operator decides its own parallelism & batching", size=12, fill=C["muted"], style='font-style="italic"')
    s.text(62, 588, "SourceNode → Intermediate → Blocking/Streaming Sink", size=12.2, fill=C["green"], font=MONO, weight="600")
    for i, (t, sub) in enumerate([
        ("Scan 任务池", "并发受控"), ("有界 channel", "背压"), ("动态 batch", "内存可控"), ("算子自定并行", "按并发度 spawn")]):
        s.tile(62 + i * 132, 604, 120, 72, t, sub, accent="green")
    s.text(62, 692, "峰值内存 ∝ morsel 大小，而非数据集大小", size=11.6, fill=C["muted"])

    s.card(624, 514, 548, 180, "Flotilla（ray / Kubernetes runner，分布式）", accent="amber",
           tag="distributed", title_top=True)
    s.text(642, 560, "Driver: Flotilla Scheduler ｜ Worker: Swordfish per node", size=12.2, fill=C["amber"],
           font=MONO, weight="600")
    for i, (t, sub) in enumerate([
        ("Stage 划分", "按 shuffle 边界"), ("任务调度", "locality + 负载"), ("Shuffle", "对象存储 / Flight"), ("容错", "重试 + 取消")]):
        s.tile(642 + i * 130, 604, 118, 72, t, sub, accent="amber")
    s.text(642, 692, "任务粒度 = 一个分区（通常一个文件）", size=11.6, fill=C["muted"])

    # ---- 底部：存储 + 语言边界 ----
    s.card(44, 724, 560, 76, "存储与数据源", ["S3 / GCS / ABFS / HDFS / 本地", "Iceberg · Delta · Hudi · Lance · Kafka",
                                       "HTTP(S) URL · HuggingFace · Ray Object Store"],
           accent="slate", title_size=14, line_size=11.6)
    s.card(624, 724, 548, 76, "语言边界", ["Python：计划构建、UDF、模型推理（PyArrow 零拷贝）",
                                      "Rust：优化器、调度、算子内核、IO（arrow-rs）"],
           accent="pink", title_size=14, line_size=11.6)
    return s


# ---------------------------------------------------------------- 图 2 crate 地图
def d02_crate_map() -> Svg:
    s = Svg(W, 906, "Daft Rust crate 地图")
    s.title_block(28, 40, "源码级地图：50+ Rust crate 的职责分层",
                  "同一份 Rust 核心同时服务 Python DataFrame API 与 SQL；Python 仅做绑定与 UDF 宿主")
    groups = [
        ("① 绑定与扩展", "violet", 3, [
            ("daft（Python 包）", ["DataFrame / Expression / Series", "daft.functions · daft.io · daft.udf"]),
            ("pyo3 绑定层", ["Python ↔ Rust 边界", "PyArrow FFI 零拷贝 / pickle 计划"]),
            ("daft-ext · daft-ext-macros", ["扩展点注册", "#[daft_function] 等宏"]),
        ]),
        ("② 计划与优化", "cyan", 3, [
            ("daft-logical-plan", ["LogicalPlan / LogicalPlanBuilder", "optimization/ 规则集"]),
            ("daft-dsl", ["Expr 树 / 函数注册表", "列解析 / 类型推导"]),
            ("daft-stats · daft-algebra", ["统计信息 Stats", "表达式代数化简"]),
            ("daft-scan · daft-catalog", ["ScanOperator / ScanTask", "表 · 命名空间 · catalog"]),
            ("daft-sql", ["sqlparser → LogicalPlan", "daft.sql() 前端"]),
        ]),
        ("③ 执行引擎", "green", 3, [
            ("daft-local-plan", ["LocalPhysicalPlan", "逻辑→物理一一映射"]),
            ("daft-local-execution", ["Swordfish pipeline", "算子 / Sink / 资源调度"]),
            ("daft-distributed", ["Flotilla 调度器", "Stage / Task / Worker"]),
            ("daft-shuffles", ["Ray 对象存储 shuffle", "Flight shuffle / spill"]),
            ("daft-runners · daft-dashboard", ["runner 抽象与选择", "进度 / 指标 / 可视化"]),
        ]),
        ("④ 数据模型与 IO", "amber", 4, [
            ("daft-core · daft-recordbatch", ["Array / Series / DataType", "RecordBatch / Literal"]),
            ("daft-micropartition", ["分区物化与惰性求值", "算子间传递单位"]),
            ("daft-io", ["ObjectStore（OpenDAL）", "缓存 / 重试 / 限流"]),
            ("daft-parquet · csv · json · avro", ["读取器 + 下推", "arrow-rs 解码"]),
            ("daft-writers", ["Parquet / CSV / Iceberg", "Delta / Lance 写出"]),
            ("daft-image · daft-ai · daft-text", ["解码 / 推理 / 分词", "多模态内核"]),
            ("daft-functions-* 系列", ["utf8 / list / json / temporal", "binary / uri / tokenize …"]),
            ("daft-file · daft-decoding · daft-mcap", ["文件抽象 / 解码", "专用格式连接器"]),
        ]),
    ]
    tops: list[tuple[int, int]] = []
    y = 70
    for title, accent, cols, items in groups:
        rows = (len(items) + cols - 1) // cols
        h = 30 + rows * 70 + (rows - 1) * 12 + 18
        s.band(24, y, 1192, h, title, accent=accent)
        cw = (1148 - (cols - 1) * 14) / cols
        for i, (t, ls) in enumerate(items):
            r, c = divmod(i, cols)
            s.card(46 + c * (cw + 14), y + 30 + r * 82, cw, 70, t, ls,
                   accent=accent, title_size=12.8, line_size=10.6, shadow=False)
        tops.append((y, y + h))
        y += h + 20
    for i in range(len(tops) - 1):
        s.arrow(16, tops[i][1] - 6, 16, tops[i + 1][0] + 6, color=C["line2"], sw=1.6)
    s.text(28, 890, "依赖方向自上而下：接口层 → 计划层 → 执行层 → 数据/IO 层；实际 crate 清单见 src/ 目录（此处按职责归并）。",
           size=11.8, fill=C["muted"])
    return s


# ---------------------------------------------------------------- 图 3 数据模型
def d03_data_model() -> Svg:
    s = Svg(W, 790, "Daft 数据模型层次")
    s.title_block(28, 40, "数据模型：从查询结果到 Arrow 缓冲区",
                  "越往下越接近 Arrow 内存模型；跨算子传递的是 Micropartition（一组 RecordBatch）")
    rows = [
        ("DataFrame（惰性）", "用户手里的对象：只有计划，没有数据", ["daft/dataframe/dataframe.py", "LogicalPlan + Session"], "violet"),
        ("PartitionSet / PartitionRef", "执行期的一组分区引用（本地或远端对象）", ["daft-partition-refs", "惰性 / 可缓存"], "cyan"),
        ("Micropartition", "一个分区的物化结果：Vec<RecordBatch> + 统计信息", ["daft-micropartition", "算子间传递的单位"], "green"),
        ("RecordBatch", "列式数据块：Vec<Series> + 行数 + schema", ["daft-recordbatch", "表达式求值的作用域"], "amber"),
        ("Series / Array", "单列数据：DataType + Arrow Array + validity", ["daft-core", "按类型分发（downcast 宏）"], "pink"),
        ("Arrow 缓冲区", "Buffer / Offset / Validity bitmap，跨语言零拷贝", ["arrow-rs · PyArrow FFI", "无数据拷贝即可 zero-copy"], "slate"),
    ]
    y = 78
    for i, (t, sub, tags, accent) in enumerate(rows):
        h = 82
        s.card(44, y, 700, h, t, [sub], accent=accent, title_size=15.5, line_size=12.4)
        tx = 764
        for j, tag in enumerate(tags):
            s.pill(tx, y + 18 + j * 28, tag, accent=accent, size=11.5, mono=True)
        if i < len(rows) - 1:
            s.arrow(394, y + h, 394, y + h + 22, color=C["faint"])
        y += h + 22
    s.note(44, 700, 1192,
           "零拷贝条件：数据类型与 Arrow 兼容、无扩展类型转换、Python 侧不触发 to_pylist()；扩展类型（image/tensor/embedding/file）在边界处按需物化。",
           accent="amber", size=12.5, h=52)
    return s


# ---------------------------------------------------------------- 图 4 Pull vs Push
def d04_push_vs_pull() -> Svg:
    s = Svg(W, 640, "Pull 与 Push 执行模型对比")
    s.title_block(28, 40, "为什么是 Morsel 驱动的 Push，而不是 Volcano 的 Pull",
                  "传统迭代器模型在“多模态 + 异步 IO + GPU”场景下会同时浪费 CPU 与网络")

    # ---- 左：Volcano Pull ----
    s.band(24, 76, 592, 500, "Volcano 模型（Pull / 迭代器）", accent="slate")
    ops_l = ["顶层算子 next()", "Project.next()", "Filter.next()", "Scan.next()（读一批）"]
    for i, t in enumerate(ops_l):
        y = 118 + i * 62
        s.card(150, y, 380, 48, t, None, accent="slate", title_size=13.2, shadow=False)
    for i in range(len(ops_l) - 1):
        s.arrow(180, 166 + i * 62, 180, 180 + i * 62, color=C["faint"])
    s.arrow(150, 118, 150, 138, color=C["muted"])
    s.text(140, 300, "next() 逐级向上拉取", size=11.5, fill=C["muted"], anchor="middle",
           style='writing-mode="tb"')
    s.arrow(560, 366, 560, 150, color=C["cyan"], sw=1.8)
    s.text(574, 262, "数据一批批返回", size=11.5, fill=C["cyan"], anchor="middle",
           style='writing-mode="tb"')
    s.note(46, 386, 548, "阻塞点：任一层等待 IO / GPU 时，整条调用栈空转，无法表达异步与背压。",
           accent="amber", size=12, h=52)
    s.note(46, 450, 548, "适合：同步、行式、单机的经典模型；不适合：大 batch 向量化 + 异步 IO + 多模态。",
           accent="slate", size=12, h=52)
    s.text(320, 540, "Pull：上层驱动，下层被动响应", size=12, fill=C["muted"], anchor="middle")

    # ---- 右：Swordfish Push ----
    s.band(624, 76, 592, 500, "Swordfish（Push / morsel 驱动）", accent="green")
    ops_r = [
        ("SourceNode：Scan 任务池产出 morsel", "green"),
        ("IntermediateNode：Project / Filter / UDF", "green"),
        ("有界 async channel（背压）", "cyan"),
        ("SinkNode：Streaming(Limit) / Blocking(Agg)", "amber"),
    ]
    for i, (t, a) in enumerate(ops_r):
        s.card(700, 118 + i * 62, 430, 48, t, None, accent=a, title_size=12.6, shadow=False)
    for i in range(len(ops_r) - 1):
        s.arrow(915, 180 + i * 62, 915, 166 + i * 62, color=C["green"], sw=2.0)
    s.text(1275 - 355, 300, "morsel 向上推送", size=11.5, fill=C["green"], anchor="middle",
           style='writing-mode="tb"')
    s.path("M 1160 150 L 1180 150 L 1180 350 L 1160 350", stroke=C["violet"], sw=1.6, dash="5 4",
           marker="arw" + C["violet"].replace("#", ""))
    s.text(1196, 250, "背压反向传播", size=11.5, fill=C["violet"], anchor="middle",
           style='writing-mode="tb"')
    s.note(646, 386, 548, "同一条 Tokio 工作线程池上交错推进多个算子：一个算子等 IO 时，CPU 去算别的 morsel。",
           accent="green", size=12, h=52)
    s.note(646, 450, 548, "背压 + 动态 batch ⇒ 峰值内存 ∝ morsel 大小，与数据集规模解耦。",
           accent="green", size=12, h=52)
    s.text(920, 540, "Push：数据驱动，算子各自决定并发与批大小", size=12, fill=C["muted"], anchor="middle")
    return s


# ---------------------------------------------------------------- 图 5 查询生命周期
def d05_lifecycle() -> Svg:
    s = Svg(W, 560, "一次查询的完整生命周期")
    s.title_block(28, 40, "一次查询的完整生命周期：从 Python 调用到 Arrow 结果",
                  "前三步是“声明”，第四步起才真正干活；materialize 操作是惰性与执行的分界线")

    stages = [
        ("① Python 构建", "violet", ["df = daft.read_parquet(...)", "  .filter(col('h') == 256)", "  .with_column('img', ...)", "  .limit(100)"],
         "只有计划，无数据"),
        ("② 逻辑计划", "cyan", ["LogicalPlanBuilder", "LogicalPlan 树", "Source / Filter /", "UDFProject / Limit"],
         "代数结构，描述 what"),
        ("③ 优化", "pink", ["32 条规则分批执行", "FixedPoint 最多 20 轮", "统计信息填充", "Join 重排 / UDF 拆分"],
         "改写为高效等价计划"),
        ("④ 物理计划", "amber", ["native: LocalPhysicalPlan", "ray: DistributedPhysicalPlan", "→ pipeline node DAG"],
         "描述 how：算子与调度"),
        ("⑤ 执行", "green", ["Swordfish pipeline", "（Flotilla: 每节点一个）", "Scan 任务池 + 有界通道", "算子自行并发"],
         "morsel 流式推进"),
        ("⑥ 结果", "slate", ["Micropartition", "→ PyArrow（零拷贝）", "collect / show / write_*"],
         "物化点：触发 ①→⑤"),
    ]
    x = 32
    cw, gap = 186, 12
    for i, (t, a, lines, note) in enumerate(stages):
        s.card(x, 90, cw, 148, t, lines, accent=a, title_size=13.6, line_size=11.2)
        s.rect(x, 246, cw, 34, fill=C["white"], stroke=C["line"], rx=9)
        s.text(x + cw / 2, 267, note, size=11, fill=C["muted"], anchor="middle")
        if i < len(stages) - 1:
            s.arrow(x + cw + 1, 164, x + cw + gap - 1, 164, color=C["line2"], sw=1.6)
        x += cw + gap

    s.arrow(32, 306, 1208, 306, color=C["violet"], dash="6 5", sw=1.5)
    s.text(620, 330, "惰性区间：只有构建计划，不读数据、不计算", size=12, fill=C["violet"], anchor="middle", weight="600")
    s.rect(628, 296, 580, 4, fill=C["bg"])
    s.text(620, 356, "触发执行：collect() · show() · to_arrow() · to_pylist() · write_parquet() …", size=12,
           fill=C["green"], anchor="middle", weight="600")

    s.card(32, 380, 590, 152, "关键事实", [
        "· DataFrame 每次变换只更新 Arc<LogicalPlan>，无数据拷贝",
        "· 优化器不可关闭（无 enable_optimizer 开关），只能关个别规则",
        "· 同一 plan 的多个 input_id 复用同一条 pipeline（plan_fingerprint 缓存）",
        "· 结果按 input_id 路由回各自的 Python 接收器（MessageRouter）",
    ], accent="cyan", title_size=14, line_size=12)
    s.card(638, 380, 570, 152, "调试入口", [
        "df.explain()                     未优化逻辑计划",
        "df.explain(show_all=True)        优化后 + 物理计划",
        "df.explain(format=\"mermaid\")     图形化计划",
        "DAFT_INSTRUMENT_LOGICAL_PLAN=1   给节点分配 node_id",
    ], accent="amber", title_size=14, line_size=11.6)
    return s


# ---------------------------------------------------------------- 图 6 优化器批次
def d06_optimizer() -> Svg:
    s = Svg(W, 860, "Daft 优化器执行框架")
    s.title_block(28, 40, "优化器：规则分批 + 固定点迭代 + 环检测",
                  "OptimizerRule 只有一个方法 try_optimize；遍历方向由规则自己决定（transform / transform_down）")

    s.card(44, 78, 1152, 62, "Optimizer { RuleBatch[] }", [
        "RuleBatch { rules: Vec<Box<dyn OptimizerRule>>, strategy: Once | FixedPoint(Option<usize>) }"
        "   ·   OptimizerConfig::default = { max_passes: 20, strict_pushdown: false }"],
        accent="violet", title_size=14.5, line_size=12)

    batches = [
        ("批次 1：默认规则（30 条，FixedPoint / Once 混合）", "cyan", [
            "LiftProjectFromAgg · RewriteCountDistinct · UnnestScalar/PredicateSubquery · EliminateSubqueryAlias",
            "ExtractWindowFunction · SplitExplodeFromProject · SimplifyExpressions · FilterNullJoinKey",
            "PushDownAntiSemiJoin · DropRepartition · DropIntoBatches · PushDownFilter · PushDownProjection",
            "EliminateCrossJoin · PushDownJoinPredicate · EliminateOffsets · RewriteOffset · PushDownLimit",
            "SplitUDFsFromFilters · SplitUDFs · SplitVLLM · DetectMonotonicId · PushDownAggregation · MaterializeScans",
        ]),
        ("批次 2：Join 重排（可关闭）", "pink", [
            "ReorderJoins：JoinGraph → 暴力枚举（≤7 关系）或 DP-ccp（≤12，实验性）",
            "之后再次运行 PushDownFilter / PushDownProjection，把谓词推入新的连接顺序",
        ]),
        ("批次 3：统计与细粒度拆分", "amber", [
            "EnrichWithStats：自底向上为每个节点填充 ApproxStats（num_rows / size_bytes / 选择率）",
            "SimplifyExpressions：二次化简    ·    SplitGranularProjection：把需要独立 morsel 的表达式拆成单独 Project",
        ]),
    ]
    y = 156
    for title, accent, lines in batches:
        h = 32 + len(lines) * 24 + 8
        s.band(24, y, 1192, h, title, accent=accent)
        for i, ln in enumerate(lines):
            ly = y + 30 + i * 24
            s.rect(44, ly, 1152, 20, fill=C["white"], stroke=C["line"], rx=6, sw=1)
            s.text(56, ly + 14.5, ln, size=11.6, fill=C["ink2"])
        y += h + 12

    s.note(44, y, 1152,
           "终止条件：某一轮所有规则都没有改写计划 ⇒ 到达固定点；若某轮改写了但计划摘要（plan hash + 节点数）重复出现 ⇒ 判定为环，提前退出该批次。",
           accent="green", size=12.4, h=46)
    y += 60
    s.card(44, y, 562, 140, "为什么 UDF / 下载必须单独成节点", [
        "· 普通表达式在 Rust 内核里按 Arrow 向量化执行，无批次概念",
        "· UDF / 模型推理 / URL 下载需要自己的 batch_size 与 concurrency",
        "· 拆出 UDFProject 后，执行器可以独立背压、独立调度、独立上报指标",
        "· 同时保证它们在 join/聚合之后执行，避免对将被丢弃的行做昂贵计算",
    ], accent="pink", title_size=13.6, line_size=11.6)
    s.card(622, y, 574, 140, "统计信息从哪来", [
        "· Parquet/文件 metadata（精确 length）→ 短路直接给出精确行数",
        "· 否则用 size_bytes × inflation_factor ÷ 行宽估算（parquet 默认 3.0）",
        "· 列级区间统计在 daft-stats，但目前不参与逻辑优化决策",
        "· 消费方：Join 策略选择、shuffle/agg 分区数、broadcast 判定",
    ], accent="cyan", title_size=13.6, line_size=11.6)
    return s


# ---------------------------------------------------------------- 图 7 计划改写
def d07_plan_rewrite() -> Svg:
    s = Svg(W, 700, "逻辑计划改写示例")
    s.title_block(28, 40, "同一个查询，优化前后的逻辑计划", 
                  "示例：读取 Parquet → 过滤尺寸 → 下载 URL 并解码 → 模型推理 → 取前 100 行")

    def tree(x, y, title, accent, nodes, widths=None):
        s.text(x, y, title, size=14, fill=C[ACCENTS[accent][0]], weight="700")
        yy = y + 18
        for i, (n, tag) in enumerate(nodes):
            w = widths[i] if widths else 300
            s.card(x, yy, w, 40, n, None, accent=accent, title_size=11.8, shadow=False, title_top=True)
            if tag:
                s.pill(x + w - tw(tag, 10.5) - 24, yy + 10, tag, accent=accent, size=10.5, h=19)
            yy += 50

    tree(32, 84, "优化前（LogicalPlanBuilder 直译）", "slate", [
        ("Limit(100)", ""),
        ("Project: name, height, width, url, labels", ""),
        ("UDFProject: ResNetModel(tensor) as labels", "Python UDF"),
        ("UDFProject: transform(image) as tensor", "Python UDF"),
        ("Project: url.download(), decode(bytes)", "IO + 解码"),
        ("Filter: height == 256 AND width == 256", ""),
        ("Source: GlobScan(parquet)", "1424 tasks"),
    ], widths=[340] * 7)

    s.arrow(400, 250, 456, 250, color=C["violet"], sw=2.4)
    s.text(428, 238, "优化", size=12, fill=C["violet"], anchor="middle", weight="700")

    tree(480, 84, "优化后（Optimizer 输出）", "green", [
        ("UDFProject: ResNetModel(...)  as labels", "concurrency"),
        ("UDFProject: transform(image) as tensor", "batch_size"),
        ("Project: name, url, tensor（折叠后）", "folding"),
        ("Project: url, decode(url.download())", "细粒度拆分"),
        ("Limit(100)  ← 再次下推", "pushdown"),
        ("Filter: height == 256 AND width == 256", "未下推"),
        ("Source: GlobScan(parquet)", "全部下推"),
    ], widths=[700] * 7)

    s.note(32, 470, 590,
           "关键改写：\n"
           "① 谓词与 Limit 下推到 Scan（少读、少算、少物化）\n"
           "② 连续 Project 折叠为一条投影链，并在 Scan 上做列裁剪\n"
           "③ UDF 从普通 Project 中拆出，获得独立的批次 / 并发 / 资源语义\n"
           "④ 下载与解码被拆成细粒度 Project，各自使用合适的 morsel 大小",
           accent="cyan", size=11.8)
    s.note(638, 470, 570,
           "为什么 Filter 没有“一路推到底”？\n"
           "Daft 显式把 UDF / 下载 / 模型推理排除在下推范围外，它们在计划中依旧是 Filter 之上的独立节点；"
           "优化器的职责反而是把它们放在 join / 聚合之后，避免对将被丢弃的行做昂贵计算。",
           accent="pink", size=11.8)
    s.text(32, 606, "同一套改写对 SQL 与 DataFrame 完全一致——两者产出的是同一个 LogicalPlanBuilder。",
           size=12, fill=C["muted"])
    return s


# ---------------------------------------------------------------- 图 8 Swordfish pipeline
def d08_swordfish_pipeline() -> Svg:
    s = Svg(W, 720, "Swordfish 执行流水线")
    s.title_block(28, 40, "Swordfish：Physical Plan → Pipeline 图 → morsel 流式执行",
                  "节点间只有 4 类角色；通道容量固定为 1，背压天然形成")

    # 主体 DAG
    s.card(44, 84, 200, 74, "SourceNode", ["Scan 任务池（默认 8 并发）", "按行数切 chunk 产出"], accent="cyan",
           title_size=13.6, line_size=11.4)
    s.card(292, 84, 200, 74, "JoinNode", ["双输入：build(左) / probe(右)", "build 完成前 probe 阻塞"], accent="violet",
           title_size=13.6, line_size=11.4)
    s.card(540, 84, 200, 74, "Intermediate", ["Project / Filter / UDF", "每 morsel 拿一个 state"], accent="green",
           title_size=13.6, line_size=11.4)
    s.card(788, 84, 200, 74, "BlockingSink", ["Agg / Sort / Write", "收齐全部输入才 finalize"], accent="amber",
           title_size=13.6, line_size=11.4)
    s.card(1036, 84, 200, 74, "StreamingSink", ["Limit / Sample", "可提前 Finished"], accent="pink",
           title_size=13.6, line_size=11.4)

    for x in (244, 492, 740, 988):
        s.arrow(x + 2, 121, x + 46, 121, color=C["line2"], sw=2)
        s.text(x + 24, 108, "ch(1)", size=10.5, fill=C["faint"], anchor="middle", font=MONO)
    s.arrow(1136, 158, 1136, 196, color=C["pink"], sw=2)
    s.text(1150, 182, "结果", size=11.5, fill=C["pink"])

    s.card(44, 200, 340, 96, "Build 侧（左输入）", [
        "广播小表 / 构建哈希表；stats 决定哪侧做 build",
        "inner/outer 取小侧；left/anti/semi 需要位图跟踪",
    ], accent="violet", title_size=13, line_size=11.4)
    s.arrow(384, 248, 492, 248, color=C["violet"], sw=1.8)
    s.text(438, 240, "BuildStateBridge", size=11, fill=C["violet"], anchor="middle")

    # 下部：背压与 morsel 机制
    s.band(24, 336, 590, 356, "三道背压闸门", accent="green")
    items = [
        ("① 通道容量 = 1", "上游算子必须等下游取走 morsel，才继续生产"),
        ("② 并发门控", "next_event 仅在 task_set.len() < max_concurrency 时收上游消息"),
        ("③ Scan 任务池", "扫描任务在 IO runtime 上限流（scantask_max_parallel）"),
    ]
    yy = 380
    for t, sub in items:
        s.card(44, yy, 550, 62, t, [sub], accent="green", title_size=13, line_size=11.4)
        yy += 74
    s.note(44, 606, 550, "结果：峰值内存 ∝ morsel 大小（默认 131072 行）× 算子并行度，与数据集规模无关。",
           accent="green", size=11.6, h=62)

    s.band(624, 336, 592, 356, "morsel 尺寸的传播", accent="cyan")
    s.card(644, 380, 552, 60, "需求自顶向下传播", [
        "根节点 Flexible(0, default_morsel_size) → propagate_morsel_size_requirement",
    ], accent="cyan", title_size=12.6, line_size=11.2, title_top=True)
    s.card(644, 452, 552, 60, "两类算子会“切断”需求", [
        "BlockingSink 强制子节点用 default；JoinNode 只把需求传给 probe 侧",
    ], accent="cyan", title_size=12.6, line_size=11.2, title_top=True)
    s.card(644, 524, 552, 60, "RowBasedBuffer 三段式", [
        "低于下界：继续攒；区间内：整块产出；超上界：切出上界行、余量塞回",
    ], accent="cyan", title_size=12.6, line_size=11.2, title_top=True)
    s.note(644, 596, 552, "UDF 可强制 Strict(batch_size)；动态批（默认关）按延迟约束二分搜索最大批量。",
           accent="cyan", size=11.6, h=72)
    return s


# ---------------------------------------------------------------- 图 9 UDF 执行
def d09_udf() -> Svg:
    s = Svg(W, 700, "UDF 的执行路径")
    s.title_block(28, 40, "Python UDF 如何被打进 Rust 流水线",
                  "同一份 UDF，Daft 会按场景在三条路径中选择：线程 / 子进程 actor pool / 异步 + GPU")

    s.card(44, 78, 1152, 88, "统一入口：UDFProject 算子（pipeline.rs 中的关键分叉）", [
        "is_async && !use_process ⇒ AsyncUdfSink（流式）     否则 ⇒ UdfOperator（中间算子）",
        "并发槽 = 每个槽一份 UdfState；batch_size 以 MorselSizeRequirement::Strict(n) 强制固定行数",
    ], accent="violet", title_size=14, line_size=11.6, title_top=True)

    paths = [
        ("路径 A：线程内执行（默认）", "green", [
            "在 compute runtime 的工作线程上 Python::attach 取 GIL",
            "调用 initialize_udfs + eval_expression_with_metrics",
            "优点：零拷贝、无序列化开销",
            "代价：受 GIL 限制，CPU 密集 UDF 无法真正并行",
        ]),
        ("路径 B：子进程 Actor Pool（推荐）", "cyan", [
            "daft.execution.udf.UdfHandle → subprocess + UNIX socket",
            "数据传输走 SharedMemory（IPC 流），非 pickle 管道",
            "子进程内线程数设为 1，避免嵌套并行；含 Python object dtype 时回退线程",
            "失败回传异常栈，Restore 为 UDFException；teardown 超时 5s 后 terminate",
        ]),
        ("路径 C：异步 / GPU（AsyncUdfSink）", "pink", [
            "每个 state 一个 JoinSet，默认最多 64 个在飞任务",
            "适合 await 模型服务 / vLLM 推理：IO 等待期间不占线程",
            "ResourceRequest.num_gpus 决定并发槽：available = (num_cpus / n).clamp(1, num_cpus)",
            "内存请求经 MemoryManager 许可后放行（spawn_with_memory_request）",
        ]),
    ]
    x = 44
    for t, a, lines in paths:
        s.card(x, 156, 376, 180, t, lines, accent=a, title_size=13.6, line_size=11.4)
        x += 388

    s.card(44, 356, 1152, 118, "一次 UDF morsel 的旅程", [
        "上游 morsel → BatchManager（按 input_id 缓冲、按行数切批）→ 取一个空闲 UdfState → spawn 到 compute runtime",
        "→ Rust 侧序列化输入（线程路径直接传 RecordBatch；进程路径写 SharedMemory）→ Python 侧执行 → 返回 Series/RecordBatch",
        "→ 与 state 一起回传 → 统计（rows in/out、耗时、放大系数）汇总到 RuntimeStatsManager → 推给进度条 / Dashboard",
    ], accent="amber", title_size=13.6, line_size=11.6)

    s.note(44, 494, 562, "本地执行没有任务级重试：UDF 抛错即整条查询失败；重试只发生在分布式层的 worker 失效场景。",
           accent="amber", size=11.8, h=64)
    s.note(622, 494, 574, "UDF 也是“不能被下推”的算子：优化器把它们拆成独立节点，保证在 join/聚合之后执行，并独立背压。",
           accent="pink", size=11.8, h=64)
    s.text(44, 596, "典型配置：@daft.func(return_dtype=..., batch_size=..., concurrency=...)；@daft.cls 支持有状态的模型加载与 GPU 资源声明。",
           size=11.8, fill=C["muted"])
    return s


# ---------------------------------------------------------------- 图 10 Flotilla
def d10_flotilla() -> Svg:
    s = Svg(W, 780, "Flotilla 分布式架构")
    s.title_block(28, 40, "Flotilla：driver 上的调度器 + 每节点一个 Swordfish worker",
                  "控制面走 Ray actor 方法调用；数据面走 Ray object store 或 Arrow Flight（仅 flight shuffle）")

    s.band(24, 74, 1192, 150, "Driver / 头节点", accent="violet")
    for i, (t, lines) in enumerate([
        ("RayRunner → PlanRunner", ["DistributedPhysicalPlan", "={query_idx, logical_plan, config}", "翻译发生在 run 时"]),
        ("Flotilla Scheduler", ["SchedulerLoop（1s tick）", "DefaultScheduler 策略", "Dispatcher 派发"]),
        ("StatisticsManager", ["消费 TaskEvent", "聚合 worker 侧 StatSnapshot", "驱动进度条 / Dashboard"]),
    ]):
        s.card(44 + i * 386, 106, 370, 104, t, lines, accent="violet", title_size=13.4, line_size=11.2)
    s.text(620, 240, "① 翻译逻辑计划 → pipeline node DAG   ② 按 BlockingSink 边界物化 stage   ③ 提交任务、回收结果",
           size=11.8, fill=C["muted"], anchor="middle")

    s.band(24, 258, 1192, 210, "Ray 集群：每个节点一个 RaySwordfishActor", accent="amber")
    for i in range(3):
        x = 44 + i * 386
        s.card(x, 292, 370, 152, f"Worker 节点 {i + 1}", [
            "RaySwordfishActor（NodeAffinity 硬绑定）",
            "NativeExecutor(is_flotilla_worker=True)",
            "内部跑 daft-local-execution（Swordfish）",
            "CPU/GPU 取自节点 Resources",
            "输出 64MiB 聚合后写入对象存储",
        ], accent="amber", title_size=13.4, line_size=11.2)
    s.text(620, 466, "任务粒度 = 一个分区（通常一个文件）；worker 复用本地引擎，无第二套运行时", size=11.8,
           fill=C["muted"], anchor="middle")

    s.band(24, 500, 590, 250, "数据面", accent="green")
    s.card(44, 534, 550, 88, "Ray Object Store（默认）", [
        "RayPartitionRef{object_ref, num_rows, size_bytes}",
        "适合可放进内存的 shuffle 与中间结果",
    ], accent="green", title_size=13, line_size=11.2)
    s.card(44, 636, 550, 100, "Arrow Flight Shuffle（大 shuffle）", [
        "FlightPartitionRef{shuffle_id, server_address, partition_ref_id}",
        "worker 本地磁盘落盘 + gRPC/tonic 传输；同节点读取走进程内",
    ], accent="cyan", title_size=13, line_size=11.2)

    s.band(624, 500, 592, 250, "容错与弹性", accent="pink")
    for i, (t, sub) in enumerate([
        ("重试", "仅 WorkerDied / WorkerUnavailable 重排；普通错误终止查询"),
        ("取消", "CancellationToken → ray.cancel()；取消任务不再调度"),
        ("扩缩容", "pending/CPU 比值 > 1.25 触发扩容；可选退役空闲 worker"),
    ]):
        s.card(644, 534 + i * 70, 552, 60, t, [sub], accent="pink", title_size=12.6, line_size=11.2)
    return s


# ---------------------------------------------------------------- 图 11 Shuffle
def d11_shuffle() -> Svg:
    s = Svg(W, 700, "Shuffle 机制")
    s.title_block(28, 40, "三种 shuffle 算法：从对象存储到 Arrow Flight",
                  "auto 只会在地图归约与预合并之间选择；超过阈值时在计划里给出开启 flight_shuffle 的提示")

    s.card(44, 78, 1152, 44, "配置：shuffle_algorithm = auto | map_reduce | pre_shuffle_merge | flight_shuffle",
           accent="violet", title_size=13, title_top=True)

    rows = [
        ("map_reduce", "green", [
            "数据面：Ray object store",
            "每个 (map, reduce) 槽一个对象",
            "驱动端元数据 ≈ M×N×3KB",
        ], "小规模 shuffle：分区数适中时最省事"),
        ("pre_shuffle_merge", "cyan", [
            "数据面：Ray object store",
            "先合并小输入分区，降低 M",
            "auto 判据：sqrt(M×N) > 200",
        ], "分区乘积大、但总字节数中等"),
        ("flight_shuffle", "pink", [
            "数据面：本地磁盘 + Arrow Flight",
            "描述符开销 ≈ (M+N)×200B",
            "按 partitioning 分区，落盘可溢写",
        ], "≥10 GiB 或槽位 ≥50 万；头节点 OOM 的解药"),
    ]
    x = 44
    for t, a, lines, tag in rows:
        s.card(x, 146, 376, 142, t, lines, accent=a, title_size=14.5, line_size=11.6)
        s.rect(x, 296, 376, 34, fill=C["white"], stroke=C["line"], rx=9)
        s.text(x + 188, 317, tag, size=11, fill=C["muted"], anchor="middle")
        x += 388

    s.band(24, 348, 590, 330, "写侧：每个 map task 只写一个文件", accent="amber")
    for i, (t, sub) in enumerate([
        ("单文件布局", "schema ｜ partition0 批次 ｜ … ｜ partitionN-1 ｜ EOS"),
        ("分区字节区间", "记录每个分区的 (start, end)，读取时按区间取"),
        ("串行写", "单个 spawn_blocking 线程 + 1 MiB BufWriter，避免 N 次任务分配"),
        ("压缩", "lz4（默认，本地 NVMe）/ zstd（网络盘）/ none"),
    ]):
        s.card(44, 376 + i * 74, 550, 62, t, [sub], accent="amber", title_size=12.6, line_size=11.2)

    s.band(624, 348, 592, 330, "读侧：ShuffleFlightServer + 本地优先", accent="green")
    for i, (t, sub) in enumerate([
        ("请求合并", "to_server_requests 把同一 server 的请求合并"),
        ("同节点短路", "get_partition_local 直接读内存/磁盘，不走 gRPC"),
        ("文件级分组", "同一文件只开一个 FD，区间排序利于预读"),
        ("协调器省内存", "只保留每 server 的 map input id 列表，reduce 端还原 refs"),
    ]):
        s.card(644, 376 + i * 74, 552, 62, t, [sub], accent="green", title_size=12.6, line_size=11.2)
    return s


# ---------------------------------------------------------------- 图 12 Parquet 读取器
def d12_parquet() -> Svg:
    s = Svg(W, 760, "Parquet 读取器内部结构")
    s.title_block(28, 40, "v0.7.14 重写的 Parquet 读取器（基于 arrow-rs array_reader）",
                  "新 IO 模型 + 新并发策略 + 两阶段谓词下推；远端读取实测提升最多 17 倍")

    # 文件结构
    s.card(44, 78, 1152, 92, "Parquet 文件与读取入口", [
        "尾读 128 KiB 取 footer → ParquetMetaDataReader::decode_metadata（spawn_blocking）；大 footer 走两段读",
        "Row Group 元数据 → 统计信息剪枝（缺少统计时保守保留）→ 仅预取“投影列 ∪ 谓词列”对应的 column chunk",
    ], accent="cyan", title_size=13.4, line_size=11.6, title_top=True)

    s.band(24, 186, 566, 262, "IO 层：ChunkSource", accent="green")
    for i, (t, sub) in enumerate([
        ("本地：pread 合并", "read_at / seek_read，64 KiB 以内的小区间自动合并"),
        ("远端：按 RG 取 range", "coalesce_and_split：间隙 ≤1 MiB 合并、>24 MiB 切分、单请求 ≤16 MiB"),
        ("立即预取", "合并后的每个区间马上 spawn single_url_get(GetRange::Bounded)"),
        ("先剪枝再预取", "只在谓词剪枝之后构建 ChunkSource，避免白读被剪掉的行组"),
    ]):
        s.card(44, 214 + i * 58, 526, 50, t, [sub], accent="green", title_size=12.4, line_size=11)

    s.band(614, 186, 602, 262, "计算层：并发解码 + 顺序回放", accent="amber")
    for i, (t, sub) in enumerate([
        ("每 RG 一个解码任务", "JoinSet::spawn_on(compute runtime)，列解码层再叠一层 JoinSet"),
        ("有界通道容量 1", "每个 RG 一个 mpsc::channel(1)，天然背压"),
        ("按 RG 顺序回放", "receivers 顺序 flatten，文件内输出始终有序"),
        ("跨 RG 提前终止", "apply_cross_rg_limit 处理跨行组的 LIMIT"),
    ]):
        s.card(634, 214 + i * 58, 562, 50, t, [sub], accent="amber", title_size=12.4, line_size=11)

    s.band(24, 464, 1192, 268, "两阶段谓词下推：行组级 + 行级", accent="pink")
    s.card(44, 500, 370, 108, "阶段 A：统计剪枝", [
        "用 row group 的 min/max 统计裁剪行组",
        "缺失/转换失败 ⇒ 保留该行组（正确性优先）",
    ], accent="pink", title_size=13, line_size=11.4)
    s.card(432, 500, 370, 108, "阶段 B：行级过滤", [
        "解码谓词列 → Daft 表达式求值出 bool mask",
        "mask RLE 成 RowSelection，与 offset 选择合并",
    ], accent="pink", title_size=13, line_size=11.4)
    s.card(820, 500, 376, 108, "列复用（关键优化）", [
        "谓词列在阶段 A 已解码，组装批次时直接 slice",
        "同一列不会解码两次",
    ], accent="pink", title_size=13, line_size=11.4)
    s.arrow(414, 554, 430, 554, color=C["pink"])
    s.arrow(802, 554, 818, 554, color=C["pink"])
    s.note(44, 622, 1152,
           "与 arrow-rs 的关系：只用底层件（ArrayReader 树 + SerializedPageReader + RowSelection），不用 ParquetRecordBatchReaderBuilder，"
           "也不用 RowFilter/ArrowPredicate —— 谓词求值复用 Daft 表达式系统，从而与 DataFrame/SQL 共享同一套语义。",
           accent="cyan", size=11.8, h=100)
    return s


# ---------------------------------------------------------------- 图 13 内存与稳定性
def d13_memory() -> Svg:
    s = Svg(W, 660, "内存压力来源与控制手段")
    s.title_block(28, 40, "内存不会自己变好：四类压力源与对应的控制旋钮",
                  "Daft 是流式引擎，但某些算子仍必须物化；理解它们是稳定运行的前提")

    items = [
        ("UDF / 模型推理", "python", "内存占用由用户代码决定", "调小 batch_size、限制 concurrency"),
        ("URL / 对象存储下载", "cyan", "高并发下载会在内存中排队响应体", "限制 max_connections；前置 into_batches"),
        ("膨胀型算子", "amber", "解码、解压、explode 让批次变大", "在膨胀前 into_batches 固定行数"),
        ("物化型算子", "pink", "聚合、排序、Join 需要持有中间数据", "加分区 / 换 join 策略 / flight_shuffle / 加内存"),
    ]
    y = 78
    for t, a, why, how in items:
        s.card(44, y, 470, 96, t, [why, "→ " + how], accent=a, title_size=14, line_size=11.6)
        y += 110

    s.band(548, 78, 668, 424, "引擎内部的四道控制", accent="green")
    for i, (t, sub) in enumerate([
        ("① morsel 尺寸", "默认 131072 行；RowBasedBuffer 只在区间内产出整块"),
        ("② 有界通道", "节点间 channel(1)，一次只允许一个 morsel 在途"),
        ("③ 并发门控", "任务占满并发槽就不再从上游取数据"),
        ("④ 内存许可", "MemoryManager + DAFT_MEMORY_LIMIT；UDF 申请后才放行"),
    ]):
        s.card(568, 106 + i * 96, 628, 82, t, [sub], accent="green", title_size=13, line_size=11.4)
    s.note(44, 524, 1172,
           "重要事实：本地执行引擎没有磁盘 spill。排序/聚合/哈希构建都是纯内存累积 —— 它们的大小决定峰值内存；"
           "真正落盘的是分布式 shuffle（flight_shuffle 的 spill 文件）。",
           accent="amber", size=12, h=70)
    return s


# ---------------------------------------------------------------- 图 14 能力全景
def d14_capabilities() -> Svg:
    s = Svg(W, 700, "Daft 能力全景")
    s.title_block(28, 40, "能力全景：哪些在 Rust 内核里，哪些在 Python 层，哪些交给外部包",
                  "理解实现层次，才能判断性能边界与依赖成本")

    cols = [
        ("核心内核（Rust）", "green", [
            ("执行引擎", "Swordfish · morsel 流水线"),
            ("调度", "Flotilla（Ray / K8s）"),
            ("IO", "本地 / S3 / GCS / Azure / HTTP / HF"),
            ("列式格式", "Parquet（arrow-rs）· CSV · JSON · Avro"),
            ("类型系统", "Arrow 原生 + 扩展类型"),
            ("内置函数", "16 个 FunctionModule 系列"),
        ]),
        ("Python 层", "violet", [
            ("DataFrame / SQL", "daft.sql() 与 DataFrame API"),
            ("UDF", "func / cls / batch / async"),
            ("AI Functions", "embed_text · prompt · classify"),
            ("表格式", "Iceberg · Delta · Hudi · Paimon"),
            ("Catalog", "Glue · Unity · Iceberg · Postgres"),
            ("音视频 / PDF", "PyAV · librosa · soundfile"),
        ]),
        ("外部包 / 可选", "amber", [
            ("向量索引", "daft-lance（IVF_PQ 等）"),
            ("分布式运行时", "ray[default]"),
            ("tokenizer", "tiktoken-rs（内建）"),
            ("对象存储扩展", "OpenDAL 通用后端"),
            ("模型服务", "OpenAI · Transformers · vLLM · LM Studio"),
            ("部署", "K8s Helm chart（Ray 集群）"),
        ]),
    ]
    x = 44
    for title, accent, items in cols:
        s.card(x, 78, 376, 30, "", None, accent=accent)
        s.text(x + 18, 98, title, size=14, fill=C[ACCENTS[accent][0]], weight="700")
        yy = 116
        for t, sub in items:
            s.card(x, yy, 376, 74, t, [sub], accent=accent, title_size=13, line_size=11.2)
            yy += 84
        x += 388

    s.note(44, 640, 1172,
           "读法：绿色 = 性能关键路径在 Rust；紫色 = 需要 Python 生态（依赖 GIL / 外部包）；琥珀色 = 依赖额外安装。"
           "多模态能力的“重活”（解码、模型、索引）多数在 Python 侧，因此 UDF 批次与并发配置往往比引擎内部参数更关键。",
           accent="cyan", size=12, h=48)
    return s


if __name__ == "__main__":
    write("01-layers", d01_layers())
    write("02-crate-map", d02_crate_map())
    write("03-data-model", d03_data_model())
    write("04-push-vs-pull", d04_push_vs_pull())
    write("05-lifecycle", d05_lifecycle())
    write("06-optimizer", d06_optimizer())
    write("07-plan-rewrite", d07_plan_rewrite())
    write("08-swordfish-pipeline", d08_swordfish_pipeline())
    write("09-udf", d09_udf())
    write("10-flotilla", d10_flotilla())
    write("11-shuffle", d11_shuffle())
    write("12-parquet", d12_parquet())
    write("13-memory", d13_memory())
    write("14-capabilities", d14_capabilities())
    print("done")
