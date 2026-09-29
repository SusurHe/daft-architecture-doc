# Daft 源码调研 01：逻辑计划 / 表达式系统 / SQL 前端

- 仓库：`D:\DOC\daft-src`，commit dadd8a0，约 v0.7.25。所有行号均来自该 checkout 的真实源码。
- 说明：任务书中假设的 `src/daft-sqlparser` crate **在本仓库不存在**（见 §3.1），相关结论已按实际依赖改写。

## 1. 逻辑计划表示

### 1.1 `LogicalPlan` 变体清单（30 个）

`src/daft-logical-plan/src/logical_plan.rs:35-66`：

| 变体 | 语义 | 输出 schema 来源 |
|---|---|---|
| `Source` | 数据源（内存/扫描/glob/占位） | 自持 `output_schema` |
| `Shard` | 按 world_size/rank 分片 | `input.schema()` |
| `Project` / `UDFProject` | 投影 / 单列 UDF 投影 | 自持 `projected_schema` |
| `Filter`、`Limit`、`Offset`、`Sample`、`IntoBatches`、`IntoPartitions`、`StageCheckpointKeys`、`Sort`、`Repartition`、`Distinct`、`Shuffle`、`TopN` | 行数/顺序/分区类算子 | `input.schema()` |
| `Explode` | 展开 list 列 | 自持 `exploded_schema` |
| `Unpivot` / `Pivot` | 宽窄变换 / 透视 | 自持 `output_schema` |
| `Aggregate` | 分组聚合 + 全局聚合 | 自持 `output_schema` |
| `Concat` | 分区内纵向拼接（schema 相同） | `input.schema()` |
| `Intersect` / `Union` | 集合运算（`is_all`、`UnionStrategy`） | `lhs.schema()` |
| `Join` / `AsofJoin` | 连接 | 自持 `output_schema` |
| `Sink` | 写出（`SinkInfo`） | 自持 `schema` |
| `MonotonicallyIncreasingId`、`VLLMProject`、`Window` | 生成列 / vLLM / 窗口 | 自持 `schema` |
| `SubqueryAlias` | 命名子查询边界 | `input.schema()` |

- `pub type LogicalPlanRef = Arc<LogicalPlan>`（`logical_plan.rs:78`）；`Debug` 在 release 下只打印 plan_id/node_id（`logical_plan.rs:68-76`）。
- 统一能力：`schema()`（`:153-194`）、`name()`（`:400-433`，`SubqueryAlias` 显示为 `"Alias"`）、`children()`（`:558-593`）、`with_new_children()`（`:595+`）、`stats_state()`（`:435-471`）、`multiline_display()`（`:521-556`）。
- 每个算子结构体都带 `plan_id: Option<usize>`、`node_id: Option<usize>`、`stats_state: StatsState`，由 builder/优化器回填。
- `stats_state()` 对 `Intersect`/`Union`/`SubqueryAlias` 直接 panic（`:464-470`）：这三类必须在统计物化前被优化掉（`eliminate_subquery_alias.rs` 等规则）。
- `required_columns()`（`:196-398`）返回 `RequiredCols{required_cols, opt_right_cols}`（`:115-146`），用于列裁剪；`Source`/`Sink` 未实现（`:372-373` 是 `todo!()`），Join 通过 `ResolvedColumn::JoinSide` 区分左右侧列（`:300-327`）。

### 1.2 `ops/` 组织方式与两个算子的 schema 推导

- `src/daft-logical-plan/src/ops/mod.rs:1-64`：一个文件一个算子，`pub use` 汇出；`logical_plan.rs:26` 用 `pub use crate::ops::*` 把算子并入同一命名空间。
- **Aggregate**（`ops/agg.rs:16-34`）：字段 `input / aggregations: Vec<ExprRef> / groupby: Vec<ExprRef> / output_schema / stats_state`。
  - schema 推导：`exprs_to_schema(&[groupby, aggregations].concat(), input.schema())`（`ops/agg.rs:42-45`），即"分组键在前、聚合值在后"，逐表达式调用 `Expr::to_field`（`daft-dsl/src/expr/mod.rs:2794-2800`）。
  - 注释明确：`aggregations` 初始**允许非聚合表达式**，依赖 `LiftProjectFromAgg` 规则把它们提升为 Project，翻译期只剩 alias/agg（`ops/agg.rs:22-27`）。
  - 统计：无 groupby 时估 1 行；有 groupby 时按"80% 行唯一"估算（`ops/agg.rs:78-93`）。
- **Join**（`ops/join.rs:176-192`）：字段 `left / right / on: JoinPredicate / join_type / join_strategy / output_schema / key_filtering_config`。
  - `JoinPredicate` 是 `Option<ExprRef>` 的 newtype，构造时校验谓词里只能出现 `ResolvedColumn::JoinSide`（`ops/join.rs:26-43`）。
  - schema 推导：`infer_join_schema(&left.schema(), &right.schema(), join_type)`（`ops/join.rs:205`），实现在 `daft-dsl/src/join.rs`；重名策略由 `Join::deduplicate_join_columns` 决定，会在右侧插入 Project 消歧（`ops/join.rs:236-252`）。
  - 谓词被切成等值键/单侧谓词：`split_eq_preds()`（`ops/join.rs:80+`）、`replace_join_side_cols` 把 JoinSide 列还原为普通列（`ops/join.rs:66-77`），供 `PushDownJoinPredicate` 使用。
- **Project**（`ops/project.rs:24-32,48-71`）：构造时先 `try_factor_subexpressions` 抽取公共子表达式，再对**改写后的输入**逐个 `to_field` 生成 `projected_schema`；`new_from_schema` 用列名反推投影（`:84-90`）。

### 1.3 Source / ScanOperator 抽象

- `Source`（`ops/source.rs:16-29`）：`output_schema`（可小于源 schema，即投影下推结果）、`source_info: Arc<SourceInfo>`、`checkpoint: Option<CheckpointConfig>`；`with_source_info` 保留其余字段供优化规则替换源（`:54-58`）。
- `SourceInfo` 四态（`source_info.rs:16-21`）：`InMemory(InMemoryInfo)`、`Physical(PhysicalScanInfo)`、`GlobScan(GlobScanInfo)`、`PlaceHolder(PlaceHolderInfo)`。
  - `InMemoryInfo`（`:24-33`）带 `cache_key/cache_entry/num_partitions/size_bytes/num_rows`，`PartialEq/Hash` **只比较 cache_key**（`:60-72`）。
  - `GlobScanInfo` 固定 schema `path/size/num_rows`（`:97-112`）。
- `ScanOperator` trait（`src/daft-scan/src/scan_operator.rs:14-65`）：`name / schema / partitioning_keys / clustering_keys / generated_fields / file_path_column / can_absorb_filter / can_absorb_select / can_absorb_limit / statistics / to_scan_tasks(Pushdowns)`；`ScanOperatorRef(pub Arc<dyn ScanOperator>)`（`:88`）。
- `ScanState` 两态（`src/daft-scan/src/scan_state.rs:13-21`）：`Tasks(Arc<Vec<ScanTaskRef>>)` 与 `Operator(ScanOperatorRef)`；**`Operator` 显式禁止 serde**（`serialize_invalid`，`:36-45`），因为扫描任务必须在 driver 端物化。
- 物化路径：`Source::build_materialized_scan_source`（`ops/source.rs:74-120`）把 `ScanState::Operator` 调 `to_scan_tasks(pushdowns)` 变成 `Tasks`，重复物化会 panic；若 operator 报告精确行数，直接用 `estimated_selectivity` 折算行数并写 `StatsState::Materialized`（`:99-117`）。
- `Pushdowns`（`src/daft-scan/src/pushdowns.rs:16-36`）：`filters / partition_filters / columns / limit / sharder / pushed_filters / aggregation`。
- builder 侧：`LogicalPlanBuilder::table_scan`（`builder/mod.rs:203-254`）从 operator 取 schema、并入 `generated_fields`（`non_distinct_union`）、按 `Pushdowns.columns` 裁剪输出 schema；`in_memory_scan`（`:166-187`）、`from_glob_scan`（`:190-200`）。

### 1.4 `LogicalPlanBuilder` 与 schema 传播机制

- `builder/mod.rs:61` 定义 `LogicalPlanBuilder { plan, config }`；`with_new_plan`（`:125`）做链式替换，`build()`（`:1209`）返回 `Arc<LogicalPlan>`。
- **schema 不是"后算"的**：每个算子的 `try_new` 在构造时就完成类型检查并缓存 `output_schema/projected_schema`；因此没有"有类型/无类型算子"两套流程，只有 `Source` 的 `PlaceHolderInfo` 表示"schema 已知但数据未绑定"（SQL 规划与 `DataFrame` 中间态都用它，见 `daft-sql/src/lib.rs:45,60` 的测试 fixture）。
- 典型构链：`select()`（`:256-359`）先用 `ExprResolver` 解析；若 select 列表含聚合，则拆成 `agg_exprs`（每个 agg 用 `semantic_id` 做内部别名）+ 可折叠字面量，先建 `ops::Aggregate` 再建 `ops::Project`（`:337-354`），否则直接建 `Project`（`:356-358`）。
- `aggregate()`（`:654-668`）：groupby 用默认 resolver 解析，聚合表达式在 `ExprResolver::builder().groupby(&groupby)` 的**聚合上下文**中解析（禁止在 agg 里引用未分组列）。
- `join()`（`:716-763`）：`resolve_join_on` 解析谓词 → `deduplicate_join_columns` 消歧 → `using` 列表展开为 `left_col(f).eq(right_col(f))` → `JoinPredicate::try_new` → `ops::Join::try_new`。
- 优化入口：`optimize()`（`:1102`）、`optimize_async()`（`:1024`）、`with_checkpoint()`（`:150`）；`alias()`（`:134`）产生 `SubqueryAlias`。

### 1.5 遍历 / 重写基础设施

- 通用树抽象在 `src/common/treenode/src/lib.rs`：`TreeNode`（`:92`，方法 `visit:124 / apply:188 / transform:206 / transform_down:222 / transform_up:254 / map_children:439`）、`TreeNodeVisitor:465`、`TreeNodeRewriter:502`、`TreeNodeRecursion:521`、`Transformed<T>:589`、`DynTreeNode:875`、`ConcreteTreeNode:917`。
- `LogicalPlan` 实现 `DynTreeNode`（`logical_plan/src/treenode.rs:15-38`），`with_new_arc_children` 用 `Arc::ptr_eq` 短路，避免无变化时重建节点。
- `LogicalPlan::map_expressions`（`treenode.rs:41-232`）：**只覆盖 Project/Filter/Repartition(Hash)/UDFProject/Sort/Explode 以及 Source 的 pushdown filters**，其余分支 `Transformed::no`；显式 TODO 说明 join 谓词暂不支持（`:47-48`）。
- 规则接口：`trait OptimizerRule { fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> }`（`optimization/rules/rule.rs:10-13`）。
- 规则全集见 `optimization/rules/`（32 个文件，含 `push_down_filter/projection/limit/aggregation`、`lift_project_from_agg`、`split_explode_from_project`、`eliminate_subquery_alias`、`materialize_scans`、`unnest_subquery`、`reorder_joins/`、`enrich_with_stats` 等）。
- 驱动器 `optimization/optimizer.rs`：配置 `OptimizerConfig`（`:24`），默认最多 20 轮（`:39-42`）；批结构 `RuleBatch`（`:60-65`）；策略 `RuleExecutionStrategy::{Once, FixedPoint}`（`:96-104`）。
- Python 侧表达式访问器：`daft-dsl/src/visitor.rs:21-60` 的 `accept()` 按 Rust 枚举分支显式派发到 Python visitor 方法（非 accept 模式）。

## 2. 表达式系统

### 2.1 `Expr` 变体与相关枚举

- `Expr`（`daft-dsl/src/expr/mod.rs:222-307`，`pub type ExprRef = Arc<Expr>` 在 `:218`）：
  `Column`、`Alias(expr,name)`、`Agg(AggExpr)`、`BinaryOp{op,left,right}`、`Cast(expr,dtype,try_cast)`、`Function{func: FunctionExpr, inputs}`、`Over(WindowExpr, WindowSpec)`、`WindowFunction(WindowExpr)`、`Not`、`IsNull`、`NotNull`、`FillNull`、`IsIn`、`Between`、`List`、`Literal`、`IfElse{if_true,if_false,predicate}`、`ScalarFn(ScalarFn)`、`Subquery`、`InSubquery`、`Exists`、`Coalesce`、`VLLM(VLLMExpr)`。
- `Column` 三级（`expr/mod.rs:115-123`）：`Unresolved(UnresolvedColumn{name, plan_ref, plan_schema})`（`:148-152`）、`Resolved(ResolvedColumn::Basic | JoinSide(Field,JoinSide) | OuterRef(Field,PlanRef))`（`:155-174`）、`Bound(BoundColumn{index, field})`（`:175-181`，`field` 已标注 deprecated，仅供显示）。
- `PlanRef`（`expr/mod.rs:125-134`）：`Alias(Arc<str>) | Unqualified | Id(usize)`。
- `Operator`（`daft-core/src/operator.rs:8-31`）：Eq/EqNullSafe/NotEq/Lt/LtEq/Gt/GtEq/Plus/Minus/Multiply/TrueDivide/FloorDivide/Modulus/And/Or/Xor/ShiftLeft/ShiftRight；`is_comparison()`（`:33+`）。
- `AggExpr`（`expr/mod.rs:395-495`）：Count/CountDistinct/Sum/Product/ApproxPercentile/ApproxCountDistinct/ApproxSketch/MergeSketch/Mean/Percentile/Stddev/Var/Min/Max/BoolAnd/BoolOr/AnyValue/List/Set/Concat/Median/Skew/MapGroups/`AggFn{handle,inputs}`，以及规划器内部三态 `AggFnMap`/`AggFnCombine`/`AggFnReduce`（map-combine-reduce 聚合）。
- `WindowExpr`（`expr/mod.rs:503+`）：`Agg(AggExpr)`、RowNumber、Rank、DenseRank、`Offset{input, offset, default}`（offset>0 为 LEAD，<0 为 LAG）。

### 2.2 函数组织与注册机制

- `FunctionExpr`（`daft-dsl/src/functions/mod.rs:36-42`）：`Map | Sketch | Struct | Python(LegacyPythonUDF) | Partitioning`，`get_evaluator()` 分派（`:55-66`）。
- `trait FunctionEvaluator`（`functions/mod.rs:44-53`）：`fn_name()` / `to_field(inputs, schema, expr)` / `evaluate(inputs: &[Series], expr) -> Series`；`FunctionExpr` 自身实现该 trait 做转发（`:74-91`）。
- 新式标量函数：`trait ScalarUDF`（`functions/scalar.rs:205+`，要求 `name`/`call`/`get_return_field`/`docstring`，见 `:256`），异步版 `AsyncScalarUDF`（`:266`）；`ScalarFn`（`:26-38`）持有 `BuiltinScalarFnVariant::{Sync,Async}`（`:40-42`），`ScalarFn::builtin(udf, inputs)` 是构造入口（`:34`）；`ScalarFunctionFactory`（`:179`）负责按输入生成函数实例。
- `FunctionRegistry`（`functions/mod.rs:132-192`）：`HashMap<String, Arc<dyn ScalarFunctionFactory>>`；`add_fn`（旧式单态化，`:167-173`）、`add_fn_factory`（同时注册 `aliases()`，`:158-164`）、`add_async_fn`（`:175-180`）、`get/entries`（`:182-188`）；全局单例 `pub static FUNCTION_REGISTRY: LazyLock<RwLock<FunctionRegistry>>`（`:191-192`）；分组注册靠 `trait FunctionModule { fn register(&mut FunctionRegistry) }`（`:139-142`）。
- 单个函数的写法（典型样板）`daft-functions-utf8/src/lower.rs`：定义 `pub struct Lower`（`:15`），`impl ScalarUDF for Lower` 实现 `name="lower"`/`call`/`get_return_field`（`:18-43`），并提供 `pub fn lower(input: ExprRef) -> ExprRef { ScalarFn::builtin(Lower, vec![input]).into() }`（`:51`）。`#[typetag::serde]` 是函数能被 serde 派发的关键。
- 模块汇总：`daft-functions-utf8/src/lib.rs:93` `impl FunctionModule for Utf8Functions`，在 `register` 中逐个 `parent.add_fn(...)`。
- **全局只注册一次**：`src/lib.rs:165-196` 在 Python 模块初始化时把 Numeric/Float/Uri/Image/Binary/List/Utf8/Json/Serde/Temporal/Misc/Distance/Similarity/Tokenize/Random/Spatial 各模块以及 `coalesce`、`daft_file::*`、`monotonically_increasing_id` 等写进 `FUNCTION_REGISTRY`。因此 `daft-dsl` 本身不依赖具体函数库（避免循环依赖）。
- **不存在 `#[daft_function]` 宏**。Rust 侧的外部扩展宏在 `src/daft-ext-macros/src/lib.rs`：`daft_extension`（`:28`）、`daft_func_batch`（`:97`，必填 `return_dtype=`，可选 `name=`，参数解析 `:115-150`）、`daft_func`（`:275`）；由 `src/daft-ext/src/lib.rs` 统一 `pub use daft_ext_macros::*` 暴露。扩展函数句柄在 worker 端靠模块注册表重新挂载（`daft-ext-internal/src/function.rs:507+` 的 serde roundtrip 测试）。

### 2.3 表达式解析（resolution）

- 解析器入口：`daft-logical-plan/src/builder/resolve_expr.rs`。`ExprResolver`（`:291-297`）携带 `allow_actor_pool_udf / allow_monotonic_id / allow_explode / in_agg_context / groupby`；`check_expr`（`:339-366`）据此拒绝越权函数（如非投影位置的 `explode()`、`monotonically_increasing_id()`、带 `concurrency` 的 UDF）。
- 通配符 `col("*")`：`expand_wildcard`（`:21-126`）先遍历表达式树定位唯一的通配符（多个通配符报错，`:29-38`），按 `PlanRef::{Alias, Id, Unqualified}` 取对应 schema 的 `field_names()` 展开（`:47-69`），并支持 `struct.get("*")`（`:72-87`）。
- 列名解析：`col_resolves_to_plan`（`:129-157`）判断未解析列是否属于当前 plan/别名/plan id；`resolve_to_basic_and_outer_cols`（`:236-256`）把可解析列改写成 `ResolvedColumn::Basic`，若带 `plan_schema` 则改成 `OuterRef`（相关子查询），否则报 `FieldNotFound("Column {e} not found.")`（`:249`）。
- `list_map/list_filter` 的 lambda 变量 `col("")` 会被替换为展开元素（`resolve_list_evals :177-234` + `replace_element_with_column_ref :159-175`）；Python UDF 在聚合上下文被改写为 `AggExpr::MapGroups`（`convert_udfs_to_map_groups :258-285`，行式 UDF 直接报错）。
- 歧义处理在 schema 层：`Schema::get_field/get_index` 在名字对应多个下标时返回 `DaftError::AmbiguousReference("Column name ... is ambiguous in schema")`（`daft-schema/src/schema.rs:129-145`、`:153-166`）。SQL 层另有"join 谓词列归属歧义"检查（`daft-sql/src/planner.rs:1453-1459`：`Ambiguous column reference in join predicate`）。

### 2.4 类型推导与 nullability

- `Expr::to_field(&Schema) -> Field`（`expr/mod.rs:2085+`）是唯一类型推导入口；`get_type`（`:2375-2377`）与 `get_name`（`:2379-2381`）都是它的薄封装。整棵投影/聚合的 schema 由 `exprs_to_schema`（`:2794-2800`）批量调用得到，重名去重见 `deduplicate_expr_names`（`:2813+`）。
- 规则示例：`Alias` → 名字取自 alias、类型取子表达式（`:2087`）；`Cast` → 直接取目标 dtype（`:2089`）；`Not` → 只接受 Boolean 或 Null，输出 Boolean（`:2111-2121`）；`IsNull/NotNull` → Boolean（`:2122-2123`）；`FillNull` → 两侧 `try_get_supertype`（`:2124-2133`）；`Literal` → 字段名固定为 `"literal"`（`:2170`）。
- `BinaryOp` 按运算符类别分派到 `InferDataType`：逻辑 `logical_op`、比较 `comparison_op`、算术等（`:2173-2199`）；`InferDataType` 定义在 `daft-core/src/datatypes/infer_datatype.rs:14`（`logical_op:34`、`comparison_op:107`、`membership_op:198`）。
- 聚合：`AggExpr::to_field`（`expr/mod.rs:885+`）——Count/CountDistinct →`UInt64`（`:887-890`）、Sum → `try_sum_supertype`（`:891-897`）、ApproxPercentile 依据 percentiles 个数产出 `Float64` 或 `FixedSizeList(Float64, n)`（`:906-933`）。
- **nullability 不在 Daft 的 `Field` 里**：`daft-schema/src/field.rs:27-31` 只有 `name/dtype/metadata`；只有导出到 Arrow 时默认 `nullable=true`（`field.rs:162`）。因此类型推导只决定 **name + dtype**，不追踪可空性；`DataType::Null` 仅在比较/逻辑推理里作为"未知类型"参与（如 `:2116`、`infer_datatype.rs:34-56`）。
- 语义标识：`Expr::semantic_id(schema) -> FieldID`（`:646`、`:1120`、`:1528`），`ScalarFn`/函数各有 `scalar_function_semantic_id`/`function_semantic_id`（`functions/mod.rs:121-129`），用于 CSE 与聚合内部别名。

### 2.5 序列化与求值路径

- 序列化：`Expr`/`Column`/`LogicalPlan` 均 derive `Serialize/Deserialize`（`expr/mod.rs:220-222`、`logical_plan.rs:30`）。例外——`Subquery` 的 serde 直接返回错误（`expr/mod.rs:81-93`："Subquery cannot be serialized"），`ScanState::Operator` 同理（`daft-scan/src/scan_state.rs:36-45`）。
- 分发：`DistributedPhysicalPlan{query_idx, query_id, logical_plan: Arc<LogicalPlan>, config}` 也是 serde 结构（`daft-distributed/src/plan/mod.rs:34-39`）；`PyDistributedPhysicalPlan` 用 bincode 做 Python pickle（`daft-distributed/src/python/mod.rs:234` + `common/py-serde/src/python.rs:93-120`，`bincode::config::legacy()`）。表达式内嵌的 Python 对象走 `RuntimePyObject` + pickle 字节（`common/py-serde/src/python.rs:27-38`）。
- 列绑定：`BoundExpr::try_new`（`daft-dsl/src/expr/bound_expr.rs:25-57`）把 `Unresolved`/`Resolved::Basic` 通过 `schema.get_index(name)` 换成 `Column::Bound(index)`；`JoinSide`/`OuterRef` 禁止绑定（`:41-50`）；`bind_all`（`:81-89`）。类型层面用 `BoundExpr`/`BoundAggExpr`/`BoundWindowExpr` 区分"已绑定/未绑定"。
- 物理化：`daft-local-plan/src/translate.rs:21` 的 `translate()` 逐算子绑定表达式并生成 LocalPhysicalPlan（Filter `:104`、Project `:157`、Aggregate `:225`、Window `:259-261`、Join `:477-478`、Sort `:378`…）。
- 执行：`RecordBatch::eval_expression`（`daft-recordbatch/src/lib.rs:1281-1284`）→ `eval_expression_internal`（`:1294+`）；`Column::Bound` 直接按 index 取列（`:1322-1324`）；`Expr::Function` 求值子节点后调 `func.evaluate(series, func)`（`:1414-1423`）；`Expr::ScalarFn(Builtin)` 求值参数后按同步/异步分派 `BuiltinScalarFnVariant::Sync(f).call(args,&ctx)` / `Async` 用 `get_compute_runtime().block_on_current_thread(...)`（`:1424-1458`），`EvalContext{row_count}` 提供行数（`:1449-1451`）。
- `Expr::Over` 不能直接求值，必须走 window 算子（`:1307-1309`）；`Expr::VLLM` 在常规路径 `unreachable!`（`:1194`、`:1526`）。

## 3. SQL 前端

### 3.1 parser 选型

- **没有 `src/daft-sqlparser` crate**（该目录不存在，`Cargo.toml` workspace members 中也没有）。直接用 crates.io 的 `sqlparser`：`Cargo.toml:364` `sqlparser = "0.59.0"`，使用方 `daft-sql/Cargo.toml:21` 与 `daft-catalog/Cargo.toml:13`。`Cargo.toml` 中**没有 `[patch]` 段**，即非 fork/非 vendored。
- 方言用 `GenericDialect`（`daft-sql/src/planner.rs:36` 导入，`:318` 构造 `Tokenizer::new(&GenericDialect{}, input)`）；先 `tokenize_with_location()` 再交给 `Parser`，以便生成 caret 定位错误（`planner.rs:50-95`、`:317-345`）。

### 3.2 AST → LogicalPlan 的结构

- `SQLPlanner`（`planner.rs:165-178`）字段：`context: Rc<RefCell<PlannerContext>>`（共享 CTE 绑定，`:140-158`）、`parent: Option<&Self>`（外层作用域）、`current_plan: Option<LogicalPlanBuilder>`、`right_side_plan`（join 右表，供谓词解析）、`bound_columns: Bindings<ExprRef>`（SELECT 别名，可先于 schema 生效）、`session`。`Bindings<T>` 是 `HashMap<String,T>`（`:99-129`）。
- 作用域通过 `new_child()`（`:184-193`）派生，子查询/外层引用由此实现；`set_plan/update_plan`（`:240-256`）维护当前 builder。
- **没有 `Relation` trait**：关系规划是 `plan_relation`（`:1027`）→ `plan_relation_table`（`:1356-1372`，先查 `bound_ctes` 再 `session.get_table`）→ 表函数（`:1032-1043`）。表因子支持面见 `:1064`（Derived 子查询）；`TableFunction/Function/UNNEST/JsonTable/NestedJoin/Pivot/Unpivot/MatchRecognize/OpenJsonTable/XmlTable/SemanticView` 一律 unsupported（`:1075-1105`）。
- 顶层语句：`plan_statement`（`statement.rs:65-107`）只处理 `Query`、`Explain/DESCRIBE`、`ExplainTable`、`ShowTables`、`Use`、`CreateTable`，其余 `unsupported_sql_err!`。`Statement` 枚举（`statement.rs:13-24`）：`Select(LogicalPlanRef) | Set | ShowTables | Use | CreateTable`。
- 执行：`execute_statement`（`exec.rs:27-40`）分派；`execute_select` 直接返回逻辑计划（`:42-44`），`Use/ShowTables/CreateTable` 走 catalog（`:50-100`）。
- catalog：`daft-catalog` 的 `trait Table`（`table.rs:42-68`：`name/schema/to_logical_plan/append/overwrite/to_py`）与 `TableSource::Schema | View(LogicalPlanRef)`（`table.rs:15-20`）；`trait Catalog`（`catalog.rs:12`）；session 侧入口 `Session::get_table`（`daft-session/src/session.rs:335`）/`get_catalog`（`:254`）。`daft-catalog/src/impls/memory.rs` 是内存实现。

### 3.3 支持范围（源码体现）

- `plan_query`（`planner.rs:398-…`）：`SetExpr::Select` 支持；`SetOperation` 支持 `UNION [ALL|DISTINCT|BY NAME]`、`INTERSECT [ALL]`，映射到 `builder.union/intersect`（`:404-461`）；`VALUES/INSERT/UPDATE/DELETE/MERGE/TABLE` 明确不支持（`:463-468`）。
- 子句：CTE `plan_ctes`（`:290+`，递归 CTE 与 `MATERIALIZED` 不支持）；FROM/JOIN `:477-478`；WHERE `:489-492`；GROUP BY 含单层 `ROLLUP`（`:498-522`）；ORDER BY `:525-542`；HAVING `:547-551`；DISTINCT / DISTINCT ON `:558-570`；LIMIT/OFFSET `:575+`（注释说明与 DataFrame 不同，强制 OFFSET 在 LIMIT 之下）。
- 明令不支持的特性集中在 `check_query_features`（`:2328-2352`：LIMIT BY、FETCH、LOCKS、SETTINGS、FORMAT）与 `check_select_features`（`:2367-2401`：TOP、INTO、LATERAL、PREWHERE、CLUSTER BY、DISTRIBUTE BY、SORT BY、WINDOW 命名窗口、QUALIFY、CONNECT BY）。
- SQL 函数注册：`SQL_FUNCTIONS` 单例（`functions.rs:69-99`）先注册 `SQLModule{Aggs,Map,Partitioning,Python,Sketch,Structs,Config,Temporal,Window}` 与 `concat/element/coalesce`，随后**把 `FUNCTION_REGISTRY` 里所有标量函数自动注册为 SQL 透传**（`:84-97`）。窗口函数 `row_number/rank/dense_rank/lag/lead` 在 `modules/window.rs:164-172`，窗口规格解析 `parse_window_spec`（`functions.rs:557-611`，命名窗口不支持 `:611`）。表函数注册在 `table_provider/mod.rs:34-38`：`read_csv/read_deltalake/read_iceberg/read_json/read_parquet`。
- Python 入口：`daft/sql/sql.py:77` `def sql(sql, register_globals=True, **bindings)`；`:147-168` 依次收集调用栈全局 DataFrame 与显式 bindings 作为 CTE；`:170-172` 取 `daft.current_session()._session` 后调 `_sql_exec`；`:177-178` 把返回的 `PyLogicalPlanBuilder` 包成 `DataFrame`。Rust 侧 `daft-sql/src/python.rs:39` 的 `sql_exec` 调 `execute_statement`，再 `LogicalPlanBuilder::new(plan, Some(config))`（`:52-56`）。`Session.sql` 在 `daft/session.py:153`，同样调用 `sql_exec`（`session.py:164`）。

### 3.4 SQL 与 DataFrame 的统一

- 两者产出同一个 `LogicalPlanBuilder`：`statement.rs:5`/`planner.rs:23` 直接使用 `daft_logical_plan::LogicalPlanBuilder`；`execute_select` 返回 `LogicalPlanRef`，`python.rs:52-56` 只是把它重新包回 builder。
- 因此 DataFrame 的 `select/filter/aggregate/join` 与 SQL 各子句共用同一套算子与解析器（§1.4）；CTE 与 Python 变量都以 `LogicalPlanBuilder` 注入（`planner.rs:230-237` 对 CTE 强制加 `alias` 以满足名称解析）。

## 4. 源码地图

| 文件/目录 | 职责 | 关键类型 |
|---|---|---|
| `src/daft-logical-plan/src/logical_plan.rs` | 逻辑计划枚举与公共能力 | `LogicalPlan`、`LogicalPlanRef`、`RequiredCols`、`SubqueryAlias` |
| `src/daft-logical-plan/src/ops/*.rs` | 逐算子定义与 schema 推导 | `Aggregate`、`Join`、`Project`、`Source`、`Explode`、`Window`、`SetQuantifier` |
| `src/daft-logical-plan/src/source_info.rs` | 数据源抽象 | `SourceInfo`、`InMemoryInfo`、`GlobScanInfo`、`PlaceHolderInfo` |
| `src/daft-logical-plan/src/scan_builder.rs` | Python 侧扫描算子构造 | `ScanOperatorRef` 组装、文件类 source config |
| `src/daft-logical-plan/src/builder/mod.rs` | 构链式 builder（Rust + PyO3 双份） | `LogicalPlanBuilder`、`PyLogicalPlanBuilder` |
| `src/daft-logical-plan/src/builder/resolve_expr.rs` | 表达式解析/通配符/作用域 | `ExprResolver`、`expand_wildcard` |
| `src/daft-logical-plan/src/treenode.rs` | 计划树遍历挂钩 | `DynTreeNode` 实现、`map_expressions` |
| `src/daft-logical-plan/src/optimization/` | 规则式优化器 | `OptimizerRule`、`RuleBatch`、32 条规则、`LogicalPlanTracker` |
| `src/daft-logical-plan/src/partitioning.rs` | 分区/聚簇规格 | `RepartitionSpec`、`ClusteringSpec` |
| `src/common/treenode/src/lib.rs` | 通用树遍历框架 | `TreeNode`、`TreeNodeRewriter`、`Transformed` |
| `src/daft-dsl/src/expr/mod.rs` | 表达式枚举与类型推导 | `Expr`、`Column`、`AggExpr`、`WindowExpr`、`PlanRef`、`exprs_to_schema` |
| `src/daft-dsl/src/expr/bound_expr.rs` | 列绑定 | `BoundExpr`、`BoundColumn` |
| `src/daft-dsl/src/functions/` | 函数框架与注册表 | `FunctionExpr`、`FunctionEvaluator`、`FunctionRegistry`、`ScalarUDF`、`ScalarFn` |
| `src/daft-dsl/src/visitor.rs` | Python 表达式访问器 | `accept()` 派发 |
| `src/daft-core/src/operator.rs` | 二元/逻辑运算符 | `Operator` |
| `src/daft-core/src/datatypes/infer_datatype.rs` | 运算类型推导 | `InferDataType`（logical/comparison/membership） |
| `src/daft-functions*/` | 内置函数实现（utf8/list/json/temporal/image/…） | 各 `ScalarUDF` + `FunctionModule`（如 `Utf8Functions`） |
| `src/daft-ext-macros/`、`src/daft-ext*/` | 外部扩展 ABI 与宏 | `daft_func`、`daft_func_batch`、`daft_extension` |
| `src/daft-scan/src/scan_operator.rs` | 扫描算子 trait | `ScanOperator`、`ScanOperatorRef` |
| `src/daft-scan/src/scan_state.rs`、`pushdowns.rs` | 扫描状态与下推 | `ScanState`、`Pushdowns` |
| `src/daft-sql/src/planner.rs` | AST → 逻辑计划 | `SQLPlanner`、`PlanRef` 解析、`Bindings` |
| `src/daft-sql/src/statement.rs`、`exec.rs` | 语句规划与执行分派 | `Statement`、`execute_statement` |
| `src/daft-sql/src/functions.rs`、`modules/` | SQL 函数/窗口/聚合注册 | `SQL_FUNCTIONS`、`SQLFunction`、`SQLModuleWindow` |
| `src/daft-sql/src/table_provider/` | SQL 表函数 | `ReadParquetFunction`、`ReadCsvFunction`、`ReadIceberg` |
| `src/daft-catalog/src/{catalog,table,identifier}.rs` | catalog/表抽象 | `Catalog`、`Table`、`TableSource`、`Identifier` |
| `src/daft-session/src/session.rs` | 会话：catalog/namespace/函数 | `Session::get_table/get_catalog/set_catalog` |
| `src/daft-local-plan/src/translate.rs` | 逻辑计划 → 本地物理计划（表达式绑定） | `translate`、`BoundExpr::bind_all` |
| `src/daft-recordbatch/src/lib.rs` | 表达式求值入口 | `eval_expression`、`eval_expression_internal` |
| `daft/sql/sql.py`、`daft/session.py` | Python SQL 入口与 CTE 绑定 | `daft.sql()`、`Session.sql` |
