# Daft 优化器 / 统计信息 / 逻辑→物理计划 源码调研

- 仓库：`D:\DOC\daft-src`，commit `dadd8a0b290be148d92acb6f9e6fc4b6e36f221f`（2026-09-25，`chore: Upgrade Rust nightly (#7393)`）
- 约定：文中路径均相对 `D:\DOC\daft-src`；所有结论均给 `相对路径:行号` 证据。
- 只读调研，未修改仓库任何文件。

## 1. 优化器框架

### 1.1 `OptimizerRule` trait：只有一个方法

```rust
// src/daft-logical-plan/src/optimization/rules/rule.rs:8-14
/// A logical plan optimization rule.
pub trait OptimizerRule {
    /// Try to optimize the logical plan with this rule.
    ///
    /// This returns Transformed::yes(new_plan) if the rule modified the plan, Transformed::no(old_plan) otherwise.
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>>;
}
```

- 没有 `apply`/`transform` 抽象方法：**遍历原语由每条规则自己在 `try_optimize` 内调用**。可选原语在 `src/common/treenode/src/lib.rs`：
  - `transform` == `transform_up`（后序）：`src/common/treenode/src/lib.rs:202-211`；
  - `transform_down`（前序）：`src/common/treenode/src/lib.rs:222-234`；
  - 另有 `transform_down_up` / `rewrite(TreeNodeRewriter)`（同文件注释 :220-221）。
  - 因此判断"自顶向下/自底向上"必须看规则内用的是 `transform_down` 还是 `transform`。

### 1.2 `OptimizerConfig` / `RuleBatch` / `RuleExecutionStrategy`

```rust
// src/daft-logical-plan/src/optimization/optimizer.rs:24-43
pub struct OptimizerConfig {
    // Default maximum number of optimization passes the optimizer will make over a fixed-point RuleBatch.
    pub default_max_optimizer_passes: usize,
    pub strict_pushdown: bool,
}
impl Default for OptimizerConfig {
    fn default() -> Self {
        // Default to a max of 20 optimizer passes for a given batch.
        Self::new(20, false)   // -> default_max_optimizer_passes = 20, strict_pushdown = false
    }
}
```

- `RuleBatch { rules, strategy }`：`optimizer.rs:60-65`；`RuleExecutionStrategy::{Once, FixedPoint(Option<usize>)}`：`optimizer.rs:96-106`。
- `max_passes`：`Once => 1`，`FixedPoint(None) => config.default_max_optimizer_passes`：`optimizer.rs:84-91`。
- debug 构建下 `OptimizerRuleInBatch: OptimizerRule + Debug`，release 下与 `OptimizerRule` 等价（仅为规则 batch 打日志）：`optimizer.rs:46-56`。

### 1.3 多轮迭代与终止条件（固定点 / 环检测）

- `Optimizer::optimize(plan, observer)`：按顺序对每个 `RuleBatch` 做 `try_fold`：`optimizer.rs:318-332`。
- `optimize_with_rule_batch`：对 `0..batch.max_passes()` 逐轮调用 `optimize_with_rules`：`optimizer.rs:345-386`。
  - 某轮返回 `transformed == false` ⇒ 认为到达固定点，`ControlFlow::Break`：`optimizer.rs:367-376`。
  - 某轮返回 `transformed == true` 但新 plan 已见过 ⇒ 判定为**环**，提前停止该 batch：`optimizer.rs:355-365`。
- 环检测器 `LogicalPlanTracker`：`HashSet<LogicalPlanDigest { plan_hash, node_count }>`，用 plan 的 Hash + 节点数做廉价摘要：`src/daft-logical-plan/src/optimization/logical_plan_tracker.rs:20-27, 53-67`。
- `optimize_with_rules`：一个 batch 内对规则做 `try_fold`，逐条 `plan.transform_data(|data| rule.try_optimize(data))`——**规则串行、共享同一棵树**：`optimizer.rs:388-398`。
- `observer` 回调签名 `FnMut(&LogicalPlan, &RuleBatch, pass, transformed, seen)`：`optimizer.rs:324`；实际用于打印每条规则后的 plan：`src/daft-logical-plan/src/builder/mod.rs:1140-1157`。

### 1.4 完整的规则执行序列（源码顺序）

调度顺序**硬编码**在 `src/daft-logical-plan/src/optimization/optimizer.rs` 中（`OptimizerBuilder::with_default_optimizations` 等），调用点在 `src/daft-logical-plan/src/builder/mod.rs`。

| # | 规则 | 文件:行 | 策略 |
|---|---|---|---|
| 1 | `LiftProjectFromAgg::new()` | `optimizer.rs:129` | FixedPoint(None) |
| 2 | `RewriteCountDistinct::new()` | `optimizer.rs:130` | 同上 |
| 3 | `UnnestScalarSubquery::new()` | `optimizer.rs:131` | 同上 |
| 4 | `UnnestPredicateSubquery::new()` | `optimizer.rs:132` | 同上 |
| 5 | `EliminateSubqueryAliasRule::new()` | `optimizer.rs:133` | 同上 |
| 6 | `ExtractWindowFunction::new()` | `optimizer.rs:134` | 同上 |
| 7 | `SplitExplodeFromProject::new()` | `optimizer.rs:135` | 同上 |
| 8 | `SimplifyExpressionsRule::new()` | `optimizer.rs:141` | FixedPoint(None) |
| 9 | `FilterNullJoinKey::new()` | `optimizer.rs:147` | **Once** |
| 10 | `PushDownAntiSemiJoin::new()` | `optimizer.rs:155` | FixedPoint(None) |
| 11 | `DropRepartition::new()` | `optimizer.rs:161` | FixedPoint(None) |
| 12 | `DropIntoBatches::new()` | `optimizer.rs:162` | 同上 |
| 13 | `PushDownFilter::new(strict_pushdown)` | `optimizer.rs:163` | 同上 |
| 14 | `PushDownProjection::new()` | `optimizer.rs:164` | 同上 |
| 15 | `EliminateCrossJoin::new()` | `optimizer.rs:165` | 同上 |
| 16 | `SimplifyNullFilteredJoin::new()` | `optimizer.rs:166` | 同上 |
| 17 | `PushDownJoinPredicate::new()` | `optimizer.rs:167` | 同上 |
| 18 | `EliminateOffsets::new()` | `optimizer.rs:168` | 同上 |
| 19 | `RewriteOffset::new()` | `optimizer.rs:182` | FixedPoint(Some(3)) |
| 20 | `PushDownLimit::new()` | `optimizer.rs:183` | 同上 |
| 21 | `SplitUDFsFromFilters::new()` | `optimizer.rs:193` | **Once** |
| 22 | `SplitUDFs::new()` | `optimizer.rs:194` | 同上 |
| 23 | `SplitVLLM`（unit struct） | `optimizer.rs:195` | 同上 |
| 24 | `PushDownProjection::new()` | `optimizer.rs:196` | 同上 |
| 25 | `DetectMonotonicId::new()` | `optimizer.rs:197` | 同上 |
| 26 | `PushDownProjection::new()` | `optimizer.rs:203` | FixedPoint(None) |
| 27 | `PushDownAggregation::new(strict_pushdown)` | `optimizer.rs:208-210` | **Once** |
| 28 | `PushDownShard::new()` | `optimizer.rs:215` | **Once** |
| 29 | `RewriteCheckpointSource::new()` | `optimizer.rs:220` | **Once** |
| 30 | `SimplifyExpressionsRule::new()` | `optimizer.rs:225` | FixedPoint(None) |
| 31 | `MaterializeScans::new()` | `optimizer.rs:230` | **Once** |
| 32 | `ShardScans::new()` | `optimizer.rs:236` | **Once** |

可选追加批次（`OptimizerBuilder` 方法，由 builder 按配置拼接）：

| # | 规则 | 文件:行 | 条件 |
|---|---|---|---|
| 33 | `ReorderJoins::new(cfg, use_dp_ccp)` + `PushDownFilter` + `PushDownProjection` + `EnrichWithStats` | `optimizer.rs:247-255` | `reorder_joins()`，`!disable_join_reordering` |
| — | `EnrichWithStats::new(cfg)` | `optimizer.rs:259-265` | `enrich_with_stats()` |
| — | `SimplifyExpressionsRule::new()` | `optimizer.rs:267-274` | `simplify_expressions()` |
| — | `SplitGranularProjection::new()` | `optimizer.rs:276-282` | `split_granular_projections()` |

真实拼装顺序（native 同步路径，`LogicalPlanBuilder::optimize`）：`OptimizerConfig{strict_pushdown}` → `with_default_optimizations()` → `enrich_with_stats()` → `reorder_joins()` → `simplify_expressions()` → `split_granular_projections()` → `enrich_with_stats()`：`src/daft-logical-plan/src/builder/mod.rs:1109-1136`。
异步路径 `optimize_async` 的批次顺序略有不同（`reorder_joins` 在 `simplify_expressions`/`split_granular_projections` 之前）：`builder/mod.rs:1035-1061`。
优化完成后若设置 `DAFT_INSTRUMENT_LOGICAL_PLAN`，会为每个节点分配 `node_id`：`builder/mod.rs:1161-1166`，实现 `assign_node_ids`：`builder/mod.rs:1171-1207`。

### 1.5 规则分类（文件路径）

- **pushdown**：`rules/push_down_filter.rs`、`push_down_projection.rs`、`push_down_limit.rs`、`push_down_aggregation.rs`、`push_down_shard.rs`、`push_down_anti_semi_join.rs`、`push_down_join_predicate.rs`（目录 `rules/mod.rs:14-19`）。
- **rewrite**：`rules/simplify_expressions.rs`、`rewrite_count_distinct.rs`、`rewrite_offset.rs`、`rewrite_checkpoint_source.rs`、`unnest_subquery.rs`、`lift_project_from_agg.rs`、`split_explode_from_project.rs`、`extract_window_function.rs`、`eliminate_subquery_alias.rs`。
- **elimination**：`rules/eliminate_cross_join.rs`、`eliminate_offsets.rs`、`drop_repartition.rs`、`drop_into_batches.rs`、`filter_null_join_key.rs`、`simplify_null_filtered_join.rs`。
- **join 相关**：`rules/reorder_joins/`（`mod.rs`、`join_graph.rs`、`relation_set.rs`、`brute_force_join_order.rs`、`dp_ccp_join_order.rs`、`naive_left_deep_join_order.rs`）+ `push_down_anti_semi_join.rs`、`push_down_join_predicate.rs`、`filter_null_join_key.rs`。
- **UDF 相关**：`rules/split_udfs.rs`（`SplitUDFs`、`SplitUDFsFromFilters`）、`rules/split_vllm.rs`。
- **repartition / 并发与 morsel**：`rules/drop_repartition.rs`、`rules/granular_projections.rs`（`SplitGranularProjection`）、`rules/drop_into_batches.rs`。
- **scan 物化与分片**：`rules/materialize_scans.rs`、`rules/shard_scans.rs`、`rules/push_down_shard.rs`。
- **其他**：`rules/detect_monotonic_id.rs`、`rules/enrich_with_stats.rs`。
- 说明：**不存在**名为 `PushDownShuffle` 的规则（`grep -rn "PushDownShuffle" src/` 无命中）；`JoinReorder`/`join strategy` 见 §2.5。

### 1.6 schema 正确性与计划一致性

- **没有全局 `assert_schema` 检查**：`grep -rn "assert_schema\|verify_schema" src/daft-logical-plan src/daft-local-plan src/daft-distributed` 只命中测试断言 `eliminate_cross_join.rs:498`。
- schema 由各算子的构造器在构造时推导并缓存，例如 `Project::try_new` 逐表达式 `expr.to_field(input.schema())` 生成 `projected_schema`：`src/daft-logical-plan/src/ops/project.rs:48-60`；`LogicalPlan::schema()` 统一按节点类型返回缓存的 schema（`Source/Project/UDFProject/Aggregate/Join/...`）：`src/daft-logical-plan/src/logical_plan.rs:153-194`。
- 二次校验点在算子语义层面：`Concat` 要求两侧 schema 相等否则报错 `src/daft-logical-plan/src/ops/concat.rs:31`；`Join` 在 :264、集合算子 `set_operations.rs:70,88,291,328` 抛 `SchemaMismatch`。
- 物理翻译阶段把"逻辑算子应当已被优化掉"作为硬断言：`Offset/Union/Intersect/SubqueryAlias` 走到 translate 即报 `InternalError("Logical plan operator {} should already be optimized away")`：`src/daft-local-plan/src/translate.rs:703-709`（分布式侧同样：`src/daft-distributed/src/pipeline_node/translate.rs:683-688`）。
- 测试侧的"plan 一致性"断言：`assert_optimized_plan_with_rules_eq`：`src/daft-logical-plan/src/optimization/test/mod.rs:13`；比较方式是 `repr_ascii` 文本比较，如 `optimizer.rs:768-774`。
- `AlwaysSame<PlanStats>` 让带了 stats 的 plan 在 `PartialEq/Eq/Hash` 上**忽略 stats**（stats 全等），使"优化后 plan 相等性 + 环检测"不被 stats 干扰：`src/daft-logical-plan/src/stats.rs:59-96`。

## 2. 规则逐条剖析

### 2.1 pushdown 类（Filter / Projection / Limit / Shard / Aggregation / Anti-Semi / JoinPredicate / NullJoinKey / Offset 族）

| 规则 | 结构体/构造器 | 入口 + 遍历方向 | 关键守卫 |
|---|---|---|---|
| `PushDownFilter` | `{strict_pushdown: bool}`，`new(strict_pushdown)` `push_down_filter.rs:25-33` | `try_optimize` :37 → `transform_down` :38（前序） | Filter∘Source/Project/Filter/Join/Sort/Shuffle/Repartition/IntoBatches/IntoPartitions/Concat；其余阻挡 :463-484 |
| `PushDownProjection` | `{}`，`new()` :20-26 | :684 → `transform_down` :685 | 分派 Project/UDFProject/Aggregate/Join/Pivot :659-680；阻挡 :507-526 |
| `PushDownLimit` | `{}`，`new()` :19-25 | :39 → `transform_down` :40 | 只穿 Repartition/IntoBatches/IntoPartitions/Project(无 Explode)/Source/Limit/Sort/Join(Left,Right)；阻挡 :289-310 |
| `PushDownShard` | `{}`，`new()` :11-16 | :20 → `transform_down` :21 | Shard 穿 22 种算子 :37-65；Shard∘Source 写入 `pushdowns.sharder` :88-98；Shard∘Join 报错 :108-110 |
| `PushDownAggregation` | `{strict_pushdown: bool}` :15-22 | :26 → **`transform`(=transform_up, 后序)** :27 | 仅空 groupby + 单 `count` 聚合，且 Input 必须是 Source :28-46 |
| `PushDownAntiSemiJoin` | `{}` :99-104 | :108 → `transform_down` :109 | 仅 Anti/Semi Join :110-120；左侧须 Project(裸列/裸列别名) :130-147 或 Inner Join :214-278 |
| `PushDownJoinPredicate` | `{}` :22-27 | :31 → **`transform`(后序)** :32 | 按 join type 决定左右可推 :43-50；Anti 左侧谓词取反 :66-71 |
| `FilterNullJoinKey` | `{}` :86-91 | :95 → **`transform`(后序)** :96 | 仅非 null-safe 等值键生成 `key.is_null().not()` :116-138；Anti 只过滤右侧 :107-114 |
| `EliminateOffsets` | `{}` :17-22 | :26 → `transform_down` :27 + 手动递归重跑 :41-43,58-60,94-96 | Offset(0) 消除、相邻 Offset 合并 :47-62、`Limit(x)∘Offset(y) -> Offset(y)∘Limit(x+y)` :79-98 |
| `RewriteOffset` | `{}` :15-20 | :24 → `transform_down` :25 | `Offset(x)∘Limit(y,o) -> Limit(y.saturating_sub(x+o), Some(x+o))` :43-59；无 Limit 报 not_implemented :60-64 |

核心片段（Filter 穿 Join 的左右判定与 anti/semi 特例）：

```rust
// src/daft-logical-plan/src/optimization/rules/push_down_filter.rs:383-403
                    match (
                        pred_cols.is_subset(&left_cols),
                        pred_cols.is_subset(&right_cols),
                    ) {
                        (true, true) => {
                            // predicate columns exist in both left and right input schemas
                            if matches!(join_type, JoinType::Anti | JoinType::Semi)
                                && !pred_cols.iter().all(|c| join_key_cols.contains(c))
                            {
                                // For anti/semi joins, if the predicate column is NOT a join key,
                                // only push to the left side. The output only contains left columns,
                                // so filtering the right side on a non-join-key column would
                                // incorrectly change the join semantics.
                                // (Issue #6086)
                                left_pushdowns.push(predicate);
                            } else {
                                left_pushdowns.push(predicate.clone());
                                right_pushdowns.push(predicate.clone());
                            }
                        }
```

```rust
// src/daft-logical-plan/src/optimization/rules/push_down_limit.rs:259-272
                            if let LogicalPlan::Limit(LogicalLimit {
                                limit: child_limit, ..
                            }) = node
                                && *child_limit <= pushdown_limit
                            {
                                return None;   // 已存在更紧的 Limit -> 整体 no-op，避免反复包裹造成 ping-pong
                            }
                            Some(Arc::new(LogicalPlan::Limit(LogicalLimit::new(
                                child.clone(),
                                pushdown_limit,
                                None,
                                *eager,
                            ))))
```

- **Filter→Scan 的三路划分**：`rewrite_predicate_for_partitioning` 产出 `partition_only_filter / data_only_filter / needing_filter_op`，分别写入 `Pushdowns.partition_filters`、`Pushdowns.filters`，或回退成 Filter 算子：`push_down_filter.rs:125-185`（实现 `src/daft-scan/src/expr_rewriter.rs:84`，`PredicateGroups` 定义 :56-67）。
- **含 UDF 的谓词不进 scan**：`Expr::ScalarFn` / Python `Expr::Function` 被标为 `has_udf` 归入 `needing_filter_op`：`src/daft-scan/src/expr_rewriter.rs:106-120, 138-142`（测试 `push_down_filter.rs:580-595`）。
- **`strict_pushdown` 在两处语义不同**：Filter 里决定是否按 scan 能力筛选谓词（`push_down_filter.rs:188-227`）；Aggregation 里决定"只有全部 filter 都能被 scan 吸收时才下推 count"（`push_down_aggregation.rs:50-66`）。
- **列裁剪（scan `Pushdowns.columns`）只在 `PushDownProjection` 发生**：`required_columns.len() < 上游 schema 列数` 且 scan 未物化（`ScanState::Tasks` 不剪）：`push_down_projection.rs:158-181`。
- **无 volatile/random 守卫**：这些文件里没有"易变表达式"检查；唯一"可重算性"判定来自 `input_mapping()`（要求单列且无计算）：`push_down_filter.rs:267-274`，实现 `src/daft-dsl/src/expr/mod.rs:2383-2394`。`PushDownLimit` 另有 `contains_explode` 守卫：`push_down_limit.rs:27-35, 70-73`。

### 2.2 Projection folding / 列裁剪

- **no-op 投影消除**（`Project` 长度相同、逐列同名裸列 ⇒ 删除该 Project）：`push_down_projection.rs:38-59`。
- **Project-Project 合并/折叠**：只有上游"需计算列"在下游被引用 ≤1 次才合并（`IndexSet::insert` 返回值判定，避免 `unfactor`）：`push_down_projection.rs:64-138`。
- **列裁剪**：Project∘Source 下推 `pushdowns.columns`（:169-181）；Project∘Project（:199-225）；Project∘Aggregate 裁 `aggregations` 但**不裁 groupby**（:237-241）；Project∘UDFProject 裁 `passthrough_columns`（:254-290）；Project∘Distinct（:492-505）。
- **在 unary op 之上插 Project 以缩短上游**：`push_down_projection.rs:291-336`（覆盖 Sort/Shard/Repartition/IntoPartitions/IntoBatches/Limit/Offset/TopN/Filter/Sample/Shuffle/Explode）。

### 2.3 UDF 拆分与细粒度投影拆分

（见 §2.3 补注：`split_udfs.rs` / `split_vllm.rs` / `granular_projections.rs` 细节）

- `SplitGranularProjection`：`transform_up` 后序（`granular_projections.rs:33-37`），只作用于 `Project`（:58-66），把"需要独立 morsel 尺寸"的表达式（当前判定：`Expr::ScalarFn(Builtin(Async(f)))` 且 `f.preferred_batch_size(..)` 有值）拆到独立 Project，子表达式用 `id-{uuid}` 别名 + 列引用替换：`granular_projections.rs:44-51, 75-107`。
- 计划示例（源码注释）：`Project(decode(url_download(...)) as image, name)` → 3 层 Project：`granular_projections.rs:19-29`。
- 注意：它**不是**按 batch size 阈值切分数据，而是把一个 Project 拆成多个 Project，让执行器对特定算子使用其 preferred morsel size。

### 2.4 CrossJoin 消除与子查询重写

- `EliminateCrossJoin`（`eliminate_cross_join.rs`，753 行，含测试；文件头声明重写自 DataFusion）：:1。
- 子查询重写：`unnest_subquery.rs` 提供 `UnnestScalarSubquery` / `UnnestPredicateSubquery`（:21-40 文档给出改写示例：标量子查询 → CROSS JOIN + 过滤），在优化器第一批次执行：`optimizer.rs:131-132`。
- `EliminateSubqueryAliasRule`：只做 alias 消除：`optimizer.rs:133`。
- 详细逻辑见 §2.4 补注。

### 2.5 Join 重排与 join 策略决策

- 规则本体 `ReorderJoins { cfg, use_dp_ccp }`，`new(cfg: Option<Arc<DaftExecutionConfig>>, use_dp_ccp: bool)`：`reorder_joins/mod.rs:27-39`。
- 关系数上限常量：`BRUTE_FORCE_MAX_RELATIONS = 7`（`reorder_joins/mod.rs:21`）、`DP_CCP_MAX_RELATIONS = 12`（:24）；`could_reorder(max_relations)` 不通过则整条规则 no-op：:52-54。
- 算法选择：`use_dp_ccp ? DpCcpJoinOrderer : BruteForceJoinOrderer`：`reorder_joins/mod.rs:55-59`；`use_dp_ccp` 来自 `DaftPlanningConfig::enable_dp_ccp_join_ordering`（`src/common/daft-config/src/lib.rs:68-72`，env `DAFT_DEV_ENABLE_DP_CCP_JOIN_ORDERING`：:86-87, 101-103）。
- 非 Join 节点递归：`rewrite_children` + `plan.map_children`：`reorder_joins/mod.rs:64-73`。
- **broadcast vs hash 决策不在逻辑优化器里，而在分布式物理翻译**：`LogicalPlanToPipelineNodeTranslator::determine_join_strategy(left_on, right_on, join_type, join_strategy, left_stats, right_stats)`：`src/daft-distributed/src/pipeline_node/join/translate_join.rs:22-65`。

```rust
// src/daft-distributed/src/pipeline_node/join/translate_join.rs:56-64
        // If the smaller table is under broadcast size threshold AND we are not broadcasting the side we are outer joining by, use broadcast join
        if smaller_size_bytes <= self.plan_config.config.broadcast_join_size_bytes_threshold
            && smaller_side_is_broadcastable
        {
            JoinStrategy::Broadcast
        // Otherwise, use a hash join
        } else {
            JoinStrategy::Hash
        }
```

- 关键点：显式 `join_strategy` 优先（:31-34）；`Inner + 无 key` ⇒ `Cross`（:37-39）；可广播性按 join type 判定（`Left/Anti/Semi` 只能广播右、`Right` 只能广播左、`Outer` 不可广播）：:49-54。阈值字段 `broadcast_join_size_bytes_threshold`：`src/common/daft-config/src/lib.rs:126`。
- hash join 分区数决策使用 `hash_join_partition_size_leniency`：`translate_join.rs:93-114`（字段 `src/common/daft-config/src/lib.rs:127`）。
- native runner 不支持 Broadcast/SortMerge/KeyFiltering：`src/daft-local-plan/src/translate.rs:429-449`（Broadcast/SortMerge 降级为 hash join 并 warn，KeyFiltering 直接报错）。

### 2.6 其他重要规则

- `DropRepartition`：`transform_down`，只删除"背靠背"的重分区算子（`Repartition∘Repartition`、`Repartition∘IntoPartitions` → 保留下层）：`drop_repartition.rs:22-42`；**"与输入分区方式相同的 Repartition"在逻辑→物理翻译期丢弃**（源码注释）：`drop_repartition.rs:9-12`。
- `RewriteCountDistinct`：`optimizer.rs:130`（详见 §2.6 补注）。
- `ExtractWindowFunction`：`optimizer.rs:134`；窗口函数会被抽出为独立 `Window` 节点，物理翻译按 `(partition_by, order_by, frame)` 组合选择 4 种窗口算子：`src/daft-local-plan/src/translate.rs:263-327`。
- `DetectMonotonicId`：`optimizer.rs:197`；`MonotonicallyIncreasingId` 在 translate 中生成物理算子并携带 `starting_offset`：`translate.rs:595-609`。
- `RewriteCheckpointSource`：`optimizer.rs:220`，必须早于 `MaterializeScans`（注释 :218）。
- `MaterializeScans` / `ShardScans`：`optimizer.rs:229-237`；scan 物化后会触发 `EnrichWithStats` 才能拿到真实行数（`EnrichWithStats` 注释 `enrich_with_stats.rs:29-30`）。
- `OptimizeSkew`：**仓库中不存在该规则名**（未在 `rules/` 下出现）。

## 3. 统计信息

### 3.1 两套"stats"：`PlanStats/ApproxStats`（基数）与 `daft-stats`（列级区间）

```rust
// src/daft-logical-plan/src/stats.rs:105-111
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct ApproxStats {
    pub num_rows: usize,
    pub size_bytes: usize,
    // Accumulated selectivity, i.e. the selectivity of the current operator and its children.
    pub acc_selectivity: f64,
}
```

- `StatsState::{Materialized(AlwaysSame<PlanStats>), NotMaterialized}`：`src/daft-logical-plan/src/stats.rs:6-19`；`PlanStats { approx_stats }`：:21-26；`Accumulated selectivity` 的 `Add` 实现按行数加权平均：:130-155。
- 访问器：`LogicalPlan::stats_state()`：`src/daft-logical-plan/src/logical_plan.rs:435`；`materialized_stats()` 对 `NotMaterialized` **panic**（`stats.rs:13-18`）。
- **列级统计**在 `src/daft-stats`：`TableStatistics { columns: Vec<ColumnRangeStatistics>, schema }`：`src/daft-stats/src/table_stats.rs:21-25`；由两行（min/max）统计表构造 `from_stats_table`：:32-58；`eval_expression` 支持对表达式求区间：:124。
- 列级统计**当前并未参与逻辑优化器决策**——`PlanStats` 注释明确"Currently we're only putting cardinality stats in the plan stats"：`src/daft-logical-plan/src/stats.rs:23-25`。

### 3.2 `EnrichWithStats`：自底向上填充

```rust
// src/daft-logical-plan/src/optimization/rules/enrich_with_stats.rs:29-42
// Add stats to all logical plan nodes in a bottom up fashion.
// All scan nodes MUST be materialized before stats are enriched.
impl OptimizerRule for EnrichWithStats {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        let cfg = self.cfg.clone();
        plan.transform_up(move |node: Arc<LogicalPlan>| {
            let node = Arc::unwrap_or_clone(node);
            if matches!(node.stats_state(), StatsState::Materialized(_)) {
                Ok(Transformed::no(node.arced()))
            } else {
                Ok(Transformed::yes(node.with_materialized_stats(&cfg).into()))
            }
        })
    }
}
```

- 每个算子自己实现 `with_materialized_stats`，统一入口 `LogicalPlan::with_materialized_stats`：`logical_plan.rs:479-509`。

### 3.3 scan 统计来源：parquet/文件 metadata → `ScanTask::approx_num_rows`

- 物化 scan 时先取 `scan_operator.statistics()`；若其 `num_rows` 是 `Precision::Exact` 则短路，直接用该精确行数 × 选择率得到 `ApproxStats`：`src/daft-logical-plan/src/ops/source.rs:91-117`。
- 否则按 scan task 累加：`st.num_rows()` 优先，回退 `st.approx_num_rows(Some(cfg))`，`size_bytes` 用 `st.estimate_in_memory_size_bytes`：`src/daft-logical-plan/src/ops/source.rs:137-152`。
- `ScanTask::approx_num_rows`：优先用 `metadata.length`（**精确 metadata**），否则用 `size_bytes_on_disk × inflation_factor / schema.estimate_row_size_bytes()` 估算：

```rust
// src/daft-scan/src/lib.rs:623-665（节选）
    pub fn approx_num_rows(&self, config: Option<&DaftExecutionConfig>) -> Option<f64> {
        let approx_total_num_rows_before_pushdowns = self
            .metadata
            .as_ref()
            .map(|metadata| {
                // Use accurate metadata if available
                metadata.length as f64
            })
            .or_else(|| {
                // Otherwise, we fall back on estimations based on the file size
                // use inflation factor from config and estimate number of rows from the schema
                self.size_bytes_on_disk.map(|file_size| {
                    ...
                    let inflation_factor = match self.source_config.as_ref() {
                        SourceConfig::File(ffc) => match ffc {
                            FileFormatConfig::Parquet(_) => config.parquet_inflation_factor,
                            ...
```

- inflation factor 默认值：`parquet_inflation_factor: 3.0`、`csv_inflation_factor: 0.5`：`src/common/daft-config/src/lib.rs:178, 180`；env 覆盖 `DAFT_PARQUET_INFLATION_FACTOR` 等：同文件 :210-213，解析 :251-259。
- 扫描端还会用行数估算把 `limit` 折算成预估物化字节：`src/daft-scan/src/scan_task_iters/mod.rs:51-53`；`daft-scan/src/lib.rs:735-817` 同样用 `approx_num_rows` 估算大小。

### 3.4 谁在读 stats

| 消费者 | 文件:行 | 用途 |
|---|---|---|
| `Offset::with_materialized_stats` | `src/daft-logical-plan/src/ops/offset.rs:45-57` | `num_rows -= offset`，按比例缩 `size_bytes`，算 offset 选择率 |
| `Source::with_materialized_stats` | `src/daft-logical-plan/src/ops/source.rs:122-158` | 生成基数/字节数/选择率 |
| `ReorderJoins` 的 join graph 构建 | `reorder_joins/join_graph.rs`（`JoinGraphBuilder::from_logical_plan(plan, cfg)`，`reorder_joins/mod.rs:45-46`） | 用统计做 join 顺序代价估计（详见 §3.5 补注） |
| 分布式 join 策略 | `src/daft-distributed/src/pipeline_node/join/translate_join.rs:301-302` | `left/right.materialized_stats().approx_stats` 决定 broadcast vs hash |
| 分布式 repartition | `pipeline_node/translate.rs:374-378, 414, 450, 498, 583, 627` | `input.materialized_stats().approx_stats.size_bytes` 作为 shuffle/agg/window/topn/pivot 的输入大小 |
| 分布式 python 侧 | `daft/runners/flotilla.py`（plan runner） | 打印/调试 |

### 3.5 `daft-stats` 的其他角色

- `TableStatistics::union`（`table_stats.rs:77`）用于分区级统计合并；`from_table`（:61）从实际数据生成区间统计；`estimate_row_size`（:117）。
- 分布式的**运行时**统计是另一套：`src/daft-distributed/src/statistics/` 基于 metrics（`statistics/stats.rs`、`task_lifecycle.rs`），用于 dashboard/事件日志，不参与逻辑优化决策。

## 4. 逻辑→物理计划转换

### 4.1 本地路径：`src/daft-local-plan`

- 入口：`pub fn translate(plan: &LogicalPlanRef, psets: &HashMap<String, Vec<MicroPartitionRef>>) -> DaftResult<(LocalPhysicalPlanRef, HashMap<SourceId, Input>)>`：`src/daft-local-plan/src/translate.rs:21-27`；递归主体 `translate_helper`：:29-33；导出 `pub use translate::translate`：`src/daft-local-plan/src/lib.rs:26`。
- `LocalPhysicalPlan` 枚举变体（`src/daft-local-plan/src/plan.rs:75-132`）：`InMemoryScan / PhysicalScan / GlobScan / PlaceholderScan / Project / UDFProject / Filter / IntoBatches / Limit / Explode / Unpivot / Sort / TopN / Sample / MonotonicallyIncreasingId / StageCheckpointKeys / UnGroupedAggregate / HashAggregate / Dedup / Pivot / Concat / HashJoin / CrossJoin / SortMergeJoin / AsofJoin / PhysicalWrite / CommitWrite / CatalogWrite / LanceWrite / DataSink / WindowPartitionOnly / WindowPartitionAndOrderBy / WindowPartitionAndDynamicFrame / WindowOrderByOnly / IntoPartitions / RepartitionWrite / GatherWrite / ShuffleRead / DistributedActorPoolProject / DistributedLimit / VLLMProject`；查询入口 `get_stats_state()`：`plan.rs:158-190`。
- 逻辑算子的映射/消除规则：
  - `Shard` → **报错**，因为已折叠进 source：`translate.rs:99-101`。
  - `Repartition` / `IntoPartitions` → **no-op**（本地 runner 不支持，warn 后直接翻译其 input）：`translate.rs:583-594`。
  - `Shuffle` → 退化为按 `random_int_expr(i64::MIN, i64::MAX, seed)` 的 Sort：`translate.rs:392-409`。
  - `Sink` 按 sink 类型展开为 `PhysicalWrite→CommitWrite` / `CatalogWrite` / `LanceWrite` / `DataSink`：`translate.rs:610-667`。
  - `Aggregate` 按 `groupby.is_empty()` 分派 `ungrouped_aggregate` / `hash_aggregate`：`translate.rs:227-250`。
  - `Window` 按 `(partition_by, order_by, frame)` 组合分派 4 种窗口算子；`order_by + frame` 未实现报错：`translate.rs:263-327`。
  - `Join`：非等值 join 报 `not_implemented("Execution of non-equality join")`（:456-460）；无 key 的 Inner join → `CrossJoin`（:465-475）；其余一律 `HashJoin`（:480-495）。
  - `Offset` / `Union` / `Intersect` / `SubqueryAlias` → **报错**"should already be optimized away"：`translate.rs:703-709`。
- 每个节点构造时都显式携带 `stats_state` 与 `LocalNodeContext`（例如 Filter：`translate.rs:102-114`），保证物理计划保留统计与节点元数据。
- `RepartitionWrite` / `GatherWrite` / `ShuffleRead` / `DistributedActorPoolProject` / `DistributedLimit` 属于分布式执行也在用的物理算子（枚举定义：`plan.rs:122-132`）。

### 4.2 分布式路径：`src/daft-distributed`

- `DistributedPhysicalPlan { query_idx, query_id, logical_plan, config }`：`src/daft-distributed/src/plan/mod.rs:34-40`；构造 `from_logical_plan_builder(builder, query_id, config)`：:43-56。**注意：它持有的是（已优化的）逻辑计划，物理化发生在翻译期**。
- 逻辑→pipeline node 翻译入口：`logical_plan_to_pipeline_node(plan_config, plan, psets, meter) -> DaftResult<TranslationOutput>`：`src/daft-distributed/src/pipeline_node/translate.rs:43-56`；使用 `LogicalPlanToPipelineNodeTranslator`（`TreeNodeVisitor`，`f_up` 后序构树）：:58-65, 121-129；导出 `pub(crate) use translate::logical_plan_to_pipeline_node`：`pipeline_node/mod.rs:74`。
- 逻辑算子 → 分布式 pipeline node 的全部 case：`translate.rs:130, 188, 212, 230, 244, 254, 264, 275, 288, 303, 320, 333, 347, 359, 369, 387, 401, 424, 488, 534, 542, 547, 563, 602, 653, 669`；已优化掉的算子报错：:683-688。
- **stage 边界 / 需要 shuffle 的算子**：
  - `Repartition(Hash|Random|Range)` → `gen_repartition_node`（stage 边界 + shuffle）：`translate.rs:369-386`；实现 `gen_repartition_node` / `gen_repartition_node_with_backend`：`pipeline_node/shuffles/translate_shuffle.rs:37-94`（含 `PreShuffleMergeNode` 预合并、`maybe_warn_large_shuffle` 大 shuffle 告警 :70-75, 79-93）。
  - `IntoPartitions` → `IntoPartitionsNode`（重分区）：`translate.rs:387-400`。
  - `Aggregate` → `gen_agg_nodes(...)`（本地预聚合 + shuffle + 终聚合）：`translate.rs:401-423`。
  - `Distinct` → 先判断 `can_skip_hash_repartition`，需要时"本地 distinct → hash repartition → 再 distinct"三段式：`translate.rs:424-486`；跳过判定 `can_skip_hash_repartition`：:96-118（单分区直接跳过；hash clustering 被 partition 列覆盖也跳过，用 `clustering_is_covered_by`）。
  - `Join`：由 `determine_join_strategy` 决定，再走 `gen_sort_merge_join_node` / `gen_broadcast_join_node` / hash join：`pipeline_node/join/translate_join.rs:281-345`；hash join 内部按聚类情况插入 `gen_repartition_node`：`translate_join.rs:116-129`。
  - `Sort` / `TopN` / `Window` / `Pivot`：`translate.rs:547, 563, 488, 602`（各自读取 `materialized_stats().approx_stats.size_bytes` 决定是否 shuffle）。
- shuffle 后端选择：`select_backend()` → `ShuffleBackend::Flight{..}` 或 `Ray`：`pipeline_node/shuffles/translate_shuffle.rs:24-33`；`ShuffleBackend` 定义于 `src/daft-local-plan`（`translate_shuffle.rs:5`）。`RepartitionSpec` 有 `Hash/Random/Range` 三型（`pipeline_node/shuffles/translate_shuffle.rs:64-66`）。
- 执行入口：`PlanRunner::run_plan(query_idx, pipeline_node, statistics_manager) -> DaftResult<PlanResult>`：`src/daft-distributed/src/plan/runner.rs:159-189`；内部 `run_plan_impl`：:191-220（`produce_tasks` → `RunningPlan::materialize` → 清理 shuffle 目录）。Python 入口：`PyDistributedPhysicalPlanRunner::run_plan`：`src/daft-distributed/src/python/mod.rs:260-330`（:320 调 `run_plan`）。

## 5. 计划可视化与配置

### 5.1 Rust 侧

- 逻辑计划展示实现 `TreeDisplay`：`src/daft-logical-plan/src/display.rs:8-27`（`display_as` 按 `DisplayLevel::{Compact,Default,Verbose}`，`repr_json` 转 JSON）。
- `LogicalPlanBuilder` 暴露：`repr_ascii(simple)`：`builder/mod.rs:1217-1219`；`repr_mermaid(opts)`：`builder/mod.rs:1221-1224`；PyO3 绑定 `repr_ascii` / `repr_mermaid`：`builder/mod.rs:1830-1836`。
- Mermaid 输出与 `MermaidDisplayOptions` 由 `common_display` 提供（`src/daft-logical-plan/src/display.rs:41`），测试里给出完整期望文本：`display.rs:86-130`。
- **本地物理计划**的可视化：`NativeExecutor::repr_ascii` 内部走 `translate` → `translate_physical_plan_to_pipeline` → `viz_pipeline_ascii`：`src/daft-local-execution/src/run.rs:631-643`；`repr_mermaid`：:645-667。
- **分布式物理计划**：`PyDistributedPhysicalPlan` 的 `repr_ascii`：`src/daft-distributed/src/python/mod.rs:147`；`repr_mermaid(simple, bottom_up)`：:173；`PipelineNode` 的 `TreeDisplay`（含 `repr_json`）：`pipeline_node/mod.rs:434-449`。
- 翻译期产生用户可见 hint（以 `UserWarning` 抛出并内联到 `repr_ascii`）：`pipeline_node/translate.rs:88-94`。

### 5.2 Python 侧

- `daft.DataFrame.explain(show_all=False, format="ascii", simple=False, file=None)`：`daft/dataframe/dataframe.py:315-423`。
  - 只打印 unoptimized logical plan：`dataframe.py:396-397`；
  - `show_all=True` 时打印 Optimized Logical Plan（`builder.optimize(execution_config)`，`dataframe.py:399-402`）+ Physical Plan：native runner 走 `NativeExecutor().pretty_print(...)`（:414-418），非 native 走 `DistributedPhysicalPlan.from_logical_plan_builder(...)` + `repr_ascii`/`repr_mermaid`（:404-413）；
  - `format="mermaid"` 返回 `MermaidFormatter`：`dataframe.py:364-380`，实现 `daft/dataframe/display.py:31`、`:61`。
- `LogicalPlanBuilder.pretty_print(simple, format)`：`daft/logical/builder.py:74-83`（`repr_ascii` 或 `repr_mermaid`）；`repr_json(include_schema)`：:85-87；`__repr__` → `repr_ascii`：:89-90。
- `NativeExecutor.pretty_print`：`daft/execution/native_executor.py:89-91`。

### 5.3 优化器相关配置

| 配置 | 位置 | 说明 |
|---|---|---|
| `OptimizerConfig::strict_pushdown` | `optimizer.rs:24-28`，`Default = false` :39-43 | 由 `DaftPlanningConfig::enable_strict_filter_pushdown` 驱动，builder 里 `.when(...)` 注入：`builder/mod.rs:1110-1120` |
| `OptimizerConfig::default_max_optimizer_passes` | `optimizer.rs:26`，默认 20 :42 | FixedPoint batch 的最大轮数 |
| `DaftPlanningConfig::disable_join_reordering` | `src/common/daft-config/src/lib.rs:66` | 跳过 `reorder_joins` 批次：`builder/mod.rs:1123-1132` |
| `DaftPlanningConfig::enable_strict_filter_pushdown` | `lib.rs:67` | 同上 |
| `DaftPlanningConfig::enable_dp_ccp_join_ordering` | `lib.rs:68-72` | 切 DP-ccp，上限 7→12 |
| env `DAFT_DEV_DISABLE_JOIN_REORDERING` | `lib.rs:83, 93-95` | |
| env `DAFT_DEV_ENABLE_STRICT_FILTER_PUSHDOWN` | `lib.rs:84-85, 97-99` | |
| env `DAFT_DEV_ENABLE_DP_CCP_JOIN_ORDERING` | `lib.rs:86-87, 101-103` | |
| env `DAFT_INSTRUMENT_LOGICAL_PLAN` | `builder/mod.rs:1161` | 为优化后计划分配 node_id |
| `broadcast_join_size_bytes_threshold` | `lib.rs:126` | broadcast join 阈值 |
| `hash_join_partition_size_leniency` | `lib.rs:127` | hash join 分区数放缩 |
| `parquet/csv/json/text_inflation_factor` | `lib.rs:132-134`（默认 3.0/0.5）：`178-180` | 无 metadata 时行数估算；env `DAFT_PARQUET_INFLATION_FACTOR` 等 :210-213 |
| `enable_scan_task_split_and_merge`、`scan_tasks_min/max_size_bytes` | `lib.rs:121-123` | scan task 切分 |
| `DAFT_RUNNER` | `src/daft-runners/src/runners.rs:257` | runner 选择（见 §4.3 补注） |

- **`enable_optimizer` 开关：源码中不存在**（`grep -rn "enable_optimizer" src/ daft/` 无命中）；优化器总是运行，只能通过上述 planning config 关闭个别规则。

## 6. 源码地图

| 文件/目录 | 职责 |
|---|---|
| `src/daft-logical-plan/src/optimization/optimizer.rs` | `Optimizer`/`OptimizerBuilder`/`OptimizerConfig`/`RuleBatch`/`RuleExecutionStrategy`，规则调度顺序 |
| `src/daft-logical-plan/src/optimization/rules/rule.rs` | `OptimizerRule` trait |
| `src/daft-logical-plan/src/optimization/logical_plan_tracker.rs` | 计划摘要 + 环检测 |
| `src/daft-logical-plan/src/optimization/rules/*.rs` | 全部逻辑优化规则 |
| `src/daft-logical-plan/src/optimization/rules/reorder_joins/` | join 重排（join graph / 暴力 / DP-ccp） |
| `src/daft-logical-plan/src/builder/mod.rs` | 拼装优化器流水线、`optimize`/`optimize_async`、`repr_*`、Python 绑定 |
| `src/daft-logical-plan/src/stats.rs` | `PlanStats`/`ApproxStats`/`StatsState` |
| `src/daft-logical-plan/src/logical_plan.rs` | `LogicalPlan` 枚举、`schema()`、`stats_state()`、`with_materialized_stats()` |
| `src/daft-logical-plan/src/display.rs` | 逻辑计划 ascii/mermaid/json 展示 |
| `src/daft-stats/src/table_stats.rs` | 列级区间统计 `TableStatistics` |
| `src/daft-scan/src/lib.rs` | `ScanTask::approx_num_rows` 等统计估算 |
| `src/daft-scan/src/expr_rewriter.rs` | 谓词按分区列/数据列/UDF 三路重写 |
| `src/daft-local-plan/src/translate.rs` | 逻辑→本地物理计划翻译 |
| `src/daft-local-plan/src/plan.rs` | `LocalPhysicalPlan` 枚举 |
| `src/daft-distributed/src/plan/mod.rs` | `DistributedPhysicalPlan` |
| `src/daft-distributed/src/plan/runner.rs` | `PlanRunner`/`RunningPlan` 执行入口 |
| `src/daft-distributed/src/pipeline_node/translate.rs` | 逻辑→分布式 pipeline node 翻译、stage/shuffle 边界 |
| `src/daft-distributed/src/pipeline_node/shuffles/` | repartition / gather / pre-shuffle-merge / shuffle 后端 |
| `src/daft-distributed/src/pipeline_node/join/` | join 策略决策与各 join 节点 |
| `src/daft-local-execution/src/run.rs` | `NativeExecutor`、本地物理计划可视化 |
| `src/daft-runners/src/runners.rs` | `Runner`/`RunnerConfig`、`DAFT_RUNNER` |
| `src/common/daft-config/src/lib.rs` | `DaftPlanningConfig`/`DaftExecutionConfig` 与 env 覆盖 |
| `daft/dataframe/dataframe.py` | `DataFrame.explain()` |
| `daft/logical/builder.py` | `LogicalPlanBuilder.pretty_print`/`repr_json` |
| `daft/execution/native_executor.py` | native 物理计划打印 |
