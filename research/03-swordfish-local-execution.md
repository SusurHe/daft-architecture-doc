# Swordfish 本地流式执行引擎源码调研

- 仓库：`D:\DOC\daft-src`，commit `dadd8a0b290be148d92acb6f9e6fc4b6e36f221f`（2026-09-25），Rust workspace 版本 `0.3.0-dev0`（`Cargo.toml:498`）。
- 调研对象：`src/daft-local-execution/`（23 个源文件、共 22617 行 Rust），辅以 `src/common/runtime`、`src/common/daft-config`、`src/daft-local-plan`、`daft/execution/*.py`。
- 说明：题面假设的目录 `pipeline/`、`dispatch/`、`monitor/` **在本版本不存在**；pipeline 逻辑集中在单文件 `pipeline.rs`，channel 集中在单文件 `channel.rs`，监控在 `runtime_stats/`。下文所有路径均相对 `daft-src/`。

---

## 1. 总体结构

**模块清单**（`src/daft-local-execution/src/lib.rs:3-18`）：`batch_manager`、`buffer`、`channel`、`checkpoint_terminus`、`concat`、`dynamic_batching`、`input_sender`、`intermediate_ops`、`join`、`pipeline`、`resource_manager`、`run`、`runtime_stats`、`sinks`、`sources`、`streaming_sink`。

四类算子目录（均为目录 + `mod.rs` 显式导出）：
- `intermediate_ops/`：project、filter、explode、unpivot、into_batches、udf、stage_checkpoint_keys、distributed_actor_pool_project（`intermediate_ops/mod.rs:1-9`）
- `sinks/`（阻塞式 sink）：aggregate、grouped_aggregate、sort、top_n、pivot、dedup、repartition、into_partitions、gather、write、commit_write、window_*（`sinks/mod.rs:1-18`）
- `streaming_sink/`（流式 sink）：limit、sample、monotonically_increasing_id、async_udf、vllm、distributed_limit（`streaming_sink/mod.rs:1-9`）
- `sources/`：scan_task、in_memory、glob_scan、shuffle_read（`sources/mod.rs:1-6`）
- `join/`：hash_join、sort_merge_join、cross_join、asof_join、build、probe、join_node、index_bitmap（`join/mod.rs:1-14`）

**入口 API 链路**：Python `NativeExecutor.run`（`daft/execution/native_executor.py:32-55`）→ pyo3 `PyNativeExecutor.run`（`src/daft-local-execution/src/run.rs:212-248`）→ `NativeExecutor::run`（`run.rs:448-569`）→ `translate_physical_plan_to_pipeline`（`pipeline.rs:397`）→ `physical_plan_to_pipeline`（`pipeline.rs:436`）→ `run_execution_loop`（`run.rs:332`）。`NativeExecutor` 以 `plan_fingerprint` 缓存 pipeline（`run.rs:490-537`），使同一算子的多个 `input_id` 复用同一条 pipeline。

**LocalPhysicalPlan 对接**：`physical_plan_to_pipeline` 是一个对 `LocalPhysicalPlan` 的大 `match`（`pipeline.rs:442`），每个分支负责 ①递归构建子节点 ②`try_new` 出算子 ③包成对应 Node。Scan 类节点额外创建无界通道并把 `InputSender` 注册进 `input_senders`，供 driver 之后投喂数据：

```rust
// pipeline.rs:446-473（PhysicalScan 分支）
LocalPhysicalPlan::PhysicalScan(PhysicalScan { source_id, source_config, pushdowns, schema, stats_state, context, .. }) => {
    let (tx, rx) = create_unbounded_channel::<(InputId, Vec<ScanTaskRef>)>();
    input_senders.insert(*source_id, InputSender::ScanTasks(tx));
    let scan_task_source = ScanTaskSource::new(rx, source_config.clone(), pushdowns.clone(), schema.clone(), cfg, Some(ctx.skipped_corrupt_files.clone()));
    SourceNode::new(Box::new(scan_task_source), stats_state.clone(), ctx, context).boxed()
}
```

`InputSender` 有四变体 `ScanTasks / InMemory / GlobPaths / FlightShuffle`（`input_sender.rs:10-15`），对应 `daft_local_plan::Input` 的四变体（`src/daft-local-plan/src/lib.rs:44-50`）。运行期由 `run_execution_loop` 把 `EnqueueInputMessage`（`run.rs:96-103`）拆解后经 `InputSender::send(input_id, input)` 投喂（`run.rs:369-384`），输出经 `MessageRouter` 按 `input_id` 路由回各自的 `PyResultReceiver`（`run.rs:106-155`，`run.rs:385-399`）。

---

## 2. Pipeline 节点模型

核心 trait 是 **`PipelineNode`**（`pipeline.rs:224-246`），四类节点都实现它：

```rust
// pipeline.rs:224-246
pub(crate) trait PipelineNode: Sync + Send + TreeDisplay {
    fn children(&self) -> Vec<&dyn PipelineNode>;
    fn boxed_children(&self) -> Vec<&Box<dyn PipelineNode>>;
    fn name(&self) -> Arc<str>;
    fn propagate_morsel_size_requirement(&mut self, downstream_requirement: MorselSizeRequirement, default_requirement: MorselSizeRequirement);
    fn start(self: Box<Self>, maintain_order: bool, runtime_handle: &mut ExecutionRuntimeContext)
        -> crate::Result<crate::channel::Receiver<PipelineMessage>>;
    fn as_tree_display(&self) -> &dyn TreeDisplay;
    fn node_id(&self) -> usize;
    fn node_info(&self) -> Arc<NodeInfo>;
}
```

`start` 是**唯一**的启动接口：递归启动子节点拿到上游 `Receiver`，再 `runtime_handle.spawn` 一个常驻 driver 任务，返回自己的 `Receiver`。节点间消息是 `PipelineMessage`（数据 or flush）：

```rust
// pipeline.rs:83-95
pub enum PipelineMessage {
    Morsel { input_id: InputId, partition: MicroPartition },
    FlightPartitionRef { input_id: InputId, partition_ref: FlightPartitionRef },
    Flush(InputId),   // 该 input 的上游已结束
}
```

`next_event`（`pipeline.rs:111-146`）是所有节点 driver 的统一事件循环原语，用 `tokio::select!` 在「上游消息」与「本节点 spawn 出去的任务完成」之间做选择，并且**只有 `task_set.len() < max_concurrency` 时才 recv 上游**（`pipeline.rs:121`）——这是全局背压的第一道闸门。事件类型 `PipelineEvent` 见 `pipeline.rs:98-107`。

**① Source**：`trait Source`（`sources/source.rs:140-151`）只有 `get_data(self, maintain_order, stats_provider, chunk_size) -> SourceStream`（返回 `BoxStream<DaftResult<PipelineMessage>>`）。`SourceNode::start`（`source.rs:255-359`）把 `chunk_size` 取自 morsel 需求上界（`source.rs:269-272`），spawn 一个循环把 stream 里的消息转发到 `create_channel(1)`；若无数据则产出一个 empty MicroPartition（`source.rs:333-351`）。

**② Intermediate**：`trait IntermediateOperator`（`intermediate_ops/intermediate_op.rs:41-69`）——`execute(&self, input, state, runtime_stats, task_spawner, input_id) -> OperatorOutput<DaftResult<(State, MicroPartition)>>`、`make_state`、`max_concurrency`（默认 = compute 线程数，`intermediate_op.rs:60-62`）、`morsel_size_requirement`、`batching_strategy`。它是**有状态可并发**的：`IntermediateNode` 预创建 `max_concurrency` 份 state，谁空闲给谁（`intermediate_op.rs:422-423`、`116-141`）。

**③ BlockingSink**（聚合/排序/写）：`trait BlockingSink`（`sinks/blocking_sink.rs:41-68`）——`sink(input, state, stats, spawner) -> State` 累积，`finalize(states, spawner) -> Vec<MicroPartition>` 一次性产出（`finalize` 签名见 `blocking_sink.rs:54-60`）。`PerInputState` 持 `states`/`pending`/`flushed`（`blocking_sink.rs:75-81`），`ready_to_finalize = flushed && all_states_idle`（`blocking_sink.rs:143-145`）。**所有分片状态在 finalize 时合并**，因此是全局物化算子。

**④ StreamingSink**：`trait StreamingSink`（`streaming_sink/base.rs:50-82`）——`execute` 返回 `(State, StreamingSinkOutput)`，其中 `StreamingSinkOutput::{NeedMoreInput(Option<MP>), Finished(Option<MP>)}`（`base.rs:32-35`）；`finalize` 返回 `StreamingSinkFinalizeOutput::{HasMoreOutput{states, output}, Finished(output)}`（`base.rs:37-43`），故 finalize **可以循环产出多批**（`base.rs:241-274`）。

**额外两类节点**（题面未列但存在）：
- `JoinNode<Op: JoinOperator>`：双输入节点，`children()` 返回 `[left, right]`（`join/join_node.rs:118-121`）；`JoinOperator` trait 定义 build/probe/`finalize_build`/`make_probe_state`/`finalize_probe`（`join/join_operator.rs:26-97`）。
- `ConcatNode`：双输入、只支持 `input_id = 0`（`concat.rs:55-106`，断言见 `concat.rs:73-77`）。

**算子 ↔ Sink 配对**（均在 `pipeline.rs` 的 match 内）：`UnGroupedAggregate → BlockingSinkNode(AggregateSink)`（`pipeline.rs:916-937`）；`HashAggregate → BlockingSinkNode(GroupedAggregateSink)`（`pipeline.rs:938-959`）；`Sort/TopN/Pivot/Dedup/Repartition/IntoPartitions/GatherWrite/PhysicalWrite/CommitWrite → BlockingSinkNode`（`pipeline.rs:1036/1056/1006/960/1609/1586/1639/1404/1447`）；`Limit/Sample/MonotonicallyIncreasingId/VLLMProject → StreamingSinkNode`（`pipeline.rs:863/745/1084/1688`）；`Project/Filter/Explode/Unpivot/IntoBatches/UDFProject(同步) → IntermediateNode`（`pipeline.rs:642/766/839/980/820/688`）。**关键分叉**在 UDFProject：`is_async && !use_process` 走 `AsyncUdfSink`（流式），否则走 `UdfOperator`（中间算子）——见 `pipeline.rs:673-707`。

**Morsel 尺寸需求传播**：根节点以 `Flexible(0, cfg.default_morsel_size)` 自上而下调用 `propagate_morsel_size_requirement`（`pipeline.rs:429-432`）；`combine_requirements` 定义了两段区间的合并规则（`pipeline.rs:178-221`）。BlockingSink 会**切断**下游需求，强制子节点用 default（`blocking_sink.rs:529-536`）；JoinNode 只把需求传给 probe（右侧），build（左侧）用 default（`join_node.rs:131-151`）。

---

## 3. 调度与并发

**两级 tokio runtime + 一个事件循环 runtime**：

| runtime | 位置 | 线程数 |
|---|---|---|
| 全局 driver runtime | `run.rs:63-93` | python feature 下用 `pyo3_async_runtimes::tokio::init(new_current_thread)` 单线程（`run.rs:68-77`）；非 python 用 `new_multi_thread`（`run.rs:82-85`）。只跑 driver/控制逻辑 |
| COMPUTE runtime | `src/common/runtime/src/lib.rs:190-206` | `worker_threads(num_worker_threads)` = `available_parallelism()`（`lib.rs:25-32`、`30-32`），线程名 `DAFTCPU-{id}`（`lib.rs:196-200`） |
| IO runtime | `src/common/runtime/src/lib.rs:208-227` | `8.min(NUM_CPUS)`（`lib.rs:27`、`212-216`），线程名 `DAFTIO-{id}` |

配置面：`get_compute_pool_num_threads()`（`lib.rs:262-264`）、`set_compute_runtime_num_worker_threads()`（`lib.rs:34-40`，UDF 子进程用它把线程数设为 1，见 `daft/execution/udf_worker.py:42`）。COMPUTE runtime 的 `spawn_blocking` 被显式 panic 禁止（`lib.rs:177-180`）。

**算子并发模型**：`IntermediateNode` 在节点内维护 `operator_states: Vec<Op::State>`（长度 = `max_concurrency`）与 `OrderingAwareJoinSet`；每收到一个 morsel 就 `batch_manager.push` 后 `try_dispatch`，把 batch 与一个空闲 state 一起 spawn 到 compute runtime：

```rust
// intermediate_ops/intermediate_op.rs:116-138（节选）
fn dispatch_ready_batches(&mut self, input_id: InputId) -> DaftResult<()> {
    while !self.operator_states.is_empty() {
        let Some(batch) = self.batch_manager.next_batch(input_id)? else { break };
        *self.active_workers.entry(input_id).or_insert(0) += 1;
        let state = self.operator_states.pop().unwrap();
        let op = self.op.clone();
        self.task_set.spawn(async move {
            let now = Instant::now();
            let (new_state, result) = op.execute(batch, state, runtime_stats, &task_spawner, input_id).await??;
            Ok(WorkerResult { state: new_state, input_id, result, elapsed: now.elapsed() })
        });
    }
    Ok(())
}
```

spawn 抽象是 `ExecutionTaskSpawner`（`src/daft-local-execution/src/lib.rs:161-209`）：`spawn` 把 future 插桩 span 后交给 `RuntimeRef`；`spawn_with_memory_request` 先 `memory_manager.request_bytes(n).await?` 拿许可再执行（`lib.rs:180-196`）。`RuntimeRef::spawn` 返回 `RuntimeTask<T>`，内部是 `tokio::task::JoinSet`——**drop 即取消**（`src/common/runtime/src/lib.rs:57-85`，测试见 `lib.rs:291-316`）。

**顺序保证**：`OrderingAwareJoinSet::new(maintain_order)`（`src/common/runtime/src/joinset.rs:201-213`）在 `maintain_order=true` 时用 `OrderedJoinSet`：为每个 spawn 的 task 记 `tokio::task::Id`，乱序完成的结果先缓存到 `finished: HashMap`，`join_next` 始终按 spawn 顺序返回（`joinset.rs:131-198`）。

**典型算子并发度**：
- Project：`max_concurrency = num_cpus.div_ceil(parallel_exprs)`，`parallel_exprs = num_cpus / max_concurrency`（`intermediate_ops/project.rs:125-141`）；`parallel_exprs > 1` 时走 `MicroPartition::par_eval_expression_list`（`project.rs:163-171`）。
- Filter：默认 `max_concurrency = get_compute_pool_num_threads()`，`StaticBatchingStrategy`（`filter.rs:114-137`）。
- Scan：`num_parallel_tasks = cfg.scantask_max_parallel`（>0）否则 `num_cpus`（`sources/scan_task.rs:55-60`，默认值 8 见 `src/common/daft-config/src/lib.rs:192`）；`spawn_scan_task_processor` 用 `JoinSet` + `pending_tasks: VecDeque` + `while task_set.len() < max_parallel` 做限流（`scan_task.rs:99-130`），整个处理器跑在 IO runtime 上（`scan_task.rs:82,99`）。

---

## 4. 通道与背压

`channel.rs` 是对 tokio mpsc 的薄封装，**只有两种**通道，没有 `ChannelCapacity` 枚举：

```rust
// channel.rs:30-33 / 54-57
pub(crate) fn create_channel<T>(buffer_size: usize) -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = tokio::sync::mpsc::channel(buffer_size); (Sender(tx), Receiver(rx))
}
pub(crate) fn create_unbounded_channel<T>() -> (UnboundedSender<T>, UnboundedReceiver<T>) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel(); (UnboundedSender(tx), UnboundedReceiver(rx))
}
```

- **节点间数据通道一律 `create_channel(1)`**：Source（`source.rs:268`）、Intermediate（`intermediate_op.rs:408`）、BlockingSink（`blocking_sink.rs:553`）、StreamingSink（`base.rs:546`）、Join（`join_node.rs:165`）。即背压粒度 = 1 个 morsel。
- **无界通道只用于「driver 投喂输入」和「输出回传」**：ScanTask/InMemory/Glob 输入（`pipeline.rs:455/481/501`）、`EnqueueInputMessage`（有界 1，`run.rs:514`）、`ExecutionEngineResultItem`（`run.rs:546`）。
- 第二道背压来自 `next_event` 的 `max_concurrency` 门控（`pipeline.rs:117-121`）：任务占满时不再从上游取 morsel。
- 第三道在 Source：`ScanTaskSource` 输出通道也是容量 1（`scan_task.rs:234`），配合 `combine_stream` 把「处理器任务错误」并入数据流（`scan_task.rs:248-251`，`src/common/runtime/src/lib.rs:267-286`）。

**Batch 切分（morsel 大小控制）**：`BatchManager`（`batch_manager.rs:39-168`）按 `input_id` 维护 `RowBasedBuffer`（`buffer.rs:16-21`，含 `lower_bound`/`upper_bound`），`next_batch` 先 `update_bounds(current_requirements)` 再交给 `BatchingStrategy`（`batch_manager.rs:82-97`）；flush 期间无条件 `pop_all`（`batch_manager.rs:94`）。`RowBasedBuffer::next_batch_if_ready` 的三种状态：低于下界不产出、区间内整块产出、超上界切出 `upper_bound` 行并把余量塞回（`buffer.rs:110-149`）。

**尺寸配置项**（`src/common/daft-config/src/lib.rs:120-155` + 默认值 `164-202`）：
- `default_morsel_size: NonZeroUsize`，默认 **131072 行**（`daft-config/src/lib.rs:142,188`）→ `MorselSizeRequirement::default()`（`pipeline.rs:167-176`）。
- `enable_dynamic_batching`（默认 `false`）、`dynamic_batching_strategy`（默认 `"auto"`）（`daft-config/src/lib.rs:150-151,196-197`）。
- `partial_aggregation_threshold` = 10000、`high_cardinality_aggregation_threshold` = 0.8（`daft-config/src/lib.rs:185-186`）。
- `broadcast_join_size_bytes_threshold` = 10 MiB（`daft-config/src/lib.rs:172`）。
- **没有** `target_morsel_size` / `max_morsel_size` 字段；UDF 的 `batch_size` 走 `UDFProperties.batch_size`（`intermediate_ops/udf.rs:568-574`）。传递单位是 `MicroPartition`（可由多个 `RecordBatch` 组成）。

**动态 batch 算法**：`LatencyConstrainedBatchingStrategy`（`dynamic_batching/latency_constrained_strategy.rs`）实现论文 *Optimizing LLM Inference Throughput via Memory-aware and SLA-constrained Dynamic Batching* 的 Algorithm 2，用二分搜索在 `[b_low, b_high]` 内逼近满足延迟约束的最大 batch（`latency_constrained_strategy.rs:165-215`）。Project 配 `target=5s / tolerance=1s / α=2048 / δ=64`（`project.rs:240-247`），UDF 配 `α=16 / δ=4`（`udf.rs:591-598`）。

**input_id 与乱序**：`maintain_order` 来自 `ctx.daft_execution_config.maintain_order`（`daft/execution/native_executor.py:48`，默认 `true`，`daft-config/src/lib.rs:195`）。它只影响 ①`OrderingAwareJoinSet` ②scan 层：`maintain_order=true` 时启动 `run_order_preserving_flattener` 按 input_id 顺序回放各 scan task 的子流（`scan_task.rs:88-94`、`scan_task.rs:440-470`），否则每个 scan task 直接向 pipeline 发 morsel 并在最后一个完成时发 `Flush`（`scan_task.rs:184-196`）。

---

## 5. 内存与 Spill

**内存管理器**：全局单例 `MemoryManager`（`resource_manager.rs:7,19-21`），总配额取 `SystemInfo::calculate_total_memory()`，可用 `DAFT_MEMORY_LIMIT` 环境变量覆盖（`resource_manager.rs:9-17,50-77`）；`request_bytes` 用 `Mutex<MemoryState> + Notify` 做「不可满足则挂起等待」（`resource_manager.rs:79-100`），`MemoryPermit::drop` 归还并 `notify_waiters`（`resource_manager.rs:28-38`）。

```rust
// resource_manager.rs:79-100（节选）
pub async fn request_bytes(&self, bytes: u64) -> DaftResult<MemoryPermit<'_>> {
    if bytes > self.total_bytes { return Err(DaftError::ComputeError(...)); }
    loop {
        if let Some(permit) = self.try_request_bytes(bytes) { return Ok(permit); }
        self.notify.notified().await;
    }
}
```

**关键事实：本地执行引擎没有磁盘 spill。** 全仓 `spill` 关键字只命中 shuffle 缓存相关的注释（`sinks/repartition.rs:29-30`、`sinks/gather.rs:24`、`sinks/into_partitions.rs:71`），没有 `temp_dir`/`spill_threshold` 之类的执行期溢写开关。所有阻塞算子都是**纯内存累积**：
- Sort：`SortState::Building(Vec<MicroPartition>)` 全收完再 `concat + sort`（`sinks/sort.rs:18-41,80-101`），`max_concurrency() == 1`（`sort.rs:137-139`）。
- GroupedAggregate：`SinglePartitionAggregateState { partially_aggregated, unaggregated }`（`sinks/grouped_aggregate.rs:111-116`）。
- HashJoin build 侧：`HashJoinBuildState { probe_table_builder, tables: Vec<RecordBatch> }`（`join/hash_join.rs:32-35`）。

内存控制只有三处：① `MemoryManager` 许可（仅 UDF 用，见 §7）② 本地 shuffle 前分区缓冲阈值 ③ 分布式/Flotilla 层的 task 级内存请求。

**真正的「落盘」是 shuffle**：`RepartitionSink` 按字节阈值融合后分区，再按后端写出——Ray 后端把分区作为返回值留在内存（`sinks/repartition.rs:191-210`），Flight 后端调用 `write_partitions_one_shot` 写 shuffle 文件并注册到本地 Flight server，只回传 `FlightPartitionRef`（`sinks/repartition.rs:211-241`）。缓冲区阈值：

```rust
// sinks/repartition.rs:27-30, 94-107
const REPARTITION_MIN_BUFFER_THRESHOLD_BYTES: usize = 16 * 1024 * 1024;   // 16 MB
const REPARTITION_MAX_BUFFER_THRESHOLD_BYTES: usize = 256 * 1024 * 1024;  // 256 MB
fn repartition_buffer_threshold_bytes(backend: &LocalShuffleBackend, num_partitions: usize) -> usize {
    match backend {
        LocalShuffleBackend::Ray => REPARTITION_MAX_BUFFER_THRESHOLD_BYTES,
        LocalShuffleBackend::Flight(_) => CHUNK_TARGET_BYTES.saturating_mul(num_partitions.max(1))
            .clamp(REPARTITION_MIN_BUFFER_THRESHOLD_BYTES, REPARTITION_MAX_BUFFER_THRESHOLD_BYTES),
    }
}
```
`CHUNK_TARGET_BYTES = 4 MiB`（`src/daft-shuffles/src/shuffle_cache.rs:30`）。落盘目录来自 `cfg.flight_shuffle_dirs`（默认 `["/tmp"]`，`daft-config/src/lib.rs:198`），压缩来自 `flight_shuffle_compression`（默认 `lz4`，`daft-config/src/lib.rs:199`；`sinks/repartition.rs:212`）。

---

## 6. 关键算子实现

**HashAggregate（`sinks/grouped_aggregate.rs`）**——两阶段聚合 + 运行期自适应策略：
- 构造时用 `daft_local_plan::agg::populate_aggregation_stages_bound` 把聚合拆成 `partial_agg_exprs` / `final_agg_exprs` / `final_projections`（`grouped_aggregate.rs:256-261`）。
- 三种策略（`grouped_aggregate.rs:27-32`）：`AggThenPartition`（先 agg 再按 hash 分区）、`PartitionThenAgg(threshold)`（先分区，单分区未聚合行数超阈值就局部 agg）、`PartitionOnly`。
- 策略由**首个非空批次**的基数比决定并缓存到全局 `Mutex<Option<AggStrategy>>`：`estimated_num_groups / input.len() >= high_cardinality_threshold_ratio(0.8)` → `PartitionThenAgg`，否则 `AggThenPartition`（`grouped_aggregate.rs:177-217`，估算用 `RecordBatch::hash_rows` 去重计数 `:195-204`）。
- `MapGroups`（Python UDAF）强制 `PartitionOnly`，且不允许与 `AggFn` 混用（`grouped_aggregate.rs:263-302`）。散列分布用 `MicroPartition::partition_by_hash(group_by, num_partitions)`，分区数 = `max_concurrency()`（`grouped_aggregate.rs:319-321`）。

**HashJoin（`join/`）**：
- 两侧并行：`JoinNode::start` 同时启动 build（左）与 probe（右）两条执行链并用 `tokio::join!` 汇合（`join/join_node.rs:162-226`）。
- 两侧解耦靠 `BuildStateBridge`：以 `input_id` 为键的 `oneshot`/就绪值槽（`join/build.rs:20-76`）。probe 侧首次见到某 `input_id` 时 spawn 一个任务 `bridge.subscribe(input_id)` 并等待 finalized build state，拿到后才放行探测（`join/probe.rs:338-354`）。
- build 状态：`probe_table_builder: Box<dyn ProbeableBuilder>`（`daft_recordbatch::make_probeable_builder`）+ `tables: Vec<RecordBatch>`（`join/hash_join.rs:42-79`）。
- 探测分派按 join 类型走不同实现：`probe_inner` / `probe_left_right(_with_bitmap)` / `probe_outer` / `probe_anti_semi(_with_bitmap)`（`join/hash_join.rs:208-240`）。`needs_bitmap()` 判定哪些情形必须用索引位图跟踪已匹配行（`hash_join.rs:132-138`），anti/semi + bitmap 时探测阶段不产出数据，全部推迟到 `finalize_probe`（`hash_join.rs:227-236, 248-323`）。
- build 侧选择（broadcast 优化）：有统计信息时 inner/outer 选小侧；只有一侧有统计且其 `size_bytes <= broadcast_join_size_bytes_threshold` 时优先把广播侧作为 build；left/right/anti/semi 因需要位图，要求另一侧小 1.5 倍才反转（`pipeline.rs:1130-1218`）。left/right/anti/semi 的 `track_indices = build_on_left`，其余恒为 `true`（`pipeline.rs:1248-1252`）。

**Sort / TopN**：Sort 为「全量收集 → concat → 一次性 `RecordBatch::sort`」，单并发（`sinks/sort.rs:80-101,137-139`），**没有多路归并**。TopN 是流式剪枝：每批先 `input.top_n(limit+offset, offset=0)` 只留候选（`sinks/top_n.rs:100-107`），finalize 时把各 state 的候选 concat 后再 `top_n(limit, offset)`（`top_n.rs:128-139`）。

**Window（`sinks/window_base.rs`）**：`WindowBaseState::push` 用 `partition_by_hash` 把数据散射到 `num_partitions` 个 `SinglePartitionWindowState`（`window_base.rs:32-50`）；finalize 阶段 `partition_into_groups`（`make_groups` 求每组行号，`window_base.rs:72-86`）→ `sort_and_materialize_groups`（组内按 order_by 排序并 `take` 物化，`window_base.rs:90-113`）。具体函数分派在 `window_partition_only` / `window_partition_and_order_by` / `window_order_by_only` / `window_partition_and_dynamic_frame` 四个 sink。

**Repartition / Shuffle**：`RepartitionAccState` 持 `post_repartitioned: Vec<Vec<RecordBatch>>` + `pre_repartitioned` 缓冲（`sinks/repartition.rs:34-41`），累计字节超阈值就 `flush_pre_partitioned`（`repartition.rs:65-91`），支持 Hash/Random/Range 三种 spec（`repartition.rs:75-85`）。`LocalShuffleBackend::{Ray, Flight}` 由 `ShuffleBackend::from_plan` 在建 pipeline 时一次性解析（`sinks/shuffle_backend.rs:32-55`）。

**Limit**：真正的流式截断——state 只记 `remaining_skip`/`remaining_take`，逐批 slice，一旦产出足量立即返回 `Finished` 使下游 flush、上游被 drop 取消（`streaming_sink/limit.rs:55-106`），`max_concurrency() == 1`（`limit.rs:135-137`）。

**Explode**：`ExplodeOperator` 是中间算子，统计里额外上报 `amplification = rows_out / rows_in`（`intermediate_ops/explode.rs:43-57`），且它覆写 `next_batch` 做数据感知切分（`dynamic_batching/mod.rs:31-39` 注释所述机制）。**Pivot** 是阻塞 sink（`sinks/pivot.rs`，`pipeline.rs:1006-1035`）。**Dedup / IntoPartitions / Gather / Write / CommitWrite** 均为阻塞 sink。

---

## 7. UDF 执行

**并发度与资源**：`UdfOperator::try_new`（`intermediate_ops/udf.rs:355-408`）：
- `max_concurrency` = `get_optimal_allocation`：若 `ResourceRequest.num_cpus()` 指定 n，则 `available = (num_cpus / n).clamp(1, num_cpus)`，若 n > 可用 CPU 直接报错（`udf.rs:411-435`）；
- `concurrency = udf_properties.concurrency.unwrap_or(max_concurrency)`（`udf.rs:366-370`），并作为 `IntermediateOperator::max_concurrency()` 返回（`udf.rs:564-566`）——即**每个并发槽一份 `UdfState`**；
- `memory_request = resource_request.memory_bytes()`（`udf.rs:372-375`），在 execute 时通过 `spawn_with_memory_request` 申请 `MemoryManager` 许可（`udf.rs:452-456`）；
- `batch_size = udf_properties.batch_size` → `MorselSizeRequirement::Strict(batch_size)`（`udf.rs:568-574`），即 UDF 批次被强制固定行数。

**GIL 规避的两条路径**（`enum UdfHandle { Thread, Process(Option<Py<PyAny>>) }`，`udf.rs:164-167`）：
1. **Thread 路径**（默认，`builtin` 或非 actor-pool）：在 compute runtime 的 worker 线程上 `Python::attach` 取 GIL，调用 `initialize_udfs` + `RecordBatch::eval_expression_with_metrics`（`udf.rs:262-276`）。GIL 是瓶颈但仍由 tokio 线程池承载。
2. **Process 路径**（多进程 actor pool）：`use_process = (is_actor_pool_udf() || use_process) && is_arrow_dtype`——含 Python object dtype 的列强制退回线程（`udf.rs:381-393`）。惰性创建 Python 侧的 `daft.execution.udf.UdfHandle`（`udf.rs:171-204`），`Drop` 时调用其 `teardown`（`udf.rs:327-336`）。

**Python 侧进程池实现（`daft/execution/udf.py`）**：每个 `UdfHandle` = 一个 `subprocess.Popen([sys.executable, "-m", "daft.execution.udf_worker", socket_path, secret])`（`daft/execution/udf.py:84-95`），父进程用 `multiprocessing.connection.Listener` + 32 字节 authkey 建立 UNIX socket（`udf.py:61-64,98`）。数据传输用**共享内存**：父进程把 `RecordBatch.to_ipc_stream()` 写入 `SharedMemory` 并 `resource_tracker.unregister`，子进程读取后 `unlink`（`udf.py:33-55,138-140`）。为避免大 batch 输出把 ~64KiB 管道写满导致死锁，`eval_input` 用 `wait([conn, stdout_fd])` **同时**排空子进程 stdout 与等待响应（`udf.py:142-159`）。失败回传 `(_UDF_ERROR, message, tb, exc)` 并还原原始异常为 `UDFException`（`udf.py:165-186`）；`teardown` 先发 `_SENTINEL`，`process.wait(timeout=5.0)` 超时则 `terminate()`（`udf.py:195-208`）。子进程侧把 compute runtime 线程数设为 1，避免嵌套并行（`daft/execution/udf_worker.py:42`），初始化被推迟到 `_READY` 之后以不阻塞父进程（`udf_worker.py:44-55`）。

**GPU**：`ResourceRequest` 有 `num_gpus` 字段与校验（`src/common/resource-request/src/lib.rs:23,30-51,146-147`），但 `daft-local-execution` 内**只消费 `num_cpus()` 与 `memory_bytes()`**（`grep num_gpus src/daft-local-execution/` 无命中）。GPU 分配属于分布式/Ray 调度层职责。

**失败重试**：`UDFProperties.max_retries` 只用于展示与 Ray actor pool（`src/daft-dsl/src/functions/python/mod.rs:312,485-486`）；**本地执行路径没有重试逻辑**（`daft-local-execution` 内 grep `retry` 无命中），一次 UDF 异常直接沿 `DaftResult` 上抛。

**Async UDF**：`AsyncUdfSink`（`streaming_sink/async_udf.rs:147-190`）为每个 state 维护独立 `JoinSet<DaftResult<RecordBatch>>`，并发上限由 `DAFT_MAX_ASYNC_UDF_INFLIGHT_TASKS` 控制，默认 **64**（`async_udf.rs:151-163`）。**分布式 Actor Pool 在本地执行**：`DistributedActorPoolProjectOperator` 先用 Python `get_ready_actors_by_location` 区分同机/远端 actor 并优先取同机（`intermediate_ops/distributed_actor_pool_project.rs:36-76,127-133`），再经 `common_runtime::python::execute_python_coroutine` 调用 actor 的 `eval_input`（`distributed_actor_pool_project.rs:78-93`）。VLLM 走 `VLLMSink`（`streaming_sink/vllm.rs`，`pipeline.rs:1688-1712`）。

---

## 8. 可观测性与容错

**统计抽象**：`trait RuntimeStats`（`runtime_stats/values.rs:8-44`）——`new(meter, node_info)` + `build_snapshot(Ordering)` + `add_rows_in/out`、`add_bytes_in/out`、`add_duration_us`、`increment_num_tasks`，另有 checkpoint 专用的 no-op 钩子 `add_checkpoint_files_staged/add_checkpoints_sealed`（`values.rs:41-43`）。默认实现 `DefaultRuntimeStats`（`values.rs:46-104`）。各算子自带上报口径：`SourceStats`（`io_stats` 按 `input_id` 隔离，修复了 bytes_read N 倍膨胀，见 `sources/source.rs:49-69` 与回归测试 `source.rs:426-469`）、`FilterStats.selectivity`（`intermediate_ops/filter.rs:32-42`）、`ExplodeStats.amplification`（`explode.rs:43-57`）、`UdfRuntimeStats`（含自定义 counter，`udf.rs:124-153`）、`JoinStats`（`join/stats.rs`）。

**StatsManager**：`RuntimeStatsManager`（`runtime_stats/mod.rs:192-583`）是一个独立 tokio 任务，通过 `StatsManagerMessage` 三变体 `NodeEvent / RegisterRuntimeStats / TakeInputSnapshot` 通信（`mod.rs:48-52`）。节点在自己 spawn 第一个 worker 时 `activate_node`，结束时 `finalize_node`（如 `intermediate_op.rs:229-232, 447`）。主循环用 `interval(throttle_interval)` 节流（`mod.rs:409-410`），每个 tick：采样进程统计 → 对活跃节点 `aggregate_node_stats` → 更新进度条 → 广播 `Event::Stats`（`mod.rs:502-553`）。快照按 `input_id` 保存并在 shutdown 前发布到 `finished_snapshots`，保证 teardown 期 `take_input_snapshot` 仍可取到（`mod.rs:435-499`，读取见 `mod.rs:140-184`）。

**Dashboard 关系**：本引擎不直接依赖 `daft-dashboard`（`Cargo.toml` 无该依赖）；统计以 **`daft_context::Subscriber` 事件**为唯一出口（`mod.rs:546-550`）。订阅者是 `daft-context` 里的 `DashboardSubscriber`（`src/daft-context/src/subscribers/dashboard.rs:78,611`，装配点 `src/daft-context/src/subscribers/mod.rs:60`、`python.rs:125`），它把事件转成 HTTP POST 给 `daft-dashboard` 服务；Python 侧启动入口是 `daft.subscribers.dashboard.launch()`（`daft/subscribers/dashboard.py:6-24`）。Flotilla worker 走另一条路：不发逐节点 `Event::Stats`，而是按 ≥1s 间隔批量发 `TaskStatsUpdate`（`mod.rs:403-408, 516-533`）。

**进度条**：`progress_bar.rs`（`ProgressBarMode::{Disabled, Enabled, Persist}`，由 `DAFT_PROGRESS_BAR` 决定，`mod.rs:72-91`），Flotilla worker 强制 Disabled（`mod.rs:79-81`）。进程指标由 `process_stats.rs` 采样（`mod.rs:503-510`），受 `DAFT_PROCESS_MONITOR_ENABLED` 控制（默认关，`mod.rs:54-60`）。

**错误传播**：统一错误枚举 `Error`（`lib.rs:258-284`），含 `PipelineCreationError`/`PipelineExecutionError`/`JoinError`/`OneShotRecvError`，并实现 `From<Error> for DaftError` 以便在 Python 边界还原类型（`lib.rs:286-301`）。`ExecutionRuntimeContext::spawn` 用 `PipelineExecutionSnafu { node_name }` 给每个节点的任务打上节点名（`lib.rs:109-117`）；`run_execution_loop` 收到任一 worker 错误即以 `QueryEndState::Failed` 跳出（`run.rs:361-368`）。**panic 被捕获**：`Runtime::execute_task` 用 `AssertUnwindSafe(future).catch_unwind()` 把 panic 转成 `DaftError::ComputeError`（`src/common/runtime/src/lib.rs:107-125`）。

**取消**：`CancellationToken` 由 `NativeExecutor` 持有（`run.rs:409,424`），`run_execution_loop` 用 `tokio::select! { biased; () = cancel.cancelled() => ... }` 置于最高优先级（`run.rs:351-360`），并额外监听 `ctrl_c`（`run.rs:357-360`）。`NativeExecutor::cancel_plan(fingerprint)` 直接 `plans.remove()`——**依赖 `RuntimeTask` 的 drop-abort 语义取消整个 pipeline**（`run.rs:626-629`）。正常结束时 `ExecutionRuntimeContext::shutdown()` 先 `abort_all()` 再逐个 join，且只吞掉「由取消引起的 `JoinError`」（`lib.rs:128-147`）。Streaming sink 的提前完成（如 LIMIT）会让上游通道自然关闭、上游任务随之 drop，从而中止无用计算（`base.rs:321-327` 返回 `ControlFlow::Break`）。

---

## 9. 源码地图

| 文件 / 目录 | 职责 | 关键类型 / 函数 |
|---|---|---|
| `src/daft-local-execution/src/lib.rs` | crate 根：模块声明、错误类型、spawn 抽象 | `OperatorOutput`、`ExecutionRuntimeContext`、`ExecutionTaskSpawner`、`Error`、`STDOUT` |
| `src/daft-local-execution/src/run.rs` | 执行入口、driver 循环、输入投喂、结果路由 | `NativeExecutor`、`PyNativeExecutor`、`run_execution_loop`、`MessageRouter`、`EnqueueInputMessage` |
| `src/daft-local-execution/src/pipeline.rs` | LocalPhysicalPlan → Pipeline 翻译、节点 trait、morsel 需求传播 | `translate_physical_plan_to_pipeline`、`physical_plan_to_pipeline`、`PipelineNode`、`PipelineMessage`、`next_event`、`MorselSizeRequirement`、`BuilderContext` |
| `src/daft-local-execution/src/channel.rs` | tokio mpsc 封装（有界/无界） | `create_channel`、`create_unbounded_channel`、`Sender`/`Receiver` |
| `src/daft-local-execution/src/batch_manager.rs` | 每 input 缓冲 + 批提取 + flush 生命周期 | `BatchManager`、`InputBuffer` |
| `src/daft-local-execution/src/buffer.rs` | 行数驱动的 morsel 缓冲 | `RowBasedBuffer`、`BufferState` |
| `src/daft-local-execution/src/dynamic_batching/` | 批大小策略 | `BatchingStrategy`、`StaticBatchingStrategy`、`LatencyConstrainedBatchingStrategy`、`DynBatchingStrategy` |
| `src/daft-local-execution/src/resource_manager.rs` | 全局内存配额 | `MemoryManager`、`MemoryPermit`、`get_or_init_memory_manager` |
| `src/daft-local-execution/src/input_sender.rs` | 输入通道类型分派 | `InputSender` |
| `src/daft-local-execution/src/sources/` | 数据源节点 | `Source`、`SourceNode`、`StatsProvider`、`ScanTaskSource`、`InMemorySource`、`GlobScanSource`、`ShuffleReadSource` |
| `src/daft-local-execution/src/sources/scan_task.rs` | 扫描任务并发调度与顺序回放 | `spawn_scan_task_processor`、`run_order_preserving_flattener` |
| `src/daft-local-execution/src/sources/scan_task_reader.rs` | 按格式读取（Parquet/CSV/JSON/…） | `read_scan_task`、`read_parquet`、`read_csv` |
| `src/daft-local-execution/src/intermediate_ops/` | 无状态/有状态流式算子 | `IntermediateOperator`、`IntermediateNode`、`ProjectOperator`、`FilterOperator`、`ExplodeOperator`、`UdfOperator` |
| `src/daft-local-execution/src/sinks/` | 阻塞式算子 | `BlockingSink`、`BlockingSinkNode`、`GroupedAggregateSink`、`SortSink`、`TopNSink`、`PivotSink`、`DedupSink`、`RepartitionSink`、`WriteSink` |
| `src/daft-local-execution/src/sinks/shuffle_backend.rs` | 本地 shuffle 后端选择 | `LocalShuffleBackend`、`FlightShuffleContext` |
| `src/daft-local-execution/src/streaming_sink/` | 流式算子 | `StreamingSink`、`StreamingSinkNode`、`LimitSink`、`SampleSink`、`AsyncUdfSink`、`VLLMSink` |
| `src/daft-local-execution/src/join/` | 双输入 join 节点与算法 | `JoinNode`、`JoinOperator`、`HashJoinOperator`、`SortMergeJoinOperator`、`BuildStateBridge`、`ProbeExecutionContext` |
| `src/daft-local-execution/src/concat.rs` | 双输入串接节点 | `ConcatNode` |
| `src/daft-local-execution/src/checkpoint_terminus.rs` | 无 sink 计划下的 checkpoint 终结节点 | `CheckpointTerminusNode` |
| `src/daft-local-execution/src/runtime_stats/` | 统计、进度条、进程监控 | `RuntimeStats`、`RuntimeStatsManager`、`RuntimeStatsManagerHandle`、`ProgressBar` |
| `src/common/runtime/src/lib.rs` | runtime 池与任务抽象 | `Runtime`、`RuntimeRef`、`RuntimeTask`、`get_compute_runtime`、`get_io_runtime`、`get_compute_pool_num_threads` |
| `src/common/runtime/src/joinset.rs` | 保序/非保序任务集合 | `JoinSet`、`OrderedJoinSet`、`OrderingAwareJoinSet` |
| `src/common/daft-config/src/lib.rs` | 执行配置与默认值 | `DaftExecutionConfig`（`default_morsel_size`、`scantask_max_parallel`、`partial_aggregation_threshold`、…） |
| `src/daft-local-plan/src/lib.rs` | 本地物理计划与输入契约 | `LocalPhysicalPlan`、`Input`、`InputId`、`SourceId` |
| `daft/execution/native_executor.py` | Python 侧驱动 | `NativeExecutor.run`（同步 generator 包异步流） |
| `daft/execution/udf.py` | UDF 子进程句柄与共享内存传输 | `UdfHandle`、`SharedMemoryTransport` |
| `daft/execution/udf_worker.py` | UDF 子进程事件循环 | `udf_event_loop` |
| `src/daft-context/src/subscribers/dashboard.rs` | 统计事件 → dashboard HTTP | `DashboardSubscriber` |
