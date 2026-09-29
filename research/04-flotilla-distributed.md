# Daft Flotilla 分布式执行引擎源码调研

仓库：`D:\DOC\daft-src`，commit `dadd8a0`（2026-09-25，v0.7.25 线）。全部结论均来自本地源码，证据为 `相对路径:行号`。

> **重要更正（对本轮调研前提）**：该 commit 中 **不存在** `StagePlan` / `DistributedNode` / `TaskScheduler` 这些类型；`grep -rn "StagePlan\|struct Stage\|DistributedNode\b" src/` 只命中 `StageCheckpointKeys`（另一个算子）。当前架构是 **distributed pipeline node DAG**：`DistributedPipelineNode` + `PipelineNodeImpl`，所谓 "stage 边界" 由算子类别 `NodeCategory::BlockingSink` 与物化（materialization）隐式表达。下游笔记按真实类型命名。

## 1. 架构总览

### 1.1 入口调用链（Python → Rust → 调度循环）
1. `daft.set_runner_ray()` → `daft/runners/__init__.py:129` 调 `_set_runner_ray`，其同时把下采样参数写进环境变量（`daft/runners/__init__.py:114-127`）。
2. pyo3 绑定 `set_runner_ray` 在 `src/daft-runners/src/python.rs:49`；构造 `RayRunner::try_new`（`src/daft-runners/src/python.rs:82-88`），最终 import Python 侧 `daft.runners.ray_runner.RayRunner`（`src/daft-runners/src/runners.rs:31-32`）。
3. `DAFT_RUNNER=ray` 走 `get_runner_type_from_env`/`get_runner_config_from_env`：`src/daft-runners/src/runners.rs:256-288`；未设置时按 `detect_ray_state()` 推断（`runners.rs:274-282`）。Runner 单例 `DAFT_RUNNER: OnceLock`（`runners.rs:294`），只能设置一次。
4. `RayRunner.run_iter`：优化逻辑计划 → `DistributedPhysicalPlan.from_logical_plan_builder`（`daft/runners/ray_runner.py:620-624`）→ `FlotillaRunner.stream_plan`（`ray_runner.py:669-671`）。
5. `FlotillaRunner.stream_plan` 把计划交给常驻单例 Ray actor `RemoteFlotillaRunner`（`daft/runners/flotilla.py:786-816`），逐个 `get_next_partition` 拉取结果。
6. 驱动侧 Rust：`PyDistributedPhysicalPlanRunner`（`src/daft-distributed/src/python/mod.rs:236-258`）持有 `PlanRunner<RaySwordfishWorker>` + `RayWorkerManager`；`run_plan`（`python/mod.rs:260-327`）做 逻辑计划→pipeline node 翻译（`:302-307`）→ 建 `StatisticsManager`（`:311`）→ `PlanRunner::run_plan`（`:318-320`）。
7. `PlanRunner::run_plan` 起 scheduler actor 并异步跑计划：`src/daft-distributed/src/plan/runner.rs:159-189`（`spawn_scheduler_actor` 在 `:170`）；`run_plan_impl` 里 `pipeline_node.produce_tasks(...)` → `RunningPlan::materialize`（`runner.rs:202-206`）。

```rust
// src/daft-distributed/src/plan/runner.rs:159
pub fn run_plan(self: &Arc<Self>, query_idx: QueryIdx,
    pipeline_node: DistributedPipelineNode, statistics_manager: StatisticsManagerRef) -> DaftResult<PlanResult> {
    let runtime = get_or_init_runtime();
    let (result_sender, result_receiver) = create_channel(1);
    let this = self.clone();
    let joinset = runtime.block_on_current_thread(async move {
        let mut joinset = create_join_set();
        let scheduler_handle = spawn_scheduler_actor(self.worker_manager.clone(), &mut joinset, statistics_manager.clone());
        joinset.spawn(async move { this.run_plan_impl(pipeline_node, query_idx, scheduler_handle, statistics_manager, result_sender).await });
        joinset
    });
    Ok(PlanResult::new(joinset, result_receiver))
}
```

### 1.2 driver / worker 角色划分
- **Driver（scheduler）**：单进程内的 `SchedulerLoop` 事件循环 + `Dispatcher` + `Scheduler` 策略对象；由 `PlanRunner` 持有，跑在"driver 的 tokio runtime"上。
- **Worker**：每个 Ray 节点一个 `RaySwordfishActor`（`daft/runners/flotilla.py:139-289`），内部 `NativeExecutor(is_flotilla_worker=True, ip=...)`（`flotilla.py:185`），真正执行本地物理计划（Swordfish pipeline）。
- 关键类型：
  - `DistributedPhysicalPlan`：只包 `LogicalPlan` + config + query_idx，**翻译发生在 run 时**（`src/daft-distributed/src/plan/mod.rs:35-73`）。`QueryIdx` 单调递增，越小优先级越高（`plan/mod.rs:28-32`）。
  - `PipelineNodeImpl` trait：`children()` / `produce_tasks()` / `multiline_display()`（`pipeline_node/mod.rs:344-361`）。
  - `DistributedPipelineNode`：`Arc<dyn PipelineNodeImpl>` + runtime_stats + children（`pipeline_node/mod.rs:363-368`）；`produce_tasks` 外包一层 `OnEndStream` 通知统计管理器（`mod.rs:399-409`）。
  - `MaterializedOutput`：物化结果 = `Vec<PartitionRef>` + 产出 worker id + ip + task_id，用于把后续任务调度回同一 worker（`pipeline_node/mod.rs:130-135`，调度意义见注释 `:126-128`）。
- `TaskBuilderStream` 是任务的生产流：每个 `SwordfishTaskBuilder` 在 `build()` 时分配 `TaskID`、拼 `TaskContext`、抽取资源请求（`pipeline_node/mod.rs:491-541`；`scheduling/task.rs:544-593`）。

## 2. 计划划分与 pipeline 执行

### 2.1 逻辑计划 → pipeline node
- 翻译器是 `LogicalPlanToPipelineNodeTranslator`，以 `TreeNodeVisitor::f_up` 自底向上建图（`pipeline_node/translate.rs:43-56`、`:121-128`）。
- 入口 `logical_plan_to_pipeline_node(plan_config, plan, psets, meter)`（`translate.rs:43-56`），在 Python 层被调用于 `python/mod.rs:302-307`。
- **是否需要 shuffle 的判定**：`can_skip_hash_repartition`（`translate.rs:96-118`）——单分区直接跳过；clustering 键被算子键覆盖时跳过（`clustering_is_covered_by`）。
- Repartition 逻辑算子统一走 `gen_repartition_node`（`translate.rs:369-372`）；distinct 两阶段（`translate.rs:451-510`）；`LogicalPlan::Shuffle` → `RandomShuffleNode`（`translate.rs:669-676`）。
- 算子类别决定"是否阻塞"：`NodeCategory` 只有 4 类 `Intermediate/Source/StreamingSink/BlockingSink`（`src/common/metrics/src/ops.rs:74-80`）。distributed 侧 15 处 `BlockingSink`（Repartition/Gather/Aggregate/Sort/BroadcastJoin/Write 等），4 处 `StreamingSink`（Limit/Sink 等）。

### 2.2 stage 之间如何物化
- 物化 = 把 pipeline node 产出的 `SubmittableTask` 提交给调度器并等结果：`materialize_all_pipeline_outputs` 里 `task_finalizer`（提交）与 `task_materializer`（用 `OrderedJoinSet` 保序收结果）两条协程（`pipeline_node/materialize.rs:23-93`）。
- 阻塞算子读下游结果的做法：
  - Repartition：`local_shuffle_write_node.materialize(...)` 后交给 shuffle 后端发 reduce 任务（`shuffles/repartition.rs:76-92`）。
  - BroadcastJoin：把 broadcast 侧 `try_collect` 全部物化，再 `into_in_memory_scan_with_psets`（`join/broadcast_join.rs:209-236`）。
  - Aggregate：`gen_gather_node`（无 group_by）或 `gen_repartition_node(Hash)`（`aggregate.rs:254-276`），两阶段时分区数被 `shuffle_aggregation_default_partitions` 截断（`aggregate.rs:306-311`）。
  - Sort：先采样求分位边界再 Range 重分区（`sort.rs:88-153`、`:181`、`:281`）。

### 2.3 worker 上的执行（复用 daft-local-execution）
- 任务是 `SwordfishTask { plan: LocalPhysicalPlanRef, inputs, psets, config, resource_request, strategy }`（`scheduling/task.rs:241-251`）。
- worker 侧：`RaySwordfishActor.run_plan` 调 `native_executor.run(plan, ctx, task_id, resolved_inputs, context, false)`（`daft/runners/flotilla.py:231-238`），即在 worker 上跑 `daft-local-execution` 的 pipeline（Swordfish），**完全复用**本地引擎。
- 输出合并优化：非分区输出按 64MiB 聚合成一个 MicroPartition，分区输出（RepartitionWrite/GatherWrite）跳过合并以免破坏 transpose 的顺序语义（`flotilla.py:242-275`，判定 `plan.has_partitioned_output()` 在 `:245`）。
- worker 间结果通过 Ray object store 传 `ray.ObjectRef`（`RayPartitionRef`），Flight shuffle 则传 `FlightPartitionRef`（`flotilla.py:334-360`）。

## 3. 调度与任务生命周期

### 3.1 核心 trait / 状态机
- `Scheduler` trait：`update_worker_state / enqueue_tasks / schedule_tasks / get_autoscaling_request / num_pending_tasks`（`scheduling/scheduler/mod.rs:26-34`）。
- `Worker` trait：`id/active_task_details/total_num_cpus/total_num_gpus`（`scheduling/worker.rs:13-33`）。
- `WorkerManager` trait：`submit_tasks_to_workers / mark_task_finished / mark_worker_died / worker_snapshots / try_autoscale / cleanup_shuffle_dirs / shutdown / retire_idle_workers`（`scheduling/worker.rs:35-76`）。
- `Task` trait：`priority / task_context / resource_request / strategy / task_name / task_metadata`（`scheduling/task.rs:98-122`）。
- 任务优先级：query_idx 小优先 → node_id 大优先 → task_id 小优先（`task.rs:225-237`）。
- 终态：`TaskStatus::{Success{result,stats}, Failed{error}, Cancelled, WorkerDied, WorkerUnavailable}`（`task.rs:597-608`）；`TaskResultAwaiter` 用 `tokio::select!` 在 cancel token 与结果之间偏向前者（`task.rs:629-641`）。
- 事件：`TaskEvent::{Submitted, Scheduled, Completed, Failed{retryable}, Cancelled}`（`statistics/mod.rs:35-59`），由 `TaskEvent::new` 从 `TaskStatus` 映射；**只有 WorkerDied / WorkerUnavailable 标记 `retryable: true`**（`statistics/mod.rs:76-91`）。

### 3.2 事件循环（无独立心跳线程，1s tick 轮询 worker 快照）
- `SchedulerLoop::run`：循环直到 输入耗尽 && 无 pending && 无 running（`scheduling/scheduler/scheduler_actor.rs:105-108`）。
- 每轮：拉 worker 快照（`worker_manager.worker_snapshots()`，`:109`）→ 先发扩容请求再调度（`:123-135`）→ `schedule_tasks()` 取就绪/取消任务（`:138-143`）→ 派发（`:163-164`）→ 下采样退役空闲 worker（`:173-182`）。
- 等待用 `tokio::select!`：新任务 / 任务完成 / 1s tick（`scheduler_actor.rs:33-34`、`:193-201`）。
- `handle_new_tasks` 会一次性 drain 通道里所有可拿任务成批入队（`scheduler_actor.rs:69-99`）。

### 3.3 分配算法（locality / 负载 / 资源匹配）
- 两种策略：`SchedulingStrategy::{Spread, WorkerAffinity{worker_id, soft}}`（`scheduling/task.rs:195-199`），`build()` 默认 `Spread`（`task.rs:549`）。
- `DefaultScheduler`：pending 用 `BinaryHeap`（按 priority），`schedule_tasks` 逐个 pop，能放则放并登记 `active_task_details`（`scheduler/default.rs:121-143`）。
- Spread：在可容纳的 worker 中选"可用 CPU+GPU 最多"者（`default.rs:48-56`）。
- WorkerAffinity：优先目标 worker，`soft=true` 时回退 Spread（`default.rs:60-77`）。实际使用点：`shuffles/pre_shuffle_merge.rs:142`、`:166`（预聚合结果留在原 worker），Asof join `join/asof_join.rs:591`。
- 资源匹配只考虑 CPU/GPU，**内存尚未纳入**：`WorkerSnapshot::can_schedule_task` 注释明说 memory TODO（`scheduler/mod.rs:239-245`）。
- 任务资源请求来自本地物理计划：`SwordfishTaskBuilder::build` 里 `self.plan.resource_request()`（`scheduling/task.rs:571`）。

### 3.4 派发、失败与重试、取消
- `Dispatcher::dispatch_tasks`：按 worker 分组 → `worker_manager.submit_tasks_to_workers` → 每个结果句柄 spawn 到 `JoinSet`，并保留 `joinset_id_to_task` 以便重排（`scheduling/dispatcher.rs:36-69`）。
- `await_completed_tasks`：先 await 一个，再 `try_join_next_with_id` 批量收割（`dispatcher.rs:82-96`）；成功→回传结果，失败→回传错误终止查询，取消→忽略，`WorkerDied`/`WorkerUnavailable`→重新塞回 `pending` 队列（`dispatcher.rs:111-138`）。
- 因此**任务级重试只在 worker 失效时发生；普通执行错误不重试**（`dispatcher.rs:120-124`、`statistics/mod.rs:76-85` 注释互证）。
- worker 失效处理：`mark_worker_died` 从 map 中移除该 worker（`python/ray/worker_manager.rs:264-270`），worker 会被下轮 `start_ray_workers` 增量补回（`python/ray/worker_manager.rs:105-152`）。
- Ray 侧 worker 死亡识别：捕获 `ActorDiedError`/`ActorUnschedulableError` → `worker_died()`，`ActorUnavailableError` → `worker_unavailable()`（`daft/runners/flotilla.py:361-364`；Rust 映射 `python/ray/task.rs:19-24`、`:105-125`）。
- 取消：任务带 `CancellationToken`，`SchedulerHandle::prepare_task_for_submission` 建立 oneshot 回传通道（`scheduler_actor.rs:322-352`）；取消回调最终落到 `ray.cancel(result_handle)`（`flotilla.py:372-375`）。
- 自动扩缩容：`needs_autoscaling` 用 pending/总CPU 比值与阈值比较（阈值 `DAFT_AUTOSCALING_THRESHOLD`，默认 1.25；`scheduler/default.rs:23`、`:40-44`、`:88-109`）；worker manager 实现 gradual / bisect 两种策略（`python/ray/worker_manager.rs:26-42`），bisect 超时默认 30s（`:23`、`:176-183`）。

### 3.5 流式 limit（v0.7.14 引入的 LimitCounterActor）
- Rust 侧 `LimitNode::limit_execution_loop`：起 actor → 给每个下游 task 的本地计划插入 `LocalPhysicalPlan::distributed_limit`（`pipeline_node/limit.rs:174-186`）→ 等 actor 报告贡献者集合 → 贡献者全部完成后取消其余任务（`limit.rs:63-82` 的 `limit_loop_done`、`:144-198`）。
- actor 实现（Python）：`_LimitCounterImpl` 维护 `remaining_skip / remaining_take / input_claims`（`daft/execution/ray_distributed_limit.py:22-26`）。
  - **幂等/退款机制**：`start_task(input_id)` 若发现该 input_id 有历史 claim，说明上次 attempt 崩溃重试，先把旧 claim 退回（`ray_distributed_limit.py:28-38`）；`claim(input_id, num_rows)` 先 skip 再 take，并把增量累加进 `input_claims`（`:40-55`）；`claim` 返回值第三项是"是否已取满"。
  - `contributors()` 只返回 take>0 的 input（`:60-61`）——用于提前收敛；`await_limit_completion` 轮询 10ms（`:63-66`）。
  - actor 是 `ray.remote(num_cpus=0)`（`:69`），并被强制 NodeAffinity 固定到 runner 所在节点以减少 RPC 跨跳（`:89-97`）；启动用 `__ray_ready__` + `asyncio.wait_for(timeout)`（`:102-108`）。

### 3.6 通信协议
- **调度器 ↔ worker 的控制面是 Ray actor 方法调用**，不是 Flight/gRPC：`RaySwordfishActorHandle.submit_task` → `actor_handle.run_plan.options(name=task.name()).remote(plan, config, context, **inputs)`（`daft/runners/flotilla.py:390-402`）。
- **数据面**：Ray object store（`ray.ObjectRef` → `RayPartitionRef`）；flight shuffle 走 **Arrow Flight（gRPC/tonic）** 从 worker 上的 shuffle server 拉数据（`src/daft-shuffles/src/server/flight_server.rs:216-219` 实现 `FlightService`；客户端 `src/daft-shuffles/src/client/flight_client.rs:20-46`）。
- 任务统计以二进制 `ExecutionStats::decode` 回传（`python/ray/task.rs:113`；编码侧 `daft/runners/flotilla.py:284-288`）。

## 4. Shuffle 实现

### 4.1 后端与选择
- `ShuffleBackend::{Ray, Flight{shuffle_id, shuffle_dirs, compression}}`（`src/daft-local-plan/src/plan.rs:2420-2437`）；读侧 `ShuffleReadBackend::{Ray, Flight}`（`:2451-2455`）。
- 选择逻辑：配置 `shuffle_algorithm == "flight_shuffle"` → Flight，否则 Ray（`shuffles/translate_shuffle.rs:24-35`）。`pre_shuffle_merge` 判定在 `translate_shuffle.rs:110-132`（`auto` 时用 `sqrt(in×out) > pre_shuffle_merge_partition_threshold`）。
- 分区策略类型来自逻辑层：`RepartitionSpec::{Hash, Random, Range}`（`src/daft-logical-plan/src/partitioning.rs:10`），`HashRepartitionConfig`（`:127`）、`RangeRepartitionConfig`（`:175`）。
- shuffle_id 编码：`(query_idx << 32) | node_id`（`shuffles/backends/mod.rs:19-21`）；partition ref id 编码 `(input_id << 32) | partition_idx`（`src/daft-shuffles/src/shuffle_cache.rs:22-24`）。

### 4.2 写侧与"每任务一个文件"
- Ray 后端（map-reduce）：每个 map task 产出 `num_partitions` 份分区数据。一致性由 `ShuffleContext` 统一封装：Ray 用 `in_memory_scan` + `psets`，Flight 用 `shuffle_read` + `with_flight_shuffle_reads`（`shuffles/backends/mod.rs:86-123`）。
- Flight 后端 **v0.7.14 的"one shuffle file per task"**：`write_partitions_one_shot` 把一个 map task 的所有输出分区写进**单个 IPC 文件**，文件内按 `[schema][partition0 batches]...[partition N-1][EOS]` 排列，并记录每分区的 `(start,end)` 字节区间（`src/daft-shuffles/src/oneshot_writer.rs:1-6`、`:58-80`）；写盘在**单个 `spawn_blocking` 线程**里做，注释说明曾因 N=8192 分区时 160 万次 task 分配而改为串行（`oneshot_writer.rs:70-77`）；`BufWriter` 1MiB 缓冲减少 syscall（`:22-27`）。
- 调用点：本地 `RepartitionWrite` sink（`src/daft-local-execution/src/sinks/repartition.rs:213`）；Gather/IntoPartitions 用 `InProgressShuffleCache`（`sinks/gather.rs:48`、`sinks/into_partitions.rs:81`）。
- 传统逐分区写路径：`InProgressShuffleCache::try_new` 用 `make_ipc_writer(dir, target_filesize, compression)`（`src/daft-shuffles/src/shuffle_cache.rs:87`），异步 writer task + `async_channel` 缓冲 `num_cpus*2`（`:97-99`）；IPC chunk 目标 4MiB（`CHUNK_TARGET_BYTES`，`:30`）；目录布局 `{base}/daft_shuffle/{shuffle_id}/partition_ref_{id}`，按 `partition_ref_id % dirs.len()` 选盘（`:10-20`）。
- 压缩：走 `make_ipc_writer` 的 compression 参数，默认来自 `flight_shuffle_compression`（默认 `lz4`，`src/common/daft-config/src/lib.rs:199`）；oneshot writer 用 `IpcWriteOptions::try_with_compression`（`oneshot_writer.rs:79-80`）。
- spill 到磁盘：flight shuffle 本身即落盘（`flight_shuffle_dirs` 默认 `["/tmp"]`，`daft-config/src/lib.rs:198`），计划结束由 `PlanExecutionContext::register_shuffle_dirs` 收集并在 `run_plan_impl` 里统一清理（`plan/runner.rs:91-93`、`:213-217`；目录构造见 `shuffles/backends/flight.rs:20-30`，实际删除是 Ray task `_clear_flight_shuffle_dirs`，`flotilla.py:63-99`）。

### 4.3 读侧与服务端
- 服务端 `ShuffleFlightServer` 持有 `HashMap<(shuffle_id, partition_ref_id), PartitionCache>`（`src/daft-shuffles/src/server/flight_server.rs:84-109`）；`get_shuffle_file_specs` 把请求**按文件分组区间读**（同一文件只开一个 FD，区间按 start 排序利于预读），无 byte_ranges 时整文件读（`:111-167`）。
- 同节点读取走进程内 `get_partition_local`，不经过 gRPC（`flight_server.rs:169-213`）。
- 客户端 `ShuffleFlightClient` 复用 `FlightClient` 连接（`src/daft-shuffles/src/client/flight_client.rs:20-46`）。
- 读任务的 refs 重建（避免协调器 O(map×partitions) 内存）：`fold_outputs_from_stream` 只保留每 server 的 map input id 列表，reduce 端再还原精确 refs（`shuffles/backends/flight.rs:42-85`）。
- 本地读算子 `ShuffleReadSource`：并行度取 `scantask_max_parallel`（为 0 时用 compute 池线程数，`src/daft-local-execution/src/sources/shuffle_read.rs:44-49`）；`to_server_requests` 合并同一 server 的请求（`:62-80`）；本地 server 与远程 Flight 流 `select_all` 合并（`:82-117`）。
- 读端 merge/去重最终由 `MaterializedOutput` + 各算子处理；shuffle 读在计划里表现为 `LocalPhysicalPlan::shuffle_read`（`plan.rs:2477-2484`）。

### 4.4 shuffle 在 join / aggregate / sort / limit 中的使用
- **Hash join**：两侧都必须**精确**按 join key 哈希分区（用 `is_exact_partition_match`），分区数按 `hash_join_partition_size_leniency` 折中（`join/translate_join.rs:80-146`）。
- **Broadcast join**：小表整体物化在 driver，然后作为 `in_memory_scan` + `psets` 附加到**每个** receiver task（`join/broadcast_join.rs:209-262`），即小表随任务分发到各 worker；阈值 `broadcast_join_size_bytes_threshold` 默认 10MiB（`daft-config/src/lib.rs:172`），策略判定 `translate_join.rs:49-64`（外连接方向受限、Outer 不支持广播 `:187-191`）。
- **Key filtering join**：仅 Python feature，走 `KeyFilteringJoinNode`（`join/translate_join.rs:352-377`；实现 `join/key_filtering_join.rs`）。
- **Sort-merge join**：按两侧最大分区数做（`translate_join.rs:223-253`，实现 `join/sort_merge_join.rs`）。
- **聚合**：两阶段（局部预聚合 + shuffle 后最终聚合），无 group_by 退化为 gather 单分区（`aggregate.rs:254-276`、`:291-340`）。
- **Sort**：采样（`sample_size_for_sort`，`sort.rs:201`）→ 合并样本求 quantiles 边界（`:178-181`）→ Range 重分区（`:281`）。
- **分布式 limit**：不 shuffle，用中心化 actor 做全局配额（见 3.5）。

## 5. 运行时与资源

### 5.1 Ray 集成
- 每个 Ray 节点（`Resources.CPU > 0` 且 `memory > 0`）起一个 `RaySwordfishActor`，用 `NodeAffinitySchedulingStrategy(soft=False)` 钉在节点上；CPU/GPU 声明直接取自 `node["Resources"]["CPU"]` / `["GPU"]`（`daft/runners/flotilla.py:449-483`）。
- actor 内：GPU 时设 `CUDA_VISIBLE_DEVICES`（`flotilla.py:177-178`）；按可见 CPU 设置 swordfish 线程数 `set_compute_runtime_num_worker_threads`（`:180`）；启动时取 `native_executor.shuffle_address()` 作为 flight server 地址（`:187-191`）。
- 与既有集群共存：若 Ray 已初始化则复用现有 context 并忽略 address（`daft/runners/ray_runner.py:556-569`）；`RemoteFlotillaRunner` 是 `get_if_exists=True` 的命名 actor，并用头节点 affinity（`flotilla.py:767-792`）。
- 扩展（`.so`）通过 `DAFT_EXTENSION_PATHS` 环境变量经 `runtime_env` 传播到 worker（`flotilla.py:102-136`、`:461-475`）。
- 扩容：`try_autoscale` → `ray.autoscaler.sdk.request_resources(bundles=...)`（`flotilla.py:569-574`）；下采样开关/阈值全在 worker manager 读环境变量（`python/ray/worker_manager.rs:304-330`）。

### 5.2 Kubernetes
- 只有 Helm quickstart chart：`k8s/charts/quickstart/`。`values.yaml` 默认 `distributed: false`（native 单 Job），镜像 `rayproject/ray:2.46.0-py312-cpu`（`k8s/charts/quickstart/values.yaml:24`、`:37`）。
- distributed 模式下 job 通过 `RAY_ADDRESS` + `DAFT_RUNNER` 连接集群（`templates/job.yaml:67-69`）；head 执行 `ray start --head --block`（`templates/head-deployment.yaml:38`），worker `ray start --block`、副本数 `worker.replicas`（`templates/worker-deployment.yaml:11`、`:61`）。
- `src/daft-cli` 只含 `dashboard.rs/lib.rs/python.rs`，**没有** k8s 相关代码。

### 5.3 分布式相关配置
`DaftExecutionConfig`（`src/common/daft-config/src/lib.rs:120-155`），默认值 `:164-199`：
- `shuffle_algorithm="auto"`（`:189`）、`pre_shuffle_merge_threshold=1GB`（`:190`）、`pre_shuffle_merge_partition_threshold=200`（`:191`）
- `flight_shuffle_dirs=["/tmp"]`、`flight_shuffle_compression=Some("lz4")`（`:198-199`）
- `broadcast_join_size_bytes_threshold=10MiB`（`:172`）、`hash_join_partition_size_leniency=0.5`（`:173`）
- `shuffle_aggregation_default_partitions=200`（`:184`）、`partial_aggregation_threshold=10000`（`:185`）、`high_cardinality_aggregation_threshold=0.8`（`:186`）
- `scantask_max_parallel`（`:146`）、`actor_udf_ready_timeout=120`（`:194`）、`maintain_order=true`（`:195`）
- `enable_scan_task_split_and_merge=false`、`scan_tasks_min/max_size_bytes=96MB/384MB`（`:167-169`）
- 环境变量映射：`DAFT_SHUFFLE_ALGORITHM`（`:206`、`:220`）、`DAFT_SCANTASK_MAX_PARALLEL`（`:207`）、`DAFT_ACTOR_UDF_READY_TIMEOUT`（`:209`）、`DAFT_MAINTAIN_ORDER`（`:214`）、`DAFT_NATIVE_PARQUET_WRITER`（`:208`）。
- 调度/扩缩容环境变量：`DAFT_AUTOSCALING_THRESHOLD`（`scheduler/default.rs:41`）、`AUTOSCALER_UPDATE_INTERVAL_S`（默认 5s，`python/ray/worker_manager.rs:19-22`、`:191-196`）、`DAFT_AUTOSCALING_PENDING_RELEASE_EXCLUDE_SECONDS`（默认 120，`:116-119`）、`DAFT_AUTOSCALING_DOWNSCALE_ENABLED` / `DAFT_AUTOSCALING_MIN_SURVIVOR_WORKERS`（`:317-327`）、`RAY_DISABLE_DASHBOARD`（`python/mod.rs:290`）、`DAFT_TASK_EVENTS_ENABLED`（`statistics/task_lifecycle.rs:24-28`）。
- **注意**：本版本没有分布式任务级"最大重试次数/超时"配置项；重试仅由 worker 失效触发（见 3.4）。

## 6. 可观测性

- 指标使用 OpenTelemetry：`Meter` 由 `common_metrics` 提供，`Meter::query_scope(query_id, "daft.execution.distributed")` 在 `python/mod.rs:300` 创建；`Meter::query_scope/global_scope` 定义于 `src/common/metrics/src/meters.rs:130-150`。
- 每个算子的指标带 `node_id` / `node_type` 属性：`key_values_from_context`（`pipeline_node/metrics.rs:6-11`，常量 `ATTR_NODE_ID`/`ATTR_NODE_TYPE`）；算子具体计数器如 BroadcastJoin 的 build/probe rows/bytes（`join/broadcast_join.rs:36-95`）。
- 分布式统计聚合：`StatisticsManager` 消费 `TaskEvent` 并把 worker 侧 `StatSnapshot` 归并到 runtime stats（`statistics/mod.rs:176-215`、`statistics/stats.rs:212-249`）；节点产出结束时通过 `OnEndStream` 通知（`pipeline_node/mod.rs:76-117`、`:404-406`）。
- Tracing span：scheduler 主循环 `#[instrument(name = "FlotillaScheduler", skip_all)]`（`scheduling/scheduler/scheduler_actor.rs:101`）；调度/派发日志 target 为 `DaftFlotillaScheduler` / `DaftFlotillaDispatcher`（`scheduler_actor.rs:33`、`dispatcher.rs:16`）。
- 进度条：`FlotillaProgressBar` 订阅 TaskEvent，bar id = `(query_idx << 32) | last_node_id`（`python/progress_bar.rs:9-60`），在 `python/mod.rs:280` 无条件加入订阅者列表。
- Dashboard：`DashboardStatisticsSubscriber` 按 task 参与的 node 输出 stats（`python/dashboard.rs:36-80`），在 `RAY_DISABLE_DASHBOARD != "1"` 时启用（`python/mod.rs:289-294`）；worker 侧通过 `DAFT_DASHBOARD_URL` + `runtime_env` 拿到地址（`flotilla.py:156-169`、`:438-446`）。Dashboard 数据模型在 `src/daft-dashboard/src/{engine.rs,state.rs,events.rs}`。
- 任务生命周期事件（可用于外部日志/审计）：`DAFT_TASK_EVENTS_ENABLED=true`（默认关，`statistics/task_lifecycle.rs:24-28`）→ `TaskLifecycleEventSubscriber`（`python/mod.rs:282-287`）；本地执行侧对称开关在 `src/daft-local-execution/src/run.rs:320-324`。

## 7. 源码地图

| 文件/目录 | 职责 | 关键类型 |
|---|---|---|
| `src/daft-distributed/src/plan/mod.rs` | driver 侧计划抽象与结果流 | `DistributedPhysicalPlan`、`PlanResult`、`QueryIdx` |
| `src/daft-distributed/src/plan/runner.rs` | 起调度器、跑计划、清理 shuffle 目录 | `PlanRunner`、`PlanExecutionContext`、`TaskIDCounter` |
| `src/daft-distributed/src/pipeline_node/mod.rs` | pipeline node DAG 抽象与物化载体 | `PipelineNodeImpl`、`DistributedPipelineNode`、`MaterializedOutput`、`TaskBuilderStream` |
| `src/daft-distributed/src/pipeline_node/translate.rs` | 逻辑计划 → pipeline node（含 shuffle 跳过判定） | `LogicalPlanToPipelineNodeTranslator` |
| `src/daft-distributed/src/pipeline_node/{limit,aggregate,sort,join/*}.rs` | 各分布式算子 | `LimitNode`、`BroadcastJoinNode`、`HashJoinNode`、`SortNode` |
| `src/daft-distributed/src/pipeline_node/materialize.rs` | 任务提交与结果按序收集 | `materialize_all_pipeline_outputs` |
| `src/daft-distributed/src/pipeline_node/shuffles/*` | 重分区/gather/预合并节点与后端抽象 | `RepartitionNode`、`GatherNode`、`PreShuffleMergeNode`、`ShuffleContext` |
| `src/daft-distributed/src/scheduling/{task,worker,dispatcher}.rs` | 任务/worker 抽象与派发 | `Task`、`SwordfishTask`、`Worker`、`WorkerManager`、`Dispatcher` |
| `src/daft-distributed/src/scheduling/scheduler/*` | 调度策略与事件循环 | `Scheduler`、`DefaultScheduler`、`SchedulerLoop`、`SchedulerHandle` |
| `src/daft-distributed/src/python/{mod,ray/*}.rs` | pyo3 绑定与 Ray worker 管理 | `PyDistributedPhysicalPlanRunner`、`RayWorkerManager`、`RaySwordfishWorker` |
| `src/daft-distributed/src/statistics/*` | TaskEvent、runtime stats、生命周期事件 | `StatisticsManager`、`TaskEvent`、`TaskLifecycleEventSubscriber` |
| `src/daft-shuffles/src/shuffle_cache.rs` | 逐分区 IPC 落盘缓存与读元数据 | `InProgressShuffleCache`、`PartitionCache` |
| `src/daft-shuffles/src/oneshot_writer.rs` | 每 map task 单文件写全部输出分区 | `write_partitions_one_shot` |
| `src/daft-shuffles/src/server/flight_server.rs` | Arrow Flight shuffle 服务端 | `ShuffleFlightServer`、`FlightServerConnectionHandle` |
| `src/daft-shuffles/src/client/flight_client.rs` | shuffle 读取客户端（连接复用） | `ShuffleFlightClient` |
| `src/daft-local-execution/src/sources/shuffle_read.rs` | worker 侧 shuffle 读算子 | `ShuffleReadSource` |
| `src/daft-local-execution/src/sinks/{repartition,gather,into_partitions}.rs` | worker 侧 shuffle 写 sink | `RepartitionWrite` 等 sink 实现 |
| `src/daft-runners/src/{runners,python}.rs` | Runner 单例、`DAFT_RUNNER` 解析 | `Runner`、`RayRunner`、`set_runner_ray` |
| `daft/runners/ray_runner.py` | Ray runner 驱动入口 | `RayRunner`、`RayMaterializedResult` |
| `daft/runners/flotilla.py` | Ray actor 定义、worker 启动、结果拉取 | `RaySwordfishActor`、`RemoteFlotillaRunner`、`start_ray_workers` |
| `daft/execution/ray_distributed_limit.py` | 分布式 limit 配额 actor（claim/refund） | `_LimitCounterImpl`、`LimitCounterActor` |
| `src/common/daft-config/src/lib.rs` | 分布式相关执行配置与默认值 | `DaftExecutionConfig` |
| `k8s/charts/quickstart/` | K8s/Ray 部署 chart（native 或 Ray 集群） | values.yaml、head/worker deployment |
