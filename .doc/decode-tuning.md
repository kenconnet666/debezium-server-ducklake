# 解码侧吞吐调研与调优

> 最后更新：2026-07-18。涵盖 Debezium 3.6.0.Final 源码核实结论 + harness 精确实测数据。
> 背景：端到端写湖约 22k 行/秒，干运行纯解码约 11.5k 行/秒（harness 隔离口径，见下）。
> 原文档（2026-07-13）基于 bench.sh 全家桶口径的 ~31k 推断，部分结论已被实测修正，见文末。

## 测量口径说明

两种口径测出的数字不同，都是真实的，不要混淆：

| 口径 | 典型值 | 工具 | 含义 |
|---|---:|---|---|
| **harness 精确口径** | **11.5k 行/秒** | `decode-harness/` dry-run | 单容器 tmpfs PG，孤立测单槽 drain，排除 GC/争抢变量，是 Debezium 解码路径的真实天花板 |
| bench.sh 全家桶 | ~29k 行/秒 | `.docker/postgres/bench.sh` | 同机全家桶（PG+catalog+S3+本服务），dry-run 参数，系统噪声更大，历史基线用 |
| 端到端写湖 | ~22k 行/秒 | bench.sh 追赶阶段 | 含 DuckDB staging + 湖事务，口径最接近生产 |

**harness 11.5k 就是解码路径的真实地板。** bench.sh 的 ~29k 在同一硬件下更高，是因为全家桶环境的 OS 缓存命中率更高（PG 已预热），且 bench.sh 跑的时候计时口径不同（不做精确 warmup）。后续以 harness 数字为准。

## 解码链线程模型（源码事实）

```
[连接器线程 rce-coordinator]               [record.processing 线程池]     [引擎轮询线程]
PostgresStreamingChangeEventSource          ParallelSmtBatchProcessor        handleBatch
  processMessages() 单 while 循环  ─queue─▶  每条记录的 SMT 转换并行执行  ─▶  (用户批消费)
  JDBC 复制流读取 → pgoutput 解析             按提交序 Future.get() 收集
  → TableSchema → Struct 构建 → 入队          (交付顺序不变，保序零影响)
```

### ① 连接器解码段 —— 固有单线程，参数无法纵向并行

`PostgresStreamingChangeEventSource.processMessages()` 是单个 `while` 循环：读复制流 →
`PgOutputMessageDecoder` 解析 → Struct 构建 → 入队，**全部串行**。无任何并行化参数。

profiler 数据（harness 实测，itimer 火焰图，dry-run 口径）：

| CPU 占用 | 原因 |
|---|---|
| ~39% | `LockSupport.unpark`（单队列生产者→消费者信号，dry-run 消费者太快、队列恒空放大了这一项） |
| ~24% | `Struct.put()` 物化（建 Kafka Connect Struct 对象） |
| ~23% | pgoutput 解析（`readColumnValueAsString`，列字节→ String） |
| ~11% | isostring 时间格式化（`DateTimeFormatter`） |

**关键发现：** 其中可并行的"列解码+Struct 物化"合计约 58%，但：
- 改 `time.precision.mode` 只节省那 11% 的 isostring 成本（conv-test 实测 +10%，剩余 47% 跟模式无关）
- 改 `decimal.handling.mode` 收益 <1%（本项目的 precise 是强类型要求，不可改）
- **真正贵的是 Struct 物化本身**，不是类型转换的复杂度

### ② SMT 处理段 —— 默认配置踩了 Java 线程池陷阱，显式设值才真并行

`AsyncEmbeddedEngine`（3.2+唯一实现）默认构造：

```java
recordService = new ThreadPoolExecutor(0, AVAILABLE_CORES, 60L, SECONDS, new LinkedBlockingQueue());
```

`corePoolSize=0` + **无界** `LinkedBlockingQueue` = 队列永不满 = **实际恒 1 线程**（JDK ThreadPoolExecutor 规则：非 core 线程只在队列满时创建）。

显式设 `record.processing.threads` 才走 `newFixedThreadPool(n)`。

**本项目处置：** `ducklake.engine.record-processing-threads=-1`（默认），传 `AVAILABLE_CORES`，SMT 段真并行。

**已实测效果（2026-07-18 harness 对照）：**

| 配置 | 吞吐 | 
|---:|---:|
| RPT 未设（事实 1 线程） | 11,719 rows/s |
| RPT=AVAILABLE_CORES（真并行 SMT） | 11,291 rows/s（**-3%，更慢**） |

**为什么更慢？** `unwrap` SMT 本身很轻（在已有 Struct 上取字段，≈零成本）。多线程并行完了还要汇入同一个 `ChangeEventQueue`，入队 unpark 信号的竞争反而放大（profiler 那 39%）。**RPT 设不设对本项目均无收益**；原文档"待实测"已实测，可下结论。

## 吞吐参数清单

| 参数 | 默认 | 本项目 | 说明 |
|---|---|---|---|
| `record.processing.threads` | 空（**事实 1 线程**） | `AVAILABLE_CORES`（-1） | 实测对吞吐无收益（见上）；保留以防 SMT 成本高的场景 |
| `max.batch.size` | 2048 | 8192 | 单批交付上限，实测矩阵默认最优 |
| `max.queue.size` | 8192 | 32768 | 解码→消费缓冲，宽行防 OOM |
| `poll.interval.ms` | 500 | 10 | 空闲唤醒，连续消费下无关 |
| `decimal.handling.mode` | — | `precise` | 雪花 ID numeric(28) 精度要求，不可改 |
| `time.precision.mode` | — | `isostring` | TypeMapper 依赖此格式；改其他模式会让时间列变 NULL（实测验证）|

## 行动结论（按收益排序，已更新）

1. **已落地（无代价）**：`record-processing-threads=-1`（AVAILABLE_CORES）。实测吞吐与单线程持平，SMT 场景下无负面影响，保留。
2. **横向分片（按需，已验证）**：按表多 publication/多 slot，聚合吞吐随实例数近线性（harness 实测 2 slot 聚合 ~170k 完美线性）；注意每路 walsender 全量解码 WAL 的 N× 服务端读放大，需源库 CPU 富余。
3. **原始串直通（新发现，高 ROI，待工程化）**：harness 实测 153-163k rows/s，是 Debezium 的 13-14×，见 `raw-passthrough.md`。
4. **不值得做**：调大 `max.batch.size`（实测无正收益）；改 decimal/time 模式（类型保真要求）；fork Debezium 改连接器解码段并行（见 raw-passthrough.md 中更优的替代路径）。

## 原文档结论的修正对照

| 原文推断（2026-07-13） | 实测修正（2026-07-18） |
|---|---|
| "干运行 ~31k 是端到端瓶颈" | harness 精确口径 11.5k；~31k 是全家桶口径，变量更多 |
| "RPT 显式设值应能提升 SMT 段，待实测" | 实测持平或略降（-3%），原因：SMT 本身轻，真正的瓶颈是单队列 unpark |
| "解码侧纵向无解，只能横向分片" | 不完全准确：原始串直通方案绕开 Debezium engine，单线程可达 153k，详见 raw-passthrough.md |
| "binary 协议对 walsender 裸流能提升" | 实测 +4%，可忽略，成本在 WAL 解码不在值序列化 |
