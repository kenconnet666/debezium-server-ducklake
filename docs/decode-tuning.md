# 解码侧（Debezium）吞吐调研与调优

> 2026-07-13，基于 Debezium 3.6.0.Final 源码逐行核实。背景：实测单流解码 ~31k 行/秒（宽行表，
> dry-run 纯解码口径）是端到端瓶颈，写入侧有 3 倍余量——本文回答"解码侧还有多少可挖"。

## 解码链的线程模型（源码事实）

```
[连接器线程 rce-coordinator]                 [record.processing 线程池]        [引擎轮询线程]
PostgresStreamingChangeEventSource           ParallelSmtBatchProcessor          handleBatch
  processMessages() 单 while 循环   ──queue──▶  每条记录的 SMT 转换并行执行  ──▶  (用户批消费)
  JDBC 复制流读取 → pgoutput 解析              按提交序 Future.get() 收集
  → TableSchema → Struct 构建 → 入队           (交付顺序不变,保序零影响)
```

两段的结论截然不同：

### ① 连接器解码段——固有单线程，无参数可开并行

`PostgresStreamingChangeEventSource.processMessages()`（v3.6.0 L254）是单个
`while (context.isRunning())` 循环：读复制流 → `PgOutputMessageDecoder` 解析 → Struct 构建 →
入队全部串行。**没有任何并行化参数**。这一段就是单槽 ~31k 的硬地板，源头上只能靠
"多 slot 分片"（多实例/多引擎各自解码）横向扩，纵向无解。

### ② SMT 处理段——默认配置踩了 Java 线程池经典陷阱，显式设值才真并行

我们用 `ChangeConsumer.handleBatch`（批接口）→ 引擎自动选
`ParallelSmtBatchProcessor`（`AsyncEmbeddedEngine.selectRecordProcessor()` 第一分支）：
每条记录的 SMT（unwrap 扁平化 + 墓碑 rewrite）作为独立任务提交线程池，**按提交序收集**
（`forEachOrdered` submit + 顺序 `Future.get()`），交付顺序与串行完全一致。

**陷阱**（`AsyncEmbeddedEngine` L166-172）：`record.processing.threads` 默认空 →

```java
recordService = new ThreadPoolExecutor(0, AVAILABLE_CORES, 60L, SECONDS, new LinkedBlockingQueue());
```

`ThreadPoolExecutor` 的规则是"超过 corePoolSize 的线程只在**队列满**时创建"——core=0 +
**无界** LinkedBlockingQueue = 队列永不满 = **实际恒 1 个线程**。官方 javadoc 声称该默认
"using Java 'Executors.newCachedThreadPool()'"（`AsyncEngineConfig` L32-33）——与实现不符
（cached pool 用 SynchronousQueue 才会按需扩线程）。JDK 25 下用同款构造实测复现：
顺序提交 500 个任务，默认分支恒用 **1** 个 worker 线程；`newFixedThreadPool(12)` 用满 12 个。

显式设置才走 `Executors.newFixedThreadPool(n)` 真并行（L171）。
`record.processing.order` 在 ChangeConsumer 分支被短路不读取（字段 Javadoc 原文：
"doesn't have any effect when ChangeConsumer is provided"），交付顺序恒等于原批次序。

**本项目处置**：`ducklake.engine.record-processing-threads` 默认 `-1` = 传
`AVAILABLE_CORES` 占位符（引擎解析为核数），SMT 段从事实单线程变真并行。
保序语义零影响；风险仅为 SMT 任务的线程调度开销（每行一个 Future，行级任务小时
收益取决于 SMT 单行成本占比——宽行 + rewrite 墓碑场景占比可观，窄行可能中性）。
**待部署环境 dry-run 对照实测确认收益幅度**（方法：同表同积压，`record-processing-threads`
0 vs -1 各跑一轮 slot 追赶）。

## 吞吐相关参数清单（3.6 默认值与本项目取值）

| 参数 | 默认 | 本项目 | 说明 |
|---|---|---|---|
| `record.processing.threads` | 空（**事实 1 线程**） | `AVAILABLE_CORES` | 上文陷阱；SMT 段真并行 |
| `record.processing.order` | ORDERED | —（不适用） | 只对 `Consumer` 单条接口生效；`ChangeConsumer` 批接口天然按序收集，不读此参数 |
| `max.batch.size` | 2048 | 8192 | 连接器内部批上限；实测 32768 反而更慢（单批交付体积大、重叠窗口短） |
| `max.queue.size` | 8192 | 32768 | 解码→消费的缓冲；堆内存量 = 队列 × 平均事件大小 |
| `max.queue.size.in.bytes` | 0（禁用） | 未设 | 按字节的队列上限，宽行表防 OOM 的补充手段 |
| `poll.interval.ms` | 500 | 10 | 仅空闲时燃烧的唤醒间隔（连续消费下 poll 立即返回） |
| `status.update.interval.ms` | 10000 | 默认 | 向 PG 回报 standby status 的频率，非吞吐路径（独立 keep-alive 线程） |
| `query.fetch.size` | 0 | 默认 | **只影响 snapshot 阶段**的 JDBC ResultSet 抓取——流式走复制协议不经 JDBC ResultSet，对稳态解码无效 |
| `decimal/time/binary handling` | — | precise/isostring | 类型保真需要，isostring 有每行格式化成本（换 numeric 保真，不可省） |

补充事实（源码核实）：
- **`Connect.class` 格式下 per-record converter 开销为 0**——`ConverterBuilder.toFormat()` 对
  Connect 分支只做轻量包装，不调用 `Converter.fromConnectData()`；`JsonConverter` 仅用于
  offset map 的低频序列化（`offset.flush.interval.ms` 节奏），与逐行吞吐无关。
- **PG 连接器 task 级并行也不存在**：`PostgresConnector.taskConfigs()` 硬编码返回单元素
  list 无视 maxTasks（官方文档：仅 SQL Server/MongoDB 支持单连接器多 task）。
- 解码线程每行 CPU 成本的压缩手段（对本项目逐一判定）：publication column filter 收窄列
  （不适用——全列入湖）、避开 `REPLICA IDENTITY FULL`（不可行——DELETE 整行墓碑必需）、
  `provide.transaction.metadata` 保持关闭（默认已关 ✓）。即本项目在解码线程成本侧已无可省项。
- 孵化中的 `queue.provider.type=hybrid_chronicle`（堆外/持久化队列，未入正式文档）理论上
  降宽行 GC 压力，成熟度未知——仅记录，不建议生产使用。

## 行动结论（按收益排序）

1. **已落地**：`record-processing-threads=-1`（AVAILABLE_CORES）——修默认单线程陷阱，
   唯一无语义代价的纵向优化点；收益待 dry-run 对照定量。
2. **横向扩（已验证可行、按需启用）**：按表分片多实例——每实例独立 slot/publication，
   聚合吞吐随实例数扩展；注意每个 walsender 全量解码 WAL 的 N× 服务端读放大，
   需要 CPU 富余的宿主才兑现（8C 饱和箱实测聚合≈单流）。
3. **不值得做**：调大 `max.batch.size`（实测负收益）；连接器解码段并行化（源码固有单线程，
   改造等于 fork Debezium）。
