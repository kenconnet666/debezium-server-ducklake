# CDC 吞吐优化：两种方案设计

> 2026-07-18。基于 `decode-harness` 精确实测数据，所有性能数字均可在该仓库复现。
> 调研背景见 `decode-tuning.md`；方案 A 的核心原型代码在 `decode-harness/src/main/java/RawPgConsumer.java`。

## 背景：瓶颈精确定位

五点实测数据链（均在 `decode-harness` tmpfs 隔离环境，PostgreSQL 17，~100B/行）：

```
PG 源库写入               220–344k rows/s   源库从不是瓶颈
walsender 单 slot 裸流      90–100k rows/s   C 语言天花板（pg_recvlogical）
Java 切帧 + new String        70–80k rows/s   绕开 Struct，只读字节
完整 Debezium                  11.5k rows/s   基线——83% 损失在这里
RawPgConsumer + DuckDB        153–163k rows/s  原始串直通，见方案 A
```

**83% 的损失源自两件事**（profiler + Stage1 三点定位交叉验证）：
- `new Struct().put()` 物化 Kafka Connect Struct（~24%）
- `LockSupport.unpark` 单队列生产者→消费者信号（~39%）

改配置旋钮（`time.precision`/`decimal.handling`/`record.processing.threads`）只能触及其中
11–14%；剩余 69% 是"建 Struct + 入队"这两个动作本身，不是转换复杂度。

---

## 方案 A：原始串直通（单解码，零读放大）

### 核心思想

**绕开 Debezium engine，直接用 pgjdbc replication API 读 pgoutput 流。**

```
pgoutput 流
  → pgjdbc readPending()：每次返回一条完整消息（切帧免费）
  → Relation 消息：解析 OID + typmod → 缓存每列的 DuckDB 类型
  → Insert/Update：列字节 → new String() → DuckDB Appender 送 staging（全 VARCHAR）
  → Begin/Commit：管理批边界
  → 投影时：CAST/TRY_CAST 基于 OID 精确还原目标类型
```

不建 Struct，不走 ChangeEventQueue，不做类型化往返——ducklake 的 staging 路径本来就是全 VARCHAR + CAST，只是省掉了 Debezium 那层"中间类型化再文本化"的白干活。

### DuckDB 1.5.4 对 PG 文本格式的 CAST 兼容性矩阵

> 实测验证，所有 ✅ 均在 RawPgConsumer VERIFY=1 模式下通过了 400k 行全量验证。

| PG 类型 | pgoutput 文本格式示例 | DuckDB castExpr | 精度/注意 |
|---|---|---|---|
| `bool` | `t` / `f` | `TRY_CAST(col AS BOOLEAN)` | ✅ DuckDB 原生识别 `t`/`f` |
| `int2/4/8` | `12345` | `CAST(col AS SMALLINT/INTEGER/BIGINT)` | ✅ |
| `float4/8` | `3.14` / `NaN` / `Infinity` | `CAST(col AS FLOAT/DOUBLE)` | ✅ 含特殊浮点值 |
| `numeric(p,s)` | `123456789012345678901234` | `CAST(col AS DECIMAL(p,s))` | ✅ typmod 解析精度；雪花 ID DECIMAL(28,0) 完全无损 |
| `numeric`（裸） | `123.456789` | `CAST(col AS DECIMAL(38,18))` | ✅ |
| `date` | `2024-02-29` | `TRY_CAST(col AS DATE)` | ✅ |
| `time` | `12:34:56.123456` | `TRY_CAST(col AS TIME)` | ✅ |
| `timetz` | `12:34:56.789012+00` | `TRY_CAST(col AS TIMETZ)` | ✅ |
| `timestamp` | `2024-02-29 12:00:00.123456` | `TRY_CAST(col AS TIMESTAMP)` | ✅ PG 格式直接解析 |
| `timestamptz` | `2024-02-29 12:00:00.123456+00` | `TRY_CAST(col AS TIMESTAMPTZ)` | ✅ 含时区 |
| `interval` | `1 year 2 mons 3 days 04:05:06.7` | `TRY_CAST(col AS INTERVAL)` | ✅ PG 原始格式；⚠️ ISO 8601（`P1Y...`）格式 DuckDB 不识别 |
| `uuid` | `550e8400-...` | `CAST(col AS UUID)` | ✅ |
| `json` / `jsonb` | JSON 文本 | `col`（VARCHAR） 或 `TRY_CAST(col AS JSON)` | ✅ |
| `text` / `varchar(n)` | 文本 | `col` | ✅ 直通 |
| `bytea` | `\x48656c6c6f` | `unhex(substring(col, 3))` | ⚠️ 需跳过 `\x` 前缀，用 `unhex` |
| 数组 | `{1,2,3}` / `{t,f}` | `TRY_CAST(replace(replace(col,'{','['),'}',']') AS TYPE[])` | ⚠️ 花括号替换后可 TRY_CAST |

**结论：除数组和 bytea 需轻量字符串处理外，所有 PG 类型文本格式可直接被 DuckDB CAST 到精确类型，强类型完全保留。**

### 实测性能

| 测试 | 吞吐 | 说明 |
|---|---:|---|
| VERIFY=0（无 DuckDB，纯读流） | **153,584 rows/s** | |
| VERIFY=1（DuckDB Appender + CAST 投影验证） | **162,882 rows/s** | sink=400,000，100% 类型正确 |
| 对比：完整 Debezium 基线 | 11,500 rows/s | |
| 对比：walsender 裸流（pg_recvlogical） | ~90–100k rows/s | |

VERIFY=1 与 VERIFY=0 几乎一样快——说明 DuckDB Appender staging + CAST 投影的代价可忽略不计，与 ducklake 现有两阶段写路径完全对齐。RawPgConsumer 超过 walsender C 基线的原因：批量 LSN ack（每8192条一次）让 walsender 推更大的滑动窗口。

### 工程化待做（从原型到生产）

| 工作项 | 难度 | 说明 |
|---|---|---|
| LSN offset 持久化 | 低 | 复用 JDBC offset 表，周期 UPDATE `confirmed_flush_lsn` |
| interval 格式适配 | 低 | 在 PG 连接设 `SET intervalstyle=postgres`（已是默认），pgoutput 输出即 PG 原始格式，DuckDB 直接识别 |
| Delete/Update 完整语义 | 中 | RawPgConsumer 已跳过 Update old-tuple，需补完 upsert/delete-by-key 逻辑；可直接复用 ducklake 的 staging → QUALIFY + DELETE 两步 SQL |
| DDL 跟随（ALTER TABLE） | 中 | pgoutput Relation 消息反映 DML 时的当前 schema，但 ALTER TABLE 本身不产生 Relation 消息；需配套 DDL 审计流（PG event trigger），可直接复用 ducklake 现有的 `DdlApplier` |
| 初始快照 | 已有 | 复用 `ScannerBootstrap`（postgres_scanner 直拉，实测 36× 快于 Debezium 快照） |
| MySQL 支持 | 不适用 | pgoutput 专用；MySQL 路径继续走 Debezium binlog 连接器 |

**实施优先级建议：** 先补 LSP offset 持久化（低风险，解锁生产可用），再补 Delete/Update，最后接 DDL 审计流。初始快照和 DDL 审计流复用现有代码，工程量不大。

---

## 方案 B：多 publication/多 slot 分片（N 读放大的横向扩展）

### 核心思想

**不改代码，多起实例，每实例独立 slot + publication，聚合吞吐随实例数近线性扩展。**

每个实例：独立 replication slot → 独立 walsender → 独立 Debezium engine → 独立 DuckDB 实例写各自分片的湖表。

### 吞吐收益实测

| 配置 | 聚合吞吐 | 说明 |
|---|---:|---|
| 1 slot（基线） | ~90–100k rows/s | pg_recvlogical 裸流，harness 测 |
| 2 slot（各独立表） | ~170k rows/s | **完美 2× 线性**（32 核机器实测）|

### 读放大成本

每个 walsender 都要**全量解码 WAL 再按 publication 过滤**。N 个实例 = N× WAL 读放大：

| 分片方式 | 读放大 | 说明 |
|---|---|---|
| 按**独立表** publication 分片 | ≈ N×（但每路只解码自己表的 WAL） | 最优：服务端过滤有效减少实际解码量 |
| 按**行过滤**（hash 分片同一表） | N×（每路解码全量 WAL，只输出一半） | harness 实测：2 slot 仅 1.25× 而非 2×，因为每路 walsender 扫全量再过滤 |

**结论：按表拆 publication 是正确分片方式；同一张热表无法通过分片获得接近 2× 的收益。** 单张大热表的吞吐瓶颈建议用方案 A 解决。

### 配置示例（PG 源）

```yaml
# 实例 1：只订阅 schema_a 的表
source:
  slot-name: dbz_ducklake_a
  publication-name: pub_schema_a  # FOR TABLE IN SCHEMA schema_a
  schema-include-list: schema_a

# 实例 2：只订阅 schema_b 的表  
source:
  slot-name: dbz_ducklake_b
  publication-name: pub_schema_b
  schema-include-list: schema_b
```

两实例分别写各自的湖表，catalog 可共享同一 PG 实例（使用不同的 offset 表名前缀隔离）。

### 适用场景

| 场景 | 建议方案 |
|---|---|
| 多 schema / 多业务模块，各模块表数相当 | **方案 B**：按 schema 分 publication，零代码 |
| 单张超高写入热表（如核心订单表） | **方案 A**：原始串直通，单线程 153k，无读放大 |
| 需要 MySQL 支持 | **方案 B**：MySQL 走 Debezium binlog，方案 A 不支持 |
| 聚合吞吐 > 200k，硬实时 | 方案 A + 方案 B 组合 |

---

## 两方案对比

| 维度 | 方案 A：原始串直通 | 方案 B：多 slot 分片 |
|---|---|---|
| 单槽吞吐 | **153–163k rows/s** | ~11.5k rows/s（每实例不变） |
| 聚合吞吐 | 单线程即达 153k | N × 11.5k（N 实例） |
| 读放大 | **零**（单 slot） | N×（每路全量 WAL） |
| 代码改动 | **需要**（新 CDC reader 替换 Debezium engine，PG 专用） | **零**（纯配置） |
| MySQL 支持 | ❌ | ✅ |
| DDL 跟随 | 需配合 event trigger（可复用现有 DdlApplier） | ✅ 现有机制 |
| 初始快照 | 复用 ScannerBootstrap | ✅ 现有机制 |
| 工程风险 | 中（需完整集成测试） | 低（已验证可行） |
| 实施时间 | 1–2 周（按工程化清单逐项） | 半天（配置） |
