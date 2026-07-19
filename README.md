# debezium-server-ducklake

PostgreSQL / MySQL → DuckLake 的单进程 CDC 入湖器。仓库名为历史遗留；当前运行时已经不再使用
Debezium Engine、Kafka Connect、SMT、schema history 或 signal 表：

```text
PostgreSQL pgoutput ── pgjdbc replication ─┐
                                           ├─ 全 VARCHAR staging ─ DuckLake 事务 ─ PG catalog + S3/本地文件
MySQL ROW binlog ─── BinaryLogClient ──────┘
```

Java 25 + Spring Boot 4.1 + DuckDB 1.5.4，单进程、零 Kafka。PG/MySQL 源由
`ducklake.source.type` 选择，对应的原生 reader 自动启动，不再有 engine 模式开关。

## 当前结论

2026-07-19 的本机 Docker Desktop 实测显示：

| 数据库 | 当前原生完整链 | 协议/写入上限 | 历史 Embedded 完整链 | 结论 |
|---|---:|---:|---:|---|
| PostgreSQL 18 | 追赶 68,870–87,873 行/秒；4 表 27,708–28,003 行/秒 | pgjdbc 直读 pgoutput 153,000–163,000 行/秒 | Debezium Embedded（无 Kafka）约 22,400–26,491 行/秒 | 原生路径有明显余量；冷表流式场景仍受首次建表阻塞 |
| MySQL 8.4 | 单表 59,934 行/秒；4 表 67,636 行/秒 | 解码 514,138 行/秒；解码 + staging 384,615 行/秒 | Debezium Embedded（无 Kafka）单表 25,803 行/秒；4 表 41,288 行/秒 | 原生完整链约为旧路径的 2.3× / 1.6× |

PostgreSQL 与 MySQL 的语义回归最近分别为 38/38（`DdlApplierTest`、原生 pgoutput 与完整应用链）
和 16/16；MySQL 两类吞吐测试为 4/4。PG 吞吐基准的追赶与 4 表项通过，但冷表在线流式项连续
两次约 4,430 行/秒，低于当前 5,000 行/秒断言。详细计时口径、延迟分位数与测试矩阵见下文，
不会把协议解码、源端造数和端到端落湖数据混为同一种吞吐。

## 数据与一致性语义

| 能力 | PostgreSQL | MySQL |
|---|---|---|
| 增量协议 | pgoutput | ROW binlog |
| 持久化位点 | `raw_pg_offset`：slot + LSN | `raw_mysql_offset`：source + file/position/GTID |
| 位点推进 | 对应 DuckLake 事务提交成功后 | 完整源事务落湖后 |
| INSERT / UPDATE / DELETE | 主键镜像 | 主键镜像 |
| 主键值 UPDATE | 旧键 delete + 新键 upsert | 旧键 delete + 新键 upsert |
| DDL | event trigger → `dbz_ddl_log` | binlog QueryEvent + `information_schema` |
| 首次存量/重灌 | 独立 `postgres_scan(...)` | 只读 MySQL scanner ATTACH |

写湖采用当前态镜像语义：同一批先按主键删除目标行，再对 staging 以主键取最后一条并 INSERT。
进程在“湖已提交、offset 尚未提交”的窗口崩溃时会重放，但主键 upsert/delete 保持幂等。

无主键表只能 insert-only：后续 INSERT 会进入湖，UPDATE/DELETE 无法可靠定位旧行；首次存量也不能
用主键 anti-join 安全收敛，因此 bootstrap 会记录为 `no-pk-skip`。生产业务表应配置主键。

### PostgreSQL 类型与精度

PG reader 从 `pg_catalog.pg_attribute`/`pg_type` 读取 `format_type`、`atttypmod` 和 `attndims`，
不再只依赖 `information_schema` 的顶层 `data_type`。因此下列声明会保留元素精度和维度：

| 源列 | 湖列 |
|---|---|
| `numeric(12,2)` | `DECIMAL(12,2)` |
| `numeric(12,2)[]` | `DECIMAL(12,2)[]` |
| `numeric(10,3)[][]` | `DECIMAL(10,3)[][]` |
| `numeric[]` | `DECIMAL(38,18)[]`（DuckDB 精度上限形态） |
| `amount_7_3[]`（域基于 `numeric(7,3)`） | `DECIMAL(7,3)[]` |

精度超过 DuckDB 38 位、复合类型/enum、以及当前没有可靠 PG 文本到 DuckDB 结构转换的数组，
整列保留为 `VARCHAR`，不静默截断。数组值通过 `TRY_CAST` 转为 DuckDB LIST；运行时维度超出
列声明的异常值可能转为 `NULL`，需要业务侧约束源数据形状。MySQL `DECIMAL/NUMERIC` 同样在
`p<=38` 时保留 `(p,s)`。

从旧版本升级时，历史上以 `VARCHAR` 保存的 PG 数组会在类型跟随换表时先把 `{...}` 文本
转换为 DuckDB 列表，再严格 `CAST` 到目标精度；无法转换的旧值会使迁移事务失败，避免静默丢值。

## MySQL 必要契约

原生 MySQL reader 启动时会 fail-fast 校验：

```ini
[mysqld]
server-id = 1
binlog_format = ROW
binlog_row_image = FULL
binlog_row_metadata = FULL
binlog_transaction_compression = OFF
gtid_mode = ON
enforce_gtid_consistency = ON
```

前四个 binlog 条件是硬要求。GTID 强烈建议启用：未启用时仍可按 file/position 续传，但切主不能
自动续位。`binlog_expire_logs_seconds` 必须覆盖可接受的最长停机窗口。

账号只需要读取元数据/scanner 和复制流：

```sql
CREATE USER IF NOT EXISTS 'dbuser_cdc'@'%' IDENTIFIED BY 'changeme';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT
  ON *.* TO 'dbuser_cdc'@'%';
```

不需要 signal、heartbeat 或 DDL 审计表。完整模板见
[.doc/init-source-mysql.sql](.doc/init-source-mysql.sql)。

## PostgreSQL 必要契约

- `wal_level=logical`，并为每个实例分配独立 slot 和 publication。
- 复制账号需 `REPLICATION` 与业务表只读权限。
- DDL 跟随需要 `dbz_ddl_log` 与两个 event trigger；初始化脚本已包含。
- publication 建议 `FOR ALL TABLES`；业务表应有主键。

完整模板见 [.doc/init-source-db.sql](.doc/init-source-db.sql)。原生 reader 会周期性向复制连接发送
standby status，不需要通过业务心跳表制造 WAL。

PG 首次接入严格先建立 replication slot/stream，再启动异步 scanner，封住“快照已读、slot 尚未建立”
的漏数窗口。每个 `BEGIN` 到 `COMMIT` 保持为不可拆分边界；pgoutput 的 unchanged-TOAST `u` 标记
不会被误写成 SQL `NULL`，主键值变更按旧键定位后更新。若 catalog 已有非零 LSN 但 slot 丢失，
reader 会 fail-fast，拒绝自动建新 slot 后静默跳 WAL。

首次存量和类型/主键重建使用独立的参数化 `postgres_scan(?, ?, ?)`，不把源 PG ATTACH 为共享
catalog；MySQL scanner 只读 ATTACH `source.dbname`。这样可避开 DuckDB 1.5.4 的 PostgreSQL
catalog 加载竞态，同时保留重灌时的主键 anti-join。reader 与维护任务共用单写者锁；任一湖提交或
offset 写入失败都会终止进程，由容器从最近已提交位点重放。

当前隔离策略对应的上游修复见 [duckdb-postgres #500](https://github.com/duckdb/duckdb-postgres/issues/500)、
[#503](https://github.com/duckdb/duckdb-postgres/pull/503)、[#502](https://github.com/duckdb/duckdb-postgres/issues/502)
和 [#506](https://github.com/duckdb/duckdb-postgres/pull/506)。等包含修复的稳定版本验证后再评估恢复并行查询。

## 快速启动

要求：Java 25、Docker Desktop Linux engine。构建使用仓库 Maven Wrapper：

```powershell
.\mvnw.cmd -DskipTests package
```

MySQL 一键栈：

```powershell
docker compose -f .docker/mysql/docker-compose.yml up -d
docker compose -f .docker/mysql/docker-compose.yml --profile app up -d --build
bash .docker/mysql/e2e-verify.sh
```

PostgreSQL 一键栈：

```powershell
docker compose -f .docker/postgres/docker-compose.yml up -d
docker compose -f .docker/postgres/docker-compose.yml --profile app up -d --build
bash .docker/postgres/e2e-verify.sh
```

第一条只启动源库、catalog PG 与 S3，适合 IntelliJ 本地运行；第二条连应用一起启动。WSL 可直接执行
`.sh` 验证脚本。持久数据在各栈的 `data/` 下，日常测试和维护不要删除或重置这些目录。

## 配置

生产配置入口是 [prod/ducklake.yml](src/main/resources/prod/ducklake.yml)。常用项：

| 配置 | 默认 | 作用 |
|---|---:|---|
| `source.type` | `POSTGRES` | `POSTGRES` / `MYSQL`，直接决定原生 reader |
| `source.name` | `ducklake` | MySQL catalog offset 命名空间；共享 catalog 时必须唯一 |
| `source.hostname/port/user/password/dbname` | — | 源库连接；MySQL `dbname` 也是 scanner 默认绑定库 |
| `source.slot-name` | `dbz_ducklake` | 仅 PG，复制槽 |
| `source.publication-name` | `dbz_publication` | 仅 PG，publication |
| `source.server-id` | `6400` | 仅 MySQL，集群内唯一的 replica client id |
| `source.schema-include-list` | 空 | PG schema / MySQL database 的逗号分隔正则 |
| `source.table-exclude-list` | 空 | `schema.table` / `db.table` 的逗号分隔正则 |
| `engine.max-batch-size` | `8192` | 已提交源事务的攒批上限 |
| `engine.poll-interval-ms` | `10` | 空闲轮询；flush 超时为 `max(poll×50, 500ms)` |
| `lake.memory-limit` | `1536MB` | DuckDB 内存上限 |
| `lake.threads` | `2` | DuckDB 并行度 |
| `lake.data-inlining-row-limit` | `512` | 小批内联阈值；`0` 明确禁用，避免沿用 DuckLake 非零默认值 |
| `lake.max-retry-count` | `20` | DuckLake catalog 乐观提交冲突重试次数 |
| `maintenance.scanner-refill` | `true` | DDL 重建后用 scanner 直灌当前态 |
| `maintenance.scanner-bootstrap` | `true` | 首次接入异步 scanner 存量首灌 |
| `maintenance.follow-*` | `true` | 类型、删列、删表、TRUNCATE 镜像开关 |
| `maintenance.schema-prefix` | 空 | 多源共享一个湖时隔离目标 schema |

容器环境变量示例：

```text
DUCKLAKE_SOURCE_TYPE=mysql
DUCKLAKE_SOURCE_NAME=mysql-shop
DUCKLAKE_SOURCE_HOST=mysql
DUCKLAKE_SOURCE_PORT=3306
DUCKLAKE_SOURCE_DBNAME=shop
DUCKLAKE_SOURCE_SERVER_ID=6400
DUCKLAKE_CATALOG_HOST=catalog-pg
DUCKLAKE_CATALOG_DB=ducklake_catalog
```

不存在 `DUCKLAKE_ENGINE=DEBEZIUM/RAW_*` 之类的模式开关；旧环境变量应从部署配置中删除。

## 性能、延迟与测试

### 测量环境与口径

| 项目 | 统一口径 |
|---|---|
| 日期 | 2026-07-19；数值来自本地 Codex 会话记录和本轮复跑 |
| 软件 | Java 25.0.3、Spring Boot 4.1、DuckDB 1.5.4 |
| 源库 | PostgreSQL 18、MySQL 8.4，均由 Docker Desktop 29.6.1 Linux engine 启动 |
| 湖与 catalog | DuckLake 本地临时数据目录 + PostgreSQL 18 catalog；不把 S3 网络耗时混入结果 |
| 数据量 | PG 吞吐基准 100,000 行（单表或 4×25,000）；MySQL 基准 200,000 行（单表或 4 表） |
| 公式 | `rows × 1000 / elapsed_ms`；四舍五入为整数行/秒 |
| `source INSERT` | 造数 SQL 的墙钟耗时；只在生产链基准中计入总耗时 |
| `reader drain` | 源端 INSERT 完成后，到预期事件/湖表计数达标的耗时 |
| `stage` / `lakeTx` | 最近一次已提交批次的分段耗时，不是 p50/p95；不能直接相加为端到端延迟 |

PG 的 `catchup` 是引擎启动后消费 slot 中已有 WAL，`streaming` 是在线 INSERT 到湖计数达标，
`multi-table` 是四张表连续造数到全部湖表达标。MySQL 生产链的 `total` 包含 source INSERT 和
reader drain；raw 协议基准不把 source INSERT 算进解码吞吐。

### 当前原生完整链吞吐

| 数据库 | 场景 | 行数 | 计时范围 | 耗时 | 吞吐 | 备注 |
|---|---|---:|---|---:|---:|---|
| PostgreSQL 18 | catchup 单表 | 100,000 | 引擎就绪 → 湖计数达标 | 1,138–1,452 ms | 68,870–87,873 行/秒 | 两次独立运行；包含首次 catchup 建表 |
| PostgreSQL 18 | streaming 单表 | 100,000 | source INSERT → 湖计数达标 | 22,567–22,570 ms | 4,430–4,431 行/秒 | INSERT 448–475 ms；首次湖表 DDL/排序约 21 秒，连续两次低于 5,000 断言 |
| PostgreSQL 18 | multi-table（4 表） | 100,000（4×25,000） | 四表 INSERT → 全部湖计数达标 | 3,571–3,609 ms | 27,708–28,003 行/秒 | 包含四张湖表的首次动态创建 |
| MySQL 8.4 | native 单表 | 200,000 | source INSERT → reader drain | 3,337 ms | 59,934 行/秒 | INSERT 2,096 ms；drain 1,241 ms；1 batch；stage 497 ms；lakeTx 268 ms |
| MySQL 8.4 | native 四表 | 200,000（4×50,000） | 四表 source INSERT → reader drain | 2,957 ms | 67,636 行/秒 | INSERT 2,568 ms；drain 389 ms；4 batches；stage 83 ms；lakeTx 202 ms |

PG streaming 的低值是冷表建湖成本，不应解释为稳态每行解码速度；当前基准保留该失败，便于后续
把“首次 DDL 延迟”和“已建表后的稳态流式吞吐”拆成两个测试。MySQL 早期一次运行曾得到单表
64,184、四表 75,872 行/秒，因运行时序不同只作为波动参考，不与上表最终代表值合并。

### 协议上限与历史对照

| 路径 | 数据库 | 行数 | source INSERT | drain/flush | 吞吐 | 口径与状态 |
|---|---|---:|---:|---:|---:|---|
| BinaryLogClient 纯 ROW 解码 | MySQL 8.4 | 200,000 | 1,775 ms（仅记录） | 389 ms | 514,138 行/秒 | 不写湖；当前 native reader 的协议上限 |
| BinaryLogClient + DuckDB staging | MySQL 8.4 | 200,000 | 1,814 ms（仅记录） | 520 ms（含 flush） | 384,615 行/秒 | 全 VARCHAR Appender；不含 DuckLake transaction |
| pgjdbc 直读 pgoutput | PostgreSQL | — | — | — | 153,000–163,000 行/秒 | 历史原生 harness；协议解码/入队对照，不等同完整落湖 |
| `pg_recvlogical` 裸流 | PostgreSQL | — | — | — | 90,000–100,000 行/秒 | 历史命令行对照 |
| 源库批量 INSERT | PostgreSQL | — | — | — | 220,000–344,000 行/秒 | 仅源端造数速度，不是 CDC 吞吐 |

旧 Debezium 完整链也保留为回归基线：MySQL 单表约 25,803 行/秒、四表约 41,288 行/秒；这些
数字来自历史路径，不能当作当前 native reader 的延迟或上限。raw MySQL 多轮记录范围为纯解码
401,606–514,138 行/秒、解码加 staging 384,615–434,782 行/秒，上表列出最终代表运行。

### 多槽解码与横向扩展边界

当前版本**没有把多槽解码加入单进程**：每个 `RawPgRunner` 只创建一个 `slotName`、一个
`publicationName` 和一个 reader 线程。`.docker/postgres/docker-compose.yml` 中的 `ducklake-b`
只是按 schema 启动第二个独立应用实例的注释模板，不是运行时自动分片。

历史 harness 做过按互不重叠的表/schema 分片的对照：

| 配置 | 历史吞吐 | 口径 | 状态 |
|---|---:|---|---|
| 1 slot | 约 90,000–100,000 行/秒 | `pg_recvlogical` 裸流 | 历史协议基线 |
| 2 slot，独立表/schema | 约 170,000 行/秒 | 两个独立实例聚合 | 约 2×，不是当前单进程测试 |

因此只能说“独立实例在该 harness 和该负载下近似线性”，不能承诺任意 N 槽都线性。每个
walsender 都会全量解码 WAL 后再按 publication 过滤，N 个实例会带来约 N 倍源库 WAL 解码/读取
开销；还要分别配置 slot、publication、offset 命名空间和湖 schema 前缀。按同一张热表做行级
hash 分片目前没有实现，也不能安全地把一个事务拆到多个 reader。

### 传统 Kafka CDC 路线对照

传统 Debezium 部署不是“给一个 slot 设置更多线程”，而是把源端、持久化日志和下游消费拆成
独立故障域：

```text
PostgreSQL WAL / MySQL binlog
  -> Debezium Source Connector
  -> Kafka data topics / partitions
  -> Kafka Connect Sink、Flink 或自定义 consumer group
  -> staging / merge
  -> DuckLake commit
```

`tasks.max` 只是 Kafka Connect 允许 connector 创建的任务上限。对单个 PostgreSQL 或 MySQL
connector，持续流式阶段仍是一条有序 WAL/binlog 流：PG 对应一个 replication slot，MySQL 对应
一个 binlog client；把 `tasks.max` 从 1 调大不会自动拆成多路源端解码。需要源端并行时仍要启动
多个 connector：PG 使用不同 slot/publication，MySQL 使用不同 server id，最好按互不重叠的
database/table 或源库分片。PG 多 slot 的源库开销与上一节相同。官方边界与部署模型见 Debezium
3.6 的 [架构说明](https://debezium.io/documentation/reference/3.6/architecture.html)、
[PostgreSQL connector](https://debezium.io/documentation/reference/3.6/connectors/postgresql.html) 和
[MySQL/Kafka Connect 教程](https://debezium.io/documentation/reference/3.6/kc-tutorial.html)。

Kafka 真正可横向扩展的是 broker 写入和下游 partition consumer。数据 topic 增加 partition 后，
consumer group / sink tasks 可以并行做转换和写入；但顺序只在单 partition 内成立，跨表、跨
partition 的数据库事务不会自动保持全局提交顺序。若 sink 需要源事务原子性，必须按事务路由、
使用事务元数据做屏障，或接受按表/键有序的较弱语义。当前 `DuckLakeEngine` 还有全局单写者锁，
因此即使 Kafka 前面有很多 partition，最终提交仍会在这里串行，除非先把写入按独立表/分片和连接
安全地拆开。

传统路线有两个有意分离的 checkpoint：Debezium 把事件可靠写入 Kafka 后保存源 LSN/binlog
offset，使 PG slot 可以继续确认 LSN，也使已入 Kafka 的 MySQL binlog 不再受 sink 进度约束；sink
只有在 DuckLake 提交成功后才提交 Kafka consumer offset。MySQL binlog 仍由源库 retention 策略
清理，并不由 consumer ack 直接删除。这正是 Kafka 的主要价值：长时间下游停机时把 backlog 留在
Kafka，而不是让 PG slot 持续保留 WAL；还可独立重放、扇出多个消费者并滚动升级 sink。代价是
序列化、网络、broker 磁盘与副本、两套 offset 和更多运维状态。

稳态端到端吞吐近似受最慢阶段限制：

```text
R_e2e <= min(R_source->Kafka, R_Kafka, sum(R_sink tasks), R_DuckLake commit)
```

Kafka broker 吞吐应同时看 MB/s 和 records/s，不能直接套成“行/秒”：Debezium event 包含 key、
before/after、source metadata，使用带 schema 的 JSON 时还可能远大于源行。例如平均序列化事件为 1 KiB，
100,000 事件/秒已经约为 100 MiB/s，尚未计副本流量。当前仓库没有在同一环境跑过
`Debezium -> Kafka -> DuckLake sink`，因此不能声称这条路线达到几十万或百万行/秒：

| 路径 | 已有吞吐 | 能否代表传统 Kafka 完整链 |
|---|---:|---|
| 当前 native reader -> DuckLake | PG catchup 68,870–87,873；MySQL 59,934–67,636 行/秒 | 是当前零 Kafka 完整链，不是 Kafka 路线 |
| native 协议解码 | PG 历史 153,000–163,000；MySQL 401,606–514,138 行/秒 | 只代表 source/read 上限 |
| 历史 Debezium Embedded dry-run | 28,701–29,000 行/秒 | 无 broker、无 Kafka sink |
| 历史 Debezium Embedded -> DuckLake | 22,400–26,491 行/秒 | 无 broker；仅作旧对象化路径基线 |
| Debezium -> Kafka -> DuckLake sink | 未测试 | 必须补同机分层基准，不能用 broker 压测数字代替 |

公平对照至少要分别测 source-to-Kafka、预灌 Kafka 后的 sink drain、完整端到端和长时间稳态，固定
事件序列化格式、平均/尾部事件字节数、partition 数、broker 副本数、connector/sink task 数和
DuckLake 批量。只有 source-to-Kafka 已经触顶时，多 connector/slot 才能提高源端吞吐；只有 sink
能按 partition 并行提交时，Kafka 的下游并行才会转化为落湖吞吐。

### 延迟数据

当前 native MySQL 基准只输出最近批次的阶段耗时，尚未收集事件级 p50/p95：

| 数据库/场景 | source INSERT | reader drain | 最近 `stage` | 最近 `lakeTx` | 是否为 p50/p95 |
|---|---:|---:|---:|---:|---|
| MySQL native 单表 | 2,096 ms | 1,241 ms | 497 ms | 268 ms | 否，单批观测 |
| MySQL native 四表 | 2,568 ms | 389 ms | 83 ms | 202 ms | 否，单批观测 |
| PostgreSQL streaming 冷表 | 448–475 ms | 22,092–22,122 ms | 未单独输出 | 未单独输出 | 否；主要是首次 DDL |

下表是历史 Debezium/旧整栈 benchmark 的延迟分位数，仅用于量级对照：

| 历史环境 | 端到端追赶吞吐 | dry-run 解码 | `batchLag` p50/p95 | `deliverLag` p50/p95 | `stage` p50/p95 | `lakeTx` p50/p95 | 空闲单行端到端 | CPU 峰值 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 环境 A（旧 Debezium） | 约 22,400 行/秒 | 约 29,000 行/秒 | 617 / 813 ms | 339 / 510 ms | 14 / 19 ms | 252 / 304 ms | 约 280 ms | 614% |
| 环境 B（6600H / WSL2，旧 Debezium） | 26,491 行/秒 | 28,701 行/秒 | 352 / 488 ms | 未记录 | 未记录 | 未记录 | 172 ms | 98% |

历史延迟表不能证明当前 native 路径的 p50/p95；要补齐该项，需要在 native reader 为每个源事务
记录提交时间、首次入队时间和湖事务提交时间，再以固定批量和重复次数统计分位数。

### 测试矩阵

下表只统计当前 `src/test/java` 中仍存在的测试类；`target` 里已删除的旧 Debezium 测试报告不计入。

| 数据库/层次 | 测试类 | 测试数 | Docker 依赖 | 最近结果 | 最近耗时 | 覆盖重点 |
|---|---|---:|---|---:|---:|---|
| 共同/DDL | `DdlApplierTest` | 13 | 否 | 13/13 | 4.08 s | CREATE/ALTER/RENAME/DROP、列注释、主键变化、numeric domain 数组精度与多维度 |
| PostgreSQL 原生 | `RawPgIntegrationTest` | 12 | PostgreSQL 18 | 12/12 | 68.59 s | pgoutput 启动、INSERT/UPDATE/DELETE、unchanged TOAST、主键变化、类型矩阵、DDL、schema、无主键、NULL |
| PostgreSQL 完整应用 | `DucklakeApplicationIntegrationTest` | 13 | PostgreSQL 18 | 13/13 | 93.25 s | scanner 首灌、流式 CRUD、DDL/重命名、合并、watermark、非 public schema、主键补加、数组/interval、TRUNCATE/DROP |
| PostgreSQL 吞吐 | `RawPgThroughputTest` | 3 | PostgreSQL 18 | 2/3 | 60.70 s | 100k catchup、100k streaming、4×25k；streaming 冷表项低于 5,000 行/秒断言 |
| MySQL 原生语义 | `MySqlIntegrationTest` | 16 | MySQL 8.4 + catalog PG 18 | 16/16 | 96.52 s | 快照、类型、CRUD、主键更新、CREATE/ALTER/RENAME/DROP/TRUNCATE、列序/注释、毒丸 TIME、主键重建、offset 回退与过滤事务 |
| MySQL 完整链吞吐 | `MySqlThroughputTest` | 2 | MySQL 8.4 + catalog PG 18 | 2/2 | 58.78 s | 200k 单表与四表 source/drain/批次/stage/lakeTx |
| MySQL 协议上限 | `RawMySqlThroughputTest` | 2 | MySQL 8.4 | 2/2 | 23.98 s | 200k 纯 binlog 解码、解码到 DuckDB staging |

已记录的语义回归为 PG 38/38、MySQL 16/16；加上 MySQL 吞吐 4/4，本轮新跑的 PG 吞吐为 2/3。
因此当前完整报告合计 60/61 通过，唯一失败是已知的 PG 冷表 streaming 性能断言，不是数据一致性
断言失败。集成测试耗时包含 Testcontainers 和 Spring 上下文启动，不应解释为 CDC 延迟。

### 运行命令

单元测试不需要 Docker：

```powershell
.\mvnw.cmd -q '-Dtest=DdlApplierTest' test
```

PG 与 MySQL 语义回归需要 Docker：

```powershell
.\mvnw.cmd -q '-Dtest=RawPgIntegrationTest,DucklakeApplicationIntegrationTest' test
.\mvnw.cmd -q '-Dtest=MySqlIntegrationTest' test
```

吞吐基准：

```powershell
.\mvnw.cmd -q '-Dtest=RawPgThroughputTest' test
.\mvnw.cmd -q '-Dtest=MySqlThroughputTest' '-Dmysql.bench.rows=200000' test
.\mvnw.cmd -q '-Dtest=RawMySqlThroughputTest' '-Dmysql.raw.rows=200000' test
```

吞吐日志会打印 source INSERT、reader drain、总耗时、湖批次数、stage 和 lake transaction；PowerShell
中建议将 `-Dtest=...` 和带逗号/数字的属性整体加引号。

### 后续性能工作

#### 先把瓶颈测准

MySQL 生产链测试在 source INSERT 期间 reader 已经并发消费，因此 `drain` 只是造数结束后的尾部，
不能用 `rows / drain` 推导消费者吞吐，也不能把 INSERT 占总耗时的比例直接当成瓶颈占比：

| 场景 | source INSERT | drain 尾部 | 总耗时/吞吐 | 当前能得出的结论 |
|---|---:|---:|---:|---|
| MySQL native 单表 | 2,096 ms | 1,241 ms | 3,337 ms / 59,934 行/秒 | 解码与写湖在 INSERT 期间重叠，无法从该结果单独归因 |
| MySQL native 四表 | 2,568 ms | 389 ms | 2,957 ms / 67,636 行/秒 | 四个源事务形成流水线，但 389 ms 不是完整消费耗时 |
| MySQL raw backlog | INSERT 不计入吞吐 | 解码 389 ms；解码 + staging 520 ms | 514,138 / 384,615 行/秒 | 已隔离协议解码与本地 staging，尚未覆盖 DuckLake 镜像事务 |

PG 的历史分层 harness 更能说明方向：纯 pgjdbc/pgoutput 为 153,584 行/秒，加入本地 DuckDB
Appender + CAST 投影为 162,882 行/秒，说明**本地 staging 与类型 CAST 不是主要损耗**。但该
`VERIFY=1` 没有覆盖当前生产链的 DuckLake `DELETE + INSERT` 镜像、catalog commit、DDL 屏障和
offset 持久化，不能据此断言全部写湖成本都可以忽略。当前完整 catchup 只有 68,870–87,873 行/秒，
新增差距更可能分布在：

- 每行 `String[]`、`boolean[]`、`PendingRow` 和每列 `new String` 的对象化与 GC；
- 事务内缓存后再次复制、按表/事件切段，再二次遍历写 Appender；
- staging 表反复 create/drop、主键 `DELETE + QUALIFY INSERT`、DuckLake catalog commit；
- 首次 `ensureTable`、排序配置、DDL 与 offset/LSN 提交。

所以首先要补：PG 的 decode、buffer/segment、stage、lakeTx、offset ack 分段计时；已建表后的固定
backlog；以及 append-only、镜像 INSERT、UPDATE/DELETE 三种负载。当前 PG 指标把整个写批耗时
记进 `stage`、`lakeTx` 记为 0，本身不足以证明哪一段最慢。

#### 解码后是否需要消息队列

要区分“采用 Kafka 架构”和“把 Kafka 当作当前吞吐补丁”。WAL/binlog 是源事实与短期恢复日志，
Kafka 是独立保留、重放和分发层，两者不能简单等同。若需要承受长时间 sink 停机、避免 PG slot
拖住 WAL、扇出多个消费者或独立回放，传统 Kafka 路线是合理选择；若目标只是提高当前单 sink、
单写者 DuckLake 的稳态吞吐，引入 Kafka 不会自动突破最慢阶段，应该先完成分段基准和写入拆分。

在继续保持本项目“单进程、零中间件”定位的前提下，更合适的局部优化仍是**有界的进程内事务
队列**，用于让网络解码和湖写入重叠，而不是承担持久化日志职责：

| 方案 | 判断 | 原因 |
|---|---|---|
| Kafka 持久化日志 | 按架构需求选择 | 解决长时缓冲、重放、扇出和下游 partition 扩展；增加序列化、broker、双 checkpoint 与运维成本 |
| RabbitMQ 等通用 MQ | 当前不做 | 不匹配本项目基于源 offset 的 CDC 重放模型，也不能自动提高单写者湖事务吞吐 |
| 无界内存队列 | 禁止 | sink 变慢时会无限占内存，掩盖源端到湖端的反压 |
| 有界事务队列 | 后续可做 | 吸收短时突发、让解码与写湖重叠，并在满时明确反压 |

队列元素必须是**完整源事务或完整提交批次**，不能直接按行并发消费。建议的最小 envelope 包含
`source`、结束 offset/LSN、源提交时间、事件序号、schema 版本和不可变事件列表；sink 按源顺序
合并多个完整事务，达到行数/字节数/等待时间任一阈值后写湖。只有 staging、湖事务和 offset 全部
成功，才向 PG slot 回 ack 或保存 MySQL offset；sink 失败时停止 reader 且不推进 offset。进程退出后
内存队列可以丢失，因为容器会从源日志的最后已提交位置重放。队列容量应同时限制行数和估算字节数，
并暴露 depth、oldest-age、enqueue-wait 和 backpressure 指标。

这类队列主要改善突发吸收和延迟抖动，**不会凭空突破单写者 DuckLake 的稳态上限**。当前
`RawPgReader`/`RawMySqlReader` 都在解码回调线程内调用 `DuckLakeEngine.withLock`；在 DuckDB/
DuckLake 单写约束和 catalog 并发缺陷未解除前，不能简单启动多个 lake writer。可先把纯 CPU 的
类型解析、列分组和 staging 缓冲做成可并行的预处理，再由一个有序 sink 提交。

多槽多实例不依赖这个队列：每个实例独立读取、处理和提交自己的表分片即可。若以后在单 JVM 中
运行多个 slot，只能按完整事务/批次做粗粒度交接；不能为每行入队。旧 Debezium profiler 中逐记录
队列的 `LockSupport.unpark` 曾占约 39% CPU，重新引入同类细粒度队列会抵消并行解码收益。

#### 建议实施顺序

1. **基准与 profiler**：增加 warm-table、固定 backlog、批次字节数和 p50/p95；分开统计 decode、对象分配/GC、segment、stage、lakeTx、offset ack。
2. **PG 多槽多实例**：把现有注释模板变成可执行的 1/2/4 slot 基准，按互不重叠的表/schema publication 分片；记录聚合吞吐、每槽 lag、源库 walsender CPU/WAL 读取和 catalog 重试。历史 2 slot 约 170k 是优先验证目标，不是当前生产承诺。
3. **单槽后处理**：用事务级紧凑/列式缓冲代替每行多数组和 record；减少 pending 复制与二次分段；复用 staging/SQL/元数据，单独优化镜像 `DELETE + INSERT`。
4. **粗粒度队列（可选）**：只有 profiler 证明 decode 与 sink 都有可重叠时间后，才实现单 reader → 有界事务批队列 → 单 ordered sink；做 queue on/off 对照，避免逐行信号成本。
5. **单进程多槽（后置）**：当前全局 `DuckLakeEngine` 锁会让多 reader 最终串行写湖。先用隔离更清晰的多实例获得扩展数据，再决定是否增加多 slot supervisor、分片 worker connection 和更细粒度锁。

每一步都必须重跑 offset 回退、主键变更、DDL 屏障、numeric 数组精度和 16 项 MySQL 语义回归；
吞吐提升不能以放宽事务原子性或 offset 恢复正确性为代价。

## 运维与可观测性

- `GET /api/ducklake/watermark` 返回 reader 状态、最近源提交时刻与最近落湖时刻。
- `/actuator/prometheus` 暴露 `ducklake_engine_running`、`ducklake_events_total`、
  `ducklake_batches_total`、`ducklake_last_stage_ms`、`ducklake_last_lake_tx_ms` 等指标。
- reader 致命断链时进程退出，由容器从最近已提交 offset 重启。
- `LakeMaintenanceJobs` 与写入共用单写者锁，执行内联 flush、分层/每日合并、快照过期与安全清理。
- 孤儿文件清理始终只做 `dry_run`，不自动删除。
- `lake.data-inlining-row-limit=0` 会明确禁用 Data Inlining。DuckLake 1.5.4 在 DROP/替换表后可能遗留
  `ducklake_inlined_%` side table，见 [#1237](https://github.com/duckdb/ducklake/issues/1237) 与
  [#1316](https://github.com/duckdb/ducklake/pull/1316)；需要规避时设为 0，并等待上游受支持的清理路径。

当前限制：单进程单写者；MySQL 仅支持 8.0+ 的未压缩 ROW binlog；PG DDL 需要 event trigger；
无主键表仅 insert-only；MySQL scanner 默认只 attach `source.dbname`，跨库表可流式同步但无法自动重灌存量。
