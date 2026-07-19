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

2026-07-19 在 WSL Debian（Docker 26.1.5、Java 25.0.3）的最终 `clean test` 复跑结果：

| 数据库 | 当前原生完整链 | 协议/写入上限 | 历史 Embedded 完整链 | 结论 |
|---|---:|---:|---:|---|
| PostgreSQL 18 | backlog 30,864；streaming 34,542；warm U/D 31,055/37,037；4 表 25,873 行/秒 | pgjdbc 直读 pgoutput 153,000–163,000 行/秒（历史 harness） | Debezium Embedded（无 Kafka）约 22,400–26,491 行/秒 | warm I/U/D 已有 100k 基线；完整链仍明显低于协议上限 |
| MySQL 8.4 | 50k I/U 26,983/24,154；25k D 27,685；4 表 20,903；500×10 backlog 5,917 行/秒 | 解码 439,560；解码 + staging 396,825 行/秒 | Debezium Embedded（无 Kafka）单表 25,803；4 表 41,288 行/秒 | bulk 改动未证明吞吐提升；小事务固定成本仍明显 |

当前源码中的 10 个测试类合计 71/71 通过，包括 PG/MySQL 语义、scanner 完整链、offset 回退、
binlog rotate、numeric 嵌套数组、TOAST、多表/小事务吞吐与纯协议上限。详细口径、耗时和覆盖矩阵
见下文；`target` 中已删除测试类的旧报告不计入。

当前性能主线限定为**单进程、单 source reader、单 DuckLake writer**：先简化解码后的批内表示、
staging 和镜像提交，降低单批 CPU、分配、SQL 往返与 catalog 成本。多槽、Kafka 和多 writer 对照
继续保留在文档中，但不进入近期实施顺序。

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
`.sh` 验证脚本。数据库/S3 持久数据在各栈的 `data/` 下，DuckDB 扩展缓存使用栈内 named volume；
日常测试和维护不要删除或重置这些目录/volume。

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
| 源库 | PostgreSQL 18、MySQL 8.4；WSL Debian Docker 26.1.5 |
| 湖与 catalog | DuckLake 本地临时数据目录 + PostgreSQL 18 catalog；不把 S3 网络耗时混入结果 |
| 数据量 | PG 100,000 行 I/U、50,000 行 D 或 4×25,000；MySQL 50,000 行 I/U、25,000 行 D、500×10 小事务、200,000 行 raw 协议 |
| 公式 | `rows × 1000 / elapsed_ms`；整数除法向下取整为行/秒 |
| `source INSERT` | 造数 SQL 的墙钟耗时；bulk/streaming 计入总耗时，backlog 只作源端诊断 |
| `reader drain` | 源端 INSERT 完成后，到预期事件/湖表计数达标的耗时 |
| `stage` / `lakeTx` | 最近一次已提交批次的分段耗时，不是 p50/p95；不能直接相加为端到端延迟 |

PG 的 `catchup` 会先停 reader、造出 slot backlog，再从 catalog LSN 重启计时，包含重连建流；
`streaming` 是在线 INSERT 到湖计数达标，`multi-table` 是四张表连续造数到全部湖表达标。MySQL
bulk 的 `total` 包含 source INSERT 和 reader 尾部；500×10 会停 reader 造 backlog 后单独计 drain；
raw 协议基准不把 source INSERT 算进解码吞吐。

### 当前原生完整链吞吐

| 数据库 | 场景 | 行数 | 计时范围 | 耗时 | 吞吐 | 备注 |
|---|---|---:|---|---:|---:|---|
| PostgreSQL 18 | catchup 单表 | 100,000 | reader 重启 → 湖计数达标 | 3,240 ms | 30,864 行/秒 | source 477 ms（不计）；1 batch；stage 922 ms；lakeTx 231 ms |
| PostgreSQL 18 | streaming INSERT | 100,000 | source INSERT → 湖计数达标 | 2,895 ms | 34,542 行/秒 | INSERT 542 ms；drain 2,353 ms；stage 520 ms；lakeTx 228 ms |
| PostgreSQL 18 | warm UPDATE | 100,000 | source UPDATE → 湖值达标 | 3,220 ms | 31,055 行/秒 | source 863 ms；drain 2,357 ms；stage 255 ms；lakeTx 213 ms |
| PostgreSQL 18 | warm DELETE | 50,000 | source DELETE → 湖计数达标 | 1,350 ms | 37,037 行/秒 | source 147 ms；drain 1,203 ms；stage 102 ms；lakeTx 122 ms |
| PostgreSQL 18 | multi-table（4 表） | 100,000（4×25,000） | 四表 INSERT → 全部湖计数达标 | 3,865 ms | 25,873 行/秒 | 3 batches；最近 stage 404 ms；lakeTx 124 ms |
| MySQL 8.4 | native INSERT | 50,000 | source INSERT → reader 尾部 | 1,853 ms | 26,983 行/秒 | source 760 ms；drain 1,093 ms；stage 302 ms；lakeTx 216 ms |
| MySQL 8.4 | warm UPDATE | 50,000 | source UPDATE → reader 尾部 | 2,070 ms | 24,154 行/秒 | source 1,616 ms；drain 454 ms；stage 121 ms；lakeTx 141 ms |
| MySQL 8.4 | warm DELETE | 25,000 | source DELETE → reader 尾部 | 903 ms | 27,685 行/秒 | source 229 ms；drain 674 ms；stage 59 ms；lakeTx 91 ms |
| MySQL 8.4 | native 四表 | 50,000（4×12,500） | 四表 source INSERT → reader 尾部 | 2,392 ms | 20,903 行/秒 | source 1,154 ms；drain 1,238 ms；3 batches；stage 30 ms；lakeTx 188 ms |
| MySQL 8.4 | 500×10 小事务 | 5,000 | 预积压后 reader 重启 → 湖计数达标 | 845 ms | 5,917 行/秒 | source 13,462 ms / 371 行/秒（不计）；1 batch；stage 16 ms；lakeTx 122 ms |

`stage/lakeTx` 保留为与历史输出兼容的最近批聚合值。新的 Timer 已把 decode、plan、lock wait、
ensure、staging DDL、Appender、mirror DML、commit、cleanup 和 offset/ack 独立记录。MySQL bulk 与
改动前同机 50k 基线（单表 35,688、
四表 20,798）相比没有可证明提升，因此这里只记录最新代表值，不把低风险分配优化写成吞吐收益。

### 协议上限与历史对照

| 路径 | 数据库 | 行数 | source INSERT | drain/flush | 吞吐 | 口径与状态 |
|---|---|---:|---:|---:|---:|---|
| BinaryLogClient 纯 ROW 解码 | MySQL 8.4 | 200,000 | 1,951 ms（仅记录） | 455 ms | 439,560 行/秒 | 不写湖；当前 native reader 的协议上限 |
| BinaryLogClient + DuckDB staging | MySQL 8.4 | 200,000 | 1,936 ms（仅记录） | 504 ms（含 flush） | 396,825 行/秒 | 独立运行；全 VARCHAR Appender；不含 DuckLake transaction |
| pgjdbc 直读 pgoutput | PostgreSQL | — | — | — | 153,000–163,000 行/秒 | 历史原生 harness；协议解码/入队对照，不等同完整落湖 |
| `pg_recvlogical` 裸流 | PostgreSQL | — | — | — | 90,000–100,000 行/秒 | 历史命令行对照 |
| 源库批量 INSERT | PostgreSQL | — | — | — | 220,000–344,000 行/秒 | 仅源端造数速度，不是 CDC 吞吐 |

旧 Debezium 完整链也保留为回归基线：MySQL 单表约 25,803 行/秒、四表约 41,288 行/秒；这些
数字来自历史路径，不能当作当前 native reader 的延迟或上限。raw MySQL 多轮记录范围为纯解码
401,606–514,138 行/秒、解码加 staging 384,615–473,933 行/秒；两项来自独立运行，不能用后者偶然
更高推导 staging 会提高解码速度。

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
| 当前 native reader -> DuckLake | PG 25,873–37,037；MySQL bulk 20,903–27,685 行/秒 | 是当前零 Kafka 完整链，不是 Kafka 路线 |
| native 协议解码 | PG 历史 153,000–163,000；MySQL 401,606–514,138 行/秒 | 只代表 source/read 上限 |
| 历史 Debezium Embedded dry-run | 28,701–29,000 行/秒 | 无 broker、无 Kafka sink |
| 历史 Debezium Embedded -> DuckLake | 22,400–26,491 行/秒 | 无 broker；仅作旧对象化路径基线 |
| Debezium -> Kafka -> DuckLake sink | 未测试 | 必须补同机分层基准，不能用 broker 压测数字代替 |

公平对照至少要分别测 source-to-Kafka、预灌 Kafka 后的 sink drain、完整端到端和长时间稳态，固定
事件序列化格式、平均/尾部事件字节数、partition 数、broker 副本数、connector/sink task 数和
DuckLake 批量。只有 source-to-Kafka 已经触顶时，多 connector/slot 才能提高源端吞吐；只有 sink
能按 partition 并行提交时，Kafka 的下游并行才会转化为落湖吞吐。

### 延迟数据

原生 reader 现在从 PG Commit 消息和 MySQL binlog EventHeader 传播真实源提交时间；
`deliverLag` 不再固定为 0，`batchLag` 是源提交到湖提交的跨主机墙钟口径；源库与应用必须同步
NTP，轻微源时钟超前会钳为 0。MySQL binlog 时间戳只有秒精度，相关延迟最多带约 999 ms 量化误差。
最近批聚合值如下：

| 数据库/场景 | source INSERT | reader drain | 最近 `stage` | 最近 `lakeTx` | 是否为 p50/p95 |
|---|---:|---:|---:|---:|---|
| MySQL INSERT | 760 ms | 1,093 ms | 302 ms | 216 ms | 否，单批观测 |
| MySQL warm UPDATE | 1,616 ms | 454 ms | 121 ms | 141 ms | 否，单批观测 |
| MySQL warm DELETE | 229 ms | 674 ms | 59 ms | 91 ms | 否，单批观测 |
| MySQL 500×10 backlog | 13,462 ms（不计 drain） | 845 ms | 16 ms | 122 ms | 否，恢复追赶单批 |
| PostgreSQL catchup | 477 ms（不计 drain） | 3,240 ms | 922 ms | 231 ms | 否，包含 reader 重启/冷表 |
| PostgreSQL streaming | 542 ms | 2,353 ms | 520 ms | 228 ms | 否，单批观测 |
| PostgreSQL warm UPDATE | 863 ms | 2,357 ms | 255 ms | 213 ms | 否，单批观测 |
| PostgreSQL warm DELETE | 147 ms | 1,203 ms | 102 ms | 122 ms | 否，单批观测 |

Micrometer `ducklake_batch_stage_duration_seconds` 固定使用 `reader` 和 `stage` 两个低基数标签，
每个 Timer 发布 p50/p95。下表来自开启 JFR 的五场景累计样本；PG 7 个成功批，MySQL 6 个成功批
（offset 含 2 个额外的独立位点样本）。样本少且混合冷建表与 warm DML，只用于首轮归因：

| 阶段 | PostgreSQL p50 / p95 | MySQL p50 / p95 | 判断 |
|---|---:|---:|---|
| decode | 83.362 / 318.243 ms | 74.449 / 418.382 ms | 包含 reader 内 tuple/row 物化；MySQL 协议库反序列化发生在回调前 |
| plan | 7.799 / 129.958 ms | 2.851 / 26.182 ms | PG 大批 `segmentsOf` 可见，但不是主导 CPU |
| lock wait | 0.006 / 0.007 ms | 0.005 / 0.127 ms | 当前无单写锁争用，不应以队列或多 writer 优化此项 |
| ensure | 335.542 / 637.532 ms | 24.116 / 117.439 ms | 混入冷表创建；需在 1M warm 矩阵单独看缓存命中样本 |
| staging DDL | 0.852 / 1.606 ms | 1.147 / 2.458 ms | create/replace 本身很小，暂不优先做 staging 复用 |
| Appender | 99.615 / 317.719 ms | 45.875 / 175.899 ms | 大批主要 Java 可见写入阶段之一 |
| mirror DML | 146.801 / 171.966 ms | 49.807 / 116.916 ms | 下一轮 mirror SQL A/B 的主要目标 |
| lake commit | 23.069 / 34.603 ms | 74.973 / 95.945 ms | MySQL 小批固定成本尤其明显 |
| cleanup | 0.803 / 0.999 ms | 0.721 / 1.999 ms | 当前不是瓶颈 |
| offset / ack | 5.112 / 7.733 ms | 2.359 / 3.670 ms | catalog 会话复用后占比较低 |

同轮 JFR `profile` 录制中，PG 项目代码最高 CPU 样本为 `segmentsOf` 2.70%、`stageRows` 1.20%；
MySQL 为 `writeBatch` 0.91%、`serializeRow` 0.68%。项目内分配热点分别是 PG `readUtf8/readTuple`
和 MySQL `serializeRow/serialize`，但更高占比来自 pgjdbc/协议库缓冲与字符串、数值物化。PG/MySQL
GC 暂停总计 245/223 ms，p95 为 30.5/25.6 ms；没有证据支持继续做零散对象级微优化。

下表是历史 Debezium/旧整栈 benchmark 的延迟分位数，仅用于量级对照：

| 历史环境 | 端到端追赶吞吐 | dry-run 解码 | `batchLag` p50/p95 | `deliverLag` p50/p95 | `stage` p50/p95 | `lakeTx` p50/p95 | 空闲单行端到端 | CPU 峰值 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 环境 A（旧 Debezium） | 约 22,400 行/秒 | 约 29,000 行/秒 | 617 / 813 ms | 339 / 510 ms | 14 / 19 ms | 252 / 304 ms | 约 280 ms | 614% |
| 环境 B（6600H / WSL2，旧 Debezium） | 26,491 行/秒 | 28,701 行/秒 | 352 / 488 ms | 未记录 | 未记录 | 未记录 | 172 ms | 98% |

历史延迟表不能替代当前 native 数据。当前 Timer 是**批阶段**分位数，不是每行事件延迟；生产告警
应结合 `batchLag`、批阶段 p95、source slot/binlog retention 和湖 snapshot 水位线，不把两者混用。

### 测试矩阵

下表只统计当前 `src/test/java` 中仍存在的测试类；`target` 里已删除的旧 Debezium 测试报告不计入。

| 数据库/层次 | 测试类 | 测试数 | Docker 依赖 | 最近结果 | 最近耗时 | 覆盖重点 |
|---|---|---:|---|---:|---:|---|
| 共同/DDL | `DdlApplierTest` | 13 | 否 | 13/13 | 14.43 s | CREATE/ALTER/RENAME/DROP、列注释、主键变化、numeric domain 数组精度与多维度 |
| 共同/catalog | `CatalogJdbcSessionTest` | 2 | 否 | 2/2 | 2.67 s | 连接/PreparedStatement 复用、SQLState 08 单次重连、资源关闭、close 后禁止重连 |
| 共同/指标 | `SyncStateTest` | 1 | 否 | 1/1 | 1.97 s | 固定 2×10 Timer 基数、阶段耗时、时钟偏差钳制和失败计数 |
| PostgreSQL 协议单元 | `RawPgReaderTest` | 1 | 否 | 1/1 | 1.53 s | PG 2000 epoch 微秒到 Unix 毫秒的精确换算 |
| PostgreSQL 原生 | `RawPgIntegrationTest` | 12 | PostgreSQL 18 | 12/12 | 49.55 s | CRUD、批量/同 key unchanged TOAST、主键变化、类型矩阵、DDL、schema、无主键、NULL |
| PostgreSQL 完整应用 | `DucklakeApplicationIntegrationTest` | 13 | PostgreSQL 18 | 13/13 | 63.34 s | scanner 首灌、CRUD、DDL/重命名、维护、watermark、主键补加、数组/interval、TRUNCATE/DROP |
| PostgreSQL 吞吐 | `RawPgThroughputTest` | 5 | PostgreSQL 18 | 5/5 | 47.52 s | 100k backlog/stream/warm U、50k warm D、4×25k、真实源时间和阶段 Timer |
| MySQL 原生语义 | `MySqlIntegrationTest` | 17 | MySQL 8.4 + catalog PG 18 | 17/17 | 86.67 s | 快照、类型/ENUM/SET、CRUD、DDL、rotate、主键重建/更新、offset 回退与过滤事务 |
| MySQL 完整链吞吐 | `MySqlThroughputTest` | 5 | MySQL 8.4 + catalog PG 18 | 5/5 | 76.26 s | 50k I/U、25k D、四表、500×10 backlog、真实源时间和阶段 Timer |
| MySQL 协议上限 | `RawMySqlThroughputTest` | 2 | MySQL 8.4 | 2/2 | 26.63 s | 200k 纯 binlog 解码、解码到 DuckDB staging |

当前源码合计 71/71 通过，suite 合计 370.549 s，完整 `clean test` 墙钟 514.1 s。表中耗时包含
Testcontainers 和 Spring 上下文启动，不是 CDC 延迟；最终全量验证使用 `clean test`，避免
`target/test-classes` 中已删除类的增量构建残留被 Surefire 误计。

### 运行命令

单元测试不需要 Docker：

```powershell
.\mvnw.cmd -q '-Dtest=DdlApplierTest,CatalogJdbcSessionTest,SyncStateTest,RawPgReaderTest' test
```

最终全量验证（需要 Docker）：

```powershell
.\mvnw.cmd -q clean test
```

PG 与 MySQL 语义回归需要 Docker：

```powershell
.\mvnw.cmd -q '-Dtest=RawPgIntegrationTest,DucklakeApplicationIntegrationTest' test
.\mvnw.cmd -q '-Dtest=MySqlIntegrationTest' test
```

吞吐基准：

```powershell
.\mvnw.cmd -q '-Dtest=RawPgThroughputTest' test
.\mvnw.cmd -q '-Dtest=MySqlThroughputTest' '-Dmysql.bench.rows=50000' test
.\mvnw.cmd -q '-Dtest=RawMySqlThroughputTest' '-Dmysql.raw.rows=200000' test
```

`pg.bench.rows` / `mysql.bench.rows` 可放大到 1M/10M；默认每个源事务最多 100k/50k 行，可用
`pg.bench.transaction-rows` / `mysql.bench.transaction-rows` 调整，避免测试自身制造不可控单事务内存峰值。
吞吐日志会打印 source、drain、总耗时、湖批次数、stage/lake transaction，并在末尾打印阶段
p50/p95；PowerShell 中建议将 `-Dtest=...` 和带逗号/数字的属性整体加引号。

### 后续性能工作

#### 本轮已经落地

| 项目 | 实现与证据 |
|---|---|
| 分段计时与可靠基准 | PG/MySQL 都记录 10 个固定阶段的 p50/p95；backlog 先停 reader 造数，避免提前消费或 source TPS 污染 drain；真实源提交时间进入 deliver/batch lag |
| warm I/U/D 与 JFR | PG 100k I/U + 50k D、MySQL 50k I/U + 25k D 已实跑；两端 JFR 已归因 CPU、allocation 与 GC，未发现值得继续的对象级微优化 |
| 元数据与 offset 固定成本 | MySQL TableMap 按完整物理 shape 缓存，DDL 失效、rotate 只清 table-id；PG/MySQL catalog 会话和 PreparedStatement 生命周期复用，SQLState `08` 只重连一次 |
| 批内分配 | PG/MySQL 刷批不再复制 pending list；MySQL 大事务交换缓冲；PG TOAST mask 按需分配，heap `ByteBuffer` 直接 UTF-8 解码 |
| PG PATCH | 相同 Relation/mask、主键不变且 key 不重复的 unchanged-TOAST 更新共用 staging + `UPDATE ... FROM`；同 key 和主键变化仍是顺序屏障 |
| 正确性边界 | 71/71 覆盖 offset 回退、DDL、rotate、ENUM/SET、numeric 嵌套数组、批量/连续 TOAST、主键变化和源时间换算 |

这些改动减少了确定的连接、SQL 和分配次数，但 50k MySQL bulk 对照没有显示吞吐提升。PG PATCH
已证明语义与 staging 数收敛，尚未做独立 before/after 吞吐 A/B。不能把结构简化直接写成性能收益。

#### 仍缺的负载与指标

| 优先级 | 工作 | 要回答的问题 |
|---|---|---|
| P0 | 1M/10M warm 矩阵 | 继续跑 append、80/15/5、纯 UPDATE/DELETE、小事务与 3/30 列、6 KiB 行宽；当前只证明 50k/100k 窄表 |
| P1 | 批计划收敛 | 构造事务内交错表 workload；仅在 plan/segment 数显著时，才按 DDL/TRUNCATE/PATCH barrier epoch 聚合普通 DML |
| P1 | 镜像 SQL A/B | 比较当前 `DELETE + QUALIFY INSERT` 与 tombstone DELETE + live MERGE 的吞吐、target scan、文件数和 catalog 增量 |
| P2 | staging 生命周期 | 当前 staging DDL/cleanup p95 均低于 2.5 ms，暂不引入可复用 TEMP 状态；大矩阵证据变化后再 A/B |
| P2 | 自适应合批和内存边界 | 同时限制行数、估算字节和等待时间；先加超大事务 bytes 告警，再根据证据决定是否本地 spill |
| P2 | 维护让路与长期布局 | 以 source lag/最近写入活跃度控制压实，测 data inlining、排序、小文件和查询退化 |
| P3 | 共用 sink | DML 方案稳定后再提取 PG/MySQL 共用 table shape、batch plan 和 mirror writer，避免提前抽象 |

DuckLake 1.5 支持 [MERGE](https://ducklake.select/docs/stable/duckdb/usage/upserting.html)，但一个
`MERGE` 目前不能同时配置多个条件化的 matched UPDATE/DELETE 分支；本项目仍需 tombstone DELETE
和 live-row MERGE 两步。DuckLake [UPDATE 的规范语义](https://ducklake.select/docs/stable/specification/queries.html)
本身也是 delete + insert，因此该实验主要可能减少新 key 的无效 DELETE 和重复目标处理，不会消除
湖格式更新的物理成本。

#### 解码后是否需要消息队列

要区分“采用 Kafka 架构”和“把 Kafka 当作当前吞吐补丁”。WAL/binlog 是源事实与短期恢复日志，
Kafka 是独立保留、重放和分发层，两者不能简单等同。若需要承受长时间 sink 停机、避免 PG slot
拖住 WAL、扇出多个消费者或独立回放，传统 Kafka 路线是合理选择；若目标只是提高当前单 sink、
单写者 DuckLake 的稳态吞吐，引入 Kafka 不会自动突破最慢阶段，应该先完成分段基准和写入拆分。

完成前述单引擎热路径优化后，如果 profiler 仍证明 decode/plan 与 ordered sink 有足够的可重叠时间，
唯一值得继续验证的本地队列是**有界的进程内事务队列**。它只用于让网络解码和湖写入重叠，
不承担持久化日志职责，也不是当前默认下一步：

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

#### 接下来执行顺序

1. **放大已固定的基准**：跑 1M/10M、30 列、6 KiB 和 80/15/5，保留 Timer/JFR 与堆峰值。
2. **镜像 SQL A/B**：优先验证当前主要可控耗时；只有 target scan、文件和 catalog 数据同时更好才替换。
3. **按证据收敛批计划**：先构造事务内交错表；segment 数和 plan p95 明显时才做 barrier epoch 聚合。
4. **再做字节边界与维护整形**：以 p95、堆峰值和 source lag 选择阈值，不拆源事务原子性。
5. **队列最后验证**：只有更大矩阵证明 decode/plan 与单 ordered sink 可稳定重叠时才加有界事务队列。

多槽、Kafka、单 JVM 多 reader 和多 lake writer 暂不进入这条主线。单引擎的 warm I/U/D 吞吐、
分配率和 p95 达到目标后，再用新的分层数据决定是否需要横向扩展。

每一步都必须重跑 offset 回退、主键变更、DDL 屏障、numeric 数组精度和 17 项 MySQL 语义回归；
吞吐提升不能以放宽事务原子性或 offset 恢复正确性为代价。

## 运维与可观测性

- `GET /api/ducklake/watermark` 返回 reader 状态、最近源提交时刻与最近落湖时刻。
- `/actuator/prometheus` 暴露 `ducklake_engine_running`、`ducklake_events_total`、
  `ducklake_batches_total`、`ducklake_batch_failures_total`、`ducklake_last_deliver_lag_ms`、
  `ducklake_last_batch_lag_ms` 和带 `reader/stage` 标签的 `ducklake_batch_stage_duration_seconds`。
- reader 致命断链时进程退出，由容器从最近已提交 offset 重启。
- 应用镜像默认以固定 UID/GID `10001` 运行；Compose 用 named volume 持久化其 DuckDB 扩展缓存。
- `LakeMaintenanceJobs` 与写入共用单写者锁，执行内联 flush、分层/每日合并、快照过期与安全清理。
- 孤儿文件清理始终只做 `dry_run`，不自动删除。
- `lake.data-inlining-row-limit=0` 会明确禁用 Data Inlining。DuckLake 1.5.4 在 DROP/替换表后可能遗留
  `ducklake_inlined_%` side table，见 [#1237](https://github.com/duckdb/ducklake/issues/1237) 与
  [#1316](https://github.com/duckdb/ducklake/pull/1316)；需要规避时设为 0，并等待上游受支持的清理路径。

当前限制：单进程单写者；MySQL 仅支持 8.0+ 的未压缩 ROW binlog；PG DDL 需要 event trigger；
无主键表仅 insert-only；MySQL scanner 默认只 attach `source.dbname`，跨库表可流式同步但无法自动重灌存量。

### 生产发布门槛

当前 correctness、offset 回退、reader 重启和 50k/100k 本地湖基准已通过，可作为目标环境候选版；
尚不能据此宣称对所有生产负载就绪。首次生产发布前至少要在目标 S3/catalog 与真实表型上完成：

1. 1M/10M、宽行和超大源事务的堆峰值/恢复测试，并确定容器内存与 binlog/WAL retention。
2. 24 小时以上稳态与维护并发 soak，验证 p95、文件数、snapshot 增长和查询退化。
3. PG/MySQL 故障转移、catalog/S3 短断、进程在湖提交与 offset 提交窗口被杀的恢复演练。
4. Prometheus 告警、凭据注入、备份恢复和无主键/跨库 scanner 限制的上线验收。
