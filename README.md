# debezium-server-ducklake

PostgreSQL / MySQL → [DuckLake](https://ducklake.select/) 的流式 CDC 入湖器。**单进程、零 Kafka**：Debezium Embedded Engine 解码复制流（PG 逻辑复制 / MySQL binlog），内嵌 DuckDB 直写湖仓——catalog 存 PostgreSQL、数据落 S3 Parquet。

```
┌─────────────┐  pgoutput / binlog  ┌──────────────────────────────────┐      ┌─────────────────┐
│ PostgreSQL  │────────────────────▶│  debezium-server-ducklake (JVM)  │─────▶│  DuckLake 湖仓   │
│  或 MySQL   │  逻辑复制槽/ROW 流   │  Debezium Embedded ──▶ 内嵌DuckDB │      │ catalog: PG 库   │
│  (业务库)   │                     │  批消费·两阶段写入·DDL 跟随·维护   │      │ data: S3 Parquet │
└─────────────┘                     └──────────────────────────────────┘      └─────────────────┘
                                            ▲ /watermark · /actuator/prometheus
```

## 特性

- **零中间件**：不需要 Kafka / Kafka Connect / Quarkus Debezium Server，一个 Spring Boot 进程搞定采集与写入
- **双源支持**：`source.type=postgres|mysql` 一键切换——PG 走逻辑复制（slot+publication），MySQL 8.0+ 走 binlog（ROW/FULL 为 8.0 出厂默认，源库近乎零配置；schema history 与 offset 同存 catalog PG，容器无状态）
- **当前态镜像**：湖表 = 主库当前态，列一一对应、无任何元数据列——UPDATE 就地更新、DELETE 物理跟随（批量 upsert/delete，merge-on-read），查询直接 `SELECT` 即所见即主库，无需窗口函数去重
- **默认整库同步**：`FOR ALL TABLES` publication + 不限 schema，存量 initial 快照 + WAL 增量都拉，湖 schema 直接镜像 PG schema（`lake.<schema>.<表>`，可配前缀），新建表自动纳入
- **高吞吐写入路径**：DuckDB Appender 两阶段写（内存 staging 全 VARCHAR 物化 → 单湖事务按主键批量 DELETE + `INSERT SELECT` 向量化 CAST），实测行成本比 prepared-batch 低两个数量级
- **自适应双模式**：小批（默认 ≤512 行）走 DuckLake Data Inlining 直写 catalog 元空间（最低延迟、零小文件）；大批自动走 Parquet 向量化直写——按批粒度自动切换，无需干预
- **DDL 跟随**：源库 `RENAME COLUMN` / `DROP COLUMN` / `DROP TABLE` / **表与列的 `COMMENT` 注释**同步到湖表——PG 经 event trigger 审计流，MySQL 直接消费 binlog 原生 schema change 事件（**源库零审计基建**，另支持 `RENAME TABLE` 与 `TRUNCATE` 跟随）；新表建表/加列由事件 schema 驱动，两者幂等互补
- **类型严格跟随**：源库 `ALTER COLUMN TYPE` 后湖列逐级对齐（就地 ALTER → 整表 CAST 重写 → 删表重建 + 增量快照重灌兜底）
- **时间旅行**：DuckLake snapshot 独立提供（默认保留 30 天）——湖是当前态镜像，但保留期内任意时刻可 `AT (TIMESTAMP ...)` 回看，误删有后悔窗口
- **可靠性**：at-least-once 语义——湖事务提交成功后才推进 offset，崩溃/断电重启自动从上个 offset 重放；镜像 upsert 天然幂等（重放同批=同样结果）
- **空闲 WAL 防护**：心跳 action query 闭环，防止 publication 空闲时复制槽无限扣留实例级 WAL
- **湖内维护**：分层压实（防写放大的关键设计）、快照过期清理、inlined 数据定时落盘，全部进程内调度
- **可观测**：`/watermark` 水位线接口（湖侧权威提交时刻 + 源事件时刻 + 分段延迟）、Prometheus 指标（`ducklake_*` 系列）

## 快速开始

### 0. 环境要求

- JDK 25+ / Maven 3.9+
- 源库二选一：PostgreSQL 14+（`wal_level=logical`，推荐 PG 16+）或 **MySQL 8.0+**（含 8.4 LTS / 9.x，不支持 5.7；binlog ROW/FULL 为出厂默认）
- S3 兼容对象存储（MinIO / rustfs / AWS S3...）；本地体验也可用文件系统路径

### 1. 一键体验（Docker Compose）

两套源栈独立自包含（`.docker/postgres/` 与 `.docker/mysql/`，project/端口/数据目录全错开，可并存同机），按源库类型选一套：

```bash
mvn package -DskipTests
docker compose -f .docker/postgres/docker-compose.yml up -d --build   # PG 源栈
```

PG 栈内容：源库 PG（Debian bookworm + [Pigsty pig](https://pigsty.io/docs/pig/) 扩展仓库，
加插件一行 `pig ext install <name> -v 18 -y`）+ **独立元空间 PG**（catalog-pg，湖元数据
高频小事务与源库隔离）+ rustfs(S3) + 本服务。CDC 全套基建（角色/publication/DDL 审计/
signal/心跳表）由 initdb 自动完成——**up 即可用，无需手动初始化**。

镜像统一约定：**全部 Debian 基础镜像**（本服务跑 JetBrains Runtime 25，rustfs 用官方
gnu 二进制自建，MySQL 用 bookworm-slim + 官方 APT 仓库自建，均非 alpine/Oracle Linux），
构建期换国内源，内置排障工具（`procps`/`iproute2`/`less`/`jq`）。目录布局——每套栈
一个 `docker-compose.yml`，每服务一个子目录放各自 `Dockerfile`，持久化数据落各自
`<服务>/data/`（bind mount，备份/清理一个目录搞定）：

```
.docker/
├── postgres/                 # PG 源完整栈(project: ducklake-pg)
│   ├── docker-compose.yml
│   ├── e2e-verify.sh         # 端到端冒烟(建表→落湖→DDL 跟随→心跳,PASS/FAIL 汇总)
│   ├── bench.sh 等           # 性能基线四阶段 + 参数矩阵(README 性能节口径)
│   ├── postgres/             # 源库:postgres:18-bookworm + pig(Dockerfile + initdb/ + data/)
│   ├── catalog-pg/           # 元空间:复用源库镜像(仅 data/)
│   ├── rustfs/               # S3:Debian 13 + rustfs gnu 二进制 + mc(Dockerfile + data/)
│   └── ducklake/             # 本服务:Debian 13 + JBR 25(Dockerfile + data/duckdb-ext)
└── mysql/                    # MySQL 源完整栈(project: ducklake-mysql)
    ├── docker-compose.yml
    ├── e2e-verify.sh         # 冒烟(另含 RENAME COLUMN/TRUNCATE 跟随断言)
    ├── mysql/                # 源库:自建 Debian 镜像(bookworm-slim + MySQL 8.4 APT 清华源,
    │                         #   Dockerfile + entrypoint.sh + conf.d/ + initdb/ + data/)
    ├── catalog-pg/           # 元空间:官方 postgres:18-bookworm(仅 data/)
    ├── rustfs/               # 同 PG 栈(独立实例与数据)
    └── ducklake/             # 同 PG 栈
```

写点数据看它流进湖：

```sql
CREATE TABLE demo (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, name text);  -- 主键必须有
INSERT INTO demo (name) VALUES ('hello'), ('ducklake');
```

```bash
curl http://127.0.0.1:19992/api/ducklake/watermark
```

**MySQL 源栈**（MySQL 8.4 + 元空间 PG + rustfs + 本服务）：

```bash
mvn package -DskipTests
docker compose -f .docker/mysql/docker-compose.yml up -d --build
# 写数据(业务库 shop 由 initdb 建好;CDC 账号/signal/心跳表同样自动就绪):
docker compose -f .docker/mysql/docker-compose.yml exec mysql \
  mysql -uroot -pchangeme shop -e "CREATE TABLE demo(id bigint AUTO_INCREMENT PRIMARY KEY, name varchar(64)); INSERT INTO demo(name) VALUES ('hello'),('mysql');"
curl http://127.0.0.1:19993/api/ducklake/watermark    # 湖表 lake.shop.demo
(cd .docker/mysql && bash e2e-verify.sh)              # 端到端冒烟
```

### 2. 本机开发运行（接入已有源库）

PG 源用 `docs/init-source-db.sql`、MySQL 源用 `docs/init-source-mysql.sql` 初始化
（catalog 推荐独立 PG 实例，见脚本注释），改 `src/main/resources/dev/ducklake.yml`
指向你的源库与 S3（MySQL 另设 `source.type: mysql` + `port: 3306`），然后：

```bash
mvn spring-boot:run
```

## 数据语义（重要）

湖表是**主库当前态镜像**：列与源表一一对应（无 `__*` 元数据列），UPDATE 就地更新、DELETE 物理跟随、`DROP TABLE` 跟随删湖表。查询当前态就是普通 `SELECT`：

```sql
SELECT * FROM lake.public.demo;   -- 所见即主库当前态(滞后=复制延迟,毫秒-秒级)
```

历史版本由 DuckLake snapshot 承担（默认保留 30 天，`maintenance.snapshot-retain-days` 可调）：

```sql
SELECT * FROM lake.public.demo AT (TIMESTAMP '2026-07-12 08:00:00');  -- 时间旅行回看
```

表/列的 `COMMENT ON` 注释也自动跟随到湖（增量跟随：改造部署后新执行的 COMMENT 会同步；存量注释可重新执行一遍 COMMENT 语句补齐）。

两个语义边界：
- **无主键表降级 insert-only**：Debezium 事件无 key 时湖侧无法定位行，UPDATE/DELETE 不跟随（两源一致；PG 逻辑复制本就要求有主键才有完整语义）
- **源 `TRUNCATE`**：MySQL 源**支持跟随**（binlog 有 op=t 事件，湖表同步清空，`maintenance.follow-truncate` 可关）；PG 源暂不跟随（pgoutput truncate 事件被 Debezium 默认跳过且 unwrap 丢弃）——PG 需要清空请用 `DELETE FROM`

### 只读查询湖

任意 DuckDB 客户端（CLI / DataGrip / Python）直连只读 ATTACH，不经过本服务、不影响写入：

```sql
INSTALL ducklake; LOAD ducklake; INSTALL httpfs; LOAD httpfs;
CREATE SECRET rfs (TYPE s3, KEY_ID 'admin', SECRET '...', ENDPOINT 'host:9000', URL_STYLE 'path', USE_SSL false);
ATTACH 'ducklake:postgres:dbname=ducklake_catalog host=... user=... password=...' AS lake (READ_ONLY);
SELECT count(*) FROM lake.public.demo;
```

建议为分析建独立的只读 PG 角色与 S3 只读凭据。

## 配置参考

配置前缀 `ducklake`，按 profile 提供（`dev`/`prod`）。核心参数（完整见 `DucklakeProperties.java` 的注释，每个默认值都有实测依据）：

| 参数 | 默认 | 说明 |
|---|---|---|
| `source.type` | `postgres` | 源类型：`postgres` / `mysql`（8.0+）——解码机制与 DDL 跟随路线随之切换 |
| `source.hostname/port/user/password/dbname` | — | 源库连接（集群形态可填代理读写口，故障转移自动跟随）。⚠️ port 默认 5432，MySQL 须显式 3306；MySQL 的 dbname=signal/心跳表所在业务库，不限定捕获范围 |
| `source.slot-name` | `dbz_ducklake` | [仅 PG] 复制槽名 |
| `source.publication-name` | `dbz_publication` | [仅 PG] 发布名（init 脚本建 `FOR ALL TABLES`：整库所有 schema、新建表自动纳入） |
| `source.server-id` | `6400` | [仅 MySQL] binlog 客户端 ID，须在 MySQL 集群 server_id/replica 空间内唯一；多实例各配不同值 |
| `source.connection-time-zone` | 空 | [仅 MySQL] 会话时区透传；空=自动查询服务端（默认即可） |
| `source.schema-include-list` | 空 | **默认整库同步**：空=全部用户 schema/库（存量 initial 快照 + 流式增量都拉，湖 schema 一一镜像 PG schema / MySQL database，`lake.<schema|db>.<表>`，可配前缀）；需收窄填逗号分隔列表 |
| `source.table-exclude-list` | 空 | 排除表（正则，`schema.table` / `db.table` 形式） |
| `source.signal-table` | 空 | 增量快照 signal 表（类型重建兜底经它触发）。空=按类型推导：PG `public.dbz_signal` / MySQL `<dbname>.dbz_signal` |
| `maintenance.follow-truncate` | `true` | [仅 MySQL] 源 `TRUNCATE TABLE` 跟随清空湖表（PG 路线暂不支持，见数据语义节） |
| `lake.catalog-*` | — | DuckLake catalog 的 PG 连接（同时承载 Debezium offset 表） |
| `lake.data-path` | `s3://lake/ducklake/` | 数据文件根路径（S3 或本地目录） |
| `lake.memory-limit` | `1536MB` | 内嵌 DuckDB 内存上限。⚠️ 须给足"常驻+批工作集"，过小会 OOM crash loop |
| `lake.threads` | `2` | DuckDB 并行线程数。CDC 小批（<1 个 row group）本就吃不满多核；每线程写出缓冲是内存乘法器，调大前先给容器内存预算 |
| `lake.data-inlining-row-limit` | `512` | Data Inlining 阈值。只对小批有利（大批 inlined 是 PG 行式写、比 Parquet 慢 60×）；0=禁用 |
| `engine.max-batch-size / max-queue-size` | `8192 / 32768` | 连续消费模型：批大小=单批写入耗时内的自然流量，负反馈自稳。加大批实测反而更慢 |
| `engine.poll-interval-ms` | `10` | 空闲唤醒间隔（唯一延迟参数，仅空闲时燃烧） |
| `engine.snapshot-mode` | `initial` | 首启全量快照后转流式 |
| `engine.heartbeat-interval-ms` | `60000` | 心跳间隔（防空闲 WAL 扣留，见运维节） |
| `engine.heartbeat-action-query` | 空 | **强烈建议配置**：零流量时唯一能触发 LSN 确认的机制（prod profile 已带默认 UPSERT 语句） |
| `engine.dry-run` | `false` | 诊断：空转不写湖测纯解码吞吐。⚠️ 生产必须 false（offset 照推进=丢数据） |
| `maintenance.enabled` | `true` | 湖维护调度总开关 |
| `maintenance.schema-prefix` | 空 | 湖 schema 前缀：空=纯镜像（源 `public.demo` → 湖 `lake.public.demo`）；多个源库实例共享同一湖时各配前缀隔离（如 `erp_` → `lake.erp_public.demo`；仅小写字母/数字/下划线） |
| `maintenance.follow-drop-table` | `true` | 源 DROP TABLE/DROP SCHEMA 跟随真删湖表（false=湖表保留） |
| `maintenance.follow-drop-column` | `true` | DDL 删列跟随真删（false=湖保留历史列） |
| `maintenance.follow-type-change` | `true` | 类型严格跟随三级策略 |
| `maintenance.snapshot-retain-days` | `30` | 快照保留窗口（time travel），过期物理清理 |

## 部署

### 预构建产物

每次推送 `v*` tag 自动发布（`.github/workflows/release.yml`）：
- **jar**：GitHub Releases 附件 `debezium-server-ducklake-<版本>.jar`
- **镜像**（多架构 amd64/arm64）：

```bash
docker pull ghcr.io/kenconnet666/debezium-server-ducklake:latest
```

维护者发布：`git tag v0.1.0 && git push origin v0.1.0` 即可；
首次发布后需在 GitHub Packages 设置里把包改为 public 才能匿名拉取。

### 内存预算（实测公式）

```
容器限额 ≈ JVM 堆(512m) + JVM 本体(~300m) + lake.memory-limit(1536m) + 余量 ≈ 3G
```

DuckDB 的 Parquet writer 按 row group 容量**预分配**列缓冲（与实际批行数无关）——本项目已内置 `parquet_row_group_size=16384` 与 `per_thread_output=false` 两个防 OOM 关键参数（宽行表实测三档内存全炸的修复项），无需手工设置。

### 生产建议

- 用 `prod` profile（全环境变量注入，变量清单见 `src/main/resources/prod/ducklake.yml` 与 compose 示例）
- **单实例部署**：本服务是湖的唯一写入者（单写者设计规避 DuckLake 并发提交缺陷）；容器 `restart: always`——任何致命错误进程自杀交给容器重启，从上个 offset 重放，零丢失
- JVM 参数照 `.docker/postgres/ducklake/Dockerfile`：ZGC + 紧凑对象头 + `--enable-native-access=ALL-UNNAMED`（DuckDB JNI）
- DuckDB 扩展缓存目录 `/root/.duckdb` 挂持久卷（否则每次重启重新下载扩展）

## 运维

### 维护任务（进程内调度，全部持写锁与 CDC 串行）

| 任务 | 频率 | 内容 |
|---|---|---|
| quick | 5 分钟 | `flush_inlined_data`（元空间小批落盘 Parquet）+ Tier0 压实（碎片合并） |
| hourly | 每小时 | Tier1 压实（小文件合并） |
| daily | 每日（默认 04:40，`maintenance.daily-cron` 可调） | **全量归并**（所有文件收敛到大文件）+ 快照过期清理 + 孤儿文件检测（dry-run 只报不删）+ 信号表 TRUNCATE |

分层压实的 `max_file_size` 过滤是**防写放大的关键**：各级只吃自己区间的输入，每个字节一生只被重写 O(层数) 次。

### 监控与告警

- Prometheus：`/api/ducklake/actuator/prometheus`，指标 `ducklake_last_{source_event_ts,batch_at,deliver_lag,stage,lake_tx,batch_lag}_ms`、`ducklake_{events,batches,batch_failures,ddl_applied}_total`、`ducklake_engine_running`
- 水位线：`GET /api/ducklake/watermark` —— `lastSyncedAt`（DuckLake 最新 snapshot 提交时刻，湖侧权威）/ `lastSourceEventTs`（已反映源库到此刻）/ 分段延迟四元组
- **告警建议**：按 `lastSourceEventTs 距今` 判滞后（计数器是进程内存值、重启归零，别用绝对值）；PG 侧加复制槽保留字节告警（`pg_replication_slots`），防消费者宕机数天磁盘涨
- **不要设 `max_slot_wal_keep_size`**：超限 slot 被 invalidate = 丢数据；对 CDC 宁可磁盘告警人工介入

### 常见坑速查（全部实测）

| 现象 | 原因与处置 |
|---|---|
| 空闲期 `pg_wal` 稳步增长 | [PG] 零流量时连接器无事件可确认、slot 冻结扣留实例级 WAL。配 `heartbeat-action-query`（心跳表 UPSERT）闭环；只配 interval 不够 |
| [MySQL] 重启报 binlog 位点不存在、引擎 crash loop | 消费者停机超过 `binlog_expire_logs_seconds`（默认 30 天）位点被清。临时改 `engine.snapshot-mode=when_needed` 自动重新全量快照后改回；心跳表 action query 可防"长期全闲"触发此边界 |
| [MySQL] 连接报 `Public Key Retrieval is not allowed` | caching_sha2 认证 + 非 TLS 连接的组合。Debezium `database.ssl.mode` 默认 preferred 通常自动走 TLS 规避；顽固场景给源库配 TLS 或加 `driver.allowPublicKeyRetrieval=true` |
| OOM crash loop | `lake.memory-limit` 与容器限额不匹配，或 threads 调大没给内存。按内存预算公式配 |
| 湖里查不到刚写的数据 | 小批走了 Data Inlining 在 catalog 元空间，属正常——查询照常可见；物理落盘由 quick 任务定时 flush |
| 崩溃重启后短暂"回退" | offset flush 间隔（默认 10s)内的批被按序重放（at-least-once），重放期间中间态短暂回退、追平后与主库一致；镜像 upsert 幂等，不会产生重复行 |
| 无主键表 UPDATE/DELETE 没跟随 | 设计降级：事件无 key 无法定位湖行，insert-only（日志有每表一次的 WARN）。给源表加主键即恢复完整镜像 |
| 源库 DELETE 报 `55000: cannot delete from table ... no replica identity` | `FOR ALL TABLES` publication 的 PG 约束：无主键（无 replica identity）的表**连源库自己的 DELETE/UPDATE 都被拒**。修法：`ALTER TABLE <表> ADD COLUMN id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY;`（有主键即可，无需 `REPLICA IDENTITY FULL`——镜像 upsert 只按主键定位，DEFAULT 足够且更省 WAL）|
| 源 TRUNCATE 后湖没清空 | TRUNCATE 事件暂不跟随（见数据语义节）；用 `DELETE FROM` 替代或湖侧手动清 |

## 性能参考

8C16G 单机 Docker Compose 全家桶实测（源 PG、元空间 PG、rustfs、本服务**同机**——即本仓一键体验栈原样；`.docker/postgres/bench.sh` 四阶段与 `bench-matrix.sh` 参数矩阵可在你的环境一键复现）：

### 测试环境硬件基准

测试机是一台**性能偏弱的入门级 KVM 云虚机**（Intel Xeon Cascadelake 8 vCPU / 15GiB / QEMU 虚拟盘），磁盘被宿主 QoS 明显限速——下文所有性能数字应视为**保守下限**，本地 NVMe / 更强单核的环境大概率跑出更好的结果：

| 项目 | 实测 | 判定 |
|---|---:|---|
| CPU 单线程（sysbench cpu） | 907 events/s | 云虚机常规水平；**单核性能直接决定解码吞吐地板**（pgoutput 单线程） |
| CPU 8 线程 | 6,600 events/s（扩展比 7.3×/8） | 无超卖挤兑，8 核是真的 |
| 内存带宽 | 29.5 GiB/s | 非瓶颈 |
| 磁盘 4k 随机读 / 写 | ~2,300 IOPS（~9 MB/s） | **受限云盘**（读写都精确撞 9 MB/s 的 QoS 特征；本地 SSD 应为 5 万+ IOPS） |
| 磁盘 1M 顺序读 / 写 | 89 / 54 MB/s | 同为受限云盘水平 |
| 容器间网络（iperf3） | 19.7 Gbit/s | bridge 软件转发，无瓶颈 |

硬件对结果的影响解读：
- 吞吐上限由 **CPU 单核性能**决定——想提吞吐，换单核更强的机型比加核数有效
- 压测时灌入器与消费者**同机争抢同一块受限盘**，生产形态（源库独立部署）下消费吞吐预期更好
- 消费侧写湖流量仅 ~2-5 MB/s，受限盘也有 10 倍富余；磁盘只在"这台机器同时承载高并发 OLTP 业务库"时才会先于 CPU 成为瓶颈（2,300 随机 IOPS 撑不起高并发事务）
- 内存与网络在此负载形态下大幅过剩（栈满载 ~2.5GiB / 15GiB）

**双环境对照**（同款 sysbench/fio、同容器口径，在一台桌面级机器上复测——量化"更强硬件能好多少"）：

| 项目 | 参考环境 A：入门云虚机（上表，压测基线） | 参考环境 B：桌面级（Ryzen 5 6600H + NVMe） | 差距 |
|---|---:|---:|---|
| CPU 单线程 | 907 events/s | 3,868 events/s | **4.3×** |
| CPU 多线程 | 6,600（8 线程） | 21,612（12 线程） | 3.3× |
| 内存带宽 | 29.5 GiB/s | 47.9 GiB/s | 1.6× |
| 4k 随机读 / 写 | 2,316 / 2,307 IOPS | 53,100 / 28,900 IOPS | **23× / 12×** |
| 1M 顺序读 / 写 | 89 / 54 MB/s | 1,746 / 1,619 MB/s | 20× / 30× |

外推（未实测整栈，按瓶颈模型估算）：解码地板随 CPU 单线程线性走，环境 B 跑同栈的追赶吞吐理论上限 ~90k 行/秒（保守估 2-3× 即 45-70k）；NVMe 的随机 IOPS 余量也让"同机兼任业务库"变得可行。上表云虚机数字作为**保守下限**的定位由此对照实证。

### 吞吐

| 指标 | 实测值 | 口径 |
|---|---:|---|
| 积压追赶吞吐（端到端写湖） | **~22,400 行/秒** | 停消费灌 100 万行再启动，两点法测消费速率，~100B/行；100 万积压 ~45s 追平 |
| 纯解码地板（dry-run 空转） | ~29,000 行/秒 | 不写湖只解码+交付——端到端已达地板的 ~77%，**瓶颈在 pgoutput 单槽解码 + 单机 CPU 总量**，写湖侧只占小头 |
| 追赶期资源峰值 | CPU 614% / 内存 845MiB | 8 核的 77% / 3GiB 容器限额内 |
| 参考：append-only 旧写路径 | ~26,700 行/秒 | 同环境——镜像语义（查询即当前态、免视图免去重）的代价约 **-16% 吞吐**，对分析湖完全值得 |

### 延迟

| 指标 | p50 | p95 | 口径 |
|---|---:|---:|---|
| **batchLag**（事件产生→落湖提交，端到端） | **617ms** | 813ms | ~2k 行/秒持续流 |
| ├ deliverLag（Debezium 解码+交付） | 339ms | 510ms | 同上 |
| ├ stage（staging 物化） | 14ms | 19ms | 同上 |
| └ lakeTx（湖事务：按键 DELETE + INSERT） | 252ms | 304ms | 镜像 upsert 两步的成本所在 |
| 空闲单行端到端 | ~280ms | — | 排空后单行 INSERT 到落湖提交（中位，5 次探针） |

崩溃恢复：kill/重启后从上个 offset 按序重放，镜像 upsert 幂等，源湖当前态精确一致。对典型业务 CDC 增量（几百-几千行/秒）有 1-2 个数量级余量。

### 调参指南（单变量矩阵实测）

吞吐对参数**不敏感**——各组均在基线 ±8% 内（瓶颈在解码侧），**默认参数即最优区**：

| 参数 | 默认 | 矩阵结论 |
|---|---|---|
| `engine.max-batch-size` | 8192 | 32768 约 +8% 吞吐，但批重放粒度 ×4，不值得改默认；超大积压追赶可临时调大 |
| `lake.threads` | 2 | 4 约 +6%（单机全家桶 CPU 已饱和，收益边际）；独立部署且内存充足可试 4 |
| `engine.record-processing-threads` | -1（=核数） | 窄行负载下与单线程持平（SMT 本身轻）；宽行/高流量才是它的设计场景，保持默认 |
| `lake.data-inlining-row-limit` | 512 | 关闭（0）对追赶 +3%（噪声级）；保留 512 换小批低延迟与零小文件 |
| `engine.poll-interval-ms` | 10 | 调 1ms 无感知收益（空闲延迟主导项是单批写入耗时），保持默认 |
| `lake.memory-limit` | 1536MB | 与容器限额联动（预算公式见部署节）；过小 OOM crash loop |

## 与其他方案对比

PostgreSQL CDC → 分析存储的常见选型对照。⚠️ 本方案列为上文同环境实测；其余列为各官方文档/公开资料的**典型配置口径，非同环境实测**，仅供架构选型参考：

| 维度 | 本方案（DuckLake） | Iceberg 链路 | Apache Doris | Cloudberry（GP 系 MPP） |
|---|---|---|---|---|
| 典型链路 | Debezium Embedded + 内嵌 DuckDB，**单进程** | Kafka + Connect/Flink CDC → Iceberg + catalog 服务（Hive/REST）+ 查询引擎 | Flink CDC → Doris（FE+BE 集群） | Kafka/gpss 等 → MPP 集群 |
| 最小部署 | 单机 `docker compose up`（本仓一键栈） | 5+ 组件各自高可用 | 3 节点起（可单机试用） | 多节点 MPP 集群 |
| 数据存储 | S3 Parquet（开放格式，catalog 在 PG） | S3/HDFS Parquet/ORC（开放格式） | 本地盘为主（存算一体；3.x 起有存算分离形态） | 本地盘 MPP 分布式 |
| CDC 端到端延迟 | **亚秒~秒级**（实测 p50 0.6s @2k 行/秒） | 分钟级为主（Flink checkpoint 驱动；激进配置 ~30s，但小文件压力大） | 秒级（典型 1~10s，攒批导入） | 批 ETL 分钟~小时；流式（gpss）秒~分钟 |
| 当前态语义 | 镜像 upsert，查询即当前态 | delete file + 后台 compaction，查询引擎 merge-on-read | Unique Key 模型 merge-on-write | 原生 UPDATE/DELETE |
| 时间旅行 | ✓ snapshot（默认保留 30 天） | ✓ snapshot | ✗（靠备份恢复） | ✗ |
| 查询引擎 | DuckDB（任意客户端只读 ATTACH 直查，不经本服务） | Trino/Spark/Flink/DuckDB…**生态最广** | 自带（MySQL 协议，高并发点查/聚合强） | 自带（PostgreSQL 协议） |
| 吞吐扩展 | 单实例 ~22k 行/秒；按表分片多实例横向扩 | Flink 并行度横向扩，上限最高 | 导入随 BE 横向扩 | 随 MPP 节点横向扩 |
| 运维面 | **最小**：单进程 + 你已有的 PG 与 S3 | 最大：Kafka/Flink/catalog 三套系统各自运维 | 中：集群自管（FE/BE/Compaction） | 大：MPP 集群管理 |
| 适合场景 | 中小规模、要开放湖格式 + 极简运维、PG 技术栈 | 组织级湖仓平台、多引擎共享、超大规模 | 高并发低延迟 OLAP 对外服务 | Greenplum 迁移、PG 系重型数仓 |

一句话：**要一个"接上就有、查询即当前态、随时能被任何 DuckDB 客户端打开"的 PG 分析副本**，选本方案；要组织级多引擎湖仓平台选 Iceberg；要高并发对外 OLAP 服务选 Doris；深度 PG 生态的重型 MPP 数仓选 Cloudberry。

## 局限

- **单表吞吐上限 ≈ 单流解码能力**（pgoutput 单槽 / binlog 单客户端串行解码是架构固有），需要更高聚合吞吐时可按表分片多实例（PG 每实例独立 slot/publication；MySQL 每实例独立 server-id + database.include 分片），但注意每路都全量读复制流的 N× 读放大
- **镜像是当前态不是备份**：主库误删会忠实同步到湖（时间旅行窗口内可 `AT (TIMESTAMP ...)` 找回，过期即不可）；备份职责在源库侧
- 无主键表降级 insert-only（UPDATE/DELETE 不跟随）；PG 源 `TRUNCATE` 暂不跟随（MySQL 源支持）
- UPDATE/DELETE 密集负载会产生较多 delete file（merge-on-read 读放大），由分层压实定期吸收
- `numeric` 的 NaN/Infinity 值无法通过 Debezium 的 BigDecimal 转换（上游限制）
- **MySQL 边界**：`TIME` 合法值域 ±838h 超出湖 TIME(24h)、历史 zero-date（`0000-00-00`）——两者经 TRY_CAST 置 NULL 保数据链不断（引擎侧另有 `event.converting.failure=warn` 一道防线）；空间类型落 BLOB(WKB)（DuckDB spatial 可 `ST_GeomFromWKB` 解析）；`BIGINT UNSIGNED` 走 DECIMAL(38,0) 防溢出；消费者停机超过 `binlog_expire_logs_seconds`（默认 30 天）会丢位点，重启需重新快照（临时 `snapshot.mode=when_needed` 可自动重灌）

## License

[Apache-2.0](LICENSE)
