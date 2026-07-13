# debezium-server-ducklake

PostgreSQL → [DuckLake](https://ducklake.select/) 的流式 CDC 入湖器。**单进程、零 Kafka**：Debezium Embedded Engine 解码逻辑复制流，内嵌 DuckDB 直写湖仓——catalog 存 PostgreSQL、数据落 S3 Parquet。

```
┌─────────────┐  pgoutput   ┌──────────────────────────────────┐      ┌─────────────────┐
│ PostgreSQL  │────────────▶│  debezium-server-ducklake (JVM)  │─────▶│  DuckLake 湖仓   │
│ (业务库)     │  逻辑复制槽  │  Debezium Embedded ──▶ 内嵌DuckDB │      │ catalog: PG 库   │
│ wal_level=  │             │  批消费·两阶段写入·DDL 跟随·维护   │      │ data: S3 Parquet │
│  logical    │             └──────────────────────────────────┘      └─────────────────┘
└─────────────┘                     ▲ /watermark · /actuator/prometheus
```

## 特性

- **零中间件**：不需要 Kafka / Kafka Connect / Quarkus Debezium Server，一个 Spring Boot 进程搞定采集与写入
- **当前态镜像**：湖表 = 主库当前态，列一一对应、无任何元数据列——UPDATE 就地更新、DELETE 物理跟随（批量 upsert/delete，merge-on-read），查询直接 `SELECT` 即所见即主库，无需窗口函数去重
- **默认整库同步**：`FOR ALL TABLES` publication + 不限 schema，存量 initial 快照 + WAL 增量都拉，湖表 `<schema>_<表>` 自动一一对应，新建表自动纳入
- **高吞吐写入路径**：DuckDB Appender 两阶段写（内存 staging 全 VARCHAR 物化 → 单湖事务按主键批量 DELETE + `INSERT SELECT` 向量化 CAST），实测行成本比 prepared-batch 低两个数量级
- **自适应双模式**：小批（默认 ≤512 行）走 DuckLake Data Inlining 直写 catalog 元空间（最低延迟、零小文件）；大批自动走 Parquet 向量化直写——按批粒度自动切换，无需干预
- **DDL 跟随**：源库 `RENAME COLUMN` / `DROP COLUMN` / `DROP TABLE` 经 event trigger 审计流同步到湖表；新表建表/加列由事件 schema 驱动，两者幂等互补
- **类型严格跟随**：源库 `ALTER COLUMN TYPE` 后湖列逐级对齐（就地 ALTER → 整表 CAST 重写 → 删表重建 + 增量快照重灌兜底）
- **时间旅行**：DuckLake snapshot 独立提供（默认保留 30 天）——湖是当前态镜像，但保留期内任意时刻可 `AT (TIMESTAMP ...)` 回看，误删有后悔窗口
- **可靠性**：at-least-once 语义——湖事务提交成功后才推进 offset，崩溃/断电重启自动从上个 offset 重放；镜像 upsert 天然幂等（重放同批=同样结果）
- **空闲 WAL 防护**：心跳 action query 闭环，防止 publication 空闲时复制槽无限扣留实例级 WAL
- **湖内维护**：分层压实（防写放大的关键设计）、快照过期清理、inlined 数据定时落盘，全部进程内调度
- **可观测**：`/watermark` 水位线接口（湖侧权威提交时刻 + 源事件时刻 + 分段延迟）、Prometheus 指标（`ducklake_*` 系列）

## 快速开始

### 0. 环境要求

- JDK 25+ / Maven 3.9+
- PostgreSQL 14+（`wal_level=logical`），推荐 PG 16+
- S3 兼容对象存储（MinIO / rustfs / AWS S3...）；本地体验也可用文件系统路径

### 1. 一键体验（Docker Compose）

```bash
mvn package -DskipTests
docker compose -f docker/docker-compose.yml up -d --build
```

栈内容：源库 PG（Debian bookworm + [Pigsty pig](https://pigsty.io/docs/pig/) 扩展仓库，
加插件一行 `pig ext install <name> -v 18 -y`）+ **独立元空间 PG**（catalog-pg，湖元数据
高频小事务与源库隔离）+ rustfs(S3) + 本服务。CDC 全套基建（角色/publication/DDL 审计/
signal/心跳表）由 initdb 自动完成——**up 即可用，无需手动初始化**。

镜像统一约定：**全部 Debian 基础镜像**（本服务跑 JetBrains Runtime 25，rustfs 用官方
gnu 二进制自建，均非 alpine），构建期换阿里云 APT 源，内置排障工具
（`procps`/`iproute2`/`less`/`jq`）。目录布局——`docker-compose.yml` 统一编排，
每服务一个子目录放各自 `Dockerfile`，持久化数据落各自 `<服务>/data/`（bind mount，
备份/清理一个目录搞定）：

```
docker/
├── docker-compose.yml     # 统一编排
├── e2e-verify.sh          # 部署后端到端冒烟(建表→落湖→DDL 跟随→心跳,PASS/FAIL 汇总)
├── ducklake/              # 本服务:Debian 13 + JBR 25(Dockerfile + data/duckdb-ext 扩展缓存)
├── postgres/              # 源库:postgres:18-bookworm + pig(Dockerfile + initdb/ + data/)
├── catalog-pg/            # 元空间:复用 postgres 镜像(仅 data/)
└── rustfs/                # S3:Debian 13 + rustfs gnu 二进制 + mc(Dockerfile + data/)
```

写点数据看它流进湖：

```sql
CREATE TABLE demo (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, name text);
ALTER TABLE demo REPLICA IDENTITY FULL;   -- UPDATE/DELETE 事件携带整行
INSERT INTO demo (name) VALUES ('hello'), ('ducklake');
```

```bash
curl http://127.0.0.1:19992/api/ducklake/watermark
```

### 2. 本机开发运行（接入已有 PG）

用 `docs/init-source-db.sql` 初始化你的源库（catalog 推荐独立实例，见脚本 ③ 段注释），
改 `src/main/resources/dev/ducklake.yml` 指向你的 PG 与 S3，然后：

```bash
mvn spring-boot:run
```

## 数据语义（重要）

湖表是**主库当前态镜像**：列与源表一一对应（无 `__*` 元数据列），UPDATE 就地更新、DELETE 物理跟随、`DROP TABLE` 跟随删湖表。查询当前态就是普通 `SELECT`：

```sql
SELECT * FROM lake.cdc.public_demo;   -- 所见即主库当前态(滞后=复制延迟,毫秒-秒级)
```

历史版本由 DuckLake snapshot 承担（默认保留 30 天，`maintenance.snapshot-retain-days` 可调）：

```sql
SELECT * FROM lake.cdc.public_demo AT (TIMESTAMP '2026-07-12 08:00:00');  -- 时间旅行回看
```

两个语义边界：
- **无主键表降级 insert-only**：Debezium 事件无 key 时湖侧无法定位行，UPDATE/DELETE 不跟随（PG 逻辑复制本就要求有主键才有完整语义）
- **源 `TRUNCATE` 暂不跟随**（Debezium 默认跳过 truncate 事件且 unwrap SMT 会丢弃它）——需要清空请用 `DELETE FROM`，或源 TRUNCATE 后到湖侧手动同步

### 只读查询湖

任意 DuckDB 客户端（CLI / DataGrip / Python）直连只读 ATTACH，不经过本服务、不影响写入：

```sql
INSTALL ducklake; LOAD ducklake; INSTALL httpfs; LOAD httpfs;
CREATE SECRET rfs (TYPE s3, KEY_ID 'admin', SECRET '...', ENDPOINT 'host:9000', URL_STYLE 'path', USE_SSL false);
ATTACH 'ducklake:postgres:dbname=ducklake_catalog host=... user=... password=...' AS lake (READ_ONLY);
SELECT count(*) FROM lake.cdc.public_demo;
```

建议为分析建独立的只读 PG 角色与 S3 只读凭据。

## 配置参考

配置前缀 `ducklake`，按 profile 提供（`dev`/`prod`）。核心参数（完整见 `DucklakeProperties.java` 的注释，每个默认值都有实测依据）：

| 参数 | 默认 | 说明 |
|---|---|---|
| `source.hostname/port/user/password/dbname` | — | PG 逻辑复制源（集群形态可填 HAProxy 读写口，故障转移自动跟随） |
| `source.slot-name` | `dbz_ducklake` | 复制槽名 |
| `source.publication-name` | `dbz_publication` | 发布名（init 脚本建 `FOR ALL TABLES`：整库所有 schema、新建表自动纳入） |
| `source.schema-include-list` | 空 | **默认整库同步**：空=全部用户 schema（存量 initial 快照 + WAL 增量都拉，湖表 `<schema>_<表>` 自动一一对应）；需收窄填逗号分隔列表 |
| `source.table-exclude-list` | 空 | 排除表（正则，`schema.table` 形式） |
| `source.signal-table` | `public.dbz_signal` | 增量快照 signal 表（类型重建兜底经它触发） |
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
- JVM 参数照 `docker/ducklake/Dockerfile`：ZGC + 紧凑对象头 + `--enable-native-access=ALL-UNNAMED`（DuckDB JNI）
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
| 空闲期 `pg_wal` 稳步增长 | 零流量时连接器无事件可确认、slot 冻结扣留实例级 WAL。配 `heartbeat-action-query`（心跳表 UPSERT）闭环；只配 interval 不够 |
| OOM crash loop | `lake.memory-limit` 与容器限额不匹配，或 threads 调大没给内存。按内存预算公式配 |
| 湖里查不到刚写的数据 | 小批走了 Data Inlining 在 catalog 元空间，属正常——查询照常可见；物理落盘由 quick 任务定时 flush |
| 崩溃重启后短暂"回退" | offset flush 间隔（默认 10s)内的批被按序重放（at-least-once），重放期间中间态短暂回退、追平后与主库一致；镜像 upsert 幂等，不会产生重复行 |
| 无主键表 UPDATE/DELETE 没跟随 | 设计降级：事件无 key 无法定位湖行，insert-only（日志有每表一次的 WARN）。给源表加主键即恢复完整镜像 |
| 源 TRUNCATE 后湖没清空 | TRUNCATE 事件暂不跟随（见数据语义节）；用 `DELETE FROM` 替代或湖侧手动清 |

## 性能参考

8C16G 单机 Docker Compose 全家桶实测（源 PG、元空间 PG、rustfs、本服务**同机**——即本仓一键体验栈原样；`docker/bench.sh` 四阶段与 `docker/bench-matrix.sh` 参数矩阵可在你的环境一键复现）：

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

- **单表吞吐上限 ≈ 单槽解码能力**（pgoutput 串行解码是架构固有），需要更高聚合吞吐时可按表分片多实例（每实例独立 slot/publication），但注意每个 walsender 都全量解码 WAL 的 N× 读放大
- **镜像是当前态不是备份**：主库误删会忠实同步到湖（时间旅行窗口内可 `AT (TIMESTAMP ...)` 找回，过期即不可）；备份职责在 PG 侧（basebackup/WAL 归档）
- 无主键表降级 insert-only（UPDATE/DELETE 不跟随）；源 `TRUNCATE` 暂不跟随
- UPDATE/DELETE 密集负载会产生较多 delete file（merge-on-read 读放大），由分层压实定期吸收
- `numeric` 的 NaN/Infinity 值无法通过 Debezium 的 BigDecimal 转换（上游限制）

## License

[Apache-2.0](LICENSE)
