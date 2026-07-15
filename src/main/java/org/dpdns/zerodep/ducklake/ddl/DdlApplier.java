package org.dpdns.zerodep.ducklake.ddl;

import org.dpdns.zerodep.ducklake.config.DucklakeProperties;
import org.dpdns.zerodep.ducklake.metrics.SyncState;
import org.dpdns.zerodep.ducklake.sink.DuckLakeEngine;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Struct;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * DDL 信号流消费——**纯跟随，不留档**，双前端共用一组湖端执行原语（rename/删列/删表/注释/重建回调）：
 * <ul>
 *   <li><b>PG 前端 {@link #apply}</b>：event trigger（ddl_command_end + sql_drop）写 dbz_ddl_log，
 *       随 publication 被当普通表抓走。判定靠 PG 目录语义（按 xid 分组配对 sql_drop）：
 *       RENAME COLUMN 是 pg_attribute 原地改名<b>不触发 sql_drop</b>，DROP+ADD 必触发——
 *       "ALTER TABLE 且同事务无 sql_drop 且语句含 RENAME COLUMN" ⇒ 真 rename。</li>
 *   <li><b>MySQL 前端 {@link #applySchemaChange}</b>：binlog 自带 DDL，Debezium 原生 schema change
 *       事件（topic=一段式 topic.prefix，经 predicate 免 unwrap 原样到达）。tableChanges 给
 *       "变更后的完整表结构快照"（含全列/主键/注释）而非语义 diff，rename/删列语义从 ddl
 *       原文正则取——源库<b>无需任何审计表/触发器基建</b>。</li>
 * </ul>
 * 其余跟随分工（两前端一致）：加列与类型安全放宽交给数据驱动 ensureTable 幂等处理；删列默认跟随真删
 * （followDropColumn=false 时保留湖列留历史）；DROP TABLE 默认跟随真删湖表（followDropTable=false
 * 时保留；快照重放的历史 DDL 一律跳过——快照给的是当前态，活表不能被历史 DDL 误删）；
 * CREATE TABLE 即刻建湖空表（PG 连源库读列定义 / MySQL 用 tableChanges 全列结构，
 * IF-NOT-EXISTS 幂等；类型映射尽力对齐事件推导口径，偏差由数据驱动 followTypeChange 自愈）；
 * 重复应用靠"列/表存在性检查"天然幂等。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DdlApplier {

    /** RENAME [COLUMN] old TO new（PG 允许省略 COLUMN 关键字；双引号/反引号/裸名通用）。
     *  负前瞻排除 "RENAME TO"（表改名，另路处理）与 RENAME INDEX/KEY */
    private static final Pattern RENAME_COLUMN = Pattern.compile(
            "RENAME\\s+(?:COLUMN\\s+)?(?!TO\\b|INDEX\\b|KEY\\b)[\"`]?([\\w$]+)[\"`]?\\s+TO\\s+[\"`]?([\\w$]+)[\"`]?",
            Pattern.CASE_INSENSITIVE);

    /** [PG] 表改名：ALTER TABLE [IF EXISTS] [ONLY] <旧名> RENAME TO <新名>——旧名从原句取
     *  （event trigger 的 object_identity 给的是改名后的新标识） */
    private static final Pattern PG_RENAME_TABLE = Pattern.compile(
            "ALTER\\s+TABLE\\s+(?:IF\\s+EXISTS\\s+)?(?:ONLY\\s+)?(?:[\"]?[\\w$]+[\"]?\\.)?[\"]?([\\w$]+)[\"]?"
                    + "\\s+RENAME\\s+TO\\s+[\"]?([\\w$]+)[\"]?", Pattern.CASE_INSENSITIVE);

    /** COMMENT ON TABLE/COLUMN ... IS <值>——取 IS 之后原样搬运（'文本'/NULL 的字面量
     *  语法 PG 与 DuckDB 兼容，含 '' 转义;E'...' 等 PG 特有形式匹配不上则跳过） */
    private static final Pattern COMMENT_IS = Pattern.compile(
            "COMMENT\\s+ON\\s+(?:TABLE|COLUMN)\\s+\\S+\\s+IS\\s+('(?:[^']|'')*'|NULL)\\s*;?\\s*$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    /** ALTER TABLE 语句涉及主键变更（ADD PRIMARY KEY / ADD COLUMN ... PRIMARY KEY / DROP PRIMARY KEY）。
     *  ⚠️ 存量表补主键时，旧行的主键值回填是 DDL 内部表重写、不产生任何 CDC 事件——
     *  湖里旧行的主键列恒 NULL，而镜像 upsert 按主键定位行，NULL 永久失配
     *  （DELETE/UPDATE 从此打不中）。唯一正解是湖表重建 + 快照重灌当前态 */
    private static final Pattern PRIMARY_KEY_CHANGE = Pattern.compile(
            "\\bPRIMARY\\s+KEY\\b", Pattern.CASE_INSENSITIVE);

    // ---- MySQL ddl 原文正则（schema change 前端专用；MySQL 标识符为反引号或裸名） ----

    /** MySQL 5.x 遗留改名语法：CHANGE [COLUMN] old new <type...>（old≠new 才是 rename，
     *  old=new 是纯类型变更，交给数据驱动跟随） */
    private static final Pattern MYSQL_CHANGE_COLUMN = Pattern.compile(
            "\\bCHANGE\\s+(?:COLUMN\\s+)?[\"`]?([\\w$]+)[\"`]?\\s+[\"`]?([\\w$]+)[\"`]?\\s",
            Pattern.CASE_INSENSITIVE);

    /** 表重命名：ALTER TABLE x RENAME [TO|AS] y（不含 RENAME COLUMN/RENAME INDEX/RENAME KEY） */
    private static final Pattern MYSQL_RENAME_TABLE_ALTER = Pattern.compile(
            "\\bRENAME\\s+(?:TO|AS)?\\s*(?!COLUMN\\b|INDEX\\b|KEY\\b)[\"`]?(?:[\\w$]+[\"`]?\\.[\"`]?)?([\\w$]+)[\"`]?",
            Pattern.CASE_INSENSITIVE);

    /** RENAME TABLE old TO new（独立语句形态；仅取第一对，跨库形态跳过） */
    private static final Pattern MYSQL_RENAME_TABLE_STMT = Pattern.compile(
            "^\\s*RENAME\\s+TABLE\\s+[\"`]?(?:[\\w$]+[\"`]?\\.[\"`]?)?([\\w$]+)[\"`]?\\s+TO\\s+"
                    + "[\"`]?(?:[\\w$]+[\"`]?\\.[\"`]?)?([\\w$]+)[\"`]?", Pattern.CASE_INSENSITIVE);

    /** TRUNCATE TABLE t（schema change 流形态的兜底；主路径是 op=t 数据事件，见消费者） */
    private static final Pattern MYSQL_TRUNCATE = Pattern.compile(
            "^\\s*TRUNCATE\\s+(?:TABLE\\s+)?[\"`]?(?:[\\w$]+[\"`]?\\.[\"`]?)?([\\w$]+)[\"`]?",
            Pattern.CASE_INSENSITIVE);

    /** DROP DATABASE/SCHEMA db → 湖侧 DROP SCHEMA CASCADE（受 followDropTable 开关管） */
    private static final Pattern MYSQL_DROP_DATABASE = Pattern.compile(
            "^\\s*DROP\\s+(?:DATABASE|SCHEMA)\\s+(?:IF\\s+EXISTS\\s+)?[\"`]?([\\w$]+)[\"`]?",
            Pattern.CASE_INSENSITIVE);

    private final DucklakeProperties props;
    private final SyncState syncState;
    private final DuckLakeEngine engine;

    // ================================ PG 前端 ================================

    /**
     * 处理一段连续的 dbz_ddl_log 事件（由消费者在湖事务内调用，与数据写入同序同事务）。
     *
     * @param cacheInvalidator 湖表结构被本方法改动后回调（消费者失效其 knownColumns 缓存）；
     *                         以回调而非直引消费者，避免 consumer↔applier 循环依赖
     * @param rebuildRequester 主键变更时回调（消费者标记湖表重建并触发快照重灌）
     */
    public void apply(Connection conn, List<Struct> run, Consumer<String> cacheInvalidator,
                      Consumer<String> rebuildRequester) throws SQLException {
        // 只认插入(c)与快照读(r)——信号表若被 DELETE 清理会产生墓碑(d)事件,不得当作信号重放
        // (常规清理走 TRUNCATE 不产生事件,此过滤是对误用 DELETE 的兜底)
        List<Struct> signals = run.stream()
                .filter(r -> {
                    String op = str(r, "__op");
                    return op == null || "c".equals(op) || "r".equals(op);
                })
                .toList();
        if (signals.isEmpty()) {
            return;
        }
        syncState.getDdlAudited().increment(signals.size());

        // 按源事务 xid 分组（同一条 ALTER 产生的 ddl_command_end 与 sql_drop 共享 xid）。
        // 已知边界:分组只在单批内做,同事务两行被批边界劈开时(截断事务中间,低概率)
        // hasDrop 误判为 false→删列跟随静默跳过(集成测试偶发复现);湖列多留无数据
        // 正确性影响,下一次同表 DDL 或人工 ALTER 可补齐,故不为此引入跨批配对缓存
        Map<Long, List<Struct>> byXid = new LinkedHashMap<>();
        for (Struct row : signals) {
            byXid.computeIfAbsent(asLong(row, "xid"), k -> new ArrayList<>()).add(row);
        }

        for (Map.Entry<Long, List<Struct>> group : byXid.entrySet()) {
            // 镜像语义:源 DROP TABLE/DROP SCHEMA CASCADE → 湖表真删(时间旅行窗口内旧 snapshot
            // 仍可回看)。⚠️ DROP 命令不出现在 pg_event_trigger_ddl_commands() 里(PG 语义,
            // 集成测试实测踩坑)——审计流只有 sql_drop 行,跟随必须由 sql_drop 行自身触发,
            // 不能挂在下面的 ddl_command_end 循环
            if (props.getMaintenance().isFollowDropTable()) {
                applyDropTables(conn, group.getValue(), cacheInvalidator);
            }
            boolean hasDrop = group.getValue().stream()
                    .anyMatch(r -> "sql_drop".equals(str(r, "ev")));
            for (Struct row : group.getValue()) {
                String tag = str(row, "tag");
                String query = str(row, "query_text");
                if (!"ddl_command_end".equals(str(row, "ev")) || tag == null || query == null) {
                    continue;
                }
                if ("ALTER TABLE".equals(tag) && !hasDrop) {
                    applyRenameIfAny(conn, str(row, "object_identity"), query, cacheInvalidator);
                } else if ("ALTER TABLE".equals(tag) && hasDrop && props.getMaintenance().isFollowDropColumn()) {
                    applyDropColumns(conn, group.getValue(), cacheInvalidator);
                } else if ("COMMENT".equals(tag)) {
                    applyComment(conn, str(row, "object_type"), str(row, "object_identity"), query);
                } else if ("CREATE TABLE".equals(tag) && "table".equals(str(row, "object_type"))) {
                    // DDL 驱动建空表:连源库读列定义(源表已删则自然跳过)——湖表形态即刻跟随,
                    // 不再等首批数据;IF-NOT-EXISTS 幂等,快照重放安全;类型以映射表尽力对齐
                    // 事件推导口径,偏差由数据驱动 followTypeChange 自愈(空表重写零成本)
                    createTableFromPgSource(conn, str(row, "object_identity"), cacheInvalidator);
                }
                // 主键变更/列对齐(快照重放的历史 DDL 跳过——当前态本就完整,重灌无意义):
                // 目标态一律连源库读(readPgTableDef),主键变更与列序/加列共享一次读取
                if ("ALTER TABLE".equals(tag) && !"r".equals(str(row, "__op"))) {
                    String identity = str(row, "object_identity");
                    String lakeTable = identity == null ? null : lakeTableOf(identity);
                    TableDef def = lakeTable == null ? null : readPgTableDef(identity);
                    if (lakeTable != null && PRIMARY_KEY_CHANGE.matcher(query).find()) {
                        // 存量行主键回填无 CDC 事件,湖旧行主键恒 NULL → 重建+重灌当前态
                        // (scanner 就绪则就地重建+直拉灌入,秒级;否则老路 signal 快照)
                        rebuildOrRefill(conn, lakeTable, def, null, cacheInvalidator, rebuildRequester);
                    } else if (def != null && !def.cols().isEmpty()) {
                        // 列定义对齐(rename/删列已在上面应用,此处补齐):ADD COLUMN 即刻建列
                        // 不等数据、列序漂移整表重排
                        syncColumnsFromDefinition(conn, lakeTable, def.cols(), def.colComments(),
                                def.pk(), cacheInvalidator, rebuildRequester);
                    }
                }
                // 其余（CREATE TABLE 建空表见下方分支）:类型变更由数据驱动的 ensureTable 跟随
            }
        }
    }

    /**
     * COMMENT ON TABLE/COLUMN 跟随：对象名取自 object_identity（规范名，免解析原句里的写法），
     * 注释值从 query_text 的 IS 之后原样搬运。重复应用=同值覆盖，天然幂等（快照重放同样无害）。
     */
    private void applyComment(Connection conn, String objectType, String identity, String query) {
        if (identity == null || query == null
                || !("table".equals(objectType) || "table column".equals(objectType))) {
            return; // index/view/function 等对象的 comment 不在湖跟随范围
        }
        Matcher m = COMMENT_IS.matcher(query.trim());
        if (!m.find()) {
            log.info("COMMENT 语句无法解析,跳过: {}", query);
            return;
        }
        String value = m.group(1);
        if ("table".equals(objectType)) {
            String lakeTable = lakeTableOf(identity);
            if (lakeTable != null) {
                commentInLake(conn, "TABLE " + DuckLakeEngine.LAKE + "." + DuckLakeEngine.quoted(lakeTable), value);
            }
        } else {
            int lastDot = identity.lastIndexOf('.');
            String col = identity.substring(lastDot + 1);
            String lakeTable = lakeTableOf(identity.substring(0, lastDot));
            if (lakeTable != null) {
                commentInLake(conn, "COLUMN " + DuckLakeEngine.LAKE + "." + DuckLakeEngine.quoted(lakeTable)
                        + ".\"" + col + "\"", value);
            }
        }
    }

    /**
     * followDropTable=true（默认）时跟随删湖表；sql_drop 的 object_type='table' 行给出被删表
     * （DROP TABLE 每表一行;DROP SCHEMA CASCADE 级联的每张表也各有一行,天然覆盖）。
     * 快照重放（op='r'）的历史 DROP 跳过：快照=当前态，活表不能被历史 DDL 误删。
     */
    private void applyDropTables(Connection conn, List<Struct> group,
                                 Consumer<String> cacheInvalidator) throws SQLException {
        for (Struct row : group) {
            if (!"sql_drop".equals(str(row, "ev")) || !"table".equals(str(row, "object_type"))
                    || "r".equals(str(row, "__op"))) {
                continue;
            }
            String identity = str(row, "object_identity");
            String lakeTable = identity == null ? null : lakeTableOf(identity);
            if (lakeTable != null) {
                dropTableInLake(conn, lakeTable, cacheInvalidator);
            }
        }
    }

    /** rename 应用（列改名或表改名，存在性检查保证幂等，快照重放旧 DDL 时安全跳过）。
     *  表改名：object_identity 是改名后的 schema.<新名>，旧表名从原句正则取 */
    private void applyRenameIfAny(Connection conn, String objectIdentity, String query,
                                  Consumer<String> cacheInvalidator) throws SQLException {
        if (objectIdentity == null) {
            return;
        }
        Matcher col = RENAME_COLUMN.matcher(query);
        if (col.find()) {
            String lakeTable = lakeTableOf(objectIdentity);
            if (lakeTable != null) {
                renameColumnInLake(conn, lakeTable, col.group(1), col.group(2), cacheInvalidator);
            }
            return;
        }
        Matcher tbl = PG_RENAME_TABLE.matcher(query);
        if (tbl.find()) {
            String schema = objectIdentity.split("\\.")[0];
            String from = lakeTableOf(schema + "." + tbl.group(1));
            if (from != null) {
                renameTableInLake(conn, from, tbl.group(2), cacheInvalidator);
            }
        }
    }

    /** followDropColumn=true（默认）时跟随删列；false 时湖保留历史列 */
    private void applyDropColumns(Connection conn, List<Struct> group,
                                  Consumer<String> cacheInvalidator) throws SQLException {
        for (Struct row : group) {
            if (!"sql_drop".equals(str(row, "ev")) || !"table column".equals(str(row, "object_type"))) {
                continue;
            }
            // object_identity 形如 public.sys_user.nickname
            String identity = str(row, "object_identity");
            if (identity == null) {
                continue;
            }
            int lastDot = identity.lastIndexOf('.');
            String col = identity.substring(lastDot + 1);
            String lakeTable = lakeTableOf(identity.substring(0, lastDot));
            if (lakeTable != null) {
                dropColumnInLake(conn, lakeTable, col, cacheInvalidator);
            }
        }
    }

    // ============================== MySQL 前端 ==============================

    /**
     * 处理一段连续的 MySQL schema change 事件（binlog DDL，Debezium 原生结构化；predicate 免
     * unwrap 原样到达）。事件字段：databaseName / ddl（逐字 SQL 原文）/ tableChanges[]
     * （type=CREATE|ALTER|DROP + 变更后的完整表结构快照）/ source.snapshot。
     * <p>
     * 语义判定：结构化优先（DROP 表、注释、主键列表来自 tableChanges），rename/删列这类
     * "快照表达不出的迁移语义"从 ddl 原文正则取——与 PG 前端"目录语义配对"异曲同工，
     * 判定后统一走湖端原语（幂等，快照重放/重复应用安全）。
     * 快照期事件（source.snapshot ≠ false，即历史结构声明）一律只做注释跟随不做结构变更，
     * 对齐 PG 前端对 op='r' 的处理。
     */
    public void applySchemaChange(Connection conn, List<Struct> events, Consumer<String> cacheInvalidator,
                                  Consumer<String> rebuildRequester) throws SQLException {
        syncState.getDdlAudited().increment(events.size());
        for (Struct ev : events) {
            String db = str(ev, "databaseName");
            String ddl = str(ev, "ddl");
            boolean snapshot = isSnapshotEvent(ev);
            List<Struct> tableChanges = structList(ev, "tableChanges");

            // ① DROP TABLE（结构化）：type=DROP 每表一个条目
            if (props.getMaintenance().isFollowDropTable() && !snapshot) {
                for (Struct tc : tableChanges) {
                    if ("DROP".equals(str(tc, "type"))) {
                        String lakeTable = lakeTableOfChangeId(db, str(tc, "id"));
                        if (lakeTable != null) {
                            dropTableInLake(conn, lakeTable, cacheInvalidator);
                        }
                    }
                }
            }
            // ⓪ CREATE TABLE（结构化，快照期也做——历史 CREATE 把存量空表一并补齐）：
            //    tableChanges 自带全列定义+注释,湖表形态即刻跟随,不再等首批数据
            for (Struct tc : tableChanges) {
                if ("CREATE".equals(str(tc, "type"))) {
                    createTableFromMysqlChange(conn, db, tc, cacheInvalidator);
                }
            }
            if (ddl == null || db == null || db.isBlank()) {
                continue;
            }
            // ② DROP DATABASE → 湖 DROP SCHEMA CASCADE（tableChanges 为空,只有 ddl 原文）
            Matcher dropDb = MYSQL_DROP_DATABASE.matcher(ddl);
            if (props.getMaintenance().isFollowDropTable() && !snapshot && dropDb.find()) {
                dropSchemaInLake(conn, dropDb.group(1), cacheInvalidator);
                continue;
            }
            boolean isAlter = ddl.stripLeading().regionMatches(true, 0, "ALTER TABLE", 0, 11);
            String targetTable = firstAlteredTable(db, tableChanges, ddl);
            if (!snapshot && targetTable != null) {
                // ③ RENAME COLUMN：8.0 语法直配；5.x CHANGE old new 且 old≠new 也是改名
                if (isAlter) {
                    Matcher rn = RENAME_COLUMN.matcher(ddl);
                    while (rn.find()) {
                        renameColumnInLake(conn, targetTable, rn.group(1), rn.group(2), cacheInvalidator);
                    }
                    Matcher ch = MYSQL_CHANGE_COLUMN.matcher(ddl);
                    while (ch.find()) {
                        if (!ch.group(1).equalsIgnoreCase(ch.group(2))) {
                            renameColumnInLake(conn, targetTable, ch.group(1), ch.group(2), cacheInvalidator);
                        }
                    }
                    // ④ 删列不再走正则——由 ⑥.5 的目标态 diff 统一跟随(免正则漏匹配)
                    // ⑤ 表重命名：ALTER TABLE x RENAME TO y（RENAME COLUMN 已在上面消费,此处负前瞻规避）
                    Matcher rt = MYSQL_RENAME_TABLE_ALTER.matcher(ddl);
                    if (rt.find() && !RENAME_COLUMN.matcher(ddl).find()) {
                        renameTableInLake(conn, targetTable, rt.group(1), cacheInvalidator);
                    }
                    // ⑥ 主键变更 → 重建 + 重灌当前态（scanner 就绪则就地直拉灌入,对齐 PG 前端;
                    //    目标态取 tableChanges 的变更后全列结构,同时带走表注释）
                    boolean pkChange = PRIMARY_KEY_CHANGE.matcher(ddl).find();
                    if (pkChange) {
                        Struct table = firstAlteredTableStruct(tableChanges);
                        rebuildOrRefill(conn, targetTable, table == null ? null : parseMysqlTableDef(table),
                                table == null ? null : str(table, "comment"), cacheInvalidator, rebuildRequester);
                    }
                    // ⑥.5 列定义对齐(rename/删列已应用;主键变更已整表重建,跳过):tableChanges
                    // 自带变更后全列序——ADD COLUMN 即刻建列(含 AFTER/FIRST 位置语义),列序漂移整表重排
                    for (Struct tc : tableChanges) {
                        if (!pkChange && "ALTER".equals(str(tc, "type"))
                                && tc.schema().field("table") != null && tc.get("table") instanceof Struct table) {
                            TableDef def = parseMysqlTableDef(table);
                            syncColumnsFromDefinition(conn, targetTable, def.cols(), def.colComments(),
                                    def.pk(), cacheInvalidator, rebuildRequester);
                        }
                    }
                }
            }
            if (!snapshot) {
                // ⑦ RENAME TABLE a TO b 独立语句（不依赖 tableChanges）
                Matcher rts = MYSQL_RENAME_TABLE_STMT.matcher(ddl);
                if (rts.find()) {
                    String from = lakeTableOf(db + "." + rts.group(1));
                    if (from != null) {
                        renameTableInLake(conn, from, rts.group(2), cacheInvalidator);
                    }
                }
                // ⑧ TRUNCATE（schema change 流形态兜底;主路径为 op=t 数据事件,见消费者）
                Matcher tr = MYSQL_TRUNCATE.matcher(ddl);
                if (props.getMaintenance().isFollowTruncate() && tr.find()) {
                    truncateInLake(conn, lakeTableOf(db + "." + tr.group(1)));
                }
            }
            // ⑨ 注释跟随（快照期也做:CREATE TABLE 的列注释借此落湖）:tableChanges 携带
            //    变更后全列 comment(include.schema.comments=true),幂等覆盖
            if (ddl.toUpperCase().contains("COMMENT")) {
                applyCommentsFromTableChanges(conn, db, tableChanges);
            }
        }
    }

    /** 快照期 schema change（历史结构声明，非实时 DDL）：source.snapshot ∈ {true,last,...}≠false */
    private static boolean isSnapshotEvent(Struct ev) {
        if (ev.schema().field("source") == null || !(ev.get("source") instanceof Struct source)) {
            return false;
        }
        String snapshot = str(source, "snapshot");
        return snapshot != null && !"false".equals(snapshot);
    }

    /** 事件指向的第一张（也几乎总是唯一一张）被改表 → 湖表名：tableChanges.id 优先，ddl 正文兜底 */
    private String firstAlteredTable(String db, List<Struct> tableChanges, String ddl) {
        for (Struct tc : tableChanges) {
            String lakeTable = lakeTableOfChangeId(db, str(tc, "id"));
            if (lakeTable != null) {
                return lakeTable;
            }
        }
        // tableChanges 为空（如 TRUNCATE/部分语句）：从 ALTER TABLE `db`.`t` / TRUNCATE t 原文抓表名
        Matcher m = Pattern.compile(
                        "(?:ALTER|TRUNCATE)\\s+TABLE\\s+[\"`]?(?:([\\w$]+)[\"`]?\\.[\"`]?)?([\\w$]+)[\"`]?",
                        Pattern.CASE_INSENSITIVE)
                .matcher(ddl);
        if (m.find()) {
            return lakeTableOf((m.group(1) != null ? m.group(1) : db) + "." + m.group(2));
        }
        return null;
    }

    /** tableChanges 里首个 ALTER 条目的变更后表结构（scanner 重建重灌的目标态来源）；无则 null */
    private static Struct firstAlteredTableStruct(List<Struct> tableChanges) {
        for (Struct tc : tableChanges) {
            if ("ALTER".equals(str(tc, "type"))
                    && tc.schema().field("table") != null && tc.get("table") instanceof Struct table) {
                return table;
            }
        }
        return null;
    }

    /** tableChanges.id（"db"."table"，表重命名时 "<old>,<new>" 取 old）→ 湖表名 */
    private String lakeTableOfChangeId(String db, String id) {
        if (id == null) {
            return null;
        }
        String first = id.split(",")[0].replace("\"", "").replace("`", "");
        // id 形如 db.table；防御仅表名的形态（拼上事件库名）
        return lakeTableOf(first.contains(".") ? first : db + "." + first);
    }

    /** tableChanges 的列/表 comment → 湖侧 COMMENT ON（值经单引号转义;null 注释跳过——
     *  MySQL 无"删注释"语义,置空表现为 comment=''，同样跟随覆盖） */
    private void applyCommentsFromTableChanges(Connection conn, String db, List<Struct> tableChanges) {
        for (Struct tc : tableChanges) {
            if (!(tc.schema().field("table") != null && tc.get("table") instanceof Struct table)) {
                continue;
            }
            String lakeTable = lakeTableOfChangeId(db, str(tc, "id"));
            if (lakeTable == null) {
                continue;
            }
            String tableComment = str(table, "comment");
            if (tableComment != null) {
                commentInLake(conn, "TABLE " + DuckLakeEngine.LAKE + "." + DuckLakeEngine.quoted(lakeTable),
                        quoteLiteral(tableComment));
            }
            for (Struct col : structList(table, "columns")) {
                String comment = str(col, "comment");
                if (comment != null) {
                    commentInLake(conn, "COLUMN " + DuckLakeEngine.LAKE + "." + DuckLakeEngine.quoted(lakeTable)
                            + ".\"" + str(col, "name") + "\"", quoteLiteral(comment));
                }
            }
        }
    }

    // ============================ DDL 驱动建空表（两源前端 + 类型映射） ============================

    /** 源表定义（列名→duck 类型按源列序、列注释、主键列） */
    private record TableDef(Map<String, String> cols, Map<String, String> colComments, List<String> pk) {
    }

    /** [PG] 连源库读 information_schema 的表定义（表已删/连接瞬断返回 null，调用方跳过） */
    private TableDef readPgTableDef(String identity) {
        String[] parts = identity.split("\\.");
        DucklakeProperties.Source src = props.getSource();
        Map<String, String> cols = new LinkedHashMap<>();
        List<String> pk = new ArrayList<>();
        try (Connection c = java.sql.DriverManager.getConnection(src.jdbcUrl(), src.getUser(), src.getPassword())) {
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT column_name, data_type, udt_name, numeric_precision, numeric_scale "
                            + "FROM information_schema.columns WHERE table_schema=? AND table_name=? "
                            + "ORDER BY ordinal_position")) {
                ps.setString(1, parts[0]);
                ps.setString(2, parts[1]);
                try (var rs = ps.executeQuery()) {
                    while (rs.next()) {
                        cols.put(rs.getString(1), pgTypeToDuck(rs.getString(2), rs.getString(3),
                                (Integer) rs.getObject(4), (Integer) rs.getObject(5)));
                    }
                }
            }
            // 主键列(排序聚簇用;查询失败不致命,无主键同形态)
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT kcu.column_name FROM information_schema.table_constraints tc "
                            + "JOIN information_schema.key_column_usage kcu "
                            + "  ON kcu.constraint_name = tc.constraint_name AND kcu.table_schema = tc.table_schema "
                            + "WHERE tc.constraint_type='PRIMARY KEY' AND tc.table_schema=? AND tc.table_name=? "
                            + "ORDER BY kcu.ordinal_position")) {
                ps.setString(1, parts[0]);
                ps.setString(2, parts[1]);
                try (var rs = ps.executeQuery()) {
                    while (rs.next()) {
                        pk.add(rs.getString(1));
                    }
                }
            }
        } catch (SQLException e) {
            log.warn("读源表定义失败(跳过本次 DDL 跟随,数据驱动兜底): {} ({})", identity, e.getMessage());
            return null;
        }
        return new TableDef(cols, Map.of(), pk);
    }

    /** [MySQL] 从 tableChanges 的 table 结构组装表定义 */
    private TableDef parseMysqlTableDef(Struct table) {
        Map<String, String> cols = new LinkedHashMap<>();
        Map<String, String> colComments = new LinkedHashMap<>();
        for (Struct col : structList(table, "columns")) {
            String name = str(col, "name");
            cols.put(name, mysqlTypeToDuck(str(col, "typeName"), asLong(col, "length"), asLong(col, "scale")));
            String comment = str(col, "comment");
            if (comment != null && !comment.isBlank()) {
                colComments.put(name, comment);
            }
        }
        return new TableDef(cols, colComments, stringList(table, "primaryKeyColumnNames"));
    }

    /** [PG] 连源库读列定义建湖空表（表已删/连接瞬断则跳过，数据驱动兜底） */
    private void createTableFromPgSource(Connection conn, String identity, Consumer<String> cacheInvalidator) {
        String lakeTable = identity == null ? null : lakeTableOf(identity);
        if (lakeTable == null) {
            return;
        }
        TableDef def = readPgTableDef(identity);
        if (def == null) {
            return;
        }
        try {
            createTableInLake(conn, lakeTable, def.cols(), null, Map.of(), def.pk(), cacheInvalidator);
        } catch (SQLException e) {
            log.warn("湖侧建空表失败(跳过,首批数据将兜底建表): {} ({})", lakeTable, e.getMessage());
        }
    }

    /** [MySQL] 以 tableChanges 的全列结构建湖空表（快照期历史 CREATE 也建=存量空表补齐），
     *  表/列注释一并应用 */
    private void createTableFromMysqlChange(Connection conn, String db, Struct tc,
                                            Consumer<String> cacheInvalidator) {
        String lakeTable = lakeTableOfChangeId(db, str(tc, "id"));
        if (lakeTable == null || !(tc.schema().field("table") != null && tc.get("table") instanceof Struct table)) {
            return;
        }
        TableDef def = parseMysqlTableDef(table);
        try {
            createTableInLake(conn, lakeTable, def.cols(), str(table, "comment"), def.colComments(),
                    def.pk(), cacheInvalidator);
        } catch (SQLException e) {
            log.warn("湖侧建空表失败(跳过,首批数据将兜底建表): {} ({})", lakeTable, e.getMessage());
        }
    }

    /** PG information_schema 类型 → DuckDB 列型（对齐事件推导口径：isostring+precise；
     *  拿不准一律 VARCHAR，数据到达后 followTypeChange 自愈——空表阶段类型偏差零成本） */
    private String pgTypeToDuck(String dataType, String udtName, Integer precision, Integer scale) {
        return switch (dataType == null ? "" : dataType) {
            case "bigint" -> "BIGINT";
            case "integer" -> "INTEGER";
            case "smallint" -> "SMALLINT";
            case "numeric", "decimal" -> scale == null || precision == null
                    ? "DECIMAL(38,18)"   // 裸 numeric:DuckDB 上限形态(与事件口径一致)
                    : (precision < 1 || precision > 38 || scale < 0 || scale > precision
                            ? "VARCHAR" : "DECIMAL(" + precision + "," + scale + ")");
            case "real" -> "FLOAT";
            case "double precision" -> "DOUBLE";
            case "boolean" -> "BOOLEAN";
            case "timestamp without time zone" -> "TIMESTAMP";
            case "timestamp with time zone" -> "TIMESTAMPTZ";
            case "date" -> "DATE";
            case "time without time zone" -> "TIME";
            case "time with time zone" -> "TIMETZ";
            case "bytea" -> "BLOB";
            case "json", "jsonb" -> props.getMaintenance().isJsonAsVariant() ? "VARIANT" : "JSON";
            case "uuid" -> "UUID";
            case "interval" -> "INTERVAL";
            case "ARRAY" -> switch (udtName == null ? "" : udtName) {
                case "_int8" -> "BIGINT[]";
                case "_int4" -> "INTEGER[]";
                case "_int2" -> "SMALLINT[]";
                case "_text", "_varchar", "_bpchar" -> "VARCHAR[]";
                case "_float4" -> "FLOAT[]";
                case "_float8" -> "DOUBLE[]";
                case "_bool" -> "BOOLEAN[]";
                case "_date" -> "DATE[]";
                case "_timestamp" -> "TIMESTAMP[]";
                case "_timestamptz" -> "TIMESTAMPTZ[]";
                case "_uuid" -> "UUID[]";
                default -> "VARCHAR";  // numeric[]/嵌套等:元素精度拿不到,兜底待数据自愈
            };
            default -> "VARCHAR";  // enum/自定义/网络类型等,与事件 VARCHAR 兜底一致
        };
    }

    /** MySQL tableChanges 的 typeName → DuckDB 列型（对齐事件推导口径：
     *  TinyIntOneToBooleanConverter + bigint.unsigned=precise + isostring） */
    private String mysqlTypeToDuck(String typeName, Long length, Long scale) {
        String tn = typeName == null ? "" : typeName.toUpperCase();
        boolean unsigned = tn.contains("UNSIGNED");
        String base = tn.replace(" UNSIGNED", "").replace(" ZEROFILL", "").trim();
        return switch (base) {
            case "TINYINT" -> length != null && length == 1 ? "BOOLEAN" : "SMALLINT";
            case "BOOLEAN", "BOOL" -> "BOOLEAN";
            case "SMALLINT" -> unsigned ? "INTEGER" : "SMALLINT";
            case "MEDIUMINT" -> "INTEGER";
            case "INT", "INTEGER" -> unsigned ? "BIGINT" : "INTEGER";
            // BIGINT UNSIGNED 对齐事件口径(UnsignedBigintConverter 文本出流 → UBIGINT 原生映射)
            case "BIGINT" -> unsigned ? "UBIGINT" : "BIGINT";
            case "DECIMAL", "NUMERIC" -> {
                long p = length == null ? 10 : length;
                long s = scale == null ? 0 : scale;
                yield p < 1 || p > 38 || s < 0 || s > p ? "VARCHAR" : "DECIMAL(" + p + "," + s + ")";
            }
            case "FLOAT" -> "FLOAT";
            case "DOUBLE", "REAL" -> "DOUBLE";
            case "BIT" -> length != null && length == 1 ? "BOOLEAN" : "BLOB";
            case "DATETIME" -> "TIMESTAMP";
            case "TIMESTAMP" -> "TIMESTAMPTZ";
            case "DATE" -> "DATE";
            case "TIME" -> "TIME";
            case "YEAR" -> "INTEGER";
            case "JSON" -> props.getMaintenance().isJsonAsVariant() ? "VARIANT" : "JSON";
            case "BINARY", "VARBINARY", "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB" -> "BLOB";
            case "GEOMETRY", "POINT", "LINESTRING", "POLYGON", "MULTIPOINT",
                 "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION" -> "BLOB";
            default -> "VARCHAR";  // CHAR/VARCHAR/TEXT 族/ENUM/SET 及未知类型
        };
    }

    /**
     * 列定义对齐原语（DDL 驱动的加列/删列/列序跟随）：以"源目标态"对齐湖表——
     * ① 湖缺列 → ADD COLUMN IF NOT EXISTS（含注释），不再等首批数据；
     * ② 湖多余列（源已删）→ 跟随删除（follow-drop-column=false 时保留追尾）——
     *    目标态 diff 驱动，无需依赖审计配对/正则；rename 已由前置步骤应用，
     *    未被识别的改名形态会退化为"删旧列+建新列"（旧值在时间旅行窗口内可回看）；
     * ③ 列集合齐但顺序不同（MySQL ADD ... AFTER/FIRST、MODIFY 重排）→ 整表重写换表按目标序重排
     *    （DuckDB/DuckLake 无列重排语法；换表在空表阶段挂排序聚簇——对含事务内 inlined
     *    数据的表 ALTER SET SORTED BY 会被 DuckLake 拒绝——并重刷列注释）；
     * ④ 重排失败 → rebuildRequester 兜底（重建 + blocking 快照重灌当前态）。
     * 湖表不存在时不做事（建表另有 createTableInLake / 数据驱动 ensureTable）。
     */
    private void syncColumnsFromDefinition(Connection conn, String lakeTable,
                                           Map<String, String> targetCols, Map<String, String> colComments,
                                           List<String> keyColumns, Consumer<String> cacheInvalidator,
                                           Consumer<String> rebuildRequester) throws SQLException {
        if (targetCols.isEmpty() || !tableExists(conn, lakeTable)) {
            return;
        }
        List<String> lakeCols = lakeColumnOrder(conn, lakeTable);
        boolean changed = false;
        // ① 补缺列(尾部追加;类型偏差由数据驱动 followTypeChange 自愈)
        for (Map.Entry<String, String> col : targetCols.entrySet()) {
            if (!lakeCols.contains(col.getKey())) {
                try (Statement s = conn.createStatement()) {
                    s.execute("ALTER TABLE " + DuckLakeEngine.LAKE + "." + DuckLakeEngine.quoted(lakeTable)
                            + " ADD COLUMN IF NOT EXISTS \"" + col.getKey() + "\" " + col.getValue());
                }
                String comment = colComments.get(col.getKey());
                if (comment != null) {
                    commentInLake(conn, "COLUMN " + DuckLakeEngine.LAKE + "." + DuckLakeEngine.quoted(lakeTable)
                            + ".\"" + col.getKey() + "\"", quoteLiteral(comment));
                }
                syncState.getDdlApplied().increment();
                log.warn("湖表加列(DDL 跟随,不等数据): {}.{} ({})", lakeTable, col.getKey(), col.getValue());
                changed = true;
            }
        }
        // ② 删多余列(目标态 diff,免审计配对)
        if (props.getMaintenance().isFollowDropColumn()) {
            for (String lakeCol : lakeCols) {
                if (!targetCols.containsKey(lakeCol)) {
                    dropColumnInLake(conn, lakeTable, lakeCol, cacheInvalidator);
                    changed = true;
                }
            }
        }
        if (changed) {
            lakeCols = lakeColumnOrder(conn, lakeTable);
            cacheInvalidator.accept(lakeTable);
        }
        // ③ 列序对齐:目标序 = 源列序 + 湖独有列(follow-drop-column=false 保留的)追尾
        List<String> targetOrder = new ArrayList<>(targetCols.keySet());
        for (String c : lakeCols) {
            if (!targetOrder.contains(c)) {
                targetOrder.add(c);
            }
        }
        targetOrder.retainAll(lakeCols); // 目标列可能因 followDropColumn=false 等原因未全在湖,取交集序
        if (lakeCols.equals(targetOrder)) {
            return;
        }
        try {
            reorderTable(conn, lakeTable, targetOrder, keyColumns, colComments);
            cacheInvalidator.accept(lakeTable);
            syncState.getDdlApplied().increment();
            log.warn("湖表列序跟随(整表重写换表): {} -> {}", lakeTable, targetOrder);
        } catch (SQLException e) {
            // ④ 重排失败(类型冲突等):重建+重灌兜底。湖事务多半已因失败语句 abort——
            // rebuildOrRefill 的就地重建此时同样失败,由其内部 catch 自动落到 signal 快照老路
            log.warn("湖表列序重排失败,转重建+重灌: {} ({})", lakeTable, e.getMessage());
            rebuildOrRefill(conn, lakeTable, new TableDef(targetCols, colComments, keyColumns), null,
                    cacheInvalidator, rebuildRequester);
        }
    }

    /**
     * 湖表重建 + 当前态重灌，二选一路径：scanner 通道就绪且目标态定义可得 → 就地 DROP 重建
     * （目标态列序/注释/排序聚簇）+ scanner 直拉全量灌入（新表空灌免去重；与 DDL 段同湖事务，
     * 原子可见；服务器实测 100 万行 PG 1.2s / MySQL 4.0s，且不停流）；否则回退
     * rebuildRequester——消费者标记重建 + signal 快照重灌老路，也是 scanner 就地失败
     * （源瞬断/事务已 abort/MySQL 跨库）后的自动降级。
     */
    private void rebuildOrRefill(Connection conn, String lakeTable, TableDef def, String tableComment,
                                 Consumer<String> cacheInvalidator, Consumer<String> rebuildRequester) {
        String srcRef = srcRefOf(lakeTable);
        if (def == null || def.cols().isEmpty() || srcRef == null) {
            rebuildRequester.accept(lakeTable);
            return;
        }
        try {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP TABLE IF EXISTS " + DuckLakeEngine.LAKE + "." + DuckLakeEngine.quoted(lakeTable));
            }
            createTableInLake(conn, lakeTable, def.cols(), tableComment, def.colComments(), def.pk(),
                    cacheInvalidator);
            int n = refillFromScanner(conn, lakeTable, def.cols(), srcRef, List.of());
            log.warn("湖表重建+scanner 直灌完成: {} ({} 行)", lakeTable, n);
        } catch (SQLException e) {
            log.warn("scanner 重建直灌失败,回退 signal 快照重灌: {} ({})", lakeTable, e.getMessage());
            rebuildRequester.accept(lakeTable);
        }
    }

    /** 湖表名反推 scanner 源表引用（剥 schema 前缀）；通道未就绪/MySQL 跨库时 null（回退 signal） */
    public String srcRefOf(String lakeTable) {
        String prefix = props.getMaintenance().getSchemaPrefix();
        String srcTable = lakeTable.startsWith(prefix) ? lakeTable.substring(prefix.length()) : lakeTable;
        int dot = srcTable.indexOf('.');
        if (dot <= 0) {
            return null;
        }
        return engine.scannerSrcRef(srcTable.substring(0, dot), srcTable.substring(dot + 1));
    }

    /**
     * scanner 直拉重灌原语：INSERT INTO 湖表 SELECT 按湖列投影（CAST 对齐列型）FROM 源表。
     * 源列名=湖列名（rename 已镜像跟随）；antiJoinKeys 非空时按主键 anti-join 跳过湖里已有行
     * （类型重建路径：重建后当批数据先落湖，补灌历史需去重；空表直灌传空列表零开销）。
     * 已知编码差异：GEOMETRY/point 列 scanner 给源文本形态而非流式路径的 WKB（见 README 局限）。
     */
    public int refillFromScanner(Connection conn, String lakeTable, Map<String, String> lakeCols,
                                 String srcRef, List<String> antiJoinKeys) throws SQLException {
        StringBuilder cols = new StringBuilder();
        StringBuilder proj = new StringBuilder();
        for (Map.Entry<String, String> c : lakeCols.entrySet()) {
            cols.append(cols.isEmpty() ? "" : ", ").append('"').append(c.getKey()).append('"');
            proj.append(proj.isEmpty() ? "" : ", ")
                    .append("CAST(s.\"").append(c.getKey()).append("\" AS ").append(c.getValue())
                    .append(") AS \"").append(c.getKey()).append('"');
        }
        String qTable = DuckLakeEngine.LAKE + "." + DuckLakeEngine.quoted(lakeTable);
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(qTable)
                .append(" (").append(cols).append(") SELECT ").append(proj)
                .append(" FROM ").append(srcRef).append(" s");
        if (antiJoinKeys != null && !antiJoinKeys.isEmpty()) {
            sql.append(" WHERE NOT EXISTS (SELECT 1 FROM ").append(qTable).append(" l WHERE ");
            for (int i = 0; i < antiJoinKeys.size(); i++) {
                sql.append(i > 0 ? " AND " : "").append("l.\"").append(antiJoinKeys.get(i))
                        .append("\" = s.\"").append(antiJoinKeys.get(i)).append('"');
            }
            sql.append(')');
        }
        long t0 = System.currentTimeMillis();
        int n;
        try (Statement s = conn.createStatement()) {
            n = s.executeUpdate(sql.toString());
        }
        syncState.getDdlApplied().increment();
        log.info("scanner 直灌 {}: {} 行 {}ms (源 {})", lakeTable, n, System.currentTimeMillis() - t0, srcRef);
        return n;
    }

    /** 整表重写换表按目标列序重排。三步式：AS SELECT LIMIT 0 建空表(继承列型) →
     *  空表阶段挂排序聚簇(含事务内 inlined 数据的表被 DuckLake 拒绝 ALTER,1.0 实测) →
     *  灌数据换表 → 重刷列注释(CREATE AS SELECT 不传递注释) */
    private void reorderTable(Connection conn, String lakeTable, List<String> targetOrder,
                              List<String> keyColumns, Map<String, String> colComments) throws SQLException {
        String schemaName = lakeTable.substring(0, lakeTable.indexOf('.'));
        String tableName = lakeTable.substring(lakeTable.indexOf('.') + 1);
        String proj = String.join(", ", targetOrder.stream().map(c -> '"' + c + '"').toList());
        String tmpName = tableName + "__reorder";
        String qTmp = DuckLakeEngine.LAKE + ".\"" + schemaName + "\".\"" + tmpName + "\"";
        String qTable = DuckLakeEngine.LAKE + "." + DuckLakeEngine.quoted(lakeTable);
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS " + qTmp);
            s.execute("CREATE TABLE " + qTmp + " AS SELECT " + proj + " FROM " + qTable + " LIMIT 0");
        }
        applySortedByPk(conn, schemaName + "." + tmpName, keyColumns);
        try (Statement s = conn.createStatement()) {
            s.execute("INSERT INTO " + qTmp + " SELECT " + proj + " FROM " + qTable);
            s.execute("DROP TABLE " + qTable);
            s.execute("ALTER TABLE " + qTmp + " RENAME TO \"" + tableName + '"');
        }
        for (Map.Entry<String, String> c : colComments.entrySet()) {
            if (targetOrder.contains(c.getKey())) {
                commentInLake(conn, "COLUMN " + DuckLakeEngine.LAKE + "." + DuckLakeEngine.quoted(lakeTable)
                        + ".\"" + c.getKey() + "\"", quoteLiteral(c.getValue()));
            }
        }
    }

    /** 湖表现有列名（按 ordinal_position 序） */
    private List<String> lakeColumnOrder(Connection conn, String lakeTable) throws SQLException {
        String[] parts = lakeTable.split("\\.", 2);
        List<String> cols = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT column_name FROM information_schema.columns "
                        + "WHERE table_catalog=? AND table_schema=? AND table_name=? ORDER BY ordinal_position")) {
            ps.setString(1, DuckLakeEngine.LAKE);
            ps.setString(2, parts[0]);
            ps.setString(3, parts[1]);
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    cols.add(rs.getString(1));
                }
            }
        }
        return cols;
    }

    /** 建湖空表原语：表已存在则跳过（不动现有形态，列对齐交给数据驱动 ensureTable）；
     *  湖 schema 按需建；表/列注释一并应用（幂等覆盖）；有主键则挂排序聚簇 */
    private void createTableInLake(Connection conn, String lakeTable, Map<String, String> cols,
                                   String tableComment, Map<String, String> colComments,
                                   List<String> keyColumns, Consumer<String> cacheInvalidator) throws SQLException {
        if (cols.isEmpty() || tableExists(conn, lakeTable)) {
            return;
        }
        String schema = lakeTable.substring(0, lakeTable.indexOf('.'));
        StringBuilder ddl = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(DuckLakeEngine.LAKE).append('.').append(DuckLakeEngine.quoted(lakeTable)).append(" (");
        int i = 0;
        for (Map.Entry<String, String> col : cols.entrySet()) {
            ddl.append(i++ > 0 ? ", " : "").append('"').append(col.getKey()).append("\" ").append(col.getValue());
        }
        ddl.append(')');
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE SCHEMA IF NOT EXISTS " + DuckLakeEngine.LAKE + ".\"" + schema + '"');
            s.execute(ddl.toString());
        }
        if (tableComment != null && !tableComment.isBlank()) {
            commentInLake(conn, "TABLE " + DuckLakeEngine.LAKE + "." + DuckLakeEngine.quoted(lakeTable),
                    quoteLiteral(tableComment));
        }
        for (Map.Entry<String, String> c : colComments.entrySet()) {
            commentInLake(conn, "COLUMN " + DuckLakeEngine.LAKE + "." + DuckLakeEngine.quoted(lakeTable)
                    + ".\"" + c.getKey() + "\"", quoteLiteral(c.getValue()));
        }
        applySortedByPk(conn, lakeTable, keyColumns);
        cacheInvalidator.accept(lakeTable);
        syncState.getDdlApplied().increment();
        log.info("湖侧建空表(DDL 跟随): {} ({} 列)", lakeTable, cols.size());
    }

    // ============================ 湖端执行原语（双前端共用） ============================

    /** 湖表 rename 列：列存在性双检保证幂等（重复应用/快照重放安全跳过） */
    private void renameColumnInLake(Connection conn, String lakeTable, String oldCol, String newCol,
                                    Consumer<String> cacheInvalidator) throws SQLException {
        if (columnExists(conn, lakeTable, oldCol) && !columnExists(conn, lakeTable, newCol)) {
            try (Statement s = conn.createStatement()) {
                s.execute("ALTER TABLE " + DuckLakeEngine.LAKE + "." + DuckLakeEngine.quoted(lakeTable)
                        + " RENAME COLUMN \"" + oldCol + "\" TO \"" + newCol + "\"");
            }
            cacheInvalidator.accept(lakeTable);
            syncState.getDdlApplied().increment();
            log.warn("湖侧真 rename 应用: {} {} -> {}", lakeTable, oldCol, newCol);
        } else {
            log.info("rename 跳过(已应用或列不存在): {} {} -> {}", lakeTable, oldCol, newCol);
        }
    }

    /** 湖表删列（列存在才删，幂等） */
    private void dropColumnInLake(Connection conn, String lakeTable, String col,
                                  Consumer<String> cacheInvalidator) throws SQLException {
        if (columnExists(conn, lakeTable, col)) {
            try (Statement s = conn.createStatement()) {
                s.execute("ALTER TABLE " + DuckLakeEngine.LAKE + "." + DuckLakeEngine.quoted(lakeTable)
                        + " DROP COLUMN \"" + col + "\"");
            }
            cacheInvalidator.accept(lakeTable);
            syncState.getDdlApplied().increment();
            log.warn("湖侧跟随删列: {}.{}", lakeTable, col);
        }
    }

    /** 湖表真删（IF EXISTS 幂等；时间旅行窗口内旧 snapshot 仍可回看） */
    private void dropTableInLake(Connection conn, String lakeTable,
                                 Consumer<String> cacheInvalidator) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS " + DuckLakeEngine.LAKE + "." + DuckLakeEngine.quoted(lakeTable));
        }
        cacheInvalidator.accept(lakeTable);
        syncState.getDdlApplied().increment();
        log.warn("湖侧跟随删表: {}", lakeTable);
    }

    /** 湖 schema 整删（MySQL DROP DATABASE 跟随；CASCADE 连带全部表） */
    private void dropSchemaInLake(Connection conn, String db, Consumer<String> cacheInvalidator) throws SQLException {
        String lakeSchema = props.getMaintenance().getSchemaPrefix() + db;
        try (Statement s = conn.createStatement()) {
            s.execute("DROP SCHEMA IF EXISTS " + DuckLakeEngine.LAKE + ".\"" + lakeSchema + "\" CASCADE");
        }
        cacheInvalidator.accept(lakeSchema + ".*");
        syncState.getDdlApplied().increment();
        log.warn("湖侧跟随删库(schema): {}", lakeSchema);
    }

    /** 湖表重命名（同 schema 内；目标已存在或源不存在时跳过，幂等） */
    private void renameTableInLake(Connection conn, String lakeTable, String newTable,
                                   Consumer<String> cacheInvalidator) throws SQLException {
        String schema = lakeTable.substring(0, lakeTable.indexOf('.'));
        String target = schema + "." + newTable;
        if (tableExists(conn, lakeTable) && !tableExists(conn, target)) {
            try (Statement s = conn.createStatement()) {
                s.execute("ALTER TABLE " + DuckLakeEngine.LAKE + "." + DuckLakeEngine.quoted(lakeTable)
                        + " RENAME TO \"" + newTable + "\"");
            }
            cacheInvalidator.accept(lakeTable);
            cacheInvalidator.accept(target);
            syncState.getDdlApplied().increment();
            log.warn("湖侧跟随表重命名: {} -> {}", lakeTable, target);
        } else {
            log.info("表重命名跳过(已应用或源表不存在): {} -> {}", lakeTable, target);
        }
    }

    /** 湖表清空（TRUNCATE 跟随；表未落湖过则静默跳过） */
    private void truncateInLake(Connection conn, String lakeTable) throws SQLException {
        if (lakeTable == null || !tableExists(conn, lakeTable)) {
            return;
        }
        try (Statement s = conn.createStatement()) {
            s.execute("DELETE FROM " + DuckLakeEngine.LAKE + "." + DuckLakeEngine.quoted(lakeTable));
        }
        syncState.getDdlApplied().increment();
        log.warn("湖侧跟随 TRUNCATE(清空): {}", lakeTable);
    }

    /** lake 是否真 DuckLake catalog（进程内一次探测缓存）：单测以普通内存库伪装 lake，
     *  其上执行 SET SORTED BY 会失败并 abort 整个湖事务（DuckDB 事务内语句失败即事务死，
     *  try-catch 救不回）——必须先探测再执行，不能靠异常兜底 */
    private volatile Boolean lakeIsDucklake;

    /**
     * 湖表排序聚簇（maintenance.sorted-by-pk）：SET SORTED BY (主键)——写出与压实时自动按键排序
     * （sort_on_insert 保持 DuckLake 默认 true：CDC 小批的写出排序毫秒级，换来每个文件即刻有序、
     * min/max 统计即刻收紧；压实时全局重排进一步收敛），主键/范围过滤的文件剪枝显著受益
     * （DuckLake 无索引，这是查询加速正道）。
     * ⚠️ 刻意不调 set_option('sort_on_insert',...)：该 CALL 不允许在事务内执行（1.0 实测），
     * 而本方法运行于湖事务中——失败会 abort 整个事务且 try-catch 救不回。
     * 供消费者数据驱动建表、类型重写换表与本类 DDL 建表三路共用。
     */
    public void applySortedByPk(Connection conn, String lakeTable, List<String> keyColumns) throws SQLException {
        if (!props.getMaintenance().isSortedByPk() || keyColumns == null || keyColumns.isEmpty()) {
            return;
        }
        if (lakeIsDucklake == null) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT type FROM duckdb_databases() WHERE database_name = ?")) {
                ps.setString(1, DuckLakeEngine.LAKE);
                try (var rs = ps.executeQuery()) {
                    lakeIsDucklake = rs.next() && "ducklake".equalsIgnoreCase(rs.getString(1));
                }
            }
        }
        if (!lakeIsDucklake) {
            log.debug("湖表排序聚簇跳过(lake 非 DuckLake catalog): {}", lakeTable);
            return;
        }
        String keys = String.join(", ", keyColumns.stream().map(k -> '"' + k + '"').toList());
        try (Statement s = conn.createStatement()) {
            s.execute("ALTER TABLE " + DuckLakeEngine.LAKE + "." + DuckLakeEngine.quoted(lakeTable)
                    + " SET SORTED BY (" + keys + ")");
        }
        log.info("湖表排序聚簇已挂(写出与压实按主键排序): {} SORTED BY ({})", lakeTable, keys);
    }

    /** COMMENT ON 执行（值须已是合法字面量）。失败仅告警——注释是元数据锦上添花，
     *  不值得让 CDC 批进入重试 */
    private void commentInLake(Connection conn, String target, String valueLiteral) {
        try (Statement s = conn.createStatement()) {
            s.execute("COMMENT ON " + target + " IS " + valueLiteral);
            syncState.getDdlApplied().increment();
            log.info("湖侧注释跟随: {} IS {}", target,
                    valueLiteral.length() > 40 ? valueLiteral.substring(0, 40) + "…" : valueLiteral);
        } catch (SQLException e) {
            log.warn("湖侧注释应用失败(跳过,不影响数据链): {} ({})", target, e.getMessage());
        }
    }

    /** public.sys_user → <前缀>public.sys_user（镜像命名，与消费者的表命名规则一致；
     *  MySQL 侧 db.table 同位映射） */
    private String lakeTableOf(String sourceIdentity) {
        String[] parts = sourceIdentity.split("\\.");
        if (parts.length < 2) {
            return null;
        }
        return props.getMaintenance().getSchemaPrefix() + parts[0] + "." + parts[1];
    }

    private boolean columnExists(Connection conn, String lakeTable, String column) throws SQLException {
        String[] parts = lakeTable.split("\\.", 2);
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT 1 FROM information_schema.columns WHERE table_catalog=? AND table_schema=? AND table_name=? AND column_name=?")) {
            ps.setString(1, DuckLakeEngine.LAKE);
            ps.setString(2, parts[0]);
            ps.setString(3, parts[1]);
            ps.setString(4, column);
            try (var rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private boolean tableExists(Connection conn, String lakeTable) throws SQLException {
        String[] parts = lakeTable.split("\\.", 2);
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT 1 FROM information_schema.tables WHERE table_catalog=? AND table_schema=? AND table_name=?")) {
            ps.setString(1, DuckLakeEngine.LAKE);
            ps.setString(2, parts[0]);
            ps.setString(3, parts[1]);
            try (var rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    /** 明文 → SQL 单引号字面量（'' 转义） */
    private static String quoteLiteral(String text) {
        return "'" + text.replace("'", "''") + "'";
    }

    private static String str(Struct row, String field) {
        if (row.schema().field(field) == null) {
            return null;
        }
        Object v = row.get(field);
        return v == null ? null : v.toString();
    }

    /** Struct 的数组字段 → List<Struct>（字段缺失/空返回空列表，防御 schema 差异） */
    @SuppressWarnings("unchecked")
    private static List<Struct> structList(Struct row, String field) {
        if (row.schema().field(field) == null) {
            return List.of();
        }
        Object v = row.get(field);
        return v instanceof List<?> list ? (List<Struct>) list : List.of();
    }

    /** Struct 的字符串数组字段 → List<String>（字段缺失/空返回空列表） */
    @SuppressWarnings("unchecked")
    private static List<String> stringList(Struct row, String field) {
        if (row.schema().field(field) == null) {
            return List.of();
        }
        Object v = row.get(field);
        return v instanceof List<?> list ? (List<String>) list : List.of();
    }

    private static Long asLong(Struct row, String field) {
        if (row.schema().field(field) == null) {
            return null;
        }
        Object v = row.get(field);
        return switch (v) {
            case null -> null;
            case Number n -> n.longValue();
            default -> Long.parseLong(v.toString());
        };
    }
}
