package org.dpdns.zerodep.ducklake.ddl;

import org.dpdns.zerodep.ducklake.config.DucklakeProperties;
import org.dpdns.zerodep.ducklake.metrics.SyncState;
import org.dpdns.zerodep.ducklake.sink.DuckType;
import org.dpdns.zerodep.ducklake.sink.DuckLakeEngine;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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
 * 原生 CDC 的 DDL 跟随器——纯跟随、不留档，两源共用湖端执行原语：
 * <ul>
 *   <li><b>PG 前端 {@link #apply}</b>：event trigger（ddl_command_end + sql_drop）写 dbz_ddl_log，
 *       随 publication 被当普通表抓走。判定靠 PG 目录语义（按 xid 分组配对 sql_drop）：
 *       RENAME COLUMN 是 pg_attribute 原地改名<b>不触发 sql_drop</b>，DROP+ADD 必触发——
 *       "ALTER TABLE 且同事务无 sql_drop 且语句含 RENAME COLUMN" ⇒ 真 rename。</li>
 *   <li><b>MySQL 前端 {@link #applyRawMySql}</b>：binlog QueryEvent 提供 SQL 原文，迁移语义从
 *       SQL 解析，变更后定义从 information_schema 读取；源库无需审计表/触发器。</li>
 * </ul>
 * 其余跟随分工：PG 加列/类型由 RawPgReader 的 Relation 元数据在写入前自愈，MySQL 由
 * QueryEvent 后读取当前表定义对齐；删列默认跟随真删
 * （followDropColumn=false 时保留湖列留历史）；DROP TABLE 默认跟随真删湖表（followDropTable=false
 * 时保留）；
 * CREATE TABLE 即刻建湖空表（两源均连源库读当前定义，IF-NOT-EXISTS 幂等）；
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

    /** MySQL CHANGE [COLUMN] old new <type...>（old≠new 才是 rename，
     *  old=new 是纯类型变更，交给数据驱动跟随） */
    private static final Pattern MYSQL_CHANGE_COLUMN = Pattern.compile(
            "\\bCHANGE\\s+(?:COLUMN\\s+)?[\"`]?([\\w$]+)[\"`]?\\s+[\"`]?([\\w$]+)[\"`]?\\s",
            Pattern.CASE_INSENSITIVE);

    /** 表重命名：ALTER TABLE x RENAME [TO|AS] y（不含 RENAME COLUMN/RENAME INDEX/RENAME KEY） */
    private static final Pattern MYSQL_RENAME_TABLE_ALTER = Pattern.compile(
            "\\bRENAME\\s+(?:TO|AS)?\\s*(?!COLUMN\\b|INDEX\\b|KEY\\b)[\"`]?(?:[\\w$]+[\"`]?\\.[\"`]?)?([\\w$]+)[\"`]?",
            Pattern.CASE_INSENSITIVE);

    /** DROP DATABASE/SCHEMA db → 湖侧 DROP SCHEMA CASCADE（受 followDropTable 开关管） */
    private static final Pattern MYSQL_DROP_DATABASE = Pattern.compile(
            "^\\s*DROP\\s+(?:DATABASE|SCHEMA)\\s+(?:IF\\s+EXISTS\\s+)?[\"`]?([\\w$]+)[\"`]?",
            Pattern.CASE_INSENSITIVE);

    private static final String MYSQL_IDENT = "[\"`]?[\\w$]+[\"`]?";
    private static final String MYSQL_QUALIFIED = "(?:" + MYSQL_IDENT + "\\.)?" + MYSQL_IDENT;
    private static final Pattern MYSQL_ALTER_TABLE = Pattern.compile(
            "^\\s*ALTER\\s+TABLE\\s+(?:IF\\s+EXISTS\\s+)?(" + MYSQL_QUALIFIED + ")",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern MYSQL_CREATE_TABLE = Pattern.compile(
            "^\\s*CREATE\\s+(?:TEMPORARY\\s+)?TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?("
                    + MYSQL_QUALIFIED + ")", Pattern.CASE_INSENSITIVE);
    private static final Pattern MYSQL_DROP_TABLE = Pattern.compile(
            "^\\s*DROP\\s+(?:TEMPORARY\\s+)?TABLE\\s+(?:IF\\s+EXISTS\\s+)?(.+)$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern MYSQL_RENAME_PAIR = Pattern.compile(
            "(" + MYSQL_QUALIFIED + ")\\s+TO\\s+(" + MYSQL_QUALIFIED + ")",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern MYSQL_TRUNCATE_QUALIFIED = Pattern.compile(
            "^\\s*TRUNCATE\\s+(?:TABLE\\s+)?(" + MYSQL_QUALIFIED + ")",
            Pattern.CASE_INSENSITIVE);

    private static final Pattern PG_NUMERIC = Pattern.compile(
            "^(?:numeric|decimal)(?:\\((\\d+)(?:\\s*,\\s*(\\d+))?\\))?$",
            Pattern.CASE_INSENSITIVE);

    private final DucklakeProperties props;
    private final SyncState syncState;
    private final DuckLakeEngine engine;

    // ================================ PG 前端 ================================

    /**
     * 处理一段连续的 dbz_ddl_log 审计事件（由 RawPgReader 在湖事务内调用）。
     *
     * @param cacheInvalidator 湖表结构被本方法改动后回调，用于失效 reader 的列缓存
     * @param rebuildRequester scanner 无法完成重建时回调；原生 reader 会终止当前批，等待外部恢复
     */
    public void apply(Connection conn, List<Map<String, String>> run, Consumer<String> cacheInvalidator,
                      Consumer<String> rebuildRequester) throws SQLException {
        if (run.isEmpty()) {
            return;
        }
        syncState.getDdlAudited().increment(run.size());

        // 按源事务 xid 分组（同一条 ALTER 产生的 ddl_command_end 与 sql_drop 共享 xid）。
        // 已知边界:分组只在单批内做,同事务两行被批边界劈开时(截断事务中间,低概率)
        // hasDrop 误判为 false→删列跟随静默跳过(集成测试偶发复现);湖列多留无数据
        // 正确性影响,下一次同表 DDL 或人工 ALTER 可补齐,故不为此引入跨批配对缓存
        Map<Long, List<Map<String, String>>> byXid = new LinkedHashMap<>();
        for (Map<String, String> row : run) {
            byXid.computeIfAbsent(asLong(row, "xid"), k -> new ArrayList<>()).add(row);
        }

        for (Map.Entry<Long, List<Map<String, String>>> group : byXid.entrySet()) {
            // 镜像语义:源 DROP TABLE/DROP SCHEMA CASCADE → 湖表真删(时间旅行窗口内旧 snapshot
            // 仍可回看)。⚠️ DROP 命令不出现在 pg_event_trigger_ddl_commands() 里(PG 语义,
            // 集成测试实测踩坑)——审计流只有 sql_drop 行,跟随必须由 sql_drop 行自身触发,
            // 不能挂在下面的 ddl_command_end 循环
            if (props.getMaintenance().isFollowDropTable()) {
                applyDropTables(conn, group.getValue(), cacheInvalidator);
            }
            boolean hasDrop = group.getValue().stream()
                    .anyMatch(r -> "sql_drop".equals(str(r, "ev")));
            for (Map<String, String> row : group.getValue()) {
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
                    // 不再等首批数据；IF-NOT-EXISTS 使位点回放天然幂等；类型以映射表尽力对齐
                    // 事件推导口径,偏差由数据驱动 followTypeChange 自愈(空表重写零成本)
                    createTableFromPgSource(conn, str(row, "object_identity"), cacheInvalidator);
                }
                // 目标态一律连源库读(readPgTableDef)，主键变更与列序/加列共享一次读取。
                if ("ALTER TABLE".equals(tag)) {
                    String identity = str(row, "object_identity");
                    String lakeTable = identity == null ? null : lakeTableOf(identity);
                    TableDef def = lakeTable == null ? null : readPgTableDef(identity);
                    if (lakeTable != null && PRIMARY_KEY_CHANGE.matcher(query).find()) {
                        // 存量行主键回填无 CDC 事件,湖旧行主键恒 NULL → 重建+重灌当前态
                        // scanner 就绪则就地重建+直拉灌入；不可用时回调让 reader 明确失败。
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
     * 注释值从 query_text 的 IS 之后原样搬运。重复应用=同值覆盖，位点回放天然幂等。
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
     */
    private void applyDropTables(Connection conn, List<Map<String, String>> group,
                                 Consumer<String> cacheInvalidator) throws SQLException {
        for (Map<String, String> row : group) {
            if (!"sql_drop".equals(str(row, "ev")) || !"table".equals(str(row, "object_type"))) {
                continue;
            }
            String identity = str(row, "object_identity");
            String lakeTable = identity == null ? null : lakeTableOf(identity);
            if (lakeTable != null) {
                dropTableInLake(conn, lakeTable, cacheInvalidator);
            }
        }
    }

    /** rename 应用（列改名或表改名，存在性检查保证位点回放幂等）。
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
    private void applyDropColumns(Connection conn, List<Map<String, String>> group,
                                  Consumer<String> cacheInvalidator) throws SQLException {
        for (Map<String, String> row : group) {
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

    /**
     * RAW_MYSQL 的 DDL 前端。binlog QueryEvent 只带 SQL 原文，因此迁移语义（rename/drop/truncate）
     * 按原文解析；变更后的完整列定义从 information_schema 读取。若 reader 正在追历史积压，
     * 读到的是更靠后的当前态，但所有操作均幂等，随后行事件仍按 TableMap 自带的列名/类型解码，
     * 最终会收敛到源当前态。
     */
    public void applyRawMySql(Connection conn, String database, String ddl,
                              Consumer<String> cacheInvalidator,
                              Consumer<String> rebuildRequester) throws SQLException {
        if (ddl == null || ddl.isBlank()) {
            return;
        }
        syncState.getDdlAudited().increment();
        String sql = ddl.strip();

        Matcher dropDb = MYSQL_DROP_DATABASE.matcher(sql);
        if (dropDb.find()) {
            if (props.getMaintenance().isFollowDropTable()) {
                dropSchemaInLake(conn, dropDb.group(1), cacheInvalidator);
            }
            return;
        }

        Matcher truncate = MYSQL_TRUNCATE_QUALIFIED.matcher(sql);
        if (truncate.find()) {
            if (props.getMaintenance().isFollowTruncate()) {
                String identity = mysqlIdentity(truncate.group(1), database);
                if (identity != null) truncateInLake(conn, lakeTableOf(identity));
            }
            return;
        }

        if (sql.regionMatches(true, 0, "RENAME TABLE", 0, "RENAME TABLE".length())) {
            Matcher pairs = MYSQL_RENAME_PAIR.matcher(sql.substring("RENAME TABLE".length()));
            while (pairs.find()) {
                String fromIdentity = mysqlIdentity(pairs.group(1), database);
                String toIdentity = mysqlIdentity(pairs.group(2), database);
                if (fromIdentity == null || toIdentity == null) continue;
                String[] from = fromIdentity.split("\\.", 2);
                String[] to = toIdentity.split("\\.", 2);
                if (!from[0].equals(to[0])) {
                    log.warn("RAW_MYSQL 暂不跟随跨库 RENAME TABLE: {} -> {}", fromIdentity, toIdentity);
                    continue;
                }
                renameTableInLake(conn, lakeTableOf(fromIdentity), to[1], cacheInvalidator);
                refreshRawMySqlDefinition(conn, toIdentity, cacheInvalidator, rebuildRequester);
            }
            return;
        }

        Matcher dropTable = MYSQL_DROP_TABLE.matcher(sql);
        if (dropTable.find()) {
            if (props.getMaintenance().isFollowDropTable()) {
                String body = dropTable.group(1).replaceFirst("(?i)\\s+(?:RESTRICT|CASCADE)\\s*;?\\s*$", "");
                for (String token : body.split(",")) {
                    String identity = mysqlIdentity(token.strip(), database);
                    if (identity != null) dropTableInLake(conn, lakeTableOf(identity), cacheInvalidator);
                }
            }
            return;
        }

        Matcher create = MYSQL_CREATE_TABLE.matcher(sql);
        if (create.find()) {
            String identity = mysqlIdentity(create.group(1), database);
            if (identity != null) refreshRawMySqlDefinition(conn, identity, cacheInvalidator, rebuildRequester);
            return;
        }

        Matcher alter = MYSQL_ALTER_TABLE.matcher(sql);
        if (!alter.find()) {
            return; // BEGIN/COMMIT/GRANT/ANALYZE 等非表结构 QueryEvent
        }
        String identity = mysqlIdentity(alter.group(1), database);
        if (identity == null) return;
        String lakeTable = lakeTableOf(identity);

        // 先做保留历史值的真 rename，再以源目标态对齐列集合/顺序/类型。
        Matcher renameColumn = RENAME_COLUMN.matcher(sql);
        while (renameColumn.find()) {
            renameColumnInLake(conn, lakeTable, renameColumn.group(1), renameColumn.group(2), cacheInvalidator);
        }
        Matcher changeColumn = MYSQL_CHANGE_COLUMN.matcher(sql);
        while (changeColumn.find()) {
            if (!changeColumn.group(1).equalsIgnoreCase(changeColumn.group(2))) {
                renameColumnInLake(conn, lakeTable, changeColumn.group(1), changeColumn.group(2), cacheInvalidator);
            }
        }

        Matcher renameTable = MYSQL_RENAME_TABLE_ALTER.matcher(sql);
        if (renameTable.find() && !RENAME_COLUMN.matcher(sql).find()) {
            String newName = renameTable.group(1);
            renameTableInLake(conn, lakeTable, newName, cacheInvalidator);
            identity = identity.substring(0, identity.indexOf('.') + 1) + newName;
            lakeTable = lakeTableOf(identity);
        }

        TableDef def = readMysqlTableDef(identity);
        if (PRIMARY_KEY_CHANGE.matcher(sql).find()) {
            rebuildOrRefill(conn, lakeTable, def, readMysqlTableComment(identity),
                    cacheInvalidator, rebuildRequester);
        } else {
            refreshRawMySqlDefinition(conn, identity, cacheInvalidator, rebuildRequester);
        }
    }

    /** RAW_MYSQL TableMap 补足 bool/binary 等仅靠物理类型无法区分的列型。 */
    public Map<String, String> mysqlColumnTypes(String database, String table) {
        TableDef def = readMysqlTableDef(database + "." + table);
        return def == null ? Map.of() : new LinkedHashMap<>(def.cols());
    }

    private void refreshRawMySqlDefinition(Connection conn, String identity,
                                           Consumer<String> cacheInvalidator,
                                           Consumer<String> rebuildRequester) throws SQLException {
        TableDef def = readMysqlTableDef(identity);
        if (def == null || def.cols().isEmpty()) return;
        String lakeTable = lakeTableOf(identity);
        String tableComment = readMysqlTableComment(identity);
        if (!tableExists(conn, lakeTable)) {
            createTableInLake(conn, lakeTable, def.cols(), tableComment, def.colComments(), def.pk(), cacheInvalidator);
            return;
        }

        // raw 行事件虽能自愈加列，但类型漂移必须在写新值前处理。DDL 是低频操作，发现任一
        // 类型变化时直接重建+scanner 当前态重灌，避免在失败的 DuckLake ALTER 事务里继续写。
        Map<String, String> lakeTypes = loadLakeColumnTypes(conn, lakeTable);
        boolean typeChanged = def.cols().entrySet().stream().anyMatch(e -> {
            String have = lakeTypes.get(e.getKey());
            return have != null && !have.equals(DuckType.normalize(e.getValue()));
        });
        if (typeChanged && props.getMaintenance().isFollowTypeChange()) {
            rebuildOrRefill(conn, lakeTable, def, tableComment, cacheInvalidator, rebuildRequester);
            return;
        }
        syncColumnsFromDefinition(conn, lakeTable, def.cols(), def.colComments(), def.pk(),
                cacheInvalidator, rebuildRequester);
        applyRawMySqlComments(conn, lakeTable, tableComment, def.colComments());
    }

    private Map<String, String> loadLakeColumnTypes(Connection conn, String lakeTable) throws SQLException {
        String[] parts = lakeTable.split("\\.", 2);
        Map<String, String> out = new LinkedHashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT column_name, data_type FROM information_schema.columns "
                        + "WHERE table_catalog=? AND table_schema=? AND table_name=? ORDER BY ordinal_position")) {
            ps.setString(1, DuckLakeEngine.LAKE);
            ps.setString(2, parts[0]);
            ps.setString(3, parts[1]);
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.put(rs.getString(1), DuckType.normalize(rs.getString(2)));
                }
            }
        }
        return out;
    }

    private void applyRawMySqlComments(Connection conn, String lakeTable, String tableComment,
                                       Map<String, String> colComments) {
        if (tableComment != null) {
            commentInLake(conn, "TABLE " + DuckLakeEngine.LAKE + "." + DuckLakeEngine.quoted(lakeTable),
                    quoteLiteral(tableComment));
        }
        for (Map.Entry<String, String> c : colComments.entrySet()) {
            commentInLake(conn, "COLUMN " + DuckLakeEngine.LAKE + "." + DuckLakeEngine.quoted(lakeTable)
                    + ".\"" + c.getKey() + "\"", quoteLiteral(c.getValue()));
        }
    }

    private static String mysqlIdentity(String token, String defaultDatabase) {
        if (token == null) return null;
        // MySQL binlog 会把 DROP TABLE 改写为
        // DROP TABLE `db`.`t` /* generated by server */，服务端注释不属于标识符。
        String cleaned = token.replaceAll("(?s)/\\*.*?\\*/", "").strip()
                .replace("`", "").replace("\"", "")
                .replaceFirst(";\\s*$", "");
        if (!cleaned.matches("[\\w$]+(?:\\.[\\w$]+)?")) return null;
        return cleaned.contains(".") ? cleaned
                : (defaultDatabase == null || defaultDatabase.isBlank() ? null : defaultDatabase + "." + cleaned);
    }

    // ============================ DDL 驱动建空表（两源前端 + 类型映射） ============================

    /** 源表定义（列名→duck 类型按源列序、列注释、主键列） */
    private record TableDef(Map<String, String> cols, Map<String, String> colComments, List<String> pk) {
    }

    /** [PG] 连源库读 pg_catalog 的表定义（表已删/连接瞬断返回 null，调用方跳过）。 */
    private TableDef readPgTableDef(String identity) {
        String[] parts = identity.split("\\.");
        DucklakeProperties.Source src = props.getSource();
        Map<String, String> cols = new LinkedHashMap<>();
        List<String> pk = new ArrayList<>();
        try (Connection c = java.sql.DriverManager.getConnection(src.jdbcUrl(), src.getUser(), src.getPassword())) {
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT a.attname, "
                            + "CASE "
                            + "WHEN t.typelem <> 0 THEN CASE WHEN et.typbasetype <> 0 "
                            + "THEN pg_catalog.format_type(et.typbasetype, et.typtypmod) "
                            + "ELSE pg_catalog.format_type(t.typelem, a.atttypmod) END "
                            + "WHEN t.typbasetype <> 0 "
                            + "THEN pg_catalog.format_type(t.typbasetype, t.typtypmod) "
                            + "ELSE pg_catalog.format_type(a.atttypid, a.atttypmod) END AS element_type, "
                            + "a.attndims "
                            + "FROM pg_catalog.pg_attribute a "
                            + "JOIN pg_catalog.pg_class c ON c.oid=a.attrelid "
                            + "JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
                            + "JOIN pg_catalog.pg_type t ON t.oid=a.atttypid "
                            + "LEFT JOIN pg_catalog.pg_type et ON et.oid=t.typelem "
                            + "WHERE n.nspname=? AND c.relname=? AND a.attnum>0 AND NOT a.attisdropped "
                            + "AND c.relkind IN ('r','p','v','m','f') ORDER BY a.attnum")) {
                ps.setString(1, parts[0]);
                ps.setString(2, parts[1]);
                try (var rs = ps.executeQuery()) {
                    while (rs.next()) {
                        cols.put(rs.getString(1), pgTypeToDuck(rs.getString(2), rs.getInt(3)));
                    }
                }
            }
            // 主键列(排序聚簇/bootstrap anti-join 用)。⚠️ 必须走 pg_catalog:
            // information_schema 的约束视图对"仅有 SELECT 权限的非 owner"不可见
            // (CDC 账号正是这个形态,服务器实测约束行直接消失,曾致主键静默判空)
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT a.attname FROM pg_index i "
                            + "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
                            + "WHERE i.indrelid = format('%I.%I', ?::text, ?::text)::regclass AND i.indisprimary "
                            + "ORDER BY array_position(i.indkey, a.attnum)")) {
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

    /**
     * [RawPgReader] 读取 PG 源表列定义（列名 → DuckDB 类型，按列序）。
     * 失败返回空 Map（调用方降级全列 VARCHAR）。
     */
    public Map<String, String> pgColumnTypes(String schema, String table) {
        TableDef def = readPgTableDef(schema + "." + table);
        return def != null ? new LinkedHashMap<>(def.cols()) : Map.of();
    }

    /**
     * [RawPgReader] 处理来自 raw-pg 流的 DDL 审计行（Map 形态，与 PG 前端 {@link #apply} 同语义）。
     * key 集合：ev / tag / object_type / object_identity / query_text / xid（均可缺失）。
     */
    public void applyRaw(Connection conn, List<Map<String, String>> rows,
                         java.util.function.Consumer<String> cacheInvalidator,
                         java.util.function.Consumer<String> rebuildRequester) throws SQLException {
        apply(conn, rows, cacheInvalidator, rebuildRequester);
    }

    /** [RawPgReader] 应用 pgoutput Truncate 消息；表尚未落湖时幂等跳过。 */
    public void applyRawPgTruncate(Connection conn, String lakeTable) throws SQLException {
        if (props.getMaintenance().isFollowTruncate()) {
            truncateInLake(conn, lakeTable);
        }
    }

    /**
     * [bootstrap] 首次接入的单表落地：连源库读目标态 → 建湖表（不存在时，含注释/排序聚簇）
     * → scanner 直拉存量（按主键 anti-join：no_data 模式下流式已并行开跑，先落的增量行
     * 更新、不被 scanner 存量读覆盖）。返回灌入行数；<b>-1 = 表已建但无主键/scanner 够不到</b>。
     */
    public int bootstrapTable(Connection conn, String lakeTable, String srcIdentity) throws SQLException {
        TableDef def = switch (props.getSource().getType()) {
            case POSTGRES -> readPgTableDef(srcIdentity);
            case MYSQL -> readMysqlTableDef(srcIdentity);
        };
        if (def == null || def.cols().isEmpty()) {
            throw new SQLException("源表定义不可读: " + srcIdentity);
        }
        String tableComment = props.getSource().getType() == DucklakeProperties.SourceType.MYSQL
                ? readMysqlTableComment(srcIdentity) : null;
        createTableInLake(conn, lakeTable, def.cols(), tableComment, def.colComments(), def.pk(), t -> { });
        DuckLakeEngine.ScannerSource source = scannerSourceOf(lakeTable);
        if (def.pk().isEmpty() || source == null) {
            return -1;
        }
        return refillFromScanner(conn, lakeTable, def.cols(), source, def.pk());
    }

    /** [MySQL] 连源库读 information_schema 的表定义（bootstrap/DDL 对齐共用；
     *  typeName 拼为 "<DATA_TYPE> UNSIGNED"，TINYINT 显示宽度取 column_type）。 */
    private TableDef readMysqlTableDef(String identity) {
        String[] parts = identity.split("\\.");
        DucklakeProperties.Source src = props.getSource();
        Map<String, String> cols = new LinkedHashMap<>();
        Map<String, String> comments = new LinkedHashMap<>();
        List<String> pk = new ArrayList<>();
        try (Connection c = java.sql.DriverManager.getConnection(src.jdbcUrl(), src.getUser(), src.getPassword())) {
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT column_name, data_type, column_type, numeric_precision, numeric_scale, column_comment "
                            + "FROM information_schema.columns WHERE table_schema=? AND table_name=? "
                            + "ORDER BY ordinal_position")) {
                ps.setString(1, parts[0]);
                ps.setString(2, parts[1]);
                try (var rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String name = rs.getString(1);
                        String columnType = rs.getString(3) == null ? "" : rs.getString(3);
                        String typeName = rs.getString(2).toUpperCase()
                                + (columnType.toLowerCase().contains("unsigned") ? " UNSIGNED" : "");
                        // 显示宽度/精度:TINYINT(1)→BOOLEAN 判定要 column_type 的括号数,DECIMAL 走 precision
                        Long length = firstParenNumber(columnType);
                        if (length == null && rs.getObject(4) != null) {
                            length = ((Number) rs.getObject(4)).longValue();
                        }
                        Long scale = rs.getObject(5) == null ? null : ((Number) rs.getObject(5)).longValue();
                        cols.put(name, mysqlTypeToDuck(typeName, length, scale));
                        String comment = rs.getString(6);
                        if (comment != null && !comment.isBlank()) {
                            comments.put(name, comment);
                        }
                    }
                }
            }
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT column_name FROM information_schema.key_column_usage "
                            + "WHERE constraint_name='PRIMARY' AND table_schema=? AND table_name=? "
                            + "ORDER BY ordinal_position")) {
                ps.setString(1, parts[0]);
                ps.setString(2, parts[1]);
                try (var rs = ps.executeQuery()) {
                    while (rs.next()) {
                        pk.add(rs.getString(1));
                    }
                }
            }
        } catch (SQLException e) {
            log.warn("读源表定义失败(bootstrap 保留待重试): {} ({})", identity, e.getMessage());
            return null;
        }
        return new TableDef(cols, comments, pk);
    }

    /** [MySQL] 表注释（bootstrap 建表带走；无注释/失败返回 null） */
    private String readMysqlTableComment(String identity) {
        String[] parts = identity.split("\\.");
        DucklakeProperties.Source src = props.getSource();
        try (Connection c = java.sql.DriverManager.getConnection(src.jdbcUrl(), src.getUser(), src.getPassword());
             PreparedStatement ps = c.prepareStatement(
                     "SELECT table_comment FROM information_schema.tables WHERE table_schema=? AND table_name=?")) {
            ps.setString(1, parts[0]);
            ps.setString(2, parts[1]);
            try (var rs = ps.executeQuery()) {
                String comment = rs.next() ? rs.getString(1) : null;
                return comment == null || comment.isBlank() ? null : comment;
            }
        } catch (SQLException e) {
            return null;
        }
    }

    /** column_type 里第一个括号数字（"tinyint(1)"→1、"decimal(12,2)"→12）；无括号返回 null */
    private static Long firstParenNumber(String columnType) {
        Matcher m = Pattern.compile("\\((\\d+)").matcher(columnType);
        return m.find() ? Long.parseLong(m.group(1)) : null;
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

    /**
     * PG {@code format_type} 的元素类型 + {@code pg_attribute.attndims} → DuckDB 列型。
     * 数组列的 atttypmod 保留 numeric 精度，attndims 保留声明维度；域先在查询中解到基类型。
     * 复合/enum 等未知元素继续整列 VARCHAR，避免把无法可靠解析的 PG 文本静默 TRY_CAST 为 NULL。
     */
    String pgTypeToDuck(String formattedElementType, int declaredArrayDimensions) {
        String pgType = formattedElementType == null ? "" : formattedElementType.strip().toLowerCase();
        int dimensions = Math.max(0, declaredArrayDimensions);
        while (pgType.endsWith("[]")) {
            dimensions++;
            pgType = pgType.substring(0, pgType.length() - 2).stripTrailing();
        }

        String duckType = pgScalarTypeToDuck(pgType);
        if (duckType == null || (dimensions > 0 && !pgArrayElementSupported(pgType))) {
            return "VARCHAR";
        }
        return duckType + "[]".repeat(dimensions);
    }

    private String pgScalarTypeToDuck(String pgType) {
        Matcher numeric = PG_NUMERIC.matcher(pgType);
        if (numeric.matches()) {
            if (numeric.group(1) == null) {
                return "DECIMAL(38,18)";
            }
            int precision = Integer.parseInt(numeric.group(1));
            int scale = numeric.group(2) == null ? 0 : Integer.parseInt(numeric.group(2));
            return precision < 1 || precision > 38 || scale < 0 || scale > precision
                    ? null : "DECIMAL(" + precision + "," + scale + ")";
        }

        String unqualified = pgTypeNameWithoutModifiers(pgType);
        return switch (unqualified) {
            case "bigint", "int8" -> "BIGINT";
            case "integer", "int", "int4" -> "INTEGER";
            case "smallint", "int2" -> "SMALLINT";
            case "real", "float4" -> "FLOAT";
            case "double precision", "float8" -> "DOUBLE";
            case "boolean", "bool" -> "BOOLEAN";
            case "timestamp without time zone", "timestamp" -> "TIMESTAMP";
            case "timestamp with time zone", "timestamptz" -> "TIMESTAMPTZ";
            case "date" -> "DATE";
            case "time without time zone", "time" -> "TIME";
            case "time with time zone", "timetz" -> "TIMETZ";
            case "text", "character varying", "varchar", "character", "char", "bpchar", "name" -> "VARCHAR";
            case "bytea" -> "BLOB";
            case "json", "jsonb" -> props.getMaintenance().isJsonAsVariant() ? "VARIANT" : "JSON";
            case "uuid" -> "UUID";
            case "interval" -> "INTERVAL";
            default -> null;
        };
    }

    private static String pgTypeNameWithoutModifiers(String pgType) {
        return pgType.replaceFirst("^pg_catalog\\.", "")
                .replaceFirst("^(timestamp|time)\\(\\d+\\)", "$1")
                .replaceFirst("^(character varying|varchar|character|char)\\(\\d+\\)$", "$1");
    }

    /** 与现有 PG 数组文本转换兼容的元素类型；JSON/bytea/time/interval 数组仍保留原文。 */
    private boolean pgArrayElementSupported(String pgType) {
        if (PG_NUMERIC.matcher(pgType).matches()) {
            return pgScalarTypeToDuck(pgType) != null;
        }
        return switch (pgTypeNameWithoutModifiers(pgType)) {
            case "bigint", "int8", "integer", "int", "int4", "smallint", "int2",
                 "real", "float4", "double precision", "float8", "boolean", "bool",
                 "timestamp without time zone", "timestamp", "timestamp with time zone", "timestamptz",
                 "date", "text", "character varying", "varchar", "character", "char", "bpchar", "name",
                 "uuid" -> true;
            default -> false;
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
            // BIGINT UNSIGNED 直接按无符号十进制文本写入 DuckDB UBIGINT。
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
     * ④ 重排失败 → scanner 重建直灌；scanner 不可用时由 rebuildRequester 终止当前批。
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
            // ④ 重排失败(类型冲突等):重建+重灌兜底。湖事务可能已因失败语句 abort，
            // rebuildOrRefill 若也失败会交由 reader 回滚并终止当前批。
            log.warn("湖表列序重排失败,转重建+重灌: {} ({})", lakeTable, e.getMessage());
            rebuildOrRefill(conn, lakeTable, new TableDef(targetCols, colComments, keyColumns), null,
                    cacheInvalidator, rebuildRequester);
        }
    }

    /**
     * 湖表重建 + 当前态重灌，二选一路径：scanner 通道就绪且目标态定义可得 → 就地 DROP 重建
     * （目标态列序/注释/排序聚簇）+ scanner 直拉全量灌入（新表空灌免去重；与 DDL 段同湖事务，
     * 原子可见；服务器实测 100 万行 PG 1.2s / MySQL 4.0s，且不停流）。源定义不可读、
     * scanner 不可用或直灌失败时调用 rebuildRequester，使原生 reader 回滚并明确失败，避免静默丢失。
     */
    private void rebuildOrRefill(Connection conn, String lakeTable, TableDef def, String tableComment,
                                 Consumer<String> cacheInvalidator, Consumer<String> rebuildRequester) {
        DuckLakeEngine.ScannerSource source = scannerSourceOf(lakeTable);
        if (def == null || def.cols().isEmpty() || source == null) {
            rebuildRequester.accept(lakeTable);
            return;
        }
        try {
            // mysql extension 的远端 schema 在 ATTACH 后会缓存；DDL rename/type/add 后先清缓存，
            // 否则下方 scanner SELECT 可能仍绑定旧列名（例如 amount 已改为 price）。
            engine.refreshScannerMetadata(conn);
            try (Statement s = conn.createStatement()) {
                s.execute("DROP TABLE IF EXISTS " + DuckLakeEngine.LAKE + "." + DuckLakeEngine.quoted(lakeTable));
            }
            createTableInLake(conn, lakeTable, def.cols(), tableComment, def.colComments(), def.pk(),
                    cacheInvalidator);
            int n = refillFromScanner(conn, lakeTable, def.cols(), source, List.of());
            log.warn("湖表重建+scanner 直灌完成: {} ({} 行)", lakeTable, n);
        } catch (SQLException e) {
            log.warn("scanner 重建直灌失败，当前批交由 reader 回滚: {} ({})", lakeTable, e.getMessage());
            rebuildRequester.accept(lakeTable);
        }
    }

    /** 湖表名反推 scanner 源描述（剥 schema 前缀）；通道未就绪/MySQL 跨库时返回 null。 */
    private DuckLakeEngine.ScannerSource scannerSourceOf(String lakeTable) {
        String prefix = props.getMaintenance().getSchemaPrefix();
        String srcTable = lakeTable.startsWith(prefix) ? lakeTable.substring(prefix.length()) : lakeTable;
        int dot = srcTable.indexOf('.');
        if (dot <= 0) {
            return null;
        }
        return engine.scannerSource(srcTable.substring(0, dot), srcTable.substring(dot + 1));
    }

    /**
     * scanner 直拉重灌原语：INSERT INTO 湖表 SELECT 按湖列投影（CAST 对齐列型）FROM 源表。
     * 源列名=湖列名（rename 已镜像跟随）；antiJoinKeys 非空时按主键 anti-join 跳过湖里已有行
     * （类型重建路径：重建后当批数据先落湖，补灌历史需去重；空表直灌传空列表零开销）。
     * 已知编码差异：GEOMETRY/point 列 scanner 给源文本形态而非流式路径的 WKB（见 README 局限）。
     */
    public int refillFromScanner(Connection conn, String lakeTable, Map<String, String> lakeCols,
                                 DuckLakeEngine.ScannerSource source,
                                 List<String> antiJoinKeys) throws SQLException {
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
                .append(" FROM ").append(source.fromSql()).append(" s");
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
        try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            source.bind(ps);
            n = ps.executeUpdate();
        }
        syncState.getDdlApplied().increment();
        log.info("scanner 直灌 {}: {} 行 {}ms (源 {})", lakeTable, n, System.currentTimeMillis() - t0, source);
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

    /** 湖表 rename 列：列存在性双检保证重复应用安全跳过。 */
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
                "SELECT 1 FROM information_schema.columns "
                        + "WHERE table_catalog=? AND table_schema=? AND table_name=? AND column_name=?")) {
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
                "SELECT 1 FROM information_schema.tables "
                        + "WHERE table_catalog=? AND table_schema=? AND table_name=?")) {
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

    private static String str(Map<String, String> row, String field) {
        return row.get(field);
    }

    private static Long asLong(Map<String, String> row, String field) {
        String value = row.get(field);
        return value == null ? null : Long.parseLong(value);
    }
}
