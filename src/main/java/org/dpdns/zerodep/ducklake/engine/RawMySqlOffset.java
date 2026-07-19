package org.dpdns.zerodep.ducklake.engine;

import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/** RAW_MYSQL 的事务提交位点（catalog PG）。文件位点用于单机续传，GTID 用于切主续传。 */
@Slf4j
class RawMySqlOffset implements AutoCloseable {

    record Position(String filename, long position, String gtidSet) {
        Position {
            gtidSet = gtidSet == null ? "" : gtidSet;
        }
    }

    private static final String LOAD_SQL = """
            SELECT binlog_filename, binlog_position, gtid_set
            FROM raw_mysql_offset WHERE source_name=?
            """;
    private static final String INITIALIZE_SQL = """
            INSERT INTO raw_mysql_offset(source_name, binlog_filename, binlog_position, gtid_set)
            VALUES (?, ?, ?, ?) ON CONFLICT (source_name) DO NOTHING
            """;
    private static final String SAVE_SQL = """
            INSERT INTO raw_mysql_offset(source_name, binlog_filename, binlog_position, gtid_set)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (source_name) DO UPDATE SET
                binlog_filename=EXCLUDED.binlog_filename,
                binlog_position=EXCLUDED.binlog_position,
                gtid_set=EXCLUDED.gtid_set,
                updated_at=now()
            """;

    private final CatalogJdbcSession catalog;

    RawMySqlOffset(String jdbcUrl, String user, String password) {
        this.catalog = new CatalogJdbcSession(jdbcUrl, user, password);
        try {
            ensureTable();
        } catch (RuntimeException | Error error) {
            catalog.close();
            throw error;
        }
    }

    private void ensureTable() {
        try {
            catalog.withConnection(connection -> {
                try (Statement statement = connection.createStatement()) {
                    statement.execute("""
                            CREATE TABLE IF NOT EXISTS raw_mysql_offset (
                                source_name      VARCHAR PRIMARY KEY,
                                binlog_filename  VARCHAR NOT NULL,
                                binlog_position  BIGINT NOT NULL,
                                gtid_set         TEXT NOT NULL DEFAULT '',
                                updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
                            )""");
                }
                return null;
            });
        } catch (SQLException e) {
            throw new IllegalStateException("raw_mysql_offset 初始化失败: " + e.getMessage(), e);
        }
    }

    Position load(String sourceName) {
        try {
            return catalog.withPrepared(LOAD_SQL, statement -> {
                statement.setString(1, sourceName);
                try (ResultSet result = statement.executeQuery()) {
                    if (result.next()) {
                        Position position = new Position(
                                result.getString(1), result.getLong(2), result.getString(3));
                        log.info("RawMySQL 续传 offset: source={} {}:{} gtid={}", sourceName,
                                position.filename(), position.position(),
                                position.gtidSet().isBlank() ? "<disabled>" : "present");
                        return position;
                    }
                }
                return null;
            });
        } catch (SQLException e) {
            throw new IllegalStateException("RawMySQL 读取 offset 失败: " + e.getMessage(), e);
        }
    }

    /** 首次起点必须先落盘再接流，防止进程在首批提交前退出后从新 HEAD 启动而跳过事件。 */
    void initialize(String sourceName, Position p) {
        saveInternal(sourceName, p, true);
    }

    /** 仅在对应源事务已成功落湖后调用；失败抛出，让进程退出并从旧位点幂等重放。 */
    void save(String sourceName, Position p) {
        saveInternal(sourceName, p, false);
    }

    private void saveInternal(String sourceName, Position p, boolean onlyIfAbsent) {
        String sql = onlyIfAbsent ? INITIALIZE_SQL : SAVE_SQL;
        try {
            catalog.withPrepared(sql, statement -> {
                statement.setString(1, sourceName);
                statement.setString(2, p.filename());
                statement.setLong(3, p.position());
                statement.setString(4, p.gtidSet());
                statement.executeUpdate();
                return null;
            });
        } catch (SQLException e) {
            throw new IllegalStateException("RawMySQL 写入 offset 失败: source=" + sourceName
                    + " position=" + p.filename() + ':' + p.position(), e);
        }
    }

    @Override
    public void close() {
        catalog.close();
    }
}
