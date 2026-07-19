package org.dpdns.zerodep.ducklake.engine;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/** RAW_MYSQL 的事务提交位点（catalog PG）。文件位点用于单机续传，GTID 用于切主续传。 */
@Slf4j
class RawMySqlOffset {

    record Position(String filename, long position, String gtidSet) {
        Position {
            gtidSet = gtidSet == null ? "" : gtidSet;
        }
    }

    private final String jdbcUrl;
    private final String user;
    private final String password;

    RawMySqlOffset(String jdbcUrl, String user, String password) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
        ensureTable();
    }

    private void ensureTable() {
        try (Connection c = connect(); Statement s = c.createStatement()) {
            s.execute("""
                    CREATE TABLE IF NOT EXISTS raw_mysql_offset (
                        source_name      VARCHAR PRIMARY KEY,
                        binlog_filename  VARCHAR NOT NULL,
                        binlog_position  BIGINT NOT NULL,
                        gtid_set         TEXT NOT NULL DEFAULT '',
                        updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
                    )""");
        } catch (SQLException e) {
            throw new IllegalStateException("raw_mysql_offset 初始化失败: " + e.getMessage(), e);
        }
    }

    Position load(String sourceName) {
        try (Connection c = connect();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT binlog_filename, binlog_position, gtid_set FROM raw_mysql_offset WHERE source_name=?")) {
            ps.setString(1, sourceName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    Position p = new Position(rs.getString(1), rs.getLong(2), rs.getString(3));
                    log.info("RawMySQL 续传 offset: source={} {}:{} gtid={}", sourceName,
                            p.filename(), p.position(), p.gtidSet().isBlank() ? "<disabled>" : "present");
                    return p;
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("RawMySQL 读取 offset 失败: " + e.getMessage(), e);
        }
        return null;
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
        String sql = onlyIfAbsent ? """
                INSERT INTO raw_mysql_offset(source_name, binlog_filename, binlog_position, gtid_set)
                VALUES (?, ?, ?, ?) ON CONFLICT (source_name) DO NOTHING
                """ : """
                INSERT INTO raw_mysql_offset(source_name, binlog_filename, binlog_position, gtid_set)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (source_name) DO UPDATE SET
                    binlog_filename=EXCLUDED.binlog_filename,
                    binlog_position=EXCLUDED.binlog_position,
                    gtid_set=EXCLUDED.gtid_set,
                    updated_at=now()
                """;
        try (Connection c = connect(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, sourceName);
            ps.setString(2, p.filename());
            ps.setLong(3, p.position());
            ps.setString(4, p.gtidSet());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("RawMySQL 写入 offset 失败: source=" + sourceName
                    + " position=" + p.filename() + ':' + p.position(), e);
        }
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, user, password);
    }
}
