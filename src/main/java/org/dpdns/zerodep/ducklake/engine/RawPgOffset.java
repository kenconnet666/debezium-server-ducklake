package org.dpdns.zerodep.ducklake.engine;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * PostgreSQL 原生 reader 的 LSN offset 持久化（catalog PG）。
 * <p>
 * 表 {@code raw_pg_offset} 以 slotName 为键，只在对应湖事务提交成功后推进。
 */
@Slf4j
class RawPgOffset {

    private static final String TABLE = "raw_pg_offset";

    private final String jdbcUrl;
    private final String user;
    private final String password;

    RawPgOffset(String jdbcUrl, String user, String password) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
        ensureTable();
    }

    private void ensureTable() {
        try (Connection c = connect(); Statement s = c.createStatement()) {
            s.execute("""
                    CREATE TABLE IF NOT EXISTS raw_pg_offset (
                        slot_name   VARCHAR PRIMARY KEY,
                        confirmed_lsn BIGINT  NOT NULL DEFAULT 0,
                        updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
                    )""");
        } catch (SQLException e) {
            throw new IllegalStateException("raw_pg_offset 初始化失败: " + e.getMessage(), e);
        }
    }

    /** 读取 slot 上次确认的 LSN；首次返回 0（从 slot 当前位置开始）。 */
    long load(String slotName) {
        try (Connection c = connect();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT confirmed_lsn FROM " + TABLE + " WHERE slot_name = ?")) {
            ps.setString(1, slotName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    long lsn = rs.getLong(1);
                    log.info("RawPg 续传 offset: slot={} lsn={}/{}", slotName,
                            lsn, Long.toHexString(lsn));
                    return lsn;
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("RawPg 读取 offset 失败，拒绝猜测起点: slot=" + slotName, e);
        }
        return 0L;
    }

    /** 落湖提交后同步回写 LSN；失败必须终止 reader，绝不能继续向 replication slot 确认。 */
    void save(String slotName, long lsn) {
        try (Connection c = connect();
             PreparedStatement ps = c.prepareStatement("""
                     INSERT INTO raw_pg_offset(slot_name, confirmed_lsn)
                     VALUES (?, ?)
                     ON CONFLICT (slot_name) DO UPDATE
                         SET confirmed_lsn = EXCLUDED.confirmed_lsn,
                             updated_at    = now()
                     """)) {
            ps.setString(1, slotName);
            ps.setLong(2, lsn);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("RawPg 写入 offset 失败: slot=" + slotName
                    + " lsn=" + Long.toHexString(lsn), e);
        }
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, user, password);
    }
}
