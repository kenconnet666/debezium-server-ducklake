package org.dpdns.zerodep.ducklake.engine;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * raw-pg 引擎的 LSN offset 持久化（catalog PG）。
 * <p>
 * 独立建表 {@code raw_pg_offset}，不复用 Debezium 的 {@code debezium_offset_storage}
 * JSON 格式——免解析复杂度，schema 清晰。
 * 同一 slotName 的 Debezium offset 与 raw-pg offset 天然隔离（不同表）。
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
            log.warn("RawPg 读取 offset 失败，从头消费: {}", e.getMessage());
        }
        return 0L;
    }

    /** 落湖提交后异步回写 LSN（失败仅告警，下次重启从上个已记录位点续传）。 */
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
            log.warn("RawPg 写入 offset 失败（下次重启从旧位点续传）: slot={} lsn={} err={}",
                    slotName, Long.toHexString(lsn), e.getMessage());
        }
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, user, password);
    }
}
