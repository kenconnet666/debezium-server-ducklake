package org.dpdns.zerodep.ducklake.engine;

import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * PostgreSQL 原生 reader 的 LSN offset 持久化（catalog PG）。
 * <p>
 * 表 {@code raw_pg_offset} 以 slotName 为键，只在对应湖事务提交成功后推进。
 */
@Slf4j
class RawPgOffset implements AutoCloseable {

    private static final String TABLE = "raw_pg_offset";
    private static final String LOAD_SQL = "SELECT confirmed_lsn FROM " + TABLE + " WHERE slot_name = ?";
    private static final String SAVE_SQL = """
            INSERT INTO raw_pg_offset(slot_name, confirmed_lsn)
            VALUES (?, ?)
            ON CONFLICT (slot_name) DO UPDATE
                SET confirmed_lsn = EXCLUDED.confirmed_lsn,
                    updated_at    = now()
            """;

    private final CatalogJdbcSession catalog;

    RawPgOffset(String jdbcUrl, String user, String password) {
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
                            CREATE TABLE IF NOT EXISTS raw_pg_offset (
                                slot_name   VARCHAR PRIMARY KEY,
                                confirmed_lsn BIGINT  NOT NULL DEFAULT 0,
                                updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
                            )""");
                }
                return null;
            });
        } catch (SQLException e) {
            throw new IllegalStateException("raw_pg_offset 初始化失败: " + e.getMessage(), e);
        }
    }

    /** 读取 slot 上次确认的 LSN；首次返回 0（从 slot 当前位置开始）。 */
    long load(String slotName) {
        try {
            return catalog.withPrepared(LOAD_SQL, statement -> {
                statement.setString(1, slotName);
                try (ResultSet result = statement.executeQuery()) {
                    if (result.next()) {
                        long lsn = result.getLong(1);
                        log.info("RawPg 续传 offset: slot={} lsn={}/{}", slotName,
                                lsn, Long.toHexString(lsn));
                        return lsn;
                    }
                }
                return 0L;
            });
        } catch (SQLException e) {
            throw new IllegalStateException("RawPg 读取 offset 失败，拒绝猜测起点: slot=" + slotName, e);
        }
    }

    /** 落湖提交后同步回写 LSN；失败必须终止 reader，绝不能继续向 replication slot 确认。 */
    void save(String slotName, long lsn) {
        try {
            catalog.withPrepared(SAVE_SQL, statement -> {
                statement.setString(1, slotName);
                statement.setLong(2, lsn);
                statement.executeUpdate();
                return null;
            });
        } catch (SQLException e) {
            throw new IllegalStateException("RawPg 写入 offset 失败: slot=" + slotName
                    + " lsn=" + Long.toHexString(lsn), e);
        }
    }

    @Override
    public void close() {
        catalog.close();
    }
}
