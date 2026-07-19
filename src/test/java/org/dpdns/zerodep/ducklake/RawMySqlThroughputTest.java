package org.dpdns.zerodep.ducklake;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import org.duckdb.DuckDBAppender;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mysql.MySQLContainer;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * MySQL binlog 协议客户端吞吐上限：直接使用 {@code mysql-binlog-connector-java}，
 * 不进入生产 sink，用于区分协议解码上限与湖写入成本。
 * <p>
 * 先记录 binlog 位点，再制造积压，最后从该位点启动客户端；分别测纯反序列化计数，
 * 以及反序列化后直接写全 VARCHAR DuckDB Appender staging。计时不包含源端 INSERT，
 * 口径对应 RAW_PG 调研里的“原始串/行事件直读”和“直通 staging”两层上限。
 */
@Testcontainers(disabledWithoutDocker = true)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RawMySqlThroughputTest {

    static final int ROWS = Integer.getInteger("mysql.raw.rows", 200_000);

    @Container
    static final MySQLContainer MYSQL = new MySQLContainer("mysql:8.4")
            .withDatabaseName("bench").withUsername("test").withPassword("test");

    @BeforeAll
    static void provision() throws Exception {
        try (Connection c = rootConnection(); Statement s = c.createStatement()) {
            s.execute("CREATE USER 'dbuser_cdc'@'%' IDENTIFIED BY 'test'");
            s.execute("GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, " +
                    "REPLICATION CLIENT ON *.* TO 'dbuser_cdc'@'%'");
            s.execute("CREATE TABLE bench.stream " +
                    "(id bigint PRIMARY KEY, name varchar(32), val decimal(12,2))");
            s.execute("CREATE TABLE bench.stream_stage LIKE bench.stream");
        }
    }

    @Test
    @Order(1)
    void rawBinlogBacklogDrain() throws Exception {
        BenchmarkResult result = runBacklog("stream", 6403, null, null);

        System.out.printf("[THROUGHPUT] raw mysql binlog backlog %,d rows  insert=%dms  " +
                        "drain=%dms  = %,d rows/s%n",
                ROWS, result.insertMs(), result.drainMs(), result.rowsPerSec());
        assertThat(result.decodedRows()).isEqualTo(ROWS);
        assertThat(result.rowsPerSec()).as("协议解码不应成为当前完整链瓶颈")
                .isGreaterThan(50_000);
    }

    @Test
    @Order(2)
    void rawBinlogToDuckDbStaging() throws Exception {
        try (Connection duck = DriverManager.getConnection("jdbc:duckdb:");
             Statement s = duck.createStatement()) {
            s.execute("CREATE TABLE stg (id VARCHAR, name VARCHAR, val VARCHAR)");
            DuckDBConnection dc = duck.unwrap(DuckDBConnection.class);
            try (DuckDBAppender appender = dc.createAppender("main", "stg")) {
                BenchmarkResult result = runBacklog("stream_stage", 6404, row -> {
                    appender.beginRow();
                    for (Serializable value : row) {
                        if (value == null) appender.appendNull();
                        else appender.append(value.toString());
                    }
                    appender.endRow();
                }, appender::flush);

                System.out.printf("[THROUGHPUT] raw mysql -> DuckDB staging %,d rows  insert=%dms  " +
                                "drain+flush=%dms  = %,d rows/s%n",
                        ROWS, result.insertMs(), result.drainMs(), result.rowsPerSec());
                assertThat(result.decodedRows()).isEqualTo(ROWS);
                assertThat(queryCount(s, "SELECT count(*) FROM stg")).isEqualTo(ROWS);
                assertThat(result.rowsPerSec()).as("raw binlog + Appender 应保留明显优化空间")
                        .isGreaterThan(50_000);
            }
        }
    }

    private static BenchmarkResult runBacklog(String table, long serverId,
                                               RowConsumer rowConsumer,
                                               CheckedRunnable finish) throws Exception {
        BinlogPosition start = currentPosition();
        long insertStarted = System.nanoTime();
        exec("INSERT INTO bench." + table + " " +
                "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < " + ROWS + ") " +
                "SELECT i, CONCAT('row-', i), i * 0.01 FROM n");
        long insertMs = nanosToMillis(System.nanoTime() - insertStarted);

        AtomicLong decodedRows = new AtomicLong();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Map<Long, String> tables = new ConcurrentHashMap<>();
        BinaryLogClient client = new BinaryLogClient(
                MYSQL.getHost(), MYSQL.getFirstMappedPort(), "dbuser_cdc", "test");
        client.setServerId(serverId);
        client.setBinlogFilename(start.filename());
        client.setBinlogPosition(start.position());
        client.setKeepAlive(false);
        client.registerEventListener(event -> {
            EventData data = event.getData();
            if (data instanceof TableMapEventData mapped) {
                tables.put(mapped.getTableId(), mapped.getDatabase() + "." + mapped.getTable());
            } else if (data instanceof WriteRowsEventData rows
                    && ("bench." + table).equals(tables.get(rows.getTableId()))) {
                try {
                    for (Serializable[] row : rows.getRows()) {
                        if (rowConsumer != null) rowConsumer.accept(row);
                        decodedRows.incrementAndGet();
                    }
                } catch (Throwable e) {
                    failure.compareAndSet(null, e);
                }
            }
        });

        long drainStarted = System.nanoTime();
        Thread stopper = Thread.ofPlatform().name("raw-mysql-benchmark-stopper").start(() -> {
            try {
                await().atMost(Duration.ofMinutes(1)).pollInterval(Duration.ofMillis(2))
                        .until(() -> failure.get() != null || decodedRows.get() >= ROWS);
            } catch (Throwable e) {
                failure.compareAndSet(null, e);
            } finally {
                try {
                    if (client.isConnected()) client.disconnect();
                } catch (Throwable e) {
                    failure.compareAndSet(null, e);
                }
            }
        });
        try {
            client.connect(); // 事件回调就在当前线程，Appender 保持单线程约束
        } catch (Throwable e) {
            if (decodedRows.get() < ROWS) failure.compareAndSet(null, e);
        }
        stopper.join(5_000);
        if (finish != null && failure.get() == null) finish.run();
        long drainMs = nanosToMillis(System.nanoTime() - drainStarted);
        if (failure.get() != null) {
            throw new AssertionError("raw MySQL binlog reader failed", failure.get());
        }
        long rowsPerSec = ROWS * 1000L / drainMs;
        return new BenchmarkResult(insertMs, drainMs, rowsPerSec, decodedRows.get());
    }

    private static BinlogPosition currentPosition() throws Exception {
        try (Connection c = rootConnection(); Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SHOW BINARY LOG STATUS")) {
            assertThat(rs.next()).isTrue();
            return new BinlogPosition(rs.getString("File"), rs.getLong("Position"));
        }
    }

    private static void exec(String sql) throws Exception {
        try (Connection c = rootConnection(); Statement s = c.createStatement()) {
            s.execute("SET SESSION cte_max_recursion_depth = " + (ROWS + 1));
            s.execute(sql);
        }
    }

    private static Connection rootConnection() throws Exception {
        return DriverManager.getConnection(MYSQL.getJdbcUrl(), "root", "test");
    }

    private static long nanosToMillis(long nanos) {
        return Math.max(1, nanos / 1_000_000L);
    }

    private static long queryCount(Statement s, String sql) throws Exception {
        try (ResultSet rs = s.executeQuery(sql)) {
            assertThat(rs.next()).isTrue();
            return rs.getLong(1);
        }
    }

    @FunctionalInterface
    private interface RowConsumer {
        void accept(Serializable[] row) throws Exception;
    }

    @FunctionalInterface
    private interface CheckedRunnable {
        void run() throws Exception;
    }

    private record BinlogPosition(String filename, long position) {
    }

    private record BenchmarkResult(long insertMs, long drainMs, long rowsPerSec, long decodedRows) {
    }
}
