package io.github.lionheartlattice.ducklake;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * debezium-server-ducklake 启动类。
 * <p>
 * 职责（唯一的湖写入者）：
 * <ul>
 *   <li>Debezium Embedded Engine 流式消费 PG18 逻辑复制（pgoutput，槽 dbz_ducklake）</li>
 *   <li>按表 append 写入 DuckLake（内嵌 DuckDB，catalog=PG 库 ducklake_catalog，数据=rustfs S3）</li>
 *   <li>DDL 审计流（sys_ddl_log，event trigger 捕获）翻译应用到湖侧</li>
 *   <li>湖维护定时任务（flush_inlined/merge/expire/cleanup，@Scheduled，零 Spark）</li>
 *   <li>同步水位线接口 /api/ducklake/watermark 与 Micrometer 指标</li>
 * </ul>
 * 形制与 a_start 对齐：扫 io.github.lionheartlattice 全包（core 的 EasyQuery/Web/异常处理可用）、
 * 排除 DataSourceAutoConfiguration（数据源由 core 的 MultiDataSourceConfiguration 按 yml 动态注册）。
 * 定时任务是本模块新增能力（全仓此前无 @EnableScheduling）。
 */
@SpringBootApplication(scanBasePackages = "io.github.lionheartlattice", exclude = {DataSourceAutoConfiguration.class})
@ConfigurationPropertiesScan(basePackages = "io.github.lionheartlattice")
@EnableScheduling
public class DucklakeApplication {

    public static void main(String[] args) {
        SpringApplication.run(DucklakeApplication.class, args);
    }
}
