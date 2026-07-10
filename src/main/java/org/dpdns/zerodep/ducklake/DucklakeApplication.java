package org.dpdns.zerodep.ducklake;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
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
 * 2026-07-08 与 core 解耦：只扫本模块包，依赖全部直接声明（版本由根 pom 统一管理）。
 * 模块内不存在 DataSource/EasyQuery——PG 走 Debezium 复制协议与 offset JDBC，湖走内嵌 DuckDB 直连，
 * 均不经 Spring 数据源体系，故无需再排除 DataSourceAutoConfiguration。
 */
@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
public class DucklakeApplication {

    public static void main(String[] args) {
        SpringApplication.run(DucklakeApplication.class, args);
    }
}
