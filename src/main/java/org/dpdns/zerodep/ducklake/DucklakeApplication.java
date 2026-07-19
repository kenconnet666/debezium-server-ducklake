package org.dpdns.zerodep.ducklake;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * PostgreSQL/MySQL 原生 CDC 入湖服务启动类（artifact 名保留历史兼容）。
 * <p>
 * 职责（唯一的湖写入者）：
 * <ul>
 *   <li>pgjdbc 直读 PostgreSQL pgoutput，BinaryLogClient 直读 MySQL ROW binlog</li>
 *   <li>按表 append 写入 DuckLake（内嵌 DuckDB，catalog=PG 库 ducklake_catalog，数据=rustfs S3）</li>
 *   <li>PG DDL 审计流与 MySQL binlog QueryEvent 同序翻译到湖侧</li>
 *   <li>湖维护定时任务（flush_inlined/merge/expire/cleanup，@Scheduled，零 Spark）</li>
 *   <li>同步水位线接口 /api/ducklake/watermark 与 Micrometer 指标</li>
 * </ul>
 * 模块内不存在 DataSource/EasyQuery——复制流、catalog offset 与湖写入均使用专用直连，
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
