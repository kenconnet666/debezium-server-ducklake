# debezium-server-ducklake 容器镜像:Debian 13 slim + JetBrains Runtime(JBR) 25
# 构建: mvn package -DskipTests && docker build -f docker/Dockerfile -t debezium-server-ducklake .
# 多架构(buildx): TARGETARCH=amd64/arm64 自动映射 JBR 的 x64/aarch64 变体
FROM debian:trixie-slim

ARG TARGETARCH=amd64

# JBR 运行依赖:libstdc++6/zlib1g(DuckDB JNI 与 JDK native 链接),curl/ca-certificates(下载 JBR+健康检查)
RUN apt-get update -qq \
    && apt-get install -y -qq --no-install-recommends curl ca-certificates libstdc++6 zlib1g \
    && rm -rf /var/lib/apt/lists/*

# JBR 25 runtime(非 SDK,更小;cache-redirector 为 JetBrains 官方分发 CDN);
# tar 顶层单目录,strip 后落 /opt/jbr
RUN case "$TARGETARCH" in \
        amd64)  JBR_ARCH=x64 ;; \
        arm64)  JBR_ARCH=aarch64 ;; \
        *) echo "unsupported arch: $TARGETARCH" && exit 1 ;; \
    esac \
    && curl -fsSL "https://cache-redirector.jetbrains.com/intellij-jbr/jbr-25.0.3-linux-${JBR_ARCH}-b508.16.tar.gz" -o /tmp/jbr.tar.gz \
    && mkdir -p /opt/jbr \
    && tar -xzf /tmp/jbr.tar.gz -C /opt/jbr --strip-components=1 \
    && rm /tmp/jbr.tar.gz \
    && /opt/jbr/bin/java -version

ENV JAVA_HOME=/opt/jbr \
    PATH=/opt/jbr/bin:$PATH \
    TZ=Asia/Shanghai

COPY target/debezium-server-ducklake-*.jar /app/app.jar

# 分代 ZGC + 紧凑对象头(JEP 519) + DuckDB JNI native access 授权。
# 内存预算公式: 容器限额 ≈ JVM 堆(512m) + JVM 本体(~300m) + DuckDB memory-limit(默认 1536m) + 余量
# → 容器建议 ≥3G;调小 DUCKLAKE_MEMORY_LIMIT 时可等比例降
ENV JAVA_OPTS="-Xmx512m -XX:+UseZGC -XX:+UseCompactObjectHeaders --enable-native-access=ALL-UNNAMED"

WORKDIR /app
EXPOSE 19992
HEALTHCHECK --interval=15s --timeout=5s --retries=3 --start-period=90s \
    CMD curl -f http://127.0.0.1:19992/api/ducklake/actuator/health || exit 1
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
