# debezium-server-ducklake 生产镜像:Debian 13 slim + JetBrains Runtime(JBR) 25
# ── 单一源无关镜像:mysql/postgres 由运行时 DUCKLAKE_SOURCE_TYPE 切换,故两栈与 CI 共用本文件 ──
# 构建(context 必须是仓库根——要 COPY target/*.jar):
#   .\mvnw.cmd package -DskipTests
#   docker build -f .docker/ducklake/Dockerfile -t debezium-server-ducklake .
# 多架构(buildx): TARGETARCH=amd64/arm64 自动映射 JBR 的 x64/aarch64 变体
FROM debian:trixie-slim

# ⚠️ 不能给默认值:TARGETARCH 由 BuildKit 按 --platform 自动注入,给了默认(如 =amd64)
#    会盖过自动值,导致 arm64 一路误取 x64 JBR、真机跑不起来(v0.1.0 首发多架构即栽在此)。
ARG TARGETARCH

# APT 换阿里云镜像(deb822 格式,主源+security 同域名替换) + 基础工具:
#   curl/ca-certificates(下载 JBR+健康检查) libstdc++6/zlib1g(DuckDB JNI 链接)
#   procps/iproute2/less/jq(容器内排障:ps、ss、翻日志、调 API)
RUN sed -i -e 's|deb.debian.org|mirrors.aliyun.com|g' -e 's|security.debian.org|mirrors.aliyun.com|g' \
        /etc/apt/sources.list.d/debian.sources \
    && apt-get update -qq \
    && apt-get install -y -qq --no-install-recommends \
        curl ca-certificates tzdata libstdc++6 zlib1g procps iproute2 less jq \
    && ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
    && echo "Asia/Shanghai" > /etc/timezone \
    && rm -rf /var/lib/apt/lists/*

# JBR 25 runtime(非 SDK,更小;cache-redirector 为 JetBrains 官方分发 CDN);
# tar 顶层单目录,strip 后落 /opt/jbr。arch 优先用 BuildKit 注入的 TARGETARCH,空则 dpkg 探测。
# 构建期不跑 java -version:多架构下 arm64 层是 QEMU 模拟,执行异架构 JVM 易挂;
# 只需下载解压,真机运行时自然可用。
RUN ARCH="${TARGETARCH:-$(dpkg --print-architecture)}" \
    && case "$ARCH" in \
        amd64)  JBR_ARCH=x64 ;; \
        arm64)  JBR_ARCH=aarch64 ;; \
        *) echo "unsupported arch: $ARCH" && exit 1 ;; \
    esac \
    && curl -fsSL "https://cache-redirector.jetbrains.com/intellij-jbr/jbr-25.0.3-linux-${JBR_ARCH}-b508.16.tar.gz" -o /tmp/jbr.tar.gz \
    && mkdir -p /opt/jbr \
    && tar -xzf /tmp/jbr.tar.gz -C /opt/jbr --strip-components=1 \
    && rm /tmp/jbr.tar.gz

ENV JAVA_HOME=/opt/jbr \
    PATH=/opt/jbr/bin:$PATH \
    TZ=Asia/Shanghai \
    HOME=/home/ducklake

# 固定 uid/gid 便于生产 volume/fsGroup 对齐；扩展缓存与应用目录都不需要 root。
RUN groupadd --gid 10001 ducklake \
    && useradd --uid 10001 --gid ducklake --home-dir /home/ducklake \
        --create-home --shell /usr/sbin/nologin ducklake \
    && install -d -o ducklake -g ducklake /app /home/ducklake/.duckdb

COPY --chown=ducklake:ducklake target/debezium-server-ducklake-*.jar /app/app.jar

# 分代 ZGC + 紧凑对象头(JEP 519) + DuckDB JNI native access 授权。
# 内存预算公式: 容器限额 ≈ JVM 堆(512m) + JVM 本体(~300m) + DuckDB memory-limit(默认 1536m) + 余量
# → 容器建议 ≥3G;调小 DUCKLAKE_MEMORY_LIMIT 时可等比例降
ENV JAVA_OPTS="-Xmx512m -XX:+UseZGC -XX:+UseCompactObjectHeaders --enable-native-access=ALL-UNNAMED"

WORKDIR /app
USER 10001:10001
EXPOSE 19992
HEALTHCHECK --interval=15s --timeout=5s --retries=3 --start-period=90s \
    CMD curl -f http://127.0.0.1:19992/api/ducklake/actuator/health || exit 1
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
