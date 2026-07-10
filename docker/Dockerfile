# debezium-server-ducklake 容器镜像
# 构建: mvn package -DskipTests && docker build -f docker/Dockerfile -t debezium-server-ducklake .
FROM eclipse-temurin:25-jre

# DuckDB JNI 运行依赖 + 健康检查用 curl
RUN apt-get update -qq \
    && apt-get install -y -qq --no-install-recommends curl libstdc++6 zlib1g \
    && rm -rf /var/lib/apt/lists/*

ENV TZ=Asia/Shanghai

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
