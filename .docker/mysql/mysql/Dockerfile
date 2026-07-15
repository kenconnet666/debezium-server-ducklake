# MySQL 8.4 LTS 源库镜像：debian:bookworm-slim + MySQL 官方 APT 仓库(清华镜像)——
# 对齐本仓"全 Debian 基础镜像 + 换国内源 + 内置排障工具"约定(MySQL 官方 docker 镜像
# 是 Oracle Linux base 且 8.4 无 debian 变体,故自建)。
# entrypoint 自写精简版,兼容官方镜像的初始化协定:MYSQL_ROOT_PASSWORD / MYSQL_DATABASE /
# /docker-entrypoint-initdb.d(*.sh|*.sql) / /etc/mysql/conf.d 挂载。
FROM debian:bookworm-slim

# 构建期换阿里云 APT 源(与 postgres/ducklake/rustfs 镜像一致)
RUN sed -i -e 's|deb.debian.org|mirrors.aliyun.com|g' -e 's|security.debian.org|mirrors.aliyun.com|g' \
        /etc/apt/sources.list.d/debian.sources \
    && apt-get update && apt-get install -y -qq --no-install-recommends \
        ca-certificates curl gnupg tzdata procps iproute2 less jq \
    && rm -rf /var/lib/apt/lists/*

# MySQL 8.4 LTS(APT 包走清华镜像;GPG key 从官方拿,一次性小文件)。
# key 并入 2025+2023 两代:2023 key 已 EXPKEYSIG(2026-06 实测),仓库签名随官方轮换,
# 双 key 兼容轮换过渡期。policy-rc.d:容器无 systemd,拦截 deb 安装期的服务自启尝试
RUN echo 'exit 101' > /usr/sbin/policy-rc.d && chmod +x /usr/sbin/policy-rc.d \
    && { curl -fsSL https://repo.mysql.com/RPM-GPG-KEY-mysql-2025; \
         curl -fsSL https://repo.mysql.com/RPM-GPG-KEY-mysql-2023; } \
        | gpg --dearmor -o /usr/share/keyrings/mysql.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/mysql.gpg] https://mirrors.tuna.tsinghua.edu.cn/mysql/apt/debian bookworm mysql-8.4-lts" \
        > /etc/apt/sources.list.d/mysql.list \
    && apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends mysql-community-server \
    && rm -rf /var/lib/apt/lists/* /var/lib/mysql/* \
    && mkdir -p /docker-entrypoint-initdb.d /var/run/mysqld \
    && chown mysql:mysql /var/run/mysqld

ENV TZ=Asia/Shanghai

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

EXPOSE 3306
ENTRYPOINT ["entrypoint.sh"]
