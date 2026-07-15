# 源库/元空间共用 PG 镜像：官方 postgres:18(Debian bookworm) + Pigsty pig 扩展仓库
# pig = Pigsty 官方 PostgreSQL 扩展包管理器(https://pigsty.io/docs/pig/)——
# 想加插件只需在此追加一行 `pig ext install <name> -v 18 -y`(alpine/musl 不受 pig 支持,
# 这也是必须用 Debian 变体的原因)
FROM postgres:18.4-bookworm

USER root

# APT 换阿里云镜像;官方镜像自带的 pgdg 源(apt.postgresql.org,国内慢)直接移除——
# 下面 pig repo add 会配好 pgdg+pigsty 的国内镜像仓。基础工具:
#   curl/ca-certificates/gnupg(pig 安装链) procps/iproute2/less/jq(排障)
RUN sed -i -e 's|deb.debian.org|mirrors.aliyun.com|g' -e 's|security.debian.org|mirrors.aliyun.com|g' \
        /etc/apt/sources.list.d/debian.sources \
    && rm -f /etc/apt/sources.list.d/pgdg.list \
    && apt-get update -qq \
    && apt-get install -y -qq --no-install-recommends \
        curl ca-certificates gnupg tzdata procps iproute2 less jq \
    && ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
    && echo "Asia/Shanghai" > /etc/timezone

# pig CLI + PGDG/Pigsty 扩展仓库(repo.pigsty.cc = 中国区镜像域,海外部署换回 repo.pigsty.io)
RUN curl -fsSL https://repo.pigsty.cc/pig | bash && \
    pig repo add pgdg pigsty -u

# 预装内核没带的常用扩展(contrib 自带 pg_stat_statements/auto_explain 无需安装):
#   pg_bigm          中文/日文模糊搜索加速(LIKE 索引)
#   pg_stat_kcache   每条查询的物理 IO/CPU(依赖 pg_stat_statements)
#   pg_wait_sampling 等待事件采样
#   hll              HyperLogLog 近似去重计数
RUN pig ext install pg_bigm pg_stat_kcache pg_wait_sampling hll -v 18 -y && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV TZ=Asia/Shanghai
