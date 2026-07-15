# S3 对象存储 rustfs:Debian 13 slim + 官方预编译二进制(gnu 变体,pin 版本+sha256 校验)
# 同镜像内置 mc(minio client)——compose 的 bucket-init 一次性任务复用本镜像建桶。
# 产物 CDN dl.rustfs.com 与 GitHub Releases 同源(Cloudflare R2),国内直连快;
# debian glibc≥2.31 选 gnu 变体(musl 静态版留给 alpine 场景)。
FROM debian:trixie-slim

ARG TARGETARCH=amd64
ARG RUSTFS_VERSION=1.0.0-beta.8

# APT 换阿里云镜像 + 基础工具(unzip 解产物;procps/iproute2/less/jq 排障)
RUN sed -i -e 's|deb.debian.org|mirrors.aliyun.com|g' -e 's|security.debian.org|mirrors.aliyun.com|g' \
        /etc/apt/sources.list.d/debian.sources \
    && apt-get update -qq \
    && apt-get install -y -qq --no-install-recommends \
        curl ca-certificates unzip tzdata procps iproute2 less jq \
    && ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
    && echo "Asia/Shanghai" > /etc/timezone \
    && rm -rf /var/lib/apt/lists/*

# rustfs 二进制(sha256 来自官方 Release assets 的 digest,升版本时两条都要换)
# + mc 客户端(仅建桶用,latest 无固定校验;dl.minio.org.cn 为 MinIO 官方中国 CDN,
#   海外部署可换回 dl.min.io——实测国内 dl.min.io 仅 ~33KB/s,中国 CDN ~5MB/s)
RUN case "$TARGETARCH" in \
        amd64)  RFS_ARCH=x86_64-gnu;  RFS_SHA=f6dbe05430743820db5c38efda403bfa7c1b30967013f792735ab37cee1dc137; MC_ARCH=amd64 ;; \
        arm64)  RFS_ARCH=aarch64-gnu; RFS_SHA=8709ec7eb7a0cdc875d1d1bc1d5ee2b17a801ea89bb8ce4a6b15443cf24c0cf3; MC_ARCH=arm64 ;; \
        *) echo "unsupported arch: $TARGETARCH" && exit 1 ;; \
    esac \
    && curl -fsSL "https://dl.rustfs.com/artifacts/rustfs/release/rustfs-linux-${RFS_ARCH}-v${RUSTFS_VERSION}.zip" -o /tmp/rustfs.zip \
    && echo "${RFS_SHA}  /tmp/rustfs.zip" | sha256sum -c - \
    && unzip -q /tmp/rustfs.zip -d /tmp/rustfs \
    && install -m 0755 /tmp/rustfs/rustfs /usr/local/bin/rustfs \
    && rm -rf /tmp/rustfs.zip /tmp/rustfs \
    && curl -fsSL "https://dl.minio.org.cn/client/mc/release/linux-${MC_ARCH}/mc" -o /usr/local/bin/mc \
    && chmod 0755 /usr/local/bin/mc \
    && /usr/local/bin/rustfs --version && /usr/local/bin/mc --version

# 单机体验栈以 root 运行(bind mount 目录无属主问题);
# 生产收紧参照官方镜像:uid 10001 rustfs 用户 + chown /data /logs
ENV TZ=Asia/Shanghai \
    RUSTFS_VOLUMES=/data \
    RUSTFS_OBS_LOG_DIRECTORY=/logs
RUN mkdir -p /data /logs

# 9000=S3 API(健康端点 /health) 9001=管理控制台(健康端点 /rustfs/console/health)
EXPOSE 9000 9001
VOLUME ["/data"]
ENTRYPOINT ["/usr/local/bin/rustfs"]
CMD ["/data"]
