#!/usr/bin/env bash
# 参数矩阵压测:单变量对照,每组测"100 万行积压追赶吞吐 + 空闲单行 batchLag"。
# 用法: nohup bash bench-matrix.sh > matrix.log 2>&1 &   (跑完打 MATRIX-DONE)
# 参数注入 = docker-compose.override.yml 环境变量(Spring relaxed binding),
# 每组重建 ducklake 容器(进程内计数器归零,两点法窗口干净)。
#   baseline  全默认(batch 8192 / lake.threads 2 / SMT 并行=核数 / inlining 512 / poll 10ms)
#   rpt0      record-processing-threads=0(Debezium 引擎默认=事实单线程)——量化 SMT 并行收益
#   threads4  lake.threads=4——单机全家桶下 DuckDB 并行加倍是否有益
#   batch32k  max-batch-size=32768(+queue 131072)——大批减少湖事务次数 vs 单批耗时上升
#   inline0   data-inlining-row-limit=0——积压大批场景 inlining 关闭的影响
#   poll1     poll-interval-ms=1——空闲唤醒间隔对单行端到端延迟的影响(吞吐不敏感)
set -uo pipefail
cd "$(dirname "$0")"

PSRC() { docker compose exec -T postgres psql -qtA -U postgres -c "$1"; }
now()  { date +%s.%N; }
SEG=0
SUMMARY=""

env_of() {
  case "$1" in
    baseline) printf "" ;;
    rpt0)     printf '      DUCKLAKE_ENGINE_RECORDPROCESSINGTHREADS: "0"\n' ;;
    threads4) printf '      DUCKLAKE_THREADS: "4"\n' ;;
    batch32k) printf '      DUCKLAKE_ENGINE_MAXBATCHSIZE: "32768"\n      DUCKLAKE_ENGINE_MAXQUEUESIZE: "131072"\n' ;;
    inline0)  printf '      DUCKLAKE_LAKE_DATAINLININGROWLIMIT: "0"\n' ;;
    poll1)    printf '      DUCKLAKE_ENGINE_POLLINTERVALMS: "1"\n' ;;
  esac
}

wait_healthy() {
  for _ in $(seq 1 60); do
    [ "$(docker inspect --format '{{.State.Health.Status}}' docker-ducklake-1 2>/dev/null)" = "healthy" ] && return 0
    sleep 3
  done
  echo "[warn] ducklake 未 healthy"; return 1
}

ev_now() {
  docker compose exec -T ducklake sh -c \
    "curl -sf -m3 http://127.0.0.1:19992/api/ducklake/watermark | jq -r .message.eventsTotal" 2>/dev/null || echo ""
}

# 两点法追赶吞吐(起点=5 万事件后,去 JVM 预热): 输出"数字 行/秒"或空
catchup_rate() {
  local target=$1 t_a="" ev_a=0 t_b="" ev_b=0 last=-1 still=0 ev
  for _ in $(seq 1 600); do
    ev=$(ev_now)
    { [ -z "$ev" ] || [ "$ev" = "null" ]; } && { sleep 1; continue; }
    if [ -z "$t_a" ] && [ "$ev" -ge 50000 ]; then t_a=$(now); ev_a=$ev; fi
    if [ "$ev" -ge "$target" ]; then t_b=$(now); ev_b=$ev; break; fi
    if [ "$ev" = "$last" ]; then still=$((still+1)); else still=0; fi
    [ "$still" -ge 12 ] && { t_b=$(now); ev_b=$ev; break; }
    last=$ev; sleep 1
  done
  { [ -z "$t_a" ] || [ -z "$t_b" ]; } && return
  awk -v ea="$ev_a" -v eb="$ev_b" -v ta="$t_a" -v tb="$t_b" \
    'BEGIN { d=tb-ta; if (d>0 && eb>ea) printf "%.0f", (eb-ea)/d }'
}

# 空闲单行 batchLag 中位(3 次)
idle_lag() {
  local base=$1 lags="" l
  sleep 6
  for i in 1 2 3; do
    PSRC "INSERT INTO t_bench(id,name,val,payload) VALUES ($((base+i)),'probe',0,'x')" >/dev/null
    sleep 2
    l=$(docker compose exec -T ducklake sh -c \
      "curl -sf -m3 http://127.0.0.1:19992/api/ducklake/watermark | jq -r .message.batchLagMs" 2>/dev/null)
    lags="$lags $l"; sleep 1
  done
  echo "$lags" | tr ' ' '\n' | grep -E '^[0-9]+$' | sort -n | awk '{a[NR]=$1} END {print a[int((NR+1)/2)]}'
}

run_group() {
  local g=$1 envs
  envs=$(env_of "$g")
  echo "════ 组 $g ════"
  if [ -n "$envs" ]; then
    { echo "services:"; echo "  ducklake:"; echo "    environment:"; printf '%s' "$envs"; } > docker-compose.override.yml
  else
    rm -f docker-compose.override.yml
  fi
  docker compose up -d --force-recreate ducklake >/dev/null 2>&1
  wait_healthy || { SUMMARY="$SUMMARY$g: 启动失败\n"; return; }
  docker compose stop ducklake >/dev/null 2>&1
  local base=$((SEG*1000000)); SEG=$((SEG+1))
  local t0 t1
  t0=$(now)
  for i in 0 1 2 3; do
    PSRC "INSERT INTO t_bench(id,name,val,payload) SELECT g, 'b-'||g, (g%1000)*1.37, repeat('x',64) FROM generate_series($((base+i*250000+1)), $((base+(i+1)*250000))) g" >/dev/null &
  done
  wait
  t1=$(now)
  docker compose start ducklake >/dev/null 2>&1
  local rate lag
  rate=$(catchup_rate 1000000)
  lag=$(idle_lag $((90000000+SEG*100)))
  awk -v a="$t0" -v b="$t1" -v g="$g" -v r="${rate:-?}" -v l="${lag:-?}" \
    'BEGIN { printf "  %s: 追赶 %s 行/秒 | 空闲单行 batchLag %sms | (灌入 %.0fs)\n", g, r, l, b-a }'
  SUMMARY="$SUMMARY$g: 追赶 ${rate:-?} 行/秒 | 空闲单行 ${lag:-?}ms\n"
}

echo "== 准备 =="
PSRC "DROP TABLE IF EXISTS t_bench" >/dev/null
PSRC "CREATE TABLE t_bench(id bigint PRIMARY KEY, name text, val numeric(12,2), payload text, created timestamptz DEFAULT now())" >/dev/null
PSRC "ALTER TABLE t_bench REPLICA IDENTITY FULL" >/dev/null
PSRC "INSERT INTO t_bench(id,name,val,payload) VALUES (99999999,'warmup',0,'x')" >/dev/null
sleep 5

for g in baseline rpt0 threads4 batch32k inline0 poll1; do
  run_group "$g"
done

echo "== 收尾:恢复默认配置,清压测表 =="
rm -f docker-compose.override.yml
docker compose up -d --force-recreate ducklake >/dev/null 2>&1
wait_healthy
PSRC "DROP TABLE IF EXISTS t_bench" >/dev/null

echo
echo "════ 矩阵汇总 ════"
printf '%b' "$SUMMARY"
echo "MATRIX-DONE"
