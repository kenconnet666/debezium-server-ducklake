#!/usr/bin/env bash
# 压力测试:在部署机 docker/ 目录内执行 bash bench.sh,产出吞吐/延迟基线(供 README 性能节)
#   阶段1 追赶吞吐:停消费→灌 100 万行→启动→两点法计消费速率(灌入器速度不影响结果)
#   阶段2 稳态延迟:~2k 行/s 持续 60s,采样服务端分段延迟(deliver/stage/lakeTx/batchLag)p50/p95
#   阶段3 空闲单行:排空后单行 INSERT×5,batchLagMs 即"事件产生→落湖提交"端到端
#   阶段4 纯解码上限:dry-run(空转不写湖)重复追赶法——写侧被摘除后的 Debezium 解码+交付地板
#   ⚠️ 阶段4 的数据 offset 照推进、不落湖(dry-run 语义);压测表用后即删
# 结果汇总打印到 stdout 末尾 [RESULT] 段。
set -uo pipefail
cd "$(dirname "$0")"

PSRC() { docker compose exec -T postgres psql -qtA -U postgres -c "$1"; }
WM()   { docker compose exec -T ducklake sh -c "curl -sf -m3 http://127.0.0.1:19992/api/ducklake/watermark" 2>/dev/null; }
EV()   { WM | docker compose exec -T ducklake jq -r '.message.eventsTotal' 2>/dev/null || echo 0; }
now()  { date +%s.%N; }

# 采样 eventsTotal 直到停涨/达标,两点法回归吞吐: catchup <目标事件数> <标签>
catchup() {
  local target=$1 label=$2 t_a="" ev_a=0 t_b="" ev_b=0 last=-1 still=0
  for _ in $(seq 1 600); do
    local ev; ev=$(docker compose exec -T ducklake sh -c \
      "curl -sf -m3 http://127.0.0.1:19992/api/ducklake/watermark | jq -r .message.eventsTotal" 2>/dev/null || echo "")
    [ -z "$ev" ] || [ "$ev" = "null" ] && { sleep 1; continue; }
    # 起点:越过 JVM 预热(5 万事件后)
    if [ -z "$t_a" ] && [ "$ev" -ge 50000 ]; then t_a=$(now); ev_a=$ev; fi
    if [ "$ev" -ge "$target" ]; then t_b=$(now); ev_b=$ev; break; fi
    if [ "$ev" = "$last" ]; then still=$((still+1)); else still=0; fi
    [ "$still" -ge 10 ] && { t_b=$(now); ev_b=$ev; echo "  [warn] $label: 停涨于 $ev(目标 $target)"; break; }
    last=$ev; sleep 1
  done
  [ -z "$t_a" ] || [ -z "$t_b" ] && { echo "  [warn] $label: 采样不足"; return; }
  awk -v ea="$ev_a" -v eb="$ev_b" -v ta="$t_a" -v tb="$t_b" -v L="$label" \
    'BEGIN { d=tb-ta; if (d>0) printf "  %s: %.0f 行/秒 (窗口 %.0f 事件 / %.1fs)\n", L, (eb-ea)/d, eb-ea, d }'
}

# 灌 100 万行(4 并发 generate_series): fill <起始id>
fill() {
  local base=$1 t0 t1
  t0=$(now)
  for i in 0 1 2 3; do
    PSRC "INSERT INTO t_bench(id,name,val,payload) SELECT g, 'bench-'||g, (g%1000)*1.37, repeat('x',64) FROM generate_series($((base+i*250000+1)), $((base+(i+1)*250000))) g" >/dev/null &
  done
  wait
  t1=$(now)
  awk -v a="$t0" -v b="$t1" 'BEGIN { printf "  灌入 100 万行耗时 %.1fs\n", b-a }'
}

RESULT=""

echo "== 准备:压测表 t_bench(≈100B/行) =="
PSRC "DROP TABLE IF EXISTS t_bench" >/dev/null
PSRC "CREATE TABLE t_bench(id bigint PRIMARY KEY, name text, val numeric(12,2), payload text, created timestamptz DEFAULT now())" >/dev/null
PSRC "ALTER TABLE t_bench REPLICA IDENTITY FULL" >/dev/null
PSRC "INSERT INTO t_bench(id,name,val,payload) VALUES (0,'warmup',0,'x')" >/dev/null
sleep 5   # 湖表注册,避免建表开销计入追赶

echo "== 阶段1:追赶吞吐(端到端写湖) =="
docker compose stop ducklake >/dev/null 2>&1
fill 0
# 资源采样器(2s 一次)
: > /tmp/bench-stats.log
( while true; do docker stats --no-stream --format '{{.Name}} {{.CPUPerc}} {{.MemUsage}}' 2>/dev/null | grep ducklake >> /tmp/bench-stats.log; sleep 2; done ) &
STATS_PID=$!
docker compose start ducklake >/dev/null 2>&1
R1=$(catchup 1000000 "追赶吞吐")
kill $STATS_PID 2>/dev/null
PEAK=$(awk '{gsub(/%/,"",$2); if ($2+0>c){c=$2; m=$3" "$4" "$5}} END {printf "  追赶期资源峰值: CPU %.0f%% MEM %s\n", c, m}' /tmp/bench-stats.log)
echo "$R1"; echo "$PEAK"
RESULT="$RESULT$R1\n$PEAK\n"

echo "== 阶段2:稳态分段延迟(~2k 行/s × 60s) =="
: > /tmp/bench-lag.csv
BASE=1500000
for s in $(seq 1 60); do
  PSRC "INSERT INTO t_bench(id,name,val,payload) SELECT g, 'st-'||g, 1.0, repeat('x',64) FROM generate_series($((BASE+(s-1)*2000+1)), $((BASE+s*2000))) g" >/dev/null
  docker compose exec -T ducklake sh -c \
    "curl -sf -m3 http://127.0.0.1:19992/api/ducklake/watermark | jq -r '[.message.deliverLagMs,.message.stageMs,.message.lakeTxMs,.message.batchLagMs] | @csv'" \
    >> /tmp/bench-lag.csv 2>/dev/null
  sleep 0.4
done
R2=$(awk -F, 'NR>5 { for(i=1;i<=4;i++) v[i]=v[i]" "$i; n++ }
  END {
    split("deliverLag stage lakeTx batchLag", name, " ")
    for(i=1;i<=4;i++) {
      m=split(v[i], a, " "); asort2(a, m)
      printf "  %s: p50=%sms p95=%sms\n", name[i], a[int(m*0.5)], a[int(m*0.95)]
    }
  }
  function asort2(a, m,  i, j, t) { for(i=2;i<=m;i++){ t=a[i]; j=i-1; while(j>=1 && a[j]+0>t+0){a[j+1]=a[j]; j--} a[j+1]=t } }' /tmp/bench-lag.csv)
echo "$R2"
RESULT="$RESULT稳态 ~2k 行/s 分段延迟:\n$R2\n"

echo "== 阶段3:空闲单行端到端(batchLagMs) =="
sleep 8   # 排空
LAGS=""
for i in 1 2 3 4 5; do
  PSRC "INSERT INTO t_bench(id,name,val,payload) VALUES ($((1900000+i)),'probe',0,'x')" >/dev/null
  sleep 2
  L=$(docker compose exec -T ducklake sh -c "curl -sf -m3 http://127.0.0.1:19992/api/ducklake/watermark | jq -r .message.batchLagMs" 2>/dev/null)
  LAGS="$LAGS $L"
  sleep 1
done
R3=$(echo "$LAGS" | tr ' ' '\n' | grep -E '^[0-9]+$' | sort -n | awk '{a[NR]=$1} END {printf "  空闲单行 batchLag: 中位 %sms (5 次: %s)\n", a[int(NR/2)+1], "'"$LAGS"'"}')
echo "$R3"
RESULT="$RESULT$R3\n"

echo "== 阶段4:纯解码上限(dry-run 空转,不写湖) =="
docker compose stop ducklake >/dev/null 2>&1
cat > docker-compose.override.yml <<'EOF'
services:
  ducklake:
    environment:
      DUCKLAKE_ENGINE_DRYRUN: "true"
EOF
fill 2000000
docker compose up -d ducklake >/dev/null 2>&1
R4=$(catchup 1000000 "纯解码吞吐(dry-run)")
echo "$R4"
RESULT="$RESULT$R4\n"
rm -f docker-compose.override.yml
docker compose up -d ducklake >/dev/null 2>&1   # 恢复正常模式(阶段4数据已被 offset 跳过,预期不落湖)

echo "== 清理:压测表 =="
PSRC "DROP TABLE IF EXISTS t_bench" >/dev/null

echo
echo "════ [RESULT] ════"
printf '%b' "$RESULT"
