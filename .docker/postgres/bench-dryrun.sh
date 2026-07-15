#!/usr/bin/env bash
# 补测:dry-run 纯解码上限(handleBatch 空转,不写湖)——同 bench-matrix 追赶法口径。
# ⚠️ dry-run 段数据 offset 照推进、不落湖;压测表用后即删。跑完打 DRYRUN-DONE。
set -uo pipefail
cd "$(dirname "$0")"
PSRC() { docker compose exec -T postgres psql -qtA -U postgres -c "$1"; }
now()  { date +%s.%N; }

PSRC "DROP TABLE IF EXISTS t_bench" >/dev/null
PSRC "CREATE TABLE t_bench(id bigint PRIMARY KEY, name text, val numeric(12,2), payload text, created timestamptz DEFAULT now())" >/dev/null
PSRC "ALTER TABLE t_bench REPLICA IDENTITY FULL" >/dev/null

cat > docker-compose.override.yml <<'EOF'
services:
  ducklake:
    environment:
      DUCKLAKE_ENGINE_DRYRUN: "true"
EOF
docker compose up -d --force-recreate ducklake >/dev/null 2>&1
for _ in $(seq 1 60); do
  [ "$(docker inspect --format '{{.State.Health.Status}}' docker-ducklake-1 2>/dev/null)" = "healthy" ] && break
  sleep 3
done
docker compose stop ducklake >/dev/null 2>&1
for i in 0 1 2 3; do
  PSRC "INSERT INTO t_bench(id,name,val,payload) SELECT g, 'b-'||g, (g%1000)*1.37, repeat('x',64) FROM generate_series($((i*250000+1)), $(((i+1)*250000))) g" >/dev/null &
done
wait
docker compose start ducklake >/dev/null 2>&1

t_a=""; ev_a=0; t_b=""; ev_b=0; last=-1; still=0
for _ in $(seq 1 600); do
  ev=$(docker compose exec -T ducklake sh -c "curl -sf -m3 http://127.0.0.1:19992/api/ducklake/watermark | jq -r .message.eventsTotal" 2>/dev/null || echo "")
  { [ -z "$ev" ] || [ "$ev" = "null" ]; } && { sleep 1; continue; }
  if [ -z "$t_a" ] && [ "$ev" -ge 50000 ]; then t_a=$(now); ev_a=$ev; fi
  if [ "$ev" -ge 1000000 ]; then t_b=$(now); ev_b=$ev; break; fi
  if [ "$ev" = "$last" ]; then still=$((still+1)); else still=0; fi
  [ "$still" -ge 12 ] && { t_b=$(now); ev_b=$ev; break; }
  last=$ev; sleep 1
done
awk -v ea="$ev_a" -v eb="$ev_b" -v ta="$t_a" -v tb="$t_b" \
  'BEGIN { d=tb-ta; if (d>0 && eb>ea) printf "dry-run 纯解码: %.0f 行/秒 (窗口 %.0f 事件 / %.1fs)\n", (eb-ea)/d, eb-ea, d }'

rm -f docker-compose.override.yml
docker compose up -d --force-recreate ducklake >/dev/null 2>&1
PSRC "DROP TABLE IF EXISTS t_bench" >/dev/null
echo "DRYRUN-DONE"
