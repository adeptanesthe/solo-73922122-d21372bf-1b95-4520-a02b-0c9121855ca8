#!/usr/bin/env bash
# 慢 GDAL 下的停机/交接演示(需要 Docker)。
#
#   ./tools/demo_shutdown.sh                 # 默认慢速因子 8s
#   SLOW_GDAL_FACTOR=15 ./tools/demo_shutdown.sh
#
# 演示内容:
#   A. 两个任务并行、其余排队时停机:运行中任务被终止、排队任务不启动,
#      全部 GDAL 进程退出并回收后才清理未完成任务目录,已完成结果保留;
#   B. 重复一次,覆盖阶段交接窗口(上一阶段刚结束、下一阶段启动前);
#   C. 停机日志序列:拒绝新任务 / 回收进程 / 清理目录 / 收尾完成;
#   D. 关闭期间新提交被拒绝(503 或连接拒绝)。
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE=(docker compose -f "$ROOT/docker-compose.yml" -f "$ROOT/docker-compose.demo.yml")
DATA_DIR="${DATA_DIR:-$ROOT/data}"
export SLOW_GDAL_FACTOR="${SLOW_GDAL_FACTOR:-8}"
BASE_URL="${BASE_URL:-http://localhost:8000}"
TASKS_DIR="$DATA_DIR/tasks"

compose() { "${COMPOSE[@]}" "$@"; }
say() { printf '\n== %s\n' "$*"; }

wait_ready() {
  for _ in $(seq 1 180); do
    if curl -fsS "$BASE_URL/readyz" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  echo "服务未在 180s 内就绪" >&2
  compose logs --no-log-prefix web | tail -n 50 >&2 || true
  exit 1
}

submit() {
  local name="${1:-dem_A_utm32n.tif}" res="${2:-0.5}"
  curl -fsS -X POST "$BASE_URL/api/tasks" \
    -F "resolution=$res" \
    -F "plan=[{\"type\":\"sample\",\"name\":\"$name\"}]" |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])'
}

# 容器内(仍在运行时)存活的 GDAL 相关进程数
gdal_alive() {
  local n
  n=$(compose exec -T web sh -c \
    'ps -eo pid,args | grep -E "[g]dal(warp|_translate|info)" | wc -l' 2>/dev/null || true)
  echo "${n:-0}" | tr -d ' \r\n'
}

unfinished_dirs() {
  # 没有 result.tif 的任务目录 = 未完成任务
  find "$TASKS_DIR" -mindepth 1 -maxdepth 1 -type d \
    ! -exec test -f '{}/result.tif' ';' -print 2>/dev/null | wc -l
}

done_dirs() {
  find "$TASKS_DIR" -mindepth 1 -maxdepth 1 -type d \
    -exec test -f '{}/result.tif' ';' -print 2>/dev/null | wc -l
}

show_tasks() {
  curl -fsS "$BASE_URL/api/tasks" |
    python3 -c 'import json,sys
for t in json.load(sys.stdin):
    print(f"  {t[\"id\"]}  {t[\"status\"]:9s} {t[\"stage\"] or \"-\"}")'
}

# 后台发起停机,同时轮询容器内 GDAL 进程数,观察进程被回收的过程
stop_and_watch() {
  compose stop -t 100 web >/dev/null 2>&1 &
  local stop_pid=$!
  local last=""
  while kill -0 "$stop_pid" 2>/dev/null; do
    local n
    n=$(gdal_alive || true)
    if [ "$n" != "$last" ]; then
      echo "  停机过程中存活 GDAL 进程: $n"
      last="$n"
    fi
    sleep 0.3
  done
  wait "$stop_pid" || true
}

say "启动(慢 GDAL 因子 ${SLOW_GDAL_FACTOR}s)"
compose up -d --build
wait_ready

say "提交 5 个任务:2 个并行,其余排队;另有 1 个较快的任务先完成"
for _ in 1 2 3 4; do submit dem_A_utm32n.tif 0.5 >/dev/null; done
submit dem_C_wgs84.tif 5 >/dev/null
sleep 3
show_tasks
echo "  停机前: 未完成任务目录 $(unfinished_dirs), 已完成 $(done_dirs), 存活 GDAL $(gdal_alive)"

say "任务 A:运行中 + 排队中停机"
stop_and_watch
say "停机后的日志序列"
compose logs --no-log-prefix web | grep -E "停机|关闭|slow-gdal" | tail -n 12 || true
echo "  停机后: 未完成任务目录 $(unfinished_dirs), 已完成 $(done_dirs)(应仅剩已完成)"
echo "  容器状态: $(compose ps --format '{{.Service}}={{.State}}' web 2>/dev/null || echo stopped)"
compose start web >/dev/null
wait_ready

say "任务 B:再次提交,覆盖阶段交接窗口"
for _ in 1 2 3; do submit dem_A_utm32n.tif 0.5 >/dev/null; done
sleep 1
compose logs --no-log-prefix web | grep -E "slow-gdal" | tail -n 3 || true
stop_and_watch
echo "  停机后: 未完成任务目录 $(unfinished_dirs)"
compose start web >/dev/null
wait_ready
show_tasks

say "任务 D:关闭期间新提交"
compose stop -t 100 web >/dev/null 2>&1 &
sleep 1
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE_URL/api/tasks" \
  -F 'resolution=1' -F 'plan=[{"type":"sample","name":"dem_A_utm32n.tif"}]' || true)
echo "  关闭期间 POST /api/tasks -> HTTP ${code:-连接被拒绝}(拒绝受理即符合预期)"
wait

say "清理"
compose down --remove-orphans
echo "演示结束。"
