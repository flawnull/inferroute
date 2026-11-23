#!/usr/bin/env bash
# Chaos benchmark: run the k6 load test, SIGKILL a worker partway through,
# restart it, and measure failovers, error rate during the outage, and
# detection/recovery time.
#
#   ./benchmarks/chaos.sh            # requires `make up` first
#   DURATION=90s RATE=300 ./benchmarks/chaos.sh
set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE=${COMPOSE:-docker compose}
export BENCH_UID=${BENCH_UID:-$(id -u)} BENCH_GID=${BENCH_GID:-$(id -g)}
ROUTER_URL=${ROUTER_URL:-http://localhost:8080}
VICTIM=${VICTIM:-worker-1}
KILL_AT=${KILL_AT:-15}   # seconds after load starts
DOWN_FOR=${DOWN_FOR:-20} # seconds the victim stays dead
DURATION=${DURATION:-60s}
RATE=${RATE:-200}
RESULTS_DIR=benchmarks/results
STAMP=$(date +%Y%m%d-%H%M%S)
K6_LOG="$RESULTS_DIR/chaos-$STAMP-k6.txt"
K6_JSON="$RESULTS_DIR/chaos-$STAMP-k6.json"
REPORT="$RESULTS_DIR/chaos-$STAMP.md"
mkdir -p "$RESULTS_DIR"

now_ms() {
  local t
  t=$(date +%s%3N)
  if [[ "$t" == *N ]]; then python3 -c 'import time; print(int(time.time()*1000))'; else echo "$t"; fi
}
log() { printf '[chaos %s] %s\n' "$(date +%H:%M:%S)" "$*" >&2; }

# Sums a Prometheus counter from the router's /metrics; optional label filter regex.
metric_sum() {
  local name=$1 filter=${2:-}
  curl -fsS "$ROUTER_URL/metrics" | awk -v n="$name" -v f="$filter" '
    index($0, n) == 1 && substr($0, length(n)+1, 1) ~ /[{ ]/ && (f == "" || $0 ~ f) { s += $NF }
    END { printf "%.0f\n", s }'
}

# Prints the victim's JSON object from /healthz.
victim_status() {
  curl -fsS "$ROUTER_URL/healthz" | grep -o "\"id\":\"$VICTIM\"[^}]*" || true
}

wait_for() { # wait_for <timeout_s> <grep pattern on victim status>
  local deadline=$(( $(now_ms) + $1 * 1000 ))
  while (( $(now_ms) < deadline )); do
    if victim_status | grep -q "$2"; then return 0; fi
    sleep 0.1
  done
  return 1
}

curl -fsS "$ROUTER_URL/readyz" >/dev/null || { log "router not ready at $ROUTER_URL; run 'make up' first"; exit 1; }
wait_for 15 '"healthy":true' || { log "$VICTIM not healthy before start"; exit 1; }

base_total=$(metric_sum inferroute_requests_total)
base_fail=$(metric_sum inferroute_failovers_total)

log "starting k6: rate=$RATE/s duration=$DURATION"
$COMPOSE --profile bench run --rm -e RATE="$RATE" -e DURATION="$DURATION" \
  -e SUMMARY_FILE="/bench/results/$(basename "$K6_JSON")" k6 run --quiet /bench/load.js >"$K6_LOG" 2>&1 &
k6_pid=$!
cleanup() {
  kill "$k6_pid" 2>/dev/null || true
  $COMPOSE start "$VICTIM" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Anchor the timeline on the first requests reaching the router, not on
# container start-up.
until (( $(metric_sum inferroute_requests_total) > base_total )); do sleep 0.2; done
log "load is flowing; killing $VICTIM in ${KILL_AT}s"
sleep "$KILL_AT"

a_total=$(metric_sum inferroute_requests_total)
a_err=$(metric_sum inferroute_requests_total 'status="5')
a_fail=$(metric_sum inferroute_failovers_total)
kill_at=$(now_ms)
$COMPOSE kill "$VICTIM" >/dev/null 2>&1
log "killed $VICTIM (SIGKILL)"

if wait_for 10 '"healthy":false'; then
  detect_ms=$(( $(now_ms) - kill_at ))
else
  detect_ms="n/a"
fi
log "router marked $VICTIM unhealthy after ${detect_ms}ms"

# Keep the victim down for DOWN_FOR seconds measured from the kill.
remaining_ms=$(( DOWN_FOR * 1000 - ($(now_ms) - kill_at) ))
if (( remaining_ms > 0 )); then
  sleep "$(( remaining_ms / 1000 )).$(( remaining_ms % 1000 / 100 ))"
fi
b_total=$(metric_sum inferroute_requests_total)
b_err=$(metric_sum inferroute_requests_total 'status="5')
b_fail=$(metric_sum inferroute_failovers_total)

restart_at=$(now_ms)
$COMPOSE start "$VICTIM" >/dev/null 2>&1
log "restarted $VICTIM"
if wait_for 60 '"healthy":true,"breaker":"closed"'; then
  recovery_ms=$(( $(now_ms) - restart_at ))
else
  recovery_ms="n/a (>60s)"
fi
log "$VICTIM healthy with breaker closed after ${recovery_ms}ms"

wait "$k6_pid" || log "k6 exited non-zero (thresholds breached?) - see $K6_LOG"
trap - EXIT
end_total=$(metric_sum inferroute_requests_total)
end_err=$(metric_sum inferroute_requests_total 'status="5')
end_fail=$(metric_sum inferroute_failovers_total)

window_total=$(( b_total - a_total ))
window_err=$(( b_err - a_err ))
window_rate=$(awk -v e="$window_err" -v t="$window_total" 'BEGIN { printf "%.3f", t ? 100*e/t : 0 }')
k6_field() { grep -o "\"$1\": *[^,}]*" "$K6_JSON" 2>/dev/null | head -1 | sed 's/.*: *//'; }
overall_err=$(awk -v r="$(k6_field error_rate)" 'BEGIN { printf "%.3f", 100*r }')

{
  echo "## Chaos run $STAMP"
  echo
  echo "Load: ${RATE} req/s for ${DURATION}; ${VICTIM} SIGKILLed at t+${KILL_AT}s, restarted after ${DOWN_FOR}s."
  echo
  echo "| Metric | Value |"
  echo "|---|---|"
  echo "| Throughput (achieved) | $(k6_field achieved_rps) req/s |"
  echo "| Latency p50 / p95 / p99 (200s) | $(k6_field p50_ms) / $(k6_field p95_ms) / $(k6_field p99_ms) ms |"
  echo "| Requests during run (router) | $(( end_total - base_total )) |"
  echo "| Failovers, whole run | $(( end_fail - base_fail )) |"
  echo "| Failovers, during outage window | $(( b_fail - a_fail )) |"
  echo "| 5xx during outage window | ${window_err} / ${window_total} (${window_rate}%) |"
  echo "| 5xx error rate, whole run (k6) | ${overall_err}% |"
  echo "| 429 rate-limited share (k6) | $(awk -v r="$(k6_field rate_limited_rate)" 'BEGIN { printf "%.2f", 100*r }')% |"
  echo "| Detection time (kill -> unhealthy) | ${detect_ms} ms |"
  echo "| Recovery time (restart -> breaker closed) | ${recovery_ms} ms |"
  echo
  echo "Raw k6 output: \`$K6_LOG\`"
} | tee "$REPORT"
