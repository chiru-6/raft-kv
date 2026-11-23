#!/usr/bin/env bash
# Start, stop or inspect a local 3-node raft-kv cluster.
#
#   scripts/cluster.sh start     build and start n1..n3 on ports 8001..8003
#   scripts/cluster.sh stop      stop all nodes
#   scripts/cluster.sh kill n2   kill one node (simulate a crash)
#   scripts/cluster.sh restart n2
#   scripts/cluster.sh status
set -euo pipefail
cd "$(dirname "$0")/.."

PEERS="n1=127.0.0.1:8001,n2=127.0.0.1:8002,n3=127.0.0.1:8003"
RUN=.cluster

build() { go build -o bin/ ./cmd/...; }

start_node() {
  local id=$1
  mkdir -p "$RUN/logs"
  ./bin/kvnode -id "$id" -peers "$PEERS" -data "$RUN/data/$id" -v >"$RUN/logs/$id.log" 2>&1 &
  echo $! >"$RUN/$id.pid"
  echo "started $id (pid $!, log $RUN/logs/$id.log)"
}

stop_node() {
  local id=$1
  if [[ -f "$RUN/$id.pid" ]]; then
    kill "$(cat "$RUN/$id.pid")" 2>/dev/null || true
    rm -f "$RUN/$id.pid"
    echo "stopped $id"
  fi
}

case "${1:-}" in
  start)
    build
    for id in n1 n2 n3; do start_node "$id"; done
    sleep 1
    ./bin/kvctl status
    ;;
  stop)
    for id in n1 n2 n3; do stop_node "$id"; done
    ;;
  kill)
    stop_node "${2:?node id}"
    ;;
  restart)
    stop_node "${2:?node id}"
    start_node "$2"
    ;;
  status)
    ./bin/kvctl status
    ;;
  clean)
    "$0" stop
    rm -rf "$RUN"
    ;;
  *)
    sed -n '2,8p' "$0"
    exit 2
    ;;
esac
