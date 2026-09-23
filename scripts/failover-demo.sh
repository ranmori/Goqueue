#!/usr/bin/env bash
# Kills the current GoQueue leader and times how long it takes a follower
# to take over. Recorded output: docs/failover-demo.md.
#
#   ./scripts/failover-demo.sh          # kill -9 the leader (lease must expire, ~TTL)
#   ./scripts/failover-demo.sh stop     # SIGTERM the leader (explicit resign, ~instant)
set -euo pipefail
cd "$(dirname "$0")/.."

MODE="${1:-kill}"
declare -A PORT=([goqueue-1]=8081 [goqueue-2]=8082)

leader_of() { curl -fs "localhost:$1/leader" | grep -o '"leader":"[^"]*"' | cut -d'"' -f4; }
is_leader() { curl -fs "localhost:$1/leader" | grep -q '"is_leader":true'; }
now_ms()    { date +%s%3N; }

echo "==> starting 3-node etcd, postgres, goqueue-1, goqueue-2"
docker compose up -d --build --wait

until [ -n "$(leader_of 8081 2>/dev/null)" ]; do sleep 0.2; done
LEADER=$(leader_of 8081)
FOLLOWER=goqueue-2; [ "$LEADER" = goqueue-2 ] && FOLLOWER=goqueue-1
echo "==> leader: $LEADER   follower: $FOLLOWER"

echo "==> enqueueing 12 jobs through the follower (any node accepts writes)"
for i in $(seq 1 12); do
  curl -fs -o /dev/null -X POST "localhost:${PORT[$FOLLOWER]}/jobs" \
    -d "{\"type\":\"report\",\"payload\":{\"report_name\":\"r$i\"}}"
done
sleep 1 # let the leader claim a batch so some jobs are mid-flight

if [ "$MODE" = stop ]; then
  echo "==> docker stop $LEADER (SIGTERM: finish in-flight jobs, then resign)"
  T0=$(now_ms); docker stop "$LEADER" >/dev/null &
else
  echo "==> docker kill -s KILL $LEADER (no cleanup: lease must expire)"
  T0=$(now_ms); docker kill -s KILL "$LEADER" >/dev/null
fi

until is_leader "${PORT[$FOLLOWER]}"; do sleep 0.05; done
echo "==> $FOLLOWER became leader $(( $(now_ms) - T0 ))ms after the signal"
wait

echo "==> waiting for the backlog to drain"
until curl -fs "localhost:${PORT[$FOLLOWER]}/stats" | grep -q '"pending":0,"running":0'; do sleep 0.5; done
curl -fs "localhost:${PORT[$FOLLOWER]}/stats"; echo

echo "==> election log lines"
docker compose logs --no-color goqueue-1 goqueue-2 |
  grep -E 'became leader|observed leader|campaigning|resigned|in-flight|dispatcher' || true
