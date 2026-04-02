#!/usr/bin/env sh
set -eu

SCENARIO="${1:-smoke}"
CLIENTS="${CLIENTS:-}"
REQUESTS="${REQUESTS:-}"
DATA_SIZE="${DATA_SIZE:-128}"
PUBLISHERS="${PUBLISHERS:-4}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.observability.yml}"

case "$SCENARIO" in
  smoke)
    TESTS="ping,set,get"
    CLIENTS="${CLIENTS:-50}"
    REQUESTS="${REQUESTS:-20000}"
    MODE="redis-benchmark"
    ;;
  read-heavy)
    TESTS="ping,get,mget"
    CLIENTS="${CLIENTS:-100}"
    REQUESTS="${REQUESTS:-50000}"
    MODE="redis-benchmark"
    ;;
  write-heavy)
    TESTS="set,incr,lpush,rpush"
    CLIENTS="${CLIENTS:-100}"
    REQUESTS="${REQUESTS:-50000}"
    MODE="redis-benchmark"
    ;;
  replication)
    CLIENTS="${CLIENTS:-20}"
    REQUESTS="${REQUESTS:-500}"
    MODE="replication"
    ;;
  pubsub)
    CLIENTS="${CLIENTS:-20}"
    REQUESTS="${REQUESTS:-500}"
    MODE="pubsub"
    ;;
  *)
    echo "Unknown scenario: $SCENARIO" >&2
    echo "Expected one of: smoke, read-heavy, write-heavy, replication, pubsub" >&2
    exit 1
    ;;
esac

case "$MODE" in
  redis-benchmark)
    docker compose -f "$COMPOSE_FILE" up -d --wait app prometheus grafana
    docker compose -f "$COMPOSE_FILE" run --rm benchmark \
      -h app \
      -p 6379 \
      -t "$TESTS" \
      -c "$CLIENTS" \
      -n "$REQUESTS" \
      -d "$DATA_SIZE"
    ;;
  replication)
    docker compose -f "$COMPOSE_FILE" up -d --wait app app-replica prometheus grafana
    python scripts/perf/replication_benchmark.py \
      --master-host 127.0.0.1 \
      --master-port 6379 \
      --replica-host 127.0.0.1 \
      --replica-port 6380 \
      --messages "$REQUESTS" \
      --concurrency "$CLIENTS" \
      --payload-size "$DATA_SIZE"
    ;;
  pubsub)
    docker compose -f "$COMPOSE_FILE" up -d --wait app prometheus grafana
    python scripts/perf/pubsub_benchmark.py \
      --host 127.0.0.1 \
      --port 6379 \
      --subscribers "$CLIENTS" \
      --publishers "$PUBLISHERS" \
      --messages "$REQUESTS" \
      --payload-size "$DATA_SIZE"
    ;;
esac
