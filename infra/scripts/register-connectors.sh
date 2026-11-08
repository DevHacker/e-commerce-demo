#!/usr/bin/env bash
# Registers (or updates) the Debezium outbox connectors.
# Run it AFTER each service has started once, so Flyway created its outbox_events table.
#
#   ./scripts/register-connectors.sh                 # all four connectors
#   ./scripts/register-connectors.sh product order   # only the named ones
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
DIR="$(cd "$(dirname "$0")/.." && pwd)/debezium"

names=("$@")
if [ ${#names[@]} -eq 0 ]; then
  names=(product order inventory payment)
fi

echo "Waiting for Kafka Connect at $CONNECT_URL ..."
for _ in $(seq 1 60); do
  if curl -fsS "$CONNECT_URL/connectors" > /dev/null 2>&1; then
    break
  fi
  sleep 2
done
curl -fsS "$CONNECT_URL/connectors" > /dev/null || { echo "Kafka Connect is not reachable" >&2; exit 1; }

for service in "${names[@]}"; do
  connector="$service-outbox"
  file="$DIR/$connector.json"
  [ -f "$file" ] || { echo "No such connector definition: $file" >&2; exit 1; }
  echo "Registering $connector"
  curl -fsS -X PUT -H 'Content-Type: application/json' --data @"$file" \
    "$CONNECT_URL/connectors/$connector/config" > /dev/null
done

echo
curl -fsS "$CONNECT_URL/connectors?expand=status" | python3 -m json.tool
