#!/usr/bin/env bash
# End-to-end check of the outbox pipeline with no service involved:
#   row in outbox_events  ->  Debezium EventRouter  ->  Kafka record (key, headers id/type, JSON value)
# Uses a throwaway database (smoke_db) and removes everything afterwards.
# Run from the infra/ directory with the stack up:  docker compose up -d --wait kafka connect product-db
set -euo pipefail

cd "$(dirname "$0")/.."
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
MIGRATION="../common/src/main/resources/db/common/V1__outbox_and_inbox.sql"
EVENT_ID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
SAGA_ID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
PSQL=(docker compose exec -T product-db psql -v ON_ERROR_STOP=1 -U postgres)

cleanup() {
  curl -fsS -X DELETE "$CONNECT_URL/connectors/smoke-outbox" > /dev/null 2>&1 || true
  sleep 2
  "${PSQL[@]}" -d postgres -c "SELECT pg_drop_replication_slot('smoke_outbox_slot') FROM pg_replication_slots WHERE slot_name = 'smoke_outbox_slot'" > /dev/null 2>&1 || true
  "${PSQL[@]}" -d postgres -c "DROP DATABASE IF EXISTS smoke_db" > /dev/null 2>&1 || true
}
trap cleanup EXIT

echo "1/5 create smoke_db and apply the common migration"
"${PSQL[@]}" -d postgres -c "DROP DATABASE IF EXISTS smoke_db" -c "CREATE DATABASE smoke_db" > /dev/null
"${PSQL[@]}" -d smoke_db < "$MIGRATION" > /dev/null

echo "2/5 register the smoke connector"
curl -fsS -X PUT -H 'Content-Type: application/json' --data @debezium/smoke/smoke-outbox.json \
  "$CONNECT_URL/connectors/smoke-outbox/config" > /dev/null

echo "3/5 wait for the connector to run"
for _ in $(seq 1 60); do
  if curl -fsS "$CONNECT_URL/connectors/smoke-outbox/status" 2>/dev/null | grep -q '"state":"RUNNING"'; then
    break
  fi
  sleep 2
done
curl -fsS "$CONNECT_URL/connectors/smoke-outbox/status" | grep -q '"state":"RUNNING"' \
  || { echo "FAIL: connector is not RUNNING"; curl -fsS "$CONNECT_URL/connectors/smoke-outbox/status"; exit 1; }

echo "4/5 insert an outbox row"
"${PSQL[@]}" -d smoke_db > /dev/null <<SQL
INSERT INTO outbox_events (id, topic, aggregatetype, aggregateid, type, payload)
VALUES ('$EVENT_ID', 'smoke.commands', 'Order', 'order-42', 'ValidateProducts',
        '{"sagaId":"$SAGA_ID","correlationId":"$SAGA_ID","replyTo":"order.saga.replies","data":{"hello":"world"}}');
SQL

echo "5/5 read the Kafka record"
OUTPUT="$(docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic smoke.commands --from-beginning --max-messages 1 --timeout-ms 60000 \
  --property print.headers=true --property print.key=true 2>/dev/null)"
echo "$OUTPUT"

fail=0
check() {
  if ! grep -q -- "$1" <<< "$OUTPUT"; then
    echo "FAIL: expected '$1'"
    fail=1
  fi
}
check "id:$EVENT_ID"
check "type:ValidateProducts"
check "order-42"
check "\"replyTo\":\"order.saga.replies\""
check "\"data\":{\"hello\":\"world\"}"

if [ "$fail" -eq 0 ]; then
  echo "PASS: outbox row became a Kafka record with headers id/type, key order-42 and an expanded JSON value"
else
  exit 1
fi
