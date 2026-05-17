#!/usr/bin/env bash
# Demo: real service crash detection for SERVICE_CRASH RCA.
# Uses docker stop/start against payment-service; no admin event injection.

set -euo pipefail

cd "$(dirname "$0")/.."

ALERT_LOG="results/alerts-realtime.log"
mkdir -p results
: > "$ALERT_LOG"

cleanup() {
    if [[ -n "${TAIL_PID:-}" ]]; then
        kill "$TAIL_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

wait_for_health() {
    local url="$1"
    local name="$2"
    local max_attempts="${3:-90}"
    local attempt=1

    until curl -fsS "$url" 2>/dev/null | grep -q '"status":"UP"'; do
        if (( attempt >= max_attempts )); then
            echo "Timed out waiting for $name at $url" >&2
            return 1
        fi
        sleep 2
        attempt=$((attempt + 1))
    done
}

post_order() {
    local customer="$1"
    curl -fsS -X POST http://localhost:8081/api/orders \
        -H "Content-Type: application/json" \
        -d "{\"customerId\":\"$customer\",\"productId\":\"PRODUCT-1\",\"quantity\":1,\"amount\":50}" \
        >/dev/null
}

echo "=== Demo: SERVICE_CRASH detection (real failure, no inject) ==="

echo ">>> Reset and start stack"
docker compose down -v
docker compose up -d --build

echo ">>> Waiting for core services"
wait_for_health "http://localhost:8081/actuator/health" "order-service"
wait_for_health "http://localhost:8082/actuator/health" "payment-service"
wait_for_health "http://localhost:8083/actuator/health" "inventory-service"
wait_for_health "http://localhost:8084/actuator/health" "correlation-service"
sleep 5

echo ">>> Reset inventory"
docker exec demo-postgres psql -U demo -d inventory_db -c \
  "UPDATE inventory SET available_quantity = 1000 WHERE product_id = 'PRODUCT-1';" >/dev/null

echo ">>> [T+0s] Sending 3 healthy baseline orders"
for i in 1 2 3; do
    post_order "demo-baseline-$i"
    sleep 2
done

echo ">>> Tailing alert log in background: $ALERT_LOG"
tail -F "$ALERT_LOG" &
TAIL_PID=$!

echo
echo ">>> [T+15s] CRASH: stopping payment-service at $(date +%T)"
docker stop payment-service >/dev/null
CRASH_TIME=$(date +%s)

echo ">>> [T+16s] Sending 3 orders while payment-service is down"
for i in 4 5 6; do
    post_order "demo-crash-$i" || true
    sleep 1
done

echo ">>> Waiting 150s for anomaly detection and auto-RCA"
sleep 150

echo
echo ">>> [T+180s] RECOVERY: starting payment-service at $(date +%T)"
docker start payment-service >/dev/null
wait_for_health "http://localhost:8082/actuator/health" "payment-service"

echo ">>> Waiting 60s for recovery processing"
sleep 60

cleanup

echo
echo "=============================="
echo "DEMO RESULTS"
echo "=============================="

echo
echo ">>> Anomalies fired during demo:"
docker exec demo-postgres psql -U demo -d correlation_db -P pager=off -c \
  "SELECT detected_at, saga_id, anomaly_type
   FROM anomalies
   WHERE detected_at > to_timestamp($CRASH_TIME - 10)
   ORDER BY detected_at;"

echo
echo ">>> RCA results auto-generated:"
docker exec demo-postgres psql -U demo -d correlation_db -P pager=off -c \
  "SELECT created_at, saga_id, root_cause, rca_status, primary_root_causes, severity
   FROM rca_results
   WHERE created_at > to_timestamp($CRASH_TIME - 10)
   ORDER BY created_at;"

echo
echo ">>> Alert log content ($ALERT_LOG):"
cat "$ALERT_LOG"

echo
echo ">>> Saga statuses after recovery:"
docker exec demo-postgres psql -U demo -d correlation_db -P pager=off -c \
  "SELECT saga_id, status, start_time, end_time
   FROM saga_instances
   WHERE start_time > to_timestamp($CRASH_TIME - 30)
   ORDER BY start_time;"

echo
echo "=== Demo complete ==="
