#!/usr/bin/env bash
# run-scenario.sh - Run ONE test scenario and print the correlation/RCA result.
#
# Usage:
#   ./scripts/run-scenario.sh happy                  # SC01: happy path
#   ./scripts/run-scenario.sh payment-fail           # SC02: payment failure (retries + compensation)
#   ./scripts/run-scenario.sh payment-fail-once      # SC03: transient payment failure
#   ./scripts/run-scenario.sh inject-timeout         # SC04: inject out-of-order / timeout events
#   ./scripts/run-scenario.sh inject-duplicate       # SC05: inject duplicate event
#
# All scenarios wait for the saga to settle, then print the full saga detail
# (status, anomalies, RCA result) from correlation-service.

set -e
cd "$(dirname "$0")/.."

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

ORDER_SVC="http://localhost:8081"
PAYMENT_SVC="http://localhost:8082"
CORR_SVC="http://localhost:8084"

SCENARIO="${1:-happy}"

# ─── Helpers ──────────────────────────────────────────────────────────────────

require_jq() {
  command -v jq &>/dev/null || { echo -e "${RED}jq is required. Install: brew install jq / apt install jq${NC}"; exit 1; }
}

check_services() {
  for url in "$ORDER_SVC/actuator/health" "$CORR_SVC/actuator/health"; do
    status=$(curl -s -o /dev/null -w "%{http_code}" --connect-timeout 3 "$url" 2>/dev/null || echo "000")
    if [ "$status" != "200" ]; then
      echo -e "${RED}Service not ready: $url (HTTP $status)${NC}"
      echo "Run: ./scripts/wait-ready.sh"
      exit 1
    fi
  done
}

create_order() {
  local customer="${1:-cust-$(shuf -i 1000-9999 -n 1)}"
  local product="${2:-prod-A}"
  local qty="${3:-1}"
  local amount="${4:-99.99}"

  curl -s -X POST "$ORDER_SVC/api/orders" \
    -H "Content-Type: application/json" \
    -d "{\"customerId\":\"$customer\",\"productId\":\"$product\",\"quantity\":$qty,\"amount\":$amount}"
}

wait_saga_settled() {
  local saga_id="$1"
  local max_wait="${2:-30}"
  local poll=3
  local elapsed=0
  local terminal=("COMPLETED" "FAILED" "COMPENSATED" "COMPENSATING")  # compensating may still be in-flight

  echo -e "  Waiting for saga ${CYAN}${saga_id:0:8}…${NC} to settle (max ${max_wait}s)"

  while true; do
    resp=$(curl -s "$CORR_SVC/api/sagas/$saga_id" 2>/dev/null)
    status=$(echo "$resp" | jq -r '.status // "UNKNOWN"' 2>/dev/null || echo "UNKNOWN")

    # COMPENSATED / COMPLETED / FAILED are truly terminal
    if [[ "$status" == "COMPLETED" || "$status" == "FAILED" || "$status" == "COMPENSATED" ]]; then
      echo -e "  Saga settled: ${BOLD}$status${NC} (${elapsed}s)"
      return 0
    fi

    if [ $elapsed -ge $max_wait ]; then
      echo -e "  ${YELLOW}Timed out waiting. Last status: $status${NC}"
      return 1
    fi
    sleep $poll
    elapsed=$((elapsed + poll))
    echo -e "  … ${elapsed}s  status=$status"
  done
}

print_saga_result() {
  local saga_id="$1"
  resp=$(curl -s "$CORR_SVC/api/sagas/$saga_id")

  echo ""
  echo -e "${BOLD}════════════ SAGA RESULT ════════════${NC}"
  echo "$resp" | jq '{
    sagaId: .sagaId,
    status: .status,
    durationMs: .durationMs,
    events: [ .events[]? | {eventType, stepName, serviceName, timestamp} ],
    anomalies: [ .anomalies[]? | {anomalyType, detectedAt, description} ],
    rca: [ .rcaResults[]? | {rootCause, outcome, confidence, explanation} ]
  }'
  echo -e "${BOLD}═════════════════════════════════════${NC}"
}

trigger_rca_if_anomalies() {
  local saga_id="$1"
  anomaly_count=$(curl -s "$CORR_SVC/api/sagas/$saga_id" | jq '.anomalies | length' 2>/dev/null || echo 0)
  if [ "$anomaly_count" -gt 0 ]; then
    echo -e "  ${YELLOW}Anomalies detected ($anomaly_count). Triggering manual RCA...${NC}"
    curl -s -X POST "$CORR_SVC/api/admin/analyze/$saga_id" > /dev/null
    sleep 2
  fi
}

# ─── Scenarios ────────────────────────────────────────────────────────────────

scenario_happy() {
  echo -e "${GREEN}${BOLD}[SC01] Happy Path${NC}"
  echo "  Creates a normal order. Expects: COMPLETED, no anomalies."
  echo ""

  resp=$(create_order "cust-happy" "prod-A" 1 "99.99")
  echo "  Order response: $(echo "$resp" | jq -c '{orderId, sagaId, status}')"
  saga_id=$(echo "$resp" | jq -r '.sagaId')

  wait_saga_settled "$saga_id" 20
  print_saga_result "$saga_id"
}

scenario_payment_fail() {
  echo -e "${RED}${BOLD}[SC02] Payment Failure (all retries exhausted)${NC}"
  echo "  Enables payment fail-mode before creating order."
  echo "  Expects: FAILED, RCA=CIRCUIT_BREAKER_OPEN (R01)"
  echo ""

  echo -e "  Enabling payment fail-mode..."
  curl -s -X POST "$PAYMENT_SVC/api/admin/fail-mode?enabled=true" | jq '{failMode, message}'
  echo ""

  resp=$(create_order "cust-failpay" "prod-B" 2 "250.00")
  echo "  Order response: $(echo "$resp" | jq -c '{orderId, sagaId, status}')"
  saga_id=$(echo "$resp" | jq -r '.sagaId')

  # Payment has 3 retries with exponential backoff (1s, 2s, 4s) → ~7-10s before PAYMENT_FAILED
  # Then compensation runs. Allow 40s total.
  wait_saga_settled "$saga_id" 45
  trigger_rca_if_anomalies "$saga_id"
  print_saga_result "$saga_id"

  echo -e "\n  Restoring payment to normal..."
  curl -s -X POST "$PAYMENT_SVC/api/admin/fail-mode?enabled=false" | jq '{failMode}'
}

scenario_payment_fail_once() {
  echo -e "${YELLOW}${BOLD}[SC03] Transient Payment Failure (fail-mode on then off quickly)${NC}"
  echo "  Payment fails once, then recovers. Tests retry logic."
  echo "  Expects: COMPLETED (retry succeeded) or COMPENSATED if timing unlucky."
  echo ""

  echo -e "  Enabling fail-mode for 3s then disabling..."
  curl -s -X POST "$PAYMENT_SVC/api/admin/fail-mode?enabled=true" > /dev/null
  
  resp=$(create_order "cust-transient" "prod-C" 1 "50.00")
  echo "  Order response: $(echo "$resp" | jq -c '{orderId, sagaId, status}')"
  saga_id=$(echo "$resp" | jq -r '.sagaId')

  sleep 3
  curl -s -X POST "$PAYMENT_SVC/api/admin/fail-mode?enabled=false" > /dev/null
  echo "  Fail-mode disabled."

  wait_saga_settled "$saga_id" 30
  trigger_rca_if_anomalies "$saga_id"
  print_saga_result "$saga_id"
}

scenario_inject_timeout() {
  echo -e "${CYAN}${BOLD}[SC04] Inject Synthetic: Missing step (saga stuck)${NC}"
  echo "  Injects ORDER_CREATED then nothing. Correlation service should detect timeout."
  echo "  Expects: SAGA_STUCK anomaly after step timeout elapses."
  echo ""

  saga_id="inject-$(uuidgen | tr '[:upper:]' '[:lower:]' | cut -c1-8)-$(date +%s)"
  echo "  Using synthetic sagaId: $saga_id"

  # Inject ORDER_CREATED — this starts the saga in correlation engine
  echo -e "  Injecting ORDER_CREATED..."
  curl -s -X POST "$CORR_SVC/api/admin/inject-event" \
    -H "Content-Type: application/json" \
    -d "{
      \"sagaId\":\"$saga_id\",
      \"eventType\":\"ORDER_CREATED\",
      \"stepName\":\"CREATE_ORDER\",
      \"serviceName\":\"order-service\",
      \"payload\":\"{\\\"orderId\\\":\\\"test-order-1\\\"}\"
    }" | jq '{status, sagaId, eventType}'

  # No follow-up events → PAYMENT step never starts
  # CREATE_ORDER timeout=5s, detectStuckSagas() runs every 30s
  echo "  Waiting 35s for SAGA_STUCK detection..."
  sleep 35

  # Manually trigger RCA on this saga
  echo -e "  Triggering RCA..."
  curl -s -X POST "$CORR_SVC/api/admin/analyze/$saga_id" | jq '.' 2>/dev/null || echo "  (RCA triggered async)"
  sleep 2

  print_saga_result "$saga_id"
}

scenario_inject_duplicate() {
  echo -e "${CYAN}${BOLD}[SC05] Inject Synthetic: Duplicate event${NC}"
  echo "  Sends the same event twice with same eventId. Deduplication should discard the second."
  echo ""

  saga_id="dup-$(uuidgen | tr '[:upper:]' '[:lower:]' | cut -c1-8)-$(date +%s)"
  event_id="evt-$(uuidgen | tr '[:upper:]' '[:lower:]' | cut -c1-8)"
  echo "  sagaId=$saga_id, eventId=$event_id"

  for i in 1 2; do
    echo -e "  Injecting ORDER_CREATED (attempt $i)..."
    curl -s -X POST "$CORR_SVC/api/admin/inject-event" \
      -H "Content-Type: application/json" \
      -d "{
        \"sagaId\":\"$saga_id\",
        \"eventId\":\"$event_id\",
        \"eventType\":\"ORDER_CREATED\",
        \"stepName\":\"CREATE_ORDER\",
        \"serviceName\":\"order-service\",
        \"payload\":\"{}\"
      }" | jq '{status, eventId}'
    sleep 1
  done

  echo "  Waiting 5s..."
  sleep 5
  trigger_rca_if_anomalies "$saga_id"
  print_saga_result "$saga_id"
  echo ""
  echo "  ↑ Events list should have exactly 1 entry (duplicate discarded by dedup cache)."
  echo "  ↑ RCA should be DUPLICATE_DELIVERY (R13) if EXACT_DUPLICATE_EVENT anomaly detected."
}

# ─── Main ─────────────────────────────────────────────────────────────────────

require_jq
check_services

echo ""
echo -e "${BOLD}Running scenario: $SCENARIO${NC}"
echo "──────────────────────────────────────"

case "$SCENARIO" in
  happy)                scenario_happy ;;
  payment-fail)         scenario_payment_fail ;;
  payment-fail-once)    scenario_payment_fail_once ;;
  inject-timeout)       scenario_inject_timeout ;;
  inject-duplicate)     scenario_inject_duplicate ;;
  *)
    echo -e "${RED}Unknown scenario: $SCENARIO${NC}"
    echo "Available: happy | payment-fail | payment-fail-once | inject-timeout | inject-duplicate"
    exit 1
    ;;
esac
