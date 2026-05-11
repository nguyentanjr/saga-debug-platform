#!/usr/bin/env bash
# =============================================================================
# run-experiment.sh
# Chạy toàn bộ test scenarios tuần tự, reset state giữa mỗi SC,
# collect kết quả ra file JSON và in summary table cuối.
#
# Usage:
#   ./scripts/run-experiment.sh            # chạy tất cả SC01-SC13
#   ./scripts/run-experiment.sh SC01 SC03  # chỉ chạy SC được chỉ định
#
# Prerequisites: jq, curl, python3 (hoặc /proc/sys/kernel/random/uuid)
# =============================================================================

set -e
cd "$(dirname "$0")/.."

# ── Colors ────────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; DIM='\033[2m'; NC='\033[0m'

# ── Config ────────────────────────────────────────────────────────────────────
ORDER_SVC="http://localhost:8081"
PAYMENT_SVC="http://localhost:8082"
CORR_SVC="http://localhost:8084"

RESULTS_DIR="./results"
RUN_TS=$(date +%Y%m%d_%H%M%S)
RESULTS_FILE="$RESULTS_DIR/run_${RUN_TS}.jsonl"
SUMMARY_FILE="$RESULTS_DIR/summary_${RUN_TS}.txt"

mkdir -p "$RESULTS_DIR"

# ── Global state ──────────────────────────────────────────────────────────────
declare -A SC_PASS   # SC_PASS[SC01]="PASS" or "FAIL"
declare -A SC_STATUS
declare -A SC_ANOMALIES
declare -A SC_RCA
declare -A SC_SAGA_ID
declare -A SC_DURATION_MS

# ── Helpers ───────────────────────────────────────────────────────────────────

log()     { echo -e "${DIM}$(date +%H:%M:%S)${NC} $*"; }
info()    { echo -e "${GREEN}[✓]${NC} $*"; }
warn()    { echo -e "${YELLOW}[!]${NC} $*"; }
die()     { echo -e "${RED}[✗]${NC} $*" >&2; exit 1; }
section() { echo -e "\n${BOLD}${CYAN}━━━ $* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; }

gen_uuid() {
  python3 -c "import uuid; print(uuid.uuid4())" 2>/dev/null \
  || cat /proc/sys/kernel/random/uuid 2>/dev/null \
  || echo "test-$(date +%s)-$RANDOM"
}

require_tools() {
  command -v jq   &>/dev/null || die "jq is required: brew install jq / apt install jq"
  command -v curl &>/dev/null || die "curl is required"
}

check_services() {
  for svc in "$ORDER_SVC" "$PAYMENT_SVC" "$CORR_SVC"; do
    code=$(curl -s -o /dev/null -w "%{http_code}" --connect-timeout 3 "$svc/actuator/health" 2>/dev/null || echo 000)
    [ "$code" != "200" ] && die "Service not ready: $svc (HTTP $code). Run ./scripts/wait-ready.sh first."
  done
  info "All services healthy"
}

# POST /api/orders, trả về raw JSON response
create_order() {
  local customer="${1}" product="${2}" qty="${3}" amount="${4}"
  curl -s --connect-timeout 5 -X POST "$ORDER_SVC/api/orders" \
    -H "Content-Type: application/json" \
    -d "{\"customerId\":\"$customer\",\"productId\":\"$product\",\"quantity\":$qty,\"amount\":$amount}"
}

# Inject event trực tiếp vào correlation engine (bypass Kafka)
inject_event() {
  local saga_id="$1" event_type="$2" step="$3" service="$4" payload="${5:-{}}" event_id="${6:-}" ts="${7:-}"
  local body="{\"sagaId\":\"$saga_id\",\"eventType\":\"$event_type\",\"stepName\":\"$step\",\"serviceName\":\"$service\",\"payload\":\"$payload\""
  [ -n "$event_id" ] && body="$body,\"eventId\":\"$event_id\""
  [ -n "$ts" ]       && body="$body,\"timestamp\":\"$ts\""
  body="$body}"
  curl -s --connect-timeout 5 -X POST "$CORR_SVC/api/admin/inject-event" \
    -H "Content-Type: application/json" -d "$body"
}

# Chờ saga đạt trạng thái terminal hoặc timeout
wait_settled() {
  local saga_id="$1" max_s="${2:-30}" sc_name="${3:-}"
  local elapsed=0 poll=3 status
  while true; do
    status=$(curl -s --connect-timeout 3 "$CORR_SVC/api/sagas/$saga_id" 2>/dev/null \
             | jq -r '.status // "UNKNOWN"' 2>/dev/null || echo "UNKNOWN")
    case "$status" in
      COMPLETED|FAILED|COMPENSATED)
        log "$sc_name → settled: ${BOLD}$status${NC} (${elapsed}s)"
        return 0 ;;
    esac
    [ $elapsed -ge $max_s ] && { warn "$sc_name → timeout waiting (last: $status)"; return 1; }
    sleep $poll; elapsed=$((elapsed + poll))
    log "$sc_name → ${elapsed}s  status=$status"
  done
}

# Kéo result từ correlation service, save vào JSONL
collect_result() {
  local sc_id="$1" sc_name="$2" saga_id="$3" expected_status="$4" expected_rca="$5"
  local detail rca_result anomaly_list actual_status duration_ms

  detail=$(curl -s --connect-timeout 5 "$CORR_SVC/api/sagas/$saga_id" 2>/dev/null || echo "{}")
  actual_status=$(echo "$detail" | jq -r '.status // "UNKNOWN"')
  duration_ms=$(echo "$detail" | jq -r '.durationMs // "null"')
  anomaly_list=$(echo "$detail" | jq -c '[.anomalies[]? | .anomalyType] | unique' 2>/dev/null || echo "[]")
  rca_result=$(echo "$detail" | jq -r '[.rcaResults[]? | .rootCause] | first // "NONE"' 2>/dev/null || echo "NONE")

  # So sánh expected vs actual
  local pass="PASS"
  [ "$actual_status" != "$expected_status" ] && pass="FAIL"

  SC_PASS[$sc_id]="$pass"
  SC_STATUS[$sc_id]="$actual_status"
  SC_ANOMALIES[$sc_id]="$anomaly_list"
  SC_RCA[$sc_id]="$rca_result"
  SC_SAGA_ID[$sc_id]="$saga_id"
  SC_DURATION_MS[$sc_id]="$duration_ms"

  # Append to JSONL
  echo "{\"sc\":\"$sc_id\",\"name\":\"$sc_name\",\"sagaId\":\"$saga_id\",\"expectedStatus\":\"$expected_status\",\"actualStatus\":\"$actual_status\",\"expectedRca\":\"$expected_rca\",\"actualRca\":\"$rca_result\",\"anomalies\":$anomaly_list,\"durationMs\":$duration_ms,\"pass\":\"$pass\",\"ts\":\"$(date -u +%FT%TZ)\"}" \
    >> "$RESULTS_FILE"

  # Print inline
  local color="$GREEN"
  [ "$pass" = "FAIL" ] && color="$RED"
  echo -e "  Status   : ${BOLD}$actual_status${NC}  (expected: $expected_status)"
  echo -e "  Anomalies: $anomaly_list"
  echo -e "  RCA      : $rca_result"
  echo -e "  Duration : ${duration_ms}ms"
  echo -e "  Result   : ${color}${BOLD}$pass${NC}"
}

# Reset state giữa các scenario
reset_state() {
  curl -s -X POST "$PAYMENT_SVC/api/admin/fail-mode?enabled=false" > /dev/null 2>&1 || true
  # Nếu có service bị pause, unpause (safety)
  docker unpause payment-service   > /dev/null 2>&1 || true
  docker unpause inventory-service > /dev/null 2>&1 || true
  # Chờ consumer groups settle
  sleep 6
}

# ── Scenarios ─────────────────────────────────────────────────────────────────

SC01_happy_path() {
  section "SC01 – Happy Path"
  echo "  Trigger  : POST order với product/qty hợp lệ"
  echo "  Expected : COMPLETED | No anomaly | No RCA"

  local resp saga_id
  resp=$(create_order "cust-happy-$(date +%s)" "prod-A" 1 "99.99")
  saga_id=$(echo "$resp" | jq -r '.sagaId')
  echo "  sagaId   : $saga_id"

  wait_settled "$saga_id" 20 "SC01"
  collect_result "SC01" "Happy Path" "$saga_id" "COMPLETED" "NONE"
}

SC02_payment_circuit_breaker() {
  section "SC02 – Payment Circuit Breaker (all retries exhausted)"
  echo "  Trigger  : fail-mode=true → 3 retries → PAYMENT_FAILED → FAILED"
  echo "  Expected : FAILED | RCA=CIRCUIT_BREAKER_OPEN"

  curl -s -X POST "$PAYMENT_SVC/api/admin/fail-mode?enabled=true" > /dev/null
  log "Payment fail-mode ENABLED"

  local resp saga_id
  resp=$(create_order "cust-cb-$(date +%s)" "prod-B" 1 "199.99")
  saga_id=$(echo "$resp" | jq -r '.sagaId')
  echo "  sagaId   : $saga_id"

  # Resilience4j: 3 retries × backoff ≈ 7s → circuit open → fallback → PAYMENT_FAILED
  # PAYMENT_FAILED is in FAILED_EVENTS → saga transitions to FAILED (no compensation)
  wait_settled "$saga_id" 50 "SC02"

  # Trigger RCA to detect CIRCUIT_BREAKER_OPEN from fallback payload
  curl -s -X POST "$CORR_SVC/api/admin/analyze/$saga_id" > /dev/null 2>&1 || true
  sleep 2

  collect_result "SC02" "Payment Circuit Breaker" "$saga_id" "FAILED" "CIRCUIT_BREAKER_OPEN"

  curl -s -X POST "$PAYMENT_SVC/api/admin/fail-mode?enabled=false" > /dev/null
}

SC03_inventory_failure() {
  section "SC03 – Inventory Failure (unknown product → compensation)"
  echo "  Trigger  : productId không tồn tại → INVENTORY_FAILED → REFUND_PAYMENT → COMPENSATED"
  echo "  Expected : COMPENSATED | RCA=FORWARD_STEP_FAILURE (R11)"

  local resp saga_id
  resp=$(create_order "cust-inv-$(date +%s)" "PROD-GHOST-9999" 1 "50.00")
  saga_id=$(echo "$resp" | jq -r '.sagaId')
  echo "  sagaId   : $saga_id"

  # Payment succeeds (~2s), then inventory fails (~2s), compensation (~5s)
  wait_settled "$saga_id" 30 "SC03"

  curl -s -X POST "$CORR_SVC/api/admin/analyze/$saga_id" > /dev/null 2>&1 || true
  sleep 2

  collect_result "SC03" "Inventory Failure" "$saga_id" "COMPENSATED" "FORWARD_STEP_FAILURE"
}

SC04_payment_service_paused() {
  section "SC04 – Service Unavailable (docker pause payment-service)"
  echo "  Trigger  : Tạo order → pause payment-service → chờ timeout → unpause"
  echo "  Expected : COMPENSATING hoặc SAGA_STUCK | MISSING_COMPLETION"

  local resp saga_id
  resp=$(create_order "cust-pause-$(date +%s)" "prod-A" 1 "75.00")
  saga_id=$(echo "$resp" | jq -r '.sagaId')
  echo "  sagaId   : $saga_id"

  # Chờ 1s để ORDER_CREATED được publish lên Kafka, rồi pause payment trước khi consume
  sleep 1
  log "Pausing payment-service..."
  docker pause payment-service || warn "Could not pause payment-service (check Docker permissions)"

  log "Waiting 18s for step timeout to elapse (payment timeout=10s + buffer)..."
  sleep 18

  log "Unpausing payment-service..."
  docker unpause payment-service || true

  # Sau khi unpause, payment có thể vẫn process và emit PAYMENT_COMPLETED
  # Chờ xem saga settle theo hướng nào
  wait_settled "$saga_id" 25 "SC04"
  collect_result "SC04" "Payment Service Paused" "$saga_id" "COMPLETED" "NONE"
  # Note: actual có thể là COMPLETED (nếu payment xử lý kịp sau unpause)
  # hoặc SAGA_STUCK nếu correlation engine đã đánh dấu stuck
  SC_PASS["SC04"]="INFO"  # overwrite - đây là observation scenario
}

SC05_stuck_saga() {
  section "SC05 – SAGA_STUCK (synthetic inject: ORDER_CREATED only)"
  echo "  Trigger  : Inject ORDER_CREATED, không có follow-up event"
  echo "  Expected : STARTED | SAGA_STUCK anomaly | RCA=STUCK_UNKNOWN_CAUSE (R21)"

  local saga_id
  saga_id="sc05-$(gen_uuid | cut -c1-12)"
  echo "  sagaId   : $saga_id"

  inject_event "$saga_id" "ORDER_CREATED" "CREATE_ORDER" "order-service" \
    "{\\\"orderId\\\":\\\"test-order\\\"}" | jq -r '"  Injected: " + .eventType' 2>/dev/null || true

  # detectStuckSagas() runs every 30s, CREATE_ORDER timeout = 5s
  # Wait 35s to ensure at least one scheduled cycle runs
  log "Waiting 35s for SAGA_STUCK detection (CREATE_ORDER timeout=5s + scheduled cycle 30s)..."
  sleep 35

  curl -s -X POST "$CORR_SVC/api/admin/analyze/$saga_id" > /dev/null 2>&1 || true
  sleep 2

  collect_result "SC05" "SAGA_STUCK (synthetic)" "$saga_id" "STARTED" "STUCK_UNKNOWN_CAUSE"
}

SC06_duplicate_event() {
  section "SC06 – Duplicate Event (same eventId × 2)"
  echo "  Trigger  : Inject 2 lần cùng eventId cho cùng một saga"
  echo "  Expected : STARTED | EXACT_DUPLICATE_EVENT anomaly | RCA=DUPLICATE_DELIVERY (R13)"

  local saga_id event_id
  saga_id="sc06-$(gen_uuid | cut -c1-12)"
  event_id="evt-dup-$(gen_uuid | cut -c1-8)"
  echo "  sagaId   : $saga_id  |  eventId: $event_id"

  for i in 1 2; do
    inject_event "$saga_id" "ORDER_CREATED" "CREATE_ORDER" "order-service" \
      "{\\\"attempt\\\":$i}" "$event_id" \
      | jq -r "\"  Inject #$i: \" + .status" 2>/dev/null || true
    sleep 1
  done

  sleep 3
  curl -s -X POST "$CORR_SVC/api/admin/analyze/$saga_id" > /dev/null 2>&1 || true
  sleep 2

  collect_result "SC06" "Duplicate Event" "$saga_id" "STARTED" "DUPLICATE_DELIVERY"

  # Verify: events list chỉ có 1 entry
  local event_count
  event_count=$(curl -s "$CORR_SVC/api/sagas/$saga_id" | jq '.events | length' 2>/dev/null || echo "?")
  echo "  EventCount: $event_count (expected: 1)"
  [ "$event_count" = "1" ] && info "Deduplication working correctly" || warn "Expected 1 event, got $event_count"
}

SC07_out_of_order() {
  section "SC07 – Out-of-Order Events (timestamp violation)"
  echo "  Trigger  : Inject PAYMENT_COMPLETED trước ORDER_CREATED (timestamp ngược)"
  echo "  Expected : IN_PROGRESS | OUT_OF_ORDER anomaly | RCA=OUT_OF_ORDER_DELIVERY (R14)"

  local saga_id
  saga_id="sc07-$(gen_uuid | cut -c1-12)"
  echo "  sagaId   : $saga_id"

  local ts_old ts_new
  ts_new=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  ts_old=$(date -u -d "60 seconds ago" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null \
        || date -u -v-60S +%Y-%m-%dT%H:%M:%SZ 2>/dev/null \
        || echo "2025-01-01T00:00:00Z")  # fallback cho macOS / Linux

  log "Injecting PAYMENT_COMPLETED (ts=$ts_new) first..."
  inject_event "$saga_id" "PAYMENT_COMPLETED" "PROCESS_PAYMENT" "payment-service" \
    "{}" "" "$ts_new" | jq -r '"  Inject 1: " + .eventType' 2>/dev/null || true

  sleep 1

  log "Injecting ORDER_CREATED (ts=$ts_old, older than above)..."
  inject_event "$saga_id" "ORDER_CREATED" "CREATE_ORDER" "order-service" \
    "{}" "" "$ts_old" | jq -r '"  Inject 2: " + .eventType' 2>/dev/null || true

  sleep 3
  curl -s -X POST "$CORR_SVC/api/admin/analyze/$saga_id" > /dev/null 2>&1 || true
  sleep 2

  # PAYMENT_COMPLETED not in any terminal set → STARTED→IN_PROGRESS, ORDER_CREATED keeps IN_PROGRESS
  collect_result "SC07" "Out-of-Order Events" "$saga_id" "IN_PROGRESS" "OUT_OF_ORDER_DELIVERY"
}

SC08_missing_completion() {
  section "SC08 – Missing Step Completion"
  echo "  Trigger  : Inject ORDER_CREATED → PAYMENT_STARTED (started but never completed)"
  echo "  Expected : IN_PROGRESS | SAGA_STUCK anomaly | RCA=STUCK_UNKNOWN_CAUSE (R21)"

  local saga_id
  saga_id="sc08-$(gen_uuid | cut -c1-12)"
  echo "  sagaId   : $saga_id"

  inject_event "$saga_id" "ORDER_CREATED"   "CREATE_ORDER"   "order-service"  "{}" \
    | jq -r '"  Inject 1: " + .eventType' 2>/dev/null || true
  sleep 1
  inject_event "$saga_id" "PAYMENT_STARTED" "PROCESS_PAYMENT" "payment-service" "{}" \
    | jq -r '"  Inject 2: " + .eventType' 2>/dev/null || true

  # PAYMENT_STARTED sets status IN_PROGRESS. No follow-up event.
  # detectStuckSagas() runs every 30s; PROCESS_PAYMENT timeout = 10s.
  # Wait 40s to cover scheduled cycle + timeout.
  log "Waiting 40s for SAGA_STUCK detection (PROCESS_PAYMENT timeout=10s + scheduled cycle 30s)..."
  sleep 40

  curl -s -X POST "$CORR_SVC/api/admin/analyze/$saga_id" > /dev/null 2>&1 || true
  sleep 2

  collect_result "SC08" "Missing Step Completion" "$saga_id" "IN_PROGRESS" "STUCK_UNKNOWN_CAUSE"
}

SC09_timeout_retry_storm() {
  section "SC09 – Timeout Retry Storm"
  echo "  Trigger  : Inject 3× PAYMENT_FAILED với payload chứa 'timeout' trong 30s"
  echo "  Expected : FAILED | RETRY_BURST anomaly | RCA=TIMEOUT_RETRY_STORM (R02)"

  local saga_id
  saga_id="sc09-$(gen_uuid | cut -c1-12)"
  echo "  sagaId   : $saga_id"

  inject_event "$saga_id" "ORDER_CREATED" "CREATE_ORDER" "order-service" "{}" \
    | jq -r '"  Inject 0: " + .eventType' 2>/dev/null || true
  sleep 1

  # Each PAYMENT_FAILED transitions saga to FAILED (terminal).
  # 2nd and 3rd will also trigger FORBIDDEN_POST_TERMINAL anomaly,
  # but RETRY_BURST (R02, 0.92) outranks TERMINAL_STATE_OVERWRITE (R04, 0.90).
  for i in 1 2 3; do
    log "Injecting PAYMENT_FAILED #$i with timeout payload..."
    inject_event "$saga_id" "PAYMENT_FAILED" "PROCESS_PAYMENT" "payment-service" \
      "{\\\"reason\\\":\\\"timeout: connection timed out\\\",\\\"attempt\\\":$i}" \
      | jq -r "\"  Inject #$i: \" + .eventType" 2>/dev/null || true
    sleep 3
  done

  sleep 3
  curl -s -X POST "$CORR_SVC/api/admin/analyze/$saga_id" > /dev/null 2>&1 || true
  sleep 2

  collect_result "SC09" "Timeout Retry Storm" "$saga_id" "FAILED" "TIMEOUT_RETRY_STORM"
}

SC10_compensation_failed() {
  section "SC10 – Compensation Step Failure"
  echo "  Trigger  : Inject REFUND_PAYMENT nhưng không có PAYMENT_REFUNDED follow-up"
  echo "  Expected : COMPENSATING | MISSING_COMPENSATION_ACK anomaly | RCA=COMPENSATION_STEP_FAILURE (R10)"

  local saga_id
  saga_id="sc10-$(gen_uuid | cut -c1-12)"
  echo "  sagaId   : $saga_id"

  inject_event "$saga_id" "ORDER_CREATED"   "CREATE_ORDER"    "order-service"  "{}" | jq -r '"  "+.eventType' 2>/dev/null || true; sleep 1
  inject_event "$saga_id" "PAYMENT_COMPLETED" "PROCESS_PAYMENT" "payment-service" "{}" | jq -r '"  "+.eventType' 2>/dev/null || true; sleep 1
  inject_event "$saga_id" "INVENTORY_FAILED" "RESERVE_INVENTORY" "inventory-service" \
    "{\\\"reason\\\":\\\"insufficient stock\\\"}" | jq -r '"  "+.eventType' 2>/dev/null || true; sleep 1
  inject_event "$saga_id" "REFUND_PAYMENT" "REFUND_PAYMENT" "order-service" "{}" \
    | jq -r '"  "+.eventType' 2>/dev/null || true

  # detectMissingCompensationAck() runs every 60s with 30s cutoff.
  # Wait 65s to ensure at least one scheduled cycle detects the missing ack.
  log "Waiting 65s for MISSING_COMPENSATION_ACK detection (60s cycle + 30s cutoff)..."
  sleep 65

  curl -s -X POST "$CORR_SVC/api/admin/analyze/$saga_id" > /dev/null 2>&1 || true
  sleep 2

  collect_result "SC10" "Compensation Step Failure" "$saga_id" "COMPENSATING" "COMPENSATION_STEP_FAILURE"
}

SC11_multi_terminal_conflict() {
  section "SC11 – Multi-Terminal Conflict (both success and failure events)"
  echo "  Trigger  : Inject PAYMENT_COMPLETED + INVENTORY_RESERVED (success) + INVENTORY_FAILED (failure)"
  echo "  Expected : COMPENSATING | MULTI_TERMINAL_CONFLICT anomaly | RCA=MULTI_TERMINAL_CONFLICT (R03)"

  local saga_id
  saga_id="sc11-$(gen_uuid | cut -c1-12)"
  echo "  sagaId   : $saga_id"

  inject_event "$saga_id" "ORDER_CREATED" "CREATE_ORDER" "order-service" "{}" \
    | jq -r '"  "+.eventType' 2>/dev/null || true; sleep 1
  inject_event "$saga_id" "PAYMENT_COMPLETED" "PROCESS_PAYMENT" "payment-service" "{}" \
    | jq -r '"  "+.eventType' 2>/dev/null || true; sleep 1

  # INVENTORY_RESERVED → COMPLETED, then INVENTORY_FAILED → triggers conflict
  inject_event "$saga_id" "INVENTORY_RESERVED" "RESERVE_INVENTORY" "inventory-service" "{}" \
    | jq -r '"  "+.eventType' 2>/dev/null || true; sleep 1
  inject_event "$saga_id" "INVENTORY_FAILED" "RESERVE_INVENTORY" "inventory-service" \
    "{\\\"reason\\\":\\\"timeout\\\"}" \
    | jq -r '"  "+.eventType' 2>/dev/null || true

  sleep 3
  curl -s -X POST "$CORR_SVC/api/admin/analyze/$saga_id" > /dev/null 2>&1 || true
  sleep 2

  collect_result "SC11" "Multi-Terminal Conflict" "$saga_id" "COMPENSATING" "MULTI_TERMINAL_CONFLICT"
}

SC12_post_terminal_event() {
  section "SC12 – Post-Terminal Event (event sau khi saga đã COMPLETED)"
  echo "  Trigger  : Chạy happy path → COMPLETED → inject thêm 1 event vào saga đó"
  echo "  Expected : COMPLETED | FORBIDDEN_POST_TERMINAL anomaly | RCA=TERMINAL_STATE_OVERWRITE (R04)"

  local resp saga_id
  resp=$(create_order "cust-post-term-$(date +%s)" "prod-A" 1 "30.00")
  saga_id=$(echo "$resp" | jq -r '.sagaId')
  echo "  sagaId   : $saga_id"

  wait_settled "$saga_id" 25 "SC12"
  log "Saga is COMPLETED. Now injecting a spurious post-terminal event..."

  inject_event "$saga_id" "PAYMENT_COMPLETED" "PROCESS_PAYMENT" "payment-service" \
    "{\\\"note\\\":\\\"duplicate late arrival\\\"}" \
    | jq -r '"  Injected post-terminal: " + .eventType' 2>/dev/null || true

  sleep 3
  curl -s -X POST "$CORR_SVC/api/admin/analyze/$saga_id" > /dev/null 2>&1 || true
  sleep 2

  collect_result "SC12" "Post-Terminal Event" "$saga_id" "COMPLETED" "TERMINAL_STATE_OVERWRITE"
}

SC13_full_compensation_success() {
  section "SC13 – Full Compensation Flow (Inventory fail → Refund → COMPENSATED)"
  echo "  Trigger  : Unknown product → INVENTORY_FAILED → payment refunded → COMPENSATED"
  echo "  Expected : COMPENSATED | RCA=FORWARD_STEP_FAILURE (R11) | full compensation chain"
  echo "  Note     : Giống SC03 nhưng verify đầy đủ compensation chain trong events"

  local resp saga_id
  resp=$(create_order "cust-comp-chain-$(date +%s)" "PROD-NO-STOCK" 9999 "1.00")
  saga_id=$(echo "$resp" | jq -r '.sagaId')
  echo "  sagaId   : $saga_id"

  wait_settled "$saga_id" 35 "SC13"

  curl -s -X POST "$CORR_SVC/api/admin/analyze/$saga_id" > /dev/null 2>&1 || true
  sleep 2

  collect_result "SC13" "Full Compensation Chain" "$saga_id" "COMPENSATED" "FORWARD_STEP_FAILURE"

  # Verify compensation events present
  local events
  events=$(curl -s "$CORR_SVC/api/sagas/$saga_id" | jq '[.events[]?.eventType]' 2>/dev/null || echo "[]")
  log "Event chain: $events"
  echo "$events" | grep -q "INVENTORY_FAILED" && info "INVENTORY_FAILED ✓" || warn "INVENTORY_FAILED missing"
  echo "$events" | grep -q "REFUND_PAYMENT"   && info "REFUND_PAYMENT ✓"   || warn "REFUND_PAYMENT missing"
  echo "$events" | grep -q "PAYMENT_REFUNDED" && info "PAYMENT_REFUNDED ✓" || warn "PAYMENT_REFUNDED missing"
}

# ── Main Runner ───────────────────────────────────────────────────────────────

print_summary() {
  section "EXPERIMENT SUMMARY"
  echo ""
  printf "%-6s %-38s %-14s %-35s %-30s %s\n" \
    "SC" "Name" "Status" "Anomalies" "RCA" "Result"
  printf '%0.s─' {1..140}; echo ""

  for sc in SC01 SC02 SC03 SC04 SC05 SC06 SC07 SC08 SC09 SC10 SC11 SC12 SC13; do
    [ -z "${SC_STATUS[$sc]:-}" ] && continue
    local color="$GREEN"
    [[ "${SC_PASS[$sc]}" == "FAIL" ]] && color="$RED"
    [[ "${SC_PASS[$sc]}" == "INFO" ]] && color="$YELLOW"

    local short_saga="${SC_SAGA_ID[$sc]}"
    short_saga="${short_saga:0:8}…"
    local anomalies="${SC_ANOMALIES[$sc]:-[]}"
    # Trim brackets for display
    anomalies=$(echo "$anomalies" | tr -d '[]"' | sed 's/,/, /g')
    [ -z "$anomalies" ] && anomalies="—"
    local rca="${SC_RCA[$sc]:-NONE}"
    local dur="${SC_DURATION_MS[$sc]:-—}"
    [ "$dur" != "—" ] && dur="${dur}ms"

    printf "%-6s %-38s %-14s %-35s %-30s " \
      "$sc" "${sc#SC??_}" "${SC_STATUS[$sc]}" \
      "${anomalies:0:34}" "${rca:0:29}"
    echo -e "${color}${BOLD}${SC_PASS[$sc]}${NC}  ${DIM}${dur}${NC}"
  done

  echo ""
  local total=0 passed=0 failed=0
  for sc in "${!SC_PASS[@]}"; do
    ((total++))
    [[ "${SC_PASS[$sc]}" == "PASS" ]] && ((passed++))
    [[ "${SC_PASS[$sc]}" == "FAIL" ]] && ((failed++))
  done
  echo -e "  Total: $total  |  ${GREEN}Pass: $passed${NC}  |  ${RED}Fail: $failed${NC}"
  echo -e "  Results: ${BOLD}$RESULTS_FILE${NC}"
  echo ""
}

# ── Entry point ───────────────────────────────────────────────────────────────

# Tất cả scenarios theo thứ tự
ALL_SCENARIOS=(SC01 SC02 SC03 SC04 SC05 SC06 SC07 SC08 SC09 SC10 SC11 SC12 SC13)

# Nếu pass arguments thì chỉ chạy những SC đó
SELECTED=("${@:-${ALL_SCENARIOS[@]}}")

require_tools
echo ""
echo -e "${BOLD}Saga Observability Platform – Experiment Run${NC}"
echo -e "Time     : $(date)"
echo -e "Results  : $RESULTS_FILE"
echo -e "Scenarios: ${SELECTED[*]}"

check_services
reset_state   # đảm bảo state sạch từ đầu

for sc in "${SELECTED[@]}"; do
  case "$sc" in
    SC01) SC01_happy_path ;;
    SC02) SC02_payment_circuit_breaker ;;
    SC03) SC03_inventory_failure ;;
    SC04) SC04_payment_service_paused ;;
    SC05) SC05_stuck_saga ;;
    SC06) SC06_duplicate_event ;;
    SC07) SC07_out_of_order ;;
    SC08) SC08_missing_completion ;;
    SC09) SC09_timeout_retry_storm ;;
    SC10) SC10_compensation_failed ;;
    SC11) SC11_multi_terminal_conflict ;;
    SC12) SC12_post_terminal_event ;;
    SC13) SC13_full_compensation_success ;;
    *)    warn "Unknown scenario: $sc. Skip." ;;
  esac
  reset_state   # clean state sau mỗi SC
done

print_summary

# Convert JSONL → simple CSV cho thesis
CSV_FILE="$RESULTS_DIR/summary_${RUN_TS}.csv"
echo "SC,Name,SagaId,ExpectedStatus,ActualStatus,ExpectedRCA,ActualRCA,Anomalies,DurationMs,Pass" > "$CSV_FILE"
while IFS= read -r line; do
  echo "$line" | jq -r '[.sc, .name, .sagaId, .expectedStatus, .actualStatus, .expectedRca, .actualRca,
    (.anomalies | join("|")), (.durationMs | tostring), .pass] | @csv' >> "$CSV_FILE" 2>/dev/null || true
done < "$RESULTS_FILE"
echo -e "CSV export: ${BOLD}$CSV_FILE${NC}"
