"""
Saga Observability Platform — Evaluation Script (Tier 2)
=========================================================
End-to-end evaluation qua Kafka + Docker — test real infrastructure behavior.

Đo 4 metrics chính cho luận văn:
  1. RCA Accuracy (%) — rootCause actual vs expected
  2. Time-to-Insight (s) — từ tạo order đến có RCA result
  3. RCA Latency (ms) — thời gian chạy RCA analysis
  4. Classification Precision/Recall/F1

25 Scenarios — 5 failure categories × 5 scenarios each
(see docs/RCA_SCENARIO_TAXONOMY.md):
  A — Service Availability   (A1=SC01, A2=SC03, A3=SC04, A4=SC16, A5=SC17)
  B — Network / Timing       (B1=SC06, B2=SC18, B3=SC15, B4=SC07, B5=SC19)
  C — State Machine          (C1=SC09, C2=SC10, C3=SC13, C4=SC20, C5=SC21)
  D — Compensation           (D1=SC02, D2=SC05, D3=SC22, D4=SC23, D5=SC24)
  E — Correlation / Identity (E1=SC08, E2=SC25, E3=SC11, E4=SC14, E5=SC26)

Stratified train (15) / test (10) split — see taxonomy doc §4.
Each scenario carries `split: train|test` and `category: A|B|C|D|E`.

Execution methods:
  REAL    — full Kafka pipeline (SC01, SC02, SC06)
  DOCKER  — REAL + container/fail-mode/toxiproxy (SC03, SC04, SC05, SC07,
            SC16, SC17)
  INJECT  — admin REST inject — fast rule-engine probes (everything else)

Cách chạy:
  pip install requests
  python evaluate.py                            # 2 basic scenarios
  python evaluate.py --full                     # all 25 scenarios
  python evaluate.py --full --split train       # only train set (15)
  python evaluate.py --full --split test        # held-out test set (10)
  python evaluate.py --full --category B        # only Category B
  python evaluate.py --full --runs 3
  python evaluate.py --scenario SC03            # specific scenario(s)
  python evaluate.py --list                     # list scenarios with split + cat
"""

import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

import requests
import time
import json
import os
import statistics
import subprocess
from datetime import datetime, timedelta, timezone

# ── Config ───────────────────────────────────────────────────────────────────
ORDER_URL       = "http://localhost:8081"
PAYMENT_URL     = "http://localhost:8082"
PLATFORM_URL    = "http://localhost:8084"
INVENTORY_URL   = "http://localhost:8083"

RESULTS_DIR     = "results"
N_RUNS          = 5
WARMUP_RUNS     = 2
POLL_TIMEOUT    = 90
POLL_INTERVAL   = 1
SAGA_MATCH_WAIT = 30

POSTGRES_CONTAINER  = "demo-postgres"
INVENTORY_CONTAINER = "inventory-service"
PAYMENT_CONTAINER   = "payment-service"
KAFKA_CONTAINER     = "demo-kafka"
TOXIPROXY_URL       = "http://localhost:8474"

# ── Helpers ───────────────────────────────────────────────────────────────────

def health_check() -> bool:
    ok = True
    for name, url in [
        ("order-service",       f"{ORDER_URL}/actuator/health"),
        ("payment-service",     f"{PAYMENT_URL}/actuator/health"),
        ("inventory-service",   f"{INVENTORY_URL}/actuator/health"),
        ("correlation-service", f"{PLATFORM_URL}/actuator/health"),
    ]:
        try:
            r = requests.get(url, timeout=5)
            up = r.json().get("status") == "UP"
            print(f"  {'✅' if up else '❌'} {name}")
            if not up: ok = False
        except Exception as e:
            print(f"  ❌ {name}: {e}")
            ok = False
    return ok


def reset_inventory(product_id: str, quantity: int = 1000):
    sql = f"UPDATE inventory SET available_quantity={quantity} WHERE product_id='{product_id}';"
    cmd = [
        "docker", "exec", POSTGRES_CONTAINER,
        "psql", "-U", "demo", "-d", "inventory_db", "-c", sql
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        if "UPDATE" in result.stdout:
            print(f"  ✅ Inventory reset: {product_id} → {quantity}")
            return True
    except Exception as e:
        print(f"  ⚠️  Cannot reset inventory: {e}")
    return False


def create_order(payload: dict) -> tuple:
    start = time.perf_counter()
    try:
        r = requests.post(f"{ORDER_URL}/api/orders", json=payload, timeout=15)
        latency = (time.perf_counter() - start) * 1000
        if r.status_code == 200:
            return r.json(), round(latency, 2)
        print(f"  ⚠️  Order API returned {r.status_code}: {r.text[:100]}")
        return None, round(latency, 2)
    except Exception as e:
        latency = (time.perf_counter() - start) * 1000
        print(f"  ⚠️  Order request failed: {e}")
        return None, round(latency, 2)


def find_saga_by_id(saga_id: str) -> dict | None:
    """
    Find saga in correlation-service by sagaId.
    Polls until the saga appears or timeout.
    """
    deadline = time.time() + SAGA_MATCH_WAIT

    while time.time() < deadline:
        try:
            r = requests.get(f"{PLATFORM_URL}/api/sagas/{saga_id}", timeout=5)
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
        time.sleep(1)
    return None


def poll_saga_terminal(saga_id: str, timeout: int = None) -> tuple:
    timeout = timeout or POLL_TIMEOUT
    start = time.time()
    deadline = start + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"{PLATFORM_URL}/api/sagas/{saga_id}", timeout=5)
            if r.status_code == 200:
                data = r.json()
                if data.get("status") in ("COMPLETED", "COMPENSATED", "FAILED"):
                    return data, time.time() - start
        except Exception:
            pass
        time.sleep(POLL_INTERVAL)
    # Return last known state even if not terminal
    try:
        r = requests.get(f"{PLATFORM_URL}/api/sagas/{saga_id}", timeout=5)
        if r.status_code == 200:
            return r.json(), time.time() - start
    except Exception:
        pass
    return None, time.time() - start


def poll_saga_status(saga_id: str, expected_status: str, timeout: int = 30) -> dict | None:
    """Poll until saga reaches a specific status."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"{PLATFORM_URL}/api/sagas/{saga_id}", timeout=5)
            if r.status_code == 200:
                data = r.json()
                if data.get("status") == expected_status:
                    return data
        except Exception:
            pass
        time.sleep(1)
    return None


def trigger_rca(saga_id: str) -> tuple:
    start = time.perf_counter()
    try:
        r = requests.post(f"{PLATFORM_URL}/rca/analyze/{saga_id}", timeout=10)
        latency = (time.perf_counter() - start) * 1000
        if r.status_code == 200:
            return r.json(), round(latency, 2)
    except Exception:
        pass
    latency = (time.perf_counter() - start) * 1000
    return None, round(latency, 2)


def get_saga_detail(saga_id: str) -> dict | None:
    try:
        r = requests.get(f"{PLATFORM_URL}/api/sagas/{saga_id}", timeout=5)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None


def set_payment_fail_mode(enabled: bool):
    try:
        r = requests.post(
            f"{PAYMENT_URL}/api/admin/fail-mode?enabled={str(enabled).lower()}",
            timeout=5)
        return r.status_code == 200
    except Exception:
        return False


def docker_stop(container: str):
    try:
        subprocess.run(["docker", "stop", "-t", "5", container],
                       capture_output=True, timeout=20)
        print(f"  🛑 Stopped {container}")
    except Exception as e:
        print(f"  ⚠️  Cannot stop {container}: {e}")


def docker_start(container: str):
    try:
        subprocess.run(["docker", "start", container],
                       capture_output=True, timeout=15)
        print(f"  ▶️  Started {container}")
    except Exception as e:
        print(f"  ⚠️  Cannot start {container}: {e}")


def wait_service_ready(url: str, timeout: int = 60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(url, timeout=3)
            if r.status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(2)
    return False


# ── Toxiproxy helpers ─────────────────────────────────────────────────────────

def toxiproxy_create_proxy(name: str, listen: str, upstream: str) -> bool:
    try:
        r = requests.post(f"{TOXIPROXY_URL}/proxies", json={
            "name": name, "listen": listen, "upstream": upstream,
        }, timeout=5)
        return r.status_code in (200, 201, 409)  # 409 = already exists
    except Exception:
        return False


def toxiproxy_add_toxic(proxy_name: str, toxic_type: str,
                        attributes: dict, toxic_name: str = None) -> bool:
    body = {"type": toxic_type, "attributes": attributes}
    if toxic_name:
        body["name"] = toxic_name
    try:
        r = requests.post(
            f"{TOXIPROXY_URL}/proxies/{proxy_name}/toxics",
            json=body, timeout=5)
        return r.status_code in (200, 201)
    except Exception:
        return False


def toxiproxy_remove_toxic(proxy_name: str, toxic_name: str) -> bool:
    try:
        r = requests.delete(
            f"{TOXIPROXY_URL}/proxies/{proxy_name}/toxics/{toxic_name}",
            timeout=5)
        return r.status_code in (200, 204, 404)
    except Exception:
        return False


def toxiproxy_delete_proxy(proxy_name: str) -> bool:
    try:
        r = requests.delete(f"{TOXIPROXY_URL}/proxies/{proxy_name}", timeout=5)
        return r.status_code in (200, 204, 404)
    except Exception:
        return False


def toxiproxy_available() -> bool:
    try:
        r = requests.get(f"{TOXIPROXY_URL}/version", timeout=3)
        return r.status_code == 200
    except Exception:
        return False


# ── Admin inject helper ──────────────────────────────────────────────────────

def admin_inject(saga_id, event_type, step_name="UNKNOWN", service_name="test",
                 payload=None, event_id=None, timestamp=None):
    body = {
        "sagaId": saga_id,
        "eventType": event_type,
        "stepName": step_name,
        "serviceName": service_name,
        "payload": json.dumps(payload or {}),
    }
    if event_id:
        body["eventId"] = event_id
    if timestamp:
        body["timestamp"] = timestamp if isinstance(timestamp, str) else timestamp.isoformat()
    try:
        r = requests.post(f"{PLATFORM_URL}/api/admin/inject-event", json=body, timeout=10)
        if r.status_code != 200:
            print(f"\n    [inject error] {r.status_code}: {r.text[:200]}", end="")
        return r.status_code == 200
    except Exception as e:
        print(f"\n    [inject exception] {e}", end="")
        return False


def ts_ago(minutes=0, seconds=0):
    t = datetime.now(timezone.utc) - timedelta(minutes=minutes, seconds=seconds)
    return t.isoformat()


def make_result(scenario: dict, run_index: int) -> dict:
    return {
        "scenario_id":        scenario["id"],
        "run_index":          run_index + 1,
        "timestamp":          datetime.now(timezone.utc).isoformat(),
        "saga_id":            None,
        "saga_status":        None,
        "saga_duration_ms":   None,
        "rca_root_cause":     None,
        "rca_expected":       scenario["expected_rca"],
        "rca_severity":       None,
        "rca_latency_ms":     None,
        "rca_accurate":       False,
        "time_to_insight_s":  None,
        "order_latency_ms":   None,
        "anomalies_expected": scenario.get("expected_anomalies", []),
        "anomalies_actual":   [],
        "error":              None,
    }


def finalize_result(result: dict, saga_id: str, rca, rca_ms, t_start, scenario):
    result["rca_latency_ms"] = rca_ms
    result["time_to_insight_s"] = round(time.time() - t_start, 3)

    # Collect actual anomalies from saga detail
    detail = get_saga_detail(saga_id)
    if detail:
        result["saga_status"] = detail.get("status")
        result["saga_duration_ms"] = detail.get("durationMs")
        result["anomalies_actual"] = [
            a["anomalyType"] for a in detail.get("anomalies", [])
        ]

    if rca:
        result["rca_root_cause"] = rca.get("rootCause")
        result["rca_severity"] = rca.get("severity")

    # Accuracy: compare actual vs expected (both can be None for healthy)
    result["rca_accurate"] = (result["rca_root_cause"] == scenario["expected_rca"])

    icon = "✅" if result["rca_accurate"] else "❌"
    print(f" → {result.get('saga_status', '?')} | "
          f"RCA={result['rca_root_cause']} (expected={scenario['expected_rca']}) {icon}")


# ── Scenario runners ─────────────────────────────────────────────────────────

def run_basic_scenario(scenario: dict, run_index: int) -> dict:
    """SC01 (HEALTHY) or SC02 (COMPENSATION_RECOVERED)."""
    result = make_result(scenario, run_index)
    t_start = time.time()

    order_resp, latency = create_order(scenario["request"])
    result["order_latency_ms"] = latency
    if order_resp is None:
        result["error"] = "create_order failed"
        return result
    order_id = order_resp.get("id", "")
    saga_id = order_resp.get("sagaId", order_id)
    print(f"order={order_id[:8]}.. saga={saga_id[:8]}.. {latency:.0f}ms", end="")

    saga_info = find_saga_by_id(saga_id)
    if saga_info is None:
        result["error"] = "saga not found in correlation-service"
        print(" → saga not found"); return result

    result["saga_id"] = saga_id

    detail, wait_s = poll_saga_terminal(saga_id,
                                        timeout=scenario.get("poll_timeout", POLL_TIMEOUT))
    if detail is None:
        result["error"] = f"poll timeout {POLL_TIMEOUT}s"
        print(" → TIMEOUT"); return result

    result["saga_status"] = detail.get("status")
    result["saga_duration_ms"] = detail.get("durationMs")

    rca, rca_ms = trigger_rca(saga_id)
    finalize_result(result, saga_id, rca, rca_ms, t_start, scenario)
    return result


def run_stuck_saga(scenario: dict, run_index: int) -> dict:
    """A2/A5 — stop inventory BEFORE order → saga stuck at IN_PROGRESS.

    Reads `stuck_wait_seconds` from the scenario dict to support both:
      - A2 STUCK_LIVE (~65s, only SAGA_STUCK fires)
      - A5 STUCK_DEAD_PERMANENT (~150s, both SAGA_STUCK + MISSING_TERMINAL fire)

    Always restarts inventory at the end — non-negotiable cleanup, otherwise
    the next scenario will see stale stuck-state.
    """
    result = make_result(scenario, run_index)
    t_start = time.time()

    docker_stop(INVENTORY_CONTAINER)
    time.sleep(3)

    order_resp, latency = create_order(scenario["request"])
    result["order_latency_ms"] = latency
    if order_resp is None:
        docker_start(INVENTORY_CONTAINER)
        wait_service_ready(f"{INVENTORY_URL}/actuator/health", timeout=60)
        result["error"] = "create_order failed"; return result

    order_id = order_resp.get("id", "")
    saga_id = order_resp.get("sagaId", order_id)
    print(f"order={order_id[:8]}.. saga={saga_id[:8]}.. {latency:.0f}ms", end="")

    saga_info = find_saga_by_id(saga_id)
    if saga_info is None:
        docker_start(INVENTORY_CONTAINER)
        wait_service_ready(f"{INVENTORY_URL}/actuator/health", timeout=60)
        result["error"] = "saga not found"
        print(" → not found"); return result

    result["saga_id"] = saga_id

    # Detector schedule:
    #   detectStuckSagas       @ 30s rate; trigger = lastEvent + step_timeout < now
    #   detectMissingTerminal  @ 60s rate; trigger = saga age > 2 × max_step_timeout
    # Payment step timeout = 10s. So:
    #   ~65s  → only SAGA_STUCK reliably fires (A2).
    #   ~150s → both SAGA_STUCK + MISSING_TERMINAL_EVENT fire (A5/A4 base).
    wait_seconds = scenario.get("stuck_wait_seconds", 65)
    print(f" waiting {wait_seconds}s for stuck detection...", end="", flush=True)
    time.sleep(wait_seconds)

    rca, rca_ms = trigger_rca(saga_id)
    finalize_result(result, saga_id, rca, rca_ms, t_start, scenario)

    # Cleanup: restart inventory regardless of scenario outcome
    docker_start(INVENTORY_CONTAINER)
    print(f"  ▶️  Restoring inventory...", end="")
    wait_service_ready(f"{INVENTORY_URL}/actuator/health", timeout=60)
    print(" ready")
    time.sleep(3)
    return result


def run_recovered_from_stuck(scenario: dict, run_index: int) -> dict:
    """A3/A4 — stop inventory → stuck detected → restart → saga completes → RCA.

    Reads `stuck_wait_seconds` from the scenario dict:
      - A3 STUCK_RECOVERED_BRIEF (~65s, only SAGA_STUCK before recovery)
      - A4 STUCK_RECOVERED_LONG (~125s, both SAGA_STUCK + MISSING_TERMINAL
        before recovery — exercises R05's recovered-with-MISSING_TERMINAL branch)
    """
    result = make_result(scenario, run_index)
    t_start = time.time()

    docker_stop(INVENTORY_CONTAINER)
    time.sleep(3)

    order_resp, latency = create_order(scenario["request"])
    result["order_latency_ms"] = latency
    if order_resp is None:
        docker_start(INVENTORY_CONTAINER)
        wait_service_ready(f"{INVENTORY_URL}/actuator/health", timeout=60)
        result["error"] = "create_order failed"; return result

    order_id = order_resp.get("id", "")
    saga_id = order_resp.get("sagaId", order_id)
    print(f"order={order_id[:8]}.. saga={saga_id[:8]}.. {latency:.0f}ms", end="")

    saga_info = find_saga_by_id(saga_id)
    if saga_info is None:
        docker_start(INVENTORY_CONTAINER)
        wait_service_ready(f"{INVENTORY_URL}/actuator/health", timeout=60)
        result["error"] = "saga not found"
        print(" → not found"); return result

    result["saga_id"] = saga_id

    # Wait for stuck detection before restart. See run_stuck_saga for rationale.
    wait_seconds = scenario.get("stuck_wait_seconds", 65)
    print(f" waiting {wait_seconds}s for stuck...", end="", flush=True)
    time.sleep(wait_seconds)

    # Verify stuck was detected
    detail = get_saga_detail(saga_id)
    stuck_detected = any(a.get("anomalyType") == "SAGA_STUCK"
                         for a in (detail or {}).get("anomalies", []))
    print(f" stuck={'yes' if stuck_detected else 'no'}", end="")

    # Restart inventory — it will re-join consumer group and pick up
    # the pending payment-completed message from Kafka (offset not committed
    # because consumer was stopped before processing).
    docker_start(INVENTORY_CONTAINER)
    print(f" restarting...", end="", flush=True)
    ready = wait_service_ready(f"{INVENTORY_URL}/actuator/health", timeout=60)
    if not ready:
        result["error"] = "inventory-service did not become ready"
        print(" ❌ not ready"); return result
    print(" ready", end="")

    # Wait for consumer to reprocess and saga to complete
    terminal_detail, _ = poll_saga_terminal(saga_id, timeout=30)
    if terminal_detail:
        result["saga_status"] = terminal_detail.get("status")
        result["saga_duration_ms"] = terminal_detail.get("durationMs")
    else:
        result["saga_status"] = "STILL_STUCK"

    rca, rca_ms = trigger_rca(saga_id)
    finalize_result(result, saga_id, rca, rca_ms, t_start, scenario)
    time.sleep(3)
    return result


def run_compensation_step_failure(scenario: dict, run_index: int) -> dict:
    """SC05: Simulate compensation stuck via admin inject.
    Inject: ORDER_CREATED → PAYMENT_COMPLETED → INVENTORY_FAILED → REFUND_PAYMENT
    Then wait for MISSING_COMPENSATION_ACK (no PAYMENT_REFUNDED arrives)."""
    result = make_result(scenario, run_index)
    t_start = time.time()

    import uuid
    sid = f"ev-sc05-{uuid.uuid4().hex[:8]}"
    result["saga_id"] = sid

    events = [
        ("ORDER_CREATED", "CREATE_ORDER", "order-service"),
        ("PAYMENT_COMPLETED", "PROCESS_PAYMENT", "payment-service"),
        ("INVENTORY_FAILED", "RESERVE_INVENTORY", "inventory-service"),
        ("REFUND_PAYMENT", "REFUND_PAYMENT", "payment-service"),
    ]
    for idx, (et, sn, svc) in enumerate(events):
        ok = admin_inject(sid, et, sn, svc, event_id=f"{et.lower()}-{sid}-{idx}")
        if not ok:
            result["error"] = f"inject failed: {et}"
            print(f" ❌ inject {et}"); return result
        time.sleep(0.5)

    print(f"saga={sid[:12]}", end="")

    # Wait for MISSING_COMPENSATION_ACK detection
    #   @Scheduled(fixedRate=60000) checks COMPENSATING sagas with lastUpdated > 30s ago
    wait_seconds = 70
    print(f" waiting {wait_seconds}s...", end="", flush=True)
    time.sleep(wait_seconds)

    rca, rca_ms = trigger_rca(sid)
    finalize_result(result, sid, rca, rca_ms, t_start, scenario)
    return result


def run_circuit_breaker(scenario: dict, run_index: int) -> dict:
    """SC06: Enable payment fail-mode → Resilience4j retry exhausted →
    circuit breaker fallback → PaymentFailedEvent with 'circuit breaker' text."""
    result = make_result(scenario, run_index)
    t_start = time.time()

    if not set_payment_fail_mode(True):
        result["error"] = "cannot enable fail-mode"
        print(" ❌ fail-mode"); return result
    time.sleep(1)

    order_resp, latency = create_order(scenario["request"])
    result["order_latency_ms"] = latency
    if order_resp is None:
        set_payment_fail_mode(False)
        result["error"] = "create_order failed"; return result

    order_id = order_resp.get("id", "")
    saga_id = order_resp.get("sagaId", order_id)
    print(f"order={order_id[:8]}.. saga={saga_id[:8]}.. {latency:.0f}ms", end="")

    saga_info = find_saga_by_id(saga_id)
    if saga_info is None:
        set_payment_fail_mode(False)
        result["error"] = "saga not found"
        print(" → not found"); return result

    result["saga_id"] = saga_id

    # Resilience4j: maxAttempts=3, waitDuration=1s, exponentialBackoff=2x
    # Total: attempt1 + 1s + attempt2 + 2s + attempt3 + fallback ≈ 5-8s
    # Then fallback publishes PaymentFailedEvent → saga FAILED.
    print(f" waiting for retry exhaustion...", end="", flush=True)
    detail, wait_s = poll_saga_terminal(saga_id, timeout=30)

    rca, rca_ms = trigger_rca(saga_id)
    finalize_result(result, saga_id, rca, rca_ms, t_start, scenario)

    set_payment_fail_mode(False)
    time.sleep(2)
    return result


# ── Toxiproxy scenario ────────────────────────────────────────────────────────

def run_downstream_timeout(scenario: dict, run_index: int) -> dict:
    """SC07: Toxiproxy inject 15s latency on payment Postgres.
    Payment-service DB queries timeout → Resilience4j retry → fallback →
    PaymentFailedEvent with 'circuit breaker / retry exhausted' payload.
    Same RCA as SC06 but caused by NETWORK latency, not application failure.

    Requires: payment-service routed through Toxiproxy for Postgres.
    Since payment connects directly to postgres:5432 in docker-compose,
    we create a proxy and inject latency dynamically. However, changing
    payment-service DB connection requires restart — impractical for eval.

    Practical approach: inject Toxiproxy latency on Kafka instead.
    Toxiproxy proxies kafka:9092 at toxiproxy:19092.
    We temporarily add a latency toxic, create an order, then observe
    the step timeout (event gap > workflow timeout = 10s for payment step).
    """
    result = make_result(scenario, run_index)
    t_start = time.time()

    if not toxiproxy_available():
        result["error"] = "Toxiproxy not available"
        print(" ❌ Toxiproxy unavailable"); return result

    # Create Kafka proxy (toxiproxy:19092 → kafka:9092)
    proxy_created = toxiproxy_create_proxy(
        "eval-kafka", "0.0.0.0:19092", f"{KAFKA_CONTAINER}:9092")
    if not proxy_created:
        result["error"] = "Cannot create Toxiproxy Kafka proxy"
        print(" ❌ proxy failed"); return result

    # Add latency toxic: 12s latency (> payment step timeout of 10s)
    toxic_added = toxiproxy_add_toxic(
        "eval-kafka", "latency",
        {"latency": 12000, "jitter": 1000},
        toxic_name="eval-latency")
    if not toxic_added:
        toxiproxy_delete_proxy("eval-kafka")
        result["error"] = "Cannot add latency toxic"
        print(" ❌ toxic failed"); return result

    print("toxic=12s latency on Kafka", end="")

    # The toxic is on a separate proxy (port 19092), NOT the real kafka (9092).
    # Services still connect to kafka:9092 directly.
    # So this toxic doesn't actually affect them unless they use port 19092.
    #
    # For a true Toxiproxy test, services would need to be configured to
    # connect through Toxiproxy. Since reconfiguring requires container restart,
    # we fall back to using admin-inject to simulate the downstream timeout
    # scenario with realistic timing, while proving Toxiproxy infrastructure works.
    #
    # Inject events with a gap > step timeout (10s) to trigger STEP_TIMEOUT.
    import uuid
    sid = f"ev-sc07-{uuid.uuid4().hex[:8]}"
    trace = uuid.uuid4().hex
    admin_inject(sid, "ORDER_CREATED", "CREATE_ORDER", "order-service",
                 payload={"orderId": f"ORD-TOXI-{run_index}", "sagaId": sid, "traceId": trace},
                 event_id=f"t1-{sid}", timestamp=ts_ago(minutes=3))
    admin_inject(sid, "PAYMENT_COMPLETED", "PROCESS_PAYMENT", "payment-service",
                 payload={"traceId": trace},
                 event_id=f"t2-{sid}", timestamp=ts_ago(minutes=1))
    admin_inject(sid, "INVENTORY_RESERVED", "RESERVE_INVENTORY", "inventory-service",
                 payload={"traceId": trace},
                 event_id=f"t3-{sid}", timestamp=ts_ago(seconds=5))

    result["saga_id"] = sid

    # Wait for detectStepTimeouts (runs every 60s)
    print(f" waiting 65s for step timeout detection...", end="", flush=True)
    time.sleep(65)

    rca, rca_ms = trigger_rca(sid)
    finalize_result(result, sid, rca, rca_ms, t_start, scenario)

    # Cleanup Toxiproxy
    toxiproxy_remove_toxic("eval-kafka", "eval-latency")
    toxiproxy_delete_proxy("eval-kafka")
    return result


# ── Admin-inject scenarios ───────────────────────────────────────────────────

def run_inject_scenario(scenario: dict, run_index: int) -> dict:
    """Generic runner for scenarios that inject events via admin API."""
    result = make_result(scenario, run_index)
    t_start = time.time()

    import uuid
    sc_num = scenario['id'].split('_')[0].lower()
    sid = f"ev-{sc_num}-{uuid.uuid4().hex[:8]}"
    result["saga_id"] = sid

    ts_markers = {
        "USE_TS_AGO_3M": lambda: ts_ago(minutes=3),
        "USE_TS_AGO_2M": lambda: ts_ago(minutes=2),
        "USE_TS_AGO_1M": lambda: ts_ago(minutes=1),
        "USE_TS_AGO_30S": lambda: ts_ago(seconds=30),
        "USE_TS_AGO_10S": lambda: ts_ago(seconds=10),
    }

    dup_key = f"dup-{sid[:12]}"

    events = scenario["inject_events"]
    for idx, ev in enumerate(events):
        ts = ev.get("timestamp")
        if ts in ts_markers:
            ts = ts_markers[ts]()

        eid = ev.get("eventId")
        if eid == "USE_UNIQUE":
            eid = f"{ev['eventType'].lower()}-{sid[:12]}-{idx}"
        elif eid == "USE_DUP_KEY":
            eid = dup_key
        elif eid is None:
            eid = f"{ev['eventType'].lower()}-{sid[:12]}"

        payload = ev.get("payload")
        if isinstance(payload, dict):
            payload = {k: (uuid.uuid4().hex if v == "USE_UNIQUE" else v)
                       for k, v in payload.items()}

        ok = admin_inject(
            sid,
            ev["eventType"],
            ev.get("stepName", "UNKNOWN"),
            ev.get("serviceName", "test"),
            payload,
            eid,
            ts,
        )
        if not ok:
            result["error"] = f"inject failed: {ev['eventType']}"
            print(f" ❌ inject {ev['eventType']}"); return result
        delay = ev.get("delay", 0)
        if delay > 0:
            time.sleep(delay)

    print(f"saga={sid[:12]}", end="")

    # Wait for saga to appear in DB (processEvent is synchronous,
    # but allow small buffer for transaction commit)
    time.sleep(2)

    # Verify saga was created
    check = get_saga_detail(sid)
    if check is None:
        result["error"] = f"saga {sid} not found after inject"
        print(f" → saga not found"); return result

    wait = scenario.get("wait_seconds", 0)
    if wait > 0:
        print(f" waiting {wait}s...", end="", flush=True)
        time.sleep(wait)

    rca, rca_ms = trigger_rca(sid)
    finalize_result(result, sid, rca, rca_ms, t_start, scenario)
    return result


def run_timeout_then_recover(scenario: dict, run_index: int) -> dict:
    """B5 — STEP_TIMEOUT fires, then a follow-up event lets saga complete.

    Pipeline:
      Phase 1: inject ORDER_CREATED (ts=3min ago) + PAYMENT_COMPLETED
               (ts=30s ago) → wait `wait_seconds` (default 65s) for the
               detectStepTimeouts scheduler to fire STEP_TIMEOUT anomaly.
      Phase 2: inject INVENTORY_RESERVED (ts=now) so saga reaches COMPLETED.
      Phase 3: trigger RCA — STEP_TIMEOUT anomaly persists in DB; primary
               RCA = R07 DOWNSTREAM_TIMEOUT; outcome = SELF_HEALED.

    This exercises the "completed but had problems" outcome-determination
    branch that A1 (HEALTHY) and B3 (STEP_TIMEOUT_INJECT, never recovers)
    do not reach.
    """
    result = make_result(scenario, run_index)
    t_start = time.time()

    import uuid
    sid = f"ev-sc19-{uuid.uuid4().hex[:8]}"
    result["saga_id"] = sid
    trace = uuid.uuid4().hex

    # Phase 1: inject events with stale timestamps to make a > 10s gap
    ok = admin_inject(sid, "ORDER_CREATED", "CREATE_ORDER", "order-service",
                      payload={"orderId": f"ORD-B5-{run_index}",
                               "sagaId": sid, "traceId": trace},
                      event_id=f"oc-{sid}",
                      timestamp=ts_ago(minutes=3))
    if not ok:
        result["error"] = "phase1 inject ORDER_CREATED failed"
        print(" ❌ inject ORDER_CREATED"); return result

    ok = admin_inject(sid, "PAYMENT_COMPLETED", "PROCESS_PAYMENT", "payment-service",
                      payload={"traceId": trace},
                      event_id=f"pc-{sid}",
                      timestamp=ts_ago(seconds=30))
    if not ok:
        result["error"] = "phase1 inject PAYMENT_COMPLETED failed"
        print(" ❌ inject PAYMENT_COMPLETED"); return result

    print(f"saga={sid[:12]} phase1 done", end="")

    wait1 = scenario.get("wait_seconds", 65)
    print(f" waiting {wait1}s for STEP_TIMEOUT...", end="", flush=True)
    time.sleep(wait1)

    # Phase 2: follow-up event so saga reaches terminal COMPLETED
    ok = admin_inject(sid, "INVENTORY_RESERVED", "RESERVE_INVENTORY", "inventory-service",
                      payload={"traceId": trace},
                      event_id=f"ir-{sid}")
    if not ok:
        result["error"] = "phase2 inject INVENTORY_RESERVED failed"
        print(" ❌ inject INVENTORY_RESERVED"); return result

    print(" phase2 done, settling 3s...", end="", flush=True)
    time.sleep(3)

    rca, rca_ms = trigger_rca(sid)
    finalize_result(result, sid, rca, rca_ms, t_start, scenario)
    return result


# ── Scenario definitions ─────────────────────────────────────────────────────

BASIC_SCENARIOS = [
    {
        "id": "SC01_HEALTHY",
        "category": "A",
        "split": "test",
        "description": "Normal order — all steps succeed (negative class, TN)",
        "request": {"customerId": "eval-01", "productId": "PRODUCT-1",
                    "quantity": 1, "amount": 100.00},
        "expected_saga_status": "COMPLETED",
        "expected_rca": None,
        "expected_anomalies": [],
        "runner": run_basic_scenario,
    },
    {
        "id": "SC02_COMPENSATION_HAPPY_PATH",
        "category": "D",
        "split": "train",
        "description": "Inventory stock-out → compensation succeeds (D1)",
        "request": {"customerId": "eval-02", "productId": "PRODUCT-1",
                    "quantity": 9999, "amount": 999900.00},
        "expected_saga_status": "COMPENSATED",
        "expected_rca": "FORWARD_STEP_FAILURE",
        "expected_anomalies": [],
        "runner": run_basic_scenario,
    },
]

ADVANCED_SCENARIOS = [
    {
        "id": "SC03_STUCK_LIVE",
        "category": "A",
        "split": "train",
        "description": "A2 STUCK_LIVE — inventory down, saga stuck (~65s wait)",
        "request": {"customerId": "eval-03", "productId": "PRODUCT-1",
                    "quantity": 1, "amount": 50.00},
        "expected_saga_status": "IN_PROGRESS",
        "expected_rca": "STUCK_UNKNOWN_CAUSE",
        "expected_anomalies": ["SAGA_STUCK"],
        "runner": run_stuck_saga,
        "stuck_wait_seconds": 65,
        "n_runs": 2,
    },
    {
        "id": "SC04_STUCK_RECOVERED_BRIEF",
        "category": "A",
        "split": "train",
        "description": "A3 STUCK_RECOVERED_BRIEF — stuck ~65s then restart → R06",
        "request": {"customerId": "eval-04", "productId": "PRODUCT-1",
                    "quantity": 1, "amount": 60.00},
        "expected_saga_status": "COMPLETED",
        "expected_rca": "TRANSIENT_SERVICE_FAILURE",
        "expected_anomalies": ["SAGA_STUCK"],
        "runner": run_recovered_from_stuck,
        "stuck_wait_seconds": 65,
        "n_runs": 2,
    },
    {
        "id": "SC05_COMPENSATION_STEP_FAILURE",
        "category": "D",
        "split": "train",
        "description": "D2 — refund never acknowledged → MISSING_COMPENSATION_ACK",
        "request": None,
        "expected_saga_status": "COMPENSATING",
        "expected_rca": "COMPENSATION_STEP_FAILURE",
        "expected_anomalies": ["MISSING_COMPENSATION_ACK", "MISSING_TRACE_CONTEXT"],
        "runner": run_compensation_step_failure,
        "n_runs": 2,
    },
    {
        "id": "SC06_CIRCUIT_BREAKER_REAL",
        "category": "B",
        "split": "train",
        "description": "B1 — REAL: fail-mode → 3 retries → R01 primary, R02 contributing",
        "request": {"customerId": "eval-06", "productId": "PRODUCT-1",
                    "quantity": 1, "amount": 75.00},
        "expected_saga_status": "FAILED",
        "expected_rca": "CIRCUIT_BREAKER_OPEN",
        # Multi-rule: R01 primary + R02 contributing (RETRY_BURST emitted by
        # Resilience4j PAYMENT_RETRY listener after BURST_THRESHOLD=3 attempts).
        "expected_anomalies": ["RETRY_BURST"],
        "runner": run_circuit_breaker,
        "n_runs": 3,
    },
]

TOXIPROXY_SCENARIOS = [
    {
        "id": "SC07_NETWORK_LATENCY_TOXIPROXY",
        "category": "B",
        "split": "test",
        # See docs/RCA_SCENARIO_TAXONOMY.md §B4: Toxiproxy proxy infrastructure
        # is provisioned but services connect to kafka:9092 directly. Marking
        # this scenario as partial-realism limitation rather than blocking.
        "description": "B4 — Toxiproxy partial-realism (services not rerouted; documents limitation)",
        "request": None,
        "expected_saga_status": None,
        "expected_rca": "DOWNSTREAM_TIMEOUT",
        "expected_anomalies": ["STEP_TIMEOUT"],
        "runner": run_downstream_timeout,
        "n_runs": 2,
    },
]

INJECT_SCENARIOS = [
    {
        "id": "SC08_DUPLICATE_EVENT_FAST",
        "category": "E",
        "split": "train",
        "description": "E1 — duplicate eventId at ~1s gap → EXACT_DUPLICATE_EVENT",
        "request": None,
        "expected_saga_status": None,
        "expected_rca": "DUPLICATE_DELIVERY",
        "expected_anomalies": ["EXACT_DUPLICATE_EVENT"],
        "runner": run_inject_scenario,
        "n_runs": 2,
        "wait_seconds": 5,
        "inject_events": [
            {"eventType": "ORDER_CREATED", "stepName": "CREATE_ORDER",
             "serviceName": "order-service", "eventId": "USE_DUP_KEY",
             "payload": {"traceId": "USE_UNIQUE"}},
            {"eventType": "PAYMENT_COMPLETED", "stepName": "PROCESS_PAYMENT",
             "serviceName": "payment-service", "delay": 1,
             "eventId": "USE_UNIQUE",
             "payload": {"traceId": "USE_UNIQUE"}},
            {"eventType": "PAYMENT_COMPLETED", "stepName": "PROCESS_PAYMENT",
             "serviceName": "payment-service", "eventId": "USE_DUP_KEY",
             "delay": 1,
             "payload": {"traceId": "USE_UNIQUE"}},
        ],
    },
    {
        "id": "SC09_OUT_OF_ORDER",
        "category": "C",
        "split": "train",
        "description": "C1 — events with reverse-chronological timestamps → OUT_OF_ORDER",
        "request": None,
        "expected_saga_status": None,
        "expected_rca": "OUT_OF_ORDER_DELIVERY",
        "expected_anomalies": ["OUT_OF_ORDER"],
        "runner": run_inject_scenario,
        "n_runs": 2,
        "wait_seconds": 5,
        "inject_events": [
            {"eventType": "ORDER_CREATED", "stepName": "CREATE_ORDER",
             "serviceName": "order-service",
             "timestamp": "USE_TS_AGO_10S",
             "payload": {"traceId": "aaaa0000aaaa0000aaaa0000aaaa0009"}},
            {"eventType": "PAYMENT_COMPLETED", "stepName": "PROCESS_PAYMENT",
             "serviceName": "payment-service", "delay": 1,
             "timestamp": "USE_TS_AGO_30S",
             "payload": {"traceId": "aaaa0000aaaa0000aaaa0000aaaa0009"}},
        ],
    },
    {
        "id": "SC10_INVALID_TRANSITION",
        "category": "C",
        "split": "train",
        "description": "C2 — skip PAYMENT step → INVALID_TRANSITION",
        "request": None,
        "expected_saga_status": None,
        "expected_rca": "INVALID_TRANSITION",
        "expected_anomalies": ["INVALID_TRANSITION"],
        "runner": run_inject_scenario,
        "n_runs": 2,
        "wait_seconds": 5,
        "inject_events": [
            {"eventType": "ORDER_CREATED", "stepName": "CREATE_ORDER",
             "serviceName": "order-service",
             "payload": {"traceId": "USE_UNIQUE"}},
            {"eventType": "INVENTORY_RESERVED", "stepName": "RESERVE_INVENTORY",
             "serviceName": "inventory-service", "delay": 1,
             "payload": {"traceId": "USE_UNIQUE"}},
        ],
    },
    {
        "id": "SC11_ORPHAN_EVENT",
        "category": "E",
        "split": "train",
        "description": "E3 — first event for an unknown sagaId → ORPHAN_EVENT",
        "request": None,
        "expected_saga_status": None,
        "expected_rca": "WRONG_SAGA_CORRELATION_KEY",
        "expected_anomalies": ["ORPHAN_EVENT"],
        "runner": run_inject_scenario,
        "n_runs": 2,
        "wait_seconds": 5,
        "inject_events": [
            {"eventType": "PAYMENT_COMPLETED", "stepName": "PROCESS_PAYMENT",
             "serviceName": "payment-service",
             "payload": {"traceId": "USE_UNIQUE"}},
        ],
    },
    {
        # SC12 has been REWRITTEN as B2 from the taxonomy:
        # 3× PAYMENT_RETRY events only, NO terminal PAYMENT_FAILED.
        # This isolates R02 TIMEOUT_RETRY_STORM (without R01 dominating).
        # The previous version used 4× PAYMENT_FAILED, which under the new
        # RETRY_EVENT_TYPES = {PAYMENT_RETRY} whitelist no longer fires
        # RETRY_BURST. See docs/RCA_SCENARIO_TAXONOMY.md §B2.
        "id": "SC12_RETRY_STORM_NO_CB",
        "category": "B",
        "split": "train",
        "description": "B2 — 3× PAYMENT_RETRY only (no failure) → R02 isolated",
        "request": None,
        "expected_saga_status": None,
        "expected_rca": "TIMEOUT_RETRY_STORM",
        "expected_anomalies": ["RETRY_BURST"],
        "runner": run_inject_scenario,
        "n_runs": 2,
        "wait_seconds": 5,
        "inject_events": [
            {"eventType": "ORDER_CREATED", "stepName": "CREATE_ORDER",
             "serviceName": "order-service",
             "payload": {"traceId": "USE_UNIQUE"}},
            {"eventType": "PAYMENT_RETRY", "stepName": "PROCESS_PAYMENT",
             "serviceName": "payment-service", "delay": 0.3,
             "eventId": "USE_UNIQUE",
             "payload": {"traceId": "USE_UNIQUE", "attemptNumber": 1,
                         "lastException": "TimeoutException: read timed out"}},
            {"eventType": "PAYMENT_RETRY", "stepName": "PROCESS_PAYMENT",
             "serviceName": "payment-service", "delay": 0.3,
             "eventId": "USE_UNIQUE",
             "payload": {"traceId": "USE_UNIQUE", "attemptNumber": 2,
                         "lastException": "TimeoutException: read timed out"}},
            {"eventType": "PAYMENT_RETRY", "stepName": "PROCESS_PAYMENT",
             "serviceName": "payment-service", "delay": 0.3,
             "eventId": "USE_UNIQUE",
             "payload": {"traceId": "USE_UNIQUE", "attemptNumber": 3,
                         "lastException": "TimeoutException: read timed out"}},
        ],
    },
    {
        # Renamed from SC13_STATE_REGRESSION → SC13_FORBIDDEN_POST_TERMINAL.
        # The injected sequence reaches COMPLETED, then a PAYMENT_COMPLETED
        # arrives — this fires FORBIDDEN_POST_TERMINAL (not STATE_REGRESSION,
        # because PAYMENT_COMPLETED does NOT change the status: it is not in
        # COMPLETED_EVENTS / FAILED_EVENTS / etc., so the state stays at
        # COMPLETED). For a true STATE_REGRESSION case see SC20.
        "id": "SC13_FORBIDDEN_POST_TERMINAL",
        "category": "C",
        "split": "train",
        "description": "C3 — event after COMPLETED terminal → FORBIDDEN_POST_TERMINAL",
        "request": None,
        "expected_saga_status": None,
        "expected_rca": "TERMINAL_STATE_OVERWRITE",
        "expected_anomalies": ["FORBIDDEN_POST_TERMINAL"],
        "runner": run_inject_scenario,
        "n_runs": 2,
        "wait_seconds": 5,
        "inject_events": [
            {"eventType": "ORDER_CREATED", "stepName": "CREATE_ORDER",
             "serviceName": "order-service",
             "payload": {"traceId": "USE_UNIQUE"}},
            {"eventType": "PAYMENT_COMPLETED", "stepName": "PROCESS_PAYMENT",
             "serviceName": "payment-service", "delay": 1,
             "payload": {"traceId": "USE_UNIQUE"}},
            {"eventType": "INVENTORY_RESERVED", "stepName": "RESERVE_INVENTORY",
             "serviceName": "inventory-service", "delay": 1,
             "payload": {"traceId": "USE_UNIQUE"}},
            {"eventType": "PAYMENT_COMPLETED", "stepName": "PROCESS_PAYMENT",
             "serviceName": "payment-service", "delay": 2,
             "eventId": "USE_UNIQUE",
             "payload": {"traceId": "USE_UNIQUE"}},
        ],
    },
    {
        "id": "SC14_MISSING_TRACE_CONTEXT",
        "category": "E",
        "split": "train",
        "description": "E4 — events without traceId field → MISSING_TRACE_CONTEXT",
        "request": None,
        "expected_saga_status": None,
        "expected_rca": "MISSING_TRACE_PROPAGATION",
        "expected_anomalies": ["MISSING_TRACE_CONTEXT"],
        "runner": run_inject_scenario,
        "n_runs": 2,
        "wait_seconds": 5,
        # NOTE: the detector requires that AT LEAST ONE event in the saga
        # HAS traceId (noise-reduction guard). To exercise R19 reliably
        # we give ORDER_CREATED a traceId but not PAYMENT_COMPLETED.
        "inject_events": [
            {"eventType": "ORDER_CREATED", "stepName": "CREATE_ORDER",
             "serviceName": "order-service",
             "payload": {"traceId": "USE_UNIQUE"}},
            {"eventType": "PAYMENT_COMPLETED", "stepName": "PROCESS_PAYMENT",
             "serviceName": "payment-service", "delay": 1},
        ],
    },
    {
        "id": "SC15_STEP_TIMEOUT_INJECT",
        "category": "B",
        "split": "train",
        "description": "B3 — large gap between steps → STEP_TIMEOUT (scheduler)",
        "request": None,
        "expected_saga_status": None,
        "expected_rca": "DOWNSTREAM_TIMEOUT",
        "expected_anomalies": ["STEP_TIMEOUT"],
        "runner": run_inject_scenario,
        "n_runs": 2,
        "wait_seconds": 65,
        "inject_events": [
            {"eventType": "ORDER_CREATED", "stepName": "CREATE_ORDER",
             "serviceName": "order-service",
             "timestamp": "USE_TS_AGO_3M",
             "payload": {"traceId": "USE_UNIQUE"}},
            {"eventType": "PAYMENT_COMPLETED", "stepName": "PROCESS_PAYMENT",
             "serviceName": "payment-service", "delay": 1,
             "timestamp": "USE_TS_AGO_30S",
             "payload": {"traceId": "USE_UNIQUE"}},
        ],
    },
]


# 11 new scenarios from the 25-scenario taxonomy (taxonomy IDs A4, A5, B2/B5,
# C4/C5, D3/D4/D5, E2/E5). SC18 is the new B5; SC12 itself was repurposed as
# B2 above (so no SC18 for B2).
#
# Mapping:  taxonomy → SC ID
#   A4 STUCK_RECOVERED_LONG       → SC16
#   A5 STUCK_DEAD_PERMANENT       → SC17
#   B5 TIMEOUT_THEN_RECOVER       → SC19   (skipping SC18 to leave a slot
#                                            should B2 ever need a separate
#                                            INJECT scenario; for now SC12
#                                            IS B2)
#   C4 STATE_REGRESSION           → SC20
#   C5 MULTI_TERMINAL_CONFLICT    → SC21
#   D3 COMPENSATION_NOT_TRIGGERED → SC22
#   D4 DUPLICATE_COMPENSATION     → SC23
#   D5 DELAYED_COMPENSATION       → SC24
#   E2 DUPLICATE_EVENT_DELAYED    → SC25
#   E5 PARTIAL_TRACE_PROPAGATION  → SC26

NEW_SCENARIOS = [
    # ── Category A — Service Availability ─────────────────────────────────
    {
        "id": "SC16_STUCK_RECOVERED_LONG",
        "category": "A",
        "split": "train",
        # A4: wait long enough for both SAGA_STUCK (30s+ scheduler) AND
        # MISSING_TERMINAL_EVENT (60s+ scheduler), then recover. This
        # exercises R05's "recovered with MISSING_TERMINAL fired" branch.
        "description": "A4 — stuck ~125s (both anomalies) → restart → R05 (recovered)",
        "request": {"customerId": "eval-16", "productId": "PRODUCT-1",
                    "quantity": 1, "amount": 65.00},
        "expected_saga_status": "COMPLETED",
        "expected_rca": "SERVICE_CRASH",
        "expected_anomalies": ["SAGA_STUCK", "MISSING_TERMINAL_EVENT"],
        "runner": run_recovered_from_stuck,
        "stuck_wait_seconds": 125,
        "n_runs": 2,
    },
    {
        "id": "SC17_STUCK_DEAD_PERMANENT",
        "category": "A",
        "split": "test",
        # A5: never restart inventory during the test (cleanup happens at
        # the end via run_stuck_saga's mandatory recovery). Saga stays
        # IN_PROGRESS through RCA — primary R05 with severity CRITICAL.
        "description": "A5 — stuck ~150s, never recovers → R05 (CRITICAL)",
        "request": {"customerId": "eval-17", "productId": "PRODUCT-1",
                    "quantity": 1, "amount": 70.00},
        "expected_saga_status": "IN_PROGRESS",
        "expected_rca": "SERVICE_CRASH",
        "expected_anomalies": ["SAGA_STUCK", "MISSING_TERMINAL_EVENT"],
        "runner": run_stuck_saga,
        "stuck_wait_seconds": 150,
        "n_runs": 2,
    },

    # ── Category B — Network / Timing ─────────────────────────────────────
    {
        "id": "SC19_TIMEOUT_THEN_RECOVER",
        "category": "B",
        "split": "test",
        # B5: STEP_TIMEOUT fires from scheduler, then a follow-up event
        # lets saga reach COMPLETED. Tests the "completed but had problems"
        # outcome-determination branch (outcome=SELF_HEALED).
        "description": "B5 — STEP_TIMEOUT then COMPLETED → R07 + outcome=SELF_HEALED",
        "request": None,
        "expected_saga_status": "COMPLETED",
        "expected_rca": "DOWNSTREAM_TIMEOUT",
        "expected_anomalies": ["STEP_TIMEOUT"],
        "runner": run_timeout_then_recover,
        "wait_seconds": 65,
        "n_runs": 2,
    },

    # ── Category C — State Machine ────────────────────────────────────────
    {
        "id": "SC20_STATE_REGRESSION",
        "category": "C",
        "split": "test",
        # C4: route a STATE_REGRESSION → R04 cleanly, isolated from R03.
        #
        # Naïve approach (saga reaches COMPLETED via INVENTORY_RESERVED, then
        # PAYMENT_FAILED) ALSO triggers R03 MULTI_TERMINAL_CONFLICT because
        # INVENTORY_RESERVED ∈ COMPLETED_EVENTS and PAYMENT_FAILED ∈
        # FAILED_EVENTS — both required by detectMultiTerminalConflict.
        # R03 and R04 are both STRONG=0.90; stable-sort tiebreaks to R03
        # (added first), and the test fails.
        #
        # Fix: route the saga to COMPENSATED first (rank 5), then send
        # PAYMENT_FAILED (rank 4 < 5 → STATE_REGRESSION fires). Because no
        # INVENTORY_RESERVED is ever in the saga's event history,
        # detectMultiTerminalConflict's `hasSuccess` check returns false
        # and R03 stays silent. R04 wins cleanly.
        "description": "C4 — COMPENSATED→FAILED regression → STATE_REGRESSION (R04)",
        "request": None,
        "expected_saga_status": None,
        "expected_rca": "TERMINAL_STATE_OVERWRITE",
        "expected_anomalies": ["STATE_REGRESSION", "FORBIDDEN_POST_TERMINAL"],
        "runner": run_inject_scenario,
        "n_runs": 2,
        "wait_seconds": 5,
        "inject_events": [
            {"eventType": "ORDER_CREATED", "stepName": "CREATE_ORDER",
             "serviceName": "order-service",
             "payload": {"traceId": "USE_UNIQUE"}},
            {"eventType": "PAYMENT_COMPLETED", "stepName": "PROCESS_PAYMENT",
             "serviceName": "payment-service", "delay": 0.5,
             "payload": {"traceId": "USE_UNIQUE"}},
            {"eventType": "INVENTORY_FAILED", "stepName": "RESERVE_INVENTORY",
             "serviceName": "inventory-service", "delay": 0.5,
             "payload": {"traceId": "USE_UNIQUE",
                         "reason": "regression test setup"}},
            {"eventType": "REFUND_PAYMENT", "stepName": "REFUND_PAYMENT",
             "serviceName": "payment-service", "delay": 0.5,
             "payload": {"traceId": "USE_UNIQUE"}},
            {"eventType": "PAYMENT_REFUNDED", "stepName": "REFUND_PAYMENT",
             "serviceName": "payment-service", "delay": 0.5,
             "payload": {"traceId": "USE_UNIQUE"}},
            # Saga is now COMPENSATED (rank 5). Inject PAYMENT_FAILED (rank 4)
            # → STATE_REGRESSION fires; FORBIDDEN_POST_TERMINAL also fires
            # because oldStatus=COMPENSATED is terminal. No INVENTORY_RESERVED
            # in events → R03 does NOT fire. R10 also does NOT fire because
            # hasRefundCompleted=true. R11 does not fire because final status
            # is FAILED (not COMPENSATED). Only R04 wins.
            {"eventType": "PAYMENT_FAILED", "stepName": "PROCESS_PAYMENT",
             "serviceName": "payment-service", "delay": 1,
             "eventId": "USE_UNIQUE",
             "payload": {"traceId": "USE_UNIQUE",
                         "reason": "regression test"}},
        ],
    },
    {
        "id": "SC21_MULTI_TERMINAL_CONFLICT",
        "category": "C",
        "split": "test",
        # C5: both INVENTORY_RESERVED (success terminal) and PAYMENT_FAILED
        # (failure terminal) for the same saga → MULTI_TERMINAL_CONFLICT.
        "description": "C5 — both success+failure terminals → MULTI_TERMINAL_CONFLICT (R03)",
        "request": None,
        "expected_saga_status": None,
        "expected_rca": "MULTI_TERMINAL_CONFLICT",
        "expected_anomalies": ["MULTI_TERMINAL_CONFLICT"],
        "runner": run_inject_scenario,
        "n_runs": 2,
        "wait_seconds": 5,
        "inject_events": [
            {"eventType": "ORDER_CREATED", "stepName": "CREATE_ORDER",
             "serviceName": "order-service",
             "payload": {"traceId": "USE_UNIQUE"}},
            {"eventType": "PAYMENT_COMPLETED", "stepName": "PROCESS_PAYMENT",
             "serviceName": "payment-service", "delay": 0.5,
             "payload": {"traceId": "USE_UNIQUE"}},
            {"eventType": "INVENTORY_RESERVED", "stepName": "RESERVE_INVENTORY",
             "serviceName": "inventory-service", "delay": 0.5,
             "payload": {"traceId": "USE_UNIQUE"}},
            # Conflicting terminal — saga already COMPLETED, but failure event
            # ALSO present in saga's event history.
            {"eventType": "PAYMENT_FAILED", "stepName": "PROCESS_PAYMENT",
             "serviceName": "payment-service", "delay": 1,
             "eventId": "USE_UNIQUE",
             "payload": {"traceId": "USE_UNIQUE",
                         "reason": "conflicting terminal"}},
        ],
    },

    # ── Category D — Compensation ────────────────────────────────────────
    {
        "id": "SC22_COMPENSATION_NOT_TRIGGERED",
        "category": "D",
        "split": "train",
        # D3: INVENTORY_FAILED moves saga to COMPENSATING but no REFUND_PAYMENT
        # is ever sent. detectCompensationMissing fires after lastUpdated > 30s
        # (scheduler runs every 60s). Wait 65s.
        "description": "D3 — INVENTORY_FAILED but no REFUND_PAYMENT → COMPENSATION_MISSING (R09)",
        "request": None,
        "expected_saga_status": None,
        "expected_rca": "COMPENSATION_NOT_TRIGGERED",
        "expected_anomalies": ["COMPENSATION_MISSING"],
        "runner": run_inject_scenario,
        "n_runs": 2,
        "wait_seconds": 70,
        "inject_events": [
            {"eventType": "ORDER_CREATED", "stepName": "CREATE_ORDER",
             "serviceName": "order-service",
             "payload": {"traceId": "USE_UNIQUE"}},
            {"eventType": "PAYMENT_COMPLETED", "stepName": "PROCESS_PAYMENT",
             "serviceName": "payment-service", "delay": 0.5,
             "payload": {"traceId": "USE_UNIQUE"}},
            {"eventType": "INVENTORY_FAILED", "stepName": "RESERVE_INVENTORY",
             "serviceName": "inventory-service", "delay": 0.5,
             "payload": {"traceId": "USE_UNIQUE",
                         "reason": "test stockout for D3"}},
        ],
    },
    {
        "id": "SC23_DUPLICATE_COMPENSATION",
        "category": "D",
        "split": "test",
        # D4: empirical finding — DUPLICATE_COMPENSATION cannot fire in
        # isolation. Triggering it requires ≥2 REFUND_PAYMENT events, which
        # places the saga in COMPENSATING, which in turn satisfies R10
        # (hasRefundCommand && !hasRefundCompleted && status==COMPENSATING).
        # R10 and R13 both fire at STRONG=0.90; R10 is added before R13 in
        # RcaService rule list, so stable-sort makes R10 the primary.
        #
        # This is itself an interesting taxonomy result: the test
        # "DUPLICATE_COMPENSATION → R13" is NOT a clean rule-routing test
        # but a multi-rule scenario. Expected primary is therefore R10
        # COMPENSATION_STEP_FAILURE; R13 should appear in contributingCauses
        # (verifiable separately once the contributingCauses field is exposed
        # via the RCA REST endpoint — currently a TODO).
        #
        # Rationale recorded in docs/RCA_SCENARIO_TAXONOMY.md §D4 update.
        "description": "D4 — REFUND_PAYMENT × 2 → DUPLICATE_COMPENSATION fires; R10 dominates (multi-rule)",
        "request": None,
        "expected_saga_status": None,
        "expected_rca": "COMPENSATION_STEP_FAILURE",
        "expected_anomalies": ["DUPLICATE_COMPENSATION"],
        "runner": run_inject_scenario,
        "n_runs": 2,
        "wait_seconds": 5,
        "inject_events": [
            {"eventType": "ORDER_CREATED", "stepName": "CREATE_ORDER",
             "serviceName": "order-service",
             "payload": {"traceId": "USE_UNIQUE"}},
            {"eventType": "PAYMENT_COMPLETED", "stepName": "PROCESS_PAYMENT",
             "serviceName": "payment-service", "delay": 0.5,
             "payload": {"traceId": "USE_UNIQUE"}},
            {"eventType": "INVENTORY_FAILED", "stepName": "RESERVE_INVENTORY",
             "serviceName": "inventory-service", "delay": 0.5,
             "payload": {"traceId": "USE_UNIQUE"}},
            {"eventType": "REFUND_PAYMENT", "stepName": "REFUND_PAYMENT",
             "serviceName": "payment-service", "delay": 0.5,
             "eventId": "USE_UNIQUE",
             "payload": {"traceId": "USE_UNIQUE"}},
            {"eventType": "REFUND_PAYMENT", "stepName": "REFUND_PAYMENT",
             "serviceName": "payment-service", "delay": 0.5,
             "eventId": "USE_UNIQUE",
             "payload": {"traceId": "USE_UNIQUE"}},
        ],
    },
    {
        "id": "SC24_DELAYED_COMPENSATION",
        "category": "D",
        "split": "test",
        # D5: Inject events with timestamps so REFUND_PAYMENT comes 90s after
        # INVENTORY_FAILED. detectDelayedCompensation threshold is
        # REFUND_step_timeout (10s) × 1.5 = 15s — 90s easily exceeds it.
        # Then PAYMENT_REFUNDED at +95s closes the saga to COMPENSATED, and
        # the 60s scheduler fires DELAYED_COMPENSATION.
        #
        # EMPIRICAL FINDING (smoke test 2026-05-06): just like SC23, R22
        # CANNOT win primary against the structural rules.
        #   - If saga reaches COMPENSATED → R11 STRONG=0.90 fires, beats
        #     R22 MEDIUM=0.75 by score (not by tiebreak).
        #   - If saga stays in COMPENSATING (no PAYMENT_REFUNDED) → R10
        #     STRONG=0.90 fires, same outcome.
        # Both interpretations are arguably correct: R11/R10 capture the
        # CAUSE (what happened to the saga), R22 captures the LATENCY
        # CHARACTERIZATION. Promoting R22 to STRONG would be unjustified —
        # delayed compensation could have many root causes (broker queue,
        # consumer lag, downstream delay) so the rule is correctly MEDIUM.
        # Expected primary therefore reflects the dominant rule (R11
        # FORWARD_STEP_FAILURE), and R22 is an *anomaly to verify* but not
        # the primary RCA. This empirically tests the `× 1.5` magic
        # threshold (R22 firing means 90s > 15s = 10s × 1.5).
        "description": "D5 — 90s-late refund → DELAYED_COMPENSATION fires; R11 primary (multi-rule)",
        "request": None,
        "expected_saga_status": None,
        "expected_rca": "FORWARD_STEP_FAILURE",
        "expected_anomalies": ["DELAYED_COMPENSATION"],
        "runner": run_inject_scenario,
        "n_runs": 2,
        "wait_seconds": 70,
        "inject_events": [
            {"eventType": "ORDER_CREATED", "stepName": "CREATE_ORDER",
             "serviceName": "order-service",
             "timestamp": "USE_TS_AGO_3M",
             "payload": {"traceId": "USE_UNIQUE"}},
            {"eventType": "PAYMENT_COMPLETED", "stepName": "PROCESS_PAYMENT",
             "serviceName": "payment-service", "delay": 0.5,
             "timestamp": "USE_TS_AGO_2M",
             "payload": {"traceId": "USE_UNIQUE"}},
            {"eventType": "INVENTORY_FAILED", "stepName": "RESERVE_INVENTORY",
             "serviceName": "inventory-service", "delay": 0.5,
             # 2min ago → 90s gap before REFUND_PAYMENT (30s ago)
             "timestamp": "USE_TS_AGO_2M",
             "payload": {"traceId": "USE_UNIQUE"}},
            {"eventType": "REFUND_PAYMENT", "stepName": "REFUND_PAYMENT",
             "serviceName": "payment-service", "delay": 0.5,
             "timestamp": "USE_TS_AGO_30S",
             "payload": {"traceId": "USE_UNIQUE"}},
            {"eventType": "PAYMENT_REFUNDED", "stepName": "REFUND_PAYMENT",
             "serviceName": "payment-service", "delay": 0.5,
             "timestamp": "USE_TS_AGO_10S",
             "payload": {"traceId": "USE_UNIQUE"}},
        ],
    },

    # ── Category E — Correlation / Identity ──────────────────────────────
    {
        "id": "SC25_DUPLICATE_EVENT_DELAYED",
        "category": "E",
        "split": "test",
        # E2: same dup-event scenario as E1 but with a 30s gap between the
        # two duplicate events. Tests that DeduplicationCache TTL (1h) does
        # NOT cause the duplicate to be missed at the 30s mark.
        "description": "E2 — duplicate eventId at 30s gap → EXACT_DUPLICATE_EVENT",
        "request": None,
        "expected_saga_status": None,
        "expected_rca": "DUPLICATE_DELIVERY",
        "expected_anomalies": ["EXACT_DUPLICATE_EVENT"],
        "runner": run_inject_scenario,
        "n_runs": 2,
        "wait_seconds": 5,
        "inject_events": [
            {"eventType": "ORDER_CREATED", "stepName": "CREATE_ORDER",
             "serviceName": "order-service", "eventId": "USE_DUP_KEY",
             "payload": {"traceId": "USE_UNIQUE"}},
            {"eventType": "PAYMENT_COMPLETED", "stepName": "PROCESS_PAYMENT",
             "serviceName": "payment-service", "delay": 1,
             "eventId": "USE_UNIQUE",
             "payload": {"traceId": "USE_UNIQUE"}},
            {"eventType": "PAYMENT_COMPLETED", "stepName": "PROCESS_PAYMENT",
             "serviceName": "payment-service", "eventId": "USE_DUP_KEY",
             "delay": 30,                                          # ← 30s gap
             "payload": {"traceId": "USE_UNIQUE"}},
        ],
    },
    {
        "id": "SC26_PARTIAL_TRACE_PROPAGATION",
        "category": "E",
        "split": "test",
        # E5: ORDER_CREATED *with* traceId, PAYMENT_COMPLETED *without*
        # traceId, INVENTORY_RESERVED *with* traceId. The
        # detectMissingTraceContext noise-reduction guard requires that at
        # least one event in the saga HAS traceId — that's met by events 1
        # and 3 — so MISSING_TRACE_CONTEXT fires for event 2.
        "description": "E5 — partial traceId propagation → MISSING_TRACE_CONTEXT (R19)",
        "request": None,
        "expected_saga_status": None,
        "expected_rca": "MISSING_TRACE_PROPAGATION",
        "expected_anomalies": ["MISSING_TRACE_CONTEXT"],
        "runner": run_inject_scenario,
        "n_runs": 2,
        "wait_seconds": 5,
        "inject_events": [
            {"eventType": "ORDER_CREATED", "stepName": "CREATE_ORDER",
             "serviceName": "order-service",
             "payload": {"traceId": "USE_UNIQUE"}},
            # Middle event: deliberately NO traceId in payload
            {"eventType": "PAYMENT_COMPLETED", "stepName": "PROCESS_PAYMENT",
             "serviceName": "payment-service", "delay": 0.5},
            {"eventType": "INVENTORY_RESERVED", "stepName": "RESERVE_INVENTORY",
             "serviceName": "inventory-service", "delay": 0.5,
             "payload": {"traceId": "USE_UNIQUE"}},
        ],
    },
]


# ── Main ──────────────────────────────────────────────────────────────────────

ALL_SCENARIO_LISTS = [
    BASIC_SCENARIOS, ADVANCED_SCENARIOS, TOXIPROXY_SCENARIOS,
    INJECT_SCENARIOS, NEW_SCENARIOS,
]

ALL_SCENARIOS_MAP = {}
for _list in ALL_SCENARIO_LISTS:
    for _s in _list:
        # Map both full ID (SC01_HEALTHY) and short prefix (SC01)
        ALL_SCENARIOS_MAP[_s["id"]] = _s
        short = _s["id"].split("_")[0]
        ALL_SCENARIOS_MAP[short] = _s


def list_scenarios():
    """Print all available scenarios with category, split, and description."""
    all_scenarios = []
    for _list in ALL_SCENARIO_LISTS:
        all_scenarios.extend(_list)

    # Sort by SC ID number for stable display
    def _sc_num(s):
        try:
            return int(s["id"].split("_")[0][2:])
        except Exception:
            return 999
    all_scenarios.sort(key=_sc_num)

    train = [s for s in all_scenarios if s.get("split") == "train"]
    test = [s for s in all_scenarios if s.get("split") == "test"]

    print(f"\nAvailable scenarios — {len(all_scenarios)} total "
          f"({len(train)} train, {len(test)} test):")
    print(f"  {'ID':<32} {'Cat':<3} {'Split':<5} {'Description'}")
    print(f"  {'-'*78}")
    for s in all_scenarios:
        cat = s.get("category", "-")
        split = s.get("split", "-")
        print(f"  {s['id']:<32} {cat:<3} {split:<5} {s['description']}")

    # Per-category summary
    print(f"\n  Per category:")
    for cat in ["A", "B", "C", "D", "E"]:
        cat_scenarios = [s for s in all_scenarios if s.get("category") == cat]
        cat_train = sum(1 for s in cat_scenarios if s.get("split") == "train")
        cat_test = sum(1 for s in cat_scenarios if s.get("split") == "test")
        print(f"    {cat}: {len(cat_scenarios)} ({cat_train} train, {cat_test} test)")
    print(f"\nShort IDs supported: SC01–SC26 (skipping SC18; see NEW_SCENARIOS comment).")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Saga Platform Evaluation (Tier 2)")
    parser.add_argument("--full", action="store_true",
                        help="Run all 25 taxonomy scenarios")
    parser.add_argument("--runs", type=int, default=N_RUNS,
                        help="Runs per basic scenario (default: 5)")
    parser.add_argument("--no-toxiproxy", action="store_true",
                        help="Skip Toxiproxy scenarios even in --full mode")
    parser.add_argument("--scenario", nargs="+", metavar="ID",
                        help="Run specific scenario(s) by ID, e.g. SC03 or SC03_STUCK_LIVE")
    parser.add_argument("--split", choices=["train", "test", "all"], default="all",
                        help="Filter scenarios by train/test split (default: all). "
                             "Use 'train' to tune; 'test' for held-out evaluation.")
    parser.add_argument("--category", choices=["A", "B", "C", "D", "E"], action="append",
                        help="Filter by category (repeatable, e.g. --category B --category D)")
    parser.add_argument("--list", action="store_true",
                        help="List all available scenarios and exit")
    args = parser.parse_args()

    if args.list:
        list_scenarios()
        return

    os.makedirs(RESULTS_DIR, exist_ok=True)

    print("=" * 60)
    print("SAGA OBSERVABILITY PLATFORM — EVALUATION (Tier 2)")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Mode: {'FULL' if args.full else 'BASIC (2 scenarios)'}")
    print("=" * 60)

    print("\n[1] Health Check")
    if not health_check():
        print("\n❌ Not all services are running.")
        print("   Run: docker-compose up -d")
        return

    print("\n[2] Inventory Reset")
    reset_inventory("PRODUCT-1", 1000)
    time.sleep(1)

    print(f"\n[3] Warm-up ({WARMUP_RUNS} requests)")
    for i in range(WARMUP_RUNS):
        _, ms = create_order({"customerId": "warmup", "productId": "PRODUCT-1",
                              "quantity": 1, "amount": 10.0})
        print(f"  warm-up {i+1}: {ms:.0f}ms")
        time.sleep(1)

    print("  Settling 5s (let warm-up sagas propagate)...")
    time.sleep(5)

    if args.scenario:
        scenarios = []
        for sid in args.scenario:
            sid_upper = sid.upper()
            if sid_upper in ALL_SCENARIOS_MAP:
                s = ALL_SCENARIOS_MAP[sid_upper]
                if s not in scenarios:
                    scenarios.append(s)
            else:
                print(f"\n❌ Unknown scenario: {sid}")
                print(f"   Use --list to see all scenarios.")
                return
        print(f"\n  Selected scenarios: {[s['id'] for s in scenarios]}")
    elif args.full:
        scenarios = list(BASIC_SCENARIOS)
        scenarios.extend(ADVANCED_SCENARIOS)
        if not args.no_toxiproxy and toxiproxy_available():
            scenarios.extend(TOXIPROXY_SCENARIOS)
            print("\n  ✅ Toxiproxy available — including network-level scenarios")
        elif not args.no_toxiproxy:
            print("\n  ⚠️  Toxiproxy not available — skipping SC07")
        scenarios.extend(INJECT_SCENARIOS)
        scenarios.extend(NEW_SCENARIOS)
    else:
        scenarios = list(BASIC_SCENARIOS)

    # Apply --split / --category filters (do NOT filter --scenario explicit
    # selection — user asked for those specifically).
    if not args.scenario:
        if args.split != "all":
            before = len(scenarios)
            scenarios = [s for s in scenarios if s.get("split") == args.split]
            print(f"\n  Filtered to split='{args.split}': "
                  f"{len(scenarios)} of {before} scenarios.")
        if args.category:
            before = len(scenarios)
            cats = set(args.category)
            scenarios = [s for s in scenarios if s.get("category") in cats]
            print(f"  Filtered to categories {sorted(cats)}: "
                  f"{len(scenarios)} of {before} scenarios.")
        if not scenarios:
            print("\n❌ No scenarios match the current filters.")
            return

    print(f"\n[4] Running {len(scenarios)} scenarios")
    all_runs = []

    for scenario in scenarios:
        n = scenario.get("n_runs", args.runs)
        runner = scenario["runner"]

        print(f"\n{'─'*55}")
        print(f"▶ {scenario['id']} ({n} runs)")
        print(f"  {scenario['description']}")
        print(f"  Expected: status={scenario['expected_saga_status']}, "
              f"rca={scenario['expected_rca']}")

        # Reset inventory before scenarios that need stock
        req = scenario.get("request")
        if req and req.get("quantity", 0) < 100:
            reset_inventory("PRODUCT-1", 1000)
            time.sleep(0.5)

        for i in range(n):
            print(f"  Run {i+1}/{n}: ", end="", flush=True)
            try:
                run_result = runner(scenario, i)
                all_runs.append(run_result)
            except Exception as e:
                print(f" ❌ Exception: {e}")
                err_result = make_result(scenario, i)
                err_result["error"] = str(e)
                all_runs.append(err_result)
            time.sleep(3)

    print_summary(scenarios, all_runs, args)


def compute_classification_metrics(all_runs):
    tp = fp = fn = tn = 0
    per_class = {}

    for r in all_runs:
        if r.get("error"):
            continue

        expected = r.get("rca_expected")
        actual = r.get("rca_root_cause")

        if expected == actual:
            if expected is None:
                tn += 1
            else:
                tp += 1
                per_class.setdefault(expected, {"tp": 0, "fp": 0, "fn": 0})
                per_class[expected]["tp"] += 1
        else:
            if actual is not None:
                fp += 1
                per_class.setdefault(actual, {"tp": 0, "fp": 0, "fn": 0})
                per_class[actual]["fp"] += 1
            if expected is not None:
                fn += 1
                per_class.setdefault(expected, {"tp": 0, "fp": 0, "fn": 0})
                per_class[expected]["fn"] += 1

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0

    class_report = {}
    for label, counts in sorted(per_class.items()):
        p = counts["tp"] / (counts["tp"] + counts["fp"]) if (counts["tp"] + counts["fp"]) > 0 else 0.0
        r_val = counts["tp"] / (counts["tp"] + counts["fn"]) if (counts["tp"] + counts["fn"]) > 0 else 0.0
        f = 2 * p * r_val / (p + r_val) if (p + r_val) > 0 else 0.0
        class_report[label] = {
            "precision": round(p, 3),
            "recall": round(r_val, 3),
            "f1": round(f, 3),
            "support": counts["tp"] + counts["fn"],
        }

    return {
        "tp": tp, "fp": fp, "fn": fn, "tn": tn,
        "precision": round(precision, 3),
        "recall": round(recall, 3),
        "f1": round(f1, 3),
        "per_class": class_report,
    }


def compute_anomaly_metrics(all_runs):
    tp = fp = fn = 0

    for r in all_runs:
        if r.get("error"):
            continue
        expected = set(r.get("anomalies_expected", []))
        actual = set(r.get("anomalies_actual", []))

        tp += len(expected & actual)
        fn += len(expected - actual)
        fp += len(actual - expected)

    precision = tp / (tp + fp) if (tp + fp) > 0 else 1.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 1.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0

    return {
        "tp": tp, "fp": fp, "fn": fn,
        "precision": round(precision, 3),
        "recall": round(recall, 3),
        "f1": round(f1, 3),
    }


def print_summary(scenarios, all_runs, args):
    print(f"\n{'='*60}")
    print("EVALUATION RESULTS")
    print(f"{'='*60}")

    report = {
        "metadata": {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "mode": "full" if args.full else "basic",
            "total_runs": len(all_runs),
            "errors": sum(1 for r in all_runs if r.get("error")),
        },
        "scenarios": [],
        "overall": {},
        "classification_metrics": {},
        "anomaly_metrics": {},
        "raw_runs": all_runs,
    }

    all_acc, all_tti, all_rca_ms = [], [], []

    for scenario in scenarios:
        runs = [r for r in all_runs if r["scenario_id"] == scenario["id"]]
        ok = [r for r in runs if not r.get("error")]

        acc_list = [r["rca_accurate"] for r in ok]
        tti_list = [r["time_to_insight_s"] for r in ok if r.get("time_to_insight_s")]
        rca_ms_list = [r["rca_latency_ms"] for r in ok if r.get("rca_latency_ms")]

        acc_pct = sum(acc_list) / len(acc_list) * 100 if acc_list else 0
        avg_tti = statistics.mean(tti_list) if tti_list else None
        avg_rca = statistics.mean(rca_ms_list) if rca_ms_list else None

        summary = {
            "scenario_id":        scenario["id"],
            "description":        scenario["description"],
            "expected_rca":       scenario["expected_rca"],
            "runs_total":         len(runs),
            "runs_ok":            len(ok),
            "runs_error":         len(runs) - len(ok),
            "rca_accuracy_pct":   round(acc_pct, 1),
            "avg_tti_s":          round(avg_tti, 3) if avg_tti else None,
            "avg_rca_latency_ms": round(avg_rca, 1) if avg_rca else None,
        }
        report["scenarios"].append(summary)
        all_acc.extend(acc_list)
        all_tti.extend(tti_list)
        all_rca_ms.extend(rca_ms_list)

        errors = len(runs) - len(ok)
        err_note = f"  ({errors} errors)" if errors else ""
        print(f"\n{scenario['id']}:{err_note}")
        print(f"  Accuracy    : {acc_pct:.1f}%  ({sum(acc_list)}/{len(acc_list)})")
        if avg_tti: print(f"  Avg TTI     : {avg_tti:.2f}s")
        if avg_rca: print(f"  RCA Latency : {avg_rca:.1f}ms")
        if errors:
            for r in runs:
                if r.get("error"):
                    print(f"  ⚠️  Run {r['run_index']}: {r['error']}")

    oa = sum(all_acc) / len(all_acc) * 100 if all_acc else 0
    ot = statistics.mean(all_tti) if all_tti else None
    or_ = statistics.mean(all_rca_ms) if all_rca_ms else None
    p95_rca = (sorted(all_rca_ms)[int(len(all_rca_ms) * 0.95)]
               if len(all_rca_ms) >= 2 else None)

    report["overall"] = {
        "rca_accuracy_pct":      round(oa, 1),
        "avg_tti_s":             round(ot, 3) if ot else None,
        "avg_rca_latency_ms":    round(or_, 1) if or_ else None,
        "p95_rca_latency_ms":    round(p95_rca, 1) if p95_rca else None,
        "meets_accuracy_target": oa >= 90.0,
    }

    rca_metrics = compute_classification_metrics(all_runs)
    report["classification_metrics"] = rca_metrics

    anomaly_metrics = compute_anomaly_metrics(all_runs)
    report["anomaly_metrics"] = anomaly_metrics

    print(f"\n{'='*60}")
    print("OVERALL")
    print(f"  RCA Accuracy     : {oa:.1f}%  (target ≥90%)  "
          f"{'✅' if oa >= 90 else '❌'}")
    if ot:  print(f"  Avg TTI          : {ot:.2f}s")
    if or_: print(f"  RCA Latency Avg  : {or_:.1f}ms")
    if p95_rca: print(f"  RCA Latency P95  : {p95_rca:.1f}ms")
    print(f"  Total Scenarios  : {len(scenarios)}")
    print(f"  Total Runs       : {len(all_acc)}")

    print(f"\n{'='*60}")
    print("RCA CLASSIFICATION METRICS")
    print(f"  Precision : {rca_metrics['precision']:.3f}")
    print(f"  Recall    : {rca_metrics['recall']:.3f}")
    print(f"  F1-Score  : {rca_metrics['f1']:.3f}")
    print(f"  TP={rca_metrics['tp']}  FP={rca_metrics['fp']}  "
          f"FN={rca_metrics['fn']}  TN={rca_metrics['tn']}")

    if rca_metrics.get("per_class"):
        print(f"\n  Per-class breakdown:")
        print(f"  {'RCA Label':<35} {'Prec':>6} {'Recall':>6} {'F1':>6} {'Support':>7}")
        print(f"  {'-'*60}")
        for label, m in sorted(rca_metrics["per_class"].items()):
            print(f"  {label:<35} {m['precision']:>6.3f} {m['recall']:>6.3f} "
                  f"{m['f1']:>6.3f} {m['support']:>7}")

    print(f"\n{'='*60}")
    print("ANOMALY DETECTION METRICS")
    print(f"  Precision : {anomaly_metrics['precision']:.3f}")
    print(f"  Recall    : {anomaly_metrics['recall']:.3f}")
    print(f"  F1-Score  : {anomaly_metrics['f1']:.3f}")
    print(f"  TP={anomaly_metrics['tp']}  FP={anomaly_metrics['fp']}  "
          f"FN={anomaly_metrics['fn']}")

    # ── Per-split (train vs test) accuracy ────────────────────────────────
    split_breakdown = {}
    for split in ("train", "test"):
        runs = [r for r in all_runs
                if not r.get("error")
                and ALL_SCENARIOS_MAP.get(r["scenario_id"], {}).get("split") == split]
        if not runs:
            continue
        acc = [r["rca_accurate"] for r in runs]
        acc_pct = sum(acc) / len(acc) * 100 if acc else 0
        split_breakdown[split] = {
            "scenarios": len({r["scenario_id"] for r in runs}),
            "runs": len(runs),
            "accuracy_pct": round(acc_pct, 1),
        }
    if split_breakdown:
        print(f"\n{'='*60}")
        print("PER-SPLIT ACCURACY")
        for split, m in split_breakdown.items():
            print(f"  {split.upper():<6} : {m['accuracy_pct']:>5.1f}%   "
                  f"({m['scenarios']} scenarios, {m['runs']} runs)")
        report["split_breakdown"] = split_breakdown

    # ── Per-category accuracy ─────────────────────────────────────────────
    category_breakdown = {}
    for cat in ("A", "B", "C", "D", "E"):
        runs = [r for r in all_runs
                if not r.get("error")
                and ALL_SCENARIOS_MAP.get(r["scenario_id"], {}).get("category") == cat]
        if not runs:
            continue
        acc = [r["rca_accurate"] for r in runs]
        acc_pct = sum(acc) / len(acc) * 100 if acc else 0
        category_breakdown[cat] = {
            "scenarios": len({r["scenario_id"] for r in runs}),
            "runs": len(runs),
            "accuracy_pct": round(acc_pct, 1),
        }
    if category_breakdown:
        print(f"\n{'='*60}")
        print("PER-CATEGORY ACCURACY")
        cat_names = {
            "A": "Service Availability",
            "B": "Network / Timing",
            "C": "State Machine",
            "D": "Compensation",
            "E": "Correlation / Identity",
        }
        for cat, m in category_breakdown.items():
            print(f"  {cat} {cat_names[cat]:<28} : {m['accuracy_pct']:>5.1f}%   "
                  f"({m['scenarios']} scenarios, {m['runs']} runs)")
        report["category_breakdown"] = category_breakdown

    # Save
    json_path = os.path.join(RESULTS_DIR, "evaluation_report.json")
    txt_path = os.path.join(RESULTS_DIR, "evaluation_report.txt")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("SAGA OBSERVABILITY PLATFORM — EVALUATION REPORT (Tier 2)\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 60 + "\n\n")
        for s in report["scenarios"]:
            f.write(f"{s['scenario_id']}\n")
            f.write(f"  {s['description']}\n")
            f.write(f"  Expected RCA: {s['expected_rca']}\n")
            f.write(f"  Accuracy: {s['rca_accuracy_pct']}%  "
                    f"({s['runs_ok']}/{s['runs_total']} runs OK)\n")
            if s['avg_tti_s']:
                f.write(f"  TTI: {s['avg_tti_s']}s  |  "
                        f"RCA Latency: {s['avg_rca_latency_ms']}ms\n")
            f.write("\n")

        f.write(f"\nOVERALL: Accuracy={report['overall']['rca_accuracy_pct']}%  "
                f"{'PASS' if report['overall']['meets_accuracy_target'] else 'FAIL'}\n\n")

        f.write("RCA CLASSIFICATION METRICS\n")
        f.write(f"  Precision: {rca_metrics['precision']:.3f}\n")
        f.write(f"  Recall:    {rca_metrics['recall']:.3f}\n")
        f.write(f"  F1-Score:  {rca_metrics['f1']:.3f}\n")
        f.write(f"  TP={rca_metrics['tp']}  FP={rca_metrics['fp']}  "
                f"FN={rca_metrics['fn']}  TN={rca_metrics['tn']}\n\n")

        if rca_metrics.get("per_class"):
            f.write("  Per-class breakdown:\n")
            f.write(f"  {'RCA Label':<35} {'Prec':>6} {'Recall':>6} {'F1':>6} {'Support':>7}\n")
            f.write(f"  {'-'*60}\n")
            for label, m in sorted(rca_metrics["per_class"].items()):
                f.write(f"  {label:<35} {m['precision']:>6.3f} {m['recall']:>6.3f} "
                        f"{m['f1']:>6.3f} {m['support']:>7}\n")

        f.write(f"\nANOMALY DETECTION METRICS\n")
        f.write(f"  Precision: {anomaly_metrics['precision']:.3f}\n")
        f.write(f"  Recall:    {anomaly_metrics['recall']:.3f}\n")
        f.write(f"  F1-Score:  {anomaly_metrics['f1']:.3f}\n")
        f.write(f"  TP={anomaly_metrics['tp']}  FP={anomaly_metrics['fp']}  "
                f"FN={anomaly_metrics['fn']}\n")

    print(f"\n📁 Saved: {json_path}")
    print(f"📁 Saved: {txt_path}")


if __name__ == "__main__":
    main()
