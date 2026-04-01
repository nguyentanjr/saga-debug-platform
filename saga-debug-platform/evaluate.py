"""
Saga Observability Platform — Evaluation Script
===================================================
Đo 3 metrics chính cho luận văn:
  1. RCA Accuracy (%) — so sánh rootCause vs expected
  2. Time-to-Insight (s) — từ tạo order đến có RCA result
  3. RCA Latency (ms) — thời gian chạy RCA analysis

6 Scenarios: HEALTHY, COMPENSATION_RECOVERED, STUCK_SAGA,
  RECOVERED_FROM_STUCK, COMPENSATION_FAILED, CIRCUIT_BREAKER_OPEN

Cách chạy:
  pip install requests
  python evaluate.py              # chạy 2 scenario cơ bản (auto)
  python evaluate.py --full       # chạy cả 6 scenario (cần docker)
"""

import requests
import time
import json
import os
import statistics
import subprocess
from datetime import datetime, timezone

# ── Config ───────────────────────────────────────────────────────────────────
ORDER_URL       = "http://localhost:8081"
PAYMENT_URL     = "http://localhost:8082"
PLATFORM_URL    = "http://localhost:8084"
INVENTORY_URL   = "http://localhost:8083"

RESULTS_DIR     = "results"
N_RUNS          = 5
WARMUP_RUNS     = 3
POLL_TIMEOUT    = 90
POLL_INTERVAL   = 1
SAGA_MATCH_WAIT = 12

# Container names — từ docker-compose.yml
POSTGRES_CONTAINER  = "demo-postgres"
INVENTORY_CONTAINER = "inventory-service"
PAYMENT_CONTAINER   = "payment-service"
KAFKA_CONTAINER     = "demo-kafka"

# ── Helpers ───────────────────────────────────────────────────────────────────

def health_check() -> bool:
    ok = True
    for name, url in [
        ("order-service",       f"{ORDER_URL}/actuator/health"),
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
    """Reset inventory qua docker exec psql."""
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
    """POST /api/orders → (response_json, latency_ms)"""
    start = time.perf_counter()
    try:
        r = requests.post(f"{ORDER_URL}/api/orders", json=payload, timeout=15)
        latency = (time.perf_counter() - start) * 1000
        if r.status_code == 200:
            return r.json(), round(latency, 2)
        return None, round(latency, 2)
    except Exception:
        latency = (time.perf_counter() - start) * 1000
        return None, round(latency, 2)


def find_saga_after(created_after_epoch: float) -> dict | None:
    """
    Tìm saga tạo SAU thời điểm created_after_epoch.
    Tránh lấy nhầm saga cũ.
    """
    cutoff = datetime.fromtimestamp(created_after_epoch, tz=timezone.utc).isoformat()
    deadline = time.time() + SAGA_MATCH_WAIT

    while time.time() < deadline:
        try:
            r = requests.get(f"{PLATFORM_URL}/api/sagas?size=10", timeout=5)
            if r.status_code == 200:
                sagas = r.json().get("content", [])
                for saga in sagas:
                    start_time = saga.get("startTime", "")
                    if start_time and start_time > cutoff:
                        return saga
        except Exception:
            pass
        time.sleep(0.5)
    return None


def poll_saga_terminal(saga_id: str, timeout: int = None) -> tuple:
    """Poll cho đến khi saga đạt terminal status."""
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
    return None, time.time() - start


def trigger_rca(saga_id: str) -> tuple:
    """Trigger RCA → (result_json, latency_ms)"""
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
        subprocess.run(["docker", "stop", container],
                       capture_output=True, timeout=15)
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


def reset_kafka_consumer_offset(group: str, topic: str):
    """Reset consumer group offset về earliest để reprocess messages."""
    # apache/kafka:3.7.0 → tools in /opt/kafka/bin/
    cmd = [
        "docker", "exec", KAFKA_CONTAINER,
        "/opt/kafka/bin/kafka-consumer-groups.sh",
        "--bootstrap-server", "localhost:9092",
        "--group", group,
        "--topic", topic,
        "--reset-offsets", "--to-earliest", "--execute"
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        if "TOPIC" in result.stdout or "SUCCESS" in result.stdout.upper():
            print(f"  ✅ Offset reset: {group}/{topic}")
            return True
        else:
            print(f"  ⚠️  Offset reset output: {result.stdout.strip()[:100]}")
    except Exception as e:
        print(f"  ⚠️  Cannot reset offset: {e}")
    return False


def wait_service_ready(url: str, timeout: int = 30):
    """Chờ service healthy sau restart."""
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


def make_result(scenario: dict, run_index: int) -> dict:
    return {
        "scenario_id":       scenario["id"],
        "run_index":         run_index + 1,
        "timestamp":         datetime.now(timezone.utc).isoformat(),
        "saga_id":           None,
        "saga_status":       None,
        "saga_duration_ms":  None,
        "rca_root_cause":    None,
        "rca_severity":      None,
        "rca_latency_ms":    None,
        "rca_accurate":      False,
        "time_to_insight_s": None,
        "order_latency_ms":  None,
        "error":             None,
    }


def finalize_rca(result: dict, rca, rca_ms, t_start, scenario):
    """Gắn kết quả RCA vào result dict."""
    result["rca_latency_ms"] = rca_ms
    result["time_to_insight_s"] = round(time.time() - t_start, 3)
    if rca:
        result["rca_root_cause"] = rca.get("rootCause")
        result["rca_severity"] = rca.get("severity")
        result["rca_accurate"] = (result["rca_root_cause"] == scenario["expected_rca"])
    icon = "✅" if result["rca_accurate"] else "❌"
    print(f" → {result.get('saga_status', '?')} | RCA={result['rca_root_cause']} {icon}")


# ── Scenario runners ─────────────────────────────────────────────────────────

def run_basic_scenario(scenario: dict, run_index: int) -> dict:
    """HEALTHY or COMPENSATION_RECOVERED — straightforward."""
    result = make_result(scenario, run_index)
    t_start = time.time()
    t_before_order = time.time()

    order_resp, latency = create_order(scenario["request"])
    result["order_latency_ms"] = latency
    if order_resp is None:
        result["error"] = "create_order failed"
        return result
    print(f"order={order_resp.get('id','?')[:8]}.. {latency:.0f}ms", end="")

    saga_info = find_saga_after(t_before_order)
    if saga_info is None:
        result["error"] = "saga not found"
        print(" → not found"); return result

    saga_id = saga_info["sagaId"]
    result["saga_id"] = saga_id

    detail, _ = poll_saga_terminal(saga_id)
    if detail is None:
        result["error"] = f"timeout {POLL_TIMEOUT}s"
        print(" → TIMEOUT"); return result

    result["saga_status"] = detail.get("status")
    result["saga_duration_ms"] = detail.get("durationMs")

    rca, rca_ms = trigger_rca(saga_id)
    finalize_rca(result, rca, rca_ms, t_start, scenario)
    return result


def run_stuck_saga(scenario: dict, run_index: int) -> dict:
    """STUCK_SAGA: stop inventory → order → wait 70s → RCA."""
    result = make_result(scenario, run_index)
    t_start = time.time()

    docker_stop(INVENTORY_CONTAINER)
    time.sleep(2)

    t_before_order = time.time()
    order_resp, latency = create_order(scenario["request"])
    result["order_latency_ms"] = latency
    if order_resp is None:
        docker_start(INVENTORY_CONTAINER)
        result["error"] = "create_order failed"; return result

    print(f"order={order_resp.get('id','?')[:8]}.. {latency:.0f}ms", end="")

    # Tìm saga NGAY sau create (trước khi chờ lâu)
    saga_info = find_saga_after(t_before_order)
    if saga_info is None:
        docker_start(INVENTORY_CONTAINER)
        result["error"] = "saga not found"
        print(" → not found"); return result

    saga_id = saga_info["sagaId"]
    result["saga_id"] = saga_id

    # Chờ stuck detection (60s timeout + max 30s scheduler)
    print(f" waiting 70s...", end="", flush=True)
    time.sleep(70)

    result["saga_status"] = "IN_PROGRESS"

    rca, rca_ms = trigger_rca(saga_id)
    finalize_rca(result, rca, rca_ms, t_start, scenario)

    docker_start(INVENTORY_CONTAINER)
    print(f"  ▶️  Restarting inventory...", end="")
    wait_service_ready(f"{INVENTORY_URL}/actuator/health", timeout=40)
    print(" ready")
    time.sleep(5)
    return result


def run_recovered_from_stuck(scenario: dict, run_index: int) -> dict:
    """RECOVERED_FROM_STUCK: stop → 70s → restart → saga auto-complete → RCA."""
    result = make_result(scenario, run_index)
    t_start = time.time()

    docker_stop(INVENTORY_CONTAINER)
    time.sleep(2)

    t_before_order = time.time()
    order_resp, latency = create_order(scenario["request"])
    result["order_latency_ms"] = latency
    if order_resp is None:
        docker_start(INVENTORY_CONTAINER)
        result["error"] = "create_order failed"; return result

    print(f"order={order_resp.get('id','?')[:8]}.. {latency:.0f}ms", end="")

    # Tìm saga NGAY
    saga_info = find_saga_after(t_before_order)
    if saga_info is None:
        docker_start(INVENTORY_CONTAINER)
        result["error"] = "saga not found"
        print(" → not found"); return result

    saga_id = saga_info["sagaId"]
    result["saga_id"] = saga_id

    # Chờ stuck detection
    print(f" waiting 70s...", end="", flush=True)
    time.sleep(70)

    # Restart inventory — nhưng cần stop trước để reset offset
    # Step 1: Reset consumer offset TRƯỚC khi start
    # (consumer phải stopped để reset offset)
    reset_kafka_consumer_offset("inventory-service-group", "payment-completed")
    time.sleep(1)

    # Step 2: Start inventory
    docker_start(INVENTORY_CONTAINER)
    print(f" waiting for service...", end="", flush=True)
    wait_service_ready(f"{INVENTORY_URL}/actuator/health", timeout=40)
    print(" ready", end="")

    # Step 3: Chờ consumer reconnect và reprocess message
    time.sleep(10)
    detail, _ = poll_saga_terminal(saga_id, timeout=30)
    if detail:
        result["saga_status"] = detail.get("status")
        result["saga_duration_ms"] = detail.get("durationMs")
    else:
        result["saga_status"] = "STUCK_AFTER_RESTART"

    rca, rca_ms = trigger_rca(saga_id)
    finalize_rca(result, rca, rca_ms, t_start, scenario)
    time.sleep(5)
    return result


def run_compensation_failed(scenario: dict, run_index: int) -> dict:
    """COMPENSATION_FAILED: qty=9999 → tìm saga → chờ 3s → stop payment → 70s → RCA."""
    result = make_result(scenario, run_index)
    t_start = time.time()

    t_before_order = time.time()
    order_resp, latency = create_order(scenario["request"])
    result["order_latency_ms"] = latency
    if order_resp is None:
        result["error"] = "create_order failed"; return result

    print(f"order={order_resp.get('id','?')[:8]}.. {latency:.0f}ms", end="")

    # Tìm saga TRƯỚC KHI stop payment
    saga_info = find_saga_after(t_before_order)
    if saga_info is None:
        result["error"] = "saga not found"
        print(" → not found"); return result

    saga_id = saga_info["sagaId"]
    result["saga_id"] = saga_id

    # Chờ compensation bắt đầu (INVENTORY_FAILED → REFUND_PAYMENT)
    time.sleep(3)

    # Stop payment TRƯỚC KHI refund kịp xử lý
    docker_stop(PAYMENT_CONTAINER)
    print(f" payment stopped, waiting 70s...", end="", flush=True)

    time.sleep(70)

    result["saga_status"] = "COMPENSATING"

    rca, rca_ms = trigger_rca(saga_id)
    finalize_rca(result, rca, rca_ms, t_start, scenario)

    docker_start(PAYMENT_CONTAINER)
    print(f"  ▶️  Restarting payment...", end="")
    wait_service_ready(f"{PAYMENT_URL}/actuator/health", timeout=40)
    print(" ready")
    time.sleep(5)
    return result


def run_circuit_breaker(scenario: dict, run_index: int) -> dict:
    """CIRCUIT_BREAKER_OPEN: fail-mode → order → tìm saga → wait retry → RCA."""
    result = make_result(scenario, run_index)
    t_start = time.time()

    set_payment_fail_mode(True)
    time.sleep(1)

    t_before_order = time.time()
    order_resp, latency = create_order(scenario["request"])
    result["order_latency_ms"] = latency
    if order_resp is None:
        set_payment_fail_mode(False)
        result["error"] = "create_order failed"; return result

    print(f"order={order_resp.get('id','?')[:8]}.. {latency:.0f}ms", end="")

    # Tìm saga NGAY sau create, trước khi chờ retry
    saga_info = find_saga_after(t_before_order)
    if saga_info is None:
        set_payment_fail_mode(False)
        result["error"] = "saga not found"
        print(" → not found"); return result

    saga_id = saga_info["sagaId"]
    result["saga_id"] = saga_id

    # Chờ Resilience4j retry exhaustion (3 retries × exponential backoff)
    print(f" waiting 15s for retry...", end="", flush=True)
    time.sleep(15)

    detail, _ = poll_saga_terminal(saga_id, timeout=15)
    if detail:
        result["saga_status"] = detail.get("status")

    rca, rca_ms = trigger_rca(saga_id)
    finalize_rca(result, rca, rca_ms, t_start, scenario)

    set_payment_fail_mode(False)
    time.sleep(2)
    return result


# ── Scenario definitions ─────────────────────────────────────────────────────

BASIC_SCENARIOS = [
    {
        "id": "SC01_HEALTHY",
        "description": "Normal order — all steps succeed",
        "request": {"customerId": "eval-01", "productId": "PRODUCT-1",
                    "quantity": 1, "amount": 100.00},
        "expected_saga_status": "COMPLETED",
        "expected_rca": "HEALTHY",
        "runner": run_basic_scenario,
    },
    {
        "id": "SC02_COMPENSATION_RECOVERED",
        "description": "Inventory fail — compensation succeeds",
        "request": {"customerId": "eval-02", "productId": "PRODUCT-1",
                    "quantity": 9999, "amount": 999900.00},
        "expected_saga_status": "COMPENSATED",
        "expected_rca": "COMPENSATION_RECOVERED",
        "runner": run_basic_scenario,
    },
]

ADVANCED_SCENARIOS = [
    {
        "id": "SC03_STUCK_SAGA",
        "description": "Inventory down — saga stuck (HIGH)",
        "request": {"customerId": "eval-03", "productId": "PRODUCT-1",
                    "quantity": 1, "amount": 50.00},
        "expected_saga_status": "IN_PROGRESS",
        "expected_rca": "STUCK_SAGA",
        "runner": run_stuck_saga,
        "n_runs": 2,
    },
    {
        "id": "SC04_RECOVERED_FROM_STUCK",
        "description": "Inventory down → restart → self-healing (LOW)",
        "request": {"customerId": "eval-04", "productId": "PRODUCT-1",
                    "quantity": 1, "amount": 60.00},
        "expected_saga_status": "COMPLETED",
        "expected_rca": "RECOVERED_FROM_STUCK",
        "runner": run_recovered_from_stuck,
        "n_runs": 2,
    },
    {
        "id": "SC05_COMPENSATION_FAILED",
        "description": "Compensation stuck — payment down during refund (HIGH)",
        "request": {"customerId": "eval-05", "productId": "PRODUCT-1",
                    "quantity": 9999, "amount": 100.00},
        "expected_saga_status": "COMPENSATING",
        "expected_rca": "COMPENSATION_FAILED",
        "runner": run_compensation_failed,
        "n_runs": 2,
    },
    {
        "id": "SC06_CIRCUIT_BREAKER",
        "description": "Payment failure — circuit breaker triggered (CRITICAL)",
        "request": {"customerId": "eval-06", "productId": "PRODUCT-1",
                    "quantity": 1, "amount": 75.00},
        "expected_saga_status": "FAILED",
        "expected_rca": "CIRCUIT_BREAKER_OPEN",
        "runner": run_circuit_breaker,
        "n_runs": 3,
    },
]


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Saga Platform Evaluation")
    parser.add_argument("--full", action="store_true",
                        help="Run all 6 scenarios (needs docker control)")
    parser.add_argument("--runs", type=int, default=N_RUNS,
                        help="Runs per basic scenario (default: 5)")
    args = parser.parse_args()

    os.makedirs(RESULTS_DIR, exist_ok=True)

    print("=" * 60)
    print("SAGA OBSERVABILITY PLATFORM — EVALUATION")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Mode: {'FULL (6 scenarios)' if args.full else 'BASIC (2 scenarios)'}")
    print("=" * 60)

    # 1. Health check
    print("\n[1] Health Check")
    if not health_check():
        print("\n❌ Services chưa chạy.")
        return

    # 2. Reset inventory
    print("\n[2] Inventory Reset")
    reset_inventory("PRODUCT-1", 1000)
    time.sleep(1)

    # 3. Warm-up
    print(f"\n[3] Warm-up ({WARMUP_RUNS} requests)")
    for i in range(WARMUP_RUNS):
        _, ms = create_order({"customerId": "warmup", "productId": "PRODUCT-1",
                              "quantity": 1, "amount": 10.0})
        print(f"  warm-up {i+1}: {ms:.0f}ms")
        time.sleep(1)

    # 4. Build scenario list
    scenarios = list(BASIC_SCENARIOS)
    if args.full:
        scenarios.extend(ADVANCED_SCENARIOS)

    # 5. Run
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

        if scenario.get("request", {}).get("quantity", 0) < 100:
            reset_inventory("PRODUCT-1", 1000)
            time.sleep(0.5)

        for i in range(n):
            print(f"  Run {i+1}/{n}: ", end="", flush=True)
            run_result = runner(scenario, i)
            all_runs.append(run_result)
            time.sleep(2)

    # 6. Summary
    print_summary(scenarios, all_runs, args)


def print_summary(scenarios, all_runs, args):
    print(f"\n{'='*60}")
    print("EVALUATION RESULTS")
    print(f"{'='*60}")

    report = {
        "metadata": {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "mode": "full" if args.full else "basic",
            "total_runs": len(all_runs),
        },
        "scenarios": [],
        "overall": {}
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
            "rca_accuracy_pct":   round(acc_pct, 1),
            "avg_tti_s":          round(avg_tti, 3) if avg_tti else None,
            "avg_rca_latency_ms": round(avg_rca, 1) if avg_rca else None,
        }
        report["scenarios"].append(summary)
        all_acc.extend(acc_list)
        all_tti.extend(tti_list)
        all_rca_ms.extend(rca_ms_list)

        print(f"\n{scenario['id']}:")
        print(f"  Accuracy    : {acc_pct:.1f}%  ({sum(acc_list)}/{len(acc_list)})")
        if avg_tti: print(f"  Avg TTI     : {avg_tti:.2f}s")
        if avg_rca: print(f"  RCA Latency : {avg_rca:.1f}ms")

    # Overall
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

    print(f"\n{'='*60}")
    print("OVERALL")
    print(f"  RCA Accuracy     : {oa:.1f}%  (target ≥90%)  "
          f"{'✅' if oa >= 90 else '❌'}")
    if ot:  print(f"  Avg TTI          : {ot:.2f}s")
    if or_: print(f"  RCA Latency Avg  : {or_:.1f}ms")
    if p95_rca: print(f"  RCA Latency P95  : {p95_rca:.1f}ms")
    print(f"  Total Scenarios  : {len(scenarios)}")
    print(f"  Total Runs       : {len(all_acc)}")

    # Save
    json_path = os.path.join(RESULTS_DIR, "evaluation_report.json")
    txt_path = os.path.join(RESULTS_DIR, "evaluation_report.txt")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("SAGA OBSERVABILITY PLATFORM — EVALUATION REPORT\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 60 + "\n\n")
        for s in report["scenarios"]:
            f.write(f"{s['scenario_id']}\n")
            f.write(f"  {s['description']}\n")
            f.write(f"  Expected: {s['expected_rca']}  |  "
                    f"Accuracy: {s['rca_accuracy_pct']}%\n")
            if s['avg_tti_s']:
                f.write(f"  TTI: {s['avg_tti_s']}s  |  "
                        f"RCA: {s['avg_rca_latency_ms']}ms\n")
            f.write("\n")
        f.write(f"OVERALL: Accuracy={report['overall']['rca_accuracy_pct']}%\n")

    print(f"\n📁 Saved: {json_path}")
    print(f"📁 Saved: {txt_path}")


if __name__ == "__main__":
    main()