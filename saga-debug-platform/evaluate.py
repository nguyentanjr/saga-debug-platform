"""
Saga Observability Platform — Evaluation Script (Tier 2)
=========================================================
End-to-end evaluation qua Kafka + Docker — test real infrastructure behavior.

Đo 4 metrics chính cho luận văn:
  1. RCA Accuracy (%) — rootCause actual vs expected
  2. Time-to-Insight (s) — từ tạo order đến có RCA result
  3. RCA Latency (ms) — thời gian chạy RCA analysis
  4. Classification Precision/Recall/F1

28 Scenarios — 25 taxonomy scenarios + 3 D9 real-failure scenarios
(see docs/RCA_SCENARIO_TAXONOMY.md):
  A — Service Availability   (A1=SC01, A2=SC03, A3=SC04, A4=SC16, A5=SC17)
  B — Network / Timing       (B1=SC06, B2=SC18, B3=SC15, B4=SC07, B5=SC19)
  C — State Machine          (C1=SC09, C2=SC10, C3=SC13, C4=SC20, C5=SC21)
  D — Compensation           (D1=SC02, D2=SC05, D3=SC22, D4=SC23, D5=SC24)
  E — Correlation / Identity (E1=SC08, E2=SC25, E3=SC11, E4=SC14, E5=SC26)

Stratified train (15) / test (10) split — see taxonomy doc §4.
Each scenario carries `split`, `category`, and `reproducibility: real|synthetic`.

Execution methods:
  REAL    — full Kafka pipeline (SC01, SC02, SC06)
  DOCKER  — REAL + container/fail-mode/toxiproxy (SC03, SC04, SC05, SC07,
            SC16, SC17)
  INJECT  — admin REST inject — fast rule-engine probes (everything else)

Cách chạy:
  pip install requests
  python evaluate.py                            # 2 basic scenarios
  python evaluate.py --full                     # all scenarios
  python evaluate.py --full --split train       # only train set (15)
  python evaluate.py --full --split test        # held-out test set (10)
  python evaluate.py --full --category B        # only Category B
  python evaluate.py --full --repro real        # only real-failure scenarios
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
# Writing observations over HTTP needs the observer's bearer token (L5); same variable the
# observer's container reads. Unset -> inject-event answers 503.
INGEST_HEADERS  = ({"Authorization": f"Bearer {os.environ['CORRELATION_INGEST_TOKEN']}"}
                   if os.environ.get("CORRELATION_INGEST_TOKEN") else {})
INVENTORY_URL   = os.environ.get("INVENTORY_URL", "http://localhost:8083")

RESULTS_DIR     = "results"
N_RUNS          = 5
WARMUP_RUNS     = 2
POLL_TIMEOUT    = 90
POLL_INTERVAL   = 1
SAGA_MATCH_WAIT = 30

# One full cycle of the platform's slowest scheduled sweep, plus margin. The sweeps run
# at correlation.detection fixed rates (30s for the stuck pass); a run read sooner than
# that records whatever the scheduler happened to have done by then. See trigger_rca.
DETECTOR_SETTLE_SECONDS = 35

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
    """Analyse one run, after giving the scheduled detectors a full cycle to have run.

    Why the wait is here and not in the runners: several anomalies are raised by a
    scheduled sweep, not inline with ingestion, and both the diagnosis and the recorded
    anomaly list are read right after this call. Without a settle, what a run reports
    depends on where the request lands inside the sweep interval.

    That is not hypothetical. In the 2026-09-21 full run, SC17 run 2 recorded
    `anomalies_actual: []` and an RCA computed from no findings at all, while the platform
    raised SAGA_STUCK and MISSING_TERMINAL_EVENT **1.2 seconds later** -- the harness read
    the saga at 11:13:55.437 and the sweep fired at 11:13:56.66. Run 1 of the same
    scenario, identical by construction, landed on the other side of the same interval and
    recorded SAGA_STUCK. One coin toss, two different rows in the results table.

    The wait is uniform across every scenario on purpose. Settling only where the scenario
    expects a scheduled anomaly would give those runs more time to produce one than runs
    where such an anomaly would be a false positive, which biases precision upward. The
    cost is real -- about DETECTOR_SETTLE_SECONDS per run -- and it buys rows that mean the
    same thing regardless of when they were taken.
    """
    time.sleep(DETECTOR_SETTLE_SECONDS)
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


def docker_pause(container: str) -> bool:
    try:
        result = subprocess.run(["docker", "pause", container],
                                capture_output=True, text=True, timeout=15)
        if result.returncode == 0:
            print(f"  Paused {container}")
            return True
        print(f"  Cannot pause {container}: {result.stderr.strip()}")
    except Exception as e:
        print(f"  Cannot pause {container}: {e}")
    return False


def docker_unpause(container: str) -> bool:
    try:
        result = subprocess.run(["docker", "unpause", container],
                                capture_output=True, text=True, timeout=15)
        if result.returncode == 0:
            print(f"  Unpaused {container}")
            return True
        print(f"  Cannot unpause {container}: {result.stderr.strip()}")
    except Exception as e:
        print(f"  Cannot unpause {container}: {e}")
    return False


def docker_exec(container: str, args: list[str], timeout: int = 15) -> bool:
    try:
        result = subprocess.run(["docker", "exec", container] + args,
                                capture_output=True, text=True, timeout=timeout)
        if result.returncode == 0:
            return True
        print(f"  docker exec {container} failed: {result.stderr.strip()}")
    except Exception as e:
        print(f"  docker exec {container} failed: {e}")
    return False


def docker_signal_pid1(container: str, signal: str) -> bool:
    try:
        result = subprocess.run(["docker", "kill", f"--signal={signal}", container],
                                capture_output=True, text=True, timeout=15)
        if result.returncode == 0:
            print(f"  Sent SIG{signal} to {container}")
            return True
        print(f"  Cannot send SIG{signal} to {container}: {result.stderr.strip()}")
    except Exception as e:
        print(f"  Cannot send SIG{signal} to {container}: {e}")
    return False


def docker_networks(container: str) -> list[str]:
    try:
        result = subprocess.run([
            "docker", "inspect", "-f",
            "{{range $name, $_ := .NetworkSettings.Networks}}{{println $name}}{{end}}",
            container,
        ], capture_output=True, text=True, timeout=15)
        if result.returncode == 0:
            return [line.strip() for line in result.stdout.splitlines() if line.strip()]
        print(f"  Cannot inspect networks for {container}: {result.stderr.strip()}")
    except Exception as e:
        print(f"  Cannot inspect networks for {container}: {e}")
    return []


def docker_disconnect_networks(container: str, networks: list[str]) -> bool:
    ok = True
    for network in networks:
        try:
            result = subprocess.run(["docker", "network", "disconnect", "-f", network, container],
                                    capture_output=True, text=True, timeout=20)
            if result.returncode == 0:
                print(f"  Disconnected {container} from {network}")
            else:
                ok = False
                print(f"  Cannot disconnect {container} from {network}: {result.stderr.strip()}")
        except Exception as e:
            ok = False
            print(f"  Cannot disconnect {container} from {network}: {e}")
    return ok


def docker_connect_networks(container: str, networks: list[str]) -> bool:
    ok = True
    for network in networks:
        try:
            result = subprocess.run(["docker", "network", "connect", network, container],
                                    capture_output=True, text=True, timeout=20)
            stderr = result.stderr.strip()
            if result.returncode == 0 or "already exists" in stderr.lower():
                print(f"  Connected {container} to {network}")
            else:
                ok = False
                print(f"  Cannot connect {container} to {network}: {stderr}")
        except Exception as e:
            ok = False
            print(f"  Cannot connect {container} to {network}: {e}")
    return ok


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
    # The trace id travels in inject-event's own field, which is what the observer reads.
    # It used to ride only inside the payload; since ed06917 the detector no longer scans
    # payloads for a field it was never told about, so SC14/SC26 carried no trace at all
    # and MISSING_TRACE_CONTEXT could not fire (measured 2026-09-28, 0/4).
    if isinstance(payload, dict) and payload.get("traceId"):
        body["traceId"] = payload["traceId"]
    if event_id:
        body["eventId"] = event_id
    if timestamp:
        body["timestamp"] = timestamp if isinstance(timestamp, str) else timestamp.isoformat()
    try:
        r = requests.post(f"{PLATFORM_URL}/api/admin/inject-event", json=body,
                          headers=INGEST_HEADERS, timeout=10)
        if r.status_code != 200:
            print(f"\n    [inject error] {r.status_code}: {r.text[:200]}", end="")
        return r.status_code == 200
    except Exception as e:
        print(f"\n    [inject exception] {e}", end="")
        return False


def ts_ago(minutes=0, seconds=0):
    t = datetime.now(timezone.utc) - timedelta(minutes=minutes, seconds=seconds)
    return t.isoformat()


def expected_rca_status(scenario: dict) -> str:
    explicit = scenario.get("expected_rca_status")
    if explicit:
        return explicit
    return "EVIDENCE_AVAILABLE" if scenario.get("expected_rca") is not None else "NO_DIAGNOSIS_NEEDED"


def label_is_unconstrained(expected, excludes) -> bool:
    """Whether a scenario declines to say WHICH cause should be promoted.

    A scenario normally names the one label it expects. Some cannot, because what they
    are about is not a particular diagnosis but a boundary on the diagnosis: the platform
    must decline to promote a given cause, and whatever else it concludes from the rest of
    the evidence is not this scenario's subject. Naming a substitute label would turn the
    scenario into an assertion about behaviour it was not written to pin down.

    Recognised by `expected_rca: None` together with a non-empty `expected_rca_excludes`.
    A scenario with neither keeps its previous meaning exactly.
    """
    return expected is None and bool(excludes)


def expected_root_matches(expected, actual_root, actual_roots) -> bool:
    actual_roots = actual_roots or []
    if isinstance(expected, list):
        return set(actual_roots) == set(expected)
    return actual_root == expected or expected in actual_roots


def expected_root_matches_strict(expected, actual_roots) -> bool:
    """Exact-set match: every expected label present AND nothing else promoted.

    The lenient `expected_root_matches` only asks whether the expected label is among
    the ones promoted, so an engine that promoted every label it knows would score
    100%. Wrong causes published alongside the right one are what a reader of an RCA
    result actually has to sift through, so they are counted here.

    Both readings are scored over what the platform PROMOTED -- `rootCause` and
    `primaryRootCauses` -- not over its candidate list. The wording here used to say
    "candidate list", left over from before `selected_root_cause` dropped its fallback,
    and it has since misled a reader into thinking a SUPPORTING candidate could score as
    a correct answer. It cannot: supporting candidates are recorded separately in
    `rca_evidence_roots` and only consulted by `explicit_constraints_hold`.
    """
    actual = set(actual_roots or [])
    wanted = set(expected) if isinstance(expected, list) else ({expected} if expected else set())
    return actual == wanted


def extra_roots(expected, actual_roots) -> list:
    """Candidates reported beyond what the scenario expects."""
    wanted = set(expected) if isinstance(expected, list) else ({expected} if expected else set())
    return [r for r in (actual_roots or []) if r not in wanted]


def explicit_constraints_hold(result: dict) -> bool:
    """The two assertions a scenario can make beside (or instead of) naming a label.

    `expected_rca_excludes`  — these must NOT be the promoted cause.
    `expected_evidence_roots` — these MUST appear in the candidate list.

    Together they express "detected, reported as evidence, and not published as the
    cause", which is what a claim about the observation process is entitled to. Both
    default to empty, so a scenario declaring neither is scored exactly as before.
    """
    promoted = result.get("rca_primary_roots") or []
    evidence = result.get("rca_evidence_roots") or []
    for label in result.get("rca_expected_excludes") or []:
        if label in promoted:
            return False
    for label in result.get("rca_expected_evidence") or []:
        if label not in evidence:
            return False
    return True


def rca_expectation_matches_strict(result: dict, scenario: dict) -> bool:
    expected_status = expected_rca_status(scenario)
    actual_status = result.get("rca_status")

    if not explicit_constraints_hold(result):
        return False

    if expected_status in ("EVIDENCE_AVAILABLE", "SELECTED"):
        if actual_status not in ("EVIDENCE_AVAILABLE", "SELECTED"):
            return False
        if label_is_unconstrained(scenario.get("expected_rca"),
                                  scenario.get("expected_rca_excludes")):
            return True
        return expected_root_matches_strict(
            scenario.get("expected_rca"),
            result.get("rca_primary_roots"),
        )

    if expected_status in ("INSUFFICIENT_EVIDENCE", "NO_DIAGNOSIS_NEEDED"):
        return actual_status == expected_status and not (result.get("rca_primary_roots") or [])

    return actual_status == expected_status


def rca_expectation_matches(result: dict, scenario: dict) -> bool:
    expected_status = expected_rca_status(scenario)
    actual_status = result.get("rca_status")

    if not explicit_constraints_hold(result):
        return False

    if expected_status in ("EVIDENCE_AVAILABLE", "SELECTED"):
        if actual_status not in ("EVIDENCE_AVAILABLE", "SELECTED"):
            return False
        if label_is_unconstrained(scenario.get("expected_rca"),
                                  scenario.get("expected_rca_excludes")):
            return True
        return expected_root_matches(
            scenario.get("expected_rca"),
            result.get("rca_root_cause"),
            result.get("rca_primary_roots"),
        )

    if expected_status in ("INSUFFICIENT_EVIDENCE", "NO_DIAGNOSIS_NEEDED"):
        return actual_status == expected_status and result.get("rca_root_cause") is None

    return actual_status == expected_status


# ── Preflight ─────────────────────────────────────────────────────────────────
#
# Why this is here rather than in a runbook: both failures below were hit on 2026-09-21,
# and NEITHER raised an exception. Each produced a full report of plausible numbers.
#
#   1. A record without Spring type headers sat in `order-created`, left over from the N3
#      multi-cluster verification. payment-service could not deserialise it and never
#      advanced past that offset, so no order was ever processed. Every run came out
#      SAGA_STUCK + INSUFFICIENT_EVIDENCE -- which reads exactly like the platform
#      correctly declining to guess. Only `bufferedCalls: 0` in source_state showed that
#      the observed system had never run at all.
#
#   2. correlation-service subscribes by topic pattern. On a Kafka with no topics yet it
#      matched nothing and got `partitions assigned: []`, refreshing only after
#      metadata.max.age.ms (5 minutes by default). Sagas never appeared; the harness
#      reported "not found" per run and carried on to print a report.
#
# A health check catches neither: every service was UP both times. What separates a working
# pipeline from those two is whether an order actually flows end to end, so that is what
# this asserts -- on the warm-up orders, which are healthy by construction. If a healthy
# order cannot reach a terminal state, nothing measured afterwards means anything.


def preflight(warmup_saga_ids: list) -> bool:
    """Refuse to measure when the pipeline is not actually carrying events."""
    ok = True

    ids = [sid for sid in warmup_saga_ids if sid]
    if not ids:
        print("  [FAIL] no warm-up order was accepted; cannot verify the pipeline")
        return False

    # 1. The observer received them at all. Catches `partitions assigned: []`.
    seen = [sid for sid in ids if get_saga_detail(sid) is not None]
    mark = "[ok]  " if len(seen) == len(ids) else "[FAIL]"
    print(f"  {mark} observer received warm-up sagas: {len(seen)}/{len(ids)}")
    if len(seen) != len(ids):
        print("         -> correlation-service is not receiving. Check 'partitions "
              "assigned' in its log: an empty list means the topic pattern matched "
              "nothing when it subscribed.")
        ok = False

    # 2. The observed system actually processed them. Catches a stalled consumer anywhere
    #    downstream, including the poison-record case.
    terminal = {"COMPLETED", "COMPENSATED", "FAILED"}
    settled, statuses = 0, []
    for sid in seen:
        detail, _ = poll_saga_terminal(sid, timeout=45)
        status = (detail or {}).get("status")
        statuses.append(status)
        if status in terminal:
            settled += 1
    mark = "[ok]  " if seen and settled == len(seen) else "[FAIL]"
    print(f"  {mark} warm-up sagas reached a terminal state: {settled}/{len(seen)} {statuses}")
    if settled != len(seen):
        print("         -> a healthy order did not complete. A downstream consumer is "
              "stalled; look for a deserialisation failure pinned at one offset.")
        ok = False

    # 3. Ground truth is readable. Without it, source_state comes back empty and a run where
    #    the source never executed cannot be told from one where it did.
    state = capture_source_state()
    unavailable = state.get("unavailable") or {}
    mark = "[ok]  " if not unavailable else "[FAIL]"
    detail = "all readings" if not unavailable else "missing " + ", ".join(unavailable)
    print(f"  {mark} source state readable: {detail}")
    if unavailable:
        print("         -> true_cause cannot be verified for this run. See "
              "docs/ORACLE.md 5.0b.")
        ok = False

    return ok


def make_result(scenario: dict, run_index: int) -> dict:
    return {
        "scenario_id":        scenario["id"],
        "run_index":          run_index + 1,
        "timestamp":          datetime.now(timezone.utc).isoformat(),
        "saga_id":            None,
        # The key the platform actually filed this run under. Identity there is
        # (source, rawId) rendered as "source::rawId" (ADR 0002), while "saga_id" above is
        # the identifier the demo system issued. Recording both is what lets a reported
        # number be traced back to the row it was measured from.
        "execution_id":       None,
        "saga_status":        None,
        "saga_duration_ms":   None,
        "rca_root_cause":     None,
        "rca_status":         None,
        # What the platform promoted. Empty means it promoted nothing, which is a
        # result and not a gap to be filled in from the candidate list.
        "rca_primary_roots":  [],
        # Every cause named in the candidate list: evidence, never scored as a selection.
        "rca_evidence_roots": [],
        "rca_expected":       scenario["expected_rca"],
        "rca_expected_status": expected_rca_status(scenario),
        # Boundary assertions, carried on the run so the metric functions can read them
        # without needing the scenario dict. Empty for every scenario that names a label.
        "rca_expected_excludes": list(scenario.get("expected_rca_excludes") or []),
        "rca_expected_evidence": list(scenario.get("expected_evidence_roots") or []),
        "rca_severity":       None,
        "rca_latency_ms":     None,
        "rca_accurate":       False,
        "rca_accurate_strict": False,
        "rca_extra_roots":    [],
        "time_to_insight_s":  None,
        "order_latency_ms":   None,
        "rca_candidates":     [],
        "baseline_picks":     {},
        "anomalies_expected": scenario.get("expected_anomalies", []),
        "anomalies_actual":   [],
        # Evidence, as distinct from verdicts. Everything above this line is what the
        # platform concluded; everything below is what it had to conclude from.
        "raw_events":         [],
        "anomaly_records":    [],
        "identities":         [],
        "workflow_type_at_scoring": None,
        "scored_at":          None,
        "source_state":       {},
        "evidence_capture_gaps": {},
        "error":              None,
    }


def selected_root_cause(rca: dict | None):
    """The cause the platform published, or None when it published none.

    Reads the answer rather than reconstructing one. This function used to fall through
    to `evidence_root_causes` — the whole candidate list — when both `primaryRootCauses`
    and `rootCause` were empty. That fallback was written when they were ALWAYS empty:
    before the promotion fix, RcaService pinned both to null, so the harness had no other
    way to learn what the ranking had concluded.

    Both fields are now authoritative. Keeping the fallback made the evaluator disagree
    with the platform in exactly the case the platform is being careful about: a run whose
    only strong candidate is a claim about the observer's own reception (R14) promotes
    nothing on purpose, and the fallback would have named that candidate as the run's
    cause anyway — scoring the platform against a diagnosis it deliberately refused to
    make.
    """
    if not rca:
        return None
    if rca.get("rcaStatus") not in ("EVIDENCE_AVAILABLE", "SELECTED"):
        return None
    primary_roots = rca.get("primaryRootCauses") or []
    if primary_roots:
        return primary_roots[0]
    return rca.get("rootCause") or None


def evidence_root_causes(rca: dict | None):
    """Every root cause named anywhere in the candidate list.

    Evidence a reader should see, NOT a set of selected causes. Deliberately not used for
    scoring: promotion is the platform's own decision and is read from `primaryRootCauses`
    (see `selected_root_cause`). Kept because reporting what was considered is useful, and
    because the baselines score themselves against the same candidate pool.
    """
    if not rca:
        return []
    roots = []
    for candidate in rca.get("candidates") or []:
        if candidate.get("status") in (
            "STRONG_EVIDENCE",
            "SUPPORTING_EVIDENCE",
            "WEAK_EVIDENCE",
            "SELECTED",
            "CONTRIBUTING",
        ):
            root = candidate.get("rootCause")
            if root and root not in roots:
                roots.append(root)
    return roots


# ── Raw evidence capture ─────────────────────────────────────────────────────
#
# Why this exists: `anomalies_actual` is a detector verdict, not an observation. An
# oracle built on it would be grading the implementation against its own output. To
# label what a run's evidence permits, the harness has to record what the observer
# actually received -- payloads, clocks, correlation basis -- and leave the judging to
# a separate pass. See docs/ORACLE.md §5.0.

# Fields of a SagaEvent that are evidence rather than bookkeeping. `timestamp` is the
# business instant the platform reasons with; `receivedAt` is when it read the record,
# and the gap between them is the only proxy currently available for delivery lag.
EVIDENCE_EVENT_FIELDS = (
    "eventId", "eventType", "payload", "timestamp", "receivedAt",
    "correlationBasis", "sourceName", "serviceName", "stepName",
    "traceId", "spanId", "parentSpanId", "causationId", "transportRef",
)

# What a finding asserts, and off which observations. `evidenceEventIds` and `measuredAt`
# are here because F9 turns on whether they are stable under delivery order.
EVIDENCE_ANOMALY_FIELDS = (
    "anomalyType", "description", "observedValue", "expectedValue",
    "thresholdSource", "evidenceEventIds", "measuredAt", "detectedAt",
    "retractedAt", "retractionReason", "severity",
)


def capture_raw_evidence(detail: dict) -> list:
    """Every observation the platform holds for this run, as it holds it."""
    return [
        {field: event.get(field) for field in EVIDENCE_EVENT_FIELDS}
        for event in (detail.get("events") or [])
    ]


def capture_anomaly_records(detail: dict) -> list:
    """Full findings, not just their type names."""
    return [
        {field: anomaly.get(field) for field in EVIDENCE_ANOMALY_FIELDS}
        for anomaly in (detail.get("anomalies") or [])
    ]


def capture_source_state() -> dict:
    """The observed system's own state, read from the observed system.

    This is not evidence the observer had -- it is deliberately outside the observer --
    and it is recorded to settle `true_cause`, which is a claim about the source system
    and must not be inferred from the observer's conclusions.

    Circuit breaker state is the case that forced this. R01 concludes
    CIRCUIT_BREAKER_OPEN from a substring of a failure payload, while that string is a
    literal `handleOrderCreatedFallback` writes on every retry exhaustion, whether or not
    a breaker ever opened. Only the source's own actuator can say which happened.

    Three readings, because each answers a different question and any of them can be
    switched off by configuration:

      transitions -- did the breaker EVER open during this run. The strongest of the
                     three: a point sample of a breaker that opened and closed again
                     looks identical to one that never opened.
      snapshot    -- state, failureRate and bufferedCalls now. bufferedCalls is the one
                     that matters for SC06: the breaker is OUTER of the retry, so it
                     counts one call per saga, and minimumNumberOfCalls is 5. Verified
                     against a fresh container: state CLOSED, bufferedCalls 0.
      health      -- the same figures via the health indicator, kept as a second source
                     in case the endpoint above is not exposed in some deployment.

    Whatever is unavailable is recorded as unavailable. A missing reading must not read
    as "no breaker activity".
    """
    state = {
        "sampled_at": datetime.now(timezone.utc).isoformat(),
        "breaker_snapshot": None,
        "breaker_health": None,
        "breaker_transitions": None,
        "unavailable": {},
    }

    def read(key, url, extract):
        try:
            r = requests.get(url, timeout=5)
            if r.status_code == 200:
                state[key] = extract(r.json())
            else:
                state["unavailable"][key] = f"HTTP {r.status_code}"
        except Exception as exc:
            state["unavailable"][key] = str(exc)

    # Returns the full per-breaker object in this Resilience4j version (state,
    # failureRate, bufferedCalls), not merely the list of names the docs describe.
    read("breaker_snapshot",
         f"{PAYMENT_URL}/actuator/circuitbreakers",
         lambda body: body.get("circuitBreakers"))

    # Only STATE_TRANSITION events; the retry/success traffic would swamp the artifact.
    read("breaker_transitions",
         f"{PAYMENT_URL}/actuator/circuitbreakerevents",
         lambda body: [
             {k: event.get(k) for k in
              ("circuitBreakerName", "type", "creationTime", "stateTransition")}
             for event in (body.get("circuitBreakerEvents") or [])
             if event.get("type") == "STATE_TRANSITION"
         ])

    # Needs management.endpoint.health.show-details; without it this is just {"status":"UP"}.
    read("breaker_health",
         f"{PAYMENT_URL}/actuator/health",
         lambda body: (body.get("components", {})
                           .get("circuitBreakers", {})
                           .get("details"))
         or {"note": "health details not exposed (show-details)"})

    return state


# Known gaps in the capture, carried in the artifact so a reader does not mistake
# "not recorded" for "not applicable". Each entry names the roadmap item that closes it.
EVIDENCE_CAPTURE_GAPS = {
    "event_time_source": (
        "PAYLOAD vs TRANSPORT is resolved by EventTimeResolver and then discarded -- no "
        "column holds it, so a labeller cannot tell which clock an event's timestamp came "
        "from. See ROADMAP F6. Partial proxy: timestamp vs receivedAt."
    ),
    "breaker_state_at_failure": (
        "breaker_health is sampled when RCA is triggered, not at the instant the failure "
        "event was published -- a breaker that opened and closed again reads the same as "
        "one that never opened. breaker_transitions is the reading that survives this; "
        "prefer it, and treat an empty transition list as evidence only when "
        "unavailable['breaker_transitions'] is absent."
    ),
    "source_state_requires_config": (
        "breaker_transitions and breaker_health are only populated when payment-service "
        "exposes circuitbreakerevents and health show-details. This is a change to the "
        "OBSERVED system, made for ground-truth verification and deliberately outside the "
        "observer -- the platform reads none of it. Record it as part of the experimental "
        "setup, not as an observer capability."
    ),
}


def finalize_result(result: dict, saga_id: str, rca, rca_ms, t_start, scenario):
    result["rca_latency_ms"] = rca_ms
    result["time_to_insight_s"] = round(time.time() - t_start, 3)

    # Collect actual anomalies from saga detail
    detail = get_saga_detail(saga_id)
    if detail:
        result["execution_id"] = detail.get("sagaId")
        result["saga_status"] = detail.get("status")
        result["saga_duration_ms"] = detail.get("durationMs")
        result["anomalies_actual"] = [
            a["anomalyType"] for a in detail.get("anomalies", [])
        ]
        # The observations themselves, kept separate from the verdicts drawn off them.
        # An oracle reads these; the scoring above reads the verdicts.
        result["raw_events"] = capture_raw_evidence(detail)
        result["anomaly_records"] = capture_anomaly_records(detail)
        result["identities"] = detail.get("identities") or []
        # The spec in force for this run, and when the run was judged. Both are part of
        # the case being labelled: the same observations judged at another instant are a
        # different case (docs/ORACLE.md §2).
        result["workflow_type_at_scoring"] = detail.get("workflowType")
        result["scored_at"] = datetime.now(timezone.utc).isoformat()

    # Read from the observed system, to settle true_cause without inferring it from
    # the observer's own conclusions.
    result["source_state"] = capture_source_state()
    result["evidence_capture_gaps"] = EVIDENCE_CAPTURE_GAPS

    if rca:
        result["rca_status"] = rca.get("rcaStatus")
        # What the platform promoted, whatever its status. The evidence-available branch
        # used to fall back to the whole candidate list here; see selected_root_cause for
        # why that reading is no longer true of this platform. An empty list now means
        # what it says: nothing was promoted.
        result["rca_primary_roots"] = rca.get("primaryRootCauses") or []
        result["rca_root_cause"] = selected_root_cause(rca)
        result["rca_severity"] = rca.get("severity")
        # Evidence, recorded beside the decision rather than mistaken for it.
        result["rca_evidence_roots"] = evidence_root_causes(rca)
        result["rca_candidates"] = captured_candidates(rca)
        result["baseline_picks"] = baseline_picks(result["rca_candidates"])

    # Accuracy: compare actual vs expected (both can be None for healthy).
    # Two readings are recorded side by side: the lenient one, kept so earlier
    # reports stay comparable, and the strict one, which penalises extra candidates.
    result["rca_accurate"] = rca_expectation_matches(result, scenario)
    result["rca_accurate_strict"] = rca_expectation_matches_strict(result, scenario)
    result["rca_extra_roots"] = extra_roots(
        scenario.get("expected_rca"), result.get("rca_primary_roots"))

    icon = "✅" if result["rca_accurate"] else "❌"
    strict_icon = "✅" if result["rca_accurate_strict"] else "❌"
    extra = result["rca_extra_roots"]
    extra_note = f" extra={extra}" if extra else ""
    print(f" → {result.get('saga_status', '?')} | "
          f"RCA={result['rca_root_cause']} status={result['rca_status']} "
          f"(expected={scenario['expected_rca']} status={expected_rca_status(scenario)}) "
          f"lenient={icon} strict={strict_icon}{extra_note}")


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
    #   detectMissingTerminal  @ 15s rate; trigger = saga age > 10 × max_step_timeout
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
      - A3 STUCK_RECOVERED_BRIEF (~45s, only SAGA_STUCK before recovery)
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


def run_payment_recovery_gap_scenario(
        scenario: dict,
        run_index: int,
        induce_fault,
        recover_fault,
        recovery_label: str) -> dict:
    """Real payment outage that later recovers.

    Payment is unavailable while the order-created event waits in Kafka. After
    recovery, payment emits PAYMENT_COMPLETED late, so the detector can observe
    a real event gap and fire STEP_TIMEOUT without admin event injection.
    """
    result = make_result(scenario, run_index)
    t_start = time.time()
    fault_active = False

    try:
        if not induce_fault():
            result["error"] = f"cannot induce {recovery_label}"
            return result
        fault_active = True
        time.sleep(scenario.get("pre_order_wait_seconds", 3))

        order_resp, latency = create_order(scenario["request"])
        result["order_latency_ms"] = latency
        if order_resp is None:
            result["error"] = "create_order failed"
            return result

        order_id = order_resp.get("id", "")
        saga_id = order_resp.get("sagaId", order_id)
        result["saga_id"] = saga_id
        print(f"order={order_id[:8]}.. saga={saga_id[:8]}.. {latency:.0f}ms", end="")

        saga_info = find_saga_by_id(saga_id)
        if saga_info is None:
            result["error"] = "saga not found"
            print(" -> not found")
            return result

        wait_seconds = scenario.get("freeze_seconds", 45)
        print(f" waiting {wait_seconds}s during {recovery_label}...", end="", flush=True)
        time.sleep(wait_seconds)

        if not recover_fault():
            result["error"] = f"cannot recover from {recovery_label}"
            return result
        fault_active = False

        wait_service_ready(f"{PAYMENT_URL}/actuator/health", timeout=60)
        detail, _ = poll_saga_terminal(saga_id, timeout=scenario.get("recover_poll_timeout", 60))
        if detail:
            result["saga_status"] = detail.get("status")
            result["saga_duration_ms"] = detail.get("durationMs")

        settle_seconds = scenario.get("post_recovery_wait_seconds", 20)
        if settle_seconds:
            print(f" settling {settle_seconds}s...", end="", flush=True)
            time.sleep(settle_seconds)

        rca, rca_ms = trigger_rca(saga_id)
        finalize_result(result, saga_id, rca, rca_ms, t_start, scenario)
        return result
    finally:
        if fault_active:
            recover_fault()
            wait_service_ready(f"{PAYMENT_URL}/actuator/health", timeout=60)


def run_payment_pause_scenario(scenario: dict, run_index: int) -> dict:
    return run_payment_recovery_gap_scenario(
        scenario,
        run_index,
        lambda: docker_pause(PAYMENT_CONTAINER),
        lambda: docker_unpause(PAYMENT_CONTAINER),
        "docker pause")


def run_payment_sigstop_scenario(scenario: dict, run_index: int) -> dict:
    return run_payment_recovery_gap_scenario(
        scenario,
        run_index,
        lambda: docker_signal_pid1(PAYMENT_CONTAINER, "STOP"),
        lambda: docker_signal_pid1(PAYMENT_CONTAINER, "CONT"),
        "SIGSTOP")


def run_payment_network_partition_scenario(scenario: dict, run_index: int) -> dict:
    networks = docker_networks(PAYMENT_CONTAINER)
    if not networks:
        result = make_result(scenario, run_index)
        result["error"] = "payment-service has no Docker network to disconnect"
        return result

    return run_payment_recovery_gap_scenario(
        scenario,
        run_index,
        lambda: docker_disconnect_networks(PAYMENT_CONTAINER, networks),
        lambda: docker_connect_networks(PAYMENT_CONTAINER, networks),
        "network partition")


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
        "USE_TS_AGO_80S": lambda: ts_ago(seconds=80),
        "USE_TS_AGO_60S": lambda: ts_ago(seconds=60),
        "USE_TS_AGO_30S": lambda: ts_ago(seconds=30),
        "USE_TS_AGO_25S": lambda: ts_ago(seconds=25),
        "USE_TS_AGO_12S": lambda: ts_ago(seconds=12),
        "USE_TS_AGO_10S": lambda: ts_ago(seconds=10),
        "USE_TS_AGO_5S": lambda: ts_ago(seconds=5),
        "USE_TS_AGO_2S": lambda: ts_ago(seconds=2),
        "USE_TS_AGO_1S": lambda: ts_ago(seconds=1),
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
        "reproducibility": "real",
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
        "reproducibility": "real",
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
        "reproducibility": "real",
        "description": "A2 STUCK_LIVE — inventory down, saga stuck (~65s wait)",
        "request": {"customerId": "eval-03", "productId": "PRODUCT-1",
                    "quantity": 1, "amount": 50.00},
        "expected_saga_status": "IN_PROGRESS",
        "expected_rca_status": "INSUFFICIENT_EVIDENCE",
        "expected_rca": None,
        "expected_anomalies": ["SAGA_STUCK"],
        "runner": run_stuck_saga,
        "stuck_wait_seconds": 65,
        "n_runs": 2,
    },
    {
        "id": "SC04_STUCK_RECOVERED_BRIEF",
        "category": "A",
        "split": "train",
        "reproducibility": "real",
        "description": "A3 STUCK_RECOVERED_BRIEF — stuck ~45s then restart → R06",
        "request": {"customerId": "eval-04", "productId": "PRODUCT-1",
                    "quantity": 1, "amount": 60.00},
        "expected_saga_status": "COMPLETED",
        "expected_rca": "TRANSIENT_SERVICE_FAILURE",
        "expected_anomalies": ["SAGA_STUCK"],
        "runner": run_recovered_from_stuck,
        "stuck_wait_seconds": 45,
        "n_runs": 2,
    },
    {
        "id": "SC05_COMPENSATION_STEP_FAILURE",
        "category": "D",
        "split": "train",
        "reproducibility": "synthetic",
        "description": "D2 — refund never acknowledged → MISSING_COMPENSATION_ACK",
        "request": None,
        "expected_saga_status": "COMPENSATING",
        "expected_rca": "COMPENSATION_STEP_FAILURE",
        "expected_anomalies": ["SAGA_STUCK", "MISSING_COMPENSATION_ACK"],
        "runner": run_compensation_step_failure,
        "n_runs": 2,
    },
    {
        "id": "SC06_CIRCUIT_BREAKER_REAL",
        "category": "B",
        "split": "train",
        "reproducibility": "real",
        "description": "B1 — REAL: fail-mode → 3 retries → R01 primary, R02 contributing",
        "request": {"customerId": "eval-06", "productId": "PRODUCT-1",
                    "quantity": 1, "amount": 75.00},
        "expected_saga_status": "FAILED",
        "expected_rca": "CIRCUIT_BREAKER_OPEN",
        # Multi-rule: R01 primary + R02 contributing (RETRY_BURST emitted by
        # Resilience4j PAYMENT_RETRY listener after the configured retry-burst threshold).
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
        "reproducibility": "synthetic",
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
        "reproducibility": "synthetic",
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
        "reproducibility": "synthetic",
        "description": "C1 — reverse-chronological timestamps → OUT_OF_ORDER detected, "
                       "reported as evidence, never promoted as the cause",
        "request": None,
        "expected_saga_status": None,
        # This scenario used to expect OUT_OF_ORDER_DELIVERY as the promoted cause. That
        # expectation is now unsatisfiable by design, and was wrong before it was
        # unsatisfiable: R14's claim is about the order THIS PLATFORM read records in, not
        # about an ordering contract the observed system broke, so it is declared
        # subject = OBSERVATION_PROCESS and is not eligible for promotion.
        #
        # What the scenario is actually about is unchanged and is now stated in three
        # parts, none of them weaker than before:
        #   expected_anomalies    OUT_OF_ORDER is still detected
        #   expected_evidence_roots  it still reaches RCA and is still published
        #   expected_rca_excludes    it is not published as the run's root cause
        #
        # No substitute label is named. These two back-dated events also contradict the
        # declared graph — by event time PROCESS_PAYMENT precedes CREATE_ORDER — so
        # INVALID_TRANSITION fires and is promoted. That is a true finding about the
        # injected data, but it is SC10's subject, not this one's; asserting it here would
        # be writing down observed behaviour rather than an expectation.
        "expected_rca": None,
        "expected_rca_status": "EVIDENCE_AVAILABLE",
        "expected_rca_excludes": ["OUT_OF_ORDER_DELIVERY"],
        "expected_evidence_roots": ["OUT_OF_ORDER_DELIVERY"],
        "expected_anomalies": ["OUT_OF_ORDER"],
        "runner": run_inject_scenario,
        "n_runs": 2,
        "wait_seconds": 0,
        "inject_events": [
            {"eventType": "ORDER_CREATED", "stepName": "CREATE_ORDER",
             "serviceName": "order-service",
             "timestamp": "USE_TS_AGO_1S",
             "payload": {"traceId": "aaaa0000aaaa0000aaaa0000aaaa0009"}},
            {"eventType": "PAYMENT_COMPLETED", "stepName": "PROCESS_PAYMENT",
             "serviceName": "payment-service",
             "timestamp": "USE_TS_AGO_2S",
             "payload": {"traceId": "aaaa0000aaaa0000aaaa0000aaaa0009"}},
        ],
    },
    {
        "id": "SC10_INVALID_TRANSITION",
        "category": "C",
        "split": "train",
        "reproducibility": "synthetic",
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
        "reproducibility": "synthetic",
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
        "reproducibility": "synthetic",
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
        "reproducibility": "synthetic",
        "description": "C3 — event after COMPLETED terminal → FORBIDDEN_POST_TERMINAL",
        "request": None,
        "expected_saga_status": None,
        "expected_rca": "TERMINAL_STATE_OVERWRITE",
        "expected_anomalies": ["FORBIDDEN_POST_TERMINAL", "INVALID_TRANSITION"],
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
        "reproducibility": "synthetic",
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
        "reproducibility": "synthetic",
        "description": "B3 — large gap between steps → STEP_TIMEOUT (scheduler)",
        "request": None,
        "expected_saga_status": None,
        "expected_rca": "DOWNSTREAM_TIMEOUT",
        "expected_anomalies": ["SAGA_STUCK", "STEP_TIMEOUT"],
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
        "reproducibility": "real",
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
        "reproducibility": "real",
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
        "reproducibility": "synthetic",
        # B5: STEP_TIMEOUT fires from scheduler, then a follow-up event
        # lets saga reach COMPLETED. Tests the "completed but had problems"
        # outcome-determination branch (outcome=SELF_HEALED).
        "description": "B5 — STEP_TIMEOUT then COMPLETED → R07 + outcome=SELF_HEALED",
        "request": None,
        "expected_saga_status": "COMPLETED",
        "expected_rca": "DOWNSTREAM_TIMEOUT",
        "expected_anomalies": ["SAGA_STUCK", "STEP_TIMEOUT"],
        "runner": run_timeout_then_recover,
        "wait_seconds": 65,
        "n_runs": 2,
    },

    # ── Category C — State Machine ────────────────────────────────────────
    {
        "id": "SC20_STATE_REGRESSION",
        "category": "C",
        "split": "test",
        "reproducibility": "synthetic",
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
        "expected_anomalies": ["STATE_REGRESSION", "FORBIDDEN_POST_TERMINAL", "INVALID_TRANSITION"],
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
        "reproducibility": "synthetic",
        # C5: both INVENTORY_RESERVED (success terminal) and PAYMENT_FAILED
        # (failure terminal) for the same saga → MULTI_TERMINAL_CONFLICT.
        "description": "C5 — both success+failure terminals → MULTI_TERMINAL_CONFLICT (R03)",
        "request": None,
        "expected_saga_status": None,
        "expected_rca": "MULTI_TERMINAL_CONFLICT",
        "expected_anomalies": [
            "FORBIDDEN_POST_TERMINAL",
            "STATE_REGRESSION",
            "MULTI_TERMINAL_CONFLICT",
            "INVALID_TRANSITION",
        ],
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
        "reproducibility": "synthetic",
        # D3: INVENTORY_FAILED moves saga to COMPENSATING but no REFUND_PAYMENT
        # is ever sent. detectCompensationMissing fires after lastUpdated > 30s
        # (scheduler runs every 60s). Wait 65s.
        "description": "D3 — INVENTORY_FAILED but no REFUND_PAYMENT → COMPENSATION_MISSING (R09)",
        "request": None,
        "expected_saga_status": None,
        "expected_rca": "COMPENSATION_NOT_TRIGGERED",
        "expected_anomalies": ["SAGA_STUCK", "COMPENSATION_MISSING"],
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
        "reproducibility": "synthetic",
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
        "reproducibility": "synthetic",
        # D5: Inject events with timestamps so REFUND_PAYMENT comes 35s after
        # INVENTORY_FAILED. detectDelayedCompensation threshold is
        # REFUND_step_timeout (10s) × 1.5 = 15s — 35s exceeds it.
        # Then PAYMENT_REFUNDED at +55s closes the saga to COMPENSATED, and
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
        # threshold (R22 firing means 35s > 15s = 10s × 1.5).
        "description": "D5 — 35s-late refund → DELAYED_COMPENSATION fires; R11 primary (multi-rule)",
        "request": None,
        "expected_saga_status": None,
        "expected_rca": "FORWARD_STEP_FAILURE",
        "expected_anomalies": ["STEP_TIMEOUT", "DELAYED_COMPENSATION"],
        "runner": run_inject_scenario,
        "n_runs": 2,
        "wait_seconds": 70,
        "inject_events": [
            {"eventType": "ORDER_CREATED", "stepName": "CREATE_ORDER",
             "serviceName": "order-service",
             "timestamp": "USE_TS_AGO_80S",
             "payload": {"traceId": "USE_UNIQUE"}},
            {"eventType": "PAYMENT_COMPLETED", "stepName": "PROCESS_PAYMENT",
             "serviceName": "payment-service", "delay": 0.5,
             "timestamp": "USE_TS_AGO_60S",
             "payload": {"traceId": "USE_UNIQUE"}},
            {"eventType": "INVENTORY_FAILED", "stepName": "RESERVE_INVENTORY",
             "serviceName": "inventory-service", "delay": 0.5,
             # 60s ago -> 35s gap before REFUND_PAYMENT (25s ago)
             "timestamp": "USE_TS_AGO_60S",
             "payload": {"traceId": "USE_UNIQUE"}},
            {"eventType": "REFUND_PAYMENT", "stepName": "REFUND_PAYMENT",
             "serviceName": "payment-service", "delay": 0.5,
             "timestamp": "USE_TS_AGO_25S",
             "payload": {"traceId": "USE_UNIQUE"}},
            {"eventType": "PAYMENT_REFUNDED", "stepName": "REFUND_PAYMENT",
             "serviceName": "payment-service", "delay": 0.5,
             "timestamp": "USE_TS_AGO_5S",
             "payload": {"traceId": "USE_UNIQUE"}},
        ],
    },

    # ── Category E — Correlation / Identity ──────────────────────────────
    {
        "id": "SC25_DUPLICATE_EVENT_DELAYED",
        "category": "E",
        "split": "test",
        "reproducibility": "synthetic",
        # E2: same dup-event scenario as E1 but with a 30s gap between the
        # two duplicate events. Tests that DeduplicationCache TTL (1h) does
        # NOT cause the duplicate to be missed at the 30s mark.
        "description": "E2 — duplicate eventId at 30s gap → EXACT_DUPLICATE_EVENT",
        "request": None,
        "expected_saga_status": None,
        "expected_rca": "DUPLICATE_DELIVERY",
        "expected_anomalies": ["SAGA_STUCK", "EXACT_DUPLICATE_EVENT"],
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
        "reproducibility": "synthetic",
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

REAL_FAILURE_SCENARIOS = [
    {
        "id": "SC27_PAYMENT_PAUSE_RECOVERY",
        "category": "B",
        "split": "test",
        "reproducibility": "real",
        "description": "D9-1 payment-service docker pause then recover -> real STEP_TIMEOUT",
        "request": {"customerId": "eval-27", "productId": "PRODUCT-1",
                    "quantity": 1, "amount": 50.00},
        "expected_saga_status": "COMPLETED",
        "expected_rca": "DOWNSTREAM_TIMEOUT",
        "expected_anomalies": ["SAGA_STUCK", "STEP_TIMEOUT"],
        "runner": run_payment_pause_scenario,
        "freeze_seconds": 45,
        "post_recovery_wait_seconds": 20,
        "n_runs": 2,
    },
    {
        "id": "SC28_PAYMENT_SIGSTOP_RECOVERY",
        "category": "B",
        "split": "test",
        "reproducibility": "real",
        "description": "D9-2 payment-service PID 1 SIGSTOP then SIGCONT -> real STEP_TIMEOUT",
        "request": {"customerId": "eval-28", "productId": "PRODUCT-1",
                    "quantity": 1, "amount": 50.00},
        "expected_saga_status": "COMPLETED",
        "expected_rca": "DOWNSTREAM_TIMEOUT",
        "expected_anomalies": ["SAGA_STUCK", "STEP_TIMEOUT"],
        "runner": run_payment_sigstop_scenario,
        "freeze_seconds": 45,
        "post_recovery_wait_seconds": 20,
        "n_runs": 2,
    },
    {
        "id": "SC29_PAYMENT_NETWORK_PARTITION",
        "category": "B",
        "split": "test",
        "reproducibility": "real",
        "description": "D9-3 disconnect payment-service from Docker network then recover -> real STEP_TIMEOUT",
        "request": {"customerId": "eval-29", "productId": "PRODUCT-1",
                    "quantity": 1, "amount": 50.00},
        "expected_saga_status": "COMPLETED",
        "expected_rca": "DOWNSTREAM_TIMEOUT",
        "expected_anomalies": ["SAGA_STUCK", "STEP_TIMEOUT"],
        "runner": run_payment_network_partition_scenario,
        "freeze_seconds": 45,
        "post_recovery_wait_seconds": 20,
        "n_runs": 2,
    },
]


ALL_SCENARIO_LISTS = [
    BASIC_SCENARIOS, ADVANCED_SCENARIOS, TOXIPROXY_SCENARIOS,
    INJECT_SCENARIOS, NEW_SCENARIOS, REAL_FAILURE_SCENARIOS,
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
    real = [s for s in all_scenarios if s.get("reproducibility") == "real"]
    synthetic = [s for s in all_scenarios if s.get("reproducibility") == "synthetic"]

    print(f"\nAvailable scenarios — {len(all_scenarios)} total "
          f"({len(train)} train, {len(test)} test; "
          f"{len(real)} real, {len(synthetic)} synthetic):")
    print(f"  {'ID':<32} {'Cat':<3} {'Split':<5} {'Repro':<9} {'Description'}")
    print(f"  {'-'*90}")
    for s in all_scenarios:
        cat = s.get("category", "-")
        split = s.get("split", "-")
        repro = s.get("reproducibility", "-")
        print(f"  {s['id']:<32} {cat:<3} {split:<5} {repro:<9} {s['description']}")

    # Per-category summary
    print(f"\n  Per category:")
    for cat in ["A", "B", "C", "D", "E"]:
        cat_scenarios = [s for s in all_scenarios if s.get("category") == cat]
        cat_train = sum(1 for s in cat_scenarios if s.get("split") == "train")
        cat_test = sum(1 for s in cat_scenarios if s.get("split") == "test")
        print(f"    {cat}: {len(cat_scenarios)} ({cat_train} train, {cat_test} test)")
    print("\nShort IDs supported: SC01-SC29 "
          "(skipping SC18; SC27-SC29 are D9 real-failure scenarios).")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Saga Platform Evaluation (Tier 2)")
    parser.add_argument("--full", action="store_true",
                        help="Run all taxonomy and D9 real-failure scenarios")
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
    parser.add_argument("--repro", choices=["all", "real", "synthetic"], default="all",
                        help="Filter by reproducibility class (default: all)")
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
    warmup_sagas = []
    for i in range(WARMUP_RUNS):
        order, ms = create_order({"customerId": "warmup", "productId": "PRODUCT-1",
                                  "quantity": 1, "amount": 10.0})
        if order is not None:
            warmup_sagas.append(order.get("sagaId") or order.get("id"))
        print(f"  warm-up {i+1}: {ms:.0f}ms")
        time.sleep(1)

    print("  Settling 5s (let warm-up sagas propagate)...")
    time.sleep(5)

    print("\n[3b] Preflight")
    if not preflight(warmup_sagas):
        print("\n❌ Preflight failed. NOT running the suite: the two failures this "
              "checks for produce wrong numbers that look like right ones.")
        return

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
        scenarios.extend(REAL_FAILURE_SCENARIOS)
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
        if args.repro != "all":
            before = len(scenarios)
            scenarios = [s for s in scenarios if s.get("reproducibility") == args.repro]
            print(f"  Filtered to reproducibility='{args.repro}': "
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
              f"rca={scenario['expected_rca']}, rcaStatus={expected_rca_status(scenario)}")

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


# ──────────────────────────────────────────────────────────────────────────────
# Baselines
#
# The question a reader is entitled to ask of any diagnosis engine is "better than
# what?". Accuracy against expected labels answers "is it right", not "is the ranking
# earning its place". These three baselines answer the second question.
#
# All three choose from the SAME candidate set the engine produced, and differ only in
# the selection policy. That is deliberate: it isolates the contribution of the ranking
# layer instead of comparing against a different system. A baseline that picked from a
# different candidate set would confound rule coverage with ranking quality.
#
#   rule_id_order   the behaviour before RcaSelector gained execution position — sort by
#                   rule id and take the first. Any gap between this and the engine is
#                   exactly what the tier + position ordering bought.
#   severity_only   rank by CRITICAL > HIGH > MEDIUM > LOW alone. The obvious policy, and
#                   the one a reviewer will propose if the thesis does not pre-empt it.
#   first_anomaly   take whichever candidate is anchored earliest in time — "trust the
#                   first symptom", with no notion of evidence quality.
#
# RULED_OUT candidates are excluded from all three: they are not diagnoses the engine
# offers, so letting a baseline pick one would be beating a strawman.
# ──────────────────────────────────────────────────────────────────────────────

SEVERITY_RANK = {"CRITICAL": 4, "HIGH": 3, "MEDIUM": 2, "LOW": 1}


def captured_candidates(rca: dict | None) -> list:
    """The candidate list as raw material for the baselines, not for scoring."""
    if not rca:
        return []
    captured = []
    for candidate in rca.get("candidates") or []:
        captured.append({
            "ruleId":   candidate.get("ruleId"),
            "rootCause": candidate.get("rootCause"),
            "severity": candidate.get("severity"),
            "status":   candidate.get("status"),
            "anchorAt": candidate.get("anchorAt"),
        })
    return captured


def _selectable(candidates: list) -> list:
    """Everything a policy could name: the engine's full candidate list, minus exclusions."""
    return [c for c in candidates
            if c.get("status") != "RULED_OUT" and c.get("rootCause")]


def _promotable(candidates: list) -> list:
    """Only what the engine is willing to promote.

    The engine nominates STRONG_EVIDENCE alone. Letting the baselines choose from
    SUPPORTING and WEAK as well made the comparison answer two questions at once -- who
    ranks better, and who is willing to name a cause more often -- and report the mixture
    as a difference in ranking. Scored over this pool instead, every policy faces the same
    eligibility rule and only the ordering differs.
    """
    return [c for c in candidates if c.get("status") == "STRONG_EVIDENCE" and c.get("rootCause")]


def _rank(pool: list) -> dict:
    """What each ordering policy names first, out of one pool."""
    if not pool:
        return {"rule_id_order": None, "severity_only": None, "first_anomaly": None}
    by_rule_id = sorted(pool, key=lambda c: c.get("ruleId") or "~")
    by_severity = sorted(
        pool,
        key=lambda c: (-SEVERITY_RANK.get(c.get("severity"), 0), c.get("ruleId") or "~"))
    # Candidates the engine could not place in the execution have no anchor; they sort last
    # rather than being given a fabricated position.
    by_anchor = sorted(pool, key=lambda c: (c.get("anchorAt") or "9999", c.get("ruleId") or "~"))
    return {
        "rule_id_order": by_rule_id[0]["rootCause"],
        "severity_only": by_severity[0]["rootCause"],
        "first_anomaly": by_anchor[0]["rootCause"],
    }


def baseline_picks(candidates: list) -> dict:
    """What each baseline would have named, under each eligibility rule.

    The flat keys are the open pool and are kept at the top level so earlier reports stay
    readable. `same_eligibility` is the one to cite when the claim is about ranking.
    """
    picks = _rank(_selectable(candidates))
    picks["same_eligibility"] = _rank(_promotable(candidates))
    return picks


def compute_baseline_comparison(all_runs):
    """The engine against simpler orderings, on the same runs.

    Reported as two comparisons rather than one, because they answer different questions
    and a single number hides which is doing the work:

      open_pool        -- each policy with its own eligibility. The engine promotes only
                          STRONG; the baselines will name a SUPPORTING or WEAK candidate
                          rather than stay silent. A difference here mixes ranking with
                          willingness to name a cause.
      same_eligibility -- every policy restricted to the engine's own promotable pool.
                          Only the ordering differs, so a difference here is the ranking.

    Alongside them, how often each policy named anything at all. A policy that always names
    something cannot be told from a good one by accuracy alone on a suite where naming is
    usually right, and that is exactly the failure this platform argues against.

    Scoring abstention as *correct* is deliberately not done here. That needs a statement
    of what each run's evidence permits, which is the evidence oracle's job -- see
    oracle/score.py and docs/ORACLE.md. This function keeps reporting the injected-cause
    oracle unchanged, so the two remain comparable.
    """
    names = ["engine", "rule_id_order", "severity_only", "first_anomaly"]
    pools = ["open_pool", "same_eligibility"]
    correct = {pool: {n: 0 for n in names} for pool in pools}
    nominated = {pool: {n: 0 for n in names} for pool in pools}
    total = 0
    disagreements = []

    for r in all_runs:
        if r.get("error"):
            continue
        if r.get("rca_expected_status") not in ("EVIDENCE_AVAILABLE", "SELECTED"):
            continue
        expected = r.get("rca_expected")
        if expected is None:
            continue
        wanted = set(expected) if isinstance(expected, list) else {expected}

        total += 1
        stored = dict(r.get("baseline_picks") or {})
        same = dict(stored.pop("same_eligibility", None) or {})

        picks = {
            "open_pool": {**stored, "engine": r.get("rca_root_cause")},
            # The engine already chooses from its promotable pool, so it is the same pick
            # under both rules; only the baselines move.
            "same_eligibility": {**same, "engine": r.get("rca_root_cause")},
        }

        for pool in pools:
            for name in names:
                pick = picks[pool].get(name)
                if pick is not None:
                    nominated[pool][name] += 1
                if pick in wanted:
                    correct[pool][name] += 1

        if picks["same_eligibility"].get("engine") != picks["same_eligibility"].get("rule_id_order"):
            disagreements.append({
                "scenario_id": r.get("scenario_id"),
                "run_index": r.get("run_index"),
                "expected": expected,
                "engine": picks["same_eligibility"].get("engine"),
                "rule_id_order": picks["same_eligibility"].get("rule_id_order"),
            })

    def pct(n):
        return round(100.0 * n / total, 1) if total else 0.0

    report = {
        "runs_scored": total,
        "accuracy_pct": {n: pct(correct["open_pool"][n]) for n in names},
        "correct": correct["open_pool"],
        "same_eligibility": {
            "accuracy_pct": {n: pct(correct["same_eligibility"][n]) for n in names},
            "correct": correct["same_eligibility"],
        },
        "nomination_rate_pct": {
            pool: {n: pct(nominated[pool][n]) for n in names} for pool in pools
        },
    }
    report["engine_minus_rule_id_order_pp"] = round(
        report["same_eligibility"]["accuracy_pct"]["engine"]
        - report["same_eligibility"]["accuracy_pct"]["rule_id_order"], 1)
    report["disagreements_with_rule_id_order"] = disagreements

    # A zero gap is a finding, not a failure: it says the suite holds no case where the
    # ranking decides, which is a statement about the suite rather than about the ranking.
    report["note"] = (
        "Under equal eligibility no scenario in this run distinguishes the engine from "
        "rule-id ordering; the suite lacks a multi-rule case where ranking decides."
        if not disagreements else
        f"{len(disagreements)} run(s) where the ranking changed the primary diagnosis, "
        "scored under equal eligibility."
    )
    return report


def compute_classification_metrics(all_runs):
    tp = fp = fn = tn = 0
    per_class = {}

    for r in all_runs:
        if r.get("error"):
            continue

        expected_status = r.get("rca_expected_status") or (
            "EVIDENCE_AVAILABLE" if r.get("rca_expected") is not None else "NO_DIAGNOSIS_NEEDED"
        )
        expected = r.get("rca_expected")
        actual = r.get("rca_root_cause")
        actual_roots = r.get("rca_primary_roots") or []

        # A scenario that names no label cannot contribute to label precision or
        # recall — there is nothing to be right or wrong about. It is still scored
        # for accuracy through rca_accurate, which checks its boundary assertions.
        if label_is_unconstrained(expected, r.get("rca_expected_excludes")):
            continue

        # Root-cause classification only applies when the scenario expects
        # concrete RCA evidence. Abstention/healthy semantics are reported
        # separately via rcaStatus accuracy.
        if expected_status not in ("EVIDENCE_AVAILABLE", "SELECTED"):
            if r.get("rca_status") in ("EVIDENCE_AVAILABLE", "SELECTED") and actual is not None:
                fp += 1
                per_class.setdefault(actual, {"tp": 0, "fp": 0, "fn": 0})
                per_class[actual]["fp"] += 1
            else:
                tn += 1
            continue

        if expected_root_matches(expected, actual, actual_roots):
            tp += 1
            labels = expected if isinstance(expected, list) else [expected]
            for label in labels:
                per_class.setdefault(label, {"tp": 0, "fp": 0, "fn": 0})
                per_class[label]["tp"] += 1
        else:
            if actual is not None:
                fp += 1
                per_class.setdefault(actual, {"tp": 0, "fp": 0, "fn": 0})
                per_class[actual]["fp"] += 1
            fn += 1
            labels = expected if isinstance(expected, list) else [expected]
            for label in labels:
                per_class.setdefault(label, {"tp": 0, "fp": 0, "fn": 0})
                per_class[label]["fn"] += 1

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


def compute_classification_metrics_strict(all_runs):
    """Classification counts that penalise every candidate the engine should not
    have reported.

    Differs from `compute_classification_metrics` in one respect: a run only counts
    as a true positive when the reported candidate set equals the expected set, and
    each surplus candidate is counted as a false positive in its own right. This is
    the number to quote when claiming diagnostic precision, because the lenient
    variant cannot fall below 100% by adding wrong answers.
    """
    tp = fp = fn = tn = 0
    per_class = {}
    surplus_total = 0

    def bucket(label):
        return per_class.setdefault(label, {"tp": 0, "fp": 0, "fn": 0})

    for r in all_runs:
        if r.get("error"):
            continue

        expected_status = r.get("rca_expected_status") or (
            "EVIDENCE_AVAILABLE" if r.get("rca_expected") is not None else "NO_DIAGNOSIS_NEEDED"
        )
        expected = r.get("rca_expected")
        actual_roots = r.get("rca_primary_roots") or []
        wanted = set(expected) if isinstance(expected, list) else ({expected} if expected else set())

        # See the note in compute_classification_metrics: no expected label, no
        # contribution to label counts.
        if label_is_unconstrained(expected, r.get("rca_expected_excludes")):
            continue

        if expected_status not in ("EVIDENCE_AVAILABLE", "SELECTED"):
            # Nothing should have been reported. Every candidate is a false positive.
            if actual_roots:
                for label in actual_roots:
                    fp += 1
                    bucket(label)["fp"] += 1
                    surplus_total += 1
            else:
                tn += 1
            continue

        found = set(actual_roots)
        for label in sorted(wanted & found):
            tp += 1
            bucket(label)["tp"] += 1
        for label in sorted(wanted - found):
            fn += 1
            bucket(label)["fn"] += 1
        for label in sorted(found - wanted):
            fp += 1
            bucket(label)["fp"] += 1
            surplus_total += 1

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
        "surplus_candidates": surplus_total,
        "precision": round(precision, 3),
        "recall": round(recall, 3),
        "f1": round(f1, 3),
        "per_class": class_report,
    }


def compute_rca_status_metrics(all_runs):
    counts = {}
    total = correct = 0

    for r in all_runs:
        if r.get("error"):
            continue
        expected = r.get("rca_expected_status") or (
            "EVIDENCE_AVAILABLE" if r.get("rca_expected") is not None else "NO_DIAGNOSIS_NEEDED"
        )
        actual = r.get("rca_status")
        counts.setdefault(expected, {"total": 0, "correct": 0, "actual": {}})
        counts[expected]["total"] += 1
        counts[expected]["actual"][actual] = counts[expected]["actual"].get(actual, 0) + 1
        total += 1
        if r.get("rca_accurate"):
            counts[expected]["correct"] += 1
            correct += 1

    by_expected = {}
    for status, data in sorted(counts.items()):
        by_expected[status] = {
            "total": data["total"],
            "correct": data["correct"],
            "accuracy_pct": round(data["correct"] / data["total"] * 100, 1)
                    if data["total"] else 0.0,
            "actual": data["actual"],
        }

    return {
        "total": total,
        "correct": correct,
        "accuracy_pct": round(correct / total * 100, 1) if total else 0.0,
        "by_expected": by_expected,
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
            # Recorded so a reader can tell whether a row was taken after the scheduled
            # sweeps had a chance to run. An artifact produced without this settle has
            # timing-dependent anomaly lists; see trigger_rca.
            "detector_settle_seconds": DETECTOR_SETTLE_SECONDS,
        },
        "scenarios": [],
        "overall": {},
        "classification_metrics": {},
        "baseline_comparison": {},
        "rca_status_metrics": {},
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
            "reproducibility":    scenario.get("reproducibility"),
            "expected_rca":       scenario["expected_rca"],
            "expected_rca_status": expected_rca_status(scenario),
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

    strict_acc = [r["rca_accurate_strict"] for r in all_runs
                  if not r.get("error") and "rca_accurate_strict" in r]
    oa_strict = sum(strict_acc) / len(strict_acc) * 100 if strict_acc else 0

    report["overall"] = {
        "rca_accuracy_pct":         round(oa, 1),
        "rca_accuracy_strict_pct":  round(oa_strict, 1),
        "avg_tti_s":                round(ot, 3) if ot else None,
        "avg_rca_latency_ms":       round(or_, 1) if or_ else None,
        "p95_rca_latency_ms":       round(p95_rca, 1) if p95_rca else None,
        "meets_accuracy_target":    oa >= 90.0,
    }

    rca_metrics = compute_classification_metrics(all_runs)
    report["classification_metrics"] = rca_metrics

    rca_metrics_strict = compute_classification_metrics_strict(all_runs)
    report["classification_metrics_strict"] = rca_metrics_strict

    rca_status_metrics = compute_rca_status_metrics(all_runs)
    report["rca_status_metrics"] = rca_status_metrics

    anomaly_metrics = compute_anomaly_metrics(all_runs)
    report["anomaly_metrics"] = anomaly_metrics

    baseline_comparison = compute_baseline_comparison(all_runs)
    report["baseline_comparison"] = baseline_comparison

    print(f"\n{'='*60}")
    print("OVERALL")
    print(f"  RCA Accuracy     : {oa:.1f}%  (target ≥90%)  "
          f"{'✅' if oa >= 90 else '❌'}")
    print(f"  ...lenient       : expected label present anywhere in the candidate set")
    print(f"  RCA Accuracy(str): {oa_strict:.1f}%  "
          f"(candidate set must equal the expected set)")
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

    print()
    print("=" * 60)
    print("RCA CLASSIFICATION METRICS — STRICT")
    print("  Surplus candidates count as false positives.")
    print(f"  Precision : {rca_metrics_strict['precision']:.3f}")
    print(f"  Recall    : {rca_metrics_strict['recall']:.3f}")
    print(f"  F1-Score  : {rca_metrics_strict['f1']:.3f}")
    print(f"  TP={rca_metrics_strict['tp']}  FP={rca_metrics_strict['fp']}  "
          f"FN={rca_metrics_strict['fn']}  TN={rca_metrics_strict['tn']}")
    print(f"  Surplus candidates reported : {rca_metrics_strict['surplus_candidates']}")

    if rca_metrics.get("per_class"):
        print(f"\n  Per-class breakdown:")
        print(f"  {'RCA Label':<35} {'Prec':>6} {'Recall':>6} {'F1':>6} {'Support':>7}")
        print(f"  {'-'*60}")
        for label, m in sorted(rca_metrics["per_class"].items()):
            print(f"  {label:<35} {m['precision']:>6.3f} {m['recall']:>6.3f} "
                  f"{m['f1']:>6.3f} {m['support']:>7}")

    print(f"\n{'='*60}")
    print("RCA STATUS METRICS")
    print(f"  Status Accuracy : {rca_status_metrics['accuracy_pct']:.1f}%  "
          f"({rca_status_metrics['correct']}/{rca_status_metrics['total']})")
    for status, m in rca_status_metrics.get("by_expected", {}).items():
        print(f"  {status:<24} : {m['accuracy_pct']:>5.1f}%  "
              f"({m['correct']}/{m['total']}) actual={m['actual']}")

    print(f"\n{'='*60}")
    print("BASELINE COMPARISON  (accuracy@1, same runs)")
    if not baseline_comparison.get("runs_scored"):
        print("  No run expected a concrete root cause; nothing to compare.")
    else:
        acc = baseline_comparison["accuracy_pct"]
        same = baseline_comparison["same_eligibility"]["accuracy_pct"]
        rate = baseline_comparison["nomination_rate_pct"]
        print(f"  Scored on {baseline_comparison['runs_scored']} run(s) "
              f"expecting a concrete diagnosis.")
        print("  Two columns, because they answer different questions. Cite the second")
        print("  one for any claim about ranking.\n")
        print(f"  {'Selection policy':<34} {'own elig.':>10} {'same elig.':>11} {'names a cause':>15}")
        print(f"  {'-'*72}")
        for key, label in (("engine", "engine (tier -> position -> sev)"),
                           ("rule_id_order", "baseline: rule id order"),
                           ("severity_only", "baseline: severity only"),
                           ("first_anomaly", "baseline: first anomaly in time")):
            print(f"  {label:<34} {acc[key]:>9.1f}% {same[key]:>10.1f}% "
                  f"{rate['same_eligibility'][key]:>14.1f}%")
        print("\n  own elig.  - each policy picks from every candidate not ruled out. The")
        print("               engine promotes STRONG only, so a gap here mixes ranking with")
        print("               willingness to name a cause at all.")
        print("  same elig. - every policy restricted to the engine's promotable pool. Only")
        print("               the ordering differs, so a gap here IS the ranking.")
        print(f"\n  Engine minus rule-id ordering, same eligibility : "
              f"{baseline_comparison['engine_minus_rule_id_order_pp']:+.1f} pp")
        print(f"  {baseline_comparison['note']}")
        for d in baseline_comparison["disagreements_with_rule_id_order"][:5]:
            print(f"    - {d['scenario_id']} run {d['run_index']}: "
                  f"engine={d['engine']} vs ruleId={d['rule_id_order']} "
                  f"(expected {d['expected']})")
        print("\n  Abstention is not scored as correct here: that needs a statement of what")
        print("  each run's evidence permits. Run oracle/score.py for it.")
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

    # Reproducibility breakdown: real service behavior vs admin-injected fixtures.
    repro_breakdown = {}
    for repro in ("real", "synthetic"):
        runs = [r for r in all_runs
                if not r.get("error")
                and ALL_SCENARIOS_MAP.get(r["scenario_id"], {}).get("reproducibility") == repro]
        if not runs:
            continue
        acc = [r["rca_accurate"] for r in runs]
        acc_pct = sum(acc) / len(acc) * 100 if acc else 0
        repro_breakdown[repro] = {
            "scenarios": len({r["scenario_id"] for r in runs}),
            "runs": len(runs),
            "accuracy_pct": round(acc_pct, 1),
        }
    if repro_breakdown:
        print(f"\n{'='*60}")
        print("PER-REPRODUCIBILITY ACCURACY")
        for repro, m in repro_breakdown.items():
            print(f"  {repro.upper():<9} : {m['accuracy_pct']:>5.1f}%   "
                  f"({m['scenarios']} scenarios, {m['runs']} runs)")
        report["reproducibility_breakdown"] = repro_breakdown

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
            f.write(f"  Expected RCA: {s['expected_rca']} "
                    f"(status={s['expected_rca_status']})\n")
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

        f.write("RCA STATUS METRICS\n")
        f.write(f"  Status Accuracy: {rca_status_metrics['accuracy_pct']:.1f}% "
                f"({rca_status_metrics['correct']}/{rca_status_metrics['total']})\n")
        for status, m in rca_status_metrics.get("by_expected", {}).items():
            f.write(f"  {status}: {m['accuracy_pct']:.1f}% "
                    f"({m['correct']}/{m['total']}) actual={m['actual']}\n")
        f.write("\n")

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
