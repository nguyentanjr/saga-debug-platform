"""
Saga Observability Platform — Full Test Suite
==============================================
Tests all 20 anomaly types and 22 RCA rules via AdminEventController.

Prerequisites:
  - correlation-service running on localhost:8084
  - Database clean (or accept existing data)

Usage:
  python test_all_scenarios.py
  python test_all_scenarios.py --base-url http://localhost:8084
  python test_all_scenarios.py --scenario S03   # run single scenario
"""

import requests
import json
import time
import uuid
import sys
import argparse
from datetime import datetime, timedelta, timezone

BASE_URL = "http://localhost:8084"
RESULTS = []


# =========================================================================
# Helpers
# =========================================================================

def inject(saga_id, event_type, step_name="UNKNOWN", service_name="test",
           payload=None, event_id=None, timestamp=None, delay=0):
    """Inject a single event into the correlation engine."""
    if delay > 0:
        time.sleep(delay)
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

    resp = requests.post(f"{BASE_URL}/api/admin/inject-event", json=body)
    if resp.status_code != 200:
        print(f"  ❌ Inject failed: {resp.status_code} {resp.text}")
    return resp.json()


def analyze(saga_id):
    """Trigger RCA analysis and return result."""
    resp = requests.post(f"{BASE_URL}/api/admin/analyze/{saga_id}")
    if resp.status_code == 200:
        return resp.json()
    return None


def get_saga(saga_id):
    """Get saga detail including anomalies."""
    resp = requests.get(f"{BASE_URL}/api/sagas/{saga_id}")
    if resp.status_code == 200:
        return resp.json()
    return None


def check(scenario_id, saga_id, expected_anomalies, expected_rca, expected_outcome):
    """Check anomalies and RCA result against expectations."""
    detail = get_saga(saga_id)
    rca = analyze(saga_id)

    anomaly_types = []
    if detail and detail.get("anomalies"):
        anomaly_types = [a["anomalyType"] for a in detail["anomalies"]]

    actual_rca = rca.get("rootCause") if rca else None
    actual_outcome = rca.get("outcome") if rca else None
    actual_confidence = rca.get("confidence") if rca else None
    actual_rule = rca.get("ruleId") if rca else None
    actual_contributing = rca.get("contributingCauses") if rca else None

    # Check anomalies
    anomaly_pass = all(ea in anomaly_types for ea in expected_anomalies)
    rca_pass = actual_rca == expected_rca
    outcome_pass = actual_outcome == expected_outcome

    status = "✅ PASS" if (anomaly_pass and rca_pass and outcome_pass) else "❌ FAIL"

    result = {
        "scenario": scenario_id,
        "status": status,
        "anomalies_expected": expected_anomalies,
        "anomalies_actual": anomaly_types,
        "rca_expected": expected_rca,
        "rca_actual": actual_rca,
        "outcome_expected": expected_outcome,
        "outcome_actual": actual_outcome,
        "confidence": actual_confidence,
        "rule_id": actual_rule,
    }
    RESULTS.append(result)

    print(f"\n{'='*60}")
    print(f"{status} | {scenario_id}")
    print(f"  Anomalies: {anomaly_types}")
    if not anomaly_pass:
        print(f"  Expected:  {expected_anomalies}")
    print(f"  RCA:       {actual_rca} (expected: {expected_rca})")
    print(f"  Outcome:   {actual_outcome} (expected: {expected_outcome})")
    print(f"  Confidence:{actual_confidence}  Rule: {actual_rule}")
    if actual_contributing:
        print(f"  Contributing: {actual_contributing}")
    return result


def sid():
    """Generate unique saga ID."""
    return f"test-{uuid.uuid4().hex[:8]}"


def ts(minutes_ago=0, seconds_ago=0):
    """Generate ISO timestamp, optionally in the past."""
    t = datetime.now(timezone.utc) - timedelta(minutes=minutes_ago, seconds=seconds_ago)
    return t.isoformat()


# =========================================================================
# Test Scenarios
# =========================================================================

def s01_happy_path():
    """S01: Normal saga — no anomalies, no RCA needed."""
    print("\n🧪 S01: Happy Path (ORDER → PAYMENT → INVENTORY)")
    s = sid()
    inject(s, "ORDER_CREATED", "CREATE_ORDER", "order-service",
           payload={"orderId": "ORD-001", "sagaId": s}, timestamp=ts(seconds_ago=10))
    inject(s, "PAYMENT_COMPLETED", "PROCESS_PAYMENT", "payment-service",
           timestamp=ts(seconds_ago=5))
    inject(s, "INVENTORY_RESERVED", "RESERVE_INVENTORY", "inventory-service",
           timestamp=ts(seconds_ago=1))
    check("S01-HAPPY", s,
          expected_anomalies=[],
          expected_rca=None,
          expected_outcome="NORMAL")


def s02_payment_failed_business():
    """S02: Payment rejected (business reason) — FAILED status, no anomaly."""
    print("\n🧪 S02: Payment Failed (Business Rejection)")
    s = sid()
    inject(s, "ORDER_CREATED", "CREATE_ORDER", "order-service", timestamp=ts(seconds_ago=5))
    inject(s, "PAYMENT_FAILED", "PROCESS_PAYMENT", "payment-service",
           payload={"reason": "insufficient funds"}, timestamp=ts(seconds_ago=1))
    check("S02-PAY-FAIL", s,
          expected_anomalies=[],
          expected_rca=None,
          expected_outcome="NORMAL")


def s03_compensation_flow():
    """S03: Inventory fails → compensation → COMPENSATED."""
    print("\n🧪 S03: Compensation Flow (INV_FAIL → REFUND → REFUNDED)")
    s = sid()
    inject(s, "ORDER_CREATED", "CREATE_ORDER", "order-service", timestamp=ts(seconds_ago=20))
    inject(s, "PAYMENT_COMPLETED", "PROCESS_PAYMENT", "payment-service", timestamp=ts(seconds_ago=15))
    inject(s, "INVENTORY_FAILED", "RESERVE_INVENTORY", "inventory-service", timestamp=ts(seconds_ago=10))
    inject(s, "REFUND_PAYMENT", "REFUND_PAYMENT", "order-service", timestamp=ts(seconds_ago=5))
    inject(s, "PAYMENT_REFUNDED", "REFUND_PAYMENT", "payment-service", timestamp=ts(seconds_ago=1))
    check("S03-COMPENSATION", s,
          expected_anomalies=[],
          expected_rca="FORWARD_STEP_FAILURE",
          expected_outcome="COMPENSATED")


def s04_circuit_breaker():
    """S04: Payment fails with circuit breaker message."""
    print("\n🧪 S04: Circuit Breaker Open")
    s = sid()
    inject(s, "ORDER_CREATED", "CREATE_ORDER", "order-service", timestamp=ts(seconds_ago=5))
    inject(s, "PAYMENT_FAILED", "PROCESS_PAYMENT", "payment-service",
           payload={"reason": "circuit breaker is open", "sagaId": s},
           timestamp=ts(seconds_ago=1))
    check("S04-CB-OPEN", s,
          expected_anomalies=[],
          expected_rca="CIRCUIT_BREAKER_OPEN",
          expected_outcome="FAILED")


def s05_retry_burst():
    """S05: Same event type ≥3 times → RETRY_BURST anomaly."""
    print("\n🧪 S05: Retry Burst (3× PAYMENT_FAILED)")
    s = sid()
    inject(s, "ORDER_CREATED", "CREATE_ORDER", "order-service",
           event_id=f"e1-{s}", timestamp=ts(seconds_ago=20))
    inject(s, "PAYMENT_FAILED", "PROCESS_PAYMENT", "payment-service",
           event_id=f"e2-{s}", payload={"attempt": 1}, timestamp=ts(seconds_ago=15))
    inject(s, "PAYMENT_FAILED", "PROCESS_PAYMENT", "payment-service",
           event_id=f"e3-{s}", payload={"attempt": 2}, timestamp=ts(seconds_ago=10))
    inject(s, "PAYMENT_FAILED", "PROCESS_PAYMENT", "payment-service",
           event_id=f"e4-{s}", payload={"attempt": 3}, timestamp=ts(seconds_ago=5))
    check("S05-RETRY-BURST", s,
          expected_anomalies=["RETRY_BURST"],
          expected_rca="TIMEOUT_RETRY_STORM",
          expected_outcome="STILL_STUCK")


def s06_duplicate_event():
    """S06: Same eventId sent twice → EXACT_DUPLICATE_EVENT."""
    print("\n🧪 S06: Duplicate Event (same eventId)")
    s = sid()
    eid = f"dup-{s}"
    inject(s, "ORDER_CREATED", "CREATE_ORDER", "order-service",
           event_id=eid, timestamp=ts(seconds_ago=5))
    inject(s, "ORDER_CREATED", "CREATE_ORDER", "order-service",
           event_id=eid, timestamp=ts(seconds_ago=3))
    # Second event is deduped but anomaly created
    check("S06-DUPLICATE", s,
          expected_anomalies=["EXACT_DUPLICATE_EVENT"],
          expected_rca="DUPLICATE_DELIVERY",
          expected_outcome="STILL_STUCK")


def s07_out_of_order():
    """S07: Events arrive out of timestamp order → OUT_OF_ORDER."""
    print("\n🧪 S07: Out-of-Order Events")
    s = sid()
    # Event 1 arrives first with timestamp T2
    inject(s, "ORDER_CREATED", "CREATE_ORDER", "order-service",
           event_id=f"e1-{s}", timestamp=ts(seconds_ago=1))
    time.sleep(0.2)
    # Event 2 arrives second with timestamp T1 (earlier!)
    inject(s, "PAYMENT_COMPLETED", "PROCESS_PAYMENT", "payment-service",
           event_id=f"e2-{s}", timestamp=ts(seconds_ago=10))
    check("S07-OUT-OF-ORDER", s,
          expected_anomalies=["OUT_OF_ORDER"],
          expected_rca="OUT_OF_ORDER_DELIVERY",
          expected_outcome="STILL_STUCK")


def s08_multi_terminal_conflict():
    """S08: Both success and failure events → MULTI_TERMINAL_CONFLICT."""
    print("\n🧪 S08: Multi-Terminal Conflict (success + failure)")
    s = sid()
    inject(s, "ORDER_CREATED", "CREATE_ORDER", "order-service",
           event_id=f"e1-{s}", timestamp=ts(seconds_ago=15))
    inject(s, "PAYMENT_COMPLETED", "PROCESS_PAYMENT", "payment-service",
           event_id=f"e2-{s}", timestamp=ts(seconds_ago=10))
    inject(s, "INVENTORY_RESERVED", "RESERVE_INVENTORY", "inventory-service",
           event_id=f"e3-{s}", timestamp=ts(seconds_ago=5))
    # Now inject a failure event into an already-completed saga
    inject(s, "PAYMENT_FAILED", "PROCESS_PAYMENT", "payment-service",
           event_id=f"e4-{s}", timestamp=ts(seconds_ago=1))
    check("S08-MULTI-TERMINAL", s,
          expected_anomalies=["MULTI_TERMINAL_CONFLICT", "FORBIDDEN_POST_TERMINAL"],
          expected_rca="MULTI_TERMINAL_CONFLICT",
          expected_outcome="SELF_HEALED")


def s09_forbidden_post_terminal():
    """S09: Event received after saga completed."""
    print("\n🧪 S09: Forbidden Post-Terminal Event")
    s = sid()
    inject(s, "ORDER_CREATED", "CREATE_ORDER", "order-service",
           event_id=f"e1-{s}", timestamp=ts(seconds_ago=10))
    inject(s, "PAYMENT_COMPLETED", "PROCESS_PAYMENT", "payment-service",
           event_id=f"e2-{s}", timestamp=ts(seconds_ago=5))
    inject(s, "INVENTORY_RESERVED", "RESERVE_INVENTORY", "inventory-service",
           event_id=f"e3-{s}", timestamp=ts(seconds_ago=3))
    # Late event after completion
    inject(s, "PAYMENT_COMPLETED", "PROCESS_PAYMENT", "payment-service",
           event_id=f"e4-{s}", timestamp=ts(seconds_ago=1))
    check("S09-POST-TERMINAL", s,
          expected_anomalies=["FORBIDDEN_POST_TERMINAL"],
          expected_rca="TERMINAL_STATE_OVERWRITE",
          expected_outcome="SELF_HEALED")


def s10_state_regression():
    """S10: Saga status goes backward (COMPLETED → earlier status)."""
    print("\n🧪 S10: State Regression")
    s = sid()
    inject(s, "ORDER_CREATED", "CREATE_ORDER", "order-service",
           event_id=f"e1-{s}", timestamp=ts(seconds_ago=10))
    inject(s, "PAYMENT_COMPLETED", "PROCESS_PAYMENT", "payment-service",
           event_id=f"e2-{s}", timestamp=ts(seconds_ago=5))
    inject(s, "INVENTORY_RESERVED", "RESERVE_INVENTORY", "inventory-service",
           event_id=f"e3-{s}", timestamp=ts(seconds_ago=3))
    # This should cause COMPLETED → COMPENSATING (backward)
    inject(s, "REFUND_PAYMENT", "REFUND_PAYMENT", "order-service",
           event_id=f"e4-{s}", timestamp=ts(seconds_ago=1))
    check("S10-REGRESSION", s,
          expected_anomalies=["STATE_REGRESSION", "FORBIDDEN_POST_TERMINAL"],
          expected_rca="TERMINAL_STATE_OVERWRITE",
          expected_outcome="INCONSISTENT")


def s11_duplicate_compensation():
    """S11: REFUND_PAYMENT appears twice → DUPLICATE_COMPENSATION."""
    print("\n🧪 S11: Duplicate Compensation (2× REFUND_PAYMENT)")
    s = sid()
    inject(s, "ORDER_CREATED", "CREATE_ORDER", "order-service",
           event_id=f"e1-{s}", timestamp=ts(seconds_ago=20))
    inject(s, "PAYMENT_COMPLETED", "PROCESS_PAYMENT", "payment-service",
           event_id=f"e2-{s}", timestamp=ts(seconds_ago=15))
    inject(s, "INVENTORY_FAILED", "RESERVE_INVENTORY", "inventory-service",
           event_id=f"e3-{s}", timestamp=ts(seconds_ago=10))
    inject(s, "REFUND_PAYMENT", "REFUND_PAYMENT", "order-service",
           event_id=f"e4-{s}", timestamp=ts(seconds_ago=5))
    inject(s, "REFUND_PAYMENT", "REFUND_PAYMENT", "order-service",
           event_id=f"e5-{s}", timestamp=ts(seconds_ago=3))
    inject(s, "PAYMENT_REFUNDED", "REFUND_PAYMENT", "payment-service",
           event_id=f"e6-{s}", timestamp=ts(seconds_ago=1))
    check("S11-DUP-COMP", s,
          expected_anomalies=["DUPLICATE_COMPENSATION"],
          expected_rca="DUPLICATE_DELIVERY",
          expected_outcome="COMPENSATED")


def s12_step_timeout():
    """S12: Step takes longer than YAML timeout → STEP_TIMEOUT.
    NOTE: Requires scheduled detection to run (60s interval). This test
    injects events with large timestamp gap and waits for detection."""
    print("\n🧪 S12: Step Timeout (gap > workflow timeout)")
    s = sid()
    # Gap of 120s between events while YAML timeout is 30s
    inject(s, "ORDER_CREATED", "CREATE_ORDER", "order-service",
           event_id=f"e1-{s}", timestamp=ts(minutes_ago=5))
    inject(s, "PAYMENT_COMPLETED", "PROCESS_PAYMENT", "payment-service",
           event_id=f"e2-{s}", timestamp=ts(minutes_ago=3))
    inject(s, "INVENTORY_RESERVED", "RESERVE_INVENTORY", "inventory-service",
           event_id=f"e3-{s}", timestamp=ts(seconds_ago=1))
    # Wait for scheduled detector
    print("  ⏳ Waiting 65s for detectStepTimeouts to run...")
    time.sleep(65)
    check("S12-STEP-TIMEOUT", s,
          expected_anomalies=["STEP_TIMEOUT"],
          expected_rca="DOWNSTREAM_TIMEOUT",
          expected_outcome="SELF_HEALED")


def s13_stuck_saga():
    """S13: Saga with no new events for > timeout → SAGA_STUCK.
    NOTE: Requires detectStuckSagas to run (30s interval)."""
    print("\n🧪 S13: Stuck Saga (no event for > timeout)")
    s = sid()
    inject(s, "ORDER_CREATED", "CREATE_ORDER", "order-service",
           event_id=f"e1-{s}", timestamp=ts(minutes_ago=5))
    inject(s, "PAYMENT_COMPLETED", "PROCESS_PAYMENT", "payment-service",
           event_id=f"e2-{s}", timestamp=ts(minutes_ago=4))
    # No INVENTORY_RESERVED — saga stuck in IN_PROGRESS
    print("  ⏳ Waiting 35s for detectStuckSagas to run...")
    time.sleep(35)
    check("S13-STUCK", s,
          expected_anomalies=["SAGA_STUCK"],
          expected_rca="STUCK_UNKNOWN_CAUSE",
          expected_outcome="STILL_STUCK")


def s14_stuck_then_recover():
    """S14: Saga gets stuck then recovers → TRANSIENT_SERVICE_FAILURE."""
    print("\n🧪 S14: Stuck → Recovered (Transient Failure)")
    s = sid()
    inject(s, "ORDER_CREATED", "CREATE_ORDER", "order-service",
           event_id=f"e1-{s}", timestamp=ts(minutes_ago=5))
    inject(s, "PAYMENT_COMPLETED", "PROCESS_PAYMENT", "payment-service",
           event_id=f"e2-{s}", timestamp=ts(minutes_ago=4))
    # Wait for stuck detection
    print("  ⏳ Waiting 35s for detectStuckSagas...")
    time.sleep(35)
    # Verify stuck detected
    detail = get_saga(s)
    stuck_detected = any(a["anomalyType"] == "SAGA_STUCK"
                         for a in (detail or {}).get("anomalies", []))
    print(f"  Stuck detected: {stuck_detected}")
    # Now recover
    inject(s, "INVENTORY_RESERVED", "RESERVE_INVENTORY", "inventory-service",
           event_id=f"e3-{s}", timestamp=ts(seconds_ago=1))
    check("S14-RECOVER", s,
          expected_anomalies=["SAGA_STUCK"],
          expected_rca="TRANSIENT_SERVICE_FAILURE",
          expected_outcome="SELF_HEALED")


def s15_compensation_missing():
    """S15: INVENTORY_FAILED but no REFUND_PAYMENT → COMPENSATION_MISSING.
    NOTE: Requires detectCompensationMissing to run (60s interval)."""
    print("\n🧪 S15: Compensation Missing (fail but no refund)")
    s = sid()
    inject(s, "ORDER_CREATED", "CREATE_ORDER", "order-service",
           event_id=f"e1-{s}", timestamp=ts(minutes_ago=3))
    inject(s, "PAYMENT_COMPLETED", "PROCESS_PAYMENT", "payment-service",
           event_id=f"e2-{s}", timestamp=ts(minutes_ago=2))
    inject(s, "INVENTORY_FAILED", "RESERVE_INVENTORY", "inventory-service",
           event_id=f"e3-{s}", timestamp=ts(minutes_ago=1))
    # No REFUND_PAYMENT follows
    print("  ⏳ Waiting 65s for detectCompensationMissing...")
    time.sleep(65)
    check("S15-COMP-MISSING", s,
          expected_anomalies=["COMPENSATION_MISSING"],
          expected_rca="COMPENSATION_NOT_TRIGGERED",
          expected_outcome="INCONSISTENT")


def s16_compensation_step_failure():
    """S16: REFUND_PAYMENT sent but no PAYMENT_REFUNDED → COMPENSATION_STEP_FAILURE."""
    print("\n🧪 S16: Compensation Step Failure (refund cmd but no ack)")
    s = sid()
    inject(s, "ORDER_CREATED", "CREATE_ORDER", "order-service",
           event_id=f"e1-{s}", timestamp=ts(minutes_ago=5))
    inject(s, "PAYMENT_COMPLETED", "PROCESS_PAYMENT", "payment-service",
           event_id=f"e2-{s}", timestamp=ts(minutes_ago=4))
    inject(s, "INVENTORY_FAILED", "RESERVE_INVENTORY", "inventory-service",
           event_id=f"e3-{s}", timestamp=ts(minutes_ago=3))
    inject(s, "REFUND_PAYMENT", "REFUND_PAYMENT", "order-service",
           event_id=f"e4-{s}", timestamp=ts(minutes_ago=2))
    # No PAYMENT_REFUNDED
    print("  ⏳ Waiting 65s for detectMissingCompensationAck...")
    time.sleep(65)
    check("S16-COMP-STEP-FAIL", s,
          expected_anomalies=["MISSING_COMPENSATION_ACK"],
          expected_rca="COMPENSATION_STEP_FAILURE",
          expected_outcome="INCONSISTENT")


def s17_invalid_transition():
    """S17: Skip a step → INVALID_TRANSITION."""
    print("\n🧪 S17: Invalid Transition (skip PAYMENT step)")
    s = sid()
    inject(s, "ORDER_CREATED", "CREATE_ORDER", "order-service",
           event_id=f"e1-{s}", timestamp=ts(seconds_ago=10))
    # Skip PAYMENT_COMPLETED, go directly to INVENTORY_RESERVED
    inject(s, "INVENTORY_RESERVED", "RESERVE_INVENTORY", "inventory-service",
           event_id=f"e2-{s}", timestamp=ts(seconds_ago=5))
    check("S17-INVALID-TRANS", s,
          expected_anomalies=["INVALID_TRANSITION"],
          expected_rca="INVALID_TRANSITION",
          expected_outcome="SELF_HEALED")


def s18_post_failed_event():
    """S18: Event after FAILED status → FORBIDDEN_POST_TERMINAL."""
    print("\n🧪 S18: Event After FAILED Status")
    s = sid()
    inject(s, "ORDER_CREATED", "CREATE_ORDER", "order-service",
           event_id=f"e1-{s}", timestamp=ts(seconds_ago=10))
    inject(s, "PAYMENT_FAILED", "PROCESS_PAYMENT", "payment-service",
           event_id=f"e2-{s}", timestamp=ts(seconds_ago=5))
    # Event after FAILED
    inject(s, "INVENTORY_RESERVED", "RESERVE_INVENTORY", "inventory-service",
           event_id=f"e3-{s}", timestamp=ts(seconds_ago=1))
    check("S18-POST-FAILED", s,
          expected_anomalies=["FORBIDDEN_POST_TERMINAL"],
          expected_rca="TERMINAL_STATE_OVERWRITE",
          expected_outcome="SELF_HEALED")


def s19_orphan_event():
    """S19: Event with wrong sagaId (no ORDER_CREATED first) → ORPHAN_EVENT."""
    print("\n🧪 S19: Orphan Event (wrong sagaId, no ORDER_CREATED)")
    s = sid()
    # First event is NOT ORDER_CREATED → orphan
    inject(s, "PAYMENT_COMPLETED", "PROCESS_PAYMENT", "payment-service",
           event_id=f"e1-{s}", timestamp=ts(seconds_ago=5))
    check("S19-ORPHAN", s,
          expected_anomalies=["ORPHAN_EVENT"],
          expected_rca="WRONG_SAGA_CORRELATION_KEY",
          expected_outcome="STILL_STUCK")


def s20_missing_trace_context():
    """S20: Events without traceId → MISSING_TRACE_CONTEXT."""
    print("\n🧪 S20: Missing Trace Context (no traceId in payload)")
    s = sid()
    # Events without traceId in payload
    inject(s, "ORDER_CREATED", "CREATE_ORDER", "order-service",
           event_id=f"e1-{s}",
           payload={"orderId": "ORD-TC", "sagaId": s},  # no traceId
           timestamp=ts(seconds_ago=10))
    inject(s, "PAYMENT_COMPLETED", "PROCESS_PAYMENT", "payment-service",
           event_id=f"e2-{s}",
           payload={"amount": 100},  # no traceId
           timestamp=ts(seconds_ago=5))
    inject(s, "INVENTORY_RESERVED", "RESERVE_INVENTORY", "inventory-service",
           event_id=f"e3-{s}",
           payload={"quantity": 1},  # no traceId
           timestamp=ts(seconds_ago=1))
    check("S20-NO-TRACE", s,
          expected_anomalies=["MISSING_TRACE_CONTEXT"],
          expected_rca="MISSING_TRACE_PROPAGATION",
          expected_outcome="SELF_HEALED")


def s21_delayed_compensation():
    """S21: Large gap between failure and compensation → DELAYED_COMPENSATION.
    NOTE: Requires detectDelayedCompensation to run (60s interval)."""
    print("\n🧪 S21: Delayed Compensation (>15s gap failure→compensation)")
    s = sid()
    inject(s, "ORDER_CREATED", "CREATE_ORDER", "order-service",
           event_id=f"e1-{s}", timestamp=ts(minutes_ago=5))
    inject(s, "PAYMENT_COMPLETED", "PROCESS_PAYMENT", "payment-service",
           event_id=f"e2-{s}", timestamp=ts(minutes_ago=4))
    # INVENTORY_FAILED at T-3min, REFUND at T-1min → 120s gap >> 15s threshold
    inject(s, "INVENTORY_FAILED", "RESERVE_INVENTORY", "inventory-service",
           event_id=f"e3-{s}", timestamp=ts(minutes_ago=3))
    inject(s, "REFUND_PAYMENT", "REFUND_PAYMENT", "order-service",
           event_id=f"e4-{s}", timestamp=ts(minutes_ago=1))
    inject(s, "PAYMENT_REFUNDED", "REFUND_PAYMENT", "payment-service",
           event_id=f"e5-{s}", timestamp=ts(seconds_ago=10))
    # Wait for scheduled detector
    print("  ⏳ Waiting 65s for detectDelayedCompensation to run...")
    time.sleep(65)
    check("S21-DELAYED-COMP", s,
          expected_anomalies=["DELAYED_COMPENSATION"],
          expected_rca="FORWARD_STEP_FAILURE",  # primary RCA is still forward step failure
          expected_outcome="COMPENSATED")


# =========================================================================
# Fast scenarios (no scheduled wait needed)
# =========================================================================
FAST_SCENARIOS = [
    s01_happy_path,
    s02_payment_failed_business,
    s03_compensation_flow,
    s04_circuit_breaker,
    s05_retry_burst,
    s06_duplicate_event,
    s07_out_of_order,
    s08_multi_terminal_conflict,
    s09_forbidden_post_terminal,
    s10_state_regression,
    s11_duplicate_compensation,
    s17_invalid_transition,
    s18_post_failed_event,
    s19_orphan_event,
    s20_missing_trace_context,
]

# Slow scenarios (require scheduled task to run)
SLOW_SCENARIOS = [
    s12_step_timeout,
    s13_stuck_saga,
    s14_stuck_then_recover,
    s15_compensation_missing,
    s16_compensation_step_failure,
    s21_delayed_compensation,
]


def print_summary():
    """Print test results summary."""
    print("\n" + "=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)

    passed = sum(1 for r in RESULTS if "PASS" in r["status"])
    failed = sum(1 for r in RESULTS if "FAIL" in r["status"])
    total = len(RESULTS)

    for r in RESULTS:
        anomaly_ok = "✅" if all(ea in r["anomalies_actual"] for ea in r["anomalies_expected"]) else "❌"
        rca_ok = "✅" if r["rca_actual"] == r["rca_expected"] else "❌"
        outcome_ok = "✅" if r["outcome_actual"] == r["outcome_expected"] else "❌"
        conf = f"{r['confidence']:.2f}" if r["confidence"] else "N/A"
        print(f"  {r['status']}  {r['scenario']:<22} "
              f"Anomaly:{anomaly_ok} RCA:{rca_ok} Outcome:{outcome_ok} "
              f"Conf:{conf} Rule:{r['rule_id']}")

    print(f"\n  Total: {total}  Passed: {passed}  Failed: {failed}")
    print("=" * 70)

    if failed > 0:
        print("\n❌ FAILED scenarios detail:")
        for r in RESULTS:
            if "FAIL" in r["status"]:
                print(f"\n  {r['scenario']}:")
                print(f"    Anomalies expected: {r['anomalies_expected']}")
                print(f"    Anomalies actual:   {r['anomalies_actual']}")
                print(f"    RCA expected: {r['rca_expected']}")
                print(f"    RCA actual:   {r['rca_actual']}")
                print(f"    Outcome expected: {r['outcome_expected']}")
                print(f"    Outcome actual:   {r['outcome_actual']}")

    return failed == 0


def main():
    parser = argparse.ArgumentParser(description="Saga Observability Platform Test Suite")
    parser.add_argument("--base-url", default="http://localhost:8084",
                        help="Correlation service URL")
    parser.add_argument("--scenario", help="Run single scenario (e.g. S03)")
    parser.add_argument("--fast-only", action="store_true",
                        help="Skip slow scenarios (scheduled tasks)")
    args = parser.parse_args()

    global BASE_URL
    BASE_URL = args.base_url

    # Health check
    try:
        resp = requests.get(f"{BASE_URL}/actuator/health", timeout=5)
        if resp.status_code != 200:
            print(f"❌ Service not healthy: {resp.status_code}")
            sys.exit(1)
        print(f"✅ Service healthy at {BASE_URL}")
    except requests.ConnectionError:
        print(f"❌ Cannot connect to {BASE_URL}")
        print("   Start the service first: docker-compose up correlation-service")
        sys.exit(1)

    all_scenarios = {
        "S01": s01_happy_path,
        "S02": s02_payment_failed_business,
        "S03": s03_compensation_flow,
        "S04": s04_circuit_breaker,
        "S05": s05_retry_burst,
        "S06": s06_duplicate_event,
        "S07": s07_out_of_order,
        "S08": s08_multi_terminal_conflict,
        "S09": s09_forbidden_post_terminal,
        "S10": s10_state_regression,
        "S11": s11_duplicate_compensation,
        "S12": s12_step_timeout,
        "S13": s13_stuck_saga,
        "S14": s14_stuck_then_recover,
        "S15": s15_compensation_missing,
        "S16": s16_compensation_step_failure,
        "S17": s17_invalid_transition,
        "S18": s18_post_failed_event,
        "S19": s19_orphan_event,
        "S20": s20_missing_trace_context,
        "S21": s21_delayed_compensation,
    }

    if args.scenario:
        key = args.scenario.upper()
        if key in all_scenarios:
            all_scenarios[key]()
        else:
            print(f"Unknown scenario: {args.scenario}")
            print(f"Available: {', '.join(all_scenarios.keys())}")
            sys.exit(1)
    else:
        print(f"\n🚀 Running {'FAST' if args.fast_only else 'ALL'} scenarios...")
        scenarios = FAST_SCENARIOS if args.fast_only else (FAST_SCENARIOS + SLOW_SCENARIOS)
        for fn in scenarios:
            try:
                fn()
            except Exception as e:
                print(f"  ❌ Exception in {fn.__name__}: {e}")
                RESULTS.append({"scenario": fn.__name__, "status": "❌ FAIL (exception)",
                                "anomalies_expected": [], "anomalies_actual": [],
                                "rca_expected": "?", "rca_actual": str(e),
                                "outcome_expected": "?", "outcome_actual": "?",
                                "confidence": None, "rule_id": None})

    success = print_summary()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
