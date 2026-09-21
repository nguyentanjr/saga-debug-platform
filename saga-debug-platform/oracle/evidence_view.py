"""Split one evaluation artifact into what the observer saw and what only the oracle sees.

Why this exists: docs/ORACLE.md muc 5.0 forbids using `anomalies_actual` or any `rca_*`
field as `available_evidence` -- they are the output of the very detector being scored.
A labeller who reads the artifact directly cannot avoid seeing that output, and after
seeing it the label is retrospective (muc 5.1). This tool removes the temptation
mechanically: it emits an evidence-only view, so `admissible_claims` can be written
before the platform's answer is ever read, and the date recorded.

Two channels, deliberately kept apart in both the fixture and the printout:

  available_evidence -- observations the observer actually received (raw_events) plus the
      framing of the decision point (scored_at, workflow_type_at_scoring, capture gaps).
  oracle_only        -- `source_state`, read from the source system's own actuator. This
      establishes `true_cause`. It is NEVER an input to `admissible_claims`, and never
      reaches the observer.

`saga_status` sits in neither: it is business state the observer derived from the events,
so it is reported as the case's `scored_at_status` (the decision point), not as evidence.

Usage:
    python oracle/evidence_view.py results/evaluation_report_settled_20260921.json --list
    python oracle/evidence_view.py <artifact> --scenario SC01_HEALTHY
    python oracle/evidence_view.py <artifact> --freeze oracle/evidence
"""

import argparse
import hashlib
import json
from pathlib import Path

# Fields that are the platform's own reasoning. Never copied into an evidence fixture,
# never printed. Listed explicitly rather than by prefix so that a new output field added
# to evaluate.py shows up in `unclassified_fields` instead of leaking silently.
PLATFORM_OUTPUT_FIELDS = {
    "anomalies_actual", "anomaly_records", "baseline_picks",
    "rca_accurate", "rca_accurate_strict", "rca_candidates", "rca_evidence_roots",
    "rca_expected", "rca_expected_evidence", "rca_expected_excludes",
    "rca_expected_status", "rca_extra_roots", "rca_latency_ms", "rca_primary_roots",
    "rca_root_cause", "rca_severity", "rca_status", "time_to_insight_s",
    # anomalies_expected is the OTHER oracle's label, not an observation.
    "anomalies_expected",
}

# Observer-side evidence and the framing of the decision point.
EVIDENCE_FIELDS = [
    "execution_id", "saga_id", "identities", "raw_events",
    "scored_at", "workflow_type_at_scoring", "saga_duration_ms", "order_latency_ms",
    "evidence_capture_gaps", "error",
]

# Derived by the observer from the events; the decision point, not evidence.
DECISION_POINT_FIELDS = ["saga_status", "timestamp"]

# The workflow declaration the demo saga runs under. Frozen alongside the observations
# so that a fixture names the spec it belongs to; see spec_identity() for what that does
# and does not establish.
DEFAULT_SPEC = "correlation-service/src/main/resources/workflows/order-saga-workflow.yaml"

NOISY_PAYLOAD_KEYS = {"traceId", "spanId", "parentSpanId", "eventId", "sagaId",
                      "orderId", "timestamp", "eventType", "stepName", "serviceName"}


def split_run(run):
    """One run -> evidence view. Unknown fields are surfaced, not silently dropped."""
    known = set(EVIDENCE_FIELDS) | set(DECISION_POINT_FIELDS) | PLATFORM_OUTPUT_FIELDS
    known |= {"scenario_id", "run_index", "source_state"}
    unknown = sorted(set(run) - known)

    view = {"scenario_id": run["scenario_id"], "run_index": run["run_index"]}
    view["available_evidence"] = {k: run[k] for k in EVIDENCE_FIELDS if k in run}
    view["decision_point"] = {k: run[k] for k in DECISION_POINT_FIELDS if k in run}
    view["unclassified_fields"] = unknown  # empty is the expected state
    return view, run.get("source_state")


def by_scenario(artifact):
    runs = {}
    for run in artifact.get("raw_runs") or []:
        runs.setdefault(run["scenario_id"], []).append(run)
    for scenario in runs.values():
        scenario.sort(key=lambda r: r["run_index"])
    return runs


def spec_identity(spec_path):
    """The workflow declaration as it stands at freeze time, hashed.

    Honest limit, and it is the reason this is a separate block rather than a field
    beside the observations: the artifact records only `workflow_type_at_scoring`, a NAME.
    Nothing in it says which bytes of the declaration were loaded when the run was scored.
    Hashing the file here fixes the spec for FUTURE comparisons -- it does not prove what
    the engine read at run time. Closing that needs the engine to stamp a spec version or
    hash into the artifact; until then this is a lock going forward, not a verification
    looking back.
    """
    path = Path(spec_path)
    if not path.is_file():
        return {"path": str(spec_path).replace("\\", "/"), "unavailable": "file not found"}
    return {
        "path": str(spec_path).replace("\\", "/"),
        "sha256_at_freeze": hashlib.sha256(path.read_bytes()).hexdigest(),
        "caveat": ("Hash of the declaration file at freeze time. The artifact carries only "
                   "workflow_type_at_scoring (a name), so this does NOT establish which "
                   "spec bytes were in effect during the run. See ROADMAP L10."),
    }


def freeze(artifact, artifact_path, out_dir, spec_path=DEFAULT_SPEC):
    """Write one evidence fixture per scenario, in the shape score.py already validates.

    docs/ORACLE.md muc 2: a case is spec + observation set + scoring moment, all three
    fixed. An earlier version of this file froze only the observations, so a fixture kept
    applying after the scoring moment or the workflow name changed -- the labels would have
    been reused for a different case (ROADMAP L10). All three now go into the fixture.
    """
    digest = hashlib.sha256(Path(artifact_path).read_bytes()).hexdigest()
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    written = []
    for scenario_id, runs in sorted(by_scenario(artifact).items()):
        payload = {
            "source_artifact": str(artifact_path).replace("\\", "/"),
            "source_sha256": digest,
            "scope": (f"{scenario_id} stored observations. source_state is oracle-only and "
                      "MUST NOT be passed to the observer. No stored diagnosis is an input."),
            "spec": spec_identity(spec_path),
            "raw_runs": [],
        }
        for run in runs:
            view, source_state = split_run(run)
            leaked = PLATFORM_OUTPUT_FIELDS & set(view["available_evidence"])
            if leaked:
                raise ValueError(f"platform output leaked into evidence view: {sorted(leaked)}")
            payload["raw_runs"].append({
                "scenario_id": run["scenario_id"],
                "run_index": run["run_index"],
                "execution_id": run.get("execution_id"),
                # The three components score.py compares against the artifact under test.
                "raw_events": run.get("raw_events"),
                "scored_at": run.get("scored_at"),
                "workflow_type_at_scoring": run.get("workflow_type_at_scoring"),
                "source_state": source_state,
            })
        path = out_dir / f"{scenario_id}.json"
        path.write_text(json.dumps(payload, indent=1, ensure_ascii=False), encoding="utf-8")
        written.append((path, hashlib.sha256(path.read_bytes()).hexdigest()))
    return digest, written


def print_event(index, event):
    print(f"   {index:>2}. {str(event.get('eventType')):<26} {event.get('stepName')}")
    print(f"       service={event.get('serviceName')}  basis={event.get('correlationBasis')}"
          f"  transport={event.get('transportRef')}")
    print(f"       timestamp={event.get('timestamp')}  receivedAt={event.get('receivedAt')}")
    payload = event.get("payload")
    if not payload:
        return
    try:
        parsed = json.loads(payload)
    except (TypeError, ValueError):
        print(f"       payload(raw)={str(payload)[:400]}")
        return
    rest = {k: v for k, v in parsed.items() if k not in NOISY_PAYLOAD_KEYS}
    print(f"       payload={json.dumps(rest, ensure_ascii=False)[:600]}")


def print_compact(scenario_id, runs, show_source_state):
    """One line per event, capture gaps printed once. For reading many runs at a time.

    Timestamps are shown as offsets from the run's first observation, because what the
    labeller needs is the SHAPE of the gaps, and absolute times bury it.
    """
    print("=" * 78)
    print(f"  {scenario_id}  --  {len(runs)} luot   [compact]")
    print("=" * 78)
    for run in runs:
        view, source_state = split_run(run)
        ev, dp = view["available_evidence"], view["decision_point"]
        events = ev.get("raw_events") or []
        print(f"\n luot {run['run_index']}: trang thai luc cham={dp.get('saga_status')}"
              f"  duration={ev.get('saga_duration_ms')} ms  moc cham={ev.get('scored_at')}"
              f"  n_event={len(events)}")
        if ev.get("error"):
            print(f"   LOI HARNESS: {ev['error']}")
        base = None
        for index, event in enumerate(events, 1):
            stamp = event.get("timestamp") or ""
            offset = ""
            try:
                from datetime import datetime
                current = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
                base = base or current
                offset = f"+{(current - base).total_seconds():>8.3f}s"
            except (TypeError, ValueError):
                offset = f"{stamp:>10}"
            extra = ""
            payload = event.get("payload")
            if payload:
                try:
                    parsed = json.loads(payload)
                    rest = {k: v for k, v in parsed.items() if k not in NOISY_PAYLOAD_KEYS
                            and k not in {"causationId", "amount", "quantity", "productId",
                                          "customerId"}}
                    if rest:
                        extra = "  " + json.dumps(rest, ensure_ascii=False)[:320]
                except (TypeError, ValueError):
                    extra = f"  payload(raw)={str(payload)[:200]}"
            print(f"   {index:>2}. {offset}  {str(event.get('eventType')):<24}"
                  f" {str(event.get('stepName')):<20} {event.get('serviceName')}{extra}")
        if not events:
            print("    (khong co event nao -- im lang)")
        if show_source_state:
            print(f"   [CHI ORACLE] {json.dumps(source_state, ensure_ascii=False)[:1400]}")
    gaps = (runs[0].get("evidence_capture_gaps") or {}) if runs else {}
    if gaps:
        print("\n  LO HONG THU THAP (chung cho moi luot):")
        for name in sorted(gaps):
            print(f"   - {name}")
    print()


def print_scenario(scenario_id, runs, show_source_state):
    print("=" * 78)
    print(f"  {scenario_id}  --  {len(runs)} luot")
    print("=" * 78)
    for run in runs:
        view, source_state = split_run(run)
        ev = view["available_evidence"]
        dp = view["decision_point"]
        print(f"\n--- luot {run['run_index']}  (execution {ev.get('execution_id')}) ---")
        print(f"  moc cham         : {ev.get('scored_at')}")
        print(f"  workflow luc cham: {ev.get('workflow_type_at_scoring')}")
        print(f"  trang thai luc cham (observer suy ra): {dp.get('saga_status')}"
              f"   duration={ev.get('saga_duration_ms')} ms")
        if ev.get("error"):
            print(f"  LOI HARNESS      : {ev['error']}")
        if ev.get("identities"):
            print(f"  identities       : {json.dumps(ev['identities'], ensure_ascii=False)[:400]}")
        if view["unclassified_fields"]:
            print(f"  TRUONG CHUA PHAN LOAI: {view['unclassified_fields']}")
        events = ev.get("raw_events") or []
        print(f"\n  OBSERVATION observer THUC SU NHAN ({len(events)}):")
        for index, event in enumerate(events, 1):
            print_event(index, event)
        if not events:
            print("      (khong co event nao -- im lang)")
        gaps = ev.get("evidence_capture_gaps") or {}
        if gaps:
            print("\n  LO HONG THU THAP da ghi nhan:")
            for name in sorted(gaps):
                print(f"   - {name}: {' '.join(str(gaps[name]).split())[:300]}")
        if show_source_state:
            print("\n  [CHI ORACLE -- KHONG PHAI EVIDENCE] source_state:")
            print("   " + json.dumps(source_state, ensure_ascii=False)[:2000])
        print()


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("artifact")
    parser.add_argument("--scenario", action="append", default=[],
                        help="scenario id or prefix (repeatable); default prints all")
    parser.add_argument("--list", action="store_true", help="just list scenarios and run counts")
    parser.add_argument("--freeze", metavar="DIR", help="write one evidence fixture per scenario")
    parser.add_argument("--spec", default=DEFAULT_SPEC,
                        help="workflow declaration to hash into the fixture")
    parser.add_argument("--compact", action="store_true",
                        help="one line per event; capture gaps printed once")
    parser.add_argument("--source-state", action="store_true",
                        help="also print the oracle-only source_state (true_cause channel)")
    args = parser.parse_args()

    artifact = json.loads(Path(args.artifact).read_text(encoding="utf-8"))
    scenarios = by_scenario(artifact)

    if args.list:
        digest = hashlib.sha256(Path(args.artifact).read_bytes()).hexdigest()
        print(f"artefact: {args.artifact}")
        print(f"sha256  : {digest}\n")
        for scenario_id, runs in sorted(scenarios.items()):
            events = sum(len(r.get("raw_events") or []) for r in runs)
            has_state = sum(1 for r in runs if r.get("source_state"))
            print(f"  {scenario_id:<34} luot={len(runs)}  event={events}"
                  f"  source_state={has_state}/{len(runs)}")
        return

    if args.freeze:
        digest, written = freeze(artifact, args.artifact, args.freeze, args.spec)
        print(f"artefact sha256: {digest}")
        for path, file_digest in written:
            print(f"  {path}  sha256={file_digest}")
        return

    wanted = args.scenario or sorted(scenarios)
    for prefix in wanted:
        matches = sorted(s for s in scenarios if s.startswith(prefix))
        if not matches:
            print(f"  khong co scenario nao khop '{prefix}'")
            continue
        for match in matches:
            printer = print_compact if args.compact else print_scenario
            printer(match, scenarios[match], args.source_state)


if __name__ == "__main__":
    main()
