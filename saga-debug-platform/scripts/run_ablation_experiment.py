import csv
import json
import os
import statistics
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import evaluate as ev


PLATFORM_HEALTH_URL = f"{ev.PLATFORM_URL}/actuator/health"
RCA_URL = f"{ev.PLATFORM_URL}/rca/analyze"

PROFILES = [
    ("score-free-selector", None),
]

# Scenarios where multiple RCA rules can legitimately co-fire and ranking can matter.
MULTI_RULE_SCENARIOS = {
    "SC05_COMPENSATION_STEP_FAILURE",  # D2: R10 + R19
    "SC06_CIRCUIT_BREAKER_REAL",       # B1: R01 + R02
    "SC16_STUCK_RECOVERED_LONG",       # A4: R05 (+ suppression behavior vs R06)
    "SC21_MULTI_TERMINAL_CONFLICT",    # C5: R03 + possible R04
    "SC23_DUPLICATE_COMPENSATION",     # D4: R10 + R13
    "SC24_DELAYED_COMPENSATION",       # D5: R11 + R22
}


def all_scenarios():
    scenarios = []
    for bucket in ev.ALL_SCENARIO_LISTS:
        scenarios.extend(bucket)
    return sorted(scenarios, key=lambda s: int(s["id"].split("_")[0][2:]))


def restart_correlation_service(spring_profile):
    env = os.environ.copy()
    if spring_profile:
        env["SPRING_PROFILES_ACTIVE"] = spring_profile
    else:
        env.pop("SPRING_PROFILES_ACTIVE", None)

    subprocess.run(
        ["docker", "compose", "up", "-d", "--build", "--no-deps", "--force-recreate", "correlation-service"],
        check=True,
        env=env,
    )
    wait_for_health()


def wait_for_health(timeout_sec=120):
    start = time.time()
    while time.time() - start < timeout_sec:
        try:
            r = requests.get(PLATFORM_HEALTH_URL, timeout=3)
            if r.status_code == 200 and r.json().get("status") == "UP":
                return
        except Exception:
            pass
        time.sleep(2)
    raise TimeoutError("correlation-service did not become healthy")


def run_baseline_once_per_scenario(scenarios):
    print("[baseline] health check...")
    if not ev.health_check():
        raise RuntimeError("health check failed before baseline run")

    print("[baseline] inventory reset + warmup...")
    ev.reset_inventory("PRODUCT-1", 1000)
    for _ in range(ev.WARMUP_RUNS):
        ev.create_order({"customerId": "warmup", "productId": "PRODUCT-1", "quantity": 1, "amount": 10.0})
        time.sleep(1)
    time.sleep(5)

    rows = []
    for scenario in scenarios:
        print(f"[baseline] {scenario['id']}")
        runner = scenario["runner"]
        try:
            result = runner(scenario, 0)
        except Exception as exc:
            result = ev.make_result(scenario, 0)
            result["error"] = str(exc)

        rows.append(
            {
                "profile": "score-free-selector",
                "scenarioId": scenario["id"],
                "predicted_root_cause": result.get("rca_root_cause"),
                "expected_root_cause": scenario.get("expected_rca"),
                "is_correct": bool(result.get("rca_root_cause") == scenario.get("expected_rca")),
                "saga_id": result.get("saga_id"),
                "error": result.get("error"),
                "category": scenario.get("category"),
                "split": scenario.get("split"),
            }
        )
        time.sleep(2)
    return rows


def rerun_rca_for_profile(profile_name, spring_profile, baseline_rows):
    print(f"[profile] switching to {profile_name} ({spring_profile or 'default'})")
    restart_correlation_service(spring_profile)

    rows = []
    for base in baseline_rows:
        scenario_id = base["scenarioId"]
        saga_id = base["saga_id"]
        expected = base["expected_root_cause"]

        predicted = None
        error = None
        if not saga_id:
            error = "missing saga_id from baseline run"
        else:
            try:
                r = requests.post(f"{RCA_URL}/{saga_id}", timeout=15)
                if r.status_code == 200:
                    predicted = r.json().get("rootCause")
                else:
                    error = f"HTTP {r.status_code}"
            except Exception as exc:
                error = str(exc)

        rows.append(
            {
                "profile": profile_name,
                "scenarioId": scenario_id,
                "predicted_root_cause": predicted,
                "expected_root_cause": expected,
                "is_correct": bool(predicted == expected),
                "saga_id": saga_id,
                "error": error,
                "category": base.get("category"),
                "split": base.get("split"),
            }
        )
    return rows


def compute_accuracy(rows):
    by_profile = defaultdict(list)
    for r in rows:
        by_profile[r["profile"]].append(r)
    acc = {}
    for profile, rs in by_profile.items():
        valid = [x for x in rs if not x.get("error")]
        if not valid:
            acc[profile] = {"accuracy_at_1": 0.0, "n": 0, "correct": 0}
            continue
        correct = sum(1 for x in valid if x["is_correct"])
        acc[profile] = {
            "accuracy_at_1": round(correct / len(valid), 4),
            "n": len(valid),
            "correct": correct,
        }
    return acc


def scenario_changes(rows):
    by_scenario = defaultdict(dict)
    for r in rows:
        by_scenario[r["scenarioId"]][r["profile"]] = r["predicted_root_cause"]

    changed = {}
    for scenario_id, profile_map in by_scenario.items():
        preds = {v for v in profile_map.values()}
        if len(preds) > 1:
            changed[scenario_id] = profile_map
    return changed


def main():
    out_dir = Path("results") / "ablation"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Ensure toxiproxy is up for SC07 path.
    subprocess.run(["docker", "compose", "up", "-d", "toxiproxy"], check=True)

    scenarios = all_scenarios()
    print(f"Running baseline on {len(scenarios)} scenarios...")

    # Baseline run generates the saga IDs. Numeric-confidence profile ablation is
    # intentionally disabled in Phase 3+ because RCA selection is score-free.
    restart_correlation_service(None)
    all_rows = run_baseline_once_per_scenario(scenarios)
    baseline_rows = [r for r in all_rows if r["profile"] == "score-free-selector"]

    # Re-score same saga IDs under remaining profiles.
    for profile_name, spring_profile in PROFILES[1:]:
        all_rows.extend(rerun_rca_for_profile(profile_name, spring_profile, baseline_rows))

    accuracy = compute_accuracy(all_rows)
    changed = scenario_changes(all_rows)

    # Multi-rule focus.
    multi_rule_rows = [r for r in all_rows if r["scenarioId"] in MULTI_RULE_SCENARIOS]
    multi_rule_changed = scenario_changes(multi_rule_rows)

    # Persist artifacts.
    with (out_dir / "ablation_rows.json").open("w", encoding="utf-8") as f:
        json.dump(all_rows, f, indent=2)
    with (out_dir / "ablation_accuracy.json").open("w", encoding="utf-8") as f:
        json.dump(accuracy, f, indent=2)
    with (out_dir / "ablation_changed_scenarios.json").open("w", encoding="utf-8") as f:
        json.dump(changed, f, indent=2)
    with (out_dir / "ablation_multi_rule_changed.json").open("w", encoding="utf-8") as f:
        json.dump(multi_rule_changed, f, indent=2)

    with (out_dir / "ablation_rows.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["profile", "scenarioId", "predicted_root_cause", "expected_root_cause", "is_correct",
                        "category", "split", "saga_id", "error"],
        )
        writer.writeheader()
        writer.writerows(all_rows)

    summary = {
        "profiles": [p[0] for p in PROFILES],
        "scenario_count": len(scenarios),
        "row_count": len(all_rows),
        "accuracy_at_1": accuracy,
        "changed_scenarios_count": len(changed),
        "changed_scenarios": sorted(changed.keys()),
        "multi_rule_scenarios": sorted(MULTI_RULE_SCENARIOS),
        "multi_rule_changed_count": len(multi_rule_changed),
        "multi_rule_changed": sorted(multi_rule_changed.keys()),
        "overall_accuracy_mean": round(statistics.mean(v["accuracy_at_1"] for v in accuracy.values()), 4),
    }
    with (out_dir / "ablation_summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("\n=== ABLATION SUMMARY ===")
    print(json.dumps(summary, indent=2))
    print(f"\nArtifacts written to: {out_dir}")


if __name__ == "__main__":
    main()
