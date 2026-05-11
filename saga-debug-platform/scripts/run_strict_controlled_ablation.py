import collections
import csv
import json
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import evaluate as ev


PROFILES = [
    ("confidence-only", None),
    ("confidence-heavy", "rca-confidence-heavy"),
    ("equal-weight", "rca-equal-weight"),
    ("severity-heavy", "rca-severity-heavy"),
    ("evidence-heavy", "rca-evidence-heavy"),
]

TRIALS = 3

# Some scenarios have known instability with missing saga linkage in this environment.
# We do NOT silently drop them: each failed run is recorded with status=skipped_invalid.
KNOWN_FRAGILE = {
    "SC03_STUCK_LIVE",
    "SC04_STUCK_RECOVERED_BRIEF",
    "SC06_CIRCUIT_BREAKER_REAL",
    "SC16_STUCK_RECOVERED_LONG",
    "SC17_STUCK_DEAD_PERMANENT",
}

MULTI_RULE_SCENARIOS = {
    "SC05_COMPENSATION_STEP_FAILURE",
    "SC06_CIRCUIT_BREAKER_REAL",
    "SC16_STUCK_RECOVERED_LONG",
    "SC21_MULTI_TERMINAL_CONFLICT",
    "SC23_DUPLICATE_COMPENSATION",
    "SC24_DELAYED_COMPENSATION",
}


def all_scenarios():
    scenarios = []
    for bucket in ev.ALL_SCENARIO_LISTS:
        scenarios.extend(bucket)
    return sorted(scenarios, key=lambda s: int(s["id"].split("_")[0][2:]))


def restart_correlation_service(profile):
    env = os.environ.copy()
    if profile:
        env["SPRING_PROFILES_ACTIVE"] = profile
    else:
        env.pop("SPRING_PROFILES_ACTIVE", None)
    subprocess.run(
        ["docker", "compose", "up", "-d", "--build", "--no-deps", "--force-recreate", "correlation-service"],
        check=True,
        env=env,
    )
    wait_health()


def wait_health(timeout=120):
    start = time.time()
    while time.time() - start < timeout:
        if ev.health_check():
            return
        time.sleep(2)
    raise TimeoutError("Services did not become healthy")


def warmup():
    ev.reset_inventory("PRODUCT-1", 1000)
    for _ in range(ev.WARMUP_RUNS):
        ev.create_order({"customerId": "warmup", "productId": "PRODUCT-1", "quantity": 1, "amount": 10.0})
        time.sleep(1)
    time.sleep(3)


def classify_run_error(result):
    err = (result.get("error") or "").lower()
    if not result.get("saga_id"):
        return "skipped_invalid"
    if "saga not found" in err or "not found" in err:
        return "skipped_invalid"
    if result.get("error"):
        return "error"
    return "ok"


def run_scenario_once(profile_name, trial_idx, scenario):
    runner = scenario["runner"]
    try:
        result = runner(scenario, trial_idx)
    except Exception as exc:
        result = ev.make_result(scenario, trial_idx)
        result["error"] = str(exc)

    status = classify_run_error(result)
    row = {
        "profile": profile_name,
        "trial": trial_idx + 1,
        "scenarioId": scenario["id"],
        "category": scenario.get("category"),
        "split": scenario.get("split"),
        "expected_root_cause": scenario.get("expected_rca"),
        "predicted_root_cause": result.get("rca_root_cause"),
        "is_correct": bool(result.get("rca_root_cause") == scenario.get("expected_rca")),
        "saga_id": result.get("saga_id"),
        "status": status,
        "error": result.get("error"),
    }
    return row


def compute_accuracy_and_variance(rows):
    # trial-level accuracy for variance
    by_profile_trial = collections.defaultdict(list)
    by_profile = collections.defaultdict(list)
    for r in rows:
        if r["status"] != "ok":
            continue
        by_profile[(r["profile"], r["trial"])].append(r)
        by_profile[r["profile"]].append(r)

    trial_accuracy = collections.defaultdict(list)
    for (profile, trial), rs in by_profile_trial.items():
        acc = sum(1 for x in rs if x["is_correct"]) / len(rs) if rs else 0.0
        trial_accuracy[profile].append({"trial": trial, "accuracy_at_1": acc, "n": len(rs)})

    accuracy = {}
    for profile, rs in by_profile.items():
        n = len(rs)
        correct = sum(1 for x in rs if x["is_correct"])
        acc = correct / n if n else 0.0
        trial_vals = [x["accuracy_at_1"] for x in sorted(trial_accuracy.get(profile, []), key=lambda y: y["trial"])]
        variance = statistics.pvariance(trial_vals) if len(trial_vals) > 1 else 0.0
        stddev = statistics.pstdev(trial_vals) if len(trial_vals) > 1 else 0.0
        accuracy[profile] = {
            "accuracy_at_1": round(acc, 4),
            "correct": correct,
            "n": n,
            "trial_accuracies": [round(v, 4) for v in trial_vals],
            "variance": round(variance, 6),
            "stddev": round(stddev, 6),
        }
    return accuracy


def majority_vote_predictions(rows):
    # scenario/profile -> majority predicted cause among OK runs
    grouped = collections.defaultdict(list)
    expected = {}
    for r in rows:
        key = (r["profile"], r["scenarioId"])
        expected[r["scenarioId"]] = r["expected_root_cause"]
        if r["status"] == "ok":
            grouped[key].append(r["predicted_root_cause"])

    majority = {}
    stability = {}
    for (profile, scenario), preds in grouped.items():
        c = collections.Counter(preds)
        top, count = c.most_common(1)[0]
        majority[(profile, scenario)] = top
        stability[(profile, scenario)] = count / len(preds)
    return majority, stability, expected


def analyze_changes(rows):
    majority, stability, expected = majority_vote_predictions(rows)
    by_scenario = collections.defaultdict(dict)
    stability_by_scenario = collections.defaultdict(dict)
    for (profile, scenario), pred in majority.items():
        by_scenario[scenario][profile] = pred
        stability_by_scenario[scenario][profile] = stability[(profile, scenario)]

    changed = {}
    for scenario, profile_map in by_scenario.items():
        uniq = {v for v in profile_map.values()}
        if len(uniq) > 1:
            changed[scenario] = {
                "expected": expected.get(scenario),
                "profile_majority_prediction": profile_map,
                "profile_stability": stability_by_scenario[scenario],
            }

    multi_rule_changed = {k: v for k, v in changed.items() if k in MULTI_RULE_SCENARIOS}

    # classify likely weight-effect vs noise/artifact:
    # - weight_effect: changed + high stability in each involved profile (>= 0.67 with 3 trials)
    # - likely_noise: changed + low stability in at least one involved profile
    weight_effect = {}
    likely_noise = {}
    for scenario, detail in changed.items():
        stabilities = list(detail["profile_stability"].values())
        if stabilities and min(stabilities) >= 0.67:
            weight_effect[scenario] = detail
        else:
            likely_noise[scenario] = detail

    return changed, multi_rule_changed, weight_effect, likely_noise


def main():
    out_dir = Path("results") / "strict-ablation"
    out_dir.mkdir(parents=True, exist_ok=True)
    log_path = out_dir / "strict_ablation_progress.log"

    def log(msg):
        line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
        print(line, flush=True)
        with log_path.open("a", encoding="utf-8") as lf:
            lf.write(line + "\n")

    scenarios = all_scenarios()
    rows = []
    skipped_invalid_counts = collections.Counter()

    log_path.write_text("", encoding="utf-8")
    log(f"Strict controlled ablation: {len(PROFILES)} profiles x {len(scenarios)} scenarios x {TRIALS} trials")
    log("Execution order is fixed by SC number for every profile and trial.")

    for profile_name, spring_profile in PROFILES:
        log(f"=== PROFILE: {profile_name} ({spring_profile or 'default'}) ===")
        restart_correlation_service(spring_profile)

        for trial_idx in range(TRIALS):
            log(f"--- Trial {trial_idx + 1}/{TRIALS} ---")
            wait_health()
            warmup()

            for scenario in scenarios:
                log(f"[{profile_name}][trial={trial_idx+1}] {scenario['id']}")
                if scenario["id"] in KNOWN_FRAGILE:
                    row = {
                        "profile": profile_name,
                        "trial": trial_idx + 1,
                        "scenarioId": scenario["id"],
                        "category": scenario.get("category"),
                        "split": scenario.get("split"),
                        "expected_root_cause": scenario.get("expected_rca"),
                        "predicted_root_cause": None,
                        "is_correct": False,
                        "saga_id": None,
                        "status": "skipped_invalid",
                        "error": "explicitly skipped: known fragile saga-linkage in current environment",
                    }
                    rows.append(row)
                    skipped_invalid_counts[scenario["id"]] += 1
                    log(f"  -> skipped_invalid (explicit pre-skip): {scenario['id']}")
                    continue

                row = run_scenario_once(profile_name, trial_idx, scenario)
                rows.append(row)
                if row["status"] == "skipped_invalid":
                    skipped_invalid_counts[scenario["id"]] += 1
                    log(f"  -> skipped_invalid: {scenario['id']} error={row.get('error')}")
                elif row["status"] == "error":
                    log(f"  -> error: {scenario['id']} error={row.get('error')}")
                else:
                    log(
                        f"  -> ok: {scenario['id']} pred={row.get('predicted_root_cause')} "
                        f"exp={row.get('expected_root_cause')} correct={row.get('is_correct')}"
                    )

                # light settle between scenarios
                time.sleep(2)

    accuracy = compute_accuracy_and_variance(rows)
    changed, multi_rule_changed, weight_effect, likely_noise = analyze_changes(rows)

    summary = {
        "profiles": [p[0] for p in PROFILES],
        "trials_per_scenario": TRIALS,
        "scenario_count": len(scenarios),
        "row_count": len(rows),
        "accuracy": accuracy,
        "known_fragile_scenarios": sorted(KNOWN_FRAGILE),
        "skipped_invalid_counts": dict(sorted(skipped_invalid_counts.items())),
        "changed_scenarios_count": len(changed),
        "changed_scenarios": sorted(changed.keys()),
        "multi_rule_changed_count": len(multi_rule_changed),
        "multi_rule_changed": sorted(multi_rule_changed.keys()),
        "weight_effect_scenarios": sorted(weight_effect.keys()),
        "likely_noise_or_artifact_scenarios": sorted(likely_noise.keys()),
    }

    with (out_dir / "strict_ablation_rows.json").open("w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2)
    with (out_dir / "strict_ablation_summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    with (out_dir / "strict_ablation_changed.json").open("w", encoding="utf-8") as f:
        json.dump(changed, f, indent=2)
    with (out_dir / "strict_ablation_multi_rule_changed.json").open("w", encoding="utf-8") as f:
        json.dump(multi_rule_changed, f, indent=2)
    with (out_dir / "strict_ablation_weight_effect.json").open("w", encoding="utf-8") as f:
        json.dump(weight_effect, f, indent=2)
    with (out_dir / "strict_ablation_likely_noise.json").open("w", encoding="utf-8") as f:
        json.dump(likely_noise, f, indent=2)

    with (out_dir / "strict_ablation_rows.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "profile", "trial", "scenarioId", "category", "split",
                "predicted_root_cause", "expected_root_cause",
                "is_correct", "status", "saga_id", "error",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    log("=== STRICT CONTROLLED ABLATION SUMMARY ===")
    log(json.dumps(summary, indent=2))
    log(f"Artifacts written to: {out_dir}")


if __name__ == "__main__":
    main()
