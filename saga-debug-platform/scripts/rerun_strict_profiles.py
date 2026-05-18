import csv
import json
import os
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import evaluate as ev
from run_strict_controlled_ablation import (
    KNOWN_FRAGILE,
    all_scenarios,
    classify_run_error,
)


TARGET_PROFILES = [
    ("score-free-selector", None),
]
TRIALS = 3


def restart_full_stack(profile):
    env = os.environ.copy()
    if profile:
        env["SPRING_PROFILES_ACTIVE"] = profile
    else:
        env.pop("SPRING_PROFILES_ACTIVE", None)

    subprocess.run(
        [
            "docker", "compose", "up", "-d", "--build",
            "order-service", "payment-service", "inventory-service", "correlation-service",
        ],
        check=True,
        env=env,
    )


def wait_health(timeout=180):
    start = time.time()
    while time.time() - start < timeout:
        if ev.health_check():
            return True
        time.sleep(3)
    return False


def warmup():
    ev.reset_inventory("PRODUCT-1", 1000)
    for _ in range(ev.WARMUP_RUNS):
        ev.create_order({"customerId": "warmup", "productId": "PRODUCT-1", "quantity": 1, "amount": 10.0})
        time.sleep(1)
    time.sleep(3)


def main():
    out_dir = Path("results") / "strict-ablation"
    out_dir.mkdir(parents=True, exist_ok=True)
    rows_path = out_dir / "strict_ablation_rows.json"
    if not rows_path.exists():
        raise RuntimeError("strict_ablation_rows.json not found")

    rows = json.loads(rows_path.read_text(encoding="utf-8"))
    scenarios = all_scenarios()

    # Drop old rows for target profiles, keep others.
    rows = [r for r in rows if r["profile"] not in {p[0] for p in TARGET_PROFILES}]

    for profile_name, spring_profile in TARGET_PROFILES:
        for trial_idx in range(TRIALS):
            print(f"[rerun] {profile_name} trial={trial_idx+1}")
            restart_full_stack(spring_profile)
            if not wait_health():
                # mark whole trial invalid
                for s in scenarios:
                    rows.append(
                        {
                            "profile": profile_name,
                            "trial": trial_idx + 1,
                            "scenarioId": s["id"],
                            "category": s.get("category"),
                            "split": s.get("split"),
                            "expected_root_cause": s.get("expected_rca"),
                            "predicted_root_cause": None,
                            "is_correct": False,
                            "saga_id": None,
                            "status": "skipped_invalid",
                            "error": "trial-level skip: stack did not become healthy",
                        }
                    )
                continue

            warmup()

            for scenario in scenarios:
                if scenario["id"] in KNOWN_FRAGILE:
                    rows.append(
                        {
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
                    )
                    continue

                try:
                    result = scenario["runner"](scenario, trial_idx)
                except Exception as exc:
                    result = ev.make_result(scenario, trial_idx)
                    result["error"] = str(exc)

                status = classify_run_error(result)
                rows.append(
                    {
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
                )
                time.sleep(2)

    # Sort and persist
    profile_order = {
        "score-free-selector": 0,
    }
    rows.sort(key=lambda r: (profile_order.get(r["profile"], 99), r["trial"], int(r["scenarioId"].split("_")[0][2:])))

    rows_path.write_text(json.dumps(rows, indent=2), encoding="utf-8")
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
    print("Rerun complete.")


if __name__ == "__main__":
    main()
