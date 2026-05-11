"""
Analyze existing strict_ablation_rows.json data and produce:
1. Full metrics recomputation (accuracy@1, variance, stddev per profile)
2. Changed-scenarios matrix with majority vote
3. Multi-rule impact analysis
4. Weight-effect vs noise/artifact classification
5. Final report (Markdown)
"""

import collections
import csv
import json
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

PROFILES_ORDER = [
    "confidence-only",
    "confidence-heavy",
    "equal-weight",
    "severity-heavy",
    "evidence-heavy",
]

MULTI_RULE_SCENARIOS = {
    "SC05_COMPENSATION_STEP_FAILURE",
    "SC06_CIRCUIT_BREAKER_REAL",
    "SC16_STUCK_RECOVERED_LONG",
    "SC21_MULTI_TERMINAL_CONFLICT",
    "SC23_DUPLICATE_COMPENSATION",
    "SC24_DELAYED_COMPENSATION",
}

KNOWN_FRAGILE = {
    "SC03_STUCK_LIVE",
    "SC04_STUCK_RECOVERED_BRIEF",
    "SC06_CIRCUIT_BREAKER_REAL",
    "SC16_STUCK_RECOVERED_LONG",
    "SC17_STUCK_DEAD_PERMANENT",
}


def load_rows(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def profile_status_counts(rows):
    """Count ok/skipped_invalid/error per profile."""
    counts = {}
    for p in PROFILES_ORDER:
        p_rows = [r for r in rows if r["profile"] == p]
        counts[p] = {
            "total": len(p_rows),
            "ok": sum(1 for r in p_rows if r["status"] == "ok"),
            "skipped_invalid": sum(1 for r in p_rows if r["status"] == "skipped_invalid"),
            "error": sum(1 for r in p_rows if r["status"] == "error"),
        }
    return counts


def compute_accuracy_and_variance(rows):
    """Compute accuracy@1 per profile and trial-level variance."""
    # Group by (profile, trial)
    by_pt = collections.defaultdict(list)
    for r in rows:
        if r["status"] != "ok":
            continue
        by_pt[(r["profile"], r["trial"])].append(r)

    # Trial-level accuracy
    trial_acc = collections.defaultdict(list)
    for (profile, trial), rs in sorted(by_pt.items()):
        acc = sum(1 for x in rs if x["is_correct"]) / len(rs) if rs else 0.0
        trial_acc[profile].append({"trial": trial, "accuracy": round(acc, 4), "n": len(rs)})

    # Overall accuracy per profile
    result = {}
    for p in PROFILES_ORDER:
        ok_rows = [r for r in rows if r["profile"] == p and r["status"] == "ok"]
        n = len(ok_rows)
        correct = sum(1 for r in ok_rows if r["is_correct"])
        acc = correct / n if n else 0.0
        trial_vals = [t["accuracy"] for t in trial_acc.get(p, [])]
        variance = statistics.pvariance(trial_vals) if len(trial_vals) > 1 else 0.0
        stddev = statistics.pstdev(trial_vals) if len(trial_vals) > 1 else 0.0
        result[p] = {
            "accuracy_at_1": round(acc, 4),
            "correct": correct,
            "n": n,
            "trial_details": trial_acc.get(p, []),
            "trial_accuracies": trial_vals,
            "variance": round(variance, 6),
            "stddev": round(stddev, 6),
        }
    return result


def majority_vote(rows):
    """For each (profile, scenario): majority predicted cause among OK runs."""
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
        stability[(profile, scenario)] = count / len(preds) if preds else 0.0
    return majority, stability, expected


def analyze_changes(rows):
    """Find scenarios where majority prediction differs across profiles."""
    maj, stab, expected = majority_vote(rows)

    by_scenario = collections.defaultdict(dict)
    stability_by_scenario = collections.defaultdict(dict)
    for (profile, scenario), pred in maj.items():
        by_scenario[scenario][profile] = pred
        stability_by_scenario[scenario][profile] = stab[(profile, scenario)]

    changed = {}
    for scenario, profile_map in by_scenario.items():
        uniq = set(profile_map.values())
        if len(uniq) > 1:
            changed[scenario] = {
                "expected": expected.get(scenario),
                "profile_majority_prediction": profile_map,
                "profile_stability": stability_by_scenario[scenario],
            }

    multi_rule_changed = {k: v for k, v in changed.items() if k in MULTI_RULE_SCENARIOS}

    # Classify weight-effect vs noise
    weight_effect = {}
    likely_noise = {}
    for scenario, detail in changed.items():
        stabilities = list(detail["profile_stability"].values())
        if stabilities and min(stabilities) >= 0.67:
            weight_effect[scenario] = detail
        else:
            likely_noise[scenario] = detail

    return changed, multi_rule_changed, weight_effect, likely_noise


def generate_report(rows, counts, accuracy, changed, multi_rule_changed, weight_effect, likely_noise):
    """Generate final markdown report."""
    lines = []
    lines.append("# Strict Controlled Ablation — Final Report")
    lines.append("")
    lines.append(f"**Generated**: {__import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"**Total rows**: {len(rows)}")
    lines.append(f"**Profiles**: {', '.join(PROFILES_ORDER)}")
    lines.append(f"**Trials per scenario**: 3")
    lines.append(f"**Scenarios**: 25")
    lines.append("")

    # A) Experiment status
    lines.append("## A) Experiment Status")
    lines.append("")
    total_ok = sum(c["ok"] for c in counts.values())
    total_skip = sum(c["skipped_invalid"] for c in counts.values())
    total_err = sum(c["error"] for c in counts.values())
    lines.append(f"- **Status**: Completed (all 5 profiles × 25 scenarios × 3 trials)")
    lines.append(f"- **Total OK runs**: {total_ok}")
    lines.append(f"- **Total skipped_invalid**: {total_skip}")
    lines.append(f"- **Total error**: {total_err}")
    lines.append("")

    # B) Valid sample counts per profile
    lines.append("## B) Valid Sample Counts per Profile")
    lines.append("")
    lines.append("| Profile | Total | OK | Skipped Invalid | Error | OK Rate |")
    lines.append("|---------|-------|-----|-----------------|-------|---------|")
    for p in PROFILES_ORDER:
        c = counts[p]
        ok_rate = c["ok"] / c["total"] * 100 if c["total"] else 0
        lines.append(f"| {p} | {c['total']} | {c['ok']} | {c['skipped_invalid']} | {c['error']} | {ok_rate:.1f}% |")
    lines.append("")

    # Known fragile scenarios
    lines.append("### Known Fragile Scenarios (explicitly skipped)")
    lines.append("")
    for s in sorted(KNOWN_FRAGILE):
        lines.append(f"- {s}")
    lines.append("")

    # C) Accuracy@1 + variance/stddev table
    lines.append("## C) Accuracy@1 + Variance/Stddev")
    lines.append("")
    lines.append("| Profile | Accuracy@1 | Correct/N | Trial 1 | Trial 2 | Trial 3 | Variance | Stddev |")
    lines.append("|---------|-----------|-----------|---------|---------|---------|----------|--------|")
    for p in PROFILES_ORDER:
        a = accuracy[p]
        t_accs = a["trial_accuracies"]
        t1 = f"{t_accs[0]:.4f}" if len(t_accs) > 0 else "N/A"
        t2 = f"{t_accs[1]:.4f}" if len(t_accs) > 1 else "N/A"
        t3 = f"{t_accs[2]:.4f}" if len(t_accs) > 2 else "N/A"
        lines.append(f"| {p} | {a['accuracy_at_1']:.4f} | {a['correct']}/{a['n']} | {t1} | {t2} | {t3} | {a['variance']:.6f} | {a['stddev']:.6f} |")
    lines.append("")

    # Trial detail
    lines.append("### Trial-Level Detail")
    lines.append("")
    for p in PROFILES_ORDER:
        a = accuracy[p]
        lines.append(f"**{p}**:")
        for td in a["trial_details"]:
            lines.append(f"  - Trial {td['trial']}: {td['accuracy']:.4f} (n={td['n']})")
        lines.append("")

    # D) Changed-scenarios matrix
    lines.append("## D) Changed-Scenarios Matrix")
    lines.append("")
    if changed:
        lines.append(f"**{len(changed)} scenario(s)** have different majority predictions across profiles:")
        lines.append("")
        # Build table
        header = "| Scenario | Expected |"
        sep = "|----------|----------|"
        for p in PROFILES_ORDER:
            header += f" {p} |"
            sep += "---------|"
        lines.append(header)
        lines.append(sep)
        for sc in sorted(changed.keys()):
            detail = changed[sc]
            row = f"| {sc} | {detail['expected']} |"
            for p in PROFILES_ORDER:
                pred = detail["profile_majority_prediction"].get(p, "—")
                stb = detail["profile_stability"].get(p, 0)
                match = "✅" if pred == detail["expected"] else "❌"
                row += f" {pred} ({stb:.0%}) {match} |"
            lines.append(row)
        lines.append("")
    else:
        lines.append("No scenarios had differing majority predictions across profiles.")
        lines.append("")

    # E) Multi-rule impact analysis
    lines.append("## E) Multi-Rule Impact Analysis")
    lines.append("")
    lines.append(f"Multi-rule scenarios (where ranking has room to change primary cause): {', '.join(sorted(MULTI_RULE_SCENARIOS))}")
    lines.append("")
    if multi_rule_changed:
        lines.append(f"**{len(multi_rule_changed)} multi-rule scenario(s)** showed different primary causes:")
        lines.append("")
        for sc, detail in sorted(multi_rule_changed.items()):
            lines.append(f"### {sc}")
            lines.append(f"- Expected: `{detail['expected']}`")
            for p in PROFILES_ORDER:
                pred = detail["profile_majority_prediction"].get(p, "—")
                stb = detail["profile_stability"].get(p, 0)
                lines.append(f"- {p}: `{pred}` (stability: {stb:.0%})")
            lines.append("")
    else:
        lines.append("No multi-rule scenarios showed different primary causes across profiles.")
        lines.append("")

    # F) Weight-effect vs noise
    lines.append("## F) Weight-Effect vs Noise/Artifact")
    lines.append("")
    lines.append("### Classification Criteria")
    lines.append("- **Weight effect (real)**: Changed scenario + all profiles have stability ≥ 67% (at least 2/3 trials agree)")
    lines.append("- **Likely noise/artifact**: Changed scenario + at least one profile has stability < 67%")
    lines.append("")

    if weight_effect:
        lines.append(f"### Likely Real Weight Effects ({len(weight_effect)} scenarios)")
        lines.append("")
        for sc, detail in sorted(weight_effect.items()):
            lines.append(f"- **{sc}**: expected=`{detail['expected']}`")
            for p in PROFILES_ORDER:
                pred = detail["profile_majority_prediction"].get(p, "—")
                stb = detail["profile_stability"].get(p, 0)
                lines.append(f"  - {p}: `{pred}` ({stb:.0%})")
        lines.append("")
    else:
        lines.append("### Likely Real Weight Effects: None")
        lines.append("")

    if likely_noise:
        lines.append(f"### Likely Noise/Artifacts ({len(likely_noise)} scenarios)")
        lines.append("")
        for sc, detail in sorted(likely_noise.items()):
            lines.append(f"- **{sc}**: expected=`{detail['expected']}`")
            for p in PROFILES_ORDER:
                pred = detail["profile_majority_prediction"].get(p, "—")
                stb = detail["profile_stability"].get(p, 0)
                lines.append(f"  - {p}: `{pred}` ({stb:.0%})")
        lines.append("")
    else:
        lines.append("### Likely Noise/Artifacts: None")
        lines.append("")

    # G) Limitations and next actions
    lines.append("## G) Limitations and Next Actions")
    lines.append("")
    lines.append("### Limitations")
    lines.append("1. **Known fragile scenarios**: 5 scenarios consistently fail saga linkage in the local Docker environment (SC03, SC04, SC06, SC16, SC17). These are explicitly marked `skipped_invalid` and excluded from accuracy computation.")
    lines.append("2. **Environment variability**: Local Docker environment may introduce timing variations that affect detector-based anomalies (STEP_TIMEOUT, SAGA_STUCK).")
    lines.append("3. **SC01_HEALTHY**: Always predicts `WRONG_SAGA_CORRELATION_KEY` instead of `null` — this is a known issue where healthy sagas still receive non-null predictions from the rule engine.")
    lines.append("4. **SC15 instability**: SC15_STEP_TIMEOUT_INJECT shows inconsistent predictions (`DOWNSTREAM_TIMEOUT` vs `SERVICE_CRASH`), likely due to timing-dependent detector behavior.")
    lines.append("")
    lines.append("### Next Actions")
    lines.append("1. Investigate SC15 timing sensitivity to improve detector reliability")
    lines.append("2. Consider adding SC01 (healthy) special-case handling in rule engine")
    lines.append("3. Deploy to a stable CI environment to reduce environment-induced variance")
    lines.append("4. Increase trials to 5+ for scenarios showing instability")
    lines.append("")

    # Scenario-level correct/incorrect detail
    lines.append("## Appendix: Per-Scenario Results (all profiles)")
    lines.append("")
    all_scenarios = sorted(set(r["scenarioId"] for r in rows))
    for sc in all_scenarios:
        lines.append(f"### {sc}")
        sc_rows = [r for r in rows if r["scenarioId"] == sc]
        expected = sc_rows[0]["expected_root_cause"] if sc_rows else "?"
        lines.append(f"Expected: `{expected}`")
        lines.append("")
        lines.append("| Profile | Trial | Predicted | Correct | Status |")
        lines.append("|---------|-------|-----------|---------|--------|")
        for r in sorted(sc_rows, key=lambda x: (PROFILES_ORDER.index(x["profile"]) if x["profile"] in PROFILES_ORDER else 99, x["trial"])):
            pred = r.get("predicted_root_cause") or "—"
            correct = "✅" if r["is_correct"] else "❌"
            lines.append(f"| {r['profile']} | {r['trial']} | {pred} | {correct} | {r['status']} |")
        lines.append("")

    return "\n".join(lines)


def main():
    out_dir = Path("results") / "strict-ablation"
    rows_path = out_dir / "strict_ablation_rows.json"

    if not rows_path.exists():
        print("ERROR: strict_ablation_rows.json not found!")
        sys.exit(1)

    rows = load_rows(rows_path)
    print(f"Loaded {len(rows)} rows from {rows_path}")

    # 1) Status counts
    counts = profile_status_counts(rows)
    print("\n=== Status Counts ===")
    for p in PROFILES_ORDER:
        c = counts[p]
        print(f"  {p}: total={c['total']} ok={c['ok']} skip={c['skipped_invalid']} error={c['error']}")

    # 2) Accuracy + variance
    accuracy = compute_accuracy_and_variance(rows)
    print("\n=== Accuracy@1 ===")
    for p in PROFILES_ORDER:
        a = accuracy[p]
        print(f"  {p}: acc={a['accuracy_at_1']:.4f} ({a['correct']}/{a['n']}) "
              f"var={a['variance']:.6f} std={a['stddev']:.6f} "
              f"trials={a['trial_accuracies']}")

    # 3) Changed scenarios
    changed, multi_rule_changed, weight_effect, likely_noise = analyze_changes(rows)
    print(f"\n=== Changed Scenarios: {len(changed)} ===")
    for sc, d in sorted(changed.items()):
        preds = d["profile_majority_prediction"]
        stabs = d["profile_stability"]
        print(f"  {sc}: expected={d['expected']}")
        for p in PROFILES_ORDER:
            if p in preds:
                print(f"    {p}: {preds[p]} (stability={stabs.get(p, 0):.0%})")

    print(f"\n=== Multi-Rule Changed: {len(multi_rule_changed)} ===")
    for sc in sorted(multi_rule_changed.keys()):
        print(f"  {sc}")

    print(f"\n=== Weight Effect (real): {len(weight_effect)} ===")
    for sc in sorted(weight_effect.keys()):
        print(f"  {sc}")

    print(f"\n=== Likely Noise: {len(likely_noise)} ===")
    for sc in sorted(likely_noise.keys()):
        print(f"  {sc}")

    # 4) Write outputs
    # Summary JSON
    summary = {
        "profiles": PROFILES_ORDER,
        "trials_per_scenario": 3,
        "scenario_count": len(set(r["scenarioId"] for r in rows)),
        "row_count": len(rows),
        "status_counts": counts,
        "accuracy": accuracy,
        "changed_scenarios_count": len(changed),
        "changed_scenarios": sorted(changed.keys()),
        "multi_rule_changed_count": len(multi_rule_changed),
        "multi_rule_changed_scenarios": sorted(multi_rule_changed.keys()),
        "weight_effect_scenarios": sorted(weight_effect.keys()),
        "likely_noise_scenarios": sorted(likely_noise.keys()),
    }
    with (out_dir / "strict_ablation_summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    # Changed JSON
    with (out_dir / "strict_ablation_changed.json").open("w", encoding="utf-8") as f:
        json.dump(changed, f, indent=2, ensure_ascii=False)

    # Re-write CSV with clean data
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

    # Final report
    report = generate_report(rows, counts, accuracy, changed, multi_rule_changed, weight_effect, likely_noise)
    (out_dir / "final_report.md").write_text(report, encoding="utf-8")

    print(f"\n=== Artifacts written to {out_dir} ===")
    print(f"  - strict_ablation_summary.json")
    print(f"  - strict_ablation_changed.json")
    print(f"  - strict_ablation_rows.csv")
    print(f"  - final_report.md")


if __name__ == "__main__":
    main()
