import collections
import json
from pathlib import Path


def main():
    workspace_root = Path(r"D:/KLTN/Code/saga-debug-platform")
    canvas_root = Path(r"C:/Users/Lenovo/.cursor/projects/d-KLTN-Code-saga-debug-platform")
    rows = json.loads((workspace_root / "results/ablation/ablation_rows.json").read_text(encoding="utf-8"))
    summary = json.loads((workspace_root / "results/ablation/ablation_summary.json").read_text(encoding="utf-8"))
    profiles = summary["profiles"]

    accuracy_rows = []
    for p in profiles:
        a = summary["accuracy_at_1"][p]
        accuracy_rows.append([p, str(a["correct"]), str(a["n"]), f"{a['accuracy_at_1'] * 100:.1f}%"])

    by = collections.defaultdict(dict)
    expected = {}
    for r in rows:
        by[r["scenarioId"]][r["profile"]] = r.get("predicted_root_cause")
        expected[r["scenarioId"]] = r.get("expected_root_cause")

    changed_rows = []
    for scenario_id in sorted(summary["changed_scenarios"]):
        changed_rows.append(
            [scenario_id, str(expected.get(scenario_id))]
            + [str(by[scenario_id].get(profile)) for profile in profiles]
        )

    changed_multi_rule = set(summary["multi_rule_changed"])
    multi_rule_rows = []
    for scenario_id in summary["multi_rule_scenarios"]:
        multi_rule_rows.append([scenario_id, "yes" if scenario_id in changed_multi_rule else "no"])

    ablation_rows = []
    for r in rows:
        ablation_rows.append([
            r["profile"],
            r["scenarioId"],
            str(r.get("predicted_root_cause")),
            str(r.get("expected_root_cause")),
            "true" if r.get("is_correct") else "false",
        ])

    def js(value):
        return json.dumps(value, ensure_ascii=True)

    content = f"""import {{ Callout, Divider, Grid, H1, H2, Stack, Stat, Table, Text }} from 'cursor/canvas';

const accuracyRows = {js(accuracy_rows)};
const changedRows = {js(changed_rows)};
const multiRuleRows = {js(multi_rule_rows)};
const ablationRows = {js(ablation_rows)};
const changedHeaders = {js(["Scenario", "Expected"] + profiles)};

export default function AblationResultsCanvas() {{
  return (
    <Stack gap={{16}}>
      <H1>Score-Free RCA Selection Results</H1>
      <Text>Dataset uses the score-free selector profile. Numeric confidence-weight ablation is disabled because Phase 3+ no longer ranks RCA by confidence numbers.</Text>
      <Callout tone="warning" title="Validity caveat">Profile re-scoring is no longer meaningful for confidence weights. Use this canvas for selector accuracy and scenario-level result inspection.</Callout>

      <Grid columns={{4}} gap={{12}}>
        <Stat label="Profiles" value={js(str(len(profiles)))} />
        <Stat label="Scenarios" value="25" />
        <Stat label="Evaluated per profile" value="see table" />
        <Stat label="Best Accuracy@1" value={js(accuracy_rows[0][3] if accuracy_rows else "N/A")} />
      </Grid>

      <H2>Accuracy@1</H2>
      <Table headers={{["Profile","Correct","Total","Accuracy@1"]}} rows={{accuracyRows}} />

      <Divider />
      <H2>Scenarios with Primary RCA Changes Across Profiles</H2>
      <Table headers={{changedHeaders}} rows={{changedRows}} />

      <Divider />
      <H2>Multi-rule Scenarios</H2>
      <Table headers={{["Scenario","Primary changed across profiles?"]}} rows={{multiRuleRows}} />

      <Divider />
      <H2>Structured Result Table</H2>
      <Text>Columns exactly as requested: (profile, scenarioId, predicted_root_cause, expected_root_cause, is_correct).</Text>
      <Table headers={{["profile","scenarioId","predicted_root_cause","expected_root_cause","is_correct"]}} rows={{ablationRows}} />
      <Text tone="secondary" size="small">Raw artifacts also saved at results/ablation/ablation_rows.csv and results/ablation/ablation_rows.json.</Text>
    </Stack>
  );
}}
"""

    target = canvas_root / "canvases" / "rca-ranking-ablation-results.canvas.tsx"
    target.write_text(content, encoding="utf-8")
    print(str(target))


if __name__ == "__main__":
    main()
