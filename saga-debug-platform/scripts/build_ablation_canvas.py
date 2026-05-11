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
        changed_rows.append([
            scenario_id,
            str(expected.get(scenario_id)),
            str(by[scenario_id].get("confidence-only")),
            str(by[scenario_id].get("confidence-heavy")),
            str(by[scenario_id].get("equal-weight")),
            str(by[scenario_id].get("severity-heavy")),
            str(by[scenario_id].get("evidence-heavy")),
        ])

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

export default function AblationResultsCanvas() {{
  return (
    <Stack gap={{16}}>
      <H1>RCA Ranking Ablation Results</H1>
      <Text>Dataset: 25 scenarios x 5 profiles = 125 profile-scenario evaluations.</Text>
      <Callout tone="warning" title="Important validity caveat">5 scenarios failed to produce saga IDs in baseline (SC03, SC04, SC06, SC16, SC17), so profile accuracy uses n=20 scenarios. Also, profile re-scoring happened over time on the same saga IDs; non-terminal sagas can accumulate scheduled anomalies, which can mimic weight effects.</Callout>

      <Grid columns={{4}} gap={{12}}>
        <Stat label="Profiles" value="5" />
        <Stat label="Scenarios" value="25" />
        <Stat label="Evaluated per profile" value="20" />
        <Stat label="Best Accuracy@1" value="90.0%" />
      </Grid>

      <H2>Accuracy@1 by Profile</H2>
      <Table headers={{["Profile","Correct","Total","Accuracy@1"]}} rows={{accuracyRows}} />

      <Divider />
      <H2>Scenarios with Primary RCA Changes Across Profiles</H2>
      <Table headers={{["Scenario","Expected","confidence-only","confidence-heavy","equal-weight","severity-heavy","evidence-heavy"]}} rows={{changedRows}} />

      <Divider />
      <H2>Multi-rule Scenarios Where Ranking Matters</H2>
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
