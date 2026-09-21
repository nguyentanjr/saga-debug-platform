"""Score an evaluation artifact against the evidence oracle.

Two oracles answer two different questions and this script keeps them apart:

  expected_rca (in evaluate.py)  -- the cause the experimenter injected.
  admissible_claims (oracle/labels/*.yaml) -- what the run's evidence permits concluding.

A run can satisfy one and not the other, and the gap is the finding, not an error to be
smoothed over. Both scores are printed side by side and never combined.

The reporting rules come from docs/ORACLE.md muc 4: four groups counted separately, plus
three rates that only mean anything together. There is deliberately no single headline
number -- a system that abstains from everything and a system that guesses at everything
can share one, and telling them apart is the whole point.

TWO READINGS OF ONE LABEL, also never combined. A platform label carries two claims: the
one its rule's cited evidence establishes, and the one its NAME asserts. DOWNSTREAM_TIMEOUT
cites "step took 72184ms against a declared 5000ms" -- an interval compared to a threshold
-- while its name attributes the delay to a downstream service. Picking one reading is
picking a number, so both are printed:

  doc theo bang chung  (lenient) -- map via each claim's `platform_labels`
  doc theo ten nhan    (strict)  -- a label listed in some claim's `name_asserts` is
                                    scored against THAT deeper claim instead

Reading B is a DECLARED INTERPRETATION, not a measurement: nothing measures what a name
asserts. Its premise -- that "DOWNSTREAM_TIMEOUT" says more than the interval its rule
cites -- is argued in oracle/claims.yaml, and rejecting that premise means reading only A.
What does NOT depend on the choice of target claim: once a name is taken to assert
anything at cause level, every cause-level claim is outside these cases' admissible sets,
so the count is the same whichever one it maps to (ROADMAP F10).

Usage:
    python oracle/score.py results/evaluation_report_settled_20260921.json
    python oracle/score.py <artifact> --labels oracle/labels
    python oracle/score.py <artifact> --labels oracle/sc06.yaml
"""

import argparse
import hashlib
import json
import sys
from pathlib import Path

try:
    import yaml
except ImportError:
    sys.exit("PyYAML required: pip install pyyaml")


# docs/ORACLE.md muc 4. CORRECT and ABSTAIN_CORRECT are both successes; they are counted
# apart because a system that only ever produces the second is not doing the job.
CORRECT = "ket luan dung"
MISS = "bo sot"
ABSTAIN_CORRECT = "tu choi phu hop"
UNWARRANTED = "ket luan khong duoc bao dam"
UNSCORED = "oracle thieu - chua cham"
# Fifth outcome, outside the four groups on purpose: the platform named something that is
# a statement about the OBSERVATIONS, not a cause. Neither a diagnosis nor a refusal, so
# folding it into either would hide what happened.
OBSERVATION_LEVEL = "de cu o muc quan sat"

GROUP_ORDER = [CORRECT, MISS, ABSTAIN_CORRECT, UNWARRANTED, OBSERVATION_LEVEL]

DEFAULT_CLAIMS = "claims.yaml"


def load_runs(artifact):
    """Every scored run in the artifact, keyed by (scenario_id, run_index)."""
    runs = {}

    def walk(node):
        if isinstance(node, dict):
            if "scenario_id" in node and "run_index" in node and "saga_status" in node:
                runs[(node["scenario_id"], node["run_index"])] = node
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(artifact)
    return runs


def claim_for_label(claims, label, strict=False):
    """The claim a platform root-cause name asserts, or None if the vocabulary has none.

    An unmapped label is not scored as wrong. It means the oracle has not said what that
    label claims, which is a gap in the oracle -- reported as such rather than charged to
    the platform.

    With strict=True, a label listed under some claim's `name_asserts` is read as asserting
    that deeper claim: what the NAME says rather than what the rule's evidence supports.
    """
    if strict:
        for name, claim in claims.items():
            if label in (claim.get("name_asserts") or []):
                return name
    for name, claim in claims.items():
        if label in (claim.get("platform_labels") or []):
            return name
        # Labels a fix removed from the platform. Kept so an older artifact still scores
        # against the claim it made, instead of landing in UNSCORED as an unknown name.
        if label in (claim.get("retired_platform_labels") or []):
            return name
    return None


# docs/ORACLE.md muc 2: a case is spec + observation set + scoring moment. All three are
# part of its identity, so all three are compared. An earlier version compared only the
# observations, which let a label file keep applying to a run scored at a different moment
# or under a different workflow -- a DIFFERENT case wearing the same run number
# (ROADMAP L10). The spec is the weak one of the three and says so in CASE_COMPONENTS.
CASE_COMPONENTS = {
    "raw_events": "tap observation",
    "scored_at": "moc cham",
    "workflow_type_at_scoring": "spec dang co hieu luc (chi ten, xem ROADMAP L10)",
}


def validate_evidence(labels, labels_path, runs):
    """Frozen labels apply to one case, not to a reusable run number.

    Raises as soon as any component of the case identity differs, naming the component:
    a mismatch means the labels were written for a different case and must not be reused.
    """
    warnings = []
    if not labels.get("evidence_fixture"):
        return warnings
    path = (Path(labels_path).resolve().parent / labels["evidence_fixture"]).resolve()
    content = path.read_bytes()
    if hashlib.sha256(content).hexdigest() != labels["evidence_fixture_sha256"]:
        raise ValueError("frozen evidence fixture hash differs; labels cannot be applied")
    fixture = json.loads(content)
    for original in fixture["raw_runs"]:
        key = (original["scenario_id"], original["run_index"])
        run = runs.get(key)
        if run is None:
            continue
        for field, description in CASE_COMPONENTS.items():
            # A component the fixture never captured cannot be checked. Do not let that
            # read as "unchanged": older fixtures predate the scoring moment being frozen.
            if field not in original:
                raise ValueError(
                    f"fixture for {key} khong giu '{field}' ({description}); "
                    "dong bang lai bang evidence_view.py --freeze truoc khi cham")
            if original[field] is None:
                # The artifact never recorded this component, so freezing it locked
                # nothing. Say so instead of letting None == None read as "verified".
                warnings.append(f"{key}: '{field}' ({description}) khong co trong artefact goc"
                                " -- thanh phan nay KHONG duoc khoa")
                continue
            if run.get(field) != original[field]:
                raise ValueError(
                    f"{description} khac voi luc dan nhan, o {key}: "
                    f"'{field}' {original[field]!r} -> {run.get(field)!r}; "
                    "day la mot ca khac, phai dan nhan lai")
    return warnings


def score_case(case, run, claims, strict=False):
    """One case against one run. Returns (group, detail)."""
    admissible = set(case.get("admissible_claims") or [])
    observed_only = set(case.get("observations_supported") or [])
    promoted = run.get("rca_root_cause")

    # docs/ORACLE.md muc 3 ties the two together; the field is explicit in the labels so
    # that "must not name a cause" is stated rather than inferred from an empty list.
    abstention_required = case.get("abstention_required")
    if abstention_required is None:
        abstention_required = not admissible

    if promoted is None:
        if abstention_required:
            return ABSTAIN_CORRECT, "bang chung khong do duoc claim nao, platform khong de cu"
        return MISS, f"evidence cho phep {sorted(admissible)}, platform khong de cu gi"

    claim = claim_for_label(claims, promoted, strict=strict)
    if claim is None:
        return UNSCORED, f"nhan '{promoted}' chua co trong tu vung claim -- ORACLE THIEU, khong tinh cho platform"

    if claim in admissible:
        level = claims[claim].get("level", "?")
        return CORRECT, f"{claim} (muc: {level})"

    if claim in observed_only:
        return OBSERVATION_LEVEL, (f"{claim} -- phat bieu ve tap quan sat, khong phai nguyen nhan; "
                                   "khong tinh vao bon nhom")

    # docs/ORACLE.md muc 4: group 4 holds two different mistakes and they need different
    # fixes, so they are never reported as one number.
    reason = (case.get("inadmissible_claims") or {}).get(claim)
    if reason:
        return UNWARRANTED, f"qua muc -- {claim}: {' '.join(str(reason).split())}"
    return UNWARRANTED, f"sai han -- {claim} khong nam trong {sorted(admissible) or '{}'}"


def load_label_files(target):
    """One label file, or every *.yaml in a directory. Returns [(path, parsed)]."""
    path = Path(target)
    if path.is_dir():
        files = sorted(p for p in path.glob("*.yaml"))
        if not files:
            sys.exit(f"khong co file nhan nao trong {target}")
    else:
        files = [path]
    return [(p, yaml.safe_load(p.read_text(encoding="utf-8"))) for p in files]


def claims_for(labels, labels_path):
    """A label file's own vocabulary, or the shared one it points at."""
    if labels.get("claims"):
        return labels["claims"]
    shared = labels.get("claims_from") or DEFAULT_CLAIMS
    path = (Path(labels_path).resolve().parent / shared).resolve()
    return (yaml.safe_load(path.read_text(encoding="utf-8")) or {}).get("claims") or {}


def rates(groups, scored, nominated, informative, title):
    print(f"\n  {title}")
    for name in GROUP_ORDER:
        print(f"    {name:<32} {groups.get(name, 0)}")
    print(f"    {'-' * 44}")
    print(f"    ty le de cu                    : {nominated}/{scored}"
          f"  ({100.0 * nominated / scored:.1f} %)" if scored else "    khong co luot")
    if nominated:
        print(f"    dung trong phan da de cu       : {groups.get(CORRECT, 0)}/{nominated}"
              f"  ({100.0 * groups.get(CORRECT, 0) / nominated:.1f} %)")
    else:
        print("    dung trong phan da de cu       : khong de cu luot nao")
    if informative:
        print(f"    nhan dien tren ca du thong tin : {groups.get(CORRECT, 0)}/{informative}"
              f"  ({100.0 * groups.get(CORRECT, 0) / informative:.1f} %)")
    else:
        print("    nhan dien tren ca du thong tin : khong ca nao du thong tin")


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("artifact", help="evaluation_report*.json to score")
    parser.add_argument("--labels", default="oracle/labels",
                        help="label file, or a directory of them (default: oracle/labels)")
    args = parser.parse_args()

    # Label reasons are written in Vietnamese with diacritics; a Windows console defaults
    # to cp1252 and would abort mid-report. Do not drop the diacritics to fit the console.
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except (AttributeError, ValueError):
        pass

    artifact = json.loads(Path(args.artifact).read_text(encoding="utf-8"))
    runs = load_runs(artifact)
    label_files = load_label_files(args.labels)

    print("=" * 78)
    print("CHAM THEO ORACLE BANG CHUNG")
    print(f"  artefact: {args.artifact}")
    print(f"  nhan    : {args.labels}  ({len(label_files)} file)")
    print("=" * 78)

    totals = {"lenient": {}, "strict": {}}
    scored = nominated = informative = old_oracle_correct = 0
    total_cases = sum(len(labels.get("cases") or []) for _, labels in label_files)
    unscored, missing, defects, lock_warnings = [], [], [], []

    for path, labels in label_files:
        claims = claims_for(labels, path)
        lock_warnings.extend(validate_evidence(labels, path, runs) or [])
        rows = []
        for case in labels.get("cases") or []:
            key = (case["id"], case["run_index"])
            run = runs.get(key)
            if run is None:
                missing.append(key)
                continue
            lenient = score_case(case, run, claims, strict=False)
            strict = score_case(case, run, claims, strict=True)
            if lenient[0] == UNSCORED:
                unscored.append((key, lenient[1]))
                continue
            rows.append((case, run, lenient, strict))

        if not rows:
            continue

        header = (f"{labels.get('labelling')}"
                  f"{'' if labels.get('reviewed_by') else '  -- CHUA co nguoi thu hai ra lai'}")
        print(f"\n{'-' * 78}")
        print(f" {path.name}   gan nhan: {header}")
        if labels.get("amendments"):
            print(f"   (co {len(labels['amendments'])} sua doi sau khi khoa -- doc trong file)")
        for case, run, lenient, strict in rows:
            scored += 1
            promoted = run.get("rca_root_cause")
            if promoted is not None:
                nominated += 1
            if case.get("admissible_claims"):
                informative += 1
            if run.get("rca_accurate"):
                old_oracle_correct += 1
            totals["lenient"][lenient[0]] = totals["lenient"].get(lenient[0], 0) + 1
            totals["strict"][strict[0]] = totals["strict"].get(strict[0], 0) + 1

            print(f"\n   luot {case['run_index']}: platform -> {promoted}")
            print(f"      doc theo bang chung : {lenient[0]}")
            print(f"         {lenient[1]}")
            if strict[0] != lenient[0]:
                print(f"      doc theo ten nhan   : {strict[0]}")
                print(f"         {strict[1]}")
            print(f"      oracle cu (nhan tiem): {'dung' if run.get('rca_accurate') else 'sai'}")

        for defect in labels.get("reasoning_defects") or []:
            defects.append((path.name, defect, [r[0]["run_index"] for r in rows]))

    if unscored:
        print(f"\n{'-' * 78}")
        for key, detail in unscored:
            print(f"  CHUA CHAM {key}: {detail}")
    print(f"\n  coverage oracle: {scored}/{total_cases}; unknown labels: {len(unscored)};"
          f" missing runs: {len(missing)}")
    if not scored:
        sys.exit("khong co luot du dieu kien cham; khong tinh ty le")

    print("\n" + "=" * 78)
    print("BON NHOM VA BA TY LE -- dem rieng, khong gop (ORACLE.md muc 4)")
    print("Hai cach doc mot nhan, cung khong gop: xem docstring dau file.")
    rates(totals["lenient"], scored, nominated, informative,
          "A. DOC THEO BANG CHUNG LUAT VIEN DAN")
    rates(totals["strict"], scored, nominated, informative,
          "B. DOC THEO CHINH TEN NHAN (dien giai da khai bao, khong phai phep do)")

    print("\n" + "-" * 78)
    print("DOI CHIEU VOI ORACLE CU -- hai bo so, khong gop\n")
    print(f"  oracle cu (nhan da tiem)        : {old_oracle_correct}/{scored}")
    print(f"  oracle bang chung, doc bang chung: {totals['lenient'].get(CORRECT, 0)}/{scored}")
    print(f"  oracle bang chung, doc ten nhan : {totals['strict'].get(CORRECT, 0)}/{scored}")

    if defects:
        print("\n" + "-" * 78)
        print("LOI LAP LUAN DA GHI NHAN TREN ARTEFACT GOC -- KHONG TU CHAM BUILD MOI\n")
        for file_name, defect, run_indexes in defects:
            affected = [r for r in defect.get("applies_to_runs") or [] if r in run_indexes]
            print(f"  {file_name}: {defect.get('rule')} / {defect.get('defect')}"
                  f" -- {len(affected)} luot: {affected}")
            print(f"     {' '.join(str(defect.get('detail', '')).split())}")
            print(f"     theo doi o: {defect.get('tracked_as')}")

    if missing:
        print(f"\n  WARNING co nhan nhung khong co trong artefact: {missing}")
    print()


if __name__ == "__main__":
    main()
