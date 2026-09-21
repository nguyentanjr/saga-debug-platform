"""F4 paired replay, using Java production logic, without contacting live services.

python oracle/replay_sc06.py --output results/sc06-before.json \
    --verify-baseline results/evaluation_report_sc06_evidence_20260921.json

After F4, use a NEW output path and omit --verify-baseline (the answer should change).
The same oracle/sc06-observations.json is used on both builds. This is correlation +
inline detection + RCA, NOT Kafka/ingestion/DB/scheduler replay. Captured source_state
is ground truth only. The Java test never reads it or the stored live diagnosis.
"""
import argparse
import hashlib
import json
import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def git(*args):
    return subprocess.check_output(["git", *args], cwd=ROOT).decode("utf-8").strip()


def sources():
    """Hash actual files, including uncommitted code; a commit alone is insufficient."""
    paths = [ROOT / "pom.xml", ROOT / "oracle/sc06-observations.json"]
    for module in ("correlation-service", "saga-events"):
        paths.append(ROOT / module / "pom.xml")
        paths.extend(p for p in (ROOT / module / "src").rglob("*") if p.is_file())
    return {str(p.relative_to(ROOT)).replace("\\", "/"): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(paths)}


def comparable(row):
    # Full evidence explanations were not captured in the original artifact. Compare
    # all candidate fields it DID capture, without pretending that proves text equality.
    fields = ("ruleId", "rootCause", "severity", "status", "anchorAt")
    return (row["saga_status"], row["rca_status"], row["rca_root_cause"],
            row["rca_primary_roots"], sorted(row["anomalies_actual"]),
            [{k: c.get(k) for k in fields} for c in row["rca_candidates"]])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--verify-baseline")
    args = parser.parse_args()
    output = Path(args.output).resolve()
    if output.exists():
        parser.error(f"refusing to overwrite {output}")
    maven = shutil.which("mvn")
    if not maven:
        parser.error("mvn is required on PATH")
    before = sources()
    revision = git("rev-parse", "HEAD")
    output.parent.mkdir(parents=True, exist_ok=True)
    log = output.with_suffix(".log")
    if log.exists():
        parser.error(f"refusing to overwrite {log}")
    with log.open("w", encoding="utf-8") as stream:
        completed = subprocess.run([
            maven, "-pl", "correlation-service", "-am", "test",
            "-Dtest=Sc06ReplayTest", "-Dsurefire.failIfNoSpecifiedTests=false",
            "-Dstyle.color=never", f"-Dsc06.output={output.as_posix()}"],
            cwd=ROOT, stdout=stream, stderr=subprocess.STDOUT)
    if completed.returncode:
        raise SystemExit(f"Replay failed ({completed.returncode}); see {log}")
    report = json.loads(output.read_text(encoding="utf-8"))
    stable = sources() == before and git("rev-parse", "HEAD") == revision
    report["metadata"].update(commit=revision, source_sha256=before,
                               source_stable_during_replay=stable)
    if args.verify_baseline:
        baseline_path = Path(args.verify_baseline)
        baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
        original = {(r["scenario_id"], r["run_index"]): r for r in baseline["raw_runs"]}
        matches = all(comparable(r) == comparable(original[(r["scenario_id"], r["run_index"])])
                      for r in report["raw_runs"])
        report["metadata"].update(baseline_matches=matches,
            baseline_sha256=hashlib.sha256(baseline_path.read_bytes()).hexdigest())
    output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    if not stable or report["metadata"].get("baseline_matches") is False:
        raise SystemExit(f"INVALID comparison: source changed or baseline differs; inspect {output}")
    for row in report["raw_runs"]:
        print(f"run {row['run_index']}: {row['rca_status']} -> {row['rca_root_cause']}")
    print(f"Report: {output}\nLog: {log}")


if __name__ == "__main__":
    main()
