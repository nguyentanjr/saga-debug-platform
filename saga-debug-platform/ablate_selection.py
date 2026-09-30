"""E1 — ablation tang chon nguyen nhan (RcaSelector), offline tren bao cao da ghi.

Moi bien the thay DUNG MOT quyet dinh cua selector, giu nguyen tap ung vien ma engine da
sinh trong luot do (rca_candidates, theo thu tu xep hang cua engine). Cham bang chinh
rca_expectation_matches / _strict cua evaluate.py tren ca 27 luot, KE CA cac luot ma ky vong
la khong ket luan (NO_DIAGNOSIS_NEEDED / INSUFFICIENT_EVIDENCE) -- baseline_comparison cua
evaluate.py bo qua cac luot do nen khong do duoc gia tri cua viec tu choi ket luan.

Gioi han (in ra cung ket qua):
  - Chi tang selection. Rule, detector, correlation, identity khong duoc ablate o day:
    tap ung vien la dau ra cua cac tang do va duoc giu co dinh.
  - Bao cao khong luu ClaimSubject. Trong code chi R14 dat OBSERVATION_PROCESS
    (RcaService.ruleOutOfOrderDelivery), nen subject duoc suy tu ruleId; bien the
    "engine" tai hien 100% ket qua da ghi la kiem tra cho suy luan nay.
  - Ket qua la so hoc tren dau vao da ghi, khong phai do live.

Chay: python ablate_selection.py results/evaluation_report_real_post_f18_20260929.json
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from evaluate import (SEVERITY_RANK, rca_expectation_matches,  # noqa: E402
                      rca_expectation_matches_strict)

FALLBACK_RULES = {"R20", "R21"}
OBSERVATION_PROCESS_RULES = {"R14"}


def _first(pool, key=None, subject_guard=True):
    pool = [c for c in pool if c.get("rootCause")]
    if subject_guard:
        pool = [c for c in pool if c.get("ruleId") not in OBSERVATION_PROCESS_RULES]
    if key:
        pool = sorted(pool, key=key)
    return pool[0] if pool else None


def _strong(cands):
    return [c for c in cands if c.get("status") == "STRONG_EVIDENCE"]


# Moi bien the: candidates (thu tu engine) -> (danh sach cause duoc de cu, co doi eligibility khong)
VARIANTS = {
    # Tai hien lai engine tu tap ung vien: STRONG dau tien theo thu tu engine.
    "engine": lambda cs: ([_first(_strong(cs))], False),
    # Bo guard "chi de cu STRONG": nhan ung vien chua bi RULED_OUT dau tien.
    "no_strong_only_guard": lambda cs: (
        [_first([c for c in cs if c.get("status") != "RULED_OUT"])], True),
    # Bo guard ClaimSubject: ung vien noi ve qua trinh quan sat (R14) duoc de cu.
    "no_subject_guard": lambda cs: ([_first(_strong(cs), subject_guard=False)], False),
    # Bo them loai tru fallback R20/R21: ung vien fallback bi RULED_OUT chi vi exclusion
    # duoc tra lai trang thai WEAK, roi de cu nhu tren.
    "no_guard_no_fallback_exclusion": lambda cs: (
        [_first([c for c in cs
                 if c.get("status") != "RULED_OUT" or c.get("ruleId") in FALLBACK_RULES])], True),
    # Giu eligibility, doi thu tu xep hang trong tap STRONG.
    "rank_severity_first": lambda cs: ([_first(_strong(cs), key=lambda c: (
        -SEVERITY_RANK.get(c.get("severity"), 0), c.get("ruleId") or "~"))], False),
    "rank_rule_id": lambda cs: ([_first(_strong(cs), key=lambda c: c.get("ruleId") or "~")], False),
    "rank_first_anchor_time": lambda cs: ([_first(_strong(cs), key=lambda c: (
        c.get("anchorAt") or "9999", c.get("ruleId") or "~"))], False),
    # Bo quyet dinh "mot nguyen nhan chinh": cong bo moi ung vien STRONG.
    "publish_all_strong": lambda cs: (
        [c for c in _strong(cs) if c.get("rootCause")
         and c.get("ruleId") not in OBSERVATION_PROCESS_RULES] or [None], False),
}


def apply_variant(run, name):
    picked, widens = VARIANTS[name](run.get("rca_candidates") or [])
    picked = [c for c in picked if c is not None]
    roots = []
    for c in picked:
        if c["rootCause"] not in roots:
            roots.append(c["rootCause"])
    result = dict(run)
    result["rca_primary_roots"] = roots
    result["rca_root_cause"] = roots[0] if roots else None
    if widens and roots and run.get("rca_status") != "EVIDENCE_AVAILABLE":
        # Mot selector ten mot nguyen nhan tu bang chung WEAK se bao co bang chung.
        result["rca_status"] = "EVIDENCE_AVAILABLE"
    return result


def scenario_of(run):
    return {
        "expected_rca": run.get("rca_expected"),
        "expected_rca_status": run.get("rca_expected_status"),
        "expected_rca_excludes": run.get("rca_expected_excludes") or [],
    }


def ablate(report):
    runs = [r for r in report["raw_runs"] if not r.get("error")]
    out = {"runs": len(runs), "variants": {}, "changed_runs": {}}

    # Kiem tra tai hien: bien the "engine" phai cho dung ket qua da ghi tren moi luot.
    mismatch = [(r["scenario_id"], r["run_index"]) for r in runs
                if apply_variant(r, "engine")["rca_primary_roots"] != (r.get("rca_primary_roots") or [])]
    out["engine_reproduction_mismatches"] = mismatch

    for name in VARIANTS:
        lenient = strict = 0
        changed = []
        for r in runs:
            v = apply_variant(r, name)
            sc = scenario_of(r)
            ok_l = rca_expectation_matches(v, sc)
            ok_s = rca_expectation_matches_strict(v, sc)
            lenient += ok_l
            strict += ok_s
            if ok_s != bool(r.get("rca_accurate_strict")) or v["rca_primary_roots"] != (r.get("rca_primary_roots") or []):
                changed.append({
                    "scenario_id": r["scenario_id"], "run_index": r["run_index"],
                    "expected": r.get("rca_expected"), "expected_status": r.get("rca_expected_status"),
                    "engine": r.get("rca_primary_roots"), "variant": v["rca_primary_roots"],
                    "engine_strict_ok": bool(r.get("rca_accurate_strict")), "variant_strict_ok": ok_s,
                })
        n = len(runs)
        out["variants"][name] = {
            "lenient": lenient, "strict": strict,
            "lenient_pct": round(100.0 * lenient / n, 1), "strict_pct": round(100.0 * strict / n, 1),
        }
        out["changed_runs"][name] = changed
    return out


def main():
    paths = sys.argv[1:]
    if not paths:
        print(__doc__)
        sys.exit(2)
    all_out = {}
    for path in paths:
        with open(path, encoding="utf-8") as f:
            res = ablate(json.load(f))
        all_out[os.path.basename(path)] = res
        print(f"\n== {path}  ({res['runs']} luot)")
        if res["engine_reproduction_mismatches"]:
            print("  CANH BAO: engine khong tai hien duoc tren", res["engine_reproduction_mismatches"])
        base = res["variants"]["engine"]
        print(f"  {'bien the':34} {'lenient':>12} {'strict':>12}  luot doi ket luan")
        for name, v in res["variants"].items():
            d = v["strict"] - base["strict"]
            print(f"  {name:34} {v['lenient']:>3} ({v['lenient_pct']:5.1f}%) {v['strict']:>3} ({v['strict_pct']:5.1f}%)"
                  f"  {len(res['changed_runs'][name]):>2}  (strict {d:+d})")
    out_path = os.path.join("results", "ablation_selection_e1.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(all_out, f, ensure_ascii=False, indent=2)
    print(f"\nGhi: {out_path}")


if __name__ == "__main__":
    main()
