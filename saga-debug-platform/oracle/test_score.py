import contextlib
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("score", Path(__file__).with_name("score.py"))
score = importlib.util.module_from_spec(spec)
spec.loader.exec_module(score)


class ScoreTest(unittest.TestCase):
    def setUp(self):
        self.claims = {"KNOWN": {"level": "mechanism", "platform_labels": ["LABEL"]}}

    def test_known_supported_claim_is_correct(self):
        self.assertEqual(score.score_case({"admissible_claims": ["KNOWN"]},
                         {"rca_root_cause": "LABEL"}, self.claims)[0], score.CORRECT)

    def test_abstention_on_informative_case_is_miss(self):
        self.assertEqual(score.score_case({"admissible_claims": ["KNOWN"]},
                         {"rca_root_cause": None}, self.claims)[0], score.MISS)

    def test_abstention_without_identifiable_claim_is_appropriate(self):
        self.assertEqual(score.score_case({"admissible_claims": []},
                         {"rca_root_cause": None}, self.claims)[0], score.ABSTAIN_CORRECT)

    def test_known_but_unsupported_claim_is_unwarranted(self):
        self.assertEqual(score.score_case({"admissible_claims": []},
                         {"rca_root_cause": "LABEL"}, self.claims)[0], score.UNWARRANTED)

    def test_unknown_label_is_not_charged_to_platform(self):
        self.assertEqual(score.score_case({"admissible_claims": ["KNOWN"]},
                         {"rca_root_cause": "NEW_LABEL"}, self.claims)[0], score.UNSCORED)

    def test_unknown_label_is_excluded_from_aggregate_denominators(self):
        labels = {"claims": self.claims, "cases": [
            {"id": "SC", "run_index": i, "admissible_claims": ["KNOWN"]} for i in (1, 2)]}
        artifact = {"raw_runs": [
            {"scenario_id": "SC", "run_index": i, "saga_status": "FAILED", "rca_root_cause": label}
            for i, label in ((1, "LABEL"), (2, "NEW_LABEL"))]}
        with tempfile.TemporaryDirectory() as directory:
            lab = Path(directory) / "labels.yaml"
            data = Path(directory) / "runs.json"
            lab.write_text(score.yaml.safe_dump(labels), encoding="utf-8")
            data.write_text(json.dumps(artifact), encoding="utf-8")
            out = io.StringIO()
            with patch.object(sys, "argv", ["score.py", str(data), "--labels", str(lab)]), contextlib.redirect_stdout(out):
                score.main()
            self.assertIn("coverage oracle: 1/2; unknown labels: 1", out.getvalue())
            self.assertIn("1/1  (100.0 %)", out.getvalue())
            self.assertNotIn("1/2  (50.0 %)", out.getvalue())

    # docs/ORACLE.md muc 2: a case is spec + observations + scoring moment. Each component
    # gets its own regression test, because an earlier validator compared only the
    # observations and a review found labels still applying to a run scored at a different
    # moment under a different workflow name (ROADMAP L10).
    CASE = {"scenario_id": "SC", "run_index": 1,
            "raw_events": [{"payload": "original"}],
            "scored_at": "2026-09-21T11:31:53.689347+00:00",
            "workflow_type_at_scoring": "ORDER_SAGA"}

    @contextlib.contextmanager
    def frozen_case(self, **overrides):
        """A fixture holding CASE, plus the labels dict that points at it."""
        original = {**self.CASE, **overrides}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.json"
            path.write_text(json.dumps({"raw_runs": [original]}), encoding="utf-8")
            labels = {"evidence_fixture": path.name,
                      "evidence_fixture_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
            yield labels, Path(directory) / "labels.yaml", original, path

    def test_unchanged_case_is_accepted(self):
        with self.frozen_case() as (labels, labels_path, original, _):
            score.validate_evidence(labels, labels_path, {("SC", 1): original})

    def test_cli_prints_warning_for_each_uncaptured_component(self):
        for field in score.CASE_COMPONENTS:
            with self.subTest(field=field), self.frozen_case(**{field: None}) as (
                    labels, labels_path, original, _):
                labels.update(claims=self.claims, cases=[
                    {"id": "SC", "run_index": 1, "admissible_claims": []}])
                labels_path.write_text(score.yaml.safe_dump(labels), encoding="utf-8")
                data = labels_path.with_name("runs.json")
                data.write_text(json.dumps({"raw_runs": [
                    {**original, "saga_status": "IN_PROGRESS", "rca_root_cause": None}
                ]}), encoding="utf-8")
                out = io.StringIO()
                with patch.object(sys, "argv", ["score.py", str(data), "--labels",
                                                str(labels_path)]), contextlib.redirect_stdout(out):
                    score.main()
                self.assertIn(f"'{field}'", out.getvalue())
                self.assertIn("KHONG duoc khoa", out.getvalue())

    def test_changed_observations_are_rejected(self):
        with self.frozen_case() as (labels, labels_path, original, _):
            changed = {**original, "raw_events": [{"payload": "changed"}]}
            with self.assertRaisesRegex(ValueError, "tap observation"):
                score.validate_evidence(labels, labels_path, {("SC", 1): changed})

    def test_changed_scoring_moment_is_rejected(self):
        with self.frozen_case() as (labels, labels_path, original, _):
            changed = {**original, "scored_at": "1900-01-01T00:00:00+00:00"}
            with self.assertRaisesRegex(ValueError, "moc cham"):
                score.validate_evidence(labels, labels_path, {("SC", 1): changed})

    def test_changed_workflow_is_rejected(self):
        with self.frozen_case() as (labels, labels_path, original, _):
            changed = {**original, "workflow_type_at_scoring": "DIFFERENT_WORKFLOW"}
            with self.assertRaisesRegex(ValueError, "spec dang co hieu luc"):
                score.validate_evidence(labels, labels_path, {("SC", 1): changed})

    def test_fixture_missing_a_component_is_rejected_not_assumed_unchanged(self):
        """An old fixture predating the lock must fail loudly, not pass by omission."""
        old_style = {"scenario_id": "SC", "run_index": 1, "raw_events": [{"payload": "original"}]}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.json"
            path.write_text(json.dumps({"raw_runs": [old_style]}), encoding="utf-8")
            labels = {"evidence_fixture": path.name,
                      "evidence_fixture_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
            with self.assertRaisesRegex(ValueError, "khong giu 'scored_at'"):
                score.validate_evidence(labels, Path(directory) / "labels.yaml",
                                        {("SC", 1): self.CASE})

    def test_tampered_fixture_is_rejected(self):
        with self.frozen_case() as (labels, labels_path, _, path):
            path.write_text("{}", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "hash differs"):
                score.validate_evidence(labels, labels_path, {})

    # ROADMAP L11: the SC02 amendment note claimed that dropping FORWARD_STEP_FAILED would
    # leave those runs UNSCORED. It does not -- the label stays mapped in the vocabulary,
    # so the run scores UNWARRANTED and the amendment moved 5 runs from unwarranted to
    # correct. Pinned here so the claim cannot drift back.
    def test_dropping_a_claim_from_a_case_is_unwarranted_not_unscored(self):
        case = {"admissible_claims": ["OTHER"], "abstention_required": False}
        run = {"rca_root_cause": "LABEL"}
        self.assertEqual(score.score_case(case, run, self.claims)[0], score.UNWARRANTED)
        self.assertEqual(score.score_case(case, run, {})[0], score.UNSCORED)


if __name__ == "__main__":
    unittest.main()
