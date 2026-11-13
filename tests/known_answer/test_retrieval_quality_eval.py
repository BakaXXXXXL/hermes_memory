"""The fixed offline retrieval-quality evaluation, in the retrieval tier.

``retrieval_quality_eval.py`` beside this file builds a synthetic store with the
known-answer harness and the deterministic hashed stemmed embedder, then scores claim
and evidence answers separately.  The expected values live in
``retrieval_quality_expected.json`` and are the single reviewed regression
oracle; this test runs the evaluation once and fails on the first value that
differs.  The remaining tests state independent behaviour contracts, so a
mistake in the oracle is not silently repeated by the tests that guard it.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent


def _load_evaluation():
    name = "retrieval_quality_eval"
    spec = importlib.util.spec_from_file_location(name, HERE / "retrieval_quality_eval.py")
    module = importlib.util.module_from_spec(spec)
    previous = sys.modules.get(name)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        if previous is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = previous
    return module


@pytest.fixture(scope="module")
def evaluation():
    return _load_evaluation()


@pytest.fixture(scope="module")
def observed(evaluation):
    values, diagnostics = evaluation.run()
    return values, diagnostics


def test_every_retrieval_quality_value_matches(evaluation, observed):
    values, diagnostics = observed
    expected = json.loads((HERE / "retrieval_quality_expected.json").read_text(encoding="utf-8"))
    mismatches = evaluation.differences(expected, values)
    assert not mismatches, (
        "Retrieval-quality values changed: "
        + "; ".join(mismatches)
        + f". Diagnostics: {json.dumps(diagnostics, default=str)}"
    )


def test_expected_file_declares_its_review(evaluation):
    expected = json.loads((HERE / "retrieval_quality_expected.json").read_text(encoding="utf-8"))
    assert expected["_about"].startswith("Regression oracle")
    assert expected["_reviewed_against"]


def test_ranking_metrics_are_split_by_answer_kind(evaluation, observed):
    values, _diagnostics = observed
    metrics = values["metrics"]
    claim_cases = sum(1 for case in evaluation.CASES if case.answer_kind == "claim")
    evidence_cases = sum(1 for case in evaluation.CASES if case.answer_kind == "evidence")
    assert metrics["claim"]["cases"] == claim_cases
    assert metrics["evidence"]["cases"] == evidence_cases
    assert metrics["overall"]["cases"] == claim_cases + evidence_cases
    for block in (metrics["overall"], metrics["claim"], metrics["evidence"]):
        assert 0.0 <= block["hit_at_1"] <= block["hit_at_3"] <= block["hit_at_6"] <= 1.0
        assert block["hit_at_1"] <= block["mrr"] <= block["hit_at_6"]
    assert 0.0 <= metrics["tool_origin_ratio"] <= 1.0


def test_old_versions_history_and_freshness_keep_their_labels(observed):
    values, _diagnostics = observed
    semantics = values["semantics"]
    assert semantics["old_claim_history_present"] is True
    assert semantics["old_claim_history_status"] == "historical"
    assert semantics["old_claim_history_revision"] == 1
    assert semantics["current_head_present"] is True
    assert semantics["current_head_status"] == "current"
    assert semantics["current_head_revision"] == 2
    assert semantics["current_old_claim_present"] is False
    assert semantics["history_current_claim_status"] == "current"
    assert semantics["as_of_old_claim_present"] is True
    assert semantics["as_of_old_claim_status"] == "historical"
    assert semantics["as_of_current_claim_present"] is False
    assert semantics["freshness_current_value"] == "green"
    assert semantics["freshness_current_revision"] == 2
    assert semantics["freshness_current_old_present"] is False
    assert semantics["freshness_history_old_present"] is True
    assert semantics["freshness_history_old_status"] == "historical"
    assert semantics["no_answer_status"] == "no_match"
    assert semantics["no_answer_answerability"] == "unknown"
    assert semantics["no_answer_items"] == 0


def test_precision_negative_cases(observed):
    values, _diagnostics = observed
    precision = values["precision"]
    assert precision["production_distractor_at_1"] is False
    assert precision["production_distractor_before_gold"] is False
    assert precision["paraphrase_production_distractor_before_gold"] is False
    assert precision["payroll_property_visible"] is False


def test_cross_scope_claim_and_vector_are_not_released(observed):
    values, diagnostics = observed
    assert diagnostics["cross_scope_companion_rows"] > 0, "the vector companion held the other-scope rows"
    cross_scope = values["cross_scope"]
    assert cross_scope["companion_rows_present"] is True
    assert cross_scope["allowed_claim_visible"] is True
    assert cross_scope["restricted_question_status"] == "no_match"
    assert cross_scope["restricted_question_answerability"] == "unknown"
    assert cross_scope["restricted_question_items"] == 0
    assert cross_scope["restricted_exact_status"] == "no_match"
    assert cross_scope["restricted_exact_items"] == 0
    assert cross_scope["empty_scopes_error"] == "ACCESS_DENIED"


def test_tool_only_claim_stays_proposed_with_its_lifecycle_reason(observed):
    values, _diagnostics = observed
    tool = values["tool"]
    assert tool["stub_proposes_tool_claim"] is True
    assert tool["candidate_work_queued"] is False
    assert tool["candidate_evaluations"] == 0
    assert tool["candidate_processing_state"] == "waiting_evidence"
    assert tool["candidate_reason"] == "no_derivation_root"
    assert tool["claim_state"] == "proposed"
    assert tool["active_tool_claim_items"] == 0
    assert tool["proposed_tool_claim_items"] == 1
    assert tool["human_control_candidate_work_queued"] is True
    assert tool["human_control_candidate_evaluations"] == 1
    assert tool["human_control_claim_state"] == "active"
    assert tool["human_control_claim_recalled"] is True


def test_position_requires_the_expected_packet_kind(evaluation):
    packet = {
        "items": [
            {"kind": "event", "content": "gold"},
            {"kind": "claim", "content": "gold"},
        ]
    }
    assert evaluation._position(packet, "gold", "event") == 1
    assert evaluation._position(packet, "gold", "claim") == 2
    assert evaluation._position(packet, "missing", "claim") is None


def test_distractor_before_gold_compares_claim_positions(evaluation):
    gold = '"value_text": "6544"'
    distractor = '"value_text": "5432"'
    packet = {
        "items": [
            {"kind": "claim", "content": distractor},
            {"kind": "claim", "content": gold},
        ]
    }
    assert evaluation._distractor_before_gold(packet, gold, distractor) is True
    packet["items"].reverse()
    assert evaluation._distractor_before_gold(packet, gold, distractor) is False
    wrong_kind = {
        "items": [
            {"kind": "event", "content": distractor},
            {"kind": "claim", "content": gold},
        ]
    }
    assert evaluation._distractor_before_gold(wrong_kind, gold, distractor) is False


def test_every_case_is_answered_by_the_expected_kind(evaluation, observed):
    _values, diagnostics = observed
    assert diagnostics["all_cases_answered"] is True
    for name in ("chinese_port", "paraphrase_port", "port_history"):
        assert diagnostics["case_positions"][name] is not None
    for name, row in diagnostics["case_results"].items():
        expected_kind = evaluation.ANSWER_KIND_TO_PACKET_KIND[row["answer_kind"]]
        assert row["matched_kind"] == expected_kind, name


def test_resources_and_modules_are_restored(observed):
    _values, diagnostics = observed
    assert diagnostics["temp_root_removed"] is True
    assert diagnostics["scope_recall_module_restored"] is True


def test_differences_reports_a_changed_value(evaluation):
    found = evaluation.differences({"metrics": {"hit_at_1": 0.5}}, {"metrics": {"hit_at_1": 0.6}})
    assert len(found) == 1
    assert "metrics.hit_at_1" in found[0]


def test_differences_reports_missing_and_unexpected_keys(evaluation):
    found = evaluation.differences({"only_expected": 1}, {"only_observed": 2})
    assert any("only_expected: missing" in item for item in found)
    assert any("only_observed: unexpected" in item for item in found)


def test_differences_uses_float_tolerance(evaluation):
    assert evaluation.differences({"m": 0.818182}, {"m": 0.8181824}) == []
    assert evaluation.differences({"m": 0.818182}, {"m": 0.818183})


def test_differences_does_not_treat_bool_and_int_as_equal(evaluation):
    assert evaluation.differences({"flag": True}, {"flag": 1})
    assert evaluation.differences({"flag": 1}, {"flag": True})
    assert evaluation.differences({"flag": False}, {"flag": 0})
    assert evaluation.differences({"flags": [True]}, {"flags": [1]})
    assert evaluation.differences({"flag": True}, {"flag": True}) == []


def test_differences_ignores_review_metadata(evaluation):
    assert evaluation.differences({"_about": "reviewed", "_reviewed_against": ["rule"]}, {}) == []
