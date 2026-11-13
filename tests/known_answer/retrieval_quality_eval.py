#!/usr/bin/env python3
"""Fixed, offline retrieval-quality evaluation over synthetic memory.

The questions and the expected metrics are a regression oracle: each value was
reviewed against the retrieval contract and the named case, not accepted only
because a run produced it.  Every run starts from an empty temporary store and
uses the deterministic hashed stemmed embedder and the known-answer harness beside this
file, so the numbers can be re-derived on any machine without a model, network
or API key.

Ranking is reported separately for claim answers and evidence (event) answers:

* Hit@1, Hit@3, Hit@6 -- the share of questions whose first answer is within
  the first 1, 3 or 6 returned items;
* MRR -- the mean over every answerable question of 1/rank, counting an
  unanswered question as 0;
* tool-origin share -- unique returned refs whose origin is tool output, across
  all answerable packets.  Tool output may answer a question as an event; it is
  never served as an active claim.

A case is matched only by the packet kind that can answer it (a claim case by a
claim item, an evidence case by an event item), and the kind that matched is
reported per case.  Precision checks both the exact and the paraphrased staging
question, and the tool-only control reports the candidate's lifecycle reason
beside a human-backed positive control.

The evaluation also pins the behaviour that is not a ranking score: a revised
claim keeps its old version as history, ``history`` and ``as_of`` can still
return that version, a newer source revision replaces the older one, a
cross-scope question cannot see another scope's claim or vector row, an absent
answer is a ``no_match`` with ``answerability`` ``unknown``, and a tool-only
proposal stays proposed rather than being evaluated or promoted.

Usage, from the source tree::

    python tests/known_answer/retrieval_quality_eval.py
    python tests/known_answer/retrieval_quality_eval.py --json observed.json

Exit code 0 when every observed value matches the expected file, 1 otherwise.
"""

from __future__ import annotations

import argparse
import importlib
import importlib.util
import json
import math
import shutil
import sys
import tempfile
import types
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parents[1]
SCOPE = "repro-scope"
OTHER_SCOPE = "other-scope"
PROJECT = "orchard"
HIT_KS = (1, 3, 6)
#: The packet item kind that can answer each case kind.  A raw event may contain
#: the same words as a claim and a claim may contain the same words as an event,
#: so a substring match alone does not prove that the right kind answered.
ANSWER_KIND_TO_PACKET_KIND = {"claim": "claim", "evidence": "event"}

#: Every fact sentence the stub model proposes a claim for.  The tool sentence
#: is included on purpose: if tool output ever reaches the model, the stub must
#: propose it, and the tool-only control below will notice a promotion.
FACT_SENTENCES = {
    "For project Orchard the staging database port is 6543.": ("Orchard", "staging database port", "6543"),
    "For project Orchard the release manager is Dana Whitfield.": ("Orchard", "release manager", "Dana Whitfield"),
    "For project Orchard the backup bucket is cold-archive-7.": ("Orchard", "backup bucket", "cold-archive-7"),
    "For project Orchard the production database port is 5432.": (
        "Orchard",
        "production database port",
        "5432",
    ),
    "Orchard 的预发布数据库端口是 6545。": ("Orchard", "预发布数据库端口", "6545"),
    "For project Orchard the build artifact is build-4711.": ("Orchard", "build artifact", "build-4711"),
    "For project Orchard the private launch code is quartz-orchid.": (
        "Orchard",
        "private launch code",
        "quartz-orchid",
    ),
    "For project Orchard the deployment status is amber.": ("Orchard", "deployment status", "amber"),
    "For project Orchard the deployment status is green.": ("Orchard", "deployment status", "green"),
}


def _claim_value(value: str) -> str:
    """The exact canonical-JSON fragment that identifies a claim answer."""
    return f'"value_text": "{value}"'


@dataclass(frozen=True)
class Case:
    name: str
    query: str
    gold: str
    answer_kind: str
    mode: str = "current"
    as_of: str | None = None

    @property
    def packet_kind(self) -> str:
        return ANSWER_KIND_TO_PACKET_KIND[self.answer_kind]


CASES = (
    Case("port_current", "What is the Orchard staging database port?", _claim_value("6544"), "claim"),
    Case("manager_current", "Who is the Orchard release manager?", _claim_value("Dana Whitfield"), "claim"),
    Case("bucket_current", "What is the Orchard backup bucket?", _claim_value("cold-archive-7"), "claim"),
    Case("chinese_port", "Orchard 的预发布数据库端口是多少？", _claim_value("6545"), "claim"),
    Case("paraphrase_port", "Where does Orchard stage its database?", _claim_value("6544"), "claim"),
    Case(
        "port_history",
        "What is the Orchard staging database port?",
        _claim_value("6543"),
        "claim",
        mode="history",
    ),
    Case(
        "port_as_of",
        "What is the Orchard staging database port?",
        _claim_value("6543"),
        "claim",
        mode="as_of",
        as_of="2026-10-01T12:00:00Z",
    ),
    Case("window", "When is the Orchard deployment window?", "Friday at 22:00", "evidence"),
    Case("rollback", "Who owns the Orchard rollback?", "on-call engineer", "evidence"),
    Case("deadline", "What is the Orchard migration deadline?", "2026-12-01", "evidence"),
    Case("build_tool", "What is the Orchard build artifact?", "build-4711", "evidence"),
    Case("incident", "What caused the Orchard incident?", "expired TLS certificate", "evidence"),
    Case("freshness", "What is the Orchard deployment status?", "green", "evidence"),
)


@contextmanager
def _source_modules():
    """Temporarily expose this checkout as ``scope_recall`` and restore the previous modules.

    Loading a source copy by module name must not depend on whether some other
    import already put an installed copy in ``sys.modules``, and it must not
    leave a synthetic package behind for the next test.
    """
    previous = {
        name: module
        for name, module in sys.modules.items()
        if name == "scope_recall" or name.startswith("scope_recall.")
    }
    for name in previous:
        del sys.modules[name]
    package = types.ModuleType("scope_recall")
    package.__path__ = [str(REPO_ROOT)]
    sys.modules["scope_recall"] = package
    try:
        yield
    finally:
        for name in [name for name in sys.modules if name == "scope_recall" or name.startswith("scope_recall.")]:
            del sys.modules[name]
        sys.modules.update(previous)


def _load_known_answer():
    name = "recall_invariants_check"
    spec = importlib.util.spec_from_file_location(name, HERE / "recall_invariants_check.py")
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


def _matching_item(packet: dict, gold: str, packet_kind: str) -> dict | None:
    return next(
        (item for item in packet["items"] if item.get("kind") == packet_kind and gold in (item.get("content") or "")),
        None,
    )


def _position(packet: dict, gold: str, packet_kind: str) -> int | None:
    return next(
        (
            position
            for position, item in enumerate(packet["items"], 1)
            if item.get("kind") == packet_kind and gold in (item.get("content") or "")
        ),
        None,
    )


def _claim_item(packet: dict, gold: str) -> dict | None:
    return _matching_item(packet, gold, "claim")


def _distractor_before_gold(packet: dict, gold: str, distractor: str, packet_kind: str = "claim") -> bool:
    gold_position = _position(packet, gold, packet_kind)
    distractor_position = _position(packet, distractor, packet_kind)
    return distractor_position is not None and (gold_position is None or distractor_position < gold_position)


def _say_revision(harness, key: str, revision: int, text: str, when: str, origin: str = "human_direct"):
    """Capture a second revision of one source key, as an updated report would."""
    role = {"human_direct": "user", "assistant_visible": "assistant", "tool_observation": "tool"}[origin]
    event = dict(
        protocol_version="1.1",
        source_event_key=key,
        source_revision=revision,
        origin=origin,
        role=role,
        content=text,
        occurred_at=when,
        recorded_at=when,
        time_precision="instant",
        capture_state="complete",
        evidence_refs=[],
    )
    context = harness.ctx("session-A", origin)
    receipt = harness.core.record_event(context, event, scope_id=SCOPE, remaining_seconds=10)
    return harness.core.source(harness.ctx("session-A"), receipt.event_refs[0].ref, revision)


def _populate(harness) -> dict:
    """Capture the synthetic corpus; the stub model proposes a claim for every fact sentence."""
    refs = {
        "port": harness.say("session-A", "For project Orchard the staging database port is 6543."),
        "manager": harness.say("session-A", "For project Orchard the release manager is Dana Whitfield."),
        "bucket": harness.say("session-A", "For project Orchard the backup bucket is cold-archive-7."),
        "production": harness.say("session-A", "For project Orchard the production database port is 5432."),
        "chinese": harness.say("session-A", "Orchard 的预发布数据库端口是 6545。"),
        "window": harness.say(
            "session-A",
            "For project Orchard the deployment window is Friday at 22:00.",
            when="2026-10-01T10:00:00Z",
        ),
        "rollback": harness.say(
            "session-A",
            "For project Orchard the rollback owner is the on-call engineer.",
            when="2026-10-01T10:01:00Z",
        ),
        "deadline": harness.say(
            "session-A",
            "For project Orchard the migration deadline is 2026-12-01.",
            when="2026-10-01T10:02:00Z",
        ),
        "build": harness.say(
            "session-A",
            "For project Orchard the build artifact is build-4711.",
            origin="tool_observation",
            when="2026-10-01T10:03:00Z",
        ),
        "incident": harness.say(
            "session-A",
            "For project Orchard the incident cause was an expired TLS certificate.",
            when="2026-10-01T10:04:00Z",
        ),
        "status_old": _say_revision(
            harness,
            "eval/freshness",
            1,
            "For project Orchard the deployment status is amber.",
            "2026-10-01T10:05:00Z",
        ),
    }
    harness.drain(harness.ctx("session-A"))
    # Drain the first status before recording the second revision, so the old
    # value becomes a claim version instead of a superseded source event.
    refs["status_new"] = _say_revision(
        harness,
        "eval/freshness",
        2,
        "For project Orchard the deployment status is green.",
        "2026-10-05T10:05:00Z",
    )
    harness.drain(harness.ctx("session-A"))
    return refs


def _revise_port(harness, ctx) -> str:
    """Revise the port claim with the person's own in-session request."""
    packet = harness.recall(ctx, "What is the Orchard staging database port?", max_items=6)
    claim = _claim_item(packet, _claim_value("6543"))
    if claim is None:
        raise AssertionError("the port claim is missing before the revision")
    request = harness.say(
        "session-A",
        "Please correct Orchard staging database port: 6544.",
        when="2026-10-02T09:00:00Z",
    )
    head = harness.core.current_claim(ctx, claim["ref"]).revision
    harness.core.revise(
        ctx,
        dict(
            protocol_version="1.1",
            target_ref=claim["ref"],
            expected_revision=head,
            new_value="6544",
            conditions=[],
            source_evidence_refs=[f"{request.ref}@{request.revision}"],
            valid_from=request.event["occurred_at"],
        ),
        remaining_seconds=10,
    )
    harness.drain(ctx)
    return claim["ref"]


def _metric_block(positions: list[int | None]) -> dict:
    return {
        "cases": len(positions),
        **{
            f"hit_at_{k}": round(sum(1 for position in positions if position and position <= k) / len(positions), 6)
            for k in HIT_KS
        },
        "mrr": round(sum(1 / position for position in positions if position) / len(positions), 6),
    }


def _ranking(harness, ctx) -> tuple[dict, dict]:
    """Run the answerable cases and return split metrics plus per-case diagnostics."""
    packets = []
    positions: dict[str, int | None] = {}
    case_rows: dict[str, dict] = {}
    for case in CASES:
        packet = harness.recall(ctx, case.query, mode=case.mode, as_of=case.as_of, max_items=6)
        packets.append(packet)
        matched = _matching_item(packet, case.gold, case.packet_kind)
        positions[case.name] = _position(packet, case.gold, case.packet_kind)
        case_rows[case.name] = {
            "answer_kind": case.answer_kind,
            "matched_kind": matched.get("kind") if matched else None,
            "status": packet["status"],
            "answerability": packet["answerability"],
        }
    claim_positions = [positions[case.name] for case in CASES if case.answer_kind == "claim"]
    evidence_positions = [positions[case.name] for case in CASES if case.answer_kind == "evidence"]
    unique = {}
    for packet in packets:
        for item in packet["items"]:
            unique[(item["ref"], item["revision"])] = item.get("origin")
    tool_items = sum(1 for origin in unique.values() if origin == "tool_observation")
    metrics = {
        "overall": _metric_block(list(positions.values())),
        "claim": _metric_block(claim_positions),
        "evidence": _metric_block(evidence_positions),
        "tool_origin_ratio": round(tool_items / len(unique), 6) if unique else 0.0,
    }
    diagnostics = {
        "case_positions": positions,
        "case_results": case_rows,
        "unique_items": len(unique),
        "tool_origin_items": tool_items,
        "all_cases_answered": all(position is not None for position in positions.values()),
    }
    return metrics, diagnostics


def _semantics(harness, ctx) -> dict:
    """Old versions, history/as_of and the current head of the revised port claim."""
    query = "What is the Orchard staging database port?"
    history = harness.recall(ctx, query, mode="history", max_items=6)
    as_of = harness.recall(ctx, query, mode="as_of", as_of="2026-10-01T12:00:00Z", max_items=6)
    current = harness.recall(ctx, query, mode="current", max_items=6)
    old_history = _claim_item(history, _claim_value("6543"))
    new_history = _claim_item(history, _claim_value("6544"))
    old_as_of = _claim_item(as_of, _claim_value("6543"))
    new_as_of = _claim_item(as_of, _claim_value("6544"))
    head = _claim_item(current, _claim_value("6544"))
    old_current = _claim_item(current, _claim_value("6543"))

    fresh_query = "What is the Orchard deployment status?"
    fresh_current = harness.recall(ctx, fresh_query, max_items=6)
    fresh_history = harness.recall(ctx, fresh_query, mode="history", max_items=6)
    current_green = _claim_item(fresh_current, "green")
    current_amber = _claim_item(fresh_current, "amber")
    history_amber = _claim_item(fresh_history, "amber")

    no_answer = harness.recall(ctx, "zebra quantum flux", max_items=6)
    return {
        "old_claim_history_present": old_history is not None,
        "old_claim_history_status": old_history.get("temporal_status") if old_history else None,
        "old_claim_history_revision": old_history.get("revision") if old_history else None,
        "current_head_present": head is not None,
        "current_head_status": head.get("temporal_status") if head else None,
        "current_head_revision": head.get("revision") if head else None,
        "current_old_claim_present": old_current is not None,
        "history_current_claim_status": new_history.get("temporal_status") if new_history else None,
        "as_of_old_claim_present": old_as_of is not None,
        "as_of_old_claim_status": old_as_of.get("temporal_status") if old_as_of else None,
        "as_of_current_claim_present": new_as_of is not None,
        "freshness_current_value": "green" if current_green else None,
        "freshness_current_revision": current_green.get("revision") if current_green else None,
        "freshness_current_old_present": current_amber is not None,
        "freshness_history_old_present": history_amber is not None,
        "freshness_history_old_status": history_amber.get("temporal_status") if history_amber else None,
        "no_answer_status": no_answer["status"],
        "no_answer_answerability": no_answer["answerability"],
        "no_answer_items": len(no_answer["items"]),
    }


def _precision(harness, ctx) -> dict:
    """A related distractor must not outrank the gold claim; an absent property must not be invented."""
    staging = harness.recall(ctx, "What is the Orchard staging database port?", max_items=6)
    paraphrase_query = next(case.query for case in CASES if case.name == "paraphrase_port")
    paraphrase = harness.recall(ctx, paraphrase_query, max_items=6)
    payroll = harness.recall(ctx, "What is the Orchard payroll provider?", max_items=6)
    return {
        "production_distractor_at_1": _position(staging, _claim_value("5432"), "claim") == 1,
        "production_distractor_before_gold": _distractor_before_gold(
            staging, _claim_value("6544"), _claim_value("5432")
        ),
        "paraphrase_production_distractor_before_gold": _distractor_before_gold(
            paraphrase, _claim_value("6544"), _claim_value("5432")
        ),
        "_paraphrase_positions": {
            "gold": _position(paraphrase, _claim_value("6544"), "claim"),
            "production_distractor": _position(paraphrase, _claim_value("5432"), "claim"),
        },
        "payroll_property_visible": any("payroll" in (item.get("content") or "").lower() for item in payroll["items"]),
    }


def _cross_scope(root: Path, known) -> tuple[dict, dict]:
    """A claim and its vector row in one scope must be invisible from another scope."""
    root.mkdir(parents=True, exist_ok=True)
    harness = None
    try:
        harness = known.Harness(
            root,
            scopes=frozenset({SCOPE, OTHER_SCOPE}),
            project_id=PROJECT,
            model=known.StubConsolidation(FACT_SENTENCES),
        )
        ctx = harness.ctx("session-A")
        secret = harness.say(
            "session-A",
            "For project Orchard the private launch code is quartz-orchid.",
            scope_id=OTHER_SCOPE,
            when="2026-10-01T10:06:00Z",
        )
        harness.drain(ctx)
        companion_rows = len(harness.store.list_records())
        allowed = harness.ctx("session-A", scopes=frozenset({OTHER_SCOPE}))
        restricted = harness.ctx("session-A", scopes=frozenset({SCOPE}))
        allowed_packet = harness.recall(allowed, "What is the Orchard private launch code?", max_items=6)
        restricted_question = harness.recall(restricted, "What is the Orchard private launch code?", max_items=6)
        restricted_exact = harness.recall(restricted, "quartz-orchid", max_items=6)
        try:
            harness.recall(harness.ctx("session-A", scopes=frozenset()), "anything", max_items=6)
            empty_error = None
        except known.ContractError as exc:
            empty_error = exc.code
        observed = {
            "companion_rows_present": companion_rows > 0,
            "allowed_claim_visible": _claim_item(allowed_packet, _claim_value("quartz-orchid")) is not None,
            "restricted_question_status": restricted_question["status"],
            "restricted_question_answerability": restricted_question["answerability"],
            "restricted_question_items": len(restricted_question["items"]),
            "restricted_exact_status": restricted_exact["status"],
            "restricted_exact_items": len(restricted_exact["items"]),
            "empty_scopes_error": empty_error,
        }
        diagnostics = {
            "cross_scope_companion_rows": companion_rows,
            "cross_scope_secret_ref": secret.ref,
        }
        return observed, diagnostics
    finally:
        if harness is not None:
            harness.store.close()


class _Evaluator:
    """A candidate evaluator that would promote the tool-only proposal if it were ever asked."""

    def __init__(self, proposal: dict):
        self.proposal = proposal
        self.calls = 0

    def evaluate_candidate(self, candidate, sources, *, remaining_seconds):
        self.calls += 1
        return json.dumps(
            dict(
                protocol_version="1.1",
                source_refs=[f"{source.ref}@{source.revision}" for source in sources],
                claim_proposals=[self.proposal],
                resume_proposals=[],
                reference_proposals=[],
            ),
            ensure_ascii=False,
        )


def _proposal_for(source, *, predicate: str, value: str) -> dict:
    return dict(
        kind="fact",
        subject="Orchard",
        predicate=predicate,
        value_text=value,
        conditions=[],
        statement_kind="assertion",
        valid_from=source.event["occurred_at"],
        valid_to=None,
        evidence_spans=[dict(source_ref=source.ref, source_revision=source.revision, quote=source.event["content"])],
    )


def _register_candidate(harness, ctx, proposal: dict, known, reason: str):
    with harness.core.storage.write(ctx) as tx:
        saved = tx.claims.append(
            SCOPE,
            proposal,
            known.proposed_qualification(reason),
            recorded_at=harness.core.clock.utc_now(),
        )
        registration = tx.candidates.register(saved.ref, saved.revision, observed_at=harness.core.clock.utc_now())
    return saved, registration


def _drain_candidate_work(harness, ctx, evaluator: _Evaluator, rounds: int = 4) -> None:
    for _ in range(rounds):
        result = harness.core.drain_worker(
            ctx,
            consolidation=evaluator,
            embed=harness.embed_port,
            purge=harness.purge_port,
            max_items=32,
            remaining_seconds=20,
        )
        if result.processed == 0 or evaluator.calls:
            break


def _tool_control(harness, ctx, tool_source, known) -> tuple[dict, dict]:
    """A tool-only proposal stays proposed with its real lifecycle reason; a human-backed one promotes."""
    stub_payload = json.loads(harness.model.propose([tool_source]))
    stub_proposes = any(claim.get("value_text") == "build-4711" for claim in stub_payload["claim_proposals"])
    proposal = _proposal_for(tool_source, predicate="build artifact", value="build-4711")
    saved, registration = _register_candidate(harness, ctx, proposal, known, "eval_tool_only")
    evaluator = _Evaluator(proposal)
    _drain_candidate_work(harness, ctx, evaluator)
    state = harness.core.claim_history(ctx, saved.ref)[-1].state
    packet = harness.recall(ctx, "What is the Orchard build artifact?", max_items=6)
    tool_claims = [
        item
        for item in packet["items"]
        if item.get("kind") == "claim"
        and any((ref.split("@")[0] == tool_source.ref) for ref in (item.get("evidence_refs") or []))
    ]

    # The same proposal, in a separate store, backed by the person's own words:
    # this is the positive control for the tool-only result above.
    human_harness = known.Harness(
        harness.root / "human-control",
        scopes=harness.scopes,
        project_id=harness.project_id,
        model=known.StubConsolidation({}),
    )
    try:
        human_ctx = human_harness.ctx("session-A")
        human_source = human_harness.say(
            "session-A",
            "For project Orchard the build artifact is build-4711.",
            when="2026-10-01T10:07:00Z",
        )
        human_harness.drain(human_ctx)
        human_proposal = _proposal_for(human_source, predicate="build artifact", value="build-4711")
        human_saved, human_registration = _register_candidate(
            human_harness, human_ctx, human_proposal, known, "eval_human_control"
        )
        human_evaluator = _Evaluator(human_proposal)
        _drain_candidate_work(human_harness, human_ctx, human_evaluator)
        human_state = human_harness.core.claim_history(human_ctx, human_saved.ref)[-1].state
        human_packet = human_harness.recall(human_ctx, "What is the Orchard build artifact?", max_items=6)
        human_claim = _claim_item(human_packet, _claim_value("build-4711"))
    finally:
        human_harness.store.close()

    # The candidate lifecycle reason explains why no evaluation ran; the claim
    # version's state alone would not.
    observed = {
        "stub_proposes_tool_claim": stub_proposes,
        "candidate_work_queued": registration.work_queued,
        "candidate_evaluations": evaluator.calls,
        "candidate_processing_state": registration.processing_state,
        "candidate_reason": registration.reason,
        "claim_state": state,
        "active_tool_claim_items": sum(1 for item in tool_claims if item.get("claim_state") == "active"),
        "proposed_tool_claim_items": sum(1 for item in tool_claims if item.get("claim_state") == "proposed"),
        "human_control_candidate_work_queued": human_registration.work_queued,
        "human_control_candidate_evaluations": human_evaluator.calls,
        "human_control_claim_state": human_state,
        "human_control_claim_recalled": human_claim is not None,
    }
    diagnostics = {
        "tool_claim_ref": saved.ref,
        "tool_claim_candidates": len(tool_claims),
        "human_control_claim_ref": human_saved.ref,
    }
    return observed, diagnostics


def run() -> tuple[dict, dict]:
    """Run the fixed evaluation on fresh temporary stores."""
    root = Path(tempfile.mkdtemp(prefix="scope-recall-retrieval-eval-"))
    previous_scope_recall = sys.modules.get("scope_recall")
    main = None
    observed: dict = {}
    diagnostics: dict = {}
    cleaned = False
    try:
        with _source_modules():
            known = _load_known_answer()
            main_root = root / "main"
            main_root.mkdir(parents=True, exist_ok=True)
            main = known.Harness(
                main_root,
                scopes=frozenset({SCOPE, OTHER_SCOPE}),
                project_id=PROJECT,
                model=known.StubConsolidation(FACT_SENTENCES),
            )
            ctx = main.ctx("session-A")
            refs = _populate(main)
            claim_ref = _revise_port(main, ctx)
            metrics, ranking_diagnostics = _ranking(main, ctx)
            semantics = _semantics(main, ctx)
            precision = _precision(main, ctx)
            cross_scope, cross_diagnostics = _cross_scope(root / "isolation", known)
            tool, tool_diagnostics = _tool_control(main, ctx, refs["build"], known)
            observed = {
                "metrics": metrics,
                "semantics": semantics,
                "precision": precision,
                "cross_scope": cross_scope,
                "tool": tool,
            }
            diagnostics = {
                "claim_ref": claim_ref,
                "synthetic_source_refs": {name: ref.ref for name, ref in refs.items()},
                **ranking_diagnostics,
                **cross_diagnostics,
                **tool_diagnostics,
            }
    finally:
        if main is not None:
            main.store.close()
        shutil.rmtree(root, ignore_errors=True)
        cleaned = not root.exists()
    diagnostics["temp_root_removed"] = cleaned
    diagnostics["scope_recall_module_restored"] = sys.modules.get("scope_recall") is previous_scope_recall
    return observed, diagnostics


def differences(expected: dict, observed: dict, path: str = "") -> list[str]:
    """Every leaf that differs, with a dotted path; floats compare within 1e-6 and a boolean never equals an int."""
    found: list[str] = []
    for key in sorted(set(expected) | set(observed)):
        if key.startswith("_"):
            continue
        here = f"{path}.{key}" if path else key
        if key not in expected:
            found.append(f"{here}: unexpected {json.dumps(observed[key], default=str)}")
        elif key not in observed:
            found.append(f"{here}: missing (expected {json.dumps(expected[key], default=str)})")
        else:
            want, got = expected[key], observed[key]
            if isinstance(want, dict) and isinstance(got, dict):
                found.extend(differences(want, got, here))
            elif isinstance(want, float) and isinstance(got, float):
                if not math.isclose(want, got, rel_tol=0.0, abs_tol=1e-6):
                    found.append(f"{here}: observed {got}, expected {want}")
            elif isinstance(want, bool) != isinstance(got, bool):
                # ``True == 1`` in Python; a boolean oracle answer must not be
                # satisfied by an integer (or the reverse).
                found.append(
                    f"{here}: observed {json.dumps(got, default=str)}, expected {json.dumps(want, default=str)}"
                )
            elif isinstance(want, list) and isinstance(got, list):
                # ``[True] == [1]`` for the same reason; compare the JSON forms
                # so a boolean inside a list keeps its own type.
                if json.dumps(want, sort_keys=True, default=str) != json.dumps(got, sort_keys=True, default=str):
                    found.append(
                        f"{here}: observed {json.dumps(got, default=str)}, expected {json.dumps(want, default=str)}"
                    )
            elif want != got:
                found.append(
                    f"{here}: observed {json.dumps(got, default=str)}, expected {json.dumps(want, default=str)}"
                )
    return found


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--expected", default=str(HERE / "retrieval_quality_expected.json"))
    parser.add_argument("--json", help="also write observed values and diagnostics to this file")
    args = parser.parse_args()

    observed, diagnostics = run()
    expected = json.loads(Path(args.expected).read_text(encoding="utf-8"))
    mismatches = differences(expected, observed)
    for mismatch in mismatches:
        print(f"FAIL {mismatch}")
    if not mismatches:
        print(f"ok   {observed['metrics']['overall']['cases']} answerable cases match")
        print(json.dumps(observed["metrics"], indent=2))
    if args.json:
        Path(args.json).write_text(
            json.dumps(dict(observed=observed, diagnostics=diagnostics), indent=2, default=str),
            encoding="utf-8",
        )
    return 1 if mismatches else 0


if __name__ == "__main__":
    raise SystemExit(main())
