"""Source generations are retired through the write/deletions path, not direct SQL."""

import json
import sqlite3
import time
from dataclasses import replace

import pytest
from scope_recall.contracts import ContractError
from scope_recall.core import CoreConfig, MemoryCore
from scope_recall.core.restore import (
    InstallationMaintenance,
    begin_restore,
    export_deletion_ledger,
    ledger_digest,
    replay_deletion_ledger,
)
from scope_recall.core.retrieval import CandidateRef, SearchContext
from scope_recall.core.retrieval_storage import RetrievalStorage
from test_episodes import apply as apply_resume
from test_episodes import resume
from v11_support import context, recall_request, source_event


class Clock:
    now = "2026-09-06T12:00:00Z"

    def utc_now(self):
        return self.now

    def monotonic(self):
        return time.monotonic()


@pytest.fixture
def app(tmp_path):
    ctx = replace(context(tmp_path / "TEST-generations"), project_id="TEST-project", branch_id="TEST-main")
    core = MemoryCore(CoreConfig(ctx.binding), clock=Clock())
    core.initialize()
    return core, ctx


def capture(core, ctx, key, revision, text):
    event = source_event(
        source_event_key=key,
        source_revision=revision,
        content=text,
        occurred_at="2026-09-01T12:00:00Z",
        time_precision="instant",
    )
    saved = core.record_event(ctx, event, scope_id="TEST-scope", remaining_seconds=10)
    assert saved.disposition == "inserted"
    return saved.event_refs[0]


def retire(core, ctx, ref, revision):
    with core.storage.write(ctx, remaining_seconds=10) as tx:
        target = tx.deletions.target(ref, revision)
        return tx.deletions.retire_revisions((target,), now=Clock.now)


def retire_below(core, ctx, ref, before):
    with core.storage.write(ctx, remaining_seconds=10) as tx:
        return tx.deletions.retire_revisions_below({ref: before}, now=Clock.now)


def retire_all(core, ctx, ref):
    with core.storage.write(ctx, remaining_seconds=10) as tx:
        return tx.deletions.retire_generations((ref,), now=Clock.now)


def sqlite_backup(source, target):
    with sqlite3.connect(source) as reader, sqlite3.connect(target) as writer:
        reader.backup(writer)


def assert_no_object_block(core, ref):
    with sqlite3.connect(core.storage.path) as conn:
        assert not conn.execute(
            "SELECT 1 FROM object_blocks WHERE object_kind='event' AND object_ref=?", (ref,)
        ).fetchone()


def test_retiring_a_generation_keeps_the_newer_generation_and_moves_the_epoch(app):
    core, ctx = app
    first = capture(core, ctx, "TEST-generation/alpha", 1, "alpha old")
    second = capture(core, ctx, "TEST-generation/alpha", 2, "alpha new")
    epoch = core.memory_epoch(ctx)

    result = retire(core, ctx, first.ref, 1)

    assert result["retired"] == 1 and result["memory_epoch"] > epoch
    assert result["affected_objects"] == 1
    assert result["requested_refs"] == [first.ref]
    assert result["minimum_content_unit"] == "source_revision"
    assert result["layers"]["retired"] == 1
    assert result["layers"]["generation_retirements"] == {"kind": "revisions", "revisions": {first.ref: [1]}}
    assert core.source(ctx, first.ref, 1).suppressed
    assert not core.source(ctx, second.ref, 2).suppressed
    assert core.memory_retracted_since(ctx, epoch)
    hits = [hit for hit in core.search_sources(ctx, "alpha", automatic=True) if hit.ref == first.ref]
    assert [(hit.revision, hit.suppressed) for hit in hits] == [(2, False)]


def test_retiring_the_same_generation_is_idempotent(app):
    core, ctx = app
    first = capture(core, ctx, "TEST-generation/beta", 1, "beta old")
    capture(core, ctx, "TEST-generation/beta", 2, "beta new")
    retire(core, ctx, first.ref, 1)
    epoch = core.memory_epoch(ctx)

    again = retire(core, ctx, first.ref, 1)

    assert again["retired"] == 0
    assert core.memory_epoch(ctx) == epoch


def test_suppressing_the_object_hides_it_from_automatic_search_but_keeps_history(app):
    core, ctx = app
    first = capture(core, ctx, "TEST-generation/gamma", 1, "gamma old")
    second = capture(core, ctx, "TEST-generation/gamma", 2, "gamma new")

    with core.storage.write(ctx, remaining_seconds=10) as tx:
        receipt = tx.deletions.retire_generations((first.ref,), now=Clock.now)

    assert receipt["read_blocked"] is False and receipt["suppressed"] is True
    assert core.source(ctx, second.ref, 2).suppressed
    assert not [hit for hit in core.search_sources(ctx, "gamma", automatic=True) if hit.ref == first.ref]
    history = [hit.revision for hit in core.search_sources(ctx, "gamma", history=True) if hit.ref == first.ref]
    assert sorted(history) == [1, 2]


def test_a_generation_from_another_project_cannot_be_retired(app):
    core, ctx = app
    first = capture(core, ctx, "TEST-generation/delta", 1, "delta old")
    other = replace(ctx, project_id="TEST-other")

    with pytest.raises(ContractError, match="target_unavailable"):
        with core.storage.write(other, remaining_seconds=10) as tx:
            tx.deletions.target(first.ref, 1)


def test_source_identity_reports_the_ceiling_after_every_generation_is_suppressed(app):
    core, ctx = app
    first = capture(core, ctx, "TEST-generation/epsilon", 1, "epsilon old")
    second = capture(core, ctx, "TEST-generation/epsilon", 2, "epsilon new")
    retire(core, ctx, first.ref, 1)

    with core.storage.write(ctx, remaining_seconds=10) as tx:
        tx.deletions.retire_generations((first.ref,), now=Clock.now)

    with core.storage.read(ctx, remaining_seconds=10) as tx:
        ref, ceiling = tx.sources.source_identity("TEST-generation/epsilon")

    assert ref == first.ref == second.ref and ceiling == 2


def test_retirement_kinds_have_distinct_idempotency_keys(app):
    core, ctx = app
    first = capture(core, ctx, "TEST-generation/zeta", 1, "zeta one")
    second = capture(core, ctx, "TEST-generation/zeta", 2, "zeta two")
    third = capture(core, ctx, "TEST-generation/zeta", 3, "zeta three")

    exact = retire(core, ctx, third.ref, 3)
    below = retire_below(core, ctx, third.ref, 3)
    all_generations = retire_all(core, ctx, third.ref)

    assert exact["operation_id"] != below["operation_id"]
    assert all_generations["operation_id"] not in {exact["operation_id"], below["operation_id"]}
    assert below["layers"]["generation_retirements"] == {"kind": "below", "below": {first.ref: 3}}
    assert below["affected_objects"] == 2
    assert core.source(ctx, first.ref, 1).suppressed
    assert core.source(ctx, second.ref, 2).suppressed
    assert core.source(ctx, third.ref, 3).suppressed


def test_retire_generations_rejects_an_unavailable_ref(app):
    core, ctx = app
    first = capture(core, ctx, "TEST-generation/eta", 1, "eta one")
    other = replace(ctx, project_id="TEST-other")

    with pytest.raises(ContractError, match="target_unavailable"):
        with core.storage.write(other, remaining_seconds=10) as tx:
            tx.deletions.retire_generations((first.ref,), now=Clock.now)


def test_restore_replays_a_revision_retirement_without_blocking_newer_generations(app, tmp_path):
    core, ctx = app
    first = capture(core, ctx, "TEST-generation/restore-revision", 1, "restore alpha one")
    second = capture(core, ctx, "TEST-generation/restore-revision", 2, "restore alpha two")
    third = capture(core, ctx, "TEST-generation/restore-revision", 3, "restore alpha three")
    backup = tmp_path / "TEST-generation-revision.sqlite3"
    sqlite_backup(core.storage.path, backup)

    retire(core, ctx, first.ref, 1)
    ledger = export_deletion_ledger(core.storage, InstallationMaintenance(ctx))
    begin_restore(core.storage, InstallationMaintenance(ctx), expected_ledger_sha256=ledger_digest(ledger))
    sqlite_backup(backup, core.storage.path)
    replay = replay_deletion_ledger(core.storage, InstallationMaintenance(ctx), ledger)

    assert replay["status"] == "deletion_ledger_replayed"
    assert core.source(ctx, first.ref, 1).suppressed
    assert not core.source(ctx, second.ref, 2).suppressed
    assert not core.source(ctx, third.ref, 3).suppressed
    hits = [hit for hit in core.search_sources(ctx, "restore alpha three", automatic=True) if hit.ref == first.ref]
    assert [(hit.revision, hit.suppressed) for hit in hits] == [(3, False)]
    assert_no_object_block(core, first.ref)


def test_restore_replays_a_below_retirement_and_keeps_the_fence_generation(app, tmp_path):
    core, ctx = app
    first = capture(core, ctx, "TEST-generation/restore-below", 1, "below alpha one")
    second = capture(core, ctx, "TEST-generation/restore-below", 2, "below alpha two")
    third = capture(core, ctx, "TEST-generation/restore-below", 3, "below alpha three")
    backup = tmp_path / "TEST-generation-below.sqlite3"
    sqlite_backup(core.storage.path, backup)

    retire_below(core, ctx, first.ref, 3)
    ledger = export_deletion_ledger(core.storage, InstallationMaintenance(ctx))
    begin_restore(core.storage, InstallationMaintenance(ctx), expected_ledger_sha256=ledger_digest(ledger))
    sqlite_backup(backup, core.storage.path)
    replay_deletion_ledger(core.storage, InstallationMaintenance(ctx), ledger)

    assert core.source(ctx, first.ref, 1).suppressed
    assert core.source(ctx, second.ref, 2).suppressed
    assert not core.source(ctx, third.ref, 3).suppressed
    assert_no_object_block(core, first.ref)


def test_restore_replays_an_all_generation_retirement_without_an_object_block(app, tmp_path):
    core, ctx = app
    first = capture(core, ctx, "TEST-generation/restore-all", 1, "all alpha one")
    second = capture(core, ctx, "TEST-generation/restore-all", 2, "all alpha two")
    third = capture(core, ctx, "TEST-generation/restore-all", 3, "all alpha three")
    backup = tmp_path / "TEST-generation-all.sqlite3"
    sqlite_backup(core.storage.path, backup)

    result = retire_all(core, ctx, first.ref)
    assert result["affected_objects"] == 3
    assert result["layers"]["generation_retirements"] == {
        "kind": "all",
        "refs": [first.ref],
        "ceilings": {first.ref: 3},
    }
    ledger = export_deletion_ledger(core.storage, InstallationMaintenance(ctx))
    begin_restore(core.storage, InstallationMaintenance(ctx), expected_ledger_sha256=ledger_digest(ledger))
    sqlite_backup(backup, core.storage.path)
    replay_deletion_ledger(core.storage, InstallationMaintenance(ctx), ledger)

    assert core.source(ctx, first.ref, 1).suppressed
    assert core.source(ctx, second.ref, 2).suppressed
    assert core.source(ctx, third.ref, 3).suppressed
    assert not [hit for hit in core.search_sources(ctx, "all alpha", automatic=True) if hit.ref == first.ref]
    assert_no_object_block(core, first.ref)


def test_restore_replays_an_all_retirement_up_to_its_recorded_ceiling(app, tmp_path):
    core, ctx = app
    first = capture(core, ctx, "TEST-generation/restore-ceiling", 1, "ceiling alpha one")
    result = retire_all(core, ctx, first.ref)
    assert result["layers"]["generation_retirements"] == {
        "kind": "all",
        "refs": [first.ref],
        "ceilings": {first.ref: 1},
    }
    second = capture(core, ctx, "TEST-generation/restore-ceiling", 2, "ceiling alpha two")
    assert not core.source(ctx, second.ref, 2).suppressed
    backup = tmp_path / "TEST-generation-ceiling.sqlite3"
    sqlite_backup(core.storage.path, backup)

    ledger = export_deletion_ledger(core.storage, InstallationMaintenance(ctx))
    begin_restore(core.storage, InstallationMaintenance(ctx), expected_ledger_sha256=ledger_digest(ledger))
    sqlite_backup(backup, core.storage.path)
    replay = replay_deletion_ledger(core.storage, InstallationMaintenance(ctx), ledger)

    assert replay["status"] == "deletion_ledger_replayed"
    assert core.source(ctx, first.ref, 1).suppressed
    assert not core.source(ctx, second.ref, 2).suppressed
    with sqlite3.connect(core.storage.path) as conn:
        work = dict(
            conn.execute(
                "SELECT subject_revision,state FROM work_items WHERE work_type='consolidate' AND subject_ref=?",
                (first.ref,),
            )
        )
    assert work == {1: "obsolete", 2: "pending"}
    hits = [hit for hit in core.search_sources(ctx, "ceiling alpha two", automatic=True) if hit.ref == first.ref]
    assert [(hit.revision, hit.suppressed) for hit in hits] == [(2, False)]
    assert_no_object_block(core, first.ref)


def _fail_consolidation(core, ref, *, code="timeout"):
    with sqlite3.connect(core.storage.path) as conn:
        conn.execute(
            """UPDATE work_items SET state='failed',attempt=3,last_error_code=?,
            available_at='2026-09-01T00:00:00Z' WHERE work_type='consolidate' AND subject_ref=?""",
            (code, ref),
        )
        conn.commit()


def _consolidation_row(core, ref):
    with sqlite3.connect(core.storage.path) as conn:
        return conn.execute(
            "SELECT state,last_error_code FROM work_items WHERE work_type='consolidate' AND subject_ref=?",
            (ref,),
        ).fetchone()


def test_failed_consolidation_of_a_retired_generation_is_not_recovered(app):
    core, ctx = app
    source = capture(core, ctx, "TEST-generation/retired-work", 1, "retired work text")
    _fail_consolidation(core, source.ref)
    retire(core, ctx, source.ref, 1)

    with core.storage.write(ctx, remaining_seconds=10) as tx:
        assert tx.work.recover_transient_failures(now=Clock.now, allowed_work_types=frozenset({"consolidate"})) == 0

    assert _consolidation_row(core, source.ref) == ("obsolete", "authority_revoked")


def test_retry_failures_does_not_reopen_a_retired_consolidation(app):
    core, ctx = app
    source = capture(core, ctx, "TEST-generation/retired-retry", 1, "retired retry text")
    _fail_consolidation(core, source.ref)
    retire(core, ctx, source.ref, 1)

    with core.storage.write(ctx, remaining_seconds=10) as tx:
        report = tx.work.retry_failed(now=Clock.now, dry_run=False)

    assert report["retried"] == 0
    assert _consolidation_row(core, source.ref) == ("obsolete", "authority_revoked")


def test_source_identity_and_target_share_the_project_branch_partition(app):
    core, ctx = app
    global_ctx = replace(ctx, project_id=None, branch_id=None)
    source = capture(core, global_ctx, "TEST-generation/global", 1, "global partition text")

    with core.storage.read(ctx, remaining_seconds=10) as tx:
        ref, ceiling = tx.sources.source_identity("TEST-generation/global")
    assert (ref, ceiling) == (source.ref, 0)
    with pytest.raises(ContractError, match="target_unavailable"):
        with core.storage.write(ctx, remaining_seconds=10) as tx:
            tx.deletions.target(source.ref, 1)

    with core.storage.read(global_ctx, remaining_seconds=10) as tx:
        assert tx.sources.source_identity("TEST-generation/global") == (source.ref, 1)
    with core.storage.write(global_ctx, remaining_seconds=10) as tx:
        assert tx.deletions.target(source.ref, 1).project_id is None


def _operation_count(core):
    with sqlite3.connect(core.storage.path) as conn:
        return conn.execute("SELECT count(*) FROM deletion_operations").fetchone()[0]


def test_retire_revisions_rejects_a_target_from_another_partition(app):
    core, ctx = app
    source = capture(core, ctx, "TEST-generation/partition", 1, "partition text")
    epoch = core.memory_epoch(ctx)

    with core.storage.write(ctx, remaining_seconds=10) as tx:
        target = tx.deletions.target(source.ref, 1)
        forged = replace(target, project_id="TEST-other", branch_id="TEST-other")
        with pytest.raises(ContractError, match="target_unavailable"):
            tx.deletions.retire_revisions((forged,), now=Clock.now)

    assert core.memory_epoch(ctx) == epoch
    assert not core.source(ctx, source.ref, 1).suppressed
    assert _consolidation_row(core, source.ref) == ("pending", None)
    assert _operation_count(core) == 0


def test_retire_revisions_rejects_a_target_from_another_scope(app):
    core, ctx = app
    source = capture(core, ctx, "TEST-generation/scope", 1, "scope text")
    epoch = core.memory_epoch(ctx)

    with core.storage.write(ctx, remaining_seconds=10) as tx:
        target = tx.deletions.target(source.ref, 1)
        forged = replace(target, scope_id="TEST-other-scope")
        with pytest.raises(ContractError, match="target_unavailable"):
            tx.deletions.retire_revisions((forged,), now=Clock.now)

    assert core.memory_epoch(ctx) == epoch
    assert not core.source(ctx, source.ref, 1).suppressed
    assert _operation_count(core) == 0


def test_retire_revisions_rejects_a_generation_the_store_does_not_hold(app):
    core, ctx = app
    source = capture(core, ctx, "TEST-generation/absent-revision", 1, "absent revision text")
    epoch = core.memory_epoch(ctx)

    with core.storage.write(ctx, remaining_seconds=10) as tx:
        target = tx.deletions.target(source.ref, 1)
        with pytest.raises(ContractError, match="target_unavailable"):
            tx.deletions.retire_revisions((replace(target, revision=2),), now=Clock.now)

    assert core.memory_epoch(ctx) == epoch
    assert not core.source(ctx, source.ref, 1).suppressed
    assert _operation_count(core) == 0


@pytest.mark.parametrize("revision", [0, -1, True, "1", 1.0])
def test_retire_revisions_rejects_an_invalid_revision(app, revision):
    core, ctx = app
    source = capture(core, ctx, "TEST-generation/invalid-revision", 1, "invalid revision text")
    epoch = core.memory_epoch(ctx)

    with core.storage.write(ctx, remaining_seconds=10) as tx:
        target = tx.deletions.target(source.ref, 1)
        with pytest.raises(ContractError, match="source_revision"):
            tx.deletions.retire_revisions((replace(target, revision=revision),), now=Clock.now)

    assert core.memory_epoch(ctx) == epoch
    assert not core.source(ctx, source.ref, 1).suppressed
    assert _operation_count(core) == 0


def test_rejected_forged_targets_leave_the_ledger_clean_for_restore(app, tmp_path):
    core, ctx = app
    source = capture(core, ctx, "TEST-generation/rejected-ledger", 1, "rejected ledger text")
    backup = tmp_path / "TEST-generation-rejected.sqlite3"
    sqlite_backup(core.storage.path, backup)
    epoch = core.memory_epoch(ctx)

    with core.storage.write(ctx, remaining_seconds=10) as tx:
        target = tx.deletions.target(source.ref, 1)
        for forged in (
            replace(target, project_id="TEST-other", branch_id="TEST-other"),
            replace(target, scope_id="TEST-other-scope"),
            replace(target, revision=0),
        ):
            with pytest.raises(ContractError):
                tx.deletions.retire_revisions((forged,), now=Clock.now)

    assert core.memory_epoch(ctx) == epoch
    ledger = export_deletion_ledger(core.storage, InstallationMaintenance(ctx))
    assert ledger["operations"] == []
    begin_restore(core.storage, InstallationMaintenance(ctx), expected_ledger_sha256=ledger_digest(ledger))
    sqlite_backup(backup, core.storage.path)
    replay = replay_deletion_ledger(core.storage, InstallationMaintenance(ctx), ledger)

    assert replay["status"] == "deletion_ledger_replayed"
    assert not core.source(ctx, source.ref, 1).suppressed


def test_a_no_op_retirement_does_not_suppress_a_generation_on_restore(app, tmp_path):
    core, ctx = app
    first = capture(core, ctx, "TEST-generation/noop-restore", 1, "noop alpha one")
    second = capture(core, ctx, "TEST-generation/noop-restore", 2, "noop alpha two")
    backup = tmp_path / "TEST-generation-noop.sqlite3"
    sqlite_backup(core.storage.path, backup)

    # A suppression the ledger does not hold, like a legacy row or a direct
    # repair: the retirement has no live generation left to suppress.
    with sqlite3.connect(core.storage.path) as conn:
        conn.execute("UPDATE source_events SET suppressed=1 WHERE event_id=? AND source_revision=1", (first.ref,))
        conn.commit()

    result = retire(core, ctx, first.ref, 1)
    assert result["retired"] == 0
    assert result["layers"]["retired"] == 0

    ledger = export_deletion_ledger(core.storage, InstallationMaintenance(ctx))
    assert len(ledger["operations"]) == 1
    layers = json.loads(ledger["operations"][0]["operation"]["layers_json"])
    assert layers["retired"] == 0
    assert layers["generation_retirements"] == {"kind": "revisions", "revisions": {first.ref: [1]}}

    begin_restore(core.storage, InstallationMaintenance(ctx), expected_ledger_sha256=ledger_digest(ledger))
    sqlite_backup(backup, core.storage.path)
    replay = replay_deletion_ledger(core.storage, InstallationMaintenance(ctx), ledger)

    assert replay["status"] == "deletion_ledger_replayed"
    assert not core.source(ctx, first.ref, 1).suppressed
    assert not core.source(ctx, second.ref, 2).suppressed


def test_replay_skips_a_no_op_retirement_with_an_invalid_descriptor(app, tmp_path):
    core, ctx = app
    source = capture(core, ctx, "TEST-generation/poisoned-noop", 1, "poisoned noop text")
    backup = tmp_path / "TEST-generation-poisoned-noop.sqlite3"
    sqlite_backup(core.storage.path, backup)

    # A no-op operation recorded by an older writer may name a revision the
    # source can never hold.  It retired nothing, so replay must skip it rather
    # than refuse the whole restore.
    with sqlite3.connect(core.storage.path) as conn:
        conn.execute("UPDATE instance_meta SET memory_epoch=memory_epoch+1 WHERE singleton=1")
        epoch = conn.execute("SELECT memory_epoch FROM instance_meta WHERE singleton=1").fetchone()[0]
        conn.execute(
            """INSERT INTO deletion_operations(operation_id,request_sha256,mode,scope_ids_json,
            project_id,branch_id,requested_refs_json,expected_revisions_json,created_at,memory_epoch,layers_json)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (
                "forged-noop",
                "a" * 64,
                "suppress",
                json.dumps(["TEST-scope"]),
                "TEST-project",
                "TEST-main",
                json.dumps([source.ref]),
                json.dumps({source.ref: 1}),
                "2026-09-06T12:00:00Z",
                epoch,
                json.dumps(
                    {
                        "generation_retirements": {"kind": "revisions", "revisions": {source.ref: [0]}},
                        "retired": 0,
                    }
                ),
            ),
        )
        conn.commit()

    ledger = export_deletion_ledger(core.storage, InstallationMaintenance(ctx))
    assert [entry["operation"]["operation_id"] for entry in ledger["operations"]] == ["forged-noop"]
    begin_restore(core.storage, InstallationMaintenance(ctx), expected_ledger_sha256=ledger_digest(ledger))
    sqlite_backup(backup, core.storage.path)
    replay = replay_deletion_ledger(core.storage, InstallationMaintenance(ctx), ledger)

    assert replay["status"] == "deletion_ledger_replayed"
    assert not core.source(ctx, source.ref, 1).suppressed


def test_an_automatic_episode_read_does_not_expose_a_retired_member(app):
    core, ctx = app
    ctx = replace(ctx, task_anchor="TEST-retired-episode")
    cited_write = capture(core, ctx, "TEST-generation/episode-cited", 1, "TEST 下一步先做版式。")
    uncited_write = capture(core, ctx, "TEST-generation/episode-uncited", 1, "TEST 颜色后面再说。")
    cited = core.source(ctx, cited_write.ref, cited_write.revision)
    uncited = core.source(ctx, uncited_write.ref, uncited_write.revision)
    apply_resume(core, ctx, resumes=[resume(cited)])
    retire(core, ctx, uncited.ref, 1)

    (episode,) = core.episodes(ctx)
    assert "source_version_changed" in episode.gaps and episode.needs_revalidation

    search = SearchContext.from_request(recall_request(mode="auto"), ctx, now=core.clock.utc_now(), deadline=200.0)
    with core.storage.read(ctx) as tx:
        automatic = RetrievalStorage(clock=core.clock).hydrate(
            tx, CandidateRef("episode", episode.ref, episode.revision, "exact_ref"), search
        )
    assert automatic is None

    history = SearchContext.from_request(recall_request(mode="history"), ctx, now=core.clock.utc_now(), deadline=200.0)
    with core.storage.read(ctx) as tx:
        explicit = RetrievalStorage(clock=core.clock).hydrate(
            tx, CandidateRef("episode", episode.ref, episode.revision, "exact_ref"), history
        )
    assert explicit is not None
    rows, _ = core.records.episode_sources(ctx, episode.ref)
    assert [source.ref for _, source in rows] == [cited.ref, uncited.ref]
