"""Tool-output vectors expire after the retention window; the rest of the memory stays.

On a busy instance four in five captured sources were tool output, each
carrying a 12 KB vector -- the bulk of daily growth -- while a few hundred of
170,000 sources ever became claim evidence.  The window lets that bulk go
without touching the text, the lexical index, or anything derived from it.
"""

import itertools
import json
import os
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
import scope_recall.runtime.scheduling as scheduling
from scope_recall.core import CoreConfig, MemoryCore
from scope_recall.core.registry_storage import Registry
from scope_recall.maintenance import doctor, doctor_store
from scope_recall.runtime import vector_retention
from scope_recall.runtime.instance_config import RuntimeInstanceConfig, VectorRuntimeConfig
from scope_recall.runtime.scheduling import _retention_due_at, next_wake
from test_claims import Clock, app, capture
from v11_support import context

SPACE = "TEST-space"
NOW = datetime(2026, 9, 6, 12, 0, tzinfo=timezone.utc)


class Store:
    def __init__(self):
        self.deleted: list[list[str]] = []

    def delete_by_ids(self, ids):
        self.deleted.append(list(ids))


class Config:
    """The fields a pass reads from a RuntimeInstanceConfig."""

    def __init__(self, vector, data_directory=None, installation_id=""):
        self.vector = vector
        self.binding = SimpleNamespace(
            installation_id=installation_id,
            data_directory=Path(data_directory or vector.storage_dir),
        )

    def embedding_space_id(self):
        return SPACE


@pytest.fixture
def shared_app(tmp_path):
    """A shared store: an entry id is required on every context and source row."""
    base = context(tmp_path / "TEST-shared-retention")
    binding = replace(base.binding, installation_kind="shared")
    ctx = replace(base, binding=binding)
    core = MemoryCore(CoreConfig(binding), clock=Clock())
    core.initialize()
    core.test_sequence = itertools.count(1)
    with core.storage.write(ctx) as tx:
        for entry_id in ("entry-one", "entry-two", "entry-short", "entry-long"):
            tx.registry.register_entry(entry_id, entry_id, "hermes", now="2026-09-01T00:00:00Z")
    return core, ctx


def _vector(tmp_path, days):
    return VectorRuntimeConfig(
        backend="sqlite-bruteforce",
        storage_dir=tmp_path / "vectors",
        table_name="TEST-vectors",
        dimensions=1,
        test_injection_override=True,
        tool_output_retention_days=days,
    )


def _shared_worker(tmp_path, ctx, *, days=180, drain_seconds=120.0):
    """The config a shared store's one worker loads: every scope, no entry of its own."""
    from scope_recall.maintenance.shared import _worker_config

    binding = replace(ctx.binding, installation_kind="shared")
    routes = dict(
        binding=dict(
            agent_id=ctx.binding.agent_id,
            installation_id=ctx.binding.installation_id,
            data_directory=str(ctx.binding.data_directory),
            scope_ids=sorted(ctx.binding.scope_ids),
            test_mode=ctx.binding.test_mode,
        ),
        session_id="TEST-entry-routes",
        allowed_scope_ids=sorted(ctx.binding.scope_ids),
        vector=dict(
            backend="sqlite-bruteforce",
            storage_dir=str(tmp_path / "vectors"),
            table_name="TEST-vectors",
            dimensions=1,
            test_injection_override=True,
            tool_output_retention_days=days,
        ),
        drain_seconds=drain_seconds,
    )
    return RuntimeInstanceConfig.from_mapping(_worker_config(routes, binding))


def _iso(moment):
    return moment.isoformat().replace("+00:00", "Z")


def _embedded(core, *, aged, persisted_at="2026-01-01T12:00:00Z"):
    """Every source has finished embed work; the aged ones entered the store long ago."""
    with sqlite3.connect(core.storage.path) as conn:
        conn.execute("""INSERT OR IGNORE INTO work_items(work_type,subject_ref,subject_revision,scope_id,project_id,branch_id,state,available_at)
                        SELECT 'embed',event_id,source_revision,scope_id,project_id,branch_id,'done',persisted_at FROM source_events""")
        conn.execute("UPDATE work_items SET state='done' WHERE work_type='embed'")
        conn.executemany(
            "UPDATE source_events SET persisted_at=? WHERE event_id=?", [(persisted_at, s.ref) for s in aged]
        )
        conn.commit()


def _count(core, sql, *args):
    with sqlite3.connect(core.storage.path) as conn:
        return conn.execute(sql, args).fetchone()[0]


def _pass(core, ctx, store, tmp_path, *, days=180, now=NOW, seconds=30.0):
    return vector_retention.expire_if_due(
        store,
        Config(_vector(tmp_path, days), tmp_path, ctx.binding.installation_id),
        core.storage,
        ctx,
        available_seconds=seconds,
        now=now,
    )


def _key(ctx, days=180):
    return vector_retention.retention_key(
        installation_id=ctx.binding.installation_id,
        entry_id=ctx.entry_id,
        project_id=ctx.project_id,
        branch_id=ctx.branch_id,
        scope_ids=ctx.allowed_scope_ids,
        days=days,
    )


def test_a_tool_output_older_than_the_window_loses_its_vector_and_nothing_else(app, tmp_path):
    core, ctx = app
    old_tools = [capture(core, ctx, f"TEST old tool output {index}", origin="tool_observation") for index in range(3)]
    capture(core, ctx, "TEST fresh tool output", origin="tool_observation")
    old_human = capture(core, ctx, "TEST old human turn")
    _embedded(core, aged=(*old_tools, old_human))
    store = Store()
    receipt = _pass(core, ctx, store, tmp_path)
    assert (receipt["outcome"], receipt["expired"], receipt["backlog"]) == ("expired", 3, False)
    assert store.deleted == [[f"p10:{s.ref}@1:{SPACE}" for s in old_tools]]
    assert _count(core, "SELECT count(*) FROM expired_vectors") == 3
    # The source, its lexical index and its finished work item are untouched...
    assert _count(core, "SELECT count(*) FROM source_events") == 5
    assert (
        _count(
            core,
            "SELECT count(*) FROM lexical_postings WHERE source_id IN (SELECT source_id FROM source_events WHERE event_id=?)",
            old_tools[0].ref,
        )
        > 0
    )
    assert core.source(ctx, old_tools[0].ref, 1) is not None
    assert _count(core, "SELECT count(*) FROM work_items WHERE work_type='embed' AND state='done'") == 5
    # ...so nothing refills an embed for an expired source.
    core.records.resume_deferred(ctx, limit=16, remaining_seconds=1)
    assert _count(core, "SELECT count(*) FROM work_items WHERE work_type='embed'") == 5
    # Within the hour the drain leaves it alone; an hour later a pass finds nothing due.
    assert _pass(core, ctx, store, tmp_path, now=NOW + timedelta(minutes=30)) is None
    later = _pass(core, ctx, store, tmp_path, now=NOW + timedelta(hours=2))
    assert later["outcome"] == "nothing_due" and len(store.deleted) == 1


def test_a_pass_without_its_reserve_backs_off_instead_of_repeating_every_five_minutes(app, tmp_path):
    """A drain that leaves less than the reserve for retention records a retry time; it does not spin."""
    core, ctx = app
    tool = capture(core, ctx, "TEST short budget tool output", origin="tool_observation")
    _embedded(core, aged=[tool])
    store = Store()
    assert _pass(core, ctx, store, tmp_path, seconds=vector_retention.RESERVE_SECONDS - 0.1) is None
    assert store.deleted == []
    assert vector_retention.read_backoff(tmp_path, _key(ctx)) == NOW + vector_retention.RETRY_BACKOFF


def test_a_store_that_is_not_open_backs_off(app, tmp_path):
    core, ctx = app
    tool = capture(core, ctx, "TEST unopened store tool output", origin="tool_observation")
    _embedded(core, aged=[tool])
    assert _pass(core, ctx, None, tmp_path) is None
    assert vector_retention.read_backoff(tmp_path, _key(ctx)) == NOW + vector_retention.RETRY_BACKOFF


def test_a_state_write_failure_backs_off(app, tmp_path, monkeypatch):
    """A pass whose receipt cannot be written still leaves a retry time for the planner."""
    core, ctx = app
    tool = capture(core, ctx, "TEST unwritable state tool output", origin="tool_observation")
    _embedded(core, aged=[tool])
    monkeypatch.setattr(vector_retention, "write_state", lambda *args, **kwargs: False)
    receipt = _pass(core, ctx, Store(), tmp_path)
    assert receipt["outcome"] == "expired" and receipt["state_written"] is False
    assert receipt["backoff_until"] == (NOW + vector_retention.RETRY_BACKOFF).isoformat().replace("+00:00", "Z")
    assert vector_retention.read_backoff(tmp_path, _key(ctx)) == NOW + vector_retention.RETRY_BACKOFF


def test_a_legacy_global_receipt_still_delays_the_first_keyed_pass(app, tmp_path):
    """A 3.10 state file was one global receipt; an upgrade must not repeat the pass it records."""
    core, ctx = app
    tool = capture(core, ctx, "TEST legacy state tool output", origin="tool_observation")
    _embedded(core, aged=[tool])
    storage_dir = tmp_path / "vectors"
    storage_dir.mkdir(parents=True, exist_ok=True)
    (storage_dir / vector_retention.STATE_FILENAME).write_text(
        json.dumps(
            {
                "schema": vector_retention.LEGACY_STATE_SCHEMA,
                "retention_days": 180,
                "finished_at": (NOW - timedelta(minutes=30)).isoformat().replace("+00:00", "Z"),
            }
        ),
        encoding="utf-8",
    )
    assert _pass(core, ctx, Store(), tmp_path) is None


def test_a_sixty_day_window_keeps_the_source_and_its_words_and_queues_no_embedding(app, tmp_path):
    """The vector goes at 60 days; the source, its lexical postings and its finished embed stay."""
    core, ctx = app
    old = capture(core, ctx, "TEST sixty day old tool output", origin="tool_observation")
    old_human = capture(core, ctx, "TEST sixty day old human turn")
    fresh = capture(core, ctx, "TEST sixty day fresh tool output", origin="tool_observation")
    _embedded(core, aged=(old, old_human), persisted_at=(NOW - timedelta(days=61)).isoformat())
    with sqlite3.connect(core.storage.path) as conn:
        conn.execute(
            "UPDATE source_events SET persisted_at=? WHERE event_id=?",
            ((NOW - timedelta(days=59)).isoformat(), fresh.ref),
        )
        conn.commit()
    store = Store()
    receipt = _pass(core, ctx, store, tmp_path, days=60)
    assert (receipt["outcome"], receipt["expired"], receipt["retention_days"]) == ("expired", 1, 60)
    assert store.deleted == [[f"p10:{old.ref}@1:{SPACE}"]]
    # The text and the words that find it are still there; only the vector is gone.
    assert _count(core, "SELECT count(*) FROM source_events") == 3
    assert core.source(ctx, old.ref, 1) is not None
    assert (
        _count(
            core,
            "SELECT count(*) FROM lexical_postings WHERE source_id="
            "(SELECT source_id FROM source_events WHERE event_id=?)",
            old.ref,
        )
        > 0
    )
    assert _count(core, "SELECT count(*) FROM expired_vectors WHERE source_ref=?", old.ref) == 1
    # The expired source is not queued for another embedding, and a deferred refill does not add one.
    core.records.resume_deferred(ctx, limit=16, remaining_seconds=1)
    assert _count(core, "SELECT count(*) FROM work_items WHERE work_type='embed'") == 3
    assert (
        _count(
            core,
            "SELECT count(*) FROM work_items WHERE work_type='embed' AND subject_ref=? AND state!='done'",
            old.ref,
        )
        == 0
    )


def test_the_retention_receipt_is_the_wake_plan_s_record(app, tmp_path):
    """The pass and the planner derive the same entry/scope partition key from the same config."""
    core, ctx = app
    tool = capture(core, ctx, "TEST planner key tool output", origin="tool_observation")
    _embedded(core, aged=[tool])
    receipt = _pass(core, ctx, Store(), tmp_path, days=60)
    assert receipt["state_written"] is True
    config = RuntimeInstanceConfig.from_mapping(
        dict(
            binding=dict(
                agent_id=ctx.binding.agent_id,
                installation_id=ctx.binding.installation_id,
                data_directory=str(ctx.binding.data_directory),
                scope_ids=sorted(ctx.binding.scope_ids),
                test_mode=True,
            ),
            session_id="TEST-session",
            allowed_scope_ids=sorted(ctx.allowed_scope_ids),
            project_id=ctx.project_id,
            branch_id=ctx.branch_id,
            vector=dict(
                backend="sqlite-bruteforce",
                storage_dir=str(tmp_path / "vectors"),
                table_name="TEST-vectors",
                dimensions=1,
                test_injection_override=True,
                tool_output_retention_days=60,
            ),
        )
    )
    plan = next_wake(config, now=NOW + timedelta(minutes=5))
    due = datetime.fromisoformat(plan.due_at.replace("Z", "+00:00"))
    assert plan.reason == "vector_retention"
    assert NOW + vector_retention.PASS_INTERVAL <= due <= NOW + vector_retention.PASS_INTERVAL + timedelta(seconds=60)


def test_a_backlog_is_cleared_one_batch_per_drain(app, tmp_path, monkeypatch):
    core, ctx = app
    monkeypatch.setattr(vector_retention, "BATCH_LIMIT", 2)
    tools = [capture(core, ctx, f"TEST backlog {index}", origin="tool_observation") for index in range(3)]
    _embedded(core, aged=tools)
    store = Store()
    first = _pass(core, ctx, store, tmp_path)
    assert (first["expired"], first["backlog"]) == (2, True)
    second = _pass(core, ctx, store, tmp_path)  # due at once: the last pass was full
    assert (second["expired"], second["backlog"]) == (1, False)
    assert [len(batch) for batch in store.deleted] == [2, 1]
    assert _count(core, "SELECT count(*) FROM expired_vectors") == 3


def test_no_window_no_store_or_no_budget_means_no_pass(app, tmp_path):
    core, ctx = app
    tool = capture(core, ctx, "TEST tool output", origin="tool_observation")
    _embedded(core, aged=[tool])
    store = Store()
    assert _pass(core, ctx, store, tmp_path, days=0) is None
    assert _pass(core, ctx, None, tmp_path) is None
    assert _pass(core, ctx, store, tmp_path, seconds=vector_retention.RESERVE_SECONDS - 0.1) is None
    assert store.deleted == [] and _count(core, "SELECT count(*) FROM expired_vectors") == 0


def test_a_failed_delete_records_nothing_and_is_retried(app, tmp_path):
    core, ctx = app
    tool = capture(core, ctx, "TEST tool output", origin="tool_observation")
    _embedded(core, aged=[tool])

    class Refusing:
        def delete_by_ids(self, ids):
            raise RuntimeError("TEST store down")

    receipt = _pass(core, ctx, Refusing(), tmp_path)
    assert (receipt["outcome"], receipt["error"]) == ("failed", "RuntimeError")
    assert _count(core, "SELECT count(*) FROM expired_vectors") == 0
    assert _pass(core, ctx, Store(), tmp_path, now=NOW + timedelta(hours=2))["expired"] == 1


def test_the_window_is_a_vector_setting_with_a_180_day_default(tmp_path):
    raw = {
        "backend": "sqlite-bruteforce",
        "storage_dir": str(tmp_path / "vectors"),
        "table_name": "TEST",
        "dimensions": 1,
        "test_injection_override": True,
    }
    assert VectorRuntimeConfig.from_mapping(raw).tool_output_retention_days == 180
    assert VectorRuntimeConfig.from_mapping({**raw, "tool_output_retention_days": 0}).tool_output_retention_days == 0
    for bad in (-1, True, "180", 1.5, 36501):
        with pytest.raises(ValueError, match="tool_output_retention_days"):
            VectorRuntimeConfig.from_mapping({**raw, "tool_output_retention_days": bad})


def test_the_doctor_counts_expired_vectors_and_names_the_window(app, tmp_path):
    core, ctx = app
    tool = capture(core, ctx, "TEST tool output", origin="tool_observation")
    _embedded(core, aged=[tool])
    _pass(core, ctx, Store(), tmp_path)
    report = doctor.DoctorReport(host="hermes", status="degraded")
    doctor_store.check_index(
        report, core.storage.path.parent, store_readable=True, config=Config(_vector(tmp_path, 180))
    )
    assert report.index_metadata["expired_vectors"] == 1
    assert report.index_metadata["tool_output_retention_days"] == 180


def test_a_repeated_or_withheld_tool_output_loses_its_vector_at_once(app, tmp_path):
    """What the intake gate now keeps as sources only, older releases embedded:
    the pass clears those whatever their age.  The earliest copy of a repeat
    keeps its vector."""
    core, ctx = app
    first = capture(core, ctx, "TEST identical build output", origin="tool_observation")
    repeat = capture(core, ctx, "TEST identical build output", origin="tool_observation")
    omitted = capture(core, ctx, "Tool execution summary (terminal): output omitted", origin="tool_observation")
    fresh = capture(core, ctx, "TEST a different, fresh output", origin="tool_observation")
    _embedded(core, aged=())  # every source has finished embed work, all of it persisted today
    store = Store()
    receipt = _pass(core, ctx, store, tmp_path)
    assert (receipt["expired"], receipt["by_reason"]) == (2, {"omitted": 1, "repeat": 1})
    assert sorted(store.deleted[0]) == sorted([f"p10:{repeat.ref}@1:{SPACE}", f"p10:{omitted.ref}@1:{SPACE}"])
    with sqlite3.connect(core.storage.path) as conn:
        ledger = dict(conn.execute("SELECT source_ref,reason FROM expired_vectors").fetchall())
    assert ledger == {repeat.ref: "repeat", omitted.ref: "omitted"}
    assert first.ref not in ledger and fresh.ref not in ledger


def test_the_retention_key_names_the_entry(app):
    _core, ctx = app
    base = dict(
        installation_id=ctx.binding.installation_id,
        project_id=ctx.project_id,
        branch_id=ctx.branch_id,
        scope_ids=ctx.allowed_scope_ids,
        days=180,
    )
    assert vector_retention.retention_key(entry_id="entry-one", **base) != vector_retention.retention_key(
        entry_id="entry-two", **base
    )
    assert vector_retention.retention_key(entry_id=None, **base) == vector_retention.retention_key(
        entry_id="local", **base
    )


def test_the_planner_reads_the_entry_from_the_config(tmp_path):
    vector = _vector(tmp_path, 60)
    binding = SimpleNamespace(installation_id="TEST-installation", data_directory=tmp_path)

    def context(entry_id):
        return SimpleNamespace(
            binding=binding,
            entry_id=entry_id,
            project_id="TEST-project",
            branch_id="TEST-main",
            allowed_scope_ids=frozenset({"TEST-scope"}),
        )

    config = SimpleNamespace(
        vector=vector,
        drain_seconds=120.0,
        binding=binding,
        project_id="TEST-project",
        branch_id="TEST-main",
        allowed_scope_ids=frozenset({"TEST-scope"}),
        entry_id="entry-one",
        context=lambda: context("entry-one"),
    )
    assert _retention_due_at(config, now=NOW) == NOW
    key = vector_retention.retention_key(
        installation_id=binding.installation_id,
        entry_id="entry-one",
        project_id=config.project_id,
        branch_id=config.branch_id,
        scope_ids=config.allowed_scope_ids,
        days=60,
    )
    vector_retention.write_state(
        vector.storage_dir, key, {"retention_days": 60, "finished_at": NOW.isoformat().replace("+00:00", "Z")}
    )
    later = NOW + timedelta(minutes=5)
    assert _retention_due_at(config, now=later) == NOW + vector_retention.PASS_INTERVAL
    other_entry = SimpleNamespace(**{**vars(config), "entry_id": "entry-two", "context": lambda: context("entry-two")})
    assert _retention_due_at(other_entry, now=later) == later


def test_a_shared_scope_expires_only_the_passing_entrys_vectors(shared_app, tmp_path):
    """Two entries write to the same shared scope; one entry's window must not touch the other's vectors."""
    core, ctx = shared_app
    first = replace(ctx, entry_id="entry-one")
    second = replace(ctx, entry_id="entry-two")
    mine = capture(core, first, "TEST entry one old tool output", origin="tool_observation")
    theirs = capture(core, second, "TEST entry two old tool output", origin="tool_observation")
    _embedded(core, aged=(mine, theirs))
    store = Store()
    receipt = _pass(core, first, store, tmp_path)
    assert (receipt["expired"], receipt["by_reason"]) == (1, {"window": 1})
    assert store.deleted == [[f"p10:{mine.ref}@1:{SPACE}"]]
    assert _count(core, "SELECT count(*) FROM expired_vectors WHERE source_ref=?", theirs.ref) == 0


def test_project_branch_and_null_partitions_expire_independently(app, tmp_path):
    core, ctx = app
    mine = replace(ctx, project_id="TEST-project-a", branch_id="TEST-main")
    other_project = replace(ctx, project_id="TEST-project-b", branch_id="TEST-main")
    other_branch = replace(ctx, project_id="TEST-project-a", branch_id="TEST-dev")
    unscoped = replace(ctx, project_id=None, branch_id=None)
    mine_source = capture(core, mine, "TEST project a main old tool", origin="tool_observation")
    project_source = capture(core, other_project, "TEST project b main old tool", origin="tool_observation")
    branch_source = capture(core, other_branch, "TEST project a dev old tool", origin="tool_observation")
    null_source = capture(core, unscoped, "TEST no project old tool", origin="tool_observation")
    _embedded(core, aged=(mine_source, project_source, branch_source, null_source))
    store = Store()
    receipt = _pass(core, mine, store, tmp_path)
    assert (receipt["expired"], receipt["by_reason"]) == (1, {"window": 1})
    assert store.deleted == [[f"p10:{mine_source.ref}@1:{SPACE}"]]


def test_a_repeat_is_only_a_repeat_within_its_entry_project_and_branch(shared_app, tmp_path):
    core, ctx = shared_app
    mine = replace(ctx, entry_id="entry-one", project_id="TEST-project-a", branch_id="TEST-main")
    other_entry = replace(ctx, entry_id="entry-two", project_id="TEST-project-a", branch_id="TEST-main")
    other_branch = replace(ctx, entry_id="entry-one", project_id="TEST-project-a", branch_id="TEST-dev")
    first = capture(core, mine, "TEST identical build output", origin="tool_observation")
    repeat = capture(core, mine, "TEST identical build output", origin="tool_observation")
    other_entry_copy = capture(core, other_entry, "TEST identical build output", origin="tool_observation")
    other_branch_copy = capture(core, other_branch, "TEST identical build output", origin="tool_observation")
    _embedded(core, aged=())
    store = Store()
    receipt = _pass(core, mine, store, tmp_path)
    assert (receipt["expired"], receipt["by_reason"]) == (1, {"repeat": 1})
    assert store.deleted == [[f"p10:{repeat.ref}@1:{SPACE}"]]
    with sqlite3.connect(core.storage.path) as conn:
        ledger = dict(conn.execute("SELECT source_ref,reason FROM expired_vectors").fetchall())
    assert ledger == {repeat.ref: "repeat"}
    assert first.ref not in ledger and other_entry_copy.ref not in ledger and other_branch_copy.ref not in ledger


def test_a_longer_window_on_another_entry_keeps_its_older_vectors(shared_app, tmp_path):
    core, ctx = shared_app
    short = replace(ctx, entry_id="entry-short")
    long = replace(ctx, entry_id="entry-long")
    short_source = capture(core, short, "TEST short window old tool", origin="tool_observation")
    long_source = capture(core, long, "TEST long window old tool", origin="tool_observation")
    _embedded(core, aged=(short_source, long_source), persisted_at=(NOW - timedelta(days=60)).isoformat())
    store = Store()
    receipt = _pass(core, short, store, tmp_path, days=30)
    assert (receipt["expired"], receipt["by_reason"]) == (1, {"window": 1})
    assert store.deleted == [[f"p10:{short_source.ref}@1:{SPACE}"]]
    assert _count(core, "SELECT count(*) FROM expired_vectors WHERE source_ref=?", long_source.ref) == 0
    kept = _pass(core, long, store, tmp_path, days=90)
    assert (kept["outcome"], kept["expired"]) == ("nothing_due", 0)
    assert len(store.deleted) == 1


def test_one_entrys_pass_does_not_suppress_anothers_with_the_same_window(shared_app, tmp_path):
    core, ctx = shared_app
    first = replace(ctx, entry_id="entry-one")
    second = replace(ctx, entry_id="entry-two")
    first_source = capture(core, first, "TEST first entry old tool", origin="tool_observation")
    second_source = capture(core, second, "TEST second entry old tool", origin="tool_observation")
    _embedded(core, aged=(first_source, second_source))
    first_receipt = _pass(core, first, Store(), tmp_path)
    assert first_receipt["expired"] == 1
    second_receipt = _pass(core, second, Store(), tmp_path, now=NOW + timedelta(minutes=5))
    assert second_receipt is not None
    assert (second_receipt["outcome"], second_receipt["expired"]) == ("expired", 1)


def test_concurrent_backoff_writes_replace_distinct_partials(tmp_path, monkeypatch):
    key = "a" * 32
    replacements = []
    barrier = threading.Barrier(2)
    real_replace = os.replace

    def replace_atomically(source, target):
        replacements.append(Path(source).name)
        barrier.wait(timeout=10)
        return real_replace(source, target)

    monkeypatch.setattr(vector_retention.os, "replace", replace_atomically)
    until = NOW + vector_retention.RETRY_BACKOFF
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(
                lambda reason: vector_retention.write_backoff(tmp_path, key, until=until, reason=reason),
                ("first", "second"),
            )
        )
    assert results == [True, True]
    assert len(set(replacements)) == 2
    path = tmp_path / vector_retention.BACKOFF_FILENAME.format(key=key)
    record = json.loads(path.read_text(encoding="utf-8"))
    assert record["schema"] == vector_retention.BACKOFF_SCHEMA
    assert record["reason"] in {"first", "second"}
    assert not list(tmp_path.glob("*.partial"))


def test_a_shared_worker_expires_every_entrys_vectors(shared_app, tmp_path):
    """The worker's context names no entry; falling back to ``local`` would delete nothing."""
    core, ctx = shared_app
    config = _shared_worker(tmp_path, ctx)
    first = capture(
        core, replace(ctx, entry_id="entry-one"), "TEST worker entry one old tool", origin="tool_observation"
    )
    second = capture(
        core, replace(ctx, entry_id="entry-two"), "TEST worker entry two old tool", origin="tool_observation"
    )
    _embedded(core, aged=(first, second))
    store = Store()
    receipt = vector_retention.expire_if_due(
        store, config, core.storage, config.context(), available_seconds=30.0, now=NOW
    )
    assert receipt is not None
    assert (receipt["outcome"], receipt["expired"]) == ("expired", 2)
    space = config.embedding_space_id()
    assert sorted(identifier for batch in store.deleted for identifier in batch) == sorted(
        [f"p10:{first.ref}@1:{space}", f"p10:{second.ref}@1:{space}"]
    )
    records = json.loads(
        (Path(config.vector.storage_dir) / vector_retention.STATE_FILENAME).read_text(encoding="utf-8")
    )["records"]
    keys = {
        vector_retention.retention_key(
            installation_id=config.binding.installation_id,
            entry_id=entry_id,
            project_id=None,
            branch_id=None,
            scope_ids=config.allowed_scope_ids,
            days=180,
        )
        for entry_id in ("entry-one", "entry-two")
    }
    assert keys <= set(records)
    local = vector_retention.retention_key(
        installation_id=config.binding.installation_id,
        entry_id=None,
        project_id=None,
        branch_id=None,
        scope_ids=config.allowed_scope_ids,
        days=180,
    )
    assert local not in records


def test_a_shared_worker_planner_reads_the_partitions_its_pass_wrote(shared_app, tmp_path):
    """A local-key planner would wake on every five-minute tick after the pass wrote entry records."""
    core, ctx = shared_app
    config = _shared_worker(tmp_path, ctx, days=60)
    tool = capture(core, replace(ctx, entry_id="entry-one"), "TEST worker planner old tool", origin="tool_observation")
    _embedded(core, aged=(tool,))
    assert next_wake(config, now=NOW).due_at == _iso(NOW)
    receipt = vector_retention.expire_if_due(
        Store(), config, core.storage, config.context(), available_seconds=30.0, now=NOW
    )
    assert receipt["expired"] == 1
    plan = next_wake(config, now=NOW + timedelta(minutes=5))
    assert plan.reason == "vector_retention"
    due = datetime.fromisoformat(plan.due_at.replace("Z", "+00:00"))
    assert NOW + vector_retention.PASS_INTERVAL <= due <= NOW + vector_retention.PASS_INTERVAL + timedelta(seconds=60)


def test_a_repeat_keeps_the_live_copy_when_the_earlier_copy_is_suppressed(app, tmp_path):
    """A suppressed copy is not what recall returns, so it cannot make the live copy a repeat."""
    core, ctx = app
    first = capture(core, ctx, "TEST identical suppressed output", origin="tool_observation")
    repeat = capture(core, ctx, "TEST identical suppressed output", origin="tool_observation")
    _embedded(core, aged=())
    with core.storage.write(ctx, remaining_seconds=10) as tx:
        target = tx.deletions.target(first.ref, first.revision)
        tx.deletions.retire_revisions((target,), now=_iso(NOW))
    store = Store()
    receipt = _pass(core, ctx, store, tmp_path)
    assert (receipt["expired"], receipt["by_reason"]) == (1, {"repeat": 1})
    assert store.deleted == [[f"p10:{first.ref}@1:{SPACE}"]]
    with sqlite3.connect(core.storage.path) as conn:
        ledger = conn.execute("SELECT source_ref,source_revision,reason FROM expired_vectors").fetchall()
    assert ledger == [(first.ref, 1, "repeat")]
    assert repeat.ref not in {row[0] for row in ledger}


def test_a_superseded_revision_loses_its_vector_and_the_live_revision_keeps_its_own(app, tmp_path):
    """An older generation is unreachable; the live head must not be mistaken for its repeat."""
    core, ctx = app
    old = capture(
        core,
        ctx,
        "TEST revisioned identical output",
        key="TEST-revisioned-output",
        revision=1,
        origin="tool_observation",
    )
    live = capture(
        core,
        ctx,
        "TEST revisioned identical output",
        key="TEST-revisioned-output",
        revision=2,
        origin="tool_observation",
    )
    assert old.ref == live.ref
    _embedded(core, aged=())
    store = Store()
    receipt = _pass(core, ctx, store, tmp_path)
    assert (receipt["expired"], receipt["by_reason"]) == (1, {"repeat": 1})
    assert store.deleted == [[f"p10:{old.ref}@1:{SPACE}"]]
    with sqlite3.connect(core.storage.path) as conn:
        ledger = conn.execute("SELECT source_ref,source_revision,reason FROM expired_vectors").fetchall()
    assert ledger == [(old.ref, 1, "repeat")]


def test_an_expired_copy_does_not_make_its_fresh_repeat_lose_the_only_vector(app, tmp_path):
    """The window deletes the old copy; the fresh copy must keep the content's vector."""
    core, ctx = app
    old = capture(core, ctx, "TEST identical output across the window", origin="tool_observation")
    fresh = capture(core, ctx, "TEST identical output across the window", origin="tool_observation")
    _embedded(core, aged=(old,), persisted_at=(NOW - timedelta(days=181)).isoformat())
    store = Store()
    receipt = _pass(core, ctx, store, tmp_path)
    assert (receipt["outcome"], receipt["expired"], receipt["by_reason"]) == ("expired", 1, {"window": 1})
    assert store.deleted == [[f"p10:{old.ref}@1:{SPACE}"]]
    with sqlite3.connect(core.storage.path) as conn:
        ledger = dict(conn.execute("SELECT source_ref,reason FROM expired_vectors").fetchall())
    assert ledger == {old.ref: "window"}
    assert fresh.ref not in ledger
    # The fresh copy is inside the window, so the next pass has nothing to do.
    assert _pass(core, ctx, store, tmp_path, now=NOW + timedelta(hours=2))["outcome"] == "nothing_due"


def test_a_repeat_of_an_already_expired_copy_keeps_the_fresh_copy(app, tmp_path):
    """A repeat's earlier copy must still have a vector; an expired one cannot make another repeat."""
    core, ctx = app
    first = capture(core, ctx, "TEST chained identical output", origin="tool_observation")
    repeat = capture(core, ctx, "TEST chained identical output", origin="tool_observation")
    first_at = NOW - timedelta(days=10)
    repeat_at = NOW - timedelta(days=9)
    with sqlite3.connect(core.storage.path) as conn:
        conn.executemany(
            "UPDATE source_events SET persisted_at=? WHERE event_id=?",
            [(first_at.isoformat(), first.ref), (repeat_at.isoformat(), repeat.ref)],
        )
        conn.commit()
    _embedded(core, aged=())
    store = Store()
    first_pass = _pass(core, ctx, store, tmp_path)
    assert (first_pass["expired"], first_pass["by_reason"]) == (1, {"repeat": 1})
    assert store.deleted == [[f"p10:{repeat.ref}@1:{SPACE}"]]
    later = NOW + timedelta(days=171)
    fresh = capture(core, ctx, "TEST chained identical output", origin="tool_observation")
    with sqlite3.connect(core.storage.path) as conn:
        conn.execute("UPDATE source_events SET persisted_at=? WHERE event_id=?", (later.isoformat(), fresh.ref))
        conn.commit()
    _embedded(core, aged=())
    second_pass = _pass(core, ctx, store, tmp_path, now=later)
    assert (second_pass["expired"], second_pass["by_reason"]) == (1, {"window": 1})
    assert store.deleted[-1] == [f"p10:{first.ref}@1:{SPACE}"]
    with sqlite3.connect(core.storage.path) as conn:
        ledger = dict(conn.execute("SELECT source_ref,reason FROM expired_vectors").fetchall())
    assert ledger == {repeat.ref: "repeat", first.ref: "window"}
    assert fresh.ref not in ledger


def test_a_shared_worker_registry_read_failure_is_visible_and_installation_backed_off(
    shared_app, tmp_path, monkeypatch
):
    """A registry read that fails leaves no partition record; the pass says failed and the planner retries."""
    core, ctx = shared_app
    config = _shared_worker(tmp_path, ctx, days=60)

    def unreadable(_registry):
        raise sqlite3.OperationalError("TEST registry unreadable")

    monkeypatch.setattr(Registry, "entries", unreadable)
    receipt = vector_retention.expire_if_due(
        Store(), config, core.storage, config.context(), available_seconds=30.0, now=NOW
    )
    assert receipt is not None
    assert (receipt["outcome"], receipt["stage"], receipt["error"]) == ("failed", "registry", "OperationalError")
    assert (receipt["expired"], receipt["partitions"], receipt["state_written"]) == (0, [], False)
    assert receipt["backoff_written"] is True
    key = vector_retention.installation_backoff_key(config.binding.installation_id)
    data_directory = Path(config.binding.data_directory)
    assert vector_retention.read_backoff(data_directory, key) == NOW + vector_retention.RETRY_BACKOFF
    plan = next_wake(config, now=NOW + timedelta(minutes=5))
    assert (plan.reason, plan.due_at) == (
        "vector_retention_retry",
        _iso(NOW + vector_retention.RETRY_BACKOFF),
    )
    monkeypatch.undo()
    # A readable registry does not run ahead of the installation's retry.
    assert next_wake(config, now=NOW + timedelta(minutes=5)).due_at == _iso(NOW + vector_retention.RETRY_BACKOFF)
    assert next_wake(config, now=NOW + vector_retention.RETRY_BACKOFF).due_at == _iso(
        NOW + vector_retention.RETRY_BACKOFF
    )


def test_a_partition_read_failure_is_visible_and_installation_backed_off(app, tmp_path, monkeypatch):
    """A partition list that cannot be built is a failed pass and an installation-level retry, not idle."""
    core, ctx = app
    tool = capture(core, ctx, "TEST partition read tool output", origin="tool_observation")
    _embedded(core, aged=[tool])

    def unreadable(*_args, **_kwargs):
        raise RuntimeError("TEST partitions unreadable")

    monkeypatch.setattr(vector_retention, "retention_partitions", unreadable)
    receipt = _pass(core, ctx, Store(), tmp_path)
    assert (receipt["outcome"], receipt["stage"], receipt["error"]) == ("failed", "partitions", "RuntimeError")
    key = vector_retention.installation_backoff_key(ctx.binding.installation_id)
    assert vector_retention.read_backoff(tmp_path, key) == NOW + vector_retention.RETRY_BACKOFF
    monkeypatch.setattr(scheduling, "retention_partitions", unreadable)
    binding = SimpleNamespace(installation_id=ctx.binding.installation_id, data_directory=tmp_path)
    config = SimpleNamespace(
        vector=_vector(tmp_path, 60),
        drain_seconds=120.0,
        binding=binding,
        project_id=ctx.project_id,
        branch_id=ctx.branch_id,
        allowed_scope_ids=ctx.allowed_scope_ids,
        entry_id=ctx.entry_id,
        context=lambda: ctx,
    )
    assert _retention_due_at(config, now=NOW + timedelta(minutes=5)) == NOW + vector_retention.RETRY_BACKOFF


def test_a_partial_shared_pass_names_failed_partitions_and_counts(shared_app, tmp_path):
    """One entry's store failure must not hide the batch another entry expired."""
    core, ctx = shared_app
    config = _shared_worker(tmp_path, ctx)
    first = capture(
        core, replace(ctx, entry_id="entry-one"), "TEST partial entry one old tool", origin="tool_observation"
    )
    second = capture(
        core, replace(ctx, entry_id="entry-two"), "TEST partial entry two old tool", origin="tool_observation"
    )
    _embedded(core, aged=(first, second))

    class RefusingOne(Store):
        def delete_by_ids(self, ids):
            if any(f"p10:{first.ref}@" in identifier for identifier in ids):
                raise RuntimeError("TEST entry one store down")
            super().delete_by_ids(ids)

    store = RefusingOne()
    receipt = vector_retention.expire_if_due(
        store, config, core.storage, config.context(), available_seconds=30.0, now=NOW
    )
    assert receipt["outcome"] == "partial"
    assert (receipt["partitions_run"], receipt["partitions_succeeded"], receipt["partitions_failed"]) == (4, 3, 1)
    assert (receipt["expired"], receipt["by_reason"]) == (1, {"window": 1})
    failed_key = vector_retention.retention_key(
        installation_id=config.binding.installation_id,
        entry_id="entry-one",
        project_id=None,
        branch_id=None,
        scope_ids=config.allowed_scope_ids,
        days=180,
    )
    assert receipt["errors"] == [{"partition": failed_key, "error": "RuntimeError"}]
    assert store.deleted == [[f"p10:{second.ref}@1:{config.embedding_space_id()}"]]
