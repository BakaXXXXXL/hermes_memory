"""Note chunks keep one object identity while their text moves through generations."""

import importlib.util
import json
import os
import sqlite3
import sys
import time
import types
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

import pytest

MODULE = Path(__file__).resolve().parents[1]
CORE_ROOT = Path(
    os.environ.get("NOTE_INDEXER_CORE_ROOT", MODULE.parent / "integration" / "scope-recall-src")
).resolve()
for _name in list(sys.modules):
    if _name == "scope_recall" or _name.startswith("scope_recall."):
        del sys.modules[_name]
_package = types.ModuleType("scope_recall")
_package.__path__ = [str(CORE_ROOT)]
_package.__file__ = str(CORE_ROOT / "__init__.py")
sys.modules["scope_recall"] = _package

from scope_recall.contracts import InstanceBinding, TrustedContext
from scope_recall.core import CoreConfig, MemoryCore
from scope_recall.core.admission import AdmissionPolicy
from scope_recall.core.capture import CaptureReceipt
from scope_recall.core.restore import (
    InstallationMaintenance,
    begin_restore,
    export_deletion_ledger,
    ledger_digest,
    replay_deletion_ledger,
)
from scope_recall.core.source_records import SourceWrite

_spec = importlib.util.spec_from_file_location("note_indexer_under_test", MODULE / "note_indexer.py")
note_indexer = importlib.util.module_from_spec(_spec)
sys.modules["note_indexer_under_test"] = note_indexer
_spec.loader.exec_module(note_indexer)


class Clock:
    now = "2026-11-12T12:00:00Z"

    def utc_now(self):
        return self.now

    def monotonic(self):
        return time.monotonic()


@pytest.fixture
def app(tmp_path):
    scope = "TEST-scope"
    binding = InstanceBinding("TEST-agent", "TEST-installation", tmp_path / "store", frozenset({scope}), True)
    ctx = TrustedContext(binding, "TEST-session", frozenset({scope}), "external_document")
    core = MemoryCore(
        CoreConfig(binding, admission_policy=AdmissionPolicy(consolidation_enabled=False)),
        clock=Clock(),
    )
    core.initialize()
    notes = tmp_path / "notes"
    notes.mkdir()
    return core, ctx, scope, notes, tmp_path / "state.json"


def note_id(path):
    return "note-" + note_indexer.hashlib.sha256(str(path).encode()).hexdigest()[:24]


def key(path, section, part=0):
    return note_indexer.chunk_key(note_id(path), section, part)


def index(core, ctx, scope, notes, state, now=Clock.now):
    return note_indexer.index_notes(core, ctx, notes, state, scope_id=scope, apply=True, now=now)


def chunk_for(path, section):
    _title, _tags, chunks = note_indexer.chunk_note(path)
    return next(chunk for chunk in chunks if chunk["section"] == section)


def record_generation(core, ctx, scope, path, section, revision, text=None):
    chunk = chunk_for(path, section)
    if text is not None:
        chunk = dict(chunk, content=text, hash=note_indexer.compact_hash(text))
    event = note_indexer.chunk_event(path, chunk, key(path, section), revision, Clock.now)
    receipt = core.record_event(ctx, event, scope_id=scope, remaining_seconds=10)
    assert receipt.disposition == "inserted", receipt
    return receipt


def retire(core, ctx, ref, revision):
    with core.storage.write(ctx, remaining_seconds=10) as tx:
        target = tx.deletions.target(ref, revision)
        return tx.deletions.retire_revisions((target,), now=Clock.now)


def tombstone(core, ctx, ref, *, delete=False):
    with core.storage.write(ctx, remaining_seconds=10) as tx:
        target = tx.deletions.target(ref)
        request = {
            "mode": "delete" if delete else "suppress",
            "target_refs": [ref],
            "expected_revisions": {ref: target.revision},
        }
        return tx.deletions.block(request, (target,), now=Clock.now)


def sqlite_backup(source, target):
    with sqlite3.connect(source) as reader, sqlite3.connect(target) as writer:
        reader.backup(writer)


def restore(core, ctx, backup, ledger):
    authority = InstallationMaintenance(replace(ctx, actor_origin="human_direct"))
    begin_restore(core.storage, authority, expected_ledger_sha256=ledger_digest(ledger))
    sqlite_backup(backup, core.storage.path)
    return replay_deletion_ledger(core.storage, authority, ledger)


def test_add_modify_delete_and_text_rollback_use_generations(app):
    core, ctx, scope, notes, state = app
    path = notes / "alpha.md"
    path.write_text("# Alpha\n\n## A\n\nalpha one\n\n## B\n\nbeta one\n", encoding="utf-8")

    first = index(core, ctx, scope, notes, state)
    assert first["inserted"] == 3
    k_a, k_b = key(path, 1), key(path, 2)
    assert core.source_by_event_key(ctx, k_a, 1).event["content"].endswith("alpha one")

    path.write_text("# Alpha\n\n## A\n\nalpha two\n\n## B\n\nbeta one\n", encoding="utf-8")
    second = index(core, ctx, scope, notes, state)
    assert second["inserted"] == 1 and second["updated"] == 1
    ref_a = core.source_by_event_key(ctx, k_a, 2).ref
    assert core.source_by_event_key(ctx, k_a, 1).suppressed
    assert not core.source(ctx, ref_a, 2).suppressed
    assert [hit.revision for hit in core.search_sources(ctx, "alpha two", automatic=True) if hit.ref == ref_a] == [2]

    path.write_text("# Alpha\n\n## A\n\nalpha two\n", encoding="utf-8")
    third = index(core, ctx, scope, notes, state)
    assert third["removed"] == 1
    assert core.source_by_event_key(ctx, k_b, 1).suppressed

    path.write_text("# Alpha\n\n## A\n\nalpha one\n", encoding="utf-8")
    fourth = index(core, ctx, scope, notes, state)
    assert fourth["inserted"] == 1
    assert core.source_by_event_key(ctx, k_a, 2).suppressed
    assert core.source_by_event_key(ctx, k_a, 3).event["content"].endswith("alpha one")


def test_a_removed_chunk_can_return_as_a_new_generation(app):
    core, ctx, scope, notes, state = app
    path = notes / "return.md"
    path.write_text("# Return\n\n## A\n\nalpha one\n\n## B\n\nbeta one\n", encoding="utf-8")
    index(core, ctx, scope, notes, state)
    k_b = key(path, 2)

    path.write_text("# Return\n\n## A\n\nalpha one\n", encoding="utf-8")
    removed = index(core, ctx, scope, notes, state)
    assert removed["removed"] == 1
    assert core.source_by_event_key(ctx, k_b, 1).suppressed

    path.write_text("# Return\n\n## A\n\nalpha one\n\n## B\n\nbeta two\n", encoding="utf-8")
    returned = index(core, ctx, scope, notes, state)
    assert returned["inserted"] == 1
    new = core.source_by_event_key(ctx, k_b, 2)
    assert new is not None and not new.suppressed
    assert [hit.revision for hit in core.search_sources(ctx, "beta two", automatic=True) if hit.ref == new.ref] == [2]


def test_rejected_chunk_retires_previous_generations_and_is_then_terminal(app):
    core, ctx, scope, notes, state = app
    path = notes / "secret.md"
    path.write_text("# Secret\n\n## A\n\nvisible\n", encoding="utf-8")
    assert index(core, ctx, scope, notes, state)["inserted"] == 2
    k_a = key(path, 1)
    ref_a = core.source_by_event_key(ctx, k_a, 1).ref

    path.write_text("# Secret\n\n## A\n\nsk-" + "a" * 24 + "\n", encoding="utf-8")
    rejected = index(core, ctx, scope, notes, state)
    assert rejected["rejected"] == 1 and rejected["skipped_rejected"] == 0
    assert core.source(ctx, ref_a, 1).suppressed

    terminal = index(core, ctx, scope, notes, state)
    assert terminal["skipped_rejected"] == 1 and terminal["rejected"] == 0


def test_unavailable_is_not_a_duplicate_and_is_retried(app, monkeypatch):
    core, ctx, scope, notes, state = app
    path = notes / "gamma.md"
    path.write_text("# Gamma\n\n## A\n\ngamma one\n", encoding="utf-8")
    monkeypatch.setattr(
        core,
        "record_event",
        lambda *args, **kwargs: CaptureReceipt(
            "unavailable", (), "unknown", "unknown", "unknown", error_code="STORAGE_UNAVAILABLE"
        ),
    )

    first = index(core, ctx, scope, notes, state)
    assert first["unavailable"] == 2 and first["duplicate"] == 0
    assert note_indexer.load_state(state)["notes"][note_id(path)]["chunks"] == {}

    monkeypatch.undo()
    second = index(core, ctx, scope, notes, state)
    assert second["inserted"] == 2


def test_a_conflicting_revision_advances_instead_of_being_recorded_as_duplicate(app):
    core, ctx, scope, notes, state = app
    path = notes / "delta.md"
    path.write_text("# Delta\n\n## A\n\ndelta one\n", encoding="utf-8")
    chunk = {"content": "delta one", "hash": note_indexer.compact_hash("delta one")}
    event = note_indexer.chunk_event(path, chunk, "delta-key", 1, Clock.now)
    assert core.record_event(ctx, event, scope_id=scope, remaining_seconds=10).disposition == "inserted"

    receipt = note_indexer._record_with_retry(
        core, ctx, dict(event, content="delta two"), scope_id=scope, revision=1
    )

    assert receipt.disposition == "inserted"
    assert receipt.event_refs[0].revision == 2


def test_legacy_heading_hash_keys_are_retired_as_whole_objects(app):
    core, ctx, scope, notes, state = app
    path = notes / "legacy.md"
    path.write_text("# Legacy\n\n## A\n\nlegacy body\n", encoding="utf-8")
    legacy = key(path, 1) + ":deadbeefdeadbeefdead"
    event = note_indexer.chunk_event(
        path, {"content": "legacy old body", "hash": "old"}, legacy, 1, Clock.now
    )
    assert core.record_event(ctx, event, scope_id=scope, remaining_seconds=10).disposition == "inserted"
    state.write_text(
        note_indexer.json.dumps(
            {
                "notes": {
                    note_id(path): {
                        "path": str(path),
                        "title": "Legacy",
                        "chunks": {legacy: {"hash": "old", "revision": 1, "section": 1, "heading": "A"}},
                        "rejected": {},
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    stats = index(core, ctx, scope, notes, state)

    assert stats["legacy_retired"] == 1
    legacy_ref = core.source_by_event_key(ctx, legacy, 1).ref
    assert core.source(ctx, legacy_ref, 1).suppressed
    assert legacy not in note_indexer.load_state(state)["notes"][note_id(path)]["chunks"]


def test_corrupt_state_is_refused_instead_of_treated_as_empty(app):
    core, ctx, scope, notes, state = app
    path = notes / "corrupt.md"
    path.write_text("# Corrupt\n\n## A\n\nbody\n", encoding="utf-8")
    state.write_text("{", encoding="utf-8")

    with pytest.raises(note_indexer.StateCorruptError):
        index(core, ctx, scope, notes, state)

    state.write_text(note_indexer.json.dumps({"notes": []}), encoding="utf-8")
    with pytest.raises(note_indexer.StateCorruptError):
        index(core, ctx, scope, notes, state)

    state.write_text(
        note_indexer.json.dumps(
            {"notes": {note_id(path): {"chunks": {key(path, 1): "not-an-object"}, "rejected": {}}}}
        ),
        encoding="utf-8",
    )
    with pytest.raises(note_indexer.StateCorruptError):
        index(core, ctx, scope, notes, state)

    state.write_text(
        note_indexer.json.dumps(
            {"notes": {note_id(path): {"chunks": {key(path, 1): {"revision": "2"}}, "rejected": {}}}}
        ),
        encoding="utf-8",
    )
    with pytest.raises(note_indexer.StateCorruptError):
        index(core, ctx, scope, notes, state)


def test_rejected_chunk_can_revert_to_the_previous_legal_text(app):
    core, ctx, scope, notes, state = app
    path = notes / "revert.md"
    path.write_text("# Revert\n\n## A\n\nlegal body\n", encoding="utf-8")
    assert index(core, ctx, scope, notes, state)["inserted"] == 2
    k_a = key(path, 1)
    ref = core.source_by_event_key(ctx, k_a, 1).ref

    path.write_text("# Revert\n\n## A\n\nsk-" + "a" * 24 + "\n", encoding="utf-8")
    rejected = index(core, ctx, scope, notes, state)
    assert rejected["rejected"] == 1
    assert core.source(ctx, ref, 1).suppressed
    assert k_a not in note_indexer.load_state(state)["notes"][note_id(path)]["chunks"]

    path.write_text("# Revert\n\n## A\n\nlegal body\n", encoding="utf-8")
    reverted = index(core, ctx, scope, notes, state)
    assert reverted["inserted"] == 1
    current = core.source_by_event_key(ctx, k_a, 2)
    assert current is not None and not current.suppressed
    hits = [hit for hit in core.search_sources(ctx, "legal body", automatic=True) if hit.ref == ref]
    assert [(hit.revision, hit.suppressed) for hit in hits] == [(2, False)]


def test_failed_generation_retirement_is_retried_on_the_next_pass(app, monkeypatch):
    core, ctx, scope, notes, state = app
    path = notes / "retry.md"
    path.write_text("# Retry\n\n## A\n\nold body\n", encoding="utf-8")
    index(core, ctx, scope, notes, state)
    k_a = key(path, 1)
    ref = core.source_by_event_key(ctx, k_a, 1).ref

    path.write_text("# Retry\n\n## A\n\nnew body\n", encoding="utf-8")

    def fail(*args, **kwargs):
        raise RuntimeError("retire unavailable")

    monkeypatch.setattr(note_indexer, "_retire_below", fail)
    first = index(core, ctx, scope, notes, state)
    assert first["failed"] == 1
    entry = note_indexer.load_state(state)["notes"][note_id(path)]["chunks"][k_a]
    assert entry["retire_below"] == 2
    assert not core.source(ctx, ref, 1).suppressed

    monkeypatch.undo()
    second = index(core, ctx, scope, notes, state)
    assert second["retired"] == 1
    assert core.source(ctx, ref, 1).suppressed
    entry = note_indexer.load_state(state)["notes"][note_id(path)]["chunks"][k_a]
    assert "retire_below" not in entry


def test_stale_state_after_an_insert_retires_every_older_generation(app):
    core, ctx, scope, notes, state = app
    path = notes / "stale.md"
    path.write_text("# Stale\n\n## A\n\nfirst body\n", encoding="utf-8")
    index(core, ctx, scope, notes, state)
    k_a = key(path, 1)
    ref = core.source_by_event_key(ctx, k_a, 1).ref

    path.write_text("# Stale\n\n## A\n\nsecond body\n", encoding="utf-8")
    _title, _tags, chunks = note_indexer.chunk_note(path)
    chunk = next(item for item in chunks if item["section"] == 1)
    event = note_indexer.chunk_event(path, chunk, k_a, 2, Clock.now)
    assert core.record_event(ctx, event, scope_id=scope, remaining_seconds=10).disposition == "inserted"
    # State still says revision 1; the next pass must not leave revision 2 active.
    assert note_indexer.load_state(state)["notes"][note_id(path)]["chunks"][k_a]["revision"] == 1

    result = index(core, ctx, scope, notes, state)
    assert result["inserted"] == 1
    assert core.source(ctx, ref, 1).suppressed
    assert core.source(ctx, ref, 2).suppressed
    current = core.source_by_event_key(ctx, k_a, 3)
    assert current is not None and not current.suppressed


def test_removed_chunks_without_a_store_generation_are_forgotten(app):
    core, ctx, scope, notes, state = app
    path = notes / "absent.md"
    path.write_text("# Absent\n\n## A\n\nkept\n\n## B\n\nremoved\n", encoding="utf-8")
    index(core, ctx, scope, notes, state)
    k_b = key(path, 2)
    ref_b = core.source_by_event_key(ctx, k_b, 1).ref
    ghost_key = key(path, 9)
    saved = note_indexer.load_state(state)
    saved["notes"][note_id(path)]["chunks"][ghost_key] = {
        "hash": "0" * 20,
        "revision": 1,
        "section": 9,
        "heading": "Ghost",
    }
    state.write_text(note_indexer.json.dumps(saved), encoding="utf-8")
    path.write_text("# Absent\n\n## A\n\nkept\n", encoding="utf-8")

    stats = index(core, ctx, scope, notes, state)
    assert stats["failed"] == 0
    assert stats["removed"] == 2
    assert core.source(ctx, ref_b, 1).suppressed
    chunks = note_indexer.load_state(state)["notes"][note_id(path)]["chunks"]
    assert k_b not in chunks and ghost_key not in chunks

    again = index(core, ctx, scope, notes, state)
    assert again["failed"] == 0 and again["removed"] == 0


def test_vanished_note_without_a_store_generation_is_forgotten(app):
    core, ctx, scope, notes, state = app
    path = notes / "gone.md"
    path.write_text("# Gone\n\n## A\n\nbody\n", encoding="utf-8")
    index(core, ctx, scope, notes, state)
    k_a = key(path, 1)
    ref_a = core.source_by_event_key(ctx, k_a, 1).ref
    ghost_key = key(path, 9)
    saved = note_indexer.load_state(state)
    saved["notes"][note_id(path)]["chunks"][ghost_key] = {
        "hash": "0" * 20,
        "revision": 1,
        "section": 9,
        "heading": "Ghost",
    }
    state.write_text(note_indexer.json.dumps(saved), encoding="utf-8")
    path.unlink()

    stats = index(core, ctx, scope, notes, state)
    assert stats["failed"] == 0
    assert stats["removed"] == 3
    assert core.source(ctx, ref_a, 1).suppressed
    assert note_id(path) not in note_indexer.load_state(state)["notes"]

    again = index(core, ctx, scope, notes, state)
    assert again["failed"] == 0 and again["removed"] == 0


def test_rejection_crash_before_checkpoint_reindexes_reverted_text(app, monkeypatch):
    core, ctx, scope, notes, state = app
    path = notes / "crash.md"
    path.write_text("# Crash\n\n## A\n\nlegal body\n", encoding="utf-8")
    assert index(core, ctx, scope, notes, state)["inserted"] == 2
    k_a = key(path, 1)
    ref = core.source_by_event_key(ctx, k_a, 1).ref

    path.write_text("# Crash\n\n## A\n\nsk-" + "a" * 24 + "\n", encoding="utf-8")
    real_save = note_indexer.save_state

    def crash_at_checkpoint(target, value):
        entry = value.get("notes", {}).get(note_id(path), {})
        if k_a in entry.get("rejected", {}):
            raise RuntimeError("crash before checkpoint")
        real_save(target, value)

    monkeypatch.setattr(note_indexer, "save_state", crash_at_checkpoint)
    with pytest.raises(RuntimeError, match="crash before checkpoint"):
        index(core, ctx, scope, notes, state)

    saved = note_indexer.load_state(state)
    pending = saved["notes"][note_id(path)]["chunks"][k_a]
    assert pending["retire_all"] is True
    assert k_a not in saved["notes"][note_id(path)]["rejected"]
    assert core.source(ctx, ref, 1).suppressed

    monkeypatch.undo()
    path.write_text("# Crash\n\n## A\n\nlegal body\n", encoding="utf-8")
    recovered = index(core, ctx, scope, notes, state)
    assert recovered["inserted"] == 1
    current = core.source_by_event_key(ctx, k_a, 2)
    assert current is not None and not current.suppressed
    hits = [hit for hit in core.search_sources(ctx, "legal body", automatic=True) if hit.ref == ref]
    assert [(hit.revision, hit.suppressed) for hit in hits] == [(2, False)]
    entry = note_indexer.load_state(state)["notes"][note_id(path)]["chunks"][k_a]
    assert "retire_all" not in entry


def test_rejection_crash_before_checkpoint_still_finalizes_the_rejection(app, monkeypatch):
    core, ctx, scope, notes, state = app
    path = notes / "crash-final.md"
    path.write_text("# Crash\n\n## A\n\nlegal body\n", encoding="utf-8")
    index(core, ctx, scope, notes, state)
    k_a = key(path, 1)
    ref = core.source_by_event_key(ctx, k_a, 1).ref

    path.write_text("# Crash\n\n## A\n\nsk-" + "a" * 24 + "\n", encoding="utf-8")
    rejected_chunk = next(chunk for chunk in note_indexer.chunk_note(path)[2] if chunk["section"] == 1)
    real_save = note_indexer.save_state

    def crash_at_checkpoint(target, value):
        entry = value.get("notes", {}).get(note_id(path), {})
        if k_a in entry.get("rejected", {}):
            raise RuntimeError("crash before checkpoint")
        real_save(target, value)

    monkeypatch.setattr(note_indexer, "save_state", crash_at_checkpoint)
    with pytest.raises(RuntimeError, match="crash before checkpoint"):
        index(core, ctx, scope, notes, state)

    saved = note_indexer.load_state(state)["notes"][note_id(path)]
    assert saved["chunks"][k_a]["retire_all"] is True
    assert k_a not in saved["rejected"]
    monkeypatch.undo()

    recovered = index(core, ctx, scope, notes, state)
    assert recovered["rejected"] == 1
    assert recovered["skipped_rejected"] == 0
    saved = note_indexer.load_state(state)["notes"][note_id(path)]
    assert k_a not in saved["chunks"]
    assert saved["rejected"][k_a]["hash"] == rejected_chunk["hash"]

    path.write_text("# Crash\n\n## A\n\nlegal body\n", encoding="utf-8")
    reverted = index(core, ctx, scope, notes, state)
    assert reverted["inserted"] == 1
    current = core.source_by_event_key(ctx, k_a, 2)
    assert current is not None and not current.suppressed
    assert core.source(ctx, ref, 1).suppressed


def test_save_state_fsyncs_the_file_and_its_directory(tmp_path, monkeypatch):
    real_fsync = note_indexer.os.fsync
    calls = []

    def counting_fsync(fd):
        calls.append(fd)
        return real_fsync(fd)

    monkeypatch.setattr(note_indexer.os, "fsync", counting_fsync)
    path = tmp_path / "state.json"
    note_indexer.save_state(path, {"notes": {}})

    assert len(calls) >= 2
    assert not path.with_name(path.name + ".tmp").exists()
    assert note_indexer.load_state(path) == {"notes": {}}


def test_the_suite_imports_the_integration_core():
    import scope_recall

    assert Path(scope_recall.__path__[0]).resolve() == CORE_ROOT


def test_import_does_not_rewrite_the_core_work_types():
    from scope_recall.core import admission

    assert admission.WORK_TYPES == frozenset({"consolidate", "embed"})


def test_core_config_disables_consolidation_through_admission_policy(tmp_path):
    binding = InstanceBinding(
        "TEST-agent", "TEST-installation", tmp_path / "store", frozenset({"TEST-scope"}), True
    )
    config = note_indexer.core_config(binding)
    assert config.admission_policy == AdmissionPolicy(consolidation_enabled=False)


def test_missing_core_api_fails_before_state_is_touched(tmp_path):
    notes = tmp_path / "notes"
    notes.mkdir()
    state = tmp_path / "state.json"
    state.write_text("sentinel", encoding="utf-8")

    class IncompleteCore:
        storage = object()

    with pytest.raises(note_indexer.CoreCompatibilityError):
        note_indexer.index_notes(IncompleteCore(), None, notes, state, scope_id="scope", apply=True)

    assert state.read_text(encoding="utf-8") == "sentinel"


def test_missing_source_identity_fails_before_state_is_touched(tmp_path, monkeypatch):
    real = note_indexer._resolve_core_symbol

    def missing(module_name, dotted):
        if dotted == "Sources.source_identity":
            raise note_indexer.CoreCompatibilityError("missing Sources.source_identity")
        return real(module_name, dotted)

    monkeypatch.setattr(note_indexer, "_resolve_core_symbol", missing)
    notes = tmp_path / "notes"
    notes.mkdir()
    state = tmp_path / "state.json"
    state.write_text("sentinel", encoding="utf-8")
    binding = InstanceBinding("TEST-agent", "TEST-installation", tmp_path / "store", frozenset({"TEST-scope"}), True)
    core = MemoryCore(
        CoreConfig(binding, admission_policy=AdmissionPolicy(consolidation_enabled=False)), clock=Clock()
    )

    with pytest.raises(note_indexer.CoreCompatibilityError, match="source_identity"):
        note_indexer.index_notes(core, None, notes, state, scope_id="TEST-scope", apply=True)

    assert state.read_text(encoding="utf-8") == "sentinel"


def test_core_version_mismatch_fails_before_state_is_touched(tmp_path, monkeypatch):
    monkeypatch.setattr(note_indexer, "core_version", lambda: (3, 10, 2))
    notes = tmp_path / "notes"
    notes.mkdir()
    state = tmp_path / "state.json"
    state.write_text("sentinel", encoding="utf-8")

    with pytest.raises(note_indexer.CoreCompatibilityError, match="does not match"):
        note_indexer.index_notes(object(), None, notes, state, scope_id="scope", apply=True)

    assert state.read_text(encoding="utf-8") == "sentinel"


def test_core_version_at_the_required_minimum_is_accepted(app, monkeypatch):
    core, ctx, scope, notes, state = app
    monkeypatch.setattr(note_indexer, "core_version", lambda: (3, 10, 3))

    stats = note_indexer.index_notes(core, ctx, notes, state, scope_id=scope, apply=False)

    assert stats["notes"] == 0
    assert not state.exists()


def test_main_returns_nonzero_when_the_core_api_is_missing(tmp_path, monkeypatch):
    notes = tmp_path / "notes"
    notes.mkdir()
    state = tmp_path / "state.json"
    config = {
        "binding": {
            "agent_id": "TEST-agent",
            "installation_id": "TEST-installation",
            "data_directory": str(tmp_path / "store"),
            "scope_ids": [note_indexer.SCOPE_ID],
            "test_mode": True,
        },
        "session_id": "note-indexer",
        "allowed_scope_ids": [note_indexer.SCOPE_ID],
    }
    config_path = tmp_path / "runtime.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    monkeypatch.setattr(note_indexer, "CONFIG_PATH", config_path)
    monkeypatch.setattr(note_indexer, "NOTES_DIR", notes)
    monkeypatch.setattr(note_indexer, "STATE_PATH", state)
    monkeypatch.setattr(note_indexer, "MemoryCore", lambda config: object())
    monkeypatch.setattr(sys, "argv", ["note_indexer.py"])

    assert note_indexer.main() == 4
    assert not state.exists()


def test_main_returns_nonzero_when_the_core_version_is_incompatible(tmp_path, monkeypatch):
    notes = tmp_path / "notes"
    notes.mkdir()
    state = tmp_path / "state.json"
    config = {
        "binding": {
            "agent_id": "TEST-agent",
            "installation_id": "TEST-installation",
            "data_directory": str(tmp_path / "store"),
            "scope_ids": [note_indexer.SCOPE_ID],
            "test_mode": True,
        },
        "session_id": "note-indexer",
        "allowed_scope_ids": [note_indexer.SCOPE_ID],
    }
    config_path = tmp_path / "runtime.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    monkeypatch.setattr(note_indexer, "CONFIG_PATH", config_path)
    monkeypatch.setattr(note_indexer, "NOTES_DIR", notes)
    monkeypatch.setattr(note_indexer, "STATE_PATH", state)
    monkeypatch.setattr(note_indexer, "core_version", lambda: (2, 9, 0))
    monkeypatch.setattr(note_indexer, "MemoryCore", lambda config: object())
    monkeypatch.setattr(sys, "argv", ["note_indexer.py"])

    assert note_indexer.main() == 4
    assert not state.exists()


def test_main_applies_notes_end_to_end_with_a_local_store(tmp_path, monkeypatch):
    scope = note_indexer.SCOPE_ID
    binding = InstanceBinding("TEST-agent", "TEST-installation", tmp_path / "store", frozenset({scope}), True)
    MemoryCore(
        CoreConfig(binding, admission_policy=AdmissionPolicy(consolidation_enabled=False)), clock=Clock()
    ).initialize()
    notes = tmp_path / "notes"
    notes.mkdir()
    path = notes / "end-to-end.md"
    path.write_text("## A\n\nend to end body\n", encoding="utf-8")
    state = tmp_path / "state.json"
    config = {
        "binding": {
            "agent_id": "TEST-agent",
            "installation_id": "TEST-installation",
            "data_directory": str(tmp_path / "store"),
            "scope_ids": [scope],
            "test_mode": True,
        },
        "session_id": "note-indexer",
        "allowed_scope_ids": [scope],
    }
    config_path = tmp_path / "runtime.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    monkeypatch.setattr(note_indexer, "CONFIG_PATH", config_path)
    monkeypatch.setattr(note_indexer, "NOTES_DIR", notes)
    monkeypatch.setattr(note_indexer, "STATE_PATH", state)
    monkeypatch.setattr(sys, "argv", ["note_indexer.py", "--apply"])

    assert note_indexer.main() == 0
    saved = note_indexer.load_state(state)
    assert saved["notes"][note_id(path)]["chunks"]
    core = MemoryCore(
        CoreConfig(binding, admission_policy=AdmissionPolicy(consolidation_enabled=False)), clock=Clock()
    )
    ctx = TrustedContext(binding, "TEST-session", frozenset({scope}), "external_document")
    current = core.source_by_event_key(ctx, key(path, 0), 1)
    assert current is not None and not current.suppressed
    assert current.event["content"].endswith("end to end body")


def test_save_state_uses_a_unique_temporary_file(tmp_path, monkeypatch):
    sources = []
    real_replace = note_indexer.os.replace

    def replace(source, target):
        sources.append(Path(source))
        return real_replace(source, target)

    monkeypatch.setattr(note_indexer.os, "replace", replace)
    path = tmp_path / "state.json"
    note_indexer.save_state(path, {"notes": {}})
    note_indexer.save_state(path, {"notes": {}})

    assert len(sources) == 2
    assert sources[0] != sources[1]
    assert all(source.name != "state.json.tmp" for source in sources)
    assert not [item for item in tmp_path.iterdir() if item.name.endswith(".tmp")]


def test_save_state_survives_concurrent_writers(tmp_path):
    path = tmp_path / "state.json"

    def write(index):
        note_indexer.save_state(path, {"notes": {}, "writer": index})

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(write, range(32)))

    assert note_indexer.load_state(path)["notes"] == {}
    assert not [item for item in tmp_path.iterdir() if item.name.endswith(".tmp")]


def test_duplicate_after_a_crash_with_a_newer_generation_reindexes_the_reverted_text(app):
    core, ctx, scope, notes, state = app
    path = notes / "crash-revert.md"
    path.write_text("## A\n\nfirst body\n", encoding="utf-8")
    assert index(core, ctx, scope, notes, state)["inserted"] == 1
    k_a = key(path, 0)
    ref = core.source_by_event_key(ctx, k_a, 1).ref

    # A newer generation reached the store, then the process stopped before
    # saving the state.  The note has already reverted to the first text.
    path.write_text("## A\n\nsecond body\n", encoding="utf-8")
    record_generation(core, ctx, scope, path, 0, 2)
    path.write_text("## A\n\nfirst body\n", encoding="utf-8")

    result = index(core, ctx, scope, notes, state)
    assert result["duplicate"] == 0
    assert result["inserted"] == 1
    assert core.source(ctx, ref, 2).suppressed
    current = core.source_by_event_key(ctx, k_a, 3)
    assert current is not None and not current.suppressed
    assert current.event["content"].endswith("first body")
    hits = [hit for hit in core.search_sources(ctx, "first body", automatic=True) if hit.ref == ref]
    assert [(hit.revision, hit.suppressed) for hit in hits] == [(3, False)]


@pytest.mark.parametrize("concurrent_revision", [2, 7])
def test_duplicate_after_a_concurrent_high_revision_reindexes(app, concurrent_revision):
    core, ctx, scope, notes, state = app
    path = notes / "concurrent.md"
    path.write_text("## A\n\nfirst body\n", encoding="utf-8")
    index(core, ctx, scope, notes, state)
    k_a = key(path, 0)
    ref = core.source_by_event_key(ctx, k_a, 1).ref

    path.write_text("## A\n\nother body\n", encoding="utf-8")
    record_generation(core, ctx, scope, path, 0, concurrent_revision)
    path.write_text("## A\n\nfirst body\n", encoding="utf-8")

    result = index(core, ctx, scope, notes, state)
    assert result["duplicate"] == 0
    assert result["inserted"] == 1
    assert core.source(ctx, ref, concurrent_revision).suppressed
    current = core.source_by_event_key(ctx, k_a, concurrent_revision + 1)
    assert current is not None and not current.suppressed
    assert current.event["content"].endswith("first body")


def test_duplicate_after_ledger_replay_reindexes_when_the_state_revision_is_missing(app, tmp_path):
    core, ctx, scope, notes, state = app
    path = notes / "ledger-missing.md"
    path.write_text("## A\n\nfirst body\n", encoding="utf-8")
    index(core, ctx, scope, notes, state)
    k_a = key(path, 0)
    ref = core.source_by_event_key(ctx, k_a, 1).ref

    backup = tmp_path / "ledger-missing.sqlite3"
    sqlite_backup(core.storage.path, backup)

    path.write_text("## A\n\nsecond body\n", encoding="utf-8")
    record_generation(core, ctx, scope, path, 0, 2)
    saved = note_indexer.load_state(state)
    chunk = chunk_for(path, 0)
    saved["notes"][note_id(path)]["chunks"][k_a] = {
        "hash": chunk["hash"],
        "revision": 2,
        "section": 0,
        "heading": chunk["heading"],
    }
    note_indexer.save_state(state, saved)
    retire(core, ctx, ref, 2)
    authority = InstallationMaintenance(replace(ctx, actor_origin="human_direct"))
    ledger = export_deletion_ledger(core.storage, authority)

    restore(core, ctx, backup, ledger)
    assert core.source_by_event_key(ctx, k_a, 1) is not None
    assert not core.source(ctx, ref, 1).suppressed
    assert core.source_by_event_key(ctx, k_a, 2) is None

    result = index(core, ctx, scope, notes, state)
    assert result["duplicate"] == 0
    assert result["inserted"] == 1
    assert core.source(ctx, ref, 1).suppressed
    current = core.source_by_event_key(ctx, k_a, 3)
    assert current is not None and not current.suppressed
    assert current.event["content"].endswith("second body")


def test_duplicate_after_ledger_replay_reindexes_when_the_state_revision_is_suppressed(app, tmp_path):
    core, ctx, scope, notes, state = app
    path = notes / "ledger-suppressed.md"
    path.write_text("## A\n\nfirst body\n", encoding="utf-8")
    index(core, ctx, scope, notes, state)
    k_a = key(path, 0)
    ref = core.source_by_event_key(ctx, k_a, 1).ref

    path.write_text("## A\n\nsecond body\n", encoding="utf-8")
    index(core, ctx, scope, notes, state)
    backup = tmp_path / "ledger-suppressed.sqlite3"
    sqlite_backup(core.storage.path, backup)

    retire(core, ctx, ref, 2)
    authority = InstallationMaintenance(replace(ctx, actor_origin="human_direct"))
    ledger = export_deletion_ledger(core.storage, authority)

    restore(core, ctx, backup, ledger)
    assert core.source(ctx, ref, 2).suppressed
    assert core.source_by_event_key(ctx, k_a, 2) is not None

    result = index(core, ctx, scope, notes, state)
    assert result["duplicate"] == 0
    assert result["inserted"] == 1
    assert core.source(ctx, ref, 2).suppressed
    current = core.source_by_event_key(ctx, k_a, 3)
    assert current is not None and not current.suppressed
    assert current.event["content"].endswith("second body")


def test_a_suppressed_newer_generation_shadows_the_recorded_one(app):
    core, ctx, scope, notes, state = app
    path = notes / "shadowed.md"
    path.write_text("## A\n\nfirst body\n", encoding="utf-8")
    index(core, ctx, scope, notes, state)
    k_a = key(path, 0)
    ref = core.source_by_event_key(ctx, k_a, 1).ref

    # A newer generation was recorded and then retired while the state still
    # pointed at the first one.  The note has reverted to the first text.
    path.write_text("## A\n\nsecond body\n", encoding="utf-8")
    record_generation(core, ctx, scope, path, 0, 2)
    retire(core, ctx, ref, 2)
    path.write_text("## A\n\nfirst body\n", encoding="utf-8")
    assert not core.source(ctx, ref, 1).suppressed
    assert core.source(ctx, ref, 2).suppressed
    assert not [hit for hit in core.search_sources(ctx, "first body", automatic=True) if hit.ref == ref]

    result = index(core, ctx, scope, notes, state)
    assert result["duplicate"] == 0
    assert result["inserted"] == 1
    assert core.source(ctx, ref, 1).suppressed
    assert core.source(ctx, ref, 2).suppressed
    current = core.source_by_event_key(ctx, k_a, 3)
    assert current is not None and not current.suppressed
    assert current.event["content"].endswith("first body")
    hits = [hit for hit in core.search_sources(ctx, "first body", automatic=True) if hit.ref == ref]
    assert [(hit.revision, hit.suppressed) for hit in hits] == [(3, False)]


def test_a_shadowed_duplicate_receipt_does_not_count_as_a_replacement(app, monkeypatch):
    core, ctx, scope, notes, state = app
    path = notes / "shadowed-receipt.md"
    path.write_text("## A\n\nfirst body\n", encoding="utf-8")
    index(core, ctx, scope, notes, state)
    k_a = key(path, 0)
    ref = core.source_by_event_key(ctx, k_a, 1).ref

    legacy = k_a + ":deadbeefdeadbeefdead"
    event = note_indexer.chunk_event(path, {"content": "old body", "hash": "old"}, legacy, 1, Clock.now)
    assert core.record_event(ctx, event, scope_id=scope, remaining_seconds=10).disposition == "inserted"
    legacy_ref = core.source_by_event_key(ctx, legacy, 1).ref

    path.write_text("## A\n\nsecond body\n", encoding="utf-8")
    record_generation(core, ctx, scope, path, 0, 2)
    retire(core, ctx, ref, 2)
    path.write_text("## A\n\nfirst body\n", encoding="utf-8")

    saved = note_indexer.load_state(state)
    entry = saved["notes"][note_id(path)]
    entry["chunks"][legacy] = {"hash": "old", "revision": 1, "section": 0, "heading": "A"}
    entry["chunks"][k_a]["hash"] = "stale"
    note_indexer.save_state(state, saved)
    monkeypatch.setattr(
        note_indexer,
        "_record_with_retry",
        lambda *args, **kwargs: CaptureReceipt(
            "duplicate",
            (SourceWrite("duplicate", ref, 1),),
            "persisted",
            "ready",
            "pending",
        ),
    )

    result = index(core, ctx, scope, notes, state)
    assert result["duplicate"] == 1
    assert result["legacy_pending"] == 1
    assert result["legacy_retired"] == 0
    assert legacy in note_indexer.load_state(state)["notes"][note_id(path)]["chunks"]
    assert not core.source(ctx, legacy_ref, 1).suppressed


def test_rejected_hash_is_rechecked_when_a_restore_brought_the_old_generation_back(app, tmp_path):
    core, ctx, scope, notes, state = app
    path = notes / "rejection-restore.md"
    path.write_text("## A\n\nlegal body\n", encoding="utf-8")
    index(core, ctx, scope, notes, state)
    k_a = key(path, 0)
    ref = core.source_by_event_key(ctx, k_a, 1).ref
    backup = tmp_path / "rejection-restore.sqlite3"
    sqlite_backup(core.storage.path, backup)

    path.write_text("## A\n\nsk-" + "a" * 24 + "\n", encoding="utf-8")
    rejected = index(core, ctx, scope, notes, state)
    assert rejected["rejected"] == 1
    assert core.source(ctx, ref, 1).suppressed
    assert k_a in note_indexer.load_state(state)["notes"][note_id(path)]["rejected"]

    sqlite_backup(backup, core.storage.path)
    assert not core.source(ctx, ref, 1).suppressed

    result = index(core, ctx, scope, notes, state)
    assert result["skipped_rejected"] == 0
    assert result["rejected"] == 1
    assert core.source(ctx, ref, 1).suppressed
    saved = note_indexer.load_state(state)["notes"][note_id(path)]
    assert k_a in saved["rejected"]
    assert k_a not in saved["chunks"]


def test_a_stale_rejection_record_does_not_hide_an_active_store_generation(app):
    core, ctx, scope, notes, state = app
    path = notes / "stale-rejection.md"
    path.write_text("## A\n\nlegal body\n", encoding="utf-8")
    index(core, ctx, scope, notes, state)
    k_a = key(path, 0)
    chunk = chunk_for(path, 0)
    saved = note_indexer.load_state(state)
    saved["notes"][note_id(path)]["rejected"][k_a] = {
        "hash": chunk["hash"],
        "section": 0,
        "heading": chunk["heading"],
        "reason": "stale",
        "at": Clock.now,
    }
    note_indexer.save_state(state, saved)

    result = index(core, ctx, scope, notes, state)
    assert result["skipped_rejected"] == 0
    assert result["inserted"] == 1
    assert core.source_by_event_key(ctx, k_a, 1).suppressed
    current = core.source_by_event_key(ctx, k_a, 2)
    assert current is not None and not current.suppressed
    assert k_a not in note_indexer.load_state(state)["notes"][note_id(path)]["rejected"]


def test_a_retired_newer_generation_does_not_hide_an_active_older_one(app):
    core, ctx, scope, notes, state = app
    path = notes / "retired-newer.md"
    path.write_text("## A\n\nlegal body\n", encoding="utf-8")
    index(core, ctx, scope, notes, state)
    k_a = key(path, 0)
    ref = core.source_by_event_key(ctx, k_a, 1).ref
    chunk = chunk_for(path, 0)

    path.write_text("## A\n\nother body\n", encoding="utf-8")
    record_generation(core, ctx, scope, path, 0, 2)
    retire(core, ctx, ref, 2)
    assert core.source(ctx, ref, 1) is not None and not core.source(ctx, ref, 1).suppressed
    assert core.source(ctx, ref, 2).suppressed

    saved = note_indexer.load_state(state)
    saved["notes"][note_id(path)]["rejected"][k_a] = {
        "hash": chunk["hash"],
        "section": 0,
        "heading": chunk["heading"],
        "reason": "stale",
        "at": Clock.now,
    }
    note_indexer.save_state(state, saved)
    path.write_text("## A\n\nlegal body\n", encoding="utf-8")

    result = index(core, ctx, scope, notes, state)
    assert result["skipped_rejected"] == 0
    assert result["inserted"] == 1
    assert core.source(ctx, ref, 1).suppressed
    assert core.source(ctx, ref, 2).suppressed
    current = core.source_by_event_key(ctx, k_a, 3)
    assert current is not None and not current.suppressed
    assert current.event["content"].endswith("legal body")


@pytest.mark.parametrize("mode", ["suppress", "delete"])
def test_a_tombstoned_chunk_is_terminal_and_is_not_retried(app, mode):
    core, ctx, scope, notes, state = app
    path = notes / "tombstone.md"
    path.write_text("## A\n\nbody\n", encoding="utf-8")
    index(core, ctx, scope, notes, state)
    k_a = key(path, 0)
    ref = core.source_by_event_key(ctx, k_a, 1).ref
    tombstone(core, ctx, ref, delete=(mode == "delete"))

    first = index(core, ctx, scope, notes, state)
    assert first["duplicate"] == 0
    assert first["blocked"] == 1
    assert first["failed"] == 0
    assert first["unavailable"] == 0
    saved = note_indexer.load_state(state)["notes"][note_id(path)]
    assert saved["blocked"][k_a]["reason"] == "tombstoned"
    assert k_a not in saved["chunks"]

    second = index(core, ctx, scope, notes, state)
    assert second["blocked"] == 0
    assert second["skipped_blocked"] == 1
    assert second["failed"] == 0
    assert second["unavailable"] == 0
    assert core.source_by_event_key(ctx, k_a, 2) is None


def test_a_legacy_key_waits_for_its_replacement_write(app, monkeypatch):
    core, ctx, scope, notes, state = app
    path = notes / "legacy-wait.md"
    path.write_text("# Legacy\n\n## A\n\nnew body\n", encoding="utf-8")
    legacy = key(path, 1) + ":deadbeefdeadbeefdead"
    event = note_indexer.chunk_event(
        path, {"content": "old body", "hash": "old"}, legacy, 1, Clock.now
    )
    assert core.record_event(ctx, event, scope_id=scope, remaining_seconds=10).disposition == "inserted"
    legacy_ref = core.source_by_event_key(ctx, legacy, 1).ref
    state.write_text(
        note_indexer.json.dumps(
            {
                "notes": {
                    note_id(path): {
                        "path": str(path),
                        "title": "Legacy",
                        "chunks": {legacy: {"hash": "old", "revision": 1, "section": 1, "heading": "A"}},
                        "rejected": {},
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        note_indexer,
        "_record_with_retry",
        lambda *args, **kwargs: CaptureReceipt(
            "unavailable", (), "unknown", "unknown", "unknown", error_code="STORAGE_UNAVAILABLE"
        ),
    )

    first = index(core, ctx, scope, notes, state)
    assert first["legacy_retired"] == 0
    assert first["legacy_pending"] == 1
    assert legacy in note_indexer.load_state(state)["notes"][note_id(path)]["chunks"]
    assert not core.source(ctx, legacy_ref, 1).suppressed

    monkeypatch.undo()
    second = index(core, ctx, scope, notes, state)
    assert second["legacy_retired"] == 1
    assert core.source(ctx, legacy_ref, 1).suppressed
    assert legacy not in note_indexer.load_state(state)["notes"][note_id(path)]["chunks"]


def test_a_legacy_key_is_kept_when_its_replacement_is_rejected(app):
    core, ctx, scope, notes, state = app
    path = notes / "legacy-rejected.md"
    path.write_text("# Legacy\n\n## A\n\nsk-" + "a" * 24 + "\n", encoding="utf-8")
    legacy = key(path, 1) + ":deadbeefdeadbeefdead"
    event = note_indexer.chunk_event(
        path, {"content": "old body", "hash": "old"}, legacy, 1, Clock.now
    )
    assert core.record_event(ctx, event, scope_id=scope, remaining_seconds=10).disposition == "inserted"
    legacy_ref = core.source_by_event_key(ctx, legacy, 1).ref
    state.write_text(
        note_indexer.json.dumps(
            {
                "notes": {
                    note_id(path): {
                        "path": str(path),
                        "title": "Legacy",
                        "chunks": {legacy: {"hash": "old", "revision": 1, "section": 1, "heading": "A"}},
                        "rejected": {},
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    result = index(core, ctx, scope, notes, state)
    assert result["rejected"] == 1
    assert result["legacy_retired"] == 0
    assert result["legacy_pending"] == 1
    assert legacy in note_indexer.load_state(state)["notes"][note_id(path)]["chunks"]
    assert not core.source(ctx, legacy_ref, 1).suppressed
