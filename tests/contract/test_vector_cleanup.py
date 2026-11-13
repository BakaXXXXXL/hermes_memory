"""Vector cleanup deletes only gone vectors by default, history only on request, and refuses
an unowned target or a missing write barrier."""

from __future__ import annotations

import importlib.util
import json
import os
import sqlite3
import stat
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path

import pytest
from scope_recall.core.schema import SCHEMA_VERSION
from scope_recall.core.writer_lease import TruthWriterBusyError
from scope_recall.maintenance import cli, vector_cleanup
from scope_recall.vector.process_store import ProcessLanceVectorStore
from scope_recall.vector.store import build_vector_store
from test_claims import app, capture, initial, revise_request

SPACE = "TEST-space"
AGENT = "TEST-agent"
INSTALLATION = "TEST-installation"
LANCE = pytest.param(
    "lancedb",
    marks=pytest.mark.skipif(importlib.util.find_spec("lancedb") is None, reason="lancedb is not installed"),
)
BACKENDS = ["sqlite-bruteforce", LANCE]


def _data_dir(tmp_path):
    return (tmp_path / "TEST-claims").resolve()


def _vector_dir(tmp_path, *, space=SPACE):
    return _data_dir(tmp_path) / "vectors" / space


def _store(tmp_path, backend, *, existing=False):
    store = build_vector_store(
        backend,
        storage_dir=_vector_dir(tmp_path),
        table_name="TEST-vectors",
        dimensions=1,
        metric="cosine",
    )
    if existing:
        store.open_existing()
    else:
        store.open()
    return store


def _row(memory_id, ref=None, revision=1, *, space=SPACE, agent=AGENT, installation=INSTALLATION):
    ref = ref or memory_id
    metadata = {
        "object_kind": "claim" if ref.startswith("claim-") else "event",
        "object_ref": ref,
        "object_revision": revision,
        "vector_id": memory_id,
        "embedding_space": space,
        "agent_id": agent,
        "installation_id": installation,
        "logical_scope_id": "TEST-scope",
        "project_id": "TEST-project",
        "branch_id": "TEST-main",
    }
    return {
        "id": memory_id,
        "scope_id": "TEST-scope",
        "source": ref,
        "target": json.dumps(metadata, sort_keys=True),
        "content": "",
        "summary": "",
        "updated_at": "2026-09-06T12:00:00Z",
        "vector": [0.0],
    }


def _seed(core, ctx):
    live_event = capture(core, ctx, "TEST live event")
    suppressed_event = capture(core, ctx, "TEST suppressed event")
    blocked_event = capture(core, ctx, "TEST read-blocked event")
    with sqlite3.connect(core.storage.path) as conn:
        conn.execute("UPDATE source_events SET suppressed=1 WHERE event_id=?", (suppressed_event.ref,))
        conn.execute("UPDATE source_events SET read_blocked=1 WHERE event_id=?", (blocked_event.ref,))
        conn.commit()
    old_claim, _source = initial(core, ctx, value="蓝色")
    newer = capture(core, ctx, "刚才写错了，TEST-project 配色 银色。", when="2026-09-03T12:00:00Z")
    core.revise(ctx, revise_request(old_claim, newer, value="银色"))
    live_claim = core.current_claim(ctx, old_claim.ref)
    assert live_claim is not None and live_claim.revision == 2
    ids = {
        "live_event": f"p10:{live_event.ref}@1:{SPACE}",
        "suppressed_event": f"p10:{suppressed_event.ref}@1:{SPACE}",
        "blocked_event": f"p10:{blocked_event.ref}@1:{SPACE}",
        "old_claim": f"p10:{old_claim.ref}@1:{SPACE}",
        "live_claim": f"p10:{live_claim.ref}@{live_claim.revision}:{SPACE}",
        "missing_event": f"p10:event-{'f' * 64}@1:{SPACE}",
        "unknown": "not-a-vector-id",
    }
    rows = [
        _row(ids["live_event"], live_event.ref),
        _row(ids["suppressed_event"], suppressed_event.ref),
        _row(ids["blocked_event"], blocked_event.ref),
        _row(ids["old_claim"], old_claim.ref, 1),
        _row(ids["live_claim"], live_claim.ref, live_claim.revision),
        _row(ids["missing_event"], "event-" + "f" * 64),
        _row(ids["unknown"], "not-a-vector-id"),
    ]
    return ids, rows


def _target(tmp_path, backend, *, agent=AGENT, installation=INSTALLATION, space=SPACE):
    return vector_cleanup.VectorTarget(
        backend=backend,
        storage_dir=_vector_dir(tmp_path, space=space),
        table_name="TEST-vectors",
        dimensions=1,
        agent_id=agent,
        installation_id=installation,
        metric="cosine",
        embedding_space=space,
        data_directory=_data_dir(tmp_path),
    )


def _minimal_schema(database):
    database = Path(database)
    database.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(database) as conn:
        conn.executescript(
            """
            CREATE TABLE source_events (
                event_id TEXT NOT NULL, source_revision INTEGER NOT NULL,
                source_group_key TEXT NOT NULL, suppressed INTEGER NOT NULL DEFAULT 0,
                read_blocked INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(event_id, source_revision)
            );
            CREATE TABLE claims (
                claim_id TEXT PRIMARY KEY NOT NULL, current_revision INTEGER NOT NULL,
                suppressed INTEGER NOT NULL DEFAULT 0, read_blocked INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE claim_versions (
                claim_id TEXT NOT NULL, revision INTEGER NOT NULL, state TEXT NOT NULL,
                PRIMARY KEY(claim_id, revision)
            );
            CREATE TABLE instance_meta (
                singleton INTEGER PRIMARY KEY, agent_id TEXT NOT NULL,
                installation_id TEXT NOT NULL, data_directory TEXT NOT NULL,
                schema_version INTEGER NOT NULL, memory_epoch INTEGER NOT NULL
            );
            CREATE TABLE object_blocks (
                object_kind TEXT NOT NULL, object_ref TEXT NOT NULL,
                suppressed INTEGER NOT NULL, read_blocked INTEGER NOT NULL,
                PRIMARY KEY(object_kind, object_ref)
            );
            CREATE TABLE restored_absence_blocks (
                object_kind TEXT NOT NULL, object_ref TEXT NOT NULL,
                PRIMARY KEY(object_kind, object_ref)
            );
            """
        )
        conn.execute(
            "INSERT INTO instance_meta VALUES(1,?,?,?,?,7)",
            (AGENT, INSTALLATION, str(database.parent.resolve()), SCHEMA_VERSION),
        )
        conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
        conn.commit()


def _minimal_database(tmp_path):
    database = _data_dir(tmp_path) / "memory.sqlite3"
    _minimal_schema(database)
    return database


@pytest.mark.parametrize("backend", BACKENDS)
def test_dry_run_separates_gone_history_live_and_unknown(app, tmp_path, backend):
    core, ctx = app
    ids, rows = _seed(core, ctx)
    store = _store(tmp_path, backend)
    store.upsert_records(rows)
    store.close()

    inventory = vector_cleanup.scan(core.storage.path, _target(tmp_path, backend))

    assert inventory.categories["gone"] == (ids["blocked_event"], ids["missing_event"])
    assert inventory.categories["history"] == (ids["old_claim"], ids["suppressed_event"])
    assert inventory.categories["unknown"] == (ids["unknown"],)
    assert inventory.live_counts == {"live_claim": 1, "live_event": 1}
    assert inventory.reason_counts["gone"] == {"event_missing": 1, "read_blocked": 1}
    assert inventory.reason_counts["history"] == {"revision_superseded": 1, "suppressed": 1}

    default = vector_cleanup.build_manifest(inventory)
    assert default["delete_ids"] == sorted([ids["blocked_event"], ids["missing_event"]])
    assert default["delete_categories"] == ["gone"]
    assert "may still reach the SQLite object" in default["history_semantics"]
    assert "readable only with history" not in default["history_semantics"]

    history = vector_cleanup.build_manifest(inventory, include_history=True)
    assert history["delete_ids"] == sorted(
        [ids["blocked_event"], ids["missing_event"], ids["old_claim"], ids["suppressed_event"]]
    )
    assert history["delete_categories"] == ["gone", "history"]


@pytest.mark.parametrize("backend", [LANCE])
def test_apply_deletes_gone_by_default_and_history_only_with_the_flag(app, tmp_path, backend):
    core, ctx = app
    ids, rows = _seed(core, ctx)
    store = _store(tmp_path, backend)
    store.upsert_records(rows)
    store.close()
    target = _target(tmp_path, backend)
    backup = tmp_path / "backup.json"

    default = vector_cleanup.build_manifest(vector_cleanup.scan(core.storage.path, target))
    result = vector_cleanup.apply_manifest(core.storage.path, target, default, backup_manifest=backup)
    assert result["deleted"] == 2
    assert result["compacted"] is False
    assert result["backup_manifest_is_data_backup"] is False
    assert result["write_barrier"]["truth_writer_lease"] == "held"
    assert result["write_barrier"]["vector_write_lock"] == "held"
    assert result["write_barrier"]["non_cooperating_writers_detected"] is False
    assert json.loads(backup.read_text(encoding="utf-8"))["data_backup"] is False

    remaining = _store(tmp_path, backend, existing=True)
    try:
        assert remaining.list_ids() == sorted(
            [ids["suppressed_event"], ids["old_claim"], ids["live_event"], ids["live_claim"], ids["unknown"]]
        )
    finally:
        remaining.close()

    history_manifest = vector_cleanup.build_manifest(
        vector_cleanup.scan(core.storage.path, target), include_history=True
    )
    history_backup = tmp_path / "history-backup.json"
    history = vector_cleanup.apply_manifest(
        core.storage.path, target, history_manifest, backup_manifest=history_backup, include_history=True
    )
    assert history["deleted"] == 2
    remaining = _store(tmp_path, backend, existing=True)
    try:
        assert remaining.list_ids() == sorted([ids["live_event"], ids["live_claim"], ids["unknown"]])
    finally:
        remaining.close()


def test_apply_refuses_a_history_manifest_without_the_flag(app, tmp_path):
    core, ctx = app
    ids, rows = _seed(core, ctx)
    store = _store(tmp_path, "sqlite-bruteforce")
    store.upsert_records(rows)
    store.close()
    target = _target(tmp_path, "sqlite-bruteforce")
    manifest = vector_cleanup.build_manifest(vector_cleanup.scan(core.storage.path, target), include_history=True)

    with pytest.raises(vector_cleanup.VectorCleanupError) as refused:
        vector_cleanup.apply_manifest(core.storage.path, target, manifest, backup_manifest=tmp_path / "backup.json")
    assert refused.value.code == "CATEGORIES_MISMATCH"
    assert not (tmp_path / "backup.json").exists()


def test_apply_refuses_a_manifest_that_no_longer_matches(app, tmp_path):
    pytest.importorskip("lancedb")
    core, ctx = app
    ids, rows = _seed(core, ctx)
    store = _store(tmp_path, "lancedb")
    store.upsert_records(rows)
    store.close()
    target = _target(tmp_path, "lancedb")
    manifest = vector_cleanup.build_manifest(vector_cleanup.scan(core.storage.path, target))
    changed = json.loads(json.dumps(manifest))
    changed["all_ids"].append(f"p10:event-{'0' * 64}@1:{SPACE}")

    with pytest.raises(vector_cleanup.VectorCleanupError) as refused:
        vector_cleanup.apply_manifest(core.storage.path, target, changed, backup_manifest=tmp_path / "backup.json")
    assert refused.value.code == "EXPECTED_MANIFEST_MISMATCH"
    assert not (tmp_path / "backup.json").exists()


def test_apply_refuses_when_the_truth_writer_lease_is_busy(app, tmp_path, monkeypatch):
    core, ctx = app
    _ids, rows = _seed(core, ctx)
    store = _store(tmp_path, "sqlite-bruteforce")
    store.upsert_records(rows)
    store.close()
    target = _target(tmp_path, "sqlite-bruteforce")
    manifest = vector_cleanup.build_manifest(vector_cleanup.scan(core.storage.path, target))

    @contextmanager
    def busy(_database, *, role):
        raise TruthWriterBusyError(role=role, scope="TEST")

    monkeypatch.setattr(vector_cleanup, "holding_truth_writer_lease", busy)
    with pytest.raises(vector_cleanup.VectorCleanupError) as refused:
        vector_cleanup.apply_manifest(core.storage.path, target, manifest, backup_manifest=tmp_path / "backup.json")
    assert refused.value.code == "WRITERS_ACTIVE"


def test_missing_tables_and_an_empty_truth_store_fail_closed(tmp_path):
    database = _minimal_database(tmp_path)
    with sqlite3.connect(database) as conn:
        conn.execute("DROP TABLE claims")
        conn.commit()
    with pytest.raises(vector_cleanup.VectorCleanupError) as missing:
        vector_cleanup.scan(database, _target(tmp_path, "sqlite-bruteforce"))
    assert missing.value.code == "DATABASE_INCOMPATIBLE"
    assert "claims" in missing.value.detail

    database = _data_dir(tmp_path) / "empty.sqlite3"
    _minimal_schema(database)
    memory_id = f"p10:event-{'a' * 64}@1:{SPACE}"
    store = _store(tmp_path, "sqlite-bruteforce")
    store.upsert_records([_row(memory_id, "event-" + "a" * 64)])
    store.close()
    with pytest.raises(vector_cleanup.VectorCleanupError) as empty:
        vector_cleanup.scan(database, _target(tmp_path, "sqlite-bruteforce"))
    assert empty.value.code == "TRUTH_STORE_EMPTY"


@pytest.mark.parametrize("table", ["claims", "claim_versions", "instance_meta"])
def test_each_required_table_is_required(tmp_path, table):
    database = _minimal_database(tmp_path)
    with sqlite3.connect(database) as conn:
        conn.execute(f"DROP TABLE {table}")
        conn.commit()
    with pytest.raises(vector_cleanup.VectorCleanupError) as missing:
        vector_cleanup.scan(database, _target(tmp_path, "sqlite-bruteforce"))
    assert missing.value.code == "DATABASE_INCOMPATIBLE"
    assert table in missing.value.detail


def test_target_identity_space_and_row_metadata_are_checked(app, tmp_path):
    core, ctx = app
    _ids, rows = _seed(core, ctx)
    store = _store(tmp_path, "sqlite-bruteforce")
    store.upsert_records(rows)
    store.close()

    with pytest.raises(vector_cleanup.VectorCleanupError) as identity:
        vector_cleanup.scan(core.storage.path, _target(tmp_path, "sqlite-bruteforce", agent="OTHER-agent"))
    assert identity.value.code == "TARGET_IDENTITY_MISMATCH"

    with pytest.raises(vector_cleanup.VectorCleanupError) as space:
        vector_cleanup.scan(
            core.storage.path,
            replace(_target(tmp_path, "sqlite-bruteforce"), embedding_space="OTHER-space"),
        )
    assert space.value.code == "TARGET_SPACE_MISMATCH"

    bad_id = f"p10:event-{'b' * 64}@1:{SPACE}"
    bad = _row(bad_id, "event-" + "b" * 64, agent="OTHER-agent")
    store = _store(tmp_path, "sqlite-bruteforce")
    store.upsert_records([bad])
    store.close()
    with pytest.raises(vector_cleanup.VectorCleanupError) as metadata:
        vector_cleanup.scan(core.storage.path, _target(tmp_path, "sqlite-bruteforce"))
    assert metadata.value.code == "VECTOR_STORE_IDENTITY_MISMATCH"


def test_runtime_config_supplies_the_binding_identity(app, tmp_path):
    core, ctx = app
    _ids, rows = _seed(core, ctx)
    store = _store(tmp_path, "sqlite-bruteforce")
    store.upsert_records(rows)
    store.close()
    config = tmp_path / "runtime.json"
    config.write_text(
        json.dumps(
            {
                "binding": {
                    "agent_id": AGENT,
                    "installation_id": INSTALLATION,
                    "data_directory": str(_data_dir(tmp_path)),
                },
                "vector": {
                    "backend": "sqlite-bruteforce",
                    "storage_dir": str(_vector_dir(tmp_path)),
                    "table_name": "TEST-vectors",
                    "dimensions": 1,
                    "embedding_space": SPACE,
                },
            }
        ),
        encoding="utf-8",
    )
    manifest = tmp_path / "manifest.json"
    code = vector_cleanup.main(
        ["--database", str(core.storage.path), "--runtime-config", str(config), "--manifest-out", str(manifest)]
    )
    assert code == 0
    assert json.loads(manifest.read_text(encoding="utf-8"))["vector"]["installation_id"] == INSTALLATION


def test_output_files_are_exclusive_and_owner_only(tmp_path):
    path = tmp_path / "manifest.json"
    vector_cleanup._write_json(path, {"version": 1})
    if os.name != "nt":
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
    with pytest.raises(vector_cleanup.VectorCleanupError) as exists:
        vector_cleanup._write_json(path, {"version": 2})
    assert exists.value.code == "OUTPUT_EXISTS"
    assert json.loads(path.read_text(encoding="utf-8")) == {"version": 1}


def test_output_paths_may_not_be_the_database_vector_store_or_each_other(app, tmp_path, capsys):
    core, ctx = app
    _ids, rows = _seed(core, ctx)
    store = _store(tmp_path, "sqlite-bruteforce")
    store.upsert_records(rows)
    store.close()
    common = [
        "--database",
        str(core.storage.path),
        "--vector-dir",
        str(_vector_dir(tmp_path)),
        "--backend",
        "sqlite-bruteforce",
        "--table-name",
        "TEST-vectors",
        "--dimensions",
        "1",
        "--agent-id",
        AGENT,
        "--installation-id",
        INSTALLATION,
        "--embedding-space",
        SPACE,
    ]
    for output, code in (
        (str(core.storage.path), "OUTPUT_PATH_IS_DATABASE"),
        (str(_vector_dir(tmp_path) / "manifest.json"), "OUTPUT_PATH_INSIDE_VECTOR_STORE"),
    ):
        assert vector_cleanup.main([*common, "--manifest-out", output]) == 2
        assert json.loads(capsys.readouterr().out)["code"] == code
    same = tmp_path / "same.json"
    assert vector_cleanup.main([*common, "--manifest-out", str(same), "--report-out", str(same)]) == 2
    assert json.loads(capsys.readouterr().out)["code"] == "OUTPUT_PATH_COLLISION"


def test_report_write_failure_after_apply_is_reported(app, tmp_path, capsys, monkeypatch):
    pytest.importorskip("lancedb")
    core, ctx = app
    _ids, rows = _seed(core, ctx)
    store = _store(tmp_path, "lancedb")
    store.upsert_records(rows)
    store.close()
    target = _target(tmp_path, "lancedb")
    manifest_path = tmp_path / "manifest.json"
    vector_cleanup._write_json(
        manifest_path, vector_cleanup.build_manifest(vector_cleanup.scan(core.storage.path, target))
    )
    report = tmp_path / "report.json"
    real = vector_cleanup._write_json

    def fail_report(path, payload):
        if Path(path).name == report.name:
            raise vector_cleanup.VectorCleanupError("OUTPUT_UNWRITABLE", str(path))
        return real(path, payload)

    monkeypatch.setattr(vector_cleanup, "_write_json", fail_report)
    code = vector_cleanup.main(
        [
            "--database",
            str(core.storage.path),
            "--vector-dir",
            str(_vector_dir(tmp_path)),
            "--backend",
            "lancedb",
            "--table-name",
            "TEST-vectors",
            "--dimensions",
            "1",
            "--agent-id",
            AGENT,
            "--installation-id",
            INSTALLATION,
            "--embedding-space",
            SPACE,
            "--apply",
            "--expected-manifest",
            str(manifest_path),
            "--backup-manifest",
            str(tmp_path / "backup.json"),
            "--report-out",
            str(report),
        ]
    )
    assert code == 2
    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "applied_report_failed"
    assert output["deleted"] == 2
    assert output["report_out_error"]["code"] == "OUTPUT_UNWRITABLE"


def test_instance_meta_data_directory_must_match_the_database(app, tmp_path):
    core, _ctx = app
    with sqlite3.connect(core.storage.path) as conn:
        conn.execute(
            "UPDATE instance_meta SET data_directory=? WHERE singleton=1",
            (str(tmp_path / "OTHER-data"),),
        )
        conn.commit()

    with pytest.raises(vector_cleanup.VectorCleanupError) as refused:
        vector_cleanup.scan(core.storage.path, _target(tmp_path, "sqlite-bruteforce"))
    assert refused.value.code == "TARGET_DATA_DIRECTORY_MISMATCH"
    assert refused.value.field == "database"


def test_runtime_binding_data_directory_must_match_instance_meta(app, tmp_path):
    core, _ctx = app
    other = (tmp_path / "OTHER-data").resolve()
    target = vector_cleanup.VectorTarget.from_runtime_config(
        {
            "binding": {
                "agent_id": AGENT,
                "installation_id": INSTALLATION,
                "data_directory": str(other),
            },
            "vector": {
                "backend": "sqlite-bruteforce",
                "storage_dir": str(other / "vectors" / SPACE),
                "table_name": "TEST-vectors",
                "dimensions": 1,
                "embedding_space": SPACE,
            },
        }
    )

    with pytest.raises(vector_cleanup.VectorCleanupError) as refused:
        vector_cleanup.scan(core.storage.path, target)
    assert refused.value.code == "TARGET_DATA_DIRECTORY_MISMATCH"
    assert refused.value.field == "binding.data_directory"


def test_runtime_config_requires_the_binding_data_directory(tmp_path):
    with pytest.raises(vector_cleanup.VectorCleanupError) as refused:
        vector_cleanup.VectorTarget.from_runtime_config(
            {
                "binding": {"agent_id": AGENT, "installation_id": INSTALLATION},
                "vector": {
                    "backend": "sqlite-bruteforce",
                    "storage_dir": str(_vector_dir(tmp_path)),
                    "table_name": "TEST-vectors",
                    "dimensions": 1,
                },
            }
        )
    assert refused.value.code == "TARGET_DATA_DIRECTORY_REQUIRED"
    assert refused.value.field == "binding.data_directory"


def test_vector_storage_outside_the_instance_is_refused(app, tmp_path):
    core, _ctx = app
    target = replace(
        _target(tmp_path, "sqlite-bruteforce"),
        storage_dir=(tmp_path / "OTHER-data" / "vectors" / SPACE).resolve(),
    )

    with pytest.raises(vector_cleanup.VectorCleanupError) as refused:
        vector_cleanup.scan(core.storage.path, target)
    assert refused.value.code == "TARGET_STORAGE_OUTSIDE_BINDING"
    assert refused.value.field == "vector.storage_dir"


def test_schema_version_must_match_user_version(app, tmp_path):
    core, _ctx = app
    with sqlite3.connect(core.storage.path) as conn:
        conn.execute("PRAGMA user_version=1111")
        conn.commit()

    with pytest.raises(vector_cleanup.VectorCleanupError) as refused:
        vector_cleanup.scan(core.storage.path, _target(tmp_path, "sqlite-bruteforce"))
    assert refused.value.code == "DATABASE_SCHEMA_MISMATCH"
    assert refused.value.detail == {"user_version": 1111, "instance_meta": SCHEMA_VERSION}


def test_an_older_upgrade_chain_schema_is_not_supported_for_cleanup(app, tmp_path):
    core, _ctx = app
    with sqlite3.connect(core.storage.path) as conn:
        conn.execute("UPDATE instance_meta SET schema_version=1109 WHERE singleton=1")
        conn.execute("PRAGMA user_version=1109")
        conn.commit()

    with pytest.raises(vector_cleanup.VectorCleanupError) as refused:
        vector_cleanup.scan(core.storage.path, _target(tmp_path, "sqlite-bruteforce"))
    assert refused.value.code == "DATABASE_SCHEMA_UNSUPPORTED"
    assert refused.value.detail == {"supported": SCHEMA_VERSION, "found": 1109}


def test_apply_refuses_a_store_without_a_physical_write_lock(app, tmp_path):
    core, ctx = app
    _ids, rows = _seed(core, ctx)
    store = _store(tmp_path, "sqlite-bruteforce")
    store.upsert_records(rows)
    store.close()
    target = _target(tmp_path, "sqlite-bruteforce")
    manifest = vector_cleanup.build_manifest(vector_cleanup.scan(core.storage.path, target))
    backup = tmp_path / "backup.json"

    with pytest.raises(vector_cleanup.VectorCleanupError) as refused:
        vector_cleanup.apply_manifest(core.storage.path, target, manifest, backup_manifest=backup)
    assert refused.value.code == "VECTOR_WRITE_LOCK_UNAVAILABLE"
    assert refused.value.field == "vector_write_lock"
    assert not backup.exists()

    remaining = _store(tmp_path, "sqlite-bruteforce", existing=True)
    try:
        assert remaining.list_ids() == sorted(manifest["all_ids"])
    finally:
        remaining.close()


def test_cli_apply_reports_an_unavailable_write_lock(app, tmp_path, capsys):
    core, ctx = app
    _ids, rows = _seed(core, ctx)
    store = _store(tmp_path, "sqlite-bruteforce")
    store.upsert_records(rows)
    store.close()
    target = _target(tmp_path, "sqlite-bruteforce")
    manifest_path = tmp_path / "manifest.json"
    vector_cleanup._write_json(
        manifest_path, vector_cleanup.build_manifest(vector_cleanup.scan(core.storage.path, target))
    )
    backup = tmp_path / "backup.json"

    code = vector_cleanup.main(
        [
            "--database",
            str(core.storage.path),
            "--vector-dir",
            str(_vector_dir(tmp_path)),
            "--backend",
            "sqlite-bruteforce",
            "--table-name",
            "TEST-vectors",
            "--dimensions",
            "1",
            "--agent-id",
            AGENT,
            "--installation-id",
            INSTALLATION,
            "--embedding-space",
            SPACE,
            "--apply",
            "--expected-manifest",
            str(manifest_path),
            "--backup-manifest",
            str(backup),
        ]
    )
    assert code == 2
    output = json.loads(capsys.readouterr().out)
    assert output["code"] == "VECTOR_WRITE_LOCK_UNAVAILABLE"
    assert output["field"] == "vector_write_lock"
    assert not backup.exists()


def test_apply_refuses_the_process_store_without_a_physical_write_lock(app, tmp_path, monkeypatch):
    core, ctx = app
    _ids, rows = _seed(core, ctx)
    store = _store(tmp_path, "sqlite-bruteforce")
    store.upsert_records(rows)
    store.close()
    target = _target(tmp_path, "sqlite-bruteforce")
    manifest = vector_cleanup.build_manifest(vector_cleanup.scan(core.storage.path, target))
    backup = tmp_path / "backup.json"
    process_store = ProcessLanceVectorStore(
        _vector_dir(tmp_path) / "lancedb",
        table_name="TEST-vectors",
        dimensions=1,
    )
    assert not hasattr(process_store, "physical_write_lock")
    monkeypatch.setattr(vector_cleanup, "_open_store", lambda target, *, writable=False: process_store)

    with pytest.raises(vector_cleanup.VectorCleanupError) as refused:
        vector_cleanup.apply_manifest(core.storage.path, target, manifest, backup_manifest=backup)
    assert refused.value.code == "VECTOR_WRITE_LOCK_UNAVAILABLE"
    assert refused.value.detail == {"backend": "lancedb"}
    assert not backup.exists()


class _BusyLock:
    def __enter__(self):
        raise TimeoutError("held by another writer")

    def __exit__(self, *exc_info):
        return False


class _BusyLockStore:
    backend = "lancedb"

    def __init__(self):
        self.timeouts = []

    def physical_write_lock(self, *, timeout_seconds=None):
        self.timeouts.append(timeout_seconds)
        return _BusyLock()

    def close(self):
        pass


def test_apply_lock_wait_is_bounded_and_reports_a_stable_error(app, tmp_path, monkeypatch):
    core, ctx = app
    _ids, rows = _seed(core, ctx)
    store = _store(tmp_path, "sqlite-bruteforce")
    store.upsert_records(rows)
    store.close()
    target = _target(tmp_path, "sqlite-bruteforce")
    manifest = vector_cleanup.build_manifest(vector_cleanup.scan(core.storage.path, target))
    backup = tmp_path / "backup.json"
    busy = _BusyLockStore()
    monkeypatch.setattr(vector_cleanup, "_open_store", lambda target, *, writable=False: busy)

    with pytest.raises(vector_cleanup.VectorCleanupError) as refused:
        vector_cleanup.apply_manifest(core.storage.path, target, manifest, backup_manifest=backup)
    assert refused.value.code == "VECTOR_WRITE_LOCK_BUSY"
    assert refused.value.field == "vector_write_lock"
    assert refused.value.detail["timeout_seconds"] == vector_cleanup.VECTOR_WRITE_LOCK_TIMEOUT_SECONDS
    assert busy.timeouts == [vector_cleanup.VECTOR_WRITE_LOCK_TIMEOUT_SECONDS]
    assert not backup.exists()


def test_cli_registers_vector_cleanup():
    assert "vector-cleanup" in cli._DELEGATED
    with pytest.raises(SystemExit) as exit_code:
        cli.main(["vector-cleanup", "--help"])
    assert exit_code.value.code == 0


def test_lance_list_ids_and_targets_do_not_materialize_every_column(tmp_path):
    pytest.importorskip("lancedb")
    memory_id = f"p10:event-{'c' * 64}@1:{SPACE}"
    store = _store(tmp_path, "lancedb")
    store.upsert_records([_row(memory_id, "event-" + "c" * 64)])
    try:
        store._table_rows = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("full materialization"))
        assert store.list_ids() == [memory_id]
        targets = store.list_id_targets()
        assert [row["id"] for row in targets] == [memory_id]
        assert json.loads(targets[0]["target"])["vector_id"] == memory_id
    finally:
        store.close()
