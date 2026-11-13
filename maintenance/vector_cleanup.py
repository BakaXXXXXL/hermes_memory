"""Dry-run and explicitly confirmed removal of vectors whose SQLite subject is gone.

SQLite is the authority.  The default delete set is ``gone``: a vector whose
object, revision or block makes it unreadable in every mode.  A ``history``
vector -- suppressed or superseded -- is not current.  A history or as-of read
may still reach its SQLite object when scope, read-block and restored-absence
rules admit it, and an exact-reference history read can load the revision, so
it is removed only with an explicit ``--include-history``.  A ``live`` vector
is never removed.

``--apply`` needs the manifest a dry run wrote and a new backup-manifest path.
It acquires the truth-database writer lease and the companion's physical write
lock before it compares and deletes.  A companion without that lock, or one
whose lock is still held after a bounded wait, is refused rather than written
without it.  That excludes cooperating writers; it is not a claim that a writer
ignoring those locks is detected.  The backup manifest records ids only: it is
not a data backup, because every vector is rebuildable from SQLite.

The sqlite companion's ``open_existing`` is read-only, so an apply opens that
same physical file writable after the read-only scan has proved it is there.
The tool never compacts; reclaiming space stays a separate operator decision.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import sys
from collections import Counter
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from ..core.schema import SCHEMA_VERSION
from ..core.writer_lease import TruthWriterBusyError, holding_truth_writer_lease
from ..vector import VectorStore
from ..vector.store import build_vector_store

MANIFEST_SCHEMA = "scope-recall.vector-cleanup.manifest.v1"
BACKUP_SCHEMA = "scope-recall.vector-cleanup.backup.v1"
RESULT_SCHEMA = "scope-recall.vector-cleanup.result.v1"

GONE_CATEGORY = "gone"
HISTORY_CATEGORY = "history"
UNKNOWN_CATEGORY = "unknown"
CATEGORIES = (GONE_CATEGORY, HISTORY_CATEGORY, UNKNOWN_CATEGORY)
DEFAULT_DELETE_CATEGORIES = (GONE_CATEGORY,)
HISTORY_DELETE_CATEGORIES = (GONE_CATEGORY, HISTORY_CATEGORY)

DELETE_BATCH = 500
#: A cleanup waits this long for the companion's cross-process write lock, then refuses.
VECTOR_WRITE_LOCK_TIMEOUT_SECONDS = 30.0
_QUERY_CHUNK = 800
_VECTOR_ID = re.compile(r"^(?:p10:)?(?P<ref>(?:event|claim)-[^@]+)@(?P<revision>[1-9][0-9]*):(?P<space>.+)$")
_SPACE_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_LIVE_CLAIM_STATES = frozenset({"active", "proposed", "disputed"})
_METADATA_KEYS = (
    "object_kind",
    "object_ref",
    "vector_id",
    "embedding_space",
    "agent_id",
    "installation_id",
    "logical_scope_id",
)


class VectorCleanupError(RuntimeError):
    """A refusal or failure with a stable code an operator can act on."""

    def __init__(self, code: str, field: str | None = None, *, detail: Any = None, exit_code: int = 2) -> None:
        super().__init__(code)
        self.code = code
        self.field = field
        self.detail = detail
        self.exit_code = exit_code


@dataclass(frozen=True)
class VectorId:
    """One parsed ``p10:<ref>@<revision>:<space>`` identity, or a legacy one of the same shape."""

    raw: str
    ref: str
    revision: int
    embedding_space: str


@dataclass(frozen=True)
class VectorTarget:
    """The companion store and the installation it is expected to belong to."""

    backend: str
    storage_dir: Path
    table_name: str
    dimensions: int
    agent_id: str
    installation_id: str
    metric: str = "cosine"
    embedding_space: str | None = None
    data_directory: Path | None = None

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any], *, binding: Mapping[str, Any] | None = None) -> VectorTarget:
        if not isinstance(raw, Mapping):
            raise VectorCleanupError("CONFIG_INVALID", "vector")
        binding = binding if isinstance(binding, Mapping) else {}
        data_directory: Path | None = None
        raw_data_directory = binding.get("data_directory")
        if raw_data_directory is not None:
            data_directory = Path(str(raw_data_directory)).expanduser()
            if not data_directory.is_absolute():
                raise VectorCleanupError("CONFIG_INVALID", "binding.data_directory")
            data_directory = data_directory.resolve()
        backend = str(raw.get("backend") or "lancedb").strip().lower()
        if backend == "sqlite":
            backend = "sqlite-bruteforce"
        if backend not in {"lancedb", "sqlite-bruteforce"}:
            raise VectorCleanupError("CONFIG_INVALID", "vector.backend")
        storage_dir = Path(str(raw.get("storage_dir") or "")).expanduser()
        if not storage_dir.is_absolute():
            raise VectorCleanupError("CONFIG_INVALID", "vector.storage_dir")
        table_name = str(raw.get("table_name") or "").strip()
        if not table_name:
            raise VectorCleanupError("CONFIG_INVALID", "vector.table_name")
        dimensions = raw.get("dimensions")
        if type(dimensions) is not int or dimensions < 1:
            raise VectorCleanupError("CONFIG_INVALID", "vector.dimensions")
        agent_id = str(raw.get("agent_id") or binding.get("agent_id") or "").strip()
        installation_id = str(raw.get("installation_id") or binding.get("installation_id") or "").strip()
        if not agent_id or not installation_id:
            raise VectorCleanupError("TARGET_IDENTITY_REQUIRED", "installation_id")
        metric = str(raw.get("metric") or "cosine").strip().lower()
        if not metric:
            raise VectorCleanupError("CONFIG_INVALID", "vector.metric")
        space = raw.get("embedding_space")
        if space is not None and (not isinstance(space, str) or not space.strip()):
            raise VectorCleanupError("CONFIG_INVALID", "vector.embedding_space")
        return cls(
            backend=backend,
            storage_dir=storage_dir.resolve(),
            table_name=table_name,
            dimensions=dimensions,
            agent_id=agent_id,
            installation_id=installation_id,
            metric=metric,
            embedding_space=space.strip() if isinstance(space, str) else None,
            data_directory=data_directory,
        )

    @classmethod
    def from_runtime_config(cls, raw: Mapping[str, Any]) -> VectorTarget:
        vector = raw.get("vector")
        if not isinstance(vector, Mapping):
            raise VectorCleanupError("CONFIG_INVALID", "vector")
        binding = raw.get("binding")
        if not isinstance(binding, Mapping):
            raise VectorCleanupError("CONFIG_INVALID", "binding")
        if not binding.get("data_directory"):
            raise VectorCleanupError("TARGET_DATA_DIRECTORY_REQUIRED", "binding.data_directory")
        return cls.from_mapping(vector, binding=binding)

    @property
    def declared_space(self) -> str | None:
        if self.embedding_space:
            return self.embedding_space
        name = self.storage_dir.name
        return name if _SPACE_DIGEST.match(name) else None

    def as_manifest(self, *, embedding_space: str | None = None) -> dict[str, Any]:
        return {
            "backend": self.backend,
            "storage_dir": str(self.storage_dir.resolve()),
            "table_name": self.table_name,
            "dimensions": self.dimensions,
            "agent_id": self.agent_id,
            "installation_id": self.installation_id,
            "metric": self.metric,
            "embedding_space": embedding_space if embedding_space is not None else self.embedding_space,
        }


@dataclass(frozen=True)
class Inventory:
    """Every vector row in one store, classified against one SQLite database."""

    all_ids: tuple[str, ...]
    categories: dict[str, tuple[str, ...]]
    reason_counts: dict[str, dict[str, int]]
    live_counts: dict[str, int]
    database: dict[str, Any]
    vector: dict[str, Any]


def parse_vector_id(raw: str) -> VectorId | None:
    match = _VECTOR_ID.match(str(raw or ""))
    if match is None:
        return None
    return VectorId(
        raw=str(raw),
        ref=match.group("ref"),
        revision=int(match.group("revision")),
        embedding_space=match.group("space"),
    )


def scan(database: Path, target: VectorTarget) -> Inventory:
    """Read the companion with ``open_existing`` and classify it against SQLite."""
    _preflight_database(database, target)
    store = _open_store(target)
    try:
        ids = store.list_ids()
        rows = store.list_id_targets()
    finally:
        store.close()
    return _inventory(database, target, ids, rows)


def build_manifest(
    inventory: Inventory,
    *,
    include_history: bool = False,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """The full, deterministic list an apply must present back unchanged."""
    selected = HISTORY_DELETE_CATEGORIES if include_history else DEFAULT_DELETE_CATEGORIES
    counts = {name: len(inventory.categories.get(name, ())) for name in CATEGORIES}
    delete_ids = sorted({raw for name in selected for raw in inventory.categories.get(name, ())})
    payload: dict[str, Any] = {
        "schema": MANIFEST_SCHEMA,
        "generated_at": generated_at or _utc_now(),
        "database": inventory.database,
        "vector": inventory.vector,
        "include_history": bool(include_history),
        "delete_categories": list(selected),
        "counts": counts,
        "reason_counts": inventory.reason_counts,
        "live_counts": inventory.live_counts,
        "categories": {name: list(inventory.categories.get(name, ())) for name in CATEGORIES},
        "all_ids": list(inventory.all_ids),
        "delete_ids": delete_ids,
        "history_semantics": (
            "gone=SQLite subject unreadable in every mode; "
            "history=not current (suppressed or superseded), but history/as-of reads may still reach "
            "the SQLite object; "
            "live=current"
        ),
    }
    payload["classification_sha256"] = _classification_digest(payload)
    return payload


def apply_manifest(
    database: Path,
    target: VectorTarget,
    expected: Mapping[str, Any],
    *,
    backup_manifest: Path | None,
    include_history: bool = False,
) -> dict[str, Any]:
    """Delete exactly the ids a dry-run manifest names, under both write barriers.

    The lease excludes cooperating truth writers; the companion's physical write
    lock excludes cooperating vector writers.  A companion that cannot provide
    that lock is refused rather than written without it.  The manifest comparison
    is a preflight, not a claim that every non-cooperating change is detected.
    """
    if backup_manifest is None:
        raise VectorCleanupError("BACKUP_MANIFEST_REQUIRED", "backup_manifest")
    selected = HISTORY_DELETE_CATEGORIES if include_history else DEFAULT_DELETE_CATEGORIES
    raw_categories = expected.get("delete_categories")
    expected_categories = tuple(raw_categories) if isinstance(raw_categories, list) else DEFAULT_DELETE_CATEGORIES
    if selected != expected_categories:
        raise VectorCleanupError(
            "CATEGORIES_MISMATCH",
            "include_history",
            detail={
                "manifest": list(expected_categories),
                "requested": list(selected),
                "hint": "pass --include-history to delete history vectors",
            },
        )
    _validate_manifest(expected)
    _assert_safe_output(backup_manifest, database, target)

    delete_ids = [str(item) for item in expected["delete_ids"]]
    with _truth_writer_lease(database):
        store = _open_store(target, writable=True)
        try:
            with _store_write_lock(store):
                ids = store.list_ids()
                rows = store.list_id_targets()
                current = _inventory(database, target, ids, rows)
                current_manifest = build_manifest(current, include_history=include_history)
                differences = _manifest_differences(expected, current_manifest)
                if differences:
                    raise VectorCleanupError(
                        "EXPECTED_MANIFEST_MISMATCH",
                        "expected_manifest",
                        detail=differences,
                        exit_code=3,
                    )
                backup = {
                    "schema": BACKUP_SCHEMA,
                    "created_at": _utc_now(),
                    "database": current.database,
                    "vector": current.vector,
                    "include_history": bool(include_history),
                    "delete_categories": list(selected),
                    "counts": current_manifest["counts"],
                    "reason_counts": current_manifest["reason_counts"],
                    "classification_sha256": current_manifest["classification_sha256"],
                    "all_ids": list(current.all_ids),
                    "delete_ids": delete_ids,
                    "backup_kind": "id-manifest-not-data-backup",
                    "data_backup": False,
                    "note": "Ids only; vectors are rebuildable from SQLite and this file is not a data backup.",
                }
                _write_json(Path(backup_manifest), backup)
                actual_ids = store.list_ids()
                if actual_ids != list(current.all_ids):
                    raise VectorCleanupError(
                        "VECTOR_STORE_CHANGED",
                        "vector_store",
                        detail={"expected_rows": len(current.all_ids), "actual_rows": len(actual_ids)},
                        exit_code=4,
                    )
                for batch in _batches(delete_ids, DELETE_BATCH):
                    store.delete_by_ids(list(batch))
                remaining = store.list_ids()
        finally:
            store.close()

    left = sorted(set(delete_ids) & set(remaining))
    if left:
        raise VectorCleanupError(
            "DELETE_INCOMPLETE",
            "vector_store",
            detail={"remaining": len(left)},
            exit_code=4,
        )
    return {
        "schema": RESULT_SCHEMA,
        "status": "applied",
        "database": current.database,
        "vector": current.vector,
        "include_history": bool(include_history),
        "delete_categories": list(selected),
        "counts": current_manifest["counts"],
        "reason_counts": current_manifest["reason_counts"],
        "classification_sha256": current_manifest["classification_sha256"],
        "deleted": len(delete_ids),
        "delete_ids": delete_ids,
        "remaining_physical_rows": len(remaining),
        "backup_manifest": str(Path(backup_manifest)),
        "backup_manifest_is_data_backup": False,
        "write_barrier": {
            "truth_writer_lease": "held",
            "vector_write_lock": "held",
            "non_cooperating_writers_detected": False,
        },
        "compacted": False,
    }


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    report_payload: Mapping[str, Any] = {}
    try:
        target = _target_from_args(args)
        if args.apply:
            if args.manifest_out:
                raise VectorCleanupError("APPLY_ONLY_ARGUMENT", "manifest_out")
            if not args.expected_manifest:
                raise VectorCleanupError("EXPECTED_MANIFEST_REQUIRED", "expected_manifest")
            if not args.backup_manifest:
                raise VectorCleanupError("BACKUP_MANIFEST_REQUIRED", "backup_manifest")
            expected_path = Path(args.expected_manifest)
            backup_path = Path(args.backup_manifest)
            if expected_path.resolve() == backup_path.resolve():
                raise VectorCleanupError("MANIFEST_PATHS_COLLIDE", "backup_manifest")
            _check_output_paths(
                Path(args.database),
                target,
                backup_manifest=backup_path,
                report_out=args.report_out,
                expected_manifest=expected_path,
                runtime_config=args.runtime_config,
            )
            expected = _read_json(expected_path)
            result = apply_manifest(
                Path(args.database),
                target,
                expected,
                backup_manifest=backup_path,
                include_history=args.include_history,
            )
            payload = _result_summary(result, show_ids=args.show_ids)
            report_payload = result
        else:
            if args.expected_manifest or args.backup_manifest:
                raise VectorCleanupError("APPLY_ONLY_ARGUMENT", "expected_manifest")
            _check_output_paths(
                Path(args.database),
                target,
                manifest_out=args.manifest_out,
                report_out=args.report_out,
                runtime_config=args.runtime_config,
            )
            inventory = scan(Path(args.database), target)
            manifest = build_manifest(inventory, include_history=args.include_history)
            if args.manifest_out:
                _write_json(Path(args.manifest_out), manifest)
            payload = _manifest_summary(manifest, show_ids=args.show_ids)
            report_payload = manifest
            if args.manifest_out:
                payload["manifest_out"] = str(Path(args.manifest_out))
    except VectorCleanupError as exc:
        payload = {"status": "error", "code": exc.code, "field": exc.field, "detail": exc.detail}
        _emit(payload)
        return exc.exit_code
    except sqlite3.Error as exc:
        payload = {"status": "error", "code": "DATABASE_UNREADABLE", "detail": str(exc)}
        _emit(payload)
        return 2
    except (OSError, RuntimeError, ValueError) as exc:
        payload = {"status": "error", "code": "VECTOR_STORE_FAILED", "detail": type(exc).__name__}
        _emit(payload)
        return 2

    if args.report_out:
        try:
            _write_json(Path(args.report_out), report_payload)
        except VectorCleanupError as exc:
            payload = dict(payload)
            payload["status"] = "applied_report_failed" if args.apply else "dry_run_report_failed"
            payload["report_out_error"] = {"code": exc.code, "field": exc.field}
            _emit(payload)
            return 2
    _emit(payload)
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="scope-recall-vector-cleanup",
        description="Dry-run or explicitly apply removal of vectors whose SQLite subject is gone.",
    )
    parser.add_argument("--database", required=True, help="the truth store's memory.sqlite3")
    parser.add_argument("--runtime-config", help="a runtime config JSON whose 'vector' and 'binding' name the target")
    parser.add_argument("--vector-dir", help="the vector storage directory (the one build_vector_store takes)")
    parser.add_argument("--backend", default="lancedb", choices=("lancedb", "sqlite-bruteforce", "sqlite"))
    parser.add_argument("--table-name")
    parser.add_argument("--dimensions", type=int)
    parser.add_argument("--metric", default="cosine")
    parser.add_argument("--agent-id", help="the installation's agent id; required without --runtime-config")
    parser.add_argument("--installation-id", help="the installation id; required without --runtime-config")
    parser.add_argument("--embedding-space", help="the expected embedding-space digest")
    parser.add_argument(
        "--include-history",
        action="store_true",
        help="also delete history vectors (suppressed or superseded); default deletes only gone vectors",
    )
    parser.add_argument("--apply", action="store_true", help="delete the manifest's ids; without it nothing changes")
    parser.add_argument("--expected-manifest", help="the full manifest a dry run wrote (required with --apply)")
    parser.add_argument(
        "--backup-manifest",
        help="a new audit list of ids before deletion; not a data backup (required with --apply)",
    )
    parser.add_argument("--manifest-out", help="write the full dry-run manifest here (new file, mode 0600)")
    parser.add_argument("--report-out", help="write the full result here (new file, mode 0600)")
    parser.add_argument("--show-ids", action="store_true", help="include delete ids in the stdout summary")
    return parser


def _target_from_args(args: argparse.Namespace) -> VectorTarget:
    if args.runtime_config:
        target = VectorTarget.from_runtime_config(_read_json(Path(args.runtime_config)))
    else:
        if not args.vector_dir or not args.table_name or args.dimensions is None:
            raise VectorCleanupError("CONFIG_REQUIRED", "vector_dir")
        target = VectorTarget.from_mapping(
            {
                "backend": args.backend,
                "storage_dir": args.vector_dir,
                "table_name": args.table_name,
                "dimensions": args.dimensions,
                "metric": args.metric,
                "agent_id": args.agent_id,
                "installation_id": args.installation_id,
            }
        )
    if args.embedding_space:
        target = replace(target, embedding_space=args.embedding_space)
    return target


def _check_output_paths(
    database: Path,
    target: VectorTarget,
    *,
    manifest_out: str | None = None,
    report_out: str | None = None,
    backup_manifest: str | Path | None = None,
    expected_manifest: str | Path | None = None,
    runtime_config: str | Path | None = None,
) -> None:
    database_path = Path(database).resolve()
    database_sidecars = {Path(f"{database_path}{suffix}") for suffix in ("-wal", "-shm", "-journal")}
    vector_root = target.storage_dir.resolve()
    input_paths = {Path(value).resolve() for value in (expected_manifest, runtime_config) if value is not None}
    seen: dict[Path, str] = {}
    for name, value in (
        ("manifest_out", manifest_out),
        ("report_out", report_out),
        ("backup_manifest", backup_manifest),
    ):
        if value is None:
            continue
        path = Path(value).resolve()
        if path == database_path or path in database_sidecars:
            raise VectorCleanupError("OUTPUT_PATH_IS_DATABASE", name)
        if path == vector_root or vector_root in path.parents:
            raise VectorCleanupError("OUTPUT_PATH_INSIDE_VECTOR_STORE", name)
        if path in input_paths:
            raise VectorCleanupError("OUTPUT_PATH_COLLISION", name)
        if path in seen:
            raise VectorCleanupError("OUTPUT_PATH_COLLISION", name, detail={"other": seen[path]})
        if path.exists() and path.is_dir():
            raise VectorCleanupError("OUTPUT_PATH_IS_DIRECTORY", name)
        seen[path] = name


def _assert_safe_output(path: Path, database: Path, target: VectorTarget) -> None:
    _check_output_paths(database, target, backup_manifest=path)


def _connect_read_only(path: Path) -> sqlite3.Connection:
    if not path.is_file():
        raise VectorCleanupError("DATABASE_MISSING", str(path))
    try:
        conn = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        return conn
    except sqlite3.Error as exc:
        raise VectorCleanupError("DATABASE_UNREADABLE", str(path)) from exc


def _table_names(conn: sqlite3.Connection) -> set[str]:
    try:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    except sqlite3.Error as exc:
        raise VectorCleanupError("DATABASE_UNREADABLE", "sqlite_master") from exc
    return {str(row["name"]) for row in rows}


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row["name"]) for row in conn.execute(f'PRAGMA table_info("{table}")').fetchall()}


def _require_schema(conn: sqlite3.Connection) -> None:
    required = {
        "source_events": {"event_id", "source_revision", "source_group_key", "suppressed", "read_blocked"},
        "claims": {"claim_id", "current_revision", "suppressed", "read_blocked"},
        "claim_versions": {"claim_id", "revision", "state"},
        "instance_meta": {
            "singleton",
            "agent_id",
            "installation_id",
            "data_directory",
            "schema_version",
            "memory_epoch",
        },
        "object_blocks": {"object_kind", "object_ref", "suppressed", "read_blocked"},
        "restored_absence_blocks": {"object_kind", "object_ref"},
    }
    tables = _table_names(conn)
    missing_tables = sorted(set(required) - tables)
    if missing_tables:
        raise VectorCleanupError("DATABASE_INCOMPATIBLE", "tables", detail=missing_tables)
    for table, columns in required.items():
        missing = sorted(columns - _columns(conn, table))
        if missing:
            raise VectorCleanupError("DATABASE_INCOMPATIBLE", table, detail=missing)


def _instance_binding(conn: sqlite3.Connection) -> dict[str, Any]:
    row = conn.execute(
        "SELECT agent_id,installation_id,memory_epoch,data_directory,schema_version "
        "FROM instance_meta WHERE singleton=1"
    ).fetchone()
    if row is None:
        raise VectorCleanupError("DATABASE_INCOMPATIBLE", "instance_meta.singleton")
    raw_directory = row["data_directory"]
    if type(raw_directory) is not str or not raw_directory:
        raise VectorCleanupError("DATABASE_INCOMPATIBLE", "instance_meta.data_directory")
    directory = Path(raw_directory).expanduser()
    if not directory.is_absolute():
        raise VectorCleanupError("DATABASE_INCOMPATIBLE", "instance_meta.data_directory")
    try:
        schema_version = int(row["schema_version"])
        memory_epoch = int(row["memory_epoch"])
    except (TypeError, ValueError) as exc:
        raise VectorCleanupError("DATABASE_INCOMPATIBLE", "instance_meta.schema_version") from exc
    return {
        "agent_id": str(row["agent_id"]),
        "installation_id": str(row["installation_id"]),
        "memory_epoch": memory_epoch,
        "data_directory": directory.resolve(),
        "schema_version": schema_version,
    }


def _validate_schema(conn: sqlite3.Connection, binding: Mapping[str, Any]) -> None:
    user_version = _schema_version(conn)
    recorded = int(binding["schema_version"])
    if user_version != recorded:
        raise VectorCleanupError(
            "DATABASE_SCHEMA_MISMATCH",
            "schema_version",
            detail={"user_version": user_version, "instance_meta": recorded},
        )
    if recorded != SCHEMA_VERSION:
        raise VectorCleanupError(
            "DATABASE_SCHEMA_UNSUPPORTED",
            "schema_version",
            detail={"supported": SCHEMA_VERSION, "found": recorded},
        )


def _validate_target_ownership(database: Path, target: VectorTarget, binding: Mapping[str, Any]) -> None:
    instance_root = cast(Path, binding["data_directory"])
    database_root = Path(database).resolve().parent
    if database_root != instance_root:
        raise VectorCleanupError(
            "TARGET_DATA_DIRECTORY_MISMATCH",
            "database",
            detail={"database_directory": str(database_root), "instance_meta": str(instance_root)},
        )
    if target.data_directory is not None and target.data_directory != instance_root:
        raise VectorCleanupError(
            "TARGET_DATA_DIRECTORY_MISMATCH",
            "binding.data_directory",
            detail={"target": str(target.data_directory), "instance_meta": str(instance_root)},
        )
    vector_root = instance_root / "vectors"
    if target.storage_dir.parent != vector_root:
        raise VectorCleanupError(
            "TARGET_STORAGE_OUTSIDE_BINDING",
            "vector.storage_dir",
            detail={"expected_parent": str(vector_root), "actual_parent": str(target.storage_dir.parent)},
        )


def _database_binding(database: Path, target: VectorTarget, conn: sqlite3.Connection) -> dict[str, Any]:
    _require_schema(conn)
    binding = _instance_binding(conn)
    _validate_schema(conn, binding)
    _validate_target_ownership(database, target, binding)
    if (target.agent_id, target.installation_id) != (binding["agent_id"], binding["installation_id"]):
        raise VectorCleanupError(
            "TARGET_IDENTITY_MISMATCH",
            "installation_id",
            detail={
                "target": [target.agent_id, target.installation_id],
                "database": [binding["agent_id"], binding["installation_id"]],
            },
        )
    return binding


def _preflight_database(database: Path, target: VectorTarget) -> None:
    conn = _connect_read_only(Path(database))
    try:
        _database_binding(Path(database), target, conn)
    finally:
        conn.close()


def _inventory(
    database: Path, target: VectorTarget, ids: Sequence[str], rows: Sequence[Mapping[str, Any]]
) -> Inventory:
    path = Path(database)
    conn = _connect_read_only(path)
    try:
        binding = _database_binding(path, target, conn)
        row_ids = [str(row.get("id") or "") for row in rows]
        if sorted(row_ids) != sorted(str(item) for item in ids):
            raise VectorCleanupError("VECTOR_STORE_INCONSISTENT", "list_id_targets")
        parsed_rows = []
        for row in rows:
            raw = str(row.get("id") or "")
            parsed = parse_vector_id(raw)
            metadata = _row_metadata(raw, str(row.get("target") or ""))
            parsed_rows.append((raw, parsed, metadata))
        expected_space = _expected_space(target, parsed_rows)
        for raw, parsed, metadata in parsed_rows:
            _validate_metadata(raw, parsed, metadata, target, expected_space)
        truth_counts = _truth_counts(conn)
        if ids and truth_counts["source_events"] == 0 and truth_counts["claims"] == 0:
            raise VectorCleanupError(
                "TRUTH_STORE_EMPTY",
                "database",
                detail={"source_events": 0, "claims": 0, "vectors": len(ids)},
            )
        event_refs = sorted(
            {item.ref for _raw, item, _metadata in parsed_rows if item and item.ref.startswith("event-")}
        )
        claim_refs = sorted(
            {item.ref for _raw, item, _metadata in parsed_rows if item and item.ref.startswith("claim-")}
        )
        events = _event_rows(conn, event_refs)
        heads = _event_head_revisions(conn, events)
        blocks = _object_blocks(conn, sorted(set(event_refs) | set(claim_refs)))
        claims = _claim_rows(conn, claim_refs)
        versions = _claim_versions(conn, claim_refs)
        restored = _restored_claims(conn, claim_refs)
        database_meta = {
            "path": str(path.resolve()),
            "schema_version": binding["schema_version"],
            "data_directory": str(binding["data_directory"]),
            "memory_epoch": binding["memory_epoch"],
            "agent_id": binding["agent_id"],
            "installation_id": binding["installation_id"],
        }
    finally:
        conn.close()

    category_ids: dict[str, list[str]] = {name: [] for name in CATEGORIES}
    reason_counts: dict[str, Counter[str]] = {name: Counter() for name in CATEGORIES}
    live: Counter[str] = Counter()
    for raw, item, _metadata in parsed_rows:
        category, reason = _classify(item, events, heads, blocks, claims, versions, restored)
        if category is None:
            live[reason] += 1
        else:
            category_ids[category].append(raw)
            reason_counts[category][reason] += 1

    categories = {name: tuple(sorted(category_ids[name])) for name in CATEGORIES}
    vector_meta = {
        **target.as_manifest(embedding_space=expected_space),
        "data_directory": str(binding["data_directory"]),
        "physical_rows": len(ids),
        "unique_ids": len(set(ids)),
        "duplicate_rows": len(ids) - len(set(ids)),
        "all_ids_sha256": _sha256_json(list(ids)),
    }
    return Inventory(
        all_ids=tuple(ids),
        categories=categories,
        reason_counts={name: dict(sorted(counts.items())) for name, counts in reason_counts.items()},
        live_counts=dict(sorted(live.items())),
        database=database_meta,
        vector=vector_meta,
    )


def _row_metadata(raw: str, target_json: str) -> Mapping[str, Any]:
    try:
        metadata = json.loads(target_json)
    except (TypeError, ValueError) as exc:
        raise VectorCleanupError("VECTOR_STORE_IDENTITY_MISMATCH", "target", detail={"id": raw}) from exc
    if not isinstance(metadata, Mapping):
        raise VectorCleanupError("VECTOR_STORE_IDENTITY_MISMATCH", "target", detail={"id": raw})
    for key in _METADATA_KEYS:
        if type(metadata.get(key)) is not str or not metadata[key]:
            raise VectorCleanupError("VECTOR_STORE_IDENTITY_MISMATCH", f"target.{key}", detail={"id": raw})
    if type(metadata.get("object_revision")) is not int or metadata["object_revision"] < 1:
        raise VectorCleanupError("VECTOR_STORE_IDENTITY_MISMATCH", "target.object_revision", detail={"id": raw})
    return metadata


def _expected_space(target: VectorTarget, rows: Sequence[tuple[str, VectorId | None, Mapping[str, Any]]]) -> str | None:
    declared = target.declared_space
    spaces = {item.embedding_space for _raw, item, _metadata in rows if item is not None}
    if declared is not None:
        if spaces - {declared}:
            raise VectorCleanupError("TARGET_SPACE_MISMATCH", "embedding_space", detail=sorted(spaces))
        expected = declared
    elif len(spaces) > 1:
        raise VectorCleanupError("TARGET_SPACE_MISMATCH", "embedding_space", detail=sorted(spaces))
    else:
        expected = next(iter(spaces), None)
    directory_space = target.storage_dir.name if _SPACE_DIGEST.match(target.storage_dir.name) else None
    if expected is not None and directory_space is not None and expected != directory_space:
        raise VectorCleanupError(
            "TARGET_SPACE_MISMATCH",
            "vector.storage_dir",
            detail={"expected": expected, "directory": directory_space},
        )
    return expected


def _validate_metadata(
    raw: str,
    item: VectorId | None,
    metadata: Mapping[str, Any],
    target: VectorTarget,
    expected_space: str | None,
) -> None:
    if metadata["vector_id"] != raw:
        raise VectorCleanupError("VECTOR_STORE_IDENTITY_MISMATCH", "target.vector_id", detail={"id": raw})
    if (metadata["agent_id"], metadata["installation_id"]) != (target.agent_id, target.installation_id):
        raise VectorCleanupError("VECTOR_STORE_IDENTITY_MISMATCH", "target.installation_id", detail={"id": raw})
    if expected_space is not None and metadata["embedding_space"] != expected_space:
        raise VectorCleanupError("VECTOR_STORE_IDENTITY_MISMATCH", "target.embedding_space", detail={"id": raw})
    if item is None:
        return
    if metadata["object_ref"] != item.ref or int(metadata["object_revision"]) != item.revision:
        raise VectorCleanupError("VECTOR_STORE_IDENTITY_MISMATCH", "target.object_ref", detail={"id": raw})
    expected_kind = "claim" if item.ref.startswith("claim-") else "event"
    if metadata["object_kind"] != expected_kind:
        raise VectorCleanupError("VECTOR_STORE_IDENTITY_MISMATCH", "target.object_kind", detail={"id": raw})


def _truth_counts(conn: sqlite3.Connection) -> dict[str, int]:
    return {
        "source_events": int(conn.execute("SELECT COUNT(*) FROM source_events").fetchone()[0]),
        "claims": int(conn.execute("SELECT COUNT(*) FROM claims").fetchone()[0]),
    }


def _schema_version(conn: sqlite3.Connection) -> int | None:
    try:
        return int(conn.execute("PRAGMA user_version").fetchone()[0])
    except (sqlite3.Error, TypeError, ValueError):
        return None


def _event_rows(conn: sqlite3.Connection, refs: Sequence[str]) -> dict[str, dict[int, sqlite3.Row]]:
    if not refs:
        return {}
    rows = _fetch_in(
        conn,
        """SELECT event_id,source_revision,source_group_key,suppressed,read_blocked
        FROM source_events WHERE event_id IN ({marks})""",
        refs,
    )
    grouped: dict[str, dict[int, sqlite3.Row]] = {}
    for row in rows:
        grouped.setdefault(str(row["event_id"]), {})[int(row["source_revision"])] = row
    return grouped


def _event_head_revisions(conn: sqlite3.Connection, events: Mapping[str, Mapping[int, sqlite3.Row]]) -> dict[str, int]:
    groups = sorted({str(row["source_group_key"]) for versions in events.values() for row in versions.values()})
    if not groups:
        return {}
    rows = _fetch_in(
        conn,
        """SELECT source_group_key,MAX(source_revision) AS head FROM source_events
        WHERE source_group_key IN ({marks}) GROUP BY source_group_key""",
        groups,
    )
    return {str(row["source_group_key"]): int(row["head"]) for row in rows}


def _object_blocks(conn: sqlite3.Connection, refs: Sequence[str]) -> dict[tuple[str, str], sqlite3.Row]:
    if not refs:
        return {}
    rows = _fetch_in(
        conn,
        """SELECT object_kind,object_ref,suppressed,read_blocked FROM object_blocks
        WHERE object_ref IN ({marks}) AND object_kind IN ('event','claim')""",
        refs,
    )
    return {(str(row["object_kind"]), str(row["object_ref"])): row for row in rows}


def _claim_rows(conn: sqlite3.Connection, refs: Sequence[str]) -> dict[str, sqlite3.Row]:
    if not refs:
        return {}
    rows = _fetch_in(
        conn,
        "SELECT claim_id,current_revision,suppressed,read_blocked FROM claims WHERE claim_id IN ({marks})",
        refs,
    )
    return {str(row["claim_id"]): row for row in rows}


def _claim_versions(conn: sqlite3.Connection, refs: Sequence[str]) -> dict[tuple[str, int], str]:
    if not refs:
        return {}
    rows = _fetch_in(
        conn,
        "SELECT claim_id,revision,state FROM claim_versions WHERE claim_id IN ({marks})",
        refs,
    )
    return {(str(row["claim_id"]), int(row["revision"])): str(row["state"]) for row in rows}


def _restored_claims(conn: sqlite3.Connection, refs: Sequence[str]) -> set[str]:
    if not refs:
        return set()
    rows = _fetch_in(
        conn,
        "SELECT object_ref FROM restored_absence_blocks WHERE object_kind='claim' AND object_ref IN ({marks})",
        refs,
    )
    return {str(row["object_ref"]) for row in rows}


def _fetch_in(conn: sqlite3.Connection, sql: str, values: Sequence[str]) -> list[sqlite3.Row]:
    rows: list[sqlite3.Row] = []
    for batch in _batches(values, _QUERY_CHUNK):
        marks = ",".join("?" for _ in batch)
        rows.extend(conn.execute(sql.format(marks=marks), list(batch)).fetchall())
    return rows


def _classify(
    item: VectorId | None,
    events: Mapping[str, Mapping[int, sqlite3.Row]],
    heads: Mapping[str, int],
    blocks: Mapping[tuple[str, str], sqlite3.Row],
    claims: Mapping[str, sqlite3.Row],
    versions: Mapping[tuple[str, int], str],
    restored: set[str],
) -> tuple[str | None, str]:
    if item is None:
        return UNKNOWN_CATEGORY, "unparsed_id"
    if item.ref.startswith("claim-"):
        return _classify_claim(item, blocks, claims, versions, restored)
    return _classify_event(item, events, heads, blocks)


def _classify_claim(
    item: VectorId,
    blocks: Mapping[tuple[str, str], sqlite3.Row],
    claims: Mapping[str, sqlite3.Row],
    versions: Mapping[tuple[str, int], str],
    restored: set[str],
) -> tuple[str | None, str]:
    claim = claims.get(item.ref)
    if claim is None:
        return GONE_CATEGORY, "claim_missing"
    if (item.ref, item.revision) not in versions:
        return GONE_CATEGORY, "claim_version_missing"
    block = blocks.get(("claim", item.ref))
    if item.ref in restored:
        return GONE_CATEGORY, "restored_absence"
    if int(claim["read_blocked"]) or (block is not None and int(block["read_blocked"])):
        return GONE_CATEGORY, "read_blocked"
    if int(claim["suppressed"]) or (block is not None and int(block["suppressed"])):
        return HISTORY_CATEGORY, "suppressed"
    if item.revision != int(claim["current_revision"]):
        return HISTORY_CATEGORY, "revision_superseded"
    if versions[(item.ref, item.revision)] not in _LIVE_CLAIM_STATES:
        return HISTORY_CATEGORY, "state"
    return None, "live_claim"


def _classify_event(
    item: VectorId,
    events: Mapping[str, Mapping[int, sqlite3.Row]],
    heads: Mapping[str, int],
    blocks: Mapping[tuple[str, str], sqlite3.Row],
) -> tuple[str | None, str]:
    versions = events.get(item.ref)
    if not versions:
        return GONE_CATEGORY, "event_missing"
    source = versions.get(item.revision)
    if source is None:
        return GONE_CATEGORY, "event_revision_missing"
    block = blocks.get(("event", item.ref))
    if int(source["read_blocked"]) or (block is not None and int(block["read_blocked"])):
        return GONE_CATEGORY, "read_blocked"
    if int(source["suppressed"]) or (block is not None and int(block["suppressed"])):
        return HISTORY_CATEGORY, "suppressed"
    head = heads.get(str(source["source_group_key"]))
    if head is not None and int(source["source_revision"]) < head:
        return HISTORY_CATEGORY, "superseded"
    return None, "live_event"


def _validate_manifest(expected: Mapping[str, Any]) -> None:
    if not isinstance(expected, Mapping) or expected.get("schema") != MANIFEST_SCHEMA:
        raise VectorCleanupError("EXPECTED_MANIFEST_INVALID", "schema")
    for key in ("database", "vector", "delete_categories", "categories", "all_ids", "delete_ids"):
        if key not in expected:
            raise VectorCleanupError("EXPECTED_MANIFEST_INVALID", key)
    if not isinstance(expected["delete_categories"], list) or not isinstance(expected["delete_ids"], list):
        raise VectorCleanupError("EXPECTED_MANIFEST_INVALID", "delete_ids")
    if not expected["delete_categories"]:
        raise VectorCleanupError("EXPECTED_MANIFEST_INVALID", "delete_categories")
    if not isinstance(expected["all_ids"], list):
        raise VectorCleanupError("EXPECTED_MANIFEST_INVALID", "all_ids")
    unknown = sorted(set(expected["delete_categories"]) - set(CATEGORIES))
    if unknown:
        raise VectorCleanupError("EXPECTED_MANIFEST_INVALID", "delete_categories", detail=unknown)
    categories = expected["categories"]
    if not isinstance(categories, Mapping):
        raise VectorCleanupError("EXPECTED_MANIFEST_INVALID", "categories")
    for name in expected["delete_categories"]:
        if not isinstance(categories.get(name), list):
            raise VectorCleanupError("EXPECTED_MANIFEST_INVALID", f"categories.{name}")
    union = sorted({str(item) for name in expected["delete_categories"] for item in categories.get(name, ())})
    if union != sorted(str(item) for item in expected["delete_ids"]):
        raise VectorCleanupError("EXPECTED_MANIFEST_INVALID", "delete_ids")
    if not set(union) <= {str(item) for item in expected["all_ids"]}:
        raise VectorCleanupError("EXPECTED_MANIFEST_INVALID", "delete_ids")


def _manifest_differences(expected: Mapping[str, Any], current: Mapping[str, Any]) -> list[str]:
    differences: list[str] = []
    for key in ("database", "vector", "include_history", "delete_categories", "counts", "reason_counts", "live_counts"):
        if expected.get(key) != current.get(key):
            differences.append(f"{key} differs")
    if expected.get("all_ids") != current.get("all_ids"):
        differences.append(
            "all_ids differs "
            f"(manifest {len(expected.get('all_ids') or ())}, current {len(current.get('all_ids') or ())})"
        )
    if expected.get("delete_ids") != current.get("delete_ids"):
        differences.append(
            "delete_ids differs "
            f"(manifest {len(expected.get('delete_ids') or ())}, current {len(current.get('delete_ids') or ())})"
        )
    expected_categories = expected.get("categories") or {}
    current_categories = current.get("categories") or {}
    for name in CATEGORIES:
        if expected_categories.get(name) != current_categories.get(name):
            differences.append(
                f"categories.{name} differs "
                "(manifest "
                f"{len(expected_categories.get(name) or ())}, current {len(current_categories.get(name) or ())})"
            )
    if expected.get("classification_sha256") != current.get("classification_sha256"):
        differences.append("classification_sha256 differs")
    return differences


def _classification_digest(payload: Mapping[str, Any]) -> str:
    return _sha256_json(
        {
            "database": payload.get("database"),
            "vector": {
                key: value
                for key, value in (payload.get("vector") or {}).items()
                if key not in {"physical_rows", "unique_ids", "duplicate_rows"}
            },
            "include_history": payload.get("include_history"),
            "delete_categories": payload.get("delete_categories"),
            "counts": payload.get("counts"),
            "reason_counts": payload.get("reason_counts"),
            "live_counts": payload.get("live_counts"),
            "categories": payload.get("categories"),
            "all_ids": payload.get("all_ids"),
            "delete_ids": payload.get("delete_ids"),
        }
    )


def _sha256_json(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _batches(values: Sequence[str], size: int) -> Iterator[Sequence[str]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def _open_store(target: VectorTarget, *, writable: bool = False) -> VectorStore:
    try:
        store = build_vector_store(
            target.backend,
            storage_dir=target.storage_dir,
            table_name=target.table_name,
            dimensions=target.dimensions,
            metric=target.metric,
        )
        if writable:
            _require_physical_write_lock(store)
        if not store.is_available():
            raise VectorCleanupError("VECTOR_STORE_UNAVAILABLE", target.backend)
        if writable and target.backend == "sqlite-bruteforce":
            if not store.db_path.is_file():
                raise VectorCleanupError("VECTOR_STORE_UNAVAILABLE", target.table_name)
            store.open_existing()
            store.close()
            store.open()
        else:
            store.open_existing()
        return store
    except VectorCleanupError:
        raise
    except (OSError, RuntimeError, ValueError) as exc:
        raise VectorCleanupError("VECTOR_STORE_UNAVAILABLE", target.table_name) from exc


@contextmanager
def _truth_writer_lease(database: Path) -> Iterator[None]:
    try:
        with holding_truth_writer_lease(Path(database).parent, role="vector_cleanup"):
            yield
    except TruthWriterBusyError as exc:
        raise VectorCleanupError(
            "WRITERS_ACTIVE",
            "truth_writer_lease",
            detail={"scope": exc.scope, "owner": exc.owner},
        ) from exc


def _require_physical_write_lock(store: VectorStore) -> Callable[..., AbstractContextManager[Any]]:
    lock = getattr(store, "physical_write_lock", None)
    backend = str(getattr(store, "backend", "") or type(store).__name__)
    if not callable(lock):
        raise VectorCleanupError(
            "VECTOR_WRITE_LOCK_UNAVAILABLE",
            "vector_write_lock",
            detail={"backend": backend},
        )
    return cast(Callable[..., AbstractContextManager[Any]], lock)


@contextmanager
def _store_write_lock(store: VectorStore) -> Iterator[None]:
    lock = _require_physical_write_lock(store)
    backend = str(getattr(store, "backend", "") or type(store).__name__)
    try:
        manager = lock(timeout_seconds=VECTOR_WRITE_LOCK_TIMEOUT_SECONDS)
    except TypeError as exc:
        raise VectorCleanupError(
            "VECTOR_WRITE_LOCK_UNAVAILABLE",
            "vector_write_lock",
            detail={"backend": backend, "reason": "bounded_timeout_not_supported"},
        ) from exc
    entered = False
    try:
        with manager:
            entered = True
            yield
    except TimeoutError as exc:
        if entered:
            raise
        raise VectorCleanupError(
            "VECTOR_WRITE_LOCK_BUSY",
            "vector_write_lock",
            detail={"backend": backend, "timeout_seconds": VECTOR_WRITE_LOCK_TIMEOUT_SECONDS},
            exit_code=3,
        ) from exc


def _manifest_summary(manifest: Mapping[str, Any], *, show_ids: bool) -> dict[str, Any]:
    summary = {
        "schema": RESULT_SCHEMA,
        "status": "dry-run",
        "database": manifest["database"],
        "vector": manifest["vector"],
        "include_history": manifest["include_history"],
        "delete_categories": manifest["delete_categories"],
        "counts": manifest["counts"],
        "reason_counts": manifest["reason_counts"],
        "live_counts": manifest["live_counts"],
        "classification_sha256": manifest["classification_sha256"],
        "delete_count": len(manifest["delete_ids"]),
        "compacted": False,
    }
    if show_ids:
        summary["delete_ids"] = manifest["delete_ids"]
    return summary


def _result_summary(result: Mapping[str, Any], *, show_ids: bool) -> dict[str, Any]:
    summary = {
        key: result[key]
        for key in (
            "schema",
            "status",
            "database",
            "vector",
            "include_history",
            "delete_categories",
            "counts",
            "reason_counts",
            "classification_sha256",
            "deleted",
            "remaining_physical_rows",
            "backup_manifest",
            "backup_manifest_is_data_backup",
            "write_barrier",
            "compacted",
        )
    }
    if show_ids:
        summary["delete_ids"] = result["delete_ids"]
    return summary


def _read_json(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise VectorCleanupError("MANIFEST_UNREADABLE", str(path)) from exc
    if not isinstance(raw, dict):
        raise VectorCleanupError("MANIFEST_UNREADABLE", str(path))
    return raw


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path = Path(path)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError as exc:
        raise VectorCleanupError("OUTPUT_EXISTS", str(path)) from exc
    except OSError as exc:
        raise VectorCleanupError("OUTPUT_UNWRITABLE", str(path)) from exc
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
    except OSError as exc:
        try:
            path.unlink()
        except OSError:
            pass
        raise VectorCleanupError("OUTPUT_UNWRITABLE", str(path)) from exc


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _emit(payload: Mapping[str, Any]) -> None:
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
