"""Expire the vectors of tool outputs older than the retention window.

Sits beside ``vector_upkeep.py``: ``runtime/instance.py`` has the single call
site, at the start of a drain, just before the compaction that reclaims what
a pass deleted.  The source text, its lexical index and everything derived
from it (claims, episodes, candidates) stay; only the vector goes.  An expired
tool output is still recalled by its words and through whatever cites it, never
again by meaning alone.  (A withheld output's placeholder has no words of its
own to be recalled by: ``core/events.indexed_terms``.)

Why tool outputs, and why a window: on a busy instance four in five captured
sources were tool output, each carrying a 12 KB vector -- the bulk of the
store's daily growth -- while a few hundred of 170,000 sources ever became
claim evidence.  A window keeps recent work fully searchable and lets the old
bulk go, at a pace the operator sets (``vector.tool_output_retention_days``).

A shared store's worker binds every entry's scopes at once.  Its context names
no entry (``TrustedContext.entry_id`` is ``None`` for the worker), so the pass
runs once per registered entry, each with its own state record.  Otherwise the
``local`` fallback would look for the worker's own entry in a store that has
none and delete nothing.

Not responsible for: deciding what a tool output is (the source's ``role``),
the native delete (the store's ``delete_by_ids``), or reclaiming the space
(``vector_upkeep.compact_if_due``, which the drain runs next).
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import time
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from ..core.events import WITHHELD_TOOL_OUTPUT_SQL
from ..core.file_lock import advisory_file_lock
from ..vector.compaction import parse_time
from .validation import utc_now

#: Seconds of the drain budget set aside for one pass; below it the pass waits.
RESERVE_SECONDS = 8.0
#: Sources one pass may expire.  A backlog is cleared over consecutive drains,
#: each deleting one batch, so no single pass holds the store for long.
BATCH_LIMIT = 2000
#: With no backlog a pass runs this often.  The candidate scan reads the source
#: table once (0.2 s for 170,000 sources), so hourly costs nothing noticeable.
PASS_INTERVAL = timedelta(hours=1)
#: A pass that could not run backs off this long, so the five-minute timer does
#: not start a worker for a store that is not open or a state file it cannot write.
RETRY_BACKOFF = timedelta(minutes=15)
STATE_FILENAME = "retention-state.json"
STATE_SCHEMA = "scope-recall.vector-retention.v2"
LEGACY_STATE_SCHEMA = "scope-recall.vector-retention.v1"
BACKOFF_SCHEMA = "scope-recall.vector-retention-backoff.v1"
BACKOFF_FILENAME = "runtime-retention-backoff-{key}.json"


def retention_key(
    *,
    installation_id: str,
    entry_id: str | None = None,
    project_id: str | None,
    branch_id: str | None,
    scope_ids: Iterable[str],
    days: int,
) -> str:
    """The state key of one entry/project/branch/scope partition and retention window.

    A shared store's vector directory holds one pass record per partition.  The
    key names the installation, the entry, the project and branch filters, the
    exact scopes the pass may expire and the window.  A context outside a shared
    store is the store's ``local`` entry, so its key is the one a shared entry
    named ``local`` would use.  Another entry, another project or branch, a
    different window or another scope set never shares or invalidates a record.
    """
    material = json.dumps(
        [installation_id, entry_id or "local", project_id, branch_id, sorted(scope_ids), days],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:32]


def installation_backoff_key(installation_id: str) -> str:
    """The key of an installation-wide retention retry when no partition could be read.

    A registry or partition read can fail before the pass knows which partition
    records to hold.  The retry belongs to the installation, so it is held under
    a key of its own; the prefix keeps it apart from a 32-hex partition key.
    """
    material = installation_id or ""
    return "installation-" + hashlib.sha256(material.encode("utf-8")).hexdigest()[:32]


@dataclass(frozen=True)
class RetentionPartition:
    """One entry's slice of a store, and the state record its pass writes."""

    entry_id: str | None
    scope_ids: frozenset[str]
    key: str


def retention_partitions(
    config: Any, context: Any, *, days: int, entry_ids: Iterable[str] | None = None
) -> list[RetentionPartition]:
    """The partitions one pass covers, in a stable order.

    A context that names an entry covers that entry alone.  A shared worker's
    context names none, so it covers every registered entry the caller read from
    the store; with no entry list it covers nothing rather than falling back to
    ``local``, which in a shared store has no rows.  Every other context is the
    store's ``local`` entry.  The planner and the pass both call this with the
    same window, so a record written by one is the record the other reads.
    """
    entry_id = getattr(context, "entry_id", None)
    if entry_id is None:
        entry_id = getattr(config, "entry_id", None)
    binding = getattr(context, "binding", None) or getattr(config, "binding", None)
    if entry_id is not None:
        ids: tuple[str | None, ...] = (entry_id,)
    elif getattr(binding, "installation_kind", "local") == "shared":
        ids = tuple(sorted(entry_ids or ()))
    else:
        ids = (None,)
    scopes = frozenset(context.allowed_scope_ids)
    return [
        RetentionPartition(
            item,
            scopes,
            retention_key(
                installation_id=getattr(binding, "installation_id", ""),
                entry_id=item,
                project_id=getattr(context, "project_id", None),
                branch_id=getattr(context, "branch_id", None),
                scope_ids=scopes,
                days=days,
            ),
        )
        for item in ids
    ]


def shared_entry_ids(storage: Any, context: Any, *, remaining_seconds: float = 1.0) -> tuple[str, ...]:
    """The entries registered in a shared store, read under ``context``.

    The registry is the store's own record of every entry that attached; a
    detached entry stays, because its memories and vectors stay.
    """
    with storage.read(context, remaining_seconds=max(0.001, remaining_seconds)) as tx:
        return tuple(sorted(tx.registry.entries()))


def _backoff_directory(config: Any) -> Path:
    """Where a pass writes retry records: the binding's data directory, else the vector directory."""
    vector = getattr(config, "vector", None)
    binding = getattr(config, "binding", None)
    directory = getattr(binding, "data_directory", None)
    if directory is None:
        directory = getattr(vector, "storage_dir", ".")
    return Path(directory)


def _failed_receipt(config: Any, moment: datetime, *, days: int, stage: str, error: BaseException) -> dict[str, Any]:
    """A visible failed pass for a read that left no partition to record.

    The retry is written as an installation-level backoff, the only key a pass
    can use before the registry or partition list is readable.  The planner
    reads the same key, so the failed read is retried after ``RETRY_BACKOFF``
    rather than every five-minute tick or never.
    """
    binding = getattr(config, "binding", None)
    key = installation_backoff_key(getattr(binding, "installation_id", ""))
    until = moment + RETRY_BACKOFF
    written = write_backoff(_backoff_directory(config), key, until=until, reason=stage)
    return {
        "started_at": _stamp(moment),
        "finished_at": _stamp(moment),
        "retention_days": days,
        "cutoff": (moment - timedelta(days=days)).isoformat(),
        "outcome": "failed",
        "stage": stage,
        "error": type(error).__name__,
        "expired": 0,
        "backlog": False,
        "partitions": [],
        "state_written": False,
        "backoff_until": _stamp(until),
        "backoff_written": written,
        "seconds": 0.0,
    }


@dataclass(frozen=True)
class _PassContext:
    store: Any
    config: Any
    storage: Any
    context: Any
    days: int
    cutoff: str
    moment: datetime


def expire_if_due(
    store: Any, config: Any, storage: Any, context: Any, *, available_seconds: float, now: datetime | None = None
) -> dict[str, Any] | None:
    """Expire one batch per due partition.  Returns the receipt, or ``None``.

    Never raises.  A pass that cannot run leaves the store and the ledger as
    they were.  A registry or partition read that leaves no partition to record
    still returns a failed receipt and writes an installation-level backoff, so
    the failure is visible and the planner retries it; failing the drain would
    trade a tidiness problem for an availability one.
    """
    vector = getattr(config, "vector", None)
    days = getattr(vector, "tool_output_retention_days", 0) if vector is not None else 0
    if not days:
        return None
    moment = now or datetime.now(timezone.utc)
    storage_dir = Path(vector.storage_dir)
    binding = getattr(config, "binding", None)
    data_directory = _backoff_directory(config)
    delete = getattr(store, "delete_by_ids", None)
    shared_worker = (
        getattr(context, "entry_id", None) is None and getattr(binding, "installation_kind", "local") == "shared"
    )
    stage = "registry"
    try:
        entry_ids = (
            shared_entry_ids(storage, context, remaining_seconds=min(1.0, available_seconds)) if shared_worker else None
        )
        stage = "partitions"
        partitions = retention_partitions(config, context, days=days, entry_ids=entry_ids)
    except Exception as exc:  # noqa: BLE001 - a read failure is a visible receipt, never a silent idle.
        return _failed_receipt(config, moment, days=days, stage=stage, error=exc)
    clear_backoff(data_directory, installation_backoff_key(getattr(binding, "installation_id", "")))
    if store is None or not callable(delete):
        for partition in partitions:
            _defer(data_directory, partition.key, moment, "store_unavailable")
        return None
    partitions = [
        partition for partition in partitions if pass_due(read_state(storage_dir, partition.key), days, now=moment)
    ]
    if not partitions:
        return None
    if available_seconds < RESERVE_SECONDS:
        for partition in partitions:
            _defer(data_directory, partition.key, moment, "insufficient_budget")
        return None
    cutoff = (moment - timedelta(days=days)).isoformat()
    pass_context = _PassContext(
        store=store,
        config=config,
        storage=storage,
        context=context,
        days=days,
        cutoff=cutoff,
        moment=moment,
    )
    started = time.monotonic()
    receipts: list[dict[str, Any]] = []
    deferred: list[str] = []
    for partition in partitions:
        remaining = available_seconds - (time.monotonic() - started)
        if remaining < RESERVE_SECONDS:
            _defer(data_directory, partition.key, moment, "insufficient_budget")
            deferred.append(partition.key)
            continue
        receipts.append(_expire_partition(pass_context, partition, remaining))
    if not receipts:
        return None
    if len(receipts) == 1 and not deferred:
        return receipts[0]
    return _aggregate(receipts, deferred, pass_context, started)


def _expire_partition(pass_context: _PassContext, partition: RetentionPartition, available_seconds: float) -> dict:
    """Delete one partition's batch, record its receipt and hold its next wake."""
    started = time.monotonic()
    receipt: dict[str, Any] = {
        "started_at": _stamp(pass_context.moment),
        "retention_days": pass_context.days,
        "cutoff": pass_context.cutoff,
        "partition": partition.key,
    }
    try:
        by_reason = expire_tool_vectors(
            pass_context.storage,
            pass_context.context,
            pass_context.store.delete_by_ids,
            partition=partition,
            embedding_space=pass_context.config.embedding_space_id(),
            cutoff=pass_context.cutoff,
            limit=BATCH_LIMIT,
            remaining_seconds=available_seconds,
        )
    except Exception as exc:  # noqa: BLE001 - see module docstring; upkeep never fails a drain.
        receipt.update(outcome="failed", error=type(exc).__name__, expired=0, backlog=False)
    else:
        expired = sum(by_reason.values())
        receipt.update(
            outcome="expired" if expired else "nothing_due",
            expired=expired,
            by_reason=dict(sorted(by_reason.items())),
            backlog=expired >= BATCH_LIMIT,
        )
    elapsed = time.monotonic() - started
    receipt.update(finished_at=_stamp(pass_context.moment + timedelta(seconds=elapsed)), seconds=round(elapsed, 3))
    written = write_state(Path(pass_context.config.vector.storage_dir), partition.key, receipt)
    receipt["state_written"] = written
    data_directory = _backoff_directory(pass_context.config)
    if written:
        clear_backoff(data_directory, partition.key)
    else:
        until = pass_context.moment + RETRY_BACKOFF
        write_backoff(data_directory, partition.key, until=until, reason="state_unwritable")
        receipt["backoff_until"] = _stamp(until)
    return receipt


def _aggregate(
    receipts: list[dict[str, Any]], deferred: list[str], pass_context: _PassContext, started: float
) -> dict[str, Any]:
    """One drain's receipt over every partition it ran.

    A partial pass says so in its outcome and partition counts; the failed
    partitions are also named in ``errors``.  Reporting ``expired`` alone made a
    pass that deleted one entry's batch and failed another's look complete.
    """
    by_reason: Counter[str] = Counter()
    for receipt in receipts:
        by_reason.update(receipt.get("by_reason") or {})
    expired = sum(receipt["expired"] for receipt in receipts)
    failures = [receipt for receipt in receipts if receipt["outcome"] == "failed"]
    succeeded = [receipt for receipt in receipts if receipt["outcome"] != "failed"]
    if failures and succeeded:
        outcome = "partial"
    elif failures:
        outcome = "failed"
    elif expired:
        outcome = "expired"
    else:
        outcome = "nothing_due"
    result: dict[str, Any] = {
        "started_at": _stamp(pass_context.moment),
        "retention_days": pass_context.days,
        "cutoff": pass_context.cutoff,
        "partitions": [receipt["partition"] for receipt in receipts],
        "partitions_run": len(receipts),
        "partitions_succeeded": len(succeeded),
        "partitions_failed": len(failures),
        "expired": expired,
        "by_reason": dict(sorted(by_reason.items())),
        "backlog": any(receipt.get("backlog") for receipt in receipts),
        "outcome": outcome,
        "state_written": all(receipt.get("state_written") for receipt in receipts),
    }
    if deferred:
        result["deferred"] = deferred
        result["partitions_deferred"] = len(deferred)
    if failures:
        result["errors"] = [{"partition": receipt["partition"], "error": receipt["error"]} for receipt in failures]
    backoffs = [receipt["backoff_until"] for receipt in receipts if "backoff_until" in receipt]
    if backoffs:
        result["backoff_until"] = max(backoffs)
    elapsed = time.monotonic() - started
    result.update(finished_at=_stamp(pass_context.moment + timedelta(seconds=elapsed)), seconds=round(elapsed, 3))
    return result


def _defer(data_directory: Path, key: str, moment: datetime, reason: str) -> None:
    """Record why a due pass could not run, so the planner waits instead of retrying at once."""
    write_backoff(data_directory, key, until=moment + RETRY_BACKOFF, reason=reason)


def next_pass_at(state: dict[str, Any], days: int, *, now: datetime) -> datetime | None:
    """When the next pass is due: now after a backlog or a changed window, else one interval after the last pass.

    ``None`` means the window is off.  The scheduler reads this to wake a worker
    for retention alone; it must not turn an idle five-minute timer tick into a
    pass when nothing is due.
    """
    if not days:
        return None
    if state.get("retention_days") != days or state.get("backlog"):
        return now
    last = parse_time(state.get("finished_at"))
    return now if last is None else last + PASS_INTERVAL


def pass_due(state: dict[str, Any], days: int, *, now: datetime) -> bool:
    """A pass is due at once after a backlog or a changed window, else hourly."""
    due = next_pass_at(state, days, now=now)
    return due is not None and due <= now


#: A source row recall can still reach: not suppressed and not superseded by a
#: newer revision of its group.  ``{alias}`` names the source row in the query.
_LIVE_SOURCE = """{alias}.suppressed=0 AND NOT EXISTS (
    SELECT 1 FROM source_events newer
    WHERE newer.source_group_key={alias}.source_group_key
      AND newer.source_revision>{alias}.source_revision)"""

#: A tool output the capture filter withheld, in this release's form and the
#: 2.0 release's (see ``core/events.py``).  Cheap enough to test first.
#: Both conditions are read against ``source_events e``; ``maintenance/shared_import.py``
#: uses them too, so an import never queues an embedding this pass would expire at once.
OMITTED_TOOL_OUTPUT = WITHHELD_TOOL_OUTPUT_SQL


def _omitted_tool_output(alias: str) -> str:
    """``OMITTED_TOOL_OUTPUT`` read against another source alias."""
    return WITHHELD_TOOL_OUTPUT_SQL.replace("e.", f"{alias}.")


def retained_repeated_tool_output(*, entry_partition: bool = True) -> str:
    """SQL for a later copy of an earlier tool output whose vector retention keeps.

    The earlier copy must still have a vector after this pass: inside the
    window, live, not an omitted placeholder, embedded, and not already
    expired.  Without those conditions an old copy deleted for its age also
    made the fresh copy a repeat, and the content lost every vector.

    ``entry_partition`` is false when every row shares one entry, as every row
    of an import does; it also lets a 1109 source store answer, since that
    schema predates ``source_events.entry_id``.  The retention pass reads the
    store's own rows; an import reads the source store, where the earlier
    copy's embed work is still ``done`` rather than the pending work it is
    about to queue.
    """
    entry = "f.entry_id=e.entry_id AND " if entry_partition else ""
    return f"""EXISTS (SELECT 1 FROM source_events f WHERE f.scope_id=e.scope_id
    AND {entry}f.project_id IS e.project_id AND f.branch_id IS e.branch_id
    AND f.role='tool' AND f.content_sha256=e.content_sha256 AND f.read_blocked=0
    AND {_LIVE_SOURCE.format(alias="f")}
    AND COALESCE(json_extract(f.extra_json,'$._scope_recall_admission.disposition'),'')!='source_only'
    AND f.persisted_at>=?
    AND NOT ({_omitted_tool_output("f")})
    AND EXISTS (SELECT 1 FROM work_items w WHERE w.work_type='embed' AND w.subject_ref=f.event_id
                AND w.subject_revision=f.source_revision AND w.state='done')
    AND NOT EXISTS (SELECT 1 FROM expired_vectors expired
                    WHERE expired.source_ref=f.event_id AND expired.source_revision=f.source_revision)
    AND (f.persisted_at<e.persisted_at OR (f.persisted_at=e.persisted_at AND f.event_id<e.event_id)))"""


#: A later copy of an earlier tool output whose vector retention keeps.  The
#: pass uses it so an old copy's age never makes the fresh copy a repeat; the
#: import uses it so it never queues an embedding the pass would expire at once.
RETAINED_REPEATED_TOOL_OUTPUT = retained_repeated_tool_output()


def expire_tool_vectors(
    storage: Any,
    context: Any,
    delete: Callable[[list[str]], Any],
    *,
    embedding_space: str,
    cutoff: str,
    limit: int,
    remaining_seconds: float,
    partition: RetentionPartition | None = None,
) -> Counter:
    """Delete the vectors of the oldest expired tool outputs, then record them by reason.

    A tool output is expired when it entered the store (``persisted_at``)
    before the cutoff, so imported history gets the full window from the day
    of its import.  Three kinds go at once, whatever their age: a repeat of an
    earlier current tool output in the same entry, project, branch and scope
    (the earlier copy keeps its vector, so it must still be inside the window,
    live and not already expired), a source recall can no longer reach
    (suppressed, or an older revision a newer one superseded), and a summary the
    capture filter left in place of an output it withheld.  The unreachable copy
    is recorded as a repeat too: it is not the live copy, and the ledger's
    reasons are fixed by the schema.  The last two are what the intake gate now
    keeps as sources only; the pass clears what older releases embedded.  Only
    sources in this context's entry, project and branch are candidates, so one
    entry's window never expires another entry's vectors, even in a shared
    scope.  Only sources whose embed work finished are touched; one still waiting
    to be embedded is left for a later pass.

    The store goes first.  A pass that dies between the two steps leaves
    vectors gone and rows unwritten, and the next pass repeats an idempotent
    delete.  The other order would leave vectors the ledger says are gone, and
    nothing would ever come back for them.
    """
    scopes = sorted(context.allowed_scope_ids if partition is None else partition.scope_ids)
    marks = ",".join("?" for _ in scopes)
    entry = (partition.entry_id if partition is not None else getattr(context, "entry_id", None)) or "local"
    project_id = getattr(context, "project_id", None)
    branch_id = getattr(context, "branch_id", None)
    with storage.read(context, remaining_seconds=remaining_seconds) as tx:
        rows = (
            tx._check()
            .execute(
                f"""SELECT e.event_id,e.source_revision,
                   CASE WHEN {OMITTED_TOOL_OUTPUT} THEN 'omitted'
                        WHEN e.persisted_at<? THEN 'window'
                        ELSE 'repeat' END AS reason
            FROM source_events e
            WHERE e.role='tool' AND e.scope_id IN ({marks})
              AND e.entry_id=? AND e.project_id IS ? AND e.branch_id IS ?
              AND (({OMITTED_TOOL_OUTPUT}) OR e.persisted_at<?
                   OR NOT ({_LIVE_SOURCE.format(alias="e")}) OR {RETAINED_REPEATED_TOOL_OUTPUT})
              AND EXISTS (SELECT 1 FROM work_items w WHERE w.work_type='embed' AND w.subject_ref=e.event_id
                          AND w.subject_revision=e.source_revision AND w.state='done')
              AND NOT EXISTS (SELECT 1 FROM expired_vectors x WHERE x.source_ref=e.event_id
                              AND x.source_revision=e.source_revision)
            ORDER BY e.persisted_at,e.event_id LIMIT ?""",
                (cutoff, *scopes, entry, project_id, branch_id, cutoff, cutoff, limit),
            )
            .fetchall()
        )
    if not rows:
        return Counter()
    delete([f"p10:{ref}@{revision}:{embedding_space}" for ref, revision, _reason in rows])
    expired_at = utc_now()
    with storage.write(context, remaining_seconds=remaining_seconds) as tx:
        tx._check(write=True).executemany(
            "INSERT OR IGNORE INTO expired_vectors(source_ref,source_revision,expired_at,reason) VALUES (?,?,?,?)",
            [(ref, revision, expired_at, reason) for ref, revision, reason in rows],
        )
    return Counter(reason for _ref, _revision, reason in rows)


def _read_document(storage_dir: Path) -> dict[str, Any] | None:
    try:
        raw = json.loads((Path(storage_dir) / STATE_FILENAME).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return raw if isinstance(raw, dict) else None


def read_state(storage_dir: Path, key: str) -> dict[str, Any]:
    """The last pass's receipt for one partition, or an empty mapping when there has been none.

    A 3.10 state file held one global receipt; it remains the starting point for
    every partition until the first keyed write, so an upgrade does not repeat a
    pass that just ran.
    """
    raw = _read_document(storage_dir)
    if raw is None:
        return {}
    if raw.get("schema") == STATE_SCHEMA:
        records = raw.get("records")
        record = records.get(key) if isinstance(records, dict) else None
        return record if isinstance(record, dict) else {}
    if raw.get("schema") == LEGACY_STATE_SCHEMA:
        return raw
    return {}


def write_state(storage_dir: Path, key: str, payload: dict[str, Any]) -> bool:
    """Record one partition's receipt.  Returns whether it was written; never raises."""
    directory = Path(storage_dir)
    try:
        directory.mkdir(parents=True, exist_ok=True)
        with advisory_file_lock(directory / f"{STATE_FILENAME}.lock", timeout_seconds=1):
            document = _read_document(directory)
            records: dict[str, Any] = {}
            if document is not None and document.get("schema") == STATE_SCHEMA:
                existing = document.get("records")
                if isinstance(existing, dict):
                    records = dict(existing)
            records[key] = dict(payload)
            record = {"schema": STATE_SCHEMA, "records": records}
            partial = directory / f"{STATE_FILENAME}.{os.getpid()}.partial"
            partial.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
            os.replace(partial, directory / STATE_FILENAME)
        return True
    except (OSError, TypeError, ValueError):
        return False


def _backoff_path(data_directory: Path, key: str) -> Path:
    return Path(data_directory) / BACKOFF_FILENAME.format(key=key)


def read_backoff(data_directory: Path, key: str) -> datetime | None:
    """When a deferred partition may try again, or ``None`` when it is not backing off."""
    try:
        raw = json.loads(_backoff_path(data_directory, key).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(raw, dict) or raw.get("schema") != BACKOFF_SCHEMA:
        return None
    return parse_time(raw.get("until"))


def write_backoff(data_directory: Path, key: str, *, until: datetime, reason: str) -> bool:
    """Record a deferred partition's retry time.  Returns whether it was written; never raises.

    Every writer gets its own partial file and replaces the record atomically.
    Two passes deferring the same partition used to share one ``.partial`` name,
    so one could replace the other's half-written file and leave a receipt no
    reader can parse.
    """
    path = _backoff_path(data_directory, key)
    record = {"schema": BACKOFF_SCHEMA, "until": _stamp(until), "reason": reason}
    partial: Path | None = None
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f"{path.name}.",
            suffix=".partial",
            delete=False,
        ) as handle:
            partial = Path(handle.name)
            handle.write(json.dumps(record, ensure_ascii=False, indent=2))
        os.replace(partial, path)
        return True
    except (OSError, TypeError, ValueError):
        return False
    finally:
        if partial is not None:
            try:
                partial.unlink(missing_ok=True)
            except OSError:
                pass


def clear_backoff(data_directory: Path, key: str) -> None:
    """Forget a deferred partition after a pass that ran.  Never raises."""
    try:
        _backoff_path(data_directory, key).unlink(missing_ok=True)
    except OSError:
        return


def _stamp(moment: datetime) -> str:
    """The ``Z``-suffixed form every worker receipt carries (``validation.utc_now``)."""
    return moment.isoformat().replace("+00:00", "Z")


__all__ = [
    "BATCH_LIMIT",
    "PASS_INTERVAL",
    "RESERVE_SECONDS",
    "RETRY_BACKOFF",
    "RetentionPartition",
    "clear_backoff",
    "expire_if_due",
    "expire_tool_vectors",
    "installation_backoff_key",
    "next_pass_at",
    "pass_due",
    "read_backoff",
    "read_state",
    "retention_key",
    "retention_partitions",
    "shared_entry_ids",
    "write_backoff",
    "write_state",
]
