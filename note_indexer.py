#!/usr/bin/env python3
"""Index knowledge/notes into Scope Recall as full-text chunks.

Design:
  * One note -> several heading-based chunks.
  * Every chunk is an external_document source event.
  * No summary index document; the chunk text is the indexed document.
  * A chunk key names the note, section ordinal and part ordinal.  It is the
    object identity; edits are generations (source revisions) of that object,
    so changing a heading or the text does not create a second competing
    document.
  * Retiring an old generation, or a chunk that no longer exists, goes through
    the core write/deletions path; this script never updates source_events.
  * The local state file is a cache, not authority.  Before a chunk is called a
    duplicate or a permanent rejection, the store is asked whether the recorded
    generation still exists, is unsuppressed and is the visible head.
  * Only `embed` work is queued.  This is expressed through AdmissionPolicy, not
    by rewriting a module global that the rest of the process shares.

Run:
  python3 note_indexer.py --apply
  python3 note_indexer.py --dry-run
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
import re
import sys
import tempfile
from dataclasses import dataclass, fields
from datetime import datetime, timezone
from pathlib import Path

import yaml

from scope_recall.contracts import ContractError, TrustedContext
from scope_recall.core import CoreConfig, MemoryCore
from scope_recall.core.events import prepare_capture
from scope_recall.runtime.instance_config import RuntimeInstanceConfig

try:
    from scope_recall.core.admission import AdmissionPolicy
except ImportError:  # reported by require_core_api before any state is written
    AdmissionPolicy = None

try:
    from scope_recall.core.visibility import allowed as _allowed
except ImportError:  # reported by require_core_api before any state is written
    _allowed = None

NOTES_DIR = Path("/root/.hermes/knowledge/notes")
STATE_PATH = Path("/root/.hermes/state/note-index.json")
CONFIG_PATH = Path("/root/.hermes/scope-recall/runtime-config.json")
SCOPE_ID = "audience:13:owner_private|platform:3:cli|user:5:local|workspace:6:hermes|agent:7:default"
MAX_CHARS = 1800
OVERLAP = 180

#: The store API this script is written against.  A different major version, or
#: an older minor release, is refused before any state is loaded or saved.
CORE_API_VERSION = (3, 10, 7)

FM_RE = re.compile(r"^---\n(.*?)\n---\n", re.S)
HEADING_RE = re.compile(r"^##\s+(.+?)\s*$", re.M)


class CoreCompatibilityError(RuntimeError):
    """The installed Scope Recall core cannot provide the API this indexer needs."""


def core_version() -> tuple[int, int, int]:
    """The installed Scope Recall distribution version."""
    try:
        module = importlib.import_module("scope_recall._version")
        value = str(module.__version__)
    except Exception as exc:
        raise CoreCompatibilityError("Scope Recall core version is unavailable") from exc
    match = re.match(r"^(\d+)\.(\d+)\.(\d+)", value)
    if match is None:
        raise CoreCompatibilityError(f"Scope Recall core version is unreadable: {value}")
    return tuple(int(part) for part in match.groups())


def _resolve_core_symbol(module_name: str, dotted: str):
    try:
        value = importlib.import_module(module_name)
    except Exception as exc:
        raise CoreCompatibilityError(f"Scope Recall core module is missing: {module_name}") from exc
    for name in dotted.split("."):
        value = getattr(value, name, None)
        if value is None:
            raise CoreCompatibilityError(f"Scope Recall core API is missing: {module_name}.{dotted}")
    return value


def require_core_api(core) -> None:
    """Refuse an incompatible core before any state is read or written."""
    version = core_version()
    if version[0] != CORE_API_VERSION[0] or version < CORE_API_VERSION:
        raise CoreCompatibilityError(
            "Scope Recall core "
            f"{'.'.join(map(str, version))} does not match the required API "
            f"{'.'.join(map(str, CORE_API_VERSION))}"
        )
    if AdmissionPolicy is None or not callable(_allowed):
        raise CoreCompatibilityError("Scope Recall core admission or visibility API is missing")
    try:
        config_fields = {item.name for item in fields(CoreConfig)}
        policy_fields = {item.name for item in fields(AdmissionPolicy)}
    except TypeError as exc:
        raise CoreCompatibilityError("Scope Recall core configuration API is unreadable") from exc
    if "admission_policy" not in config_fields:
        raise CoreCompatibilityError("Scope Recall core CoreConfig is missing admission_policy")
    if "consolidation_enabled" not in policy_fields:
        raise CoreCompatibilityError("Scope Recall core AdmissionPolicy is missing consolidation_enabled")
    for module_name, dotted in (
        ("scope_recall.core.source_storage", "Sources.source_identity"),
        ("scope_recall.core.source_storage", "Sources.source_current"),
        ("scope_recall.core.delete_storage", "Deletions.retire_revisions_below"),
        ("scope_recall.core.delete_storage", "Deletions.retire_generations"),
        ("scope_recall.core.composition", "MemoryCore.record_event"),
        ("scope_recall.core.events", "prepare_capture"),
    ):
        if not callable(_resolve_core_symbol(module_name, dotted)):
            raise CoreCompatibilityError(f"Scope Recall core API is not callable: {module_name}.{dotted}")
    if not callable(getattr(core, "record_event", None)):
        raise CoreCompatibilityError("Scope Recall core is missing MemoryCore.record_event")
    storage = getattr(core, "storage", None)
    for name in ("read", "write"):
        if not callable(getattr(storage, name, None)):
            raise CoreCompatibilityError(f"Scope Recall core storage is missing {name}()")


def core_config(binding) -> CoreConfig:
    """The core configuration this indexer runs with: embedding work only."""
    if AdmissionPolicy is None:
        raise CoreCompatibilityError("Scope Recall core is missing AdmissionPolicy")
    return CoreConfig(
        binding,
        write_timeout_seconds=10,
        admission_policy=AdmissionPolicy(consolidation_enabled=False),
    )


def compact_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:20]


def parse_note(path: Path):
    raw = path.read_text(encoding="utf-8")
    fm = {}
    body = raw
    m = FM_RE.match(raw)
    if m:
        try:
            fm = yaml.safe_load(m.group(1)) or {}
        except Exception:
            fm = {}
        body = raw[m.end():]
    title = ""
    for line in body.splitlines():
        if line.startswith("# "):
            title = line[2:].strip()
            break
    if not title:
        title = path.stem
    tags = fm.get("tags") or []
    if isinstance(tags, str):
        tags = [tags]
    return title, [str(t) for t in tags], body


def split_sections(body: str):
    """Return [(heading, text)] preserving the preamble and each ## section."""
    matches = list(HEADING_RE.finditer(body))
    if not matches:
        return [("", body.strip())]
    out = []
    pre = body[:matches[0].start()].strip()
    if pre:
        out.append(("", pre))
    for i, m in enumerate(matches):
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(body)
        text = body[start:end].strip()
        if text:
            out.append((m.group(1).strip(), text))
    return out


def split_long(text: str):
    """Paragraph-aware split with a small overlap; never cuts mid-line."""
    if len(text) <= MAX_CHARS:
        return [text]
    paras = [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]
    chunks = []
    cur = ""
    for p in paras:
        if len(p) > MAX_CHARS:
            # hard fallback for a single huge paragraph
            for i in range(0, len(p), MAX_CHARS - OVERLAP):
                piece = p[i:i + MAX_CHARS]
                if len(piece) > OVERLAP:
                    chunks.append(piece)
            cur = ""
            continue
        if cur and len(cur) + 2 + len(p) > MAX_CHARS:
            chunks.append(cur)
            cur = p
        else:
            cur = (cur + "\n\n" + p).strip()
    if cur:
        chunks.append(cur)
    # small overlap between adjacent chunks
    if len(chunks) > 1:
        merged = []
        prev = ""
        for c in chunks:
            merged.append((prev[-OVERLAP:] + "\n" if prev else "") + c)
            prev = c
        chunks = merged
    return chunks


def chunk_note(path: Path):
    title, tags, body = parse_note(path)
    sections = split_sections(body)
    chunks = []
    for idx, (heading, text) in enumerate(sections):
        if heading.strip() == "索引卡":
            # Existing notes may end with a hand-written summary section.  It
            # would duplicate the note body in the retrieval corpus; the body
            # sections are the authoritative chunks.
            continue
        for part_idx, part in enumerate(split_long(text)):
            label = heading or "正文"
            content = (
                f"Note: {title}\n"
                f"Path: {path}\n"
                f"Tags: {', '.join(tags) if tags else '-'}\n"
                f"Section: {label}\n\n"
                f"{part}"
            )
            chunks.append(
                {
                    "section": idx,
                    "part": part_idx,
                    "heading": label,
                    "content": content,
                    "hash": compact_hash(content),
                }
            )
    return title, tags, chunks


def chunk_key(note_id: str, section: int, part: int) -> str:
    """The stable identity of a chunk; heading and text are generations."""
    return f"note-index:{note_id}:{section:03d}:{part:02d}"


def is_legacy_chunk_key(key: str) -> bool:
    """Earlier keys carried a heading hash, which made a rename a new object."""
    return key.startswith("note-index:") and len(key.split(":")) == 5


def replacement_key(legacy_key: str) -> str | None:
    """The stable key that supersedes a legacy heading-hash key."""
    parts = legacy_key.split(":")
    if len(parts) != 5:
        return None
    try:
        section, part = int(parts[2]), int(parts[3])
    except ValueError:
        return None
    return chunk_key(parts[1], section, part)


def event_id_for(key: str) -> str:
    # Scope Recall derives event ids from the key; this is only for state/debug.
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def chunk_event(path: Path, chunk: dict, key: str, revision: int, now: str) -> dict:
    return {
        "protocol_version": "1.1",
        "source_event_key": key,
        "source_revision": revision,
        "origin": "external_document",
        "role": "document",
        "content": chunk["content"],
        "occurred_at": datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat(),
        "recorded_at": now,
        "time_precision": "instant",
        "capture_state": "complete",
        "evidence_refs": [],
        "source_context": {"platform": "local", "chat_type": "document"},
    }


class StateCorruptError(RuntimeError):
    """The note-index state cannot be trusted; refusing to treat it as empty."""


def load_state(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        state = json.loads(path.read_text())
    except Exception as exc:
        raise StateCorruptError(f"note-index state is not valid JSON: {path}") from exc
    if not isinstance(state, dict):
        raise StateCorruptError(f"note-index state is not an object: {path}")
    notes = state.get("notes")
    if notes is None:
        state["notes"] = {}
    elif not isinstance(notes, dict):
        raise StateCorruptError(f"note-index notes is not an object: {path}")
    for note_id, entry in state["notes"].items():
        if not isinstance(entry, dict):
            raise StateCorruptError(f"note-index entry is not an object: {path}:{note_id}")
        chunks = entry.get("chunks", {})
        if not isinstance(chunks, dict):
            raise StateCorruptError(f"note-index chunks is not an object: {path}:{note_id}")
        for key, chunk in chunks.items():
            if not isinstance(chunk, dict):
                raise StateCorruptError(f"note-index chunk is not an object: {path}:{note_id}:{key}")
            revision = chunk.get("revision")
            if revision is not None and (type(revision) is not int or revision < 1):
                raise StateCorruptError(f"note-index chunk revision is invalid: {path}:{note_id}:{key}")
            pending = chunk.get("retire_below")
            if pending is not None and (type(pending) is not int or pending < 1):
                raise StateCorruptError(f"note-index retire_below is invalid: {path}:{note_id}:{key}")
        rejected = entry.get("rejected", {})
        if not isinstance(rejected, dict):
            raise StateCorruptError(f"note-index rejected is not an object: {path}:{note_id}")
        for key, value in rejected.items():
            if not isinstance(value, dict):
                raise StateCorruptError(f"note-index rejection is not an object: {path}:{note_id}:{key}")
        blocked = entry.get("blocked", {})
        if not isinstance(blocked, dict):
            raise StateCorruptError(f"note-index blocked is not an object: {path}:{note_id}")
        for key, value in blocked.items():
            if not isinstance(value, dict):
                raise StateCorruptError(f"note-index block is not an object: {path}:{note_id}:{key}")
    return state


def _fsync_directory(path: Path) -> None:
    """Persist a rename in the directory that holds the state file."""
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        # Not every filesystem lets a directory be fsynced; the state file
        # itself was already flushed.
        pass
    finally:
        os.close(fd)


def save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(state, ensure_ascii=False, indent=2)
    fd, raw = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    tmp = Path(raw)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            fd = -1
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(tmp, 0o600)
        os.replace(tmp, path)
    except OSError:
        if fd != -1:
            os.close(fd)
        try:
            tmp.unlink()
        except OSError:
            pass
        raise
    _fsync_directory(path.parent)


@dataclass(frozen=True)
class StoreSnapshot:
    """What the store says about one chunk identity right now."""

    ref: str
    ceiling: int
    admitted: bool
    head_revision: int | None
    head_hash: str | None
    head_suppressed: bool
    recorded_revision: int | None
    recorded_exists: bool
    recorded_suppressed: bool


def _snapshot(core: MemoryCore, ctx: TrustedContext, key: str, revision=None) -> StoreSnapshot:
    """Reconcile a state entry with the store, including tombstone visibility."""
    if _allowed is None:
        raise CoreCompatibilityError("Scope Recall core visibility API is missing")
    try:
        with core.storage.read(ctx, remaining_seconds=10.0) as tx:
            ref, ceiling = tx.sources.source_identity(key)
            admitted = bool(_allowed(tx, "event", ref, automatic=True))
            head = tx.sources.source_current(ref) if admitted else None
            if head is not None and head.suppressed:
                # ``source_current`` returns the highest non-blocked revision,
                # which may be a retired one.  A terminal-state recheck still
                # counts an older unsuppressed generation as live, even though
                # a retired newer generation keeps it out of search; callers
                # that need the search-visible generation compare ``ceiling``.
                retired = head.revision
                head = None
                for candidate_revision in range(retired - 1, 0, -1):
                    candidate = tx.source(ref, candidate_revision)
                    if candidate is not None and not candidate.suppressed:
                        head = candidate
                        break
            recorded = tx.source(ref, revision) if type(revision) is int and revision >= 1 else None
    except CoreCompatibilityError:
        raise
    except AttributeError as exc:
        raise CoreCompatibilityError("Scope Recall core source API is missing") from exc
    return StoreSnapshot(
        ref=ref,
        ceiling=ceiling,
        admitted=admitted,
        head_revision=head.revision if head is not None else None,
        head_hash=compact_hash(head.event["content"]) if head is not None else None,
        head_suppressed=bool(head.suppressed) if head is not None else False,
        recorded_revision=revision if type(revision) is int and revision >= 1 else None,
        recorded_exists=recorded is not None,
        recorded_suppressed=bool(recorded.suppressed) if recorded is not None else False,
    )


def _is_current_generation(snapshot: StoreSnapshot, revision, content_hash: str) -> bool:
    """Whether the store serves this generation as the chunk's current content.

    A newer generation shadows every older one even when the newer one was
    retired, so the stored ceiling decides currency; an older generation being
    unsuppressed is not enough.
    """
    return (
        snapshot.admitted
        and snapshot.ceiling == revision
        and snapshot.head_revision == revision
        and snapshot.head_hash == content_hash
        and not snapshot.head_suppressed
        and snapshot.recorded_exists
        and not snapshot.recorded_suppressed
    )


def _identity(core: MemoryCore, ctx: TrustedContext, key: str) -> tuple[str, int]:
    snapshot = _snapshot(core, ctx, key)
    return snapshot.ref, snapshot.ceiling


def _retire_below(
    core: MemoryCore, ctx: TrustedContext, ref: str, before: int, now: str
) -> dict | None:
    """Retire every stored generation below a newly written revision."""
    if before <= 1:
        return None
    try:
        with core.storage.write(ctx, remaining_seconds=10.0) as tx:
            return tx.deletions.retire_revisions_below({ref: before}, now=now)
    except CoreCompatibilityError:
        raise
    except AttributeError as exc:
        raise CoreCompatibilityError("Scope Recall core retirement API is missing") from exc


def _suppress_objects(core: MemoryCore, ctx: TrustedContext, refs, now: str) -> dict | None:
    """Retire every generation of chunk objects that no longer exist in their note."""
    refs = sorted(set(refs))
    if not refs:
        return None
    try:
        with core.storage.write(ctx, remaining_seconds=10.0) as tx:
            return tx.deletions.retire_generations(tuple(refs), now=now)
    except CoreCompatibilityError:
        raise
    except AttributeError as exc:
        raise CoreCompatibilityError("Scope Recall core retirement API is missing") from exc


def _record_with_retry(
    core: MemoryCore, ctx: TrustedContext, event: dict, *, scope_id: str, revision: int
):
    """Record a generation, advancing past a revision the store already holds."""
    receipt = None
    for _ in range(3):
        event["source_revision"] = revision
        try:
            receipt = core.record_event(ctx, event, scope_id=scope_id, remaining_seconds=10.0)
        except AttributeError as exc:
            raise CoreCompatibilityError("Scope Recall core capture API is missing") from exc
        if receipt.disposition != "conflict":
            return receipt
        try:
            _ref, ceiling = _identity(core, ctx, event["source_event_key"])
        except Exception:
            return receipt
        next_revision = max(revision + 1, ceiling + 1)
        if next_revision <= revision:
            next_revision = revision + 1
        revision = next_revision
    return receipt


def _written_revision(receipt, fallback: int) -> int:
    if receipt is not None and receipt.event_refs:
        return int(receipt.event_refs[0].revision)
    return fallback


def _blocked_entry(chunk: dict, reason: str, now: str) -> dict:
    return {
        "hash": chunk["hash"],
        "section": chunk["section"],
        "heading": chunk["heading"],
        "reason": reason,
        "at": now,
    }


def _rejection_entry(chunk: dict, reason: str, now: str) -> dict:
    return {
        "hash": chunk["hash"],
        "section": chunk["section"],
        "heading": chunk["heading"],
        "reason": reason,
        "at": now,
    }


def _mark_blocked(entry: dict, key: str, chunk: dict, reason: str, now: str, stats: dict) -> None:
    entry.setdefault("blocked", {})[key] = _blocked_entry(chunk, reason, now)
    entry["chunks"].pop(key, None)
    entry["rejected"].pop(key, None)
    stats["blocked"] += 1


def index_notes(
    core: MemoryCore,
    ctx: TrustedContext,
    notes_dir: Path,
    state_path: Path,
    *,
    scope_id: str,
    apply: bool,
    now: str | None = None,
) -> dict:
    require_core_api(core)
    if not notes_dir.is_dir():
        raise FileNotFoundError(f"notes directory does not exist: {notes_dir}")
    now = now or datetime.now(timezone.utc).isoformat()
    state = load_state(state_path)
    notes = state.setdefault("notes", {})
    stats = {
        "notes": 0,
        "chunks": 0,
        "inserted": 0,
        "updated": 0,
        "duplicate": 0,
        "removed": 0,
        "rejected": 0,
        "skipped_rejected": 0,
        "blocked": 0,
        "skipped_blocked": 0,
        "conflict": 0,
        "unavailable": 0,
        "failed": 0,
        "retired": 0,
        "legacy_retired": 0,
        "legacy_pending": 0,
    }
    paths = sorted(notes_dir.rglob("*.md"))
    for path in paths:
        stats["notes"] += 1
        note_id = "note-" + hashlib.sha256(str(path).encode()).hexdigest()[:24]
        try:
            title, tags, chunks = chunk_note(path)
        except Exception as exc:
            stats["failed"] += 1
            print(f"failed {path} reason={exc}")
            continue
        entry = notes.setdefault(note_id, {"path": str(path), "title": title, "chunks": {}})
        entry.setdefault("chunks", {})
        entry.setdefault("rejected", {})
        entry.setdefault("blocked", {})
        entry["path"] = str(path)
        entry["title"] = title
        entry["tags"] = tags
        seen = set()
        written_ok = set()
        for chunk in chunks:
            stats["chunks"] += 1
            key = chunk_key(note_id, chunk["section"], chunk["part"])
            seen.add(key)
            old = entry["chunks"].get(key) or {}
            rej_old = entry["rejected"].get(key) or {}
            blocked_old = entry["blocked"].get(key) or {}
            recheck_rejection = False

            if blocked_old:
                if blocked_old.get("hash") != chunk["hash"]:
                    entry["blocked"].pop(key, None)
                    blocked_old = {}
                else:
                    try:
                        snapshot = _snapshot(core, ctx, key)
                    except CoreCompatibilityError:
                        raise
                    except Exception as exc:
                        stats["failed"] += 1
                        print(f"failed to resolve blocked {path} section={chunk['heading']} reason={exc}")
                        continue
                    if not snapshot.admitted:
                        stats["skipped_blocked"] += 1
                        continue
                    # The tombstone is gone; this chunk is no longer terminal.
                    entry["blocked"].pop(key, None)

            pending = old.get("retire_below")
            if apply and type(pending) is int and pending > 1:
                try:
                    ref, _ceiling = _identity(core, ctx, key)
                    receipt = _retire_below(core, ctx, ref, pending, now)
                except CoreCompatibilityError:
                    raise
                except Exception as exc:
                    stats["failed"] += 1
                    print(f"failed to retire {path} section={chunk['heading']} below={pending} reason={exc}")
                else:
                    old.pop("retire_below", None)
                    entry["chunks"][key] = old
                    stats["retired"] += int((receipt or {}).get("retired", 0))
                    # The generation was written on an earlier pass; finishing
                    # its retirement proves the replacement exists.
                    written_ok.add(key)

            if rej_old.get("hash") == chunk["hash"]:
                # A rejection is only terminal while the store has no active
                # generation for this object.  A restore or ledger replay can
                # bring an older generation back; then the rejection must be
                # applied again instead of being skipped forever.
                try:
                    snapshot = _snapshot(core, ctx, key)
                except CoreCompatibilityError:
                    raise
                except Exception as exc:
                    stats["failed"] += 1
                    print(f"failed to recheck rejected {path} section={chunk['heading']} reason={exc}")
                    continue
                if not snapshot.admitted:
                    stats["skipped_rejected"] += 1
                    continue
                if snapshot.head_revision is None or snapshot.head_suppressed:
                    stats["skipped_rejected"] += 1
                    continue
                recheck_rejection = True
            elif rej_old:
                entry["rejected"].pop(key, None)

            if not recheck_rejection and old.get("hash") == chunk["hash"] and not old.get("retire_all"):
                try:
                    snapshot = _snapshot(core, ctx, key, old.get("revision"))
                except CoreCompatibilityError:
                    raise
                except Exception as exc:
                    stats["failed"] += 1
                    print(f"failed to reconcile duplicate {path} section={chunk['heading']} reason={exc}")
                    continue
                if not snapshot.admitted:
                    _mark_blocked(entry, key, chunk, "tombstoned", now, stats)
                    print(f"blocked {path} section={chunk['heading']} reason=tombstoned")
                    continue
                if _is_current_generation(snapshot, old.get("revision"), chunk["hash"]):
                    stats["duplicate"] += 1
                    written_ok.add(key)
                    continue
                # The state hash is not enough: the recorded generation is
                # missing, suppressed, or shadowed by a newer generation.  Fall
                # through and write the current text as a new generation.

            if not apply:
                revision = max(int(old.get("revision", 0)) + 1, 1)
                print(f"would index {path} section={chunk['heading']} rev={revision}")
                continue
            event = chunk_event(path, chunk, key, 1, now)
            try:
                prepared = prepare_capture(event, ctx)
            except CoreCompatibilityError:
                raise
            except ContractError as exc:
                stats["failed"] += 1
                print(f"failed {path} section={chunk['heading']} reason={exc.code}:{exc.field}")
                continue
            except Exception as exc:
                stats["failed"] += 1
                print(f"failed {path} section={chunk['heading']} reason={exc}")
                continue
            if prepared.rejection:
                # Failure isolation: one rejected chunk must not block the rest
                # of the batch.  The chunk cannot be stored, so every previous
                # generation of the same object is retired, not just the one the
                # local state remembers.
                try:
                    snapshot = _snapshot(core, ctx, key, old.get("revision"))
                except CoreCompatibilityError:
                    raise
                except Exception as exc:
                    stats["failed"] += 1
                    print(f"failed to resolve rejected {path} section={chunk['heading']} reason={exc}")
                    continue
                if not snapshot.admitted:
                    entry["chunks"].pop(key, None)
                    entry["blocked"].pop(key, None)
                    entry["rejected"][key] = _rejection_entry(chunk, prepared.rejection, now)
                    stats["rejected"] += 1
                    print(f"rejected {path} section={chunk['heading']} reason={prepared.rejection}")
                    continue
                if old:
                    # The retirement below is destructive and the checkpoint
                    # comes later.  Persist the intent first: after a crash the
                    # next pass must not treat the old hash as still indexed,
                    # even if the note has already reverted to that text.
                    old["retire_all"] = True
                    entry["chunks"][key] = old
                    save_state(state_path, state)
                if snapshot.ceiling:
                    try:
                        retired = _suppress_objects(core, ctx, [snapshot.ref], now)
                    except CoreCompatibilityError:
                        raise
                    except Exception as exc:
                        stats["failed"] += 1
                        print(f"failed to retire rejected {path} section={chunk['heading']} reason={exc}")
                        # The pending marker stays in the state, so the next
                        # pass retries the retirement before recording anything.
                        continue
                    stats["retired"] += int((retired or {}).get("retired", 0))
                # The previous text is no longer indexed.  Keeping it in
                # "chunks" would make a later revert to that legal text look
                # like a duplicate and leave the chunk permanently absent.
                entry["chunks"].pop(key, None)
                entry["blocked"].pop(key, None)
                entry["rejected"][key] = _rejection_entry(chunk, prepared.rejection, now)
                stats["rejected"] += 1
                print(f"rejected {path} section={chunk['heading']} reason={prepared.rejection}")
                continue
            if recheck_rejection:
                entry["rejected"].pop(key, None)
            try:
                snapshot = _snapshot(core, ctx, key, old.get("revision"))
            except CoreCompatibilityError:
                raise
            except Exception as exc:
                stats["unavailable"] += 1
                print(f"unavailable {path} section={chunk['heading']} reason={exc}")
                continue
            if not snapshot.admitted:
                _mark_blocked(entry, key, chunk, "tombstoned", now, stats)
                print(f"blocked {path} section={chunk['heading']} reason=tombstoned")
                continue
            ref, ceiling = snapshot.ref, snapshot.ceiling
            revision = max(int(old.get("revision", 0)) + 1, ceiling + 1)
            try:
                receipt = _record_with_retry(core, ctx, event, scope_id=scope_id, revision=revision)
            except CoreCompatibilityError:
                raise
            except ContractError as exc:
                if exc.code == "ACCESS_DENIED" and exc.field == "source_unavailable":
                    _mark_blocked(entry, key, chunk, "tombstoned", now, stats)
                    print(f"blocked {path} section={chunk['heading']} reason=tombstoned")
                    continue
                stats["failed"] += 1
                print(f"failed {path} section={chunk['heading']} reason={exc.code}:{exc.field}")
                continue
            except Exception as exc:
                stats["unavailable"] += 1
                print(f"unavailable {path} section={chunk['heading']} reason={exc}")
                continue
            outcome = receipt.disposition
            written = _written_revision(receipt, revision)
            if outcome == "inserted":
                stats["inserted"] += 1
                if old:
                    stats["updated"] += 1
                entry["chunks"][key] = {
                    "hash": chunk["hash"],
                    "revision": written,
                    "section": chunk["section"],
                    "heading": chunk["heading"],
                }
                entry["blocked"].pop(key, None)
                written_ok.add(key)
                if written > 1:
                    entry["chunks"][key]["retire_below"] = written
                    try:
                        receipt = _retire_below(core, ctx, ref, written, now)
                    except CoreCompatibilityError:
                        raise
                    except Exception as exc:
                        stats["failed"] += 1
                        print(f"failed to retire {path} section={chunk['heading']} below={written} reason={exc}")
                    else:
                        entry["chunks"][key].pop("retire_below", None)
                        stats["retired"] += int((receipt or {}).get("retired", 0))
                print(f"{outcome} {path} section={chunk['heading']} rev={written}")
            elif outcome == "duplicate":
                # The store already holds this exact generation.  State may be
                # behind; record the generation the store answered with, but
                # only treat it as a successful replacement when it is visible.
                stats["duplicate"] += 1
                entry["chunks"][key] = {
                    "hash": chunk["hash"],
                    "revision": written,
                    "section": chunk["section"],
                    "heading": chunk["heading"],
                }
                entry["blocked"].pop(key, None)
                try:
                    verified = _snapshot(core, ctx, key, written)
                except CoreCompatibilityError:
                    raise
                except Exception:
                    verified = None
                if verified is not None and _is_current_generation(
                    verified, written, chunk["hash"]
                ):
                    written_ok.add(key)
                    if written > 1:
                        entry["chunks"][key]["retire_below"] = written
                        try:
                            receipt = _retire_below(core, ctx, ref, written, now)
                        except CoreCompatibilityError:
                            raise
                        except Exception as exc:
                            stats["failed"] += 1
                            print(f"failed to retire {path} section={chunk['heading']} below={written} reason={exc}")
                        else:
                            entry["chunks"][key].pop("retire_below", None)
                            stats["retired"] += int((receipt or {}).get("retired", 0))
                print(f"{outcome} {path} section={chunk['heading']} rev={written}")
            elif outcome == "conflict":
                stats["conflict"] += 1
                print(f"conflict {path} section={chunk['heading']} rev={revision}")
            elif outcome == "unavailable":
                stats["unavailable"] += 1
                print(f"unavailable {path} section={chunk['heading']} rev={revision}")
            elif outcome == "rejected":
                stats["rejected"] += 1
                entry["chunks"].pop(key, None)
                entry["blocked"].pop(key, None)
                entry["rejected"][key] = _rejection_entry(
                    chunk, receipt.error_code or "rejected", now
                )
                print(f"rejected {path} section={chunk['heading']} rev={revision}")
            else:
                stats["failed"] += 1
                print(f"failed {path} section={chunk['heading']} disposition={outcome}")

        # Retire chunks that no longer exist in the note.  A legacy key waits
        # until its stable replacement has actually been written: otherwise a
        # transient write failure would delete the only indexed copy.
        removed_keys = [key for key in entry["chunks"] if key not in seen]
        legacy_keys = [key for key in removed_keys if is_legacy_chunk_key(key)]
        retire_keys = [key for key in removed_keys if not is_legacy_chunk_key(key)]
        for key in legacy_keys:
            replacement = replacement_key(key)
            if replacement in seen and replacement not in written_ok:
                stats["legacy_pending"] += 1
            else:
                retire_keys.append(key)

        if retire_keys and apply:
            refs = []
            lookup_failed = False
            for key in retire_keys:
                try:
                    snapshot = _snapshot(core, ctx, key)
                except CoreCompatibilityError:
                    raise
                except Exception as exc:
                    stats["failed"] += 1
                    print(f"failed to resolve removed {path} key={key} reason={exc}")
                    lookup_failed = True
                    break
                if snapshot.ceiling and snapshot.admitted:
                    refs.append(snapshot.ref)
            if not lookup_failed:
                if refs:
                    try:
                        retired = _suppress_objects(core, ctx, refs, now)
                    except CoreCompatibilityError:
                        raise
                    except Exception as exc:
                        stats["failed"] += 1
                        print(f"failed to retire removed chunks {path} reason={exc}")
                        lookup_failed = True
                    else:
                        stats["retired"] += int((retired or {}).get("retired", 0))
                if not lookup_failed:
                    for key in retire_keys:
                        entry["chunks"].pop(key, None)
                        entry["blocked"].pop(key, None)
                        if is_legacy_chunk_key(key):
                            stats["legacy_retired"] += 1
                        else:
                            stats["removed"] += 1
        elif retire_keys and not apply:
            for key in retire_keys:
                if is_legacy_chunk_key(key):
                    stats["legacy_retired"] += 1
                else:
                    stats["removed"] += 1

        # Drop terminal records for chunks that no longer exist in the note.
        for key in list(entry.get("rejected", {})):
            if key not in seen:
                entry["rejected"].pop(key, None)
        for key in list(entry.get("blocked", {})):
            if key not in seen:
                entry["blocked"].pop(key, None)

        # Per-note checkpoint: save progress even if a later note fails.
        if apply:
            save_state(state_path, state)

    if apply:
        # Retire note entries whose file disappeared; otherwise their chunks
        # stay active forever because the scan never visits them again.
        seen_note_ids = {
            "note-" + hashlib.sha256(str(path).encode()).hexdigest()[:24] for path in paths
        }
        for note_id in list(notes):
            if note_id in seen_note_ids:
                continue
            entry = notes[note_id]
            keys = list(entry.get("chunks", {}))
            refs = []
            lookup_failed = False
            for key in keys:
                try:
                    snapshot = _snapshot(core, ctx, key)
                except CoreCompatibilityError:
                    raise
                except Exception as exc:
                    stats["failed"] += 1
                    print(f"failed to resolve vanished note {note_id} key={key} reason={exc}")
                    lookup_failed = True
                    break
                if snapshot.ceiling and snapshot.admitted:
                    refs.append(snapshot.ref)
            if lookup_failed:
                continue
            retired = None
            if refs:
                try:
                    retired = _suppress_objects(core, ctx, refs, now)
                except CoreCompatibilityError:
                    raise
                except Exception as exc:
                    stats["failed"] += 1
                    print(f"failed to retire vanished note {note_id} reason={exc}")
                    continue
            stats["retired"] += int((retired or {}).get("retired", 0))
            stats["removed"] += len(keys)
            del notes[note_id]

        save_state(state_path, state)

    return stats


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--notes-dir", type=Path, default=NOTES_DIR)
    ap.add_argument("--state-path", type=Path, default=STATE_PATH)
    args = ap.parse_args()
    apply = args.apply and not args.dry_run

    raw_cfg = json.loads(CONFIG_PATH.read_text())
    cfg = RuntimeInstanceConfig.from_mapping(raw_cfg)
    if SCOPE_ID not in cfg.allowed_scope_ids:
        print(f"scope not allowed: {SCOPE_ID}", file=sys.stderr)
        return 2
    ctx = TrustedContext(
        binding=cfg.binding,
        session_id="note-indexer",
        allowed_scope_ids=cfg.allowed_scope_ids,
        actor_origin="external_document",
        entry_id="main-gateway" if cfg.binding.installation_kind == "shared" else None,
    )

    try:
        core = MemoryCore(core_config(cfg.binding))
        stats = index_notes(
            core,
            ctx,
            args.notes_dir,
            args.state_path,
            scope_id=SCOPE_ID,
            apply=apply,
        )
    except CoreCompatibilityError as exc:
        print(f"core incompatible: {exc}", file=sys.stderr)
        return 4
    except StateCorruptError as exc:
        print(f"state error: {exc}", file=sys.stderr)
        return 3
    except FileNotFoundError as exc:
        print(f"input error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(stats, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
