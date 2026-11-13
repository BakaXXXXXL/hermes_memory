#!/usr/bin/env python3
"""共享记忆库自动备份（每日 03:30，由 cron 以 no-agent 脚本模式调用）。

做什么：
  * 用 sqlite3 backup API 生成一致性快照（WAL 安全，不直接 cp 写入中的库）
  * 每日快照：backups/auto/daily/memory-YYYYMMDD.sqlite3
  * 每周快照（周一）：backups/auto/weekly/memory-YYYY-Www.sqlite3
  * 同步备份小文件：MEMORY.md、USER.md、knowledge/notes/、installation.json、
    runtime-config.json、note-index.json → 同名 meta-*.tar.gz
  * 快照和 meta 都先写同目录临时文件，校验后原子替换；每次生成 sha256 manifest
  * generation 只写入 manifest 和 meta tar；快照保持源库 schema 的 PRAGMA user_version
  * --verify 校验 db/meta/manifest 同代、db 的 user_version 与 manifest 记录一致、
    manifest 的 meta.members/required 与 tar 实际成员一致；
    没有 manifest 的存量快照按 legacy 方式校验，避免上线即误报
  * 非阻塞 flock：备份独占、校验共享；锁冲突退出 75，不伪装成成功
  * weekly：周一生成；周一错过时，本周内后续运行会补建；
    已有合法 legacy weekly 时保留，不用当天 daily 覆盖；
    三件套缺失、混合代或 sha256 不一致时自动重建，重建失败则明确退出非 0
  * 保留：daily 7 份、weekly 4 份；更老的自动删除（只删 auto/ 下的文件）
  * 每次对快照跑 PRAGMA quick_check，失败则退出码非 0
  * prune 失败只告警，不把已成功的备份报成失败
  * 启动即 umask 077，备份目录 0700，临时文件从创建起就是 0600

输出：3 行以内摘要；成功退出 0，锁被占用退出 75，失败退出 1。
"""

from __future__ import annotations

import argparse
import errno
import fcntl
import hashlib
import io
import json
import math
import os
import secrets
import shutil
import sqlite3
import sys
import tarfile
import time
import uuid
import zlib
from datetime import datetime
from pathlib import Path
from typing import IO, Callable, Sequence, TypeVar
from urllib.parse import quote

STORE = Path("/root/hermes-shared-store")
META_FILES = [
    ("/root/.hermes/memories/MEMORY.md", "memories/MEMORY.md"),
    ("/root/.hermes/memories/USER.md", "memories/USER.md"),
    ("/root/hermes-shared-store/installation.json", "installation.json"),
    ("/root/hermes-shared-store/runtime-config.json", "runtime-config.json"),
    ("/root/.hermes/scope-recall/runtime-config.json", "scope-recall/runtime-config.json"),
    ("/root/.hermes/state/note-index.json", "note-index.json"),
]
NOTES_DIR = Path("/root/.hermes/knowledge/notes")
REQUIRED_META = [
    "memories/MEMORY.md",
    "memories/USER.md",
    "installation.json",
    "runtime-config.json",
]
KEEP_DAILY = 7
KEEP_WEEKLY = 4
SQLITE_TIMEOUT = 30.0
RETRY_DELAY = 2.0
GENERATION_MEMBER = ".backup-generation.json"
MAX_GENERATION = (1 << 31) - 1
MANIFEST_SCHEMA_VERSION = 3
LOCK_BUSY_EXIT = 75

_T = TypeVar("_T")


class BackupError(RuntimeError):
    """A backup cannot be completed without risking the previous snapshot."""


def _sqlite_uri(path: Path, *, mode: str, immutable: bool = False) -> str:
    suffix = f"?mode={mode}"
    if immutable:
        suffix += "&immutable=1"
    return "file:" + quote(str(path), safe="/") + suffix


def _fsync_path(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        try:
            os.fsync(fd)
        except OSError as exc:
            if path.is_dir() and exc.errno in (errno.EINVAL, errno.ENOTSUP):
                return
            raise
    finally:
        os.close(fd)


def _temp_path(dst: Path) -> Path:
    return dst.parent / f".{dst.name}.tmp.{os.getpid()}.{uuid.uuid4().hex}"


def _new_generation() -> int:
    return secrets.randbits(31) or 1


def _valid_generation(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and 1 <= value <= MAX_GENERATION


def _valid_user_version(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= MAX_GENERATION


def _ensure_private_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path, 0o700)


def _remove_sidecars(path: Path) -> None:
    for suffix in ("-wal", "-shm", "-journal"):
        Path(str(path) + suffix).unlink(missing_ok=True)


def _is_transient_sqlite_error(exc: BaseException) -> bool:
    if not isinstance(exc, sqlite3.OperationalError):
        return False
    message = str(exc).lower()
    return "locked" in message or "busy" in message


def run_with_one_retry(action: Callable[[], _T], *, delay: float) -> _T:
    """Run action once, and retry exactly once after a transient SQLite busy/locked error."""
    try:
        return action()
    except sqlite3.OperationalError as exc:
        if not _is_transient_sqlite_error(exc):
            raise
        if delay:
            time.sleep(delay)
        return action()


def _write_snapshot(src_path: Path, dst_path: Path, timeout: float) -> int:
    """Copy the source database and return its schema user_version.

    The generation is deliberately not stored in the SQLite header: user_version belongs
    to the source schema and must survive a backup unchanged.
    """
    timeout_ms = max(0, int(timeout * 1000))
    src = sqlite3.connect(_sqlite_uri(src_path, mode="ro"), uri=True, timeout=timeout)
    try:
        src.execute(f"PRAGMA busy_timeout = {timeout_ms}")
        source_user_version = int(src.execute("PRAGMA user_version").fetchone()[0])
        out = sqlite3.connect(dst_path, timeout=timeout)
        try:
            out.execute(f"PRAGMA busy_timeout = {timeout_ms}")
            with out:
                src.backup(out)
            out.execute("PRAGMA journal_mode = DELETE")
            snapshot_user_version = int(out.execute("PRAGMA user_version").fetchone()[0])
            if snapshot_user_version != source_user_version:
                raise BackupError(
                    "snapshot user_version changed: "
                    f"source={source_user_version} snapshot={snapshot_user_version}"
                )
        finally:
            out.close()
    finally:
        src.close()
    os.chmod(dst_path, 0o600)
    _fsync_path(dst_path)

    con = sqlite3.connect(_sqlite_uri(dst_path, mode="ro", immutable=True), uri=True, timeout=timeout)
    try:
        con.execute(f"PRAGMA busy_timeout = {timeout_ms}")
        check = con.execute("PRAGMA quick_check").fetchone()[0]
    finally:
        con.close()
    _remove_sidecars(dst_path)
    if check != "ok":
        raise BackupError(f"quick_check failed: {check}")
    return source_user_version


def _snapshot_to_temp(src: Path, dst: Path, timeout: float) -> tuple[Path, int]:
    tmp = _temp_path(dst)
    try:
        source_user_version = _write_snapshot(src, tmp, timeout)
    except BaseException:
        _remove_sidecars(tmp)
        tmp.unlink(missing_ok=True)
        raise
    return tmp, source_user_version


def _meta_entries(
    meta_files: Sequence[tuple[Path, str]], notes_dir: Path, required: set[str]
) -> list[tuple[Path, str]]:
    entries: list[tuple[Path, str]] = []
    seen: set[str] = set()
    for src, arc in meta_files:
        src = Path(src)
        if not src.exists():
            if arc in required:
                raise BackupError(f"missing required meta file: {src} -> {arc}")
            continue
        if not src.is_file():
            raise BackupError(f"meta source is not a regular file: {src}")
        if arc in seen:
            raise BackupError(f"duplicate meta archive name: {arc}")
        seen.add(arc)
        entries.append((src, arc))
    if notes_dir.exists():
        for path in sorted(notes_dir.rglob("*.md")):
            if not path.is_file():
                continue
            arc = "notes/" + path.relative_to(notes_dir).as_posix()
            if arc in seen:
                raise BackupError(f"duplicate meta archive name: {arc}")
            seen.add(arc)
            entries.append((path, arc))
    missing = required - seen
    if missing:
        raise BackupError("missing required meta files: " + ", ".join(sorted(missing)))
    return entries


def validate_meta_tar(path: Path, required: Sequence[str] | set[str]) -> list[str]:
    """Read a meta tar back, reject unsafe/duplicate/unreadable members, and check required names."""
    required_set = set(required)
    names: list[str] = []
    seen: set[str] = set()
    file_names: set[str] = set()
    try:
        with tarfile.open(path, "r:gz") as tf:
            for member in tf.getmembers():
                name = member.name
                pure = Path(name)
                if pure.is_absolute() or ".." in pure.parts:
                    raise BackupError(f"unsafe meta tar member: {name}")
                if name in seen:
                    raise BackupError(f"duplicate meta tar member: {name}")
                seen.add(name)
                names.append(name)
                if member.isfile():
                    file_names.add(name)
                    extracted = tf.extractfile(member)
                    if extracted is None:
                        raise BackupError(f"unreadable meta tar member: {name}")
                    while extracted.read(1024 * 1024):
                        pass
                elif not member.isdir():
                    raise BackupError(f"unsupported meta tar member: {name}")
    except (tarfile.TarError, OSError, EOFError, zlib.error) as exc:
        raise BackupError(f"invalid meta tar: {exc}") from exc
    missing = required_set - file_names
    if missing:
        raise BackupError("meta tar missing required files: " + ", ".join(sorted(missing)))
    return sorted(names)


def _meta_tar_to_temp(
    dst: Path,
    entries: Sequence[tuple[Path, str]],
    required: set[str],
    *,
    generation: int,
    kind: str,
    snapshot_name: str,
) -> tuple[Path, list[str]]:
    tmp = _temp_path(dst)
    try:
        with tarfile.open(tmp, "w:gz") as tf:
            for src, arc in entries:
                tf.add(src, arcname=arc, recursive=False)
            payload = json.dumps(
                {"generation": generation, "kind": kind, "snapshot": snapshot_name},
                sort_keys=True,
            ).encode("utf-8")
            info = tarfile.TarInfo(GENERATION_MEMBER)
            info.size = len(payload)
            info.mode = 0o600
            info.mtime = int(time.time())
            tf.addfile(info, io.BytesIO(payload))
        os.chmod(tmp, 0o600)
        _fsync_path(tmp)
        members = validate_meta_tar(tmp, required)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    return tmp, members


def _read_generation_payload(path: Path) -> dict[str, object]:
    try:
        with tarfile.open(path, "r:gz") as tf:
            member = tf.getmember(GENERATION_MEMBER)
            extracted = tf.extractfile(member)
            if extracted is None:
                raise BackupError(f"unreadable generation member in {path}")
            payload = json.loads(extracted.read().decode("utf-8"))
    except BackupError:
        raise
    except (tarfile.TarError, OSError, EOFError, zlib.error, KeyError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BackupError(f"cannot read generation from {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise BackupError(f"invalid generation payload in {path}")
    return payload


def read_tar_generation(path: Path) -> int:
    payload = _read_generation_payload(path)
    generation = payload.get("generation")
    if not _valid_generation(generation):
        raise BackupError(f"invalid generation in {path}")
    return int(generation)


def read_db_metadata(path: Path, timeout: float) -> tuple[int, str]:
    """Return (schema user_version, quick_check). user_version is schema state, never a generation."""
    con = sqlite3.connect(_sqlite_uri(path, mode="ro"), uri=True, timeout=timeout)
    try:
        con.execute(f"PRAGMA busy_timeout = {max(0, int(timeout * 1000))}")
        user_version = con.execute("PRAGMA user_version").fetchone()[0]
        check = con.execute("PRAGMA quick_check").fetchone()[0]
    finally:
        con.close()
    return int(user_version), str(check)


def _verify_legacy_triple(
    db: Path,
    meta: Path,
    manifest: Path,
    required: set[str],
    timeout: float,
) -> list[str]:
    """Validate a pre-manifest snapshot (db + meta only) without inventing a generation."""
    errors: list[str] = []
    try:
        _, check = read_db_metadata(db, timeout)
        if check != "ok":
            errors.append(f"quick_check failed for {db}: {check}")
    except (sqlite3.Error, OSError, BackupError) as exc:
        errors.append(f"cannot read db {db}: {exc}")

    try:
        validate_meta_tar(meta, required)
        with tarfile.open(meta, "r:gz") as tf:
            has_generation = GENERATION_MEMBER in tf.getnames()
    except BackupError as exc:
        errors.append(f"cannot verify meta {meta}: {exc}")
        return errors
    except (tarfile.TarError, OSError, EOFError, zlib.error) as exc:
        errors.append(f"cannot verify meta {meta}: {exc}")
        return errors
    if has_generation:
        errors.append(f"missing manifest for generated snapshot: {manifest}")
    return errors


def _validate_manifest_meta(
    payload: dict[str, object],
    manifest: Path,
    members: Sequence[str],
) -> list[str]:
    errors: list[str] = []
    meta = payload.get("meta")
    if not isinstance(meta, dict):
        return [f"invalid meta map in {manifest}"]

    recorded_members = meta.get("members")
    if not isinstance(recorded_members, list) or not all(
        isinstance(name, str) for name in recorded_members
    ):
        errors.append(f"invalid meta.members in {manifest}")
        recorded_members = []
    else:
        if len(set(recorded_members)) != len(recorded_members):
            errors.append(f"duplicate meta.members in {manifest}")
        if sorted(recorded_members) != list(members):
            errors.append(f"meta.members mismatch in {manifest}")

    recorded_required = meta.get("required")
    if not isinstance(recorded_required, list) or not all(
        isinstance(name, str) for name in recorded_required
    ):
        errors.append(f"invalid meta.required in {manifest}")
        recorded_required = []
    else:
        if len(set(recorded_required)) != len(recorded_required):
            errors.append(f"duplicate meta.required in {manifest}")
        missing = set(recorded_required) - set(recorded_members)
        if missing:
            errors.append(
                f"meta.required not in members in {manifest}: " + ", ".join(sorted(missing))
            )
    return errors


def _verify_triple(
    db: Path,
    meta: Path,
    manifest: Path,
    required: set[str],
    timeout: float,
    kind: str,
    *,
    allow_legacy: bool = True,
) -> list[str]:
    errors: list[str] = []
    if not db.is_file():
        return [f"missing {db}"]
    if not meta.is_file():
        return [f"missing {meta}"]
    if not manifest.exists() and not manifest.is_symlink():
        if not allow_legacy:
            return [f"missing manifest for snapshot: {manifest}"]
        return _verify_legacy_triple(db, meta, manifest, required, timeout)
    if not manifest.is_file():
        return [f"invalid manifest {manifest}: not a regular file"]

    try:
        payload = json.loads(manifest.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        return [f"invalid manifest {manifest}: {exc}"]
    if not isinstance(payload, dict):
        return [f"invalid manifest {manifest}: not an object"]

    schema_version = payload.get("schema_version")
    if schema_version != MANIFEST_SCHEMA_VERSION:
        errors.append(
            f"unsupported manifest schema_version in {manifest}: {schema_version!r}"
        )
    if payload.get("kind") != kind:
        errors.append(f"manifest kind mismatch in {manifest}: {payload.get('kind')!r} != {kind!r}")
    if payload.get("snapshot") != db.name:
        errors.append(f"manifest snapshot mismatch in {manifest}: {payload.get('snapshot')!r} != {db.name!r}")
    generation = payload.get("generation")
    if not _valid_generation(generation):
        errors.append(f"invalid generation in {manifest}")
        generation = None
    source_user_version = payload.get("source_user_version")
    if not _valid_user_version(source_user_version):
        errors.append(f"invalid source_user_version in {manifest}")
        source_user_version = None
    files = payload.get("files")
    if not isinstance(files, dict):
        errors.append(f"invalid files map in {manifest}")
        files = {}
    for expected in (db.name, meta.name):
        if expected not in files:
            errors.append(f"manifest {manifest.name} missing {expected}")
    for name, info in files.items():
        pure = Path(name)
        if pure.is_absolute() or ".." in pure.parts or pure.name != name:
            errors.append(f"unsafe manifest path: {name}")
            continue
        if not isinstance(info, dict):
            errors.append(f"invalid file entry {name} in {manifest.name}")
            continue
        path = db.parent / name
        if not path.is_file():
            errors.append(f"missing {path}")
            continue
        if info.get("sha256") != _sha256(path):
            errors.append(f"sha256 mismatch: {path}")
        if info.get("size") != path.stat().st_size:
            errors.append(f"size mismatch: {path}")

    try:
        db_user_version, check = read_db_metadata(db, timeout)
        if check != "ok":
            errors.append(f"quick_check failed for {db}: {check}")
        if source_user_version is not None and db_user_version != source_user_version:
            errors.append(
                "user_version mismatch: "
                f"manifest source_user_version={source_user_version} db={db_user_version} ({db.name})"
            )
    except (sqlite3.Error, OSError, BackupError) as exc:
        errors.append(f"cannot read db {db}: {exc}")

    try:
        tar_payload = _read_generation_payload(meta)
        tar_generation = tar_payload.get("generation")
        if not _valid_generation(tar_generation):
            errors.append(f"invalid generation in {meta}")
        elif generation is not None and tar_generation != generation:
            errors.append(f"mixed generation: manifest={generation} meta={tar_generation} ({meta.name})")
        members = validate_meta_tar(meta, required)
        errors.extend(_validate_manifest_meta(payload, manifest, members))
    except BackupError as exc:
        errors.append(f"cannot verify meta {meta}: {exc}")
    return errors


def _allows_legacy(directory: Path, key: str) -> bool:
    """Legacy snapshots are accepted unless the directory has manifests and this key is newest."""
    databases = sorted(
        directory.glob("memory-*.sqlite3"), key=lambda path: _snapshot_key(path.name)
    )
    latest_key = _snapshot_key(databases[-1].name) if databases else None
    has_manifests = any(directory.glob("manifest-*.json"))
    return not (has_manifests and key == latest_key)


def _verify_backup_root(args: argparse.Namespace) -> int:
    root = args.backup_root
    if not root.is_dir():
        print(f"ERROR: backup root does not exist: {root}", file=sys.stderr)
        return 1

    lock = try_acquire_lock(args.lock_file, shared=True)
    if lock is None:
        print(f"verify skipped: another run holds {args.lock_file}", file=sys.stderr)
        return LOCK_BUSY_EXIT
    try:
        errors: list[str] = []
        triples = 0
        required = set(args.required_meta)
        for directory in (root / "daily", root / "weekly"):
            if not directory.is_dir():
                continue
            databases = sorted(
                directory.glob("memory-*.sqlite3"), key=lambda path: _snapshot_key(path.name)
            )
            keys = {_snapshot_key(path.name) for path in databases}
            for db in databases:
                key = _snapshot_key(db.name)
                triples += 1
                errors.extend(
                    _verify_triple(
                        db,
                        directory / f"meta-{key}.tar.gz",
                        directory / f"manifest-{key}.json",
                        required,
                        args.sqlite_timeout,
                        directory.name,
                        allow_legacy=_allows_legacy(directory, key),
                    )
                )
            for path in directory.glob("meta-*.tar.gz"):
                if _snapshot_key(path.name) not in keys:
                    errors.append(f"orphan meta: {path}")
            for path in directory.glob("manifest-*.json"):
                if _snapshot_key(path.name) not in keys:
                    errors.append(f"orphan manifest: {path}")

        if triples == 0:
            errors.append(f"no snapshots found under {root}")
        if errors:
            for error in errors:
                print(f"ERROR: {error}", file=sys.stderr)
            print(f"verify failed: {len(errors)} error(s) across {triples} snapshot(s)", file=sys.stderr)
            return 1
        print(f"verify ok: {triples} snapshot(s)")
        return 0
    finally:
        release_lock(lock)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _manifest_payload(
    kind: str,
    files: Sequence[tuple[str, Path]],
    members: Sequence[str],
    required: set[str],
    generated_at: datetime,
    generation: int,
    source_user_version: int,
) -> dict[str, object]:
    described: dict[str, object] = {}
    for name, path in files:
        described[name] = {"sha256": _sha256(path), "size": path.stat().st_size}
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "kind": kind,
        "generation": generation,
        "source_user_version": source_user_version,
        "generated_at": generated_at.isoformat(timespec="seconds"),
        "snapshot": files[0][0],
        "files": described,
        "meta": {"members": sorted(members), "required": sorted(required)},
    }


def _write_json_to_temp(dst: Path, payload: dict[str, object]) -> Path:
    tmp = _temp_path(dst)
    try:
        data = json.dumps(payload, indent=2, sort_keys=True) + "\n"
        with tmp.open("x", encoding="utf-8") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(tmp, 0o600)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    return tmp


def _copy_to_temp(src: Path, dst: Path) -> Path:
    tmp = _temp_path(dst)
    try:
        shutil.copy2(src, tmp)
        os.chmod(tmp, 0o600)
        _fsync_path(tmp)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    return tmp


def _replace_all(replacements: Sequence[tuple[Path, Path]]) -> None:
    """Replace staged files in order; callers put the manifest last so a crash is detectable by --verify."""
    for tmp, dst in replacements:
        os.replace(tmp, dst)
        _fsync_path(dst.parent)


def _cleanup_stale_temps(directory: Path) -> None:
    if not directory.exists():
        return
    for pattern in (".memory-*.tmp.*", ".meta-*.tmp.*", ".manifest-*.tmp.*"):
        for path in directory.glob(pattern):
            _remove_sidecars(path)
            path.unlink(missing_ok=True)


def _snapshot_key(name: str) -> str:
    for prefix in ("memory-", "meta-", "manifest-"):
        if name.startswith(prefix):
            rest = name[len(prefix) :]
            for suffix in (".sqlite3", ".tar.gz", ".json"):
                if rest.endswith(suffix):
                    return rest[: -len(suffix)]
    return name


def _prune(directory: Path, keep: int) -> int:
    if not directory.exists():
        return 0
    databases = sorted(directory.glob("memory-*.sqlite3"), key=lambda path: path.name)
    kept = databases[-keep:]
    kept_keys = {_snapshot_key(path.name) for path in kept}
    removed = 0
    for path in databases:
        if path not in kept:
            path.unlink(missing_ok=True)
            removed += 1
    for pattern in ("meta-*.tar.gz", "manifest-*.json"):
        for path in directory.glob(pattern):
            if _snapshot_key(path.name) not in kept_keys:
                path.unlink(missing_ok=True)
                removed += 1
    return removed


def _prune_safely(directory: Path, keep: int) -> tuple[int, list[str]]:
    """Prune without turning an already successful backup into a failed run."""
    try:
        return _prune(directory, keep), []
    except OSError as exc:
        return 0, [f"{directory}: {exc}"]


def try_acquire_lock(lock_path: Path, *, shared: bool = False) -> IO[str] | None:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+")
    try:
        mode = fcntl.LOCK_SH if shared else fcntl.LOCK_EX
        fcntl.flock(handle.fileno(), mode | fcntl.LOCK_NB)
    except OSError as exc:
        handle.close()
        if exc.errno in (errno.EACCES, errno.EAGAIN, errno.EWOULDBLOCK):
            return None
        raise
    os.chmod(lock_path, 0o600)
    return handle


def release_lock(handle: IO[str]) -> None:
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    finally:
        handle.close()


def _stage_weekly(
    daily_db: Path,
    daily_meta: Path,
    weekly_db: Path,
    weekly_meta: Path,
    weekly_manifest: Path,
    required: set[str],
    now: datetime,
    generation: int,
    source_user_version: int,
    timeout: float,
) -> None:
    db_tmp = _copy_to_temp(daily_db, weekly_db)
    meta_tmp: Path | None = None
    manifest_tmp: Path | None = None
    try:
        meta_tmp = _copy_to_temp(daily_meta, weekly_meta)
        members = validate_meta_tar(meta_tmp, required)
        db_user_version, check = read_db_metadata(db_tmp, timeout)
        if check != "ok":
            raise BackupError(f"weekly copy quick_check failed: {check}")
        if db_user_version != source_user_version:
            raise BackupError(
                "weekly copy user_version mismatch: "
                f"{db_user_version} != {source_user_version}"
            )
        if read_tar_generation(meta_tmp) != generation:
            raise BackupError("weekly copy meta generation mismatch")
        payload = _manifest_payload(
            "weekly",
            [(weekly_db.name, db_tmp), (weekly_meta.name, meta_tmp)],
            members,
            required,
            now,
            generation,
            source_user_version,
        )
        manifest_tmp = _write_json_to_temp(weekly_manifest, payload)
        _replace_all(
            [
                (db_tmp, weekly_db),
                (meta_tmp, weekly_meta),
                (manifest_tmp, weekly_manifest),
            ]
        )
        db_tmp = meta_tmp = manifest_tmp = None
    finally:
        for tmp in (db_tmp, meta_tmp, manifest_tmp):
            if tmp is not None:
                _remove_sidecars(tmp)
                tmp.unlink(missing_ok=True)
    _remove_sidecars(weekly_db)


def _weekly_state(
    directory: Path,
    key: str,
    required: set[str],
    timeout: float,
) -> tuple[bool, list[str]]:
    """Return whether the weekly triple exists and its verification errors.

    Legacy triples follow the same rule as --verify: accepted unless the directory
    already contains manifests and this key is the newest snapshot.
    """
    db = directory / f"memory-{key}.sqlite3"
    meta = directory / f"meta-{key}.tar.gz"
    manifest = directory / f"manifest-{key}.json"
    exists = db.exists() or meta.exists() or manifest.exists()
    if not exists:
        return False, []
    errors = _verify_triple(
        db,
        meta,
        manifest,
        required,
        timeout,
        "weekly",
        allow_legacy=_allows_legacy(directory, key),
    )
    return True, errors


def _run_backup(args: argparse.Namespace, now: datetime) -> int:
    src = args.store / "memory.sqlite3"
    if not src.exists():
        raise BackupError(f"missing {src}")
    _ensure_private_dir(args.backup_root)
    daily = args.backup_root / "daily"
    weekly = args.backup_root / "weekly"
    _ensure_private_dir(daily)
    _ensure_private_dir(weekly)
    required = set(args.required_meta)
    entries = _meta_entries(args.meta_file, args.notes_dir, required)

    day = now.strftime("%Y%m%d")
    daily_db = daily / f"memory-{day}.sqlite3"
    daily_meta = daily / f"meta-{day}.tar.gz"
    daily_manifest = daily / f"manifest-{day}.json"
    generation = _new_generation()

    db_tmp, source_user_version = run_with_one_retry(
        lambda: _snapshot_to_temp(src, daily_db, args.sqlite_timeout),
        delay=args.retry_delay,
    )
    meta_tmp: Path | None = None
    manifest_tmp: Path | None = None
    try:
        meta_tmp, members = _meta_tar_to_temp(
            daily_meta,
            entries,
            required,
            generation=generation,
            kind="daily",
            snapshot_name=daily_db.name,
        )
        payload = _manifest_payload(
            "daily",
            [(daily_db.name, db_tmp), (daily_meta.name, meta_tmp)],
            members,
            required,
            now,
            generation,
            source_user_version,
        )
        manifest_tmp = _write_json_to_temp(daily_manifest, payload)
        _replace_all(
            [
                (db_tmp, daily_db),
                (meta_tmp, daily_meta),
                (manifest_tmp, daily_manifest),
            ]
        )
        db_tmp = meta_tmp = manifest_tmp = None
    finally:
        for tmp in (db_tmp, meta_tmp, manifest_tmp):
            if tmp is not None:
                _remove_sidecars(tmp)
                tmp.unlink(missing_ok=True)
    _remove_sidecars(daily_db)
    size_mb = round(daily_db.stat().st_size / 1024 / 1024, 1)

    week = now.strftime("%G-W%V")
    weekly_db = weekly / f"memory-{week}.sqlite3"
    weekly_meta = weekly / f"meta-{week}.tar.gz"
    weekly_manifest = weekly / f"manifest-{week}.json"
    weekly_exists, weekly_errors = _weekly_state(
        weekly, week, required, args.sqlite_timeout
    )
    weekly_manifest_present = weekly_manifest.exists() or weekly_manifest.is_symlink()
    refresh_on_monday = now.weekday() == 0 and weekly_manifest_present
    if weekly_errors or not weekly_exists or refresh_on_monday:
        for error in weekly_errors:
            print(
                f"WARNING: weekly snapshot inconsistent; rebuilding: {error}",
                file=sys.stderr,
            )
        _stage_weekly(
            daily_db,
            daily_meta,
            weekly_db,
            weekly_meta,
            weekly_manifest,
            required,
            now,
            generation,
            source_user_version,
            args.sqlite_timeout,
        )
        _, remaining = _weekly_state(weekly, week, required, args.sqlite_timeout)
        if remaining:
            raise BackupError(
                "weekly snapshot still inconsistent after rebuild: "
                + "; ".join(remaining)
            )
        if weekly_exists and not weekly_errors:
            weekly_line = f"weekly: {weekly_db.name}"
        elif weekly_exists:
            weekly_line = f"weekly: {weekly_db.name} (repaired)"
        elif now.weekday() == 0:
            weekly_line = f"weekly: {weekly_db.name}"
        else:
            weekly_line = f"weekly: {weekly_db.name} (backfilled)"
    else:
        weekly_line = f"weekly: present ({weekly_db.name})"

    weekly_removed, weekly_prune_errors = _prune_safely(weekly, args.keep_weekly)
    daily_removed, daily_prune_errors = _prune_safely(daily, args.keep_daily)
    prune_errors = weekly_prune_errors + daily_prune_errors
    for error in prune_errors:
        print(f"WARNING: prune failed: {error}", file=sys.stderr)
    removed = daily_removed + weekly_removed
    print(f"backup ok: {daily_db} ({size_mb} MiB, quick_check ok)")
    print(f"{weekly_line}; pruned {removed} old daily/weekly files")
    print(
        "total auto backups: "
        f"{len(list(args.backup_root.rglob('*.sqlite3')))} sqlite, "
        f"{len(list(args.backup_root.rglob('*.tar.gz')))} meta, "
        f"{len(list(args.backup_root.rglob('manifest-*.json')))} manifest"
    )
    return 0


def _parse_meta_file(value: str) -> tuple[Path, str]:
    src, separator, arc = value.partition(":")
    if not separator or not src or not arc:
        raise argparse.ArgumentTypeError("expected SRC:ARC")
    return Path(src), arc


def _parse_now(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid datetime: {value}") from exc


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Back up the Hermes shared memory store.")
    parser.add_argument("--verify", action="store_true", help="verify existing snapshot triples and exit")
    parser.add_argument("--store", type=Path, default=STORE)
    parser.add_argument("--backup-root", type=Path)
    parser.add_argument("--lock-file", type=Path)
    parser.add_argument("--meta-file", action="append", type=_parse_meta_file, metavar="SRC:ARC")
    parser.add_argument("--required-meta", action="append", metavar="ARC")
    parser.add_argument("--notes-dir", type=Path, default=NOTES_DIR)
    parser.add_argument("--now", type=_parse_now)
    parser.add_argument("--sqlite-timeout", type=float, default=SQLITE_TIMEOUT)
    parser.add_argument("--retry-delay", type=float, default=RETRY_DELAY)
    parser.add_argument("--keep-daily", type=int, default=KEEP_DAILY)
    parser.add_argument("--keep-weekly", type=int, default=KEEP_WEEKLY)
    args = parser.parse_args(argv)
    if args.backup_root is None:
        args.backup_root = args.store / "backups" / "auto"
    if args.lock_file is None:
        args.lock_file = args.backup_root / ".backup.lock"
    if args.meta_file is None:
        args.meta_file = [(Path(src), arc) for src, arc in META_FILES]
    if args.required_meta is None:
        args.required_meta = list(REQUIRED_META)
    if not math.isfinite(args.sqlite_timeout) or args.sqlite_timeout < 0:
        parser.error("--sqlite-timeout must be non-negative and finite")
    if not math.isfinite(args.retry_delay) or args.retry_delay < 0:
        parser.error("--retry-delay must be non-negative and finite")
    if args.keep_daily < 1:
        parser.error("--keep-daily must be at least 1")
    if args.keep_weekly < 1:
        parser.error("--keep-weekly must be at least 1")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    os.umask(0o077)
    args = parse_args(argv)
    now = args.now or datetime.now()
    if args.verify:
        try:
            return _verify_backup_root(args)
        except (sqlite3.Error, OSError, tarfile.TarError) as exc:
            print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
            return 1
    try:
        _ensure_private_dir(args.backup_root)
        lock = try_acquire_lock(args.lock_file)
        if lock is None:
            print(f"backup skipped: another run holds {args.lock_file}")
            return LOCK_BUSY_EXIT
        try:
            _cleanup_stale_temps(args.backup_root / "daily")
            _cleanup_stale_temps(args.backup_root / "weekly")
            return _run_backup(args, now)
        finally:
            release_lock(lock)
    except BackupError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except (sqlite3.Error, OSError, tarfile.TarError) as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
