#!/usr/bin/env python3
"""Health checks and alerts for the Hermes memory stack.

The watchdog runs from a systemd timer and as the ``OnFailure`` target of the
note-indexer, wake and gateway units.  A healthy run prints nothing.  Alerts go
through independent channels so a failed gateway cannot swallow the message
that reports it.

The checks are deliberately read-only: systemd unit results, state-file
freshness, the gateway heartbeat, backup age and manifest consistency, the
weekly review report, recent source events and de-duplicated capture
rejections.  The only files this script writes are its state file, its JSONL
log, the pending OnFailure queue and quarantine copies of corrupt queue data.
"""

from __future__ import annotations

import argparse
import copy
import fcntl
import glob
import gzip
import hashlib
import json
import math
import os
import re
import smtplib
import sqlite3
import ssl
import subprocess
import sys
import tarfile
import tempfile
import time
import zlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from email.utils import parseaddr
from pathlib import Path
from typing import Callable, Iterable, Sequence

DEFAULT_CONFIG_PATH = "/root/.hermes/health-watchdog.json"
DEFAULT_STATE_PATH = "/root/.hermes/scripts/.health_watchdog.state.json"
DEFAULT_LOG_PATH = "/root/.hermes/logs/health-watchdog.log"
DEFAULT_HERMES_BIN = "/usr/local/lib/hermes-agent/.hermes/bin/hermes"
DEFAULT_SMTP_ENV = "/root/.hermes/qq_smtp.env"
REJECTION_MARKER = "not stored (plaintext_secret_rejected)"
LOG_TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})")
MAX_ALERT_CHARS = 3500
MAX_LOG_BYTES = 5_000_000
MAX_REJECTION_KEYS = 5000
LOCK_TIMEOUT_SECONDS = 30
BACKUP_MANIFEST_SCHEMA = 3
BACKUP_GENERATION_MEMBER = ".backup-generation.json"
MAX_GENERATION = (1 << 31) - 1

DEFAULTS: dict = {
    "paths": {
        "note_state": "/root/.hermes/state/note-index.json",
        "notes_dir": "/root/.hermes/knowledge/notes",
        "heartbeat": "/root/.hermes/state/gateway.heartbeat",
        "backup_daily": "/root/hermes-shared-store/backups/auto/daily",
        "backup_weekly": "/root/hermes-shared-store/backups/auto/weekly",
        "weekly_reports": "/root/.hermes/reports/memory-health",
        "errors_log": "/root/.hermes/logs/errors.log",
        "memory_db": "/root/hermes-shared-store/memory.sqlite3",
        "cron_jobs": "/root/.hermes/cron/jobs.json",
        "state": DEFAULT_STATE_PATH,
        "log": DEFAULT_LOG_PATH,
        "lock": "/root/.hermes/scripts/.health_watchdog.lock",
        "pending": "/root/.hermes/scripts/.health_watchdog.pending.jsonl",
    },
    "units": {
        "note_indexer": "note-indexer.service",
        "note_indexer_timer": "note-indexer.timer",
        "wake": "scope-recall-wake.service",
        "wake_timer": "scope-recall-wake.timer",
        "gateway": "hermes-gateway.service",
    },
    "thresholds": {
        "note_state_max_age": 900,
        "note_timer_max_age": 1200,
        "wake_max_age": 900,
        "heartbeat_max_age": 180,
        "backup_max_age": 108000,
        "backup_weekly_max_age": 691200,
        "backup_min_bytes": 1_000_000,
        "backup_cron_max_age": 129600,
        "backup_cron_grace": 7200,
        "weekly_report_max_age": 691200,
        "weekly_next_run_grace": 7200,
        "entry_max_age": 86400,
        "entry_scan_rows": 5000,
        "rejection_lookback": 604800,
        "rejection_state_keep": 2592000,
        "rejection_scan_max_bytes": 8_000_000,
        "rejection_scan_max_lines": 50_000,
        "dedup_seconds": 1800,
        "command_timeout": 15,
        "lock_timeout": LOCK_TIMEOUT_SECONDS,
        "failure_lock_timeout": 120,
        "pending_failure_max_age": 86400,
        "max_alert_issues": 20,
    },
    "notify": {
        "channels": ["hermes", "smtp"],
        "hermes_bin": DEFAULT_HERMES_BIN,
        "hermes_target": "telegram",
        "smtp_env": DEFAULT_SMTP_ENV,
        "smtp_timeout": 20,
    },
    "weekly_job_id": "549fe0406878",
    "backup_job_id": "4d313aab436e",
    "rejection_marker": REJECTION_MARKER,
    "rejection_bootstrap": True,
}


class ConfigError(RuntimeError):
    """The watchdog cannot run with the supplied configuration."""


class SystemdError(RuntimeError):
    """``systemctl show`` did not return usable data."""


class ChannelError(RuntimeError):
    """One notification channel failed to deliver."""


class LockBusy(RuntimeError):
    """Another watchdog instance held the instance lock too long."""


class InstanceLock:
    """Exclusive, non-blocking-with-timeout lock shared by timer and OnFailure runs."""

    def __init__(self, path: Path, timeout: float = LOCK_TIMEOUT_SECONDS):
        self.path = path
        self.timeout = max(0.0, float(timeout))
        self.handle = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = open(self.path, "a+", encoding="utf-8")
        os.chmod(self.path, 0o600)
        deadline = time.monotonic() + self.timeout
        while True:
            try:
                fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    self.handle.close()
                    self.handle = None
                    raise LockBusy(f"another watchdog instance holds {self.path}")
                time.sleep(0.2)
        self.handle.seek(0)
        self.handle.truncate()
        self.handle.write(f"{os.getpid()}\n")
        self.handle.flush()
        os.fsync(self.handle.fileno())
        return self

    def __exit__(self, exc_type, exc, traceback):
        if self.handle is not None:
            try:
                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
            finally:
                self.handle.close()
                self.handle = None
        return False


class PendingFailures:
    """Append-only queue of ``OnFailure`` units that must survive lock contention.

    ``OnFailure`` runs can arrive while the timer instance holds the watchdog
    lock.  The unit is recorded before the lock is attempted, so a lock timeout
    leaves a durable marker for the next timer run instead of dropping the alert.
    """

    def __init__(self, path: Path, max_age: float = 86400):
        self.path = path
        self.max_age = max(0.0, float(max_age))
        self.quarantine_path: Path | None = None
        self.read_error = ""
        self.expired: list[dict] = []

    def _open(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        return self.path.open("a+b")

    def append(self, unit: str, moment: float) -> None:
        with self._open() as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                handle.seek(0, os.SEEK_END)
                if handle.tell() > 0:
                    handle.seek(-1, os.SEEK_END)
                    if handle.read(1) != b"\n":
                        handle.write(b"\n")
                handle.write(
                    (json.dumps({"unit": unit, "at": float(moment)}, ensure_ascii=False) + "\n").encode(
                        "utf-8"
                    )
                )
                handle.flush()
                os.fsync(handle.fileno())
                os.fchmod(handle.fileno(), 0o600)
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    @staticmethod
    def _decode_lines(data: bytes) -> tuple[list[str], int, str]:
        try:
            return data.decode("utf-8").splitlines(keepends=True), 0, ""
        except UnicodeDecodeError as exc:
            lines: list[str] = []
            invalid = 0
            for raw in data.splitlines(keepends=True):
                try:
                    lines.append(raw.decode("utf-8"))
                except UnicodeDecodeError:
                    invalid += 1
            return lines, invalid, sanitize(exc)

    def _quarantine(self, data: bytes) -> tuple[Path | None, str]:
        try:
            descriptor, name = tempfile.mkstemp(
                prefix=f"{self.path.name}.corrupt-",
                dir=str(self.path.parent),
            )
        except OSError as exc:
            return None, sanitize(exc)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(name, 0o600)
        except OSError as exc:
            try:
                os.unlink(name)
            except OSError:
                pass
            return None, sanitize(exc)
        return Path(name), ""

    @staticmethod
    def _parse_line(line: str) -> tuple[dict | None, str]:
        if not line.strip():
            return None, ""
        try:
            entry = json.loads(line)
        except ValueError as exc:
            return None, sanitize(exc)
        if not isinstance(entry, dict):
            return None, "not a JSON object"
        unit = entry.get("unit")
        try:
            moment = float(entry.get("at"))
        except (TypeError, ValueError, OverflowError):
            return None, "invalid at"
        if not isinstance(unit, str) or not unit or not math.isfinite(moment):
            return None, "invalid unit/at"
        return {"unit": unit, "at": moment}, ""

    def _load_locked(self, handle, now: float, quarantine: bool) -> tuple[list[dict], str]:
        handle.seek(0)
        data = handle.read()
        lines, invalid_utf8, decode_error = self._decode_lines(data)

        latest: dict[str, dict] = {}
        expired: list[dict] = []
        readable_lines: list[str] = []
        invalid_json = 0
        for line in lines:
            entry, _ = self._parse_line(line)
            if entry is None:
                if line.strip():
                    invalid_json += 1
                continue
            readable_lines.append(line if line.endswith("\n") else line + "\n")
            unit = entry["unit"]
            moment = entry["at"]
            if now - moment > self.max_age:
                expired.append({"unit": unit, "at": moment})
                continue
            if unit not in latest or moment >= latest[unit]["at"]:
                latest[unit] = {"unit": unit, "at": moment}
        self.expired = sorted(expired, key=lambda entry: entry["at"])

        parts: list[str] = []
        if invalid_utf8:
            parts.append(f"{invalid_utf8} 行非法 UTF-8")
        if invalid_json:
            parts.append(f"{invalid_json} 行损坏的 JSON")
        detail = "、".join(parts)
        error = ""
        if detail and not quarantine:
            error = f"待处理队列包含 {detail}（dry-run 未隔离）"
            if decode_error:
                error += f"：{decode_error}"
        elif detail:
            quarantine_path, quarantine_error = self._quarantine(data)
            if quarantine_path is None:
                error = f"待处理队列包含 {detail}，且无法隔离：{quarantine_error}"
                if decode_error:
                    error += f"（{decode_error}）"
            else:
                self.quarantine_path = quarantine_path
                try:
                    handle.seek(0)
                    handle.truncate()
                    for line in readable_lines:
                        handle.write(line.encode("utf-8"))
                    handle.flush()
                    os.fsync(handle.fileno())
                except OSError as exc:
                    error = (
                        f"待处理队列包含 {detail}，已隔离到 {quarantine_path}，"
                        f"但无法重写队列：{sanitize(exc)}"
                    )
                else:
                    error = (
                        f"待处理队列包含 {detail}，已隔离到 {quarantine_path}；"
                        f"保留 {len(readable_lines)} 行可读记录"
                    )
                    if decode_error:
                        error += f"（{decode_error}）"

        return sorted(latest.values(), key=lambda entry: entry["at"]), error

    def load(self, now: float, *, quarantine: bool = True) -> tuple[list[dict], str]:
        self.quarantine_path = None
        self.read_error = ""
        self.expired = []
        if not self.path.exists():
            return [], ""
        try:
            with self._open() as handle:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                try:
                    entries, error = self._load_locked(handle, now, quarantine)
                finally:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        except OSError as exc:
            self.read_error = sanitize(exc)
            return [], self.read_error
        self.read_error = error
        return entries, error

    def clear(self, before: float) -> None:
        if not self.path.exists():
            return
        with self._open() as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                handle.seek(0)
                data = handle.read()
                lines, invalid_utf8, _ = self._decode_lines(data)
                kept: list[str] = []
                invalid_json = 0
                for line in lines:
                    entry, _ = self._parse_line(line)
                    if entry is None:
                        if line.strip():
                            invalid_json += 1
                        continue
                    if entry["at"] > before:
                        kept.append(line if line.endswith("\n") else line + "\n")
                if invalid_utf8 or invalid_json:
                    quarantine, quarantine_error = self._quarantine(data)
                    if quarantine is None:
                        raise OSError(
                            f"cannot quarantine corrupt pending queue: {quarantine_error}"
                        )
                    self.quarantine_path = quarantine
                handle.seek(0)
                handle.truncate()
                for line in kept:
                    handle.write(line.encode("utf-8"))
                handle.flush()
                os.fsync(handle.fileno())
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@dataclass(frozen=True)
class Issue:
    code: str
    message: str
    severity: str = "error"
    dedup_key: str = ""

    @property
    def signature(self) -> str:
        return f"{self.code}:{self.dedup_key}" if self.dedup_key else self.code


@dataclass(frozen=True)
class DeliveryResult:
    channel: str
    ok: bool
    error: str = ""


@dataclass
class Decision:
    action: str
    subject: str = ""
    body: str = ""
    state: dict | None = None


def sanitize(value: object, limit: int = 240) -> str:
    """Shorten an error string and remove credential-shaped fragments."""
    text = str(value or "").replace("\n", " ")
    text = re.sub(r"bot\d+:[A-Za-z0-9_-]+", "bot<redacted>", text)
    text = re.sub(r"(?i)bearer\s+\S+", "Bearer <redacted>", text)
    text = re.sub(r"(?i)(api[_-]?key|token|password|auth[_-]?code)\s*[=:]\s*\S+", r"\1=<redacted>", text)
    text = re.sub(r"(?i)\b(sk-|ghp_|xox[baprs]-)[A-Za-z0-9_-]{8,}", r"\1<redacted>", text)
    text = re.sub(r"https?://\S+", "<url>", text)
    return text[:limit].strip()


def _merge(base: dict, override: dict) -> dict:
    merged = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_config(path: str | Path) -> dict:
    config = copy.deepcopy(DEFAULTS)
    config_path = Path(path)
    if not config_path.exists():
        return config
    try:
        raw = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ConfigError(f"cannot read {config_path}: {sanitize(exc)}") from exc
    if not isinstance(raw, dict):
        raise ConfigError(f"{config_path} must contain a JSON object")
    return _merge(config, raw)


def parse_iso(value: object) -> float | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def format_age(seconds: float) -> str:
    seconds = max(0, int(seconds))
    if seconds < 60:
        return f"{seconds}秒"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}分钟"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}小时{minutes}分" if minutes else f"{hours}小时"
    days, hours = divmod(hours, 24)
    return f"{days}天{hours}小时" if hours else f"{days}天"


def read_uptime() -> float:
    try:
        return float(Path("/proc/uptime").read_text(encoding="ascii").split()[0])
    except (OSError, ValueError, IndexError) as exc:
        raise SystemdError(f"cannot read /proc/uptime: {sanitize(exc)}") from exc


def _unix_age(value: object, now: float) -> float | None:
    """Age from systemd's ``--timestamp=unix`` value (``@<epoch>``).

    systemd 259 formats ``*TimestampMonotonic`` as a duration string, so that
    property cannot be used for age checks; the realtime unix value can.
    """
    if not isinstance(value, str):
        return None
    text = value.strip()
    if text.startswith("@"):
        try:
            return now - float(text[1:])
        except ValueError:
            return None
    parsed = parse_iso(text)
    return now - parsed if parsed is not None else None


class Systemd:
    """Read-only access to ``systemctl show``."""

    def __init__(
        self,
        systemctl: str = "systemctl",
        timeout: float = 15,
        runner: Callable = subprocess.run,
    ):
        self.systemctl = systemctl
        self.timeout = timeout
        self.runner = runner

    def show(self, unit: str, properties: Sequence[str]) -> dict[str, str]:
        command = [self.systemctl, "show", unit, "--no-pager", "--timestamp=unix"]
        command.extend(f"--property={name}" for name in properties)
        try:
            completed = self.runner(
                command,
                capture_output=True,
                text=True,
                timeout=self.timeout,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise SystemdError(f"systemctl show {unit} failed: {sanitize(exc)}") from exc
        if completed.returncode != 0:
            raise SystemdError(
                f"systemctl show {unit} exited {completed.returncode}: {sanitize(completed.stderr)}"
            )
        values: dict[str, str] = {}
        for line in completed.stdout.splitlines():
            if "=" in line:
                key, value = line.split("=", 1)
                values[key] = value
        return values


def _newest(directory: Path, pattern: str) -> Path | None:
    if not directory.is_dir():
        return None
    files = [path for path in directory.glob(pattern) if path.is_file()]
    return max(files, key=lambda path: path.stat().st_mtime) if files else None


def _oldest(directory: Path, pattern: str) -> Path | None:
    if not directory.is_dir():
        return None
    files = [path for path in directory.glob(pattern) if path.is_file()]
    return min(files, key=lambda path: path.stat().st_mtime) if files else None


def check_note_indexer(config: dict, systemd: Systemd, uptime: float, now: float) -> list[Issue]:
    del uptime
    units = config["units"]
    thresholds = config["thresholds"]
    issues: list[Issue] = []

    props = systemd.show(
        units["note_indexer"],
        ["ActiveState", "SubState", "Result", "ExecMainStatus"],
    )
    result = props.get("Result", "")
    status = props.get("ExecMainStatus", "")
    if result not in ("", "success") or status not in ("", "0") or props.get("ActiveState") == "failed":
        issues.append(
            Issue(
                "note_indexer.exit",
                f"note-indexer 上次运行失败：Result={result or '?'} exit={status or '?'}",
                dedup_key=f"{result}:{status}",
            )
        )

    timer = systemd.show(units["note_indexer_timer"], ["ActiveState", "LastTriggerUSec"])
    if timer.get("ActiveState") != "active":
        issues.append(
            Issue(
                "note_indexer.timer_inactive",
                f"note-indexer.timer 状态 {timer.get('ActiveState') or 'unknown'}，索引不会自动运行",
                dedup_key="timer",
            )
        )
    else:
        trigger_age = _unix_age(timer.get("LastTriggerUSec"), now)
        if trigger_age is not None and trigger_age > thresholds["note_timer_max_age"]:
            issues.append(
                Issue(
                    "note_indexer.timer_stale",
                    f"note-indexer.timer 已 {format_age(trigger_age)} 未触发",
                    dedup_key="timer",
                )
            )

    state_path = Path(config["paths"]["note_state"])
    if not state_path.exists():
        issues.append(
            Issue("note_indexer.state_missing", f"索引状态文件缺失：{state_path}", dedup_key="missing")
        )
        return issues

    state_age = now - state_path.stat().st_mtime
    if state_age > thresholds["note_state_max_age"]:
        issues.append(
            Issue(
                "note_indexer.state_stale",
                f"索引状态 {format_age(state_age)} 未更新"
                f"（阈值 {format_age(thresholds['note_state_max_age'])}）",
                dedup_key="stale",
            )
        )

    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        issues.append(
            Issue(
                "note_indexer.state_invalid",
                f"索引状态无法解析：{sanitize(exc)}",
                dedup_key="invalid",
            )
        )
        return issues

    notes = state.get("notes") if isinstance(state, dict) else None
    if not isinstance(notes, dict):
        issues.append(
            Issue("note_indexer.state_invalid", "索引状态缺少 notes 映射", dedup_key="invalid")
        )
        return issues

    rejected = sum(
        len(entry.get("rejected", {}))
        for entry in notes.values()
        if isinstance(entry, dict) and isinstance(entry.get("rejected", {}), dict)
    )
    if rejected:
        rejected_keys = []
        for key, value in sorted(notes.items()):
            rejected_map = value.get("rejected") if isinstance(value, dict) else None
            if isinstance(rejected_map, dict) and rejected_map:
                rejected_keys.append(f"{key}:{sorted(rejected_map.items())}")
        digest = hashlib.sha256(
            "\n".join(rejected_keys).encode("utf-8", "replace")
        ).hexdigest()[:16]
        issues.append(
            Issue(
                "note_indexer.rejected",
                f"索引状态记录 {rejected} 个永久拒绝块（内容被安全策略拒绝）",
                severity="notice",
                dedup_key=digest,
            )
        )

    notes_dir = Path(config["paths"]["notes_dir"])
    if notes_dir.is_dir():
        note_count = sum(1 for _ in notes_dir.rglob("*.md"))
        if note_count and not notes:
            issues.append(
                Issue(
                    "note_indexer.state_empty",
                    f"笔记目录有 {note_count} 篇，但索引状态没有任何笔记",
                    dedup_key="empty",
                )
            )
    return issues


def check_wake(config: dict, systemd: Systemd, uptime: float, now: float) -> list[Issue]:
    del uptime
    units = config["units"]
    thresholds = config["thresholds"]
    issues: list[Issue] = []

    props = systemd.show(
        units["wake"],
        ["ActiveState", "SubState", "Result", "ExecMainStatus", "ExecMainExitTimestamp"],
    )
    result = props.get("Result", "")
    status = props.get("ExecMainStatus", "")
    if result not in ("", "success") or status not in ("", "0") or props.get("ActiveState") == "failed":
        issues.append(
            Issue(
                "wake.exit",
                f"scope-recall-wake 上次运行失败：Result={result or '?'} exit={status or '?'}",
                dedup_key=f"{result}:{status}",
            )
        )
    if props.get("ActiveState") not in ("active", "activating"):
        exit_age = _unix_age(props.get("ExecMainExitTimestamp"), now)
        if exit_age is not None and exit_age > thresholds["wake_max_age"]:
            issues.append(
                Issue(
                    "wake.stale",
                    f"scope-recall-wake 已 {format_age(exit_age)} 未成功运行",
                    dedup_key="stale",
                )
            )

    timer = systemd.show(units["wake_timer"], ["ActiveState", "LastTriggerUSec"])
    if timer.get("ActiveState") != "active":
        issues.append(
            Issue(
                "wake.timer_inactive",
                f"scope-recall-wake.timer 状态 {timer.get('ActiveState') or 'unknown'}，唤醒不会自动运行",
                dedup_key="timer",
            )
        )
    else:
        trigger_age = _unix_age(timer.get("LastTriggerUSec"), now)
        if trigger_age is not None and trigger_age > thresholds["wake_max_age"]:
            issues.append(
                Issue(
                    "wake.timer_stale",
                    f"scope-recall-wake.timer 已 {format_age(trigger_age)} 未触发",
                    dedup_key="timer",
                )
            )
    return issues


def check_gateway(config: dict, systemd: Systemd, uptime: float, now: float) -> list[Issue]:
    del uptime
    units = config["units"]
    thresholds = config["thresholds"]
    props = systemd.show(units["gateway"], ["ActiveState", "SubState", "MainPID"])
    if props.get("ActiveState") != "active":
        return [
            Issue(
                "gateway.inactive",
                f"hermes-gateway 状态 {props.get('ActiveState') or 'unknown'}/{props.get('SubState') or '?'}",
                dedup_key=props.get("ActiveState", "unknown"),
            )
        ]

    issues: list[Issue] = []
    heartbeat_path = Path(config["paths"]["heartbeat"])
    if not heartbeat_path.exists():
        return [
            Issue(
                "gateway.heartbeat_missing",
                f"网关在运行，但心跳文件缺失：{heartbeat_path}",
                dedup_key="missing",
            )
        ]

    try:
        heartbeat = json.loads(heartbeat_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        return [
            Issue(
                "gateway.heartbeat_invalid",
                f"网关心跳无法解析：{sanitize(exc)}",
                dedup_key="invalid",
            )
        ]
    if not isinstance(heartbeat, dict):
        return [
            Issue("gateway.heartbeat_invalid", "网关心跳不是 JSON 对象", dedup_key="invalid")
        ]

    raw_updated = heartbeat.get("updated_at")
    updated = parse_iso(raw_updated)
    if raw_updated and updated is None:
        issues.append(
            Issue(
                "gateway.heartbeat_invalid",
                f"网关心跳 updated_at 无法解析：{sanitize(raw_updated)}",
                dedup_key="timestamp",
            )
        )
    heartbeat_age = now - (updated if updated is not None else heartbeat_path.stat().st_mtime)
    if heartbeat_age > thresholds["heartbeat_max_age"]:
        issues.append(
            Issue(
                "gateway.heartbeat_stale",
                f"网关心跳已 {format_age(heartbeat_age)} 未更新"
                f"（阈值 {format_age(thresholds['heartbeat_max_age'])}）",
                dedup_key="stale",
            )
        )

    pid = heartbeat.get("pid")
    main_pid = props.get("MainPID")
    if pid not in (None, "") and main_pid not in (None, "", "0") and str(pid) != str(main_pid):
        issues.append(
            Issue(
                "gateway.pid_mismatch",
                f"网关心跳 PID={pid} 与 systemd MainPID={main_pid} 不一致（心跳可能来自旧进程）",
                dedup_key="pid",
            )
        )
    return issues


def _backup_key(name: str) -> str:
    for prefix in ("memory-", "meta-", "manifest-"):
        if not name.startswith(prefix):
            continue
        rest = name[len(prefix) :]
        for suffix in (".sqlite3", ".tar.gz", ".json"):
            if rest.endswith(suffix):
                return rest[: -len(suffix)]
    return ""


def _valid_generation(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and 1 <= value <= MAX_GENERATION


def _valid_user_version(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= MAX_GENERATION


def _read_backup_manifest(path: Path) -> tuple[dict | None, str]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        return None, f"manifest 无法解析：{sanitize(exc)}"
    if not isinstance(payload, dict):
        return None, "manifest 不是 JSON 对象"
    return payload, ""


def _read_db_user_version(path: Path, timeout: float) -> tuple[int | None, str]:
    try:
        connection = sqlite3.connect(
            f"file:{path}?mode=ro&immutable=1",
            uri=True,
            timeout=max(1.0, min(float(timeout), 10.0)),
        )
        try:
            return int(connection.execute("PRAGMA user_version").fetchone()[0]), ""
        finally:
            connection.close()
    except (sqlite3.Error, OSError, ValueError) as exc:
        return None, sanitize(exc)


def _meta_has_generation(path: Path) -> tuple[bool, str]:
    try:
        with tarfile.open(path, "r:gz") as archive:
            return BACKUP_GENERATION_MEMBER in archive.getnames(), ""
    except (tarfile.TarError, OSError, EOFError, zlib.error) as exc:
        return False, sanitize(exc)


def _read_meta_generation(path: Path) -> tuple[int | None, str]:
    try:
        with tarfile.open(path, "r:gz") as archive:
            member = archive.getmember(BACKUP_GENERATION_MEMBER)
            extracted = archive.extractfile(member)
            if extracted is None:
                return None, "generation member unreadable"
            payload = json.loads(extracted.read().decode("utf-8"))
    except (tarfile.TarError, OSError, EOFError, KeyError, UnicodeDecodeError, ValueError, zlib.error) as exc:
        return None, sanitize(exc)
    generation = payload.get("generation") if isinstance(payload, dict) else None
    if not _valid_generation(generation):
        return None, "invalid generation"
    return int(generation), ""


def _check_backup_triple(
    directory: Path,
    database: Path,
    kind: str,
    label: str,
    config: dict,
) -> list[Issue]:
    """Check the manifest, meta archive and DB metadata for one snapshot.

    The repaired backup writes ``memory-*``, ``meta-*`` and ``manifest-*`` as one
    generation.  The generation lives in the manifest and the meta tar; the DB
    ``user_version`` is the source schema version and is compared with the
    manifest's ``source_user_version``.  The watchdog only reads the small
    manifest and scalar metadata; it does not hash or ``quick_check`` a
    multi-gigabyte snapshot.
    """
    prefix = f"backup.{kind}"
    key = _backup_key(database.name)
    if not key:
        return [
            Issue(
                f"{prefix}_manifest_invalid",
                f"{label}备份文件名无法识别：{database.name}",
                dedup_key="name",
            )
        ]
    meta = directory / f"meta-{key}.tar.gz"
    manifest = directory / f"manifest-{key}.json"

    if not manifest.is_file():
        if not meta.is_file():
            return [
                Issue(
                    f"{prefix}_meta_missing",
                    f"{label}备份缺少 meta 包：{meta}",
                    dedup_key="meta",
                )
            ]
        if meta.stat().st_size == 0:
            return [
                Issue(
                    f"{prefix}_meta_empty",
                    f"{label}备份 meta 包为空：{meta}",
                    dedup_key="meta",
                )
            ]
        has_generation, meta_error = _meta_has_generation(meta)
        if meta_error:
            return [
                Issue(
                    f"{prefix}_meta_invalid",
                    f"{label}备份 meta 包无法读取：{meta_error}",
                    dedup_key="meta",
                )
            ]
        if has_generation:
            return [
                Issue(
                    f"{prefix}_manifest_missing",
                    f"最新{label}备份 {database.name} 缺少同代 manifest（meta 带代次），"
                    "备份可能未完成",
                    dedup_key="manifest",
                )
            ]
        return [
            Issue(
                f"{prefix}_legacy",
                f"最新{label}备份 {database.name} 是 legacy 格式（无 manifest/代次），"
                "只做年龄/大小检查；升级备份脚本后自动转为 manifest 校验",
                severity="notice",
                dedup_key="legacy",
            )
        ]

    if not meta.is_file():
        return [
            Issue(
                f"{prefix}_meta_missing",
                f"{label}备份缺少同代 meta 包：{meta}",
                dedup_key="meta",
            )
        ]

    payload, manifest_error = _read_backup_manifest(manifest)
    if payload is None:
        return [
            Issue(
                f"{prefix}_manifest_invalid",
                f"{label}备份 manifest 无效：{manifest_error}",
                dedup_key="manifest",
            )
        ]

    issues: list[Issue] = []
    schema_version = payload.get("schema_version")
    if schema_version != BACKUP_MANIFEST_SCHEMA:
        issues.append(
            Issue(
                f"{prefix}_manifest_invalid",
                f"{label}备份 manifest schema_version={schema_version!r}，"
                f"期望 {BACKUP_MANIFEST_SCHEMA}",
                dedup_key="schema",
            )
        )
    if payload.get("kind") != kind:
        issues.append(
            Issue(
                f"{prefix}_manifest_invalid",
                f"{label}备份 manifest kind={payload.get('kind')!r}，期望 {kind!r}",
                dedup_key="kind",
            )
        )
    if payload.get("snapshot") != database.name:
        issues.append(
            Issue(
                f"{prefix}_manifest_invalid",
                f"{label}备份 manifest snapshot={payload.get('snapshot')!r}，"
                f"期望 {database.name!r}",
                dedup_key="snapshot",
            )
        )

    generation = payload.get("generation")
    if not _valid_generation(generation):
        issues.append(
            Issue(
                f"{prefix}_manifest_invalid",
                f"{label}备份 manifest generation 无效：{generation!r}",
                dedup_key="generation",
            )
        )
        generation = None

    expected_user_version: int | None = None
    if schema_version == BACKUP_MANIFEST_SCHEMA:
        source_user_version = payload.get("source_user_version")
        if not _valid_user_version(source_user_version):
            issues.append(
                Issue(
                    f"{prefix}_manifest_invalid",
                    f"{label}备份 manifest source_user_version 无效："
                    f"{source_user_version!r}",
                    dedup_key="source-user-version",
                )
            )
        else:
            expected_user_version = int(source_user_version)

    files = payload.get("files")
    if not isinstance(files, dict):
        issues.append(
            Issue(
                f"{prefix}_manifest_invalid",
                f"{label}备份 manifest 缺少 files 映射",
                dedup_key="files",
            )
        )
        files = {}
    for expected in (database.name, meta.name):
        if expected not in files:
            issues.append(
                Issue(
                    f"{prefix}_manifest_invalid",
                    f"{label}备份 manifest 缺少 {expected}",
                    dedup_key=f"missing:{expected}",
                )
            )
    for name, info in files.items():
        pure = Path(name)
        if pure.is_absolute() or ".." in pure.parts or pure.name != name:
            issues.append(
                Issue(
                    f"{prefix}_manifest_invalid",
                    f"{label}备份 manifest 含不安全路径：{name}",
                    dedup_key="unsafe",
                )
            )
            continue
        if not isinstance(info, dict):
            issues.append(
                Issue(
                    f"{prefix}_manifest_invalid",
                    f"{label}备份 manifest 文件项无效：{name}",
                    dedup_key=f"entry:{name}",
                )
            )
            continue
        target = directory / name
        if not target.is_file():
            issues.append(
                Issue(
                    f"{prefix}_manifest_invalid",
                    f"{label}备份 manifest 指向不存在的文件：{target}",
                    dedup_key=f"missing:{name}",
                )
            )
            continue
        if info.get("size") != target.stat().st_size:
            issues.append(
                Issue(
                    f"{prefix}_manifest_mismatch",
                    f"{label}备份 {name} 大小与 manifest 不一致："
                    f"{target.stat().st_size} != {info.get('size')!r}",
                    dedup_key=f"size:{name}",
                )
            )
        digest = info.get("sha256")
        if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            issues.append(
                Issue(
                    f"{prefix}_manifest_invalid",
                    f"{label}备份 manifest 的 {name} 缺少有效 sha256",
                    dedup_key=f"sha256:{name}",
                )
            )

    meta_index = payload.get("meta")
    members = meta_index.get("members") if isinstance(meta_index, dict) else None
    required = meta_index.get("required") if isinstance(meta_index, dict) else None
    if not isinstance(members, list) or not isinstance(required, list):
        issues.append(
            Issue(
                f"{prefix}_manifest_invalid",
                f"{label}备份 manifest 缺少 meta members/required 列表",
                dedup_key="meta-index",
            )
        )
    else:
        missing_members = sorted(
            {str(name) for name in required} - {str(name) for name in members}
        )
        if missing_members:
            issues.append(
                Issue(
                    f"{prefix}_manifest_invalid",
                    f"{label}备份 manifest 缺少必需 meta 成员："
                    + ", ".join(missing_members),
                    dedup_key="meta-members",
                )
            )

    if generation is not None:
        db_user_version, db_error = _read_db_user_version(
            database, config["thresholds"]["command_timeout"]
        )
        if db_user_version is None:
            issues.append(
                Issue(
                    f"{prefix}_db_unreadable",
                    f"{label}备份数据库无法读取 user_version：{db_error}",
                    dedup_key="db",
                )
            )
        elif expected_user_version is not None and db_user_version != expected_user_version:
            issues.append(
                Issue(
                    f"{prefix}_user_version_mismatch",
                    f"{label}备份 user_version 不一致："
                    f"manifest={expected_user_version} db={db_user_version}"
                    f"（{database.name}）",
                    dedup_key="db-user-version",
                )
            )
        meta_generation, meta_error = _read_meta_generation(meta)
        if meta_generation is None:
            issues.append(
                Issue(
                    f"{prefix}_meta_invalid",
                    f"{label}备份 meta 包无法读取 generation：{meta_error}",
                    dedup_key="meta",
                )
            )
        elif meta_generation != generation:
            issues.append(
                Issue(
                    f"{prefix}_generation_mismatch",
                    f"{label}备份代次不一致：manifest={generation} "
                    f"meta={meta_generation}（{meta.name}）",
                    dedup_key="meta-generation",
                )
            )
    return issues


def check_backup(config: dict, systemd: Systemd, uptime: float, now: float) -> list[Issue]:
    del systemd, uptime
    thresholds = config["thresholds"]
    daily_dir = Path(config["paths"]["backup_daily"])
    weekly_dir = Path(config["paths"]["backup_weekly"])
    issues: list[Issue] = []

    daily = _newest(daily_dir, "memory-*.sqlite3")
    if daily is None:
        issues.append(
            Issue(
                "backup.daily_missing",
                f"每日备份不存在：{daily_dir}/memory-*.sqlite3",
                dedup_key="missing",
            )
        )
    else:
        daily_age = now - daily.stat().st_mtime
        if daily_age > thresholds["backup_max_age"]:
            issues.append(
                Issue(
                    "backup.daily_stale",
                    f"最新每日备份 {daily.name} 已 {format_age(daily_age)}"
                    f"（阈值 {format_age(thresholds['backup_max_age'])}）",
                    dedup_key="stale",
                )
            )
        if daily.stat().st_size < thresholds["backup_min_bytes"]:
            issues.append(
                Issue(
                    "backup.daily_small",
                    f"最新每日备份 {daily.name} 只有 {daily.stat().st_size} 字节，可能被截断",
                    dedup_key="small",
                )
            )
        issues.extend(_check_backup_triple(daily_dir, daily, "daily", "每日", config))

    now_local = datetime.fromtimestamp(now)
    monday = (now_local - timedelta(days=now_local.weekday())).replace(
        hour=3, minute=30, second=0, microsecond=0
    )
    expected_after = monday + timedelta(seconds=thresholds["weekly_next_run_grace"])
    week_key = now_local.strftime("%G-W%V")
    current_weekly = weekly_dir / f"memory-{week_key}.sqlite3"
    weekly = current_weekly if current_weekly.is_file() else None
    if weekly is not None:
        weekly_age = now - weekly.stat().st_mtime
        if weekly_age > thresholds["backup_weekly_max_age"]:
            issues.append(
                Issue(
                    "backup.weekly_stale",
                    f"最新每周备份 {weekly.name} 已 {format_age(weekly_age)}",
                    dedup_key="stale",
                )
            )
        if weekly.stat().st_size < thresholds["backup_min_bytes"]:
            issues.append(
                Issue(
                    "backup.weekly_small",
                    f"最新每周备份 {weekly.name} 只有 {weekly.stat().st_size} 字节，可能被截断",
                    dedup_key="small",
                )
            )
        issues.extend(_check_backup_triple(weekly_dir, weekly, "weekly", "每周", config))
    else:
        old_weekly = _newest(weekly_dir, "memory-*.sqlite3")
        oldest_daily = _oldest(daily_dir, "memory-*.sqlite3")
        backup_before_deadline = (
            oldest_daily is not None
            and datetime.fromtimestamp(oldest_daily.stat().st_mtime) <= expected_after
        )
        if now_local >= expected_after and (old_weekly is not None or backup_before_deadline):
            existing = f"（现有最新 {old_weekly.name}）" if old_weekly is not None else ""
            issues.append(
                Issue(
                    "backup.weekly_missing",
                    f"本周（{week_key}）没有每周备份{existing}，"
                    f"本周一 03:30 的备份窗口已过（{weekly_dir}）",
                    dedup_key=f"missing:{week_key}",
                )
            )

    issues.extend(
        _check_cron_job(
            config,
            now,
            job_id=str(config.get("backup_job_id") or ""),
            prefix="backup",
            label="共享库自动备份",
            missing_label="备份",
            missing_severity="error",
            max_age=float(thresholds["backup_cron_max_age"]),
            grace=float(thresholds["backup_cron_grace"]),
        )
    )
    return issues


def _load_cron_jobs(path: str | Path) -> tuple[list[dict] | None, str]:
    cron_path = Path(path)
    if not cron_path.exists():
        return None, f"cron 任务文件不存在：{cron_path}"
    try:
        payload = json.loads(cron_path.read_text(encoding="utf-8"))
    except OSError as exc:
        return None, f"cron 任务文件不可读：{sanitize(exc)}"
    except ValueError as exc:
        return None, f"cron 任务文件无法解析：{sanitize(exc)}"
    jobs = payload.get("jobs") if isinstance(payload, dict) else payload
    if not isinstance(jobs, list):
        return None, "cron 任务文件的 jobs 不是列表"
    return [job for job in jobs if isinstance(job, dict)], ""


def _check_cron_job(
    config: dict,
    now: float,
    *,
    job_id: str,
    prefix: str,
    label: str,
    missing_label: str,
    missing_severity: str,
    max_age: float,
    grace: float,
) -> list[Issue]:
    cron_path = Path(config["paths"]["cron_jobs"])
    jobs, cron_error = _load_cron_jobs(cron_path)
    if jobs is None:
        severity = "notice" if not cron_path.exists() else "error"
        return [Issue("cron.unreadable", cron_error, severity=severity, dedup_key="unreadable")]
    job = next((entry for entry in jobs if entry.get("id") == job_id), None)
    if job is None:
        return [
            Issue(
                f"{prefix}.cron_missing",
                f"cron 中找不到{missing_label}任务 {job_id}",
                severity=missing_severity,
                dedup_key="missing",
            )
        ]

    issues: list[Issue] = []
    if job.get("enabled") is False:
        issues.append(
            Issue(f"{prefix}.cron_disabled", f"{label} cron 任务已停用", dedup_key="disabled")
        )
    last_status = job.get("last_status")
    if last_status not in (None, "ok", "success"):
        issues.append(
            Issue(
                f"{prefix}.cron_failed",
                f"{label} cron 上次状态 {last_status}：{sanitize(job.get('last_error') or '')}",
                dedup_key=str(last_status),
            )
        )
    last_run = parse_iso(job.get("last_run_at"))
    if last_run is None:
        raw_last = job.get("last_run_at")
        issues.append(
            Issue(
                f"{prefix}.cron_timestamp_missing",
                f"{label} cron 缺少可解析的 last_run_at（{sanitize(raw_last)!r}），"
                "无法判断是否运行",
                severity="notice" if raw_last in (None, "") else "error",
                dedup_key="last-run-missing",
            )
        )
    elif now - last_run > max_age:
        issues.append(
            Issue(
                f"{prefix}.cron_stale",
                f"{label} cron 已 {format_age(now - last_run)} 未运行",
                dedup_key="stale",
            )
        )
    next_run = parse_iso(job.get("next_run_at"))
    if next_run is None:
        raw_next = job.get("next_run_at")
        issues.append(
            Issue(
                f"{prefix}.cron_timestamp_missing",
                f"{label} cron 缺少可解析的 next_run_at（{sanitize(raw_next)!r}），"
                "无法判断是否超过计划时间",
                severity="notice" if raw_next in (None, "") else "error",
                dedup_key="next-run-missing",
            )
        )
    elif now > next_run + grace:
        issues.append(
            Issue(
                f"{prefix}.cron_overdue",
                f"{label} cron 已超过计划时间 {format_age(now - next_run)} 仍未运行",
                dedup_key="overdue",
            )
        )
    return issues


def check_weekly(config: dict, systemd: Systemd, uptime: float, now: float) -> list[Issue]:
    del systemd, uptime
    thresholds = config["thresholds"]
    issues: list[Issue] = []
    reports_dir = Path(config["paths"]["weekly_reports"])
    reports = [
        path
        for path in reports_dir.glob("*.md")
        if path.is_file() and re.fullmatch(r"\d{4}-\d{2}-\d{2}\.md", path.name)
    ]
    if not reports:
        issues.append(
            Issue(
                "weekly.report_missing",
                f"没有记忆周检报告：{reports_dir}",
                dedup_key="missing",
            )
        )
    else:
        latest = max(reports, key=lambda path: path.stat().st_mtime)
        report_age = now - latest.stat().st_mtime
        if report_age > thresholds["weekly_report_max_age"]:
            issues.append(
                Issue(
                    "weekly.report_stale",
                    f"最新周检报告 {latest.name} 已 {format_age(report_age)}",
                    dedup_key="stale",
                )
            )

    issues.extend(
        _check_cron_job(
            config,
            now,
            job_id=str(config["weekly_job_id"]),
            prefix="weekly",
            label="记忆体检",
            missing_label="周检",
            missing_severity="notice",
            max_age=float(thresholds["weekly_report_max_age"]),
            grace=float(thresholds["weekly_next_run_grace"]),
        )
    )
    return issues


def check_entries(config: dict, systemd: Systemd, uptime: float, now: float) -> list[Issue]:
    del systemd, uptime
    thresholds = config["thresholds"]
    database = Path(config["paths"]["memory_db"])
    if not database.exists():
        return [
            Issue("entries.db_missing", f"共享记忆库不存在：{database}", dedup_key="missing")
        ]
    connection = None
    try:
        connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True, timeout=10)
        connection.execute("PRAGMA query_only=ON")
        max_rowid = connection.execute("SELECT MAX(rowid) FROM source_events").fetchone()[0]
        scan_rows = max(1, int(thresholds["entry_scan_rows"]))
        lower = max(0, int(max_rowid or 0) - scan_rows)
        rows = connection.execute(
            "SELECT recorded_at, origin FROM source_events "
            "WHERE rowid > ? ORDER BY rowid DESC",
            (lower,),
        ).fetchall()
    except sqlite3.Error as exc:
        return [
            Issue(
                "entries.db_error",
                f"共享记忆库读取失败：{sanitize(exc)}",
                dedup_key="error",
            )
        ]
    finally:
        if connection is not None:
            connection.close()

    if max_rowid is None:
        return [
            Issue(
                "entries.empty",
                "共享记忆库没有任何对话条目（source_events，排除笔记索引）",
                dedup_key="empty",
            )
        ]

    cutoff = now - 86400
    conversation: list[float] = []
    for recorded_at, origin in rows:
        if origin in ("external_document", "memory_reinjection"):
            continue
        parsed = parse_iso(recorded_at)
        if parsed is not None:
            conversation.append(parsed)
    if not conversation:
        if lower == 0:
            return [
                Issue(
                    "entries.empty",
                    "共享记忆库没有任何对话条目（source_events，排除笔记索引）",
                    dedup_key="empty",
                )
            ]
        return [
            Issue(
                "entries.scan_window",
                f"最近 {scan_rows} 行没有对话条目，可能已停写（未做全表扫描）",
                severity="notice",
                dedup_key="scan-window",
            )
        ]

    latest = max(conversation)
    count_24h = sum(1 for timestamp in conversation if timestamp >= cutoff)
    entry_age = now - latest
    entry_max_age = max(86400.0, float(thresholds["entry_max_age"]))
    if entry_age > entry_max_age:
        return [
            Issue(
                "entries.stalled",
                f"共享记忆库已 {format_age(entry_age)} 没有新对话条目"
                f"（最近 {len(rows)} 行中近 24 小时 {count_24h} 条，"
                f"阈值 {format_age(entry_max_age)}）",
                severity="notice",
                dedup_key="stalled",
            )
        ]
    return []


def _log_timestamp(line: str) -> datetime | None:
    match = LOG_TS_RE.match(line)
    if not match:
        return None
    try:
        return datetime.strptime(f"{match.group(1)} {match.group(2)}", "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def _cursor_offset(cursor: object, inode: int, size: int) -> int:
    if not isinstance(cursor, dict) or cursor.get("inode") != inode:
        return 0
    offset = cursor.get("offset")
    if not _valid_cursor_int(offset):
        return 0
    if offset > size:
        return 0
    return offset


def _read_plain_incremental(
    path: Path,
    cursor: object,
    max_bytes: int,
    max_lines: int,
) -> tuple[list[str], dict]:
    stat = path.stat()
    offset = _cursor_offset(cursor, stat.st_ino, stat.st_size)
    lines: list[str] = []
    consumed = 0
    with path.open("rb") as handle:
        handle.seek(offset)
        while len(lines) < max_lines:
            line = handle.readline()
            if not line:
                break
            if consumed and consumed + len(line) > max_bytes:
                break
            lines.append(line.decode("utf-8", "replace").rstrip("\n"))
            consumed += len(line)
            if consumed >= max_bytes:
                break
    return lines, {"inode": stat.st_ino, "offset": offset + consumed, "size": stat.st_size}


def _read_gzip_incremental(
    path: Path,
    cursor: object,
    max_bytes: int,
    max_lines: int,
) -> tuple[list[str], dict]:
    stat = path.stat()
    same_file = isinstance(cursor, dict) and cursor.get("inode") == stat.st_ino
    if same_file and cursor.get("size") == stat.st_size and cursor.get("eof") is True:
        return [], cursor
    offset = _cursor_offset(cursor, stat.st_ino, stat.st_size) if same_file else 0
    lines: list[str] = []
    consumed = 0
    eof = False
    with gzip.open(path, "rb") as handle:
        handle.seek(offset)
        while len(lines) < max_lines:
            line = handle.readline()
            if not line:
                eof = True
                break
            if consumed and consumed + len(line) > max_bytes:
                break
            lines.append(line.decode("utf-8", "replace").rstrip("\n"))
            consumed += len(line)
            if consumed >= max_bytes:
                break
    return lines, {"inode": stat.st_ino, "offset": offset + consumed, "size": stat.st_size, "eof": eof}


def _read_log_incremental(
    path: Path,
    cursor: object,
    max_bytes: int,
    max_lines: int,
) -> tuple[list[str], dict]:
    if path.suffix == ".gz":
        return _read_gzip_incremental(path, cursor, max_bytes, max_lines)
    return _read_plain_incremental(path, cursor, max_bytes, max_lines)


def check_rejections(
    config: dict,
    now: float,
    seen: dict[str, str],
    cursors: object = None,
    bootstrap: bool = False,
) -> tuple[list[Issue], dict[str, str], dict[str, dict]]:
    marker = str(config.get("rejection_marker") or REJECTION_MARKER)
    lookback = config["thresholds"]["rejection_lookback"]
    max_bytes = max(1, int(config["thresholds"]["rejection_scan_max_bytes"]))
    max_lines = max(1, int(config["thresholds"]["rejection_scan_max_lines"]))
    now_local = datetime.fromtimestamp(now)
    files = sorted(glob.glob(str(Path(config["paths"]["errors_log"])) + "*"))
    previous_cursors = cursors if isinstance(cursors, dict) else {}
    new_cursors: dict[str, dict] = {}
    total = 0
    new: dict[str, str] = {}
    issues: list[Issue] = []
    for name in files:
        path = Path(name)
        if not path.is_file():
            continue
        try:
            lines, cursor = _read_log_incremental(
                path,
                previous_cursors.get(str(path)),
                max_bytes,
                max_lines,
            )
        except (OSError, EOFError, ValueError, zlib.error) as exc:
            issues.append(
                Issue(
                    "rejections.log_unreadable",
                    f"日志不可读：{path}：{sanitize(exc)}",
                    severity="notice",
                    dedup_key=str(path),
                )
            )
            continue
        new_cursors[str(path)] = cursor
        for line in lines:
            if marker not in line:
                continue
            timestamp = _log_timestamp(line)
            if timestamp is not None and (now_local - timestamp).total_seconds() > lookback:
                continue
            tail = line.split(marker, 1)[1].lstrip(": ").strip()
            key = hashlib.sha256(tail.encode("utf-8", "replace")).hexdigest()[:20]
            total += 1
            if key not in seen:
                new.setdefault(key, timestamp.astimezone().isoformat() if timestamp else "")

    if bootstrap or not new:
        return issues, new, new_cursors
    ordered = sorted(new.items(), key=lambda item: item[1])
    digest = hashlib.sha256(",".join(sorted(new)).encode("ascii")).hexdigest()[:16]

    def display(value: str) -> str:
        parsed = parse_iso(value)
        return datetime.fromtimestamp(parsed).strftime("%m-%d %H:%M") if parsed is not None else ""

    issues.append(
        Issue(
            "rejections.new",
            f"本次增量扫描 errors.log 捕获拒绝 {total} 行，"
            f"按事件键去重后新增 {len(new)} 条（安全策略拦截，未写入共享库）",
            severity="notice",
            dedup_key=f"summary:{digest}",
        )
    )
    for key, timestamp in ordered[:8]:
        issues.append(
            Issue(
                "rejections.new",
                f"新增拒绝事件 {key[:10]}（{display(timestamp) or '时间未知'}）",
                severity="notice",
                dedup_key=key,
            )
        )
    if len(ordered) > 8:
        issues.append(
            Issue(
                "rejections.new",
                f"另有 {len(ordered) - 8} 条新增拒绝事件未列出",
                severity="notice",
                dedup_key=f"overflow:{digest}",
            )
        )
    return issues, new, new_cursors


def _safe_check(
    check: Callable[[dict, Systemd, float, float], list[Issue]],
    config: dict,
    systemd: Systemd,
    uptime: float,
    now: float,
) -> list[Issue]:
    try:
        return check(config, systemd, uptime, now)
    except Exception as exc:  # noqa: BLE001 - one broken probe must not hide the others
        return [
            Issue(
                f"watchdog.{check.__name__}",
                f"巡检项 {check.__name__} 执行失败：{sanitize(exc)}",
                dedup_key=type(exc).__name__,
            )
        ]


def dedupe_issues(issues: Iterable[Issue]) -> list[Issue]:
    seen: set[str] = set()
    unique: list[Issue] = []
    for issue in issues:
        if issue.signature in seen:
            continue
        seen.add(issue.signature)
        unique.append(issue)
    return unique


def run_checks(
    config: dict,
    systemd: Systemd,
    uptime: float,
    now: float,
    seen_rejections: dict[str, str],
    cursors: object = None,
    bootstrap_rejections: bool = False,
) -> tuple[list[Issue], dict[str, str], dict[str, dict]]:
    issues: list[Issue] = []
    for check in (check_note_indexer, check_wake, check_gateway, check_backup, check_weekly, check_entries):
        issues.extend(_safe_check(check, config, systemd, uptime, now))
    try:
        rejection_issues, new_rejections, new_cursors = check_rejections(
            config,
            now,
            seen_rejections,
            cursors,
            bootstrap=bootstrap_rejections,
        )
    except Exception as exc:  # noqa: BLE001 - a log scan must not stop the other checks
        rejection_issues = [
            Issue(
                "watchdog.check_rejections",
                f"巡检项 check_rejections 执行失败：{sanitize(exc)}",
                dedup_key=type(exc).__name__,
            )
        ]
        new_rejections = {}
        new_cursors = cursors if isinstance(cursors, dict) else {}
    issues.extend(rejection_issues)
    return dedupe_issues(issues), new_rejections, new_cursors


def unit_failure_issues(systemd: Systemd, unit: str) -> list[Issue]:
    name = unit if unit.endswith((".service", ".timer", ".socket")) else f"{unit}.service"
    try:
        props = systemd.show(name, ["ActiveState", "Result", "ExecMainStatus", "InvocationID"])
    except SystemdError as exc:
        return [
            Issue(
                "unit.failed",
                f"{name} 触发 OnFailure，但无法读取状态：{sanitize(exc)}",
                dedup_key=name,
            )
        ]
    result = props.get("Result", "")
    status = props.get("ExecMainStatus", "")
    detail = f"Result={result or '?'} exit={status or '?'}"
    if result in ("", "success") and status in ("", "0"):
        detail += "，单元可能已自动恢复"
    return [Issue("unit.failed", f"{name} 触发 OnFailure（{detail}）", dedup_key=name)]


def _valid_cursor_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def sanitize_cursors(value: object) -> tuple[dict[str, dict], list[str]]:
    """Keep only well-formed log cursors and describe entries that must be reset."""
    if value is None:
        return {}, []
    if not isinstance(value, dict):
        return {}, ["log_cursors 不是 JSON 对象"]
    valid: dict[str, dict] = {}
    problems: list[str] = []
    for key, cursor in value.items():
        if not isinstance(key, str) or not key:
            problems.append("游标路径无效")
            continue
        if not isinstance(cursor, dict):
            problems.append(f"{key}: 游标不是 JSON 对象")
            continue
        inode = cursor.get("inode")
        offset = cursor.get("offset")
        size = cursor.get("size")
        if not (_valid_cursor_int(inode) and _valid_cursor_int(offset) and _valid_cursor_int(size)):
            problems.append(f"{key}: inode/offset/size 无效")
            continue
        eof = cursor.get("eof")
        if "eof" in cursor and not isinstance(eof, bool):
            problems.append(f"{key}: eof 无效")
            continue
        entry = {"inode": inode, "offset": offset, "size": size}
        if isinstance(eof, bool):
            entry["eof"] = eof
        valid[key] = entry
    return valid, problems


def _cursors_complete(cursors: dict[str, dict]) -> bool:
    if not isinstance(cursors, dict):
        return False
    for cursor in cursors.values():
        if not isinstance(cursor, dict):
            return False
        if "eof" in cursor and not isinstance(cursor["eof"], bool):
            return False
        if cursor.get("eof") is False:
            return False
        offset = cursor.get("offset")
        size = cursor.get("size")
        if offset is None or size is None:
            continue
        if not _valid_cursor_int(offset) or not _valid_cursor_int(size):
            return False
        if offset < size:
            return False
    return True


def _prune_rejections(mapping: object, now: float, keep: float) -> dict[str, str]:
    if not isinstance(mapping, dict):
        return {}
    kept: dict[str, str] = {}
    for key, value in mapping.items():
        if not isinstance(key, str):
            continue
        timestamp = parse_iso(value) if isinstance(value, str) else None
        if timestamp is None or now - timestamp <= keep:
            kept[key] = value if isinstance(value, str) else ""
    if len(kept) > MAX_REJECTION_KEYS:
        newest = sorted(kept.items(), key=lambda item: parse_iso(item[1]) or 0, reverse=True)
        kept = dict(newest[:MAX_REJECTION_KEYS])
    return kept


def load_state(path: Path) -> tuple[dict, str]:
    if not path.exists():
        return {}, ""
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        return {}, sanitize(exc)
    if not isinstance(state, dict):
        return {}, "state is not a JSON object"
    return state, ""


def save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True))
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except OSError:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def append_log(path: Path, payload: dict) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists() and path.stat().st_size > MAX_LOG_BYTES:
            os.replace(path, path.with_name(path.name + ".1"))
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(path, 0o600)
    except OSError as exc:
        print(f"health-watchdog: cannot write log {path}: {sanitize(exc)}", file=sys.stderr)
        raise


def _persist_state(path: Path, state: dict) -> bool:
    try:
        save_state(path, state)
        return True
    except OSError as exc:
        print(f"health-watchdog: cannot write state {path}: {sanitize(exc)}", file=sys.stderr)
        return False


def _persist_log(path: Path, payload: dict) -> bool:
    try:
        append_log(path, payload)
        return True
    except OSError:
        return False


def _clear_pending(pending: PendingFailures | None, before: float) -> bool:
    if pending is None:
        return True
    try:
        pending.clear(before)
        return True
    except (OSError, UnicodeError, ValueError) as exc:
        print(f"health-watchdog: cannot clear pending failures: {sanitize(exc)}", file=sys.stderr)
        return False


def _truncate(text: str, limit: int = MAX_ALERT_CHARS) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 12].rstrip() + "\n…（已截断）"


def format_alert(issues: Sequence[Issue], now: float, max_issues: int = 20) -> tuple[str, str]:
    stamp = datetime.fromtimestamp(now).strftime("%m-%d %H:%M")
    errors = [issue for issue in issues if issue.severity == "error"]
    notices = [issue for issue in issues if issue.severity != "error"]
    subject = f"【Hermes 记忆巡检{'异常' if errors else '提示'}】{stamp}"
    lines: list[str] = []
    for issue in errors[:max_issues]:
        lines.append(f"- {issue.message}")
    if len(errors) > max_issues:
        lines.append(f"- 另有 {len(errors) - max_issues} 项异常未列出")
    if notices:
        lines.append("提示（非故障或需关注）：")
        for issue in notices[:max_issues]:
            lines.append(f"- {issue.message}")
        if len(notices) > max_issues:
            lines.append(f"- 另有 {len(notices) - max_issues} 条提示未列出")
    if not lines:
        lines.append("- 巡检发现异常，详见 health-watchdog.log。")
    return subject, _truncate("\n".join(lines))


def format_recovery(previous_codes: Sequence[str], now: float) -> tuple[str, str]:
    stamp = datetime.fromtimestamp(now).strftime("%m-%d %H:%M")
    codes = sorted({str(code) for code in previous_codes if code})
    detail = "、".join(codes[:10]) if codes else "此前的异常"
    return f"【Hermes 记忆巡检恢复】{stamp}", _truncate(f"此前报告的问题已消失（{detail}）。")


def decide(
    state: dict,
    issues: Sequence[Issue],
    now: float,
    dedup_seconds: float,
    max_issues: int = 20,
) -> Decision:
    new_state = copy.deepcopy(state)
    new_state["last_run_at"] = now
    new_state["last_issue_codes"] = sorted({issue.code for issue in issues})
    if not issues:
        previous = state.get("last_issue_codes")
        if not isinstance(previous, (list, tuple)):
            previous = []
        had_error = bool(state.get("active_error", state.get("active", False)))
        new_state["active"] = False
        new_state["active_error"] = False
        new_state["alert_sig"] = ""
        if had_error:
            subject, body = format_recovery(previous, now)
            return Decision("recovery", subject, body, new_state)
        return Decision("silent", state=new_state)

    signature = "|".join(sorted({issue.signature for issue in issues}))
    previous_signature = str(state.get("alert_sig") or "")
    try:
        last_alert = float(state.get("alert_at") or 0)
    except (TypeError, ValueError):
        last_alert = 0.0
    new_state["active"] = True
    new_state["active_error"] = bool(state.get("active_error")) or any(
        issue.severity == "error" for issue in issues
    )
    new_state["alert_sig"] = signature
    should_send = signature != previous_signature or now - last_alert >= dedup_seconds
    if not should_send:
        return Decision("silent", state=new_state)
    new_state["alert_at"] = now
    subject, body = format_alert(issues, now, max_issues)
    return Decision("alert", subject, body, new_state)


class StdoutChannel:
    name = "stdout"

    def send(self, subject: str, body: str) -> None:
        print(subject)
        print(body)
        sys.stdout.flush()


class HermesSendChannel:
    name = "hermes"

    def __init__(
        self,
        binary: str,
        target: str,
        timeout: float = 30,
        runner: Callable = subprocess.run,
    ):
        self.binary = binary
        self.target = target
        self.timeout = timeout
        self.runner = runner

    def send(self, subject: str, body: str) -> None:
        command = [
            self.binary,
            "send",
            "--to",
            self.target,
            "--subject",
            subject,
            "--file",
            "-",
        ]
        try:
            completed = self.runner(
                command,
                input=body,
                capture_output=True,
                text=True,
                timeout=self.timeout,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise ChannelError(f"hermes send failed: {sanitize(exc)}") from exc
        if completed.returncode != 0:
            raise ChannelError(
                f"hermes send exited {completed.returncode}: {sanitize(completed.stderr)}"
            )


def parse_env_file(path: str | Path) -> dict[str, str]:
    values: dict[str, str] = {}
    try:
        lines = Path(path).read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise ChannelError(f"cannot read SMTP env file: {sanitize(exc)}") from exc
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


class SmtpChannel:
    name = "smtp"

    def __init__(self, env_path: str | Path, timeout: float = 20):
        self.env_path = Path(env_path)
        self.timeout = timeout

    def send(self, subject: str, body: str) -> None:
        env = parse_env_file(self.env_path)
        missing = [key for key in ("SENDER", "TO", "SMTP_HOST", "SMTP_PORT", "AUTH_CODE") if not env.get(key)]
        if missing:
            raise ChannelError("SMTP 配置缺少 " + ",".join(missing))
        try:
            port = int(env["SMTP_PORT"])
        except ValueError as exc:
            raise ChannelError("SMTP_PORT 不是整数") from exc

        message = EmailMessage()
        message["From"] = env["SENDER"]
        message["To"] = env["TO"]
        message["Subject"] = subject
        message.set_content(body)

        server = None
        try:
            context = ssl.create_default_context()
            if port == 465:
                server = smtplib.SMTP_SSL(
                    env["SMTP_HOST"],
                    port,
                    timeout=self.timeout,
                    context=context,
                )
            else:
                server = smtplib.SMTP(env["SMTP_HOST"], port, timeout=self.timeout)
                server.starttls(context=context)
            login_user = parseaddr(env["SENDER"])[1] or env["SENDER"]
            server.login(login_user, env["AUTH_CODE"])
            server.send_message(message)
        except (smtplib.SMTPException, OSError, ValueError) as exc:
            raise ChannelError(f"SMTP 发送失败：{sanitize(exc)}") from exc
        finally:
            if server is not None:
                try:
                    server.quit()
                except (smtplib.SMTPException, OSError):
                    pass


class FanoutNotifier:
    def __init__(self, channels: Sequence):
        self.channels = list(channels)

    def send(self, subject: str, body: str) -> list[DeliveryResult]:
        results: list[DeliveryResult] = []
        for channel in self.channels:
            try:
                channel.send(subject, body)
            except Exception as exc:  # noqa: BLE001 - the other channel must still be tried
                results.append(DeliveryResult(getattr(channel, "name", "channel"), False, sanitize(exc)))
            else:
                results.append(DeliveryResult(getattr(channel, "name", "channel"), True))
        return results


def build_notifier(config: dict) -> FanoutNotifier:
    notify = config["notify"]
    names = notify.get("channels")
    if not isinstance(names, list):
        raise ConfigError("notify.channels must be a list")
    channels = []
    for name in names:
        if name == "stdout":
            channels.append(StdoutChannel())
        elif name == "hermes":
            channels.append(
                HermesSendChannel(
                    notify["hermes_bin"],
                    notify["hermes_target"],
                    timeout=float(config["thresholds"]["command_timeout"]),
                )
            )
        elif name == "smtp":
            channels.append(SmtpChannel(notify["smtp_env"], timeout=float(notify["smtp_timeout"])))
        else:
            raise ConfigError(f"unknown notification channel: {name}")
    if not channels:
        raise ConfigError("no notification channels configured")
    return FanoutNotifier(channels)


def _apply_cli_overrides(config: dict, args: argparse.Namespace) -> None:
    if args.state:
        config["paths"]["state"] = args.state
    if args.log:
        config["paths"]["log"] = args.log
    if args.channel:
        config["notify"]["channels"] = list(args.channel)


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hermes memory health watchdog")
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--state", help="override the state file path")
    parser.add_argument("--log", help="override the JSONL log path")
    parser.add_argument("--trigger", choices=("timer", "failure", "manual"), default="manual")
    parser.add_argument("--unit", action="append", default=[], help="failed unit for --trigger failure")
    parser.add_argument("--channel", action="append", help="notification channel (repeatable)")
    parser.add_argument("--dry-run", action="store_true", help="run checks, print the result, send nothing")
    parser.add_argument("--json", action="store_true", help="print a machine-readable result")
    return parser.parse_args(argv)


def _log_payload(
    trigger: str,
    units: Sequence[str],
    action: str,
    issues: Sequence[Issue],
    results: Sequence[DeliveryResult],
    new_rejections: int,
    now: float,
) -> dict:
    return {
        "ts": datetime.fromtimestamp(now).isoformat(timespec="seconds"),
        "trigger": trigger,
        "units": list(units),
        "action": action,
        "issues": [{"code": issue.code, "severity": issue.severity} for issue in issues],
        "new_rejections": new_rejections,
        "channels": [{"name": result.channel, "ok": result.ok, "error": result.error} for result in results],
        "delivered": any(result.ok for result in results),
    }


def _execute(
    args: argparse.Namespace,
    config: dict,
    systemd_client: Systemd,
    moment: float,
    uptime_seconds: float,
    notifier: FanoutNotifier | None,
    pending: PendingFailures | None = None,
) -> int:
    state_path = Path(config["paths"]["state"])
    log_path = Path(config["paths"]["log"])
    state, state_error = load_state(state_path)
    raw_cursors = state.get("log_cursors", {})
    cursors, cursor_problems = sanitize_cursors(raw_cursors)
    if "log_cursors" in state and raw_cursors is None:
        cursor_problems.append("log_cursors 不是 JSON 对象")
    pending_expired: list[dict] = []
    try:
        if pending is not None:
            pending_entries, pending_error = pending.load(moment, quarantine=not args.dry_run)
            pending_expired = list(getattr(pending, "expired", []) or [])
        else:
            pending_entries, pending_error = [], ""
    except Exception as exc:  # noqa: BLE001 - a broken queue must not hide the checks
        pending_entries, pending_error = [], sanitize(exc)
    seen_rejections = _prune_rejections(
        state.get("rejections"),
        moment,
        float(config["thresholds"]["rejection_state_keep"]),
    )
    bootstrap_pending = bool(state.get("rejection_bootstrap_pending"))
    bootstrap_rejections = bool(config.get("rejection_bootstrap", True)) and (
        bootstrap_pending or "rejections" not in state
    )

    issues, new_rejections, new_cursors = run_checks(
        config,
        systemd_client,
        uptime_seconds,
        moment,
        seen_rejections,
        cursors,
        bootstrap_rejections,
    )
    if state_error:
        issues.insert(
            0,
            Issue(
                "watchdog.state_corrupt",
                f"巡检状态文件无法读取，去重记录已重置：{state_error}",
                dedup_key="corrupt",
            ),
        )
    if cursor_problems:
        detail = "；".join(cursor_problems[:5])
        if len(cursor_problems) > 5:
            detail += f"；另有 {len(cursor_problems) - 5} 项"
        issues.insert(
            0,
            Issue(
                "watchdog.state_cursor_reset",
                f"巡检状态中的日志游标无效，已安全重置并重新扫描：{detail}",
                severity="notice",
                dedup_key=hashlib.sha256(
                    "\n".join(cursor_problems).encode("utf-8", "replace")
                ).hexdigest()[:16],
            ),
        )
    if pending_error:
        if getattr(pending, "quarantine_path", None) is not None:
            issues.append(
                Issue(
                    "watchdog.pending_quarantined",
                    pending_error,
                    severity="notice",
                    dedup_key="quarantined",
                )
            )
        else:
            issues.append(
                Issue(
                    "watchdog.pending_unreadable",
                    f"待处理 OnFailure 队列无法读取：{pending_error}",
                    severity="notice",
                    dedup_key="unreadable",
                )
            )
    if pending_expired:
        names = "、".join(
            f"{entry['unit']}（{format_age(max(0.0, moment - entry['at']))}前）"
            for entry in pending_expired[:5]
        )
        if len(pending_expired) > 5:
            names += f" 等 {len(pending_expired)} 条"
        issues.append(
            Issue(
                "watchdog.pending_expired",
                f"待处理 OnFailure 队列有 {len(pending_expired)} 条超过 "
                f"{format_age(getattr(pending, 'max_age', 0.0))} 未处理，已过期并将清理：{names}",
                severity="notice",
                dedup_key=hashlib.sha256(
                    "\n".join(f"{entry['unit']}:{entry['at']}" for entry in pending_expired).encode(
                        "utf-8", "replace"
                    )
                ).hexdigest()[:16],
            )
        )
    for entry in pending_entries:
        issues.extend(unit_failure_issues(systemd_client, entry["unit"]))
    for unit in args.unit:
        issues.extend(unit_failure_issues(systemd_client, unit))
    issues = dedupe_issues(issues)
    bootstrap_pending_next = bootstrap_rejections and (
        not _cursors_complete(new_cursors)
        or any(issue.code == "rejections.log_unreadable" for issue in issues)
    )

    decision = decide(
        state,
        issues,
        moment,
        float(config["thresholds"]["dedup_seconds"]),
        int(config["thresholds"]["max_alert_issues"]),
    )
    if args.dry_run:
        print(
            json.dumps(
                {
                    "action": decision.action,
                    "subject": decision.subject,
                    "body": decision.body,
                    "issues": [
                        {"code": issue.code, "severity": issue.severity, "message": issue.message}
                        for issue in issues
                    ],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    if decision.action == "silent":
        silent_state = decision.state or state
        silent_state["rejections"] = {**seen_rejections, **new_rejections}
        silent_state["log_cursors"] = new_cursors
        silent_state["rejection_bootstrap_pending"] = bootstrap_pending_next
        state_ok = _persist_state(state_path, silent_state)
        pending_ok = _clear_pending(pending, moment) if state_ok else False
        log_ok = _persist_log(
            log_path,
            _log_payload(args.trigger, args.unit, "silent", issues, [], len(new_rejections), moment),
        )
        if args.json:
            print(json.dumps({"action": "silent", "issues": [issue.code for issue in issues]}, ensure_ascii=False))
        return 0 if state_ok and pending_ok and log_ok else 1

    try:
        notifier_client = notifier or build_notifier(config)
    except ConfigError as exc:
        print(f"health-watchdog: {sanitize(exc)}", file=sys.stderr)
        return 2
    results = notifier_client.send(decision.subject, decision.body)
    delivered = any(result.ok for result in results)
    if delivered:
        final_state = decision.state or state
        final_state["rejections"] = {**seen_rejections, **new_rejections}
        final_state["log_cursors"] = new_cursors
        final_state["rejection_bootstrap_pending"] = bootstrap_pending_next
        state_ok = _persist_state(state_path, final_state)
        pending_ok = _clear_pending(pending, moment) if state_ok else False
    else:
        failed_state = copy.deepcopy(state)
        failed_state["last_run_at"] = moment
        failed_state["active"] = True
        failed_state["active_error"] = bool(state.get("active_error")) or any(
            issue.severity == "error" for issue in issues
        )
        failed_state["last_issue_codes"] = sorted({issue.code for issue in issues})
        state_ok = _persist_state(state_path, failed_state)
        pending_ok = True

    log_ok = _persist_log(
        log_path,
        _log_payload(args.trigger, args.unit, decision.action, issues, results, len(new_rejections), moment),
    )
    for result in results:
        if not result.ok:
            print(f"health-watchdog: channel {result.channel} failed: {result.error}", file=sys.stderr)
    if args.json:
        print(
            json.dumps(
                {
                    "action": decision.action,
                    "delivered": delivered,
                    "channels": [
                        {"name": result.channel, "ok": result.ok, "error": result.error}
                        for result in results
                    ],
                },
                ensure_ascii=False,
            )
        )
    if not delivered or not state_ok or not pending_ok or not log_ok:
        return 1
    return 0


def main(
    argv: Sequence[str] | None = None,
    *,
    notifier: FanoutNotifier | None = None,
    systemd: Systemd | None = None,
    now: float | None = None,
    uptime: float | None = None,
) -> int:
    args = _parse_args(argv)
    try:
        config = load_config(args.config)
        _apply_cli_overrides(config, args)
    except ConfigError as exc:
        print(f"health-watchdog: {sanitize(exc)}", file=sys.stderr)
        return 2

    moment = time.time() if now is None else float(now)
    pending = PendingFailures(
        Path(config["paths"]["pending"]),
        float(config["thresholds"]["pending_failure_max_age"]),
    )
    if args.trigger == "failure" and args.unit and not args.dry_run:
        for unit in args.unit:
            try:
                pending.append(unit, moment)
            except OSError as exc:
                print(
                    f"health-watchdog: cannot record pending failure {unit}: {sanitize(exc)}",
                    file=sys.stderr,
                )
    try:
        uptime_seconds = read_uptime() if uptime is None else float(uptime)
    except SystemdError as exc:
        print(f"health-watchdog: {sanitize(exc)}", file=sys.stderr)
        return 2
    systemd_client = systemd or Systemd(timeout=float(config["thresholds"]["command_timeout"]))

    lock_timeout = float(config["thresholds"]["lock_timeout"])
    if args.trigger == "failure":
        lock_timeout = max(lock_timeout, float(config["thresholds"]["failure_lock_timeout"]))
    try:
        with InstanceLock(Path(config["paths"]["lock"]), lock_timeout):
            return _execute(
                args,
                config,
                systemd_client,
                moment,
                uptime_seconds,
                notifier,
                pending,
            )
    except LockBusy as exc:
        follow_up = (
            "failure is queued for the next watchdog run"
            if args.trigger == "failure"
            else "next run will retry"
        )
        print(f"health-watchdog: {sanitize(exc)}; {follow_up}", file=sys.stderr)
        _persist_log(
            Path(config["paths"]["log"]),
            _log_payload(args.trigger, args.unit, "lock_busy", [], [], 0, moment),
        )
        return 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001 - the CLI must not dump a traceback into the journal
        print(f"health-watchdog: internal error: {sanitize(exc)}", file=sys.stderr)
        raise SystemExit(1) from None
