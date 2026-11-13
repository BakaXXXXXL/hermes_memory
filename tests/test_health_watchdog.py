"""Hermetic checks for the Hermes memory health watchdog.

The suite uses a fake systemd client, temporary state/backup/log trees and
mock notification channels.  Nothing here talks to the production host,
systemd, the gateway or a mail server.
"""

from __future__ import annotations

import contextlib
import copy
import gzip
import hashlib
import io
import json
import os
import shutil
import sqlite3
import ssl
import stat
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "deploy" / "scripts"))

import health_watchdog as hw  # noqa: E402


class FakeSystemd:
    def __init__(self, props: dict[str, dict[str, str]]):
        self.props = props

    def show(self, unit: str, properties):
        if unit not in self.props:
            raise hw.SystemdError(f"unit {unit} not found")
        return {name: self.props[unit].get(name, "") for name in properties}


class RecordingNotifier:
    def __init__(self):
        self.sent: list[tuple[str, str]] = []

    def send(self, subject: str, body: str):
        self.sent.append((subject, body))
        return [hw.DeliveryResult("recording", True)]


class FailingNotifier:
    def __init__(self, name: str = "failing"):
        self.name = name
        self.calls = 0

    def send(self, subject: str, body: str):
        self.calls += 1
        return [hw.DeliveryResult(self.name, False, "channel down")]


def _iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat().replace("+00:00", "Z")


def _write(path: Path, content: str = "", mtime: float | None = None) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    if mtime is not None:
        os.utime(path, (mtime, mtime))
    return path


def _write_bytes(path: Path, content: bytes, mtime: float | None = None) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    if mtime is not None:
        os.utime(path, (mtime, mtime))
    return path


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_backup_triple(
    directory: Path,
    key: str,
    kind: str,
    now: float,
    *,
    generation: int = 123456,
    source_user_version: int = 1110,
    db_user_version: int | None = None,
    meta_generation: int | None = None,
) -> tuple[Path, Path, Path]:
    """Create a repaired-format snapshot triple: DB, meta tar and manifest."""
    directory.mkdir(parents=True, exist_ok=True)
    database = directory / f"memory-{key}.sqlite3"
    connection = sqlite3.connect(database)
    try:
        connection.execute(
            f"PRAGMA user_version = {source_user_version if db_user_version is None else db_user_version}"
        )
        connection.execute("DROP TABLE IF EXISTS snapshot")
        connection.execute("CREATE TABLE snapshot (id INTEGER PRIMARY KEY, value TEXT)")
        connection.execute("INSERT INTO snapshot(value) VALUES ('x')")
        connection.commit()
    finally:
        connection.close()
    os.chmod(database, 0o600)

    meta = directory / f"meta-{key}.tar.gz"
    payload = json.dumps(
        {
            "generation": generation if meta_generation is None else meta_generation,
            "kind": kind,
            "snapshot": database.name,
        },
        sort_keys=True,
    ).encode("utf-8")
    members = [".backup-generation.json", "memories/MEMORY.md"]
    required = ["memories/MEMORY.md"]
    with tarfile.open(meta, "w:gz") as archive:
        info = tarfile.TarInfo(".backup-generation.json")
        info.size = len(payload)
        info.mode = 0o600
        archive.addfile(info, io.BytesIO(payload))
        note = b"memory\n"
        info = tarfile.TarInfo("memories/MEMORY.md")
        info.size = len(note)
        info.mode = 0o600
        archive.addfile(info, io.BytesIO(note))
    os.chmod(meta, 0o600)

    manifest = directory / f"manifest-{key}.json"
    _write(
        manifest,
        json.dumps(
            {
                "schema_version": 3,
                "kind": kind,
                "generation": generation,
                "source_user_version": source_user_version,
                "generated_at": _iso(now),
                "snapshot": database.name,
                "files": {
                    database.name: {
                        "sha256": _sha256(database),
                        "size": database.stat().st_size,
                    },
                    meta.name: {"sha256": _sha256(meta), "size": meta.stat().st_size},
                },
                "meta": {"members": members, "required": required},
            },
            indent=2,
            sort_keys=True,
        ),
        now,
    )
    os.utime(database, (now, now))
    os.utime(meta, (now, now))
    return database, meta, manifest


def write_legacy_backup(directory: Path, key: str, now: float) -> tuple[Path, Path]:
    """Create a pre-manifest snapshot: DB plus a meta tar without a generation member."""
    directory.mkdir(parents=True, exist_ok=True)
    database = directory / f"memory-{key}.sqlite3"
    connection = sqlite3.connect(database)
    try:
        connection.execute("CREATE TABLE snapshot (id INTEGER PRIMARY KEY, value TEXT)")
        connection.execute("INSERT INTO snapshot(value) VALUES ('legacy')")
        connection.commit()
    finally:
        connection.close()
    os.chmod(database, 0o600)

    meta = directory / f"meta-{key}.tar.gz"
    with tarfile.open(meta, "w:gz") as archive:
        note = b"legacy memory\n"
        info = tarfile.TarInfo("memories/MEMORY.md")
        info.size = len(note)
        info.mode = 0o600
        archive.addfile(info, io.BytesIO(note))
    os.chmod(meta, 0o600)
    os.utime(database, (now, now))
    os.utime(meta, (now, now))
    return database, meta


def make_env(root: Path):
    now = 1_800_000_000.0
    uptime = 10_000.0
    config = copy.deepcopy(hw.DEFAULTS)
    config["paths"] = {
        "note_state": str(root / "state" / "note-index.json"),
        "notes_dir": str(root / "notes"),
        "heartbeat": str(root / "state" / "gateway.heartbeat"),
        "backup_daily": str(root / "backups" / "daily"),
        "backup_weekly": str(root / "backups" / "weekly"),
        "weekly_reports": str(root / "reports"),
        "errors_log": str(root / "logs" / "errors.log"),
        "memory_db": str(root / "memory.sqlite3"),
        "cron_jobs": str(root / "cron" / "jobs.json"),
        "state": str(root / "watchdog-state.json"),
        "log": str(root / "health-watchdog.log"),
        "lock": str(root / "watchdog.lock"),
        "pending": str(root / "watchdog-pending.jsonl"),
    }
    config["thresholds"]["backup_min_bytes"] = 1
    config["notify"]["channels"] = ["stdout"]
    config["rejection_bootstrap"] = False
    config_path = _write(root / "config.json", json.dumps(config))

    _write(
        Path(config["paths"]["note_state"]),
        json.dumps({"notes": {"note-1": {"chunks": {"k": {"hash": "h"}}, "rejected": {}}}}),
        now,
    )
    _write(Path(config["paths"]["notes_dir"]) / "one.md", "# one\n", now)
    _write(
        Path(config["paths"]["heartbeat"]),
        json.dumps({"pid": 100, "updated_at": _iso(now)}),
        now,
    )
    write_backup_triple(Path(config["paths"]["backup_daily"]), "20261112", "daily", now)
    week_key = datetime.fromtimestamp(now).strftime("%G-W%V")
    write_backup_triple(Path(config["paths"]["backup_weekly"]), week_key, "weekly", now)
    _write(Path(config["paths"]["weekly_reports"]) / "2026-11-09.md", "report\n", now)
    _write(Path(config["paths"]["errors_log"]), "", now)
    _write(
        Path(config["paths"]["cron_jobs"]),
        json.dumps(
            {
                "jobs": [
                    {
                        "id": "549fe0406878",
                        "enabled": True,
                        "last_status": "ok",
                        "last_error": None,
                        "last_run_at": _iso(now - 86400),
                        "next_run_at": _iso(now + 86400),
                    },
                    {
                        "id": "4d313aab436e",
                        "enabled": True,
                        "last_status": "ok",
                        "last_error": None,
                        "last_run_at": _iso(now - 3600),
                        "next_run_at": _iso(now + 82800),
                    },
                ]
            }
        ),
        now,
    )
    database = sqlite3.connect(config["paths"]["memory_db"])
    try:
        database.execute("CREATE TABLE source_events (recorded_at TEXT, origin TEXT)")
        database.execute("INSERT INTO source_events VALUES (?, ?)", (_iso(now - 60), "human_direct"))
        database.commit()
    finally:
        database.close()

    units = {
        "note-indexer.service": {
            "ActiveState": "inactive",
            "SubState": "dead",
            "Result": "success",
            "ExecMainStatus": "0",
            "ExecMainExitTimestamp": f"@{int(now - 60)}",
        },
        "note-indexer.timer": {
            "ActiveState": "active",
            "LastTriggerUSec": f"@{int(now - 60)}",
        },
        "scope-recall-wake.service": {
            "ActiveState": "inactive",
            "SubState": "dead",
            "Result": "success",
            "ExecMainStatus": "0",
            "ExecMainExitTimestamp": f"@{int(now - 60)}",
        },
        "scope-recall-wake.timer": {
            "ActiveState": "active",
            "LastTriggerUSec": f"@{int(now - 60)}",
        },
        "hermes-gateway.service": {
            "ActiveState": "active",
            "SubState": "running",
            "MainPID": "100",
        },
    }
    return types.SimpleNamespace(
        root=root,
        config_path=config_path,
        config=config,
        now=now,
        uptime=uptime,
        systemd=FakeSystemd(units),
    )


def run_watchdog(env, notifier, *, now=None, extra_args=()):
    stdout = io.StringIO()
    stderr = io.StringIO()
    args = [
        "--config",
        str(env.config_path),
        "--state",
        env.config["paths"]["state"],
        "--log",
        env.config["paths"]["log"],
        *extra_args,
    ]
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        code = hw.main(
            args,
            notifier=notifier,
            systemd=env.systemd,
            now=env.now if now is None else now,
            uptime=env.uptime,
        )
    return code, stdout.getvalue(), stderr.getvalue()


def read_state(env) -> dict:
    return json.loads(Path(env.config["paths"]["state"]).read_text(encoding="utf-8"))


def add_rejection(env, tail: str, *, age: float = 0, name: str = "errors.log") -> None:
    stamp = datetime.fromtimestamp(env.now - age).strftime("%Y-%m-%d %H:%M:%S")
    line = (
        f"{stamp},123 WARNING scope_recall.adapters.hermes.provider: "
        f"scope-recall: not stored (plaintext_secret_rejected): {tail}\n"
    )
    path = Path(env.config["paths"]["errors_log"]).with_name(name)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(line)
    os.utime(path, (env.now, env.now))


class WatchdogTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.env = make_env(self.root)

    def tearDown(self):
        self.temporary.cleanup()

    def test_a_healthy_run_is_silent(self):
        notifier = RecordingNotifier()
        code, stdout, stderr = run_watchdog(self.env, notifier)

        self.assertEqual(code, 0)
        self.assertEqual(notifier.sent, [])
        self.assertEqual(stdout, "")
        self.assertEqual(stderr, "")
        state = read_state(self.env)
        self.assertFalse(state["active"])
        self.assertEqual(stat.S_IMODE(os.stat(self.env.config["paths"]["state"]).st_mode), 0o600)
        self.assertEqual(list(self.root.glob(".watchdog-state.json.*.tmp")), [])

    def test_note_indexer_failure_alerts_then_deduplicates_for_thirty_minutes(self):
        self.env.systemd.props["note-indexer.service"].update(
            {"ActiveState": "failed", "Result": "failed", "ExecMainStatus": "1"}
        )
        notifier = RecordingNotifier()

        code, _, _ = run_watchdog(self.env, notifier)
        self.assertEqual(code, 0)
        self.assertEqual(len(notifier.sent), 1)
        self.assertIn("note-indexer", notifier.sent[0][1])

        code, _, _ = run_watchdog(self.env, notifier, now=self.env.now + 60)
        self.assertEqual(code, 0)
        self.assertEqual(len(notifier.sent), 1, "the same signature must be suppressed inside 30 minutes")

        code, _, _ = run_watchdog(self.env, notifier, now=self.env.now + 1801)
        self.assertEqual(code, 0)
        self.assertEqual(len(notifier.sent), 2, "a persistent issue is repeated after the cooldown")

    def test_recovery_is_announced_once(self):
        self.env.systemd.props["note-indexer.service"].update(
            {"ActiveState": "failed", "Result": "failed", "ExecMainStatus": "1"}
        )
        notifier = RecordingNotifier()
        run_watchdog(self.env, notifier)
        self.env.systemd.props["note-indexer.service"].update(
            {"ActiveState": "inactive", "Result": "success", "ExecMainStatus": "0"}
        )

        code, _, _ = run_watchdog(self.env, notifier, now=self.env.now + 60)
        self.assertEqual(code, 0)
        self.assertEqual(len(notifier.sent), 2)
        self.assertIn("恢复", notifier.sent[1][0])

        code, _, _ = run_watchdog(self.env, notifier, now=self.env.now + 120)
        self.assertEqual(code, 0)
        self.assertEqual(len(notifier.sent), 2, "recovery is sent once")

    def test_stale_or_corrupt_note_index_state_is_reported(self):
        state_path = Path(self.env.config["paths"]["note_state"])
        os.utime(state_path, (self.env.now - 1000, self.env.now - 1000))
        notifier = RecordingNotifier()
        run_watchdog(self.env, notifier)
        self.assertIn("索引状态", notifier.sent[0][1])

        _write(state_path, "{not json", self.env.now)
        notifier = RecordingNotifier()
        run_watchdog(self.env, notifier, now=self.env.now + 1801)
        self.assertIn("无法解析", notifier.sent[0][1])

    def test_timer_staleness_uses_unix_timestamps_not_formatted_monotonic(self):
        self.env.systemd.props["note-indexer.timer"].update(
            {
                "LastTriggerUSec": f"@{int(self.env.now - 2000)}",
                "LastTriggerUSecMonotonic": "6d 12h 4min 52.394960s",
            }
        )
        notifier = RecordingNotifier()
        run_watchdog(self.env, notifier)
        self.assertIn("note-indexer.timer 已", notifier.sent[0][1])

    def test_systemd_259_formatted_monotonic_is_ignored(self):
        self.env.systemd.props["note-indexer.timer"].update(
            {
                "LastTriggerUSec": f"@{int(self.env.now - 60)}",
                "LastTriggerUSecMonotonic": "6d 12h 4min 52.394960s",
            }
        )
        notifier = RecordingNotifier()
        code, stdout, stderr = run_watchdog(self.env, notifier)
        self.assertEqual(code, 0)
        self.assertEqual(notifier.sent, [])
        self.assertEqual(stdout, "")
        self.assertEqual(stderr, "")

    def test_systemd_show_uses_unix_timestamp_and_parses_259_fixture(self):
        fixture = (ROOT / "tests" / "fixtures" / "systemd259_timer_show.txt").read_text(encoding="utf-8")
        calls = []

        def runner(command, **kwargs):
            calls.append((command, kwargs))
            return types.SimpleNamespace(returncode=0, stdout=fixture, stderr="")

        systemd = hw.Systemd(runner=runner)
        props = systemd.show("note-indexer.timer", ["ActiveState", "LastTriggerUSec", "LastTriggerUSecMonotonic"])
        self.assertIn("--timestamp=unix", calls[0][0])
        self.assertEqual(props["LastTriggerUSec"], "@1794491161")
        self.assertEqual(props["LastTriggerUSecMonotonic"], "6d 12h 4min 52.394960s")

    def test_instance_lock_rejects_a_concurrent_run(self):
        lock_path = self.root / "instance.lock"
        first = hw.InstanceLock(lock_path, timeout=0)
        first.__enter__()
        try:
            second = hw.InstanceLock(lock_path, timeout=0.1)
            with self.assertRaises(hw.LockBusy):
                second.__enter__()
        finally:
            first.__exit__(None, None, None)

    def test_main_refuses_when_the_instance_lock_is_held(self):
        config = json.loads(self.env.config_path.read_text(encoding="utf-8"))
        config["thresholds"]["lock_timeout"] = 0.1
        _write(self.env.config_path, json.dumps(config))
        lock_path = Path(self.env.config["paths"]["lock"])
        first = hw.InstanceLock(lock_path, timeout=0)
        first.__enter__()
        try:
            notifier = RecordingNotifier()
            code, _, stderr = run_watchdog(self.env, notifier)
        finally:
            first.__exit__(None, None, None)
        self.assertEqual(code, 1)
        self.assertIn("another watchdog instance", stderr)
        self.assertEqual(notifier.sent, [])

    def test_instance_lock_retries_until_the_holder_releases(self):
        lock_path = self.root / "retry.lock"
        first = hw.InstanceLock(lock_path, timeout=0)
        first.__enter__()

        def release():
            time.sleep(0.25)
            first.__exit__(None, None, None)

        thread = threading.Thread(target=release)
        thread.start()
        try:
            second = hw.InstanceLock(lock_path, timeout=2)
            with second:
                pass
        finally:
            thread.join()

    def test_failure_trigger_queues_the_unit_when_the_lock_is_held(self):
        config = json.loads(self.env.config_path.read_text(encoding="utf-8"))
        config["thresholds"]["lock_timeout"] = 0.1
        config["thresholds"]["failure_lock_timeout"] = 0.1
        _write(self.env.config_path, json.dumps(config))
        lock_path = Path(self.env.config["paths"]["lock"])
        first = hw.InstanceLock(lock_path, timeout=0)
        first.__enter__()
        try:
            notifier = RecordingNotifier()
            code, _, stderr = run_watchdog(
                self.env,
                notifier,
                extra_args=("--trigger", "failure", "--unit", "note-indexer"),
            )
        finally:
            first.__exit__(None, None, None)

        self.assertEqual(code, 1)
        self.assertIn("another watchdog instance", stderr)
        self.assertIn("queued", stderr)
        pending_path = Path(self.env.config["paths"]["pending"])
        self.assertIn("note-indexer", pending_path.read_text(encoding="utf-8"))
        log_lines = Path(self.env.config["paths"]["log"]).read_text(encoding="utf-8").splitlines()
        self.assertEqual(json.loads(log_lines[-1])["action"], "lock_busy")

        notifier = RecordingNotifier()
        code, _, _ = run_watchdog(self.env, notifier, now=self.env.now + 60)
        self.assertEqual(code, 0)
        self.assertEqual(len(notifier.sent), 1)
        self.assertIn("触发 OnFailure", notifier.sent[0][1])
        self.assertEqual(hw.PendingFailures(pending_path).load(self.env.now + 60)[0], [])

    def test_pending_failure_survives_delivery_failure(self):
        config = json.loads(self.env.config_path.read_text(encoding="utf-8"))
        config["thresholds"]["lock_timeout"] = 0.1
        config["thresholds"]["failure_lock_timeout"] = 0.1
        _write(self.env.config_path, json.dumps(config))
        pending_path = Path(self.env.config["paths"]["pending"])
        pending_path.write_text(
            json.dumps({"unit": "note-indexer.service", "at": self.env.now}) + "\n",
            encoding="utf-8",
        )

        failing = FailingNotifier()
        code, _, _ = run_watchdog(self.env, failing, now=self.env.now + 60)
        self.assertEqual(code, 1)
        self.assertIn("note-indexer", pending_path.read_text(encoding="utf-8"))

        retry = RecordingNotifier()
        code, _, _ = run_watchdog(self.env, retry, now=self.env.now + 120)
        self.assertEqual(code, 0)
        self.assertEqual(len(retry.sent), 1)
        self.assertIn("触发 OnFailure", retry.sent[0][1])
        self.assertEqual(hw.PendingFailures(pending_path).load(self.env.now + 120)[0], [])

    def test_invalid_utf8_pending_is_quarantined_and_keeps_valid_entries(self):
        pending_path = Path(self.env.config["paths"]["pending"])
        valid = json.dumps({"unit": "note-indexer.service", "at": self.env.now}) + "\n"
        pending_path.write_bytes(valid.encode("utf-8") + b"\xff\xfe broken\n")

        notifier = RecordingNotifier()
        code, _, _ = run_watchdog(self.env, notifier)
        self.assertEqual(code, 0)
        self.assertEqual(len(notifier.sent), 1)
        body = notifier.sent[0][1]
        self.assertIn("非法 UTF-8", body)
        self.assertIn("note-indexer", body)

        quarantined = list(pending_path.parent.glob(pending_path.name + ".corrupt-*"))
        self.assertEqual(len(quarantined), 1)
        self.assertIn(b"\xff\xfe", quarantined[0].read_bytes())
        self.assertEqual(pending_path.read_bytes(), b"")

    def test_corrupt_json_pending_is_quarantined_and_reported(self):
        pending_path = Path(self.env.config["paths"]["pending"])
        valid = json.dumps({"unit": "note-indexer.service", "at": self.env.now}) + "\n"
        broken = '{"unit": "scope-recall-wake.service", "at": }\n'
        pending_path.write_text(valid + broken, encoding="utf-8")

        notifier = RecordingNotifier()
        code, _, _ = run_watchdog(self.env, notifier)
        self.assertEqual(code, 0)
        self.assertEqual(len(notifier.sent), 1)
        body = notifier.sent[0][1]
        self.assertIn("损坏的 JSON", body)
        self.assertIn("note-indexer", body)

        quarantined = list(pending_path.parent.glob(pending_path.name + ".corrupt-*"))
        self.assertEqual(len(quarantined), 1)
        self.assertIn(broken.encode("utf-8"), quarantined[0].read_bytes())
        self.assertEqual(pending_path.read_text(encoding="utf-8"), "")

    def test_pending_append_separates_an_entry_without_a_trailing_newline(self):
        pending_path = Path(self.env.config["paths"]["pending"])
        first = {"unit": "note-indexer.service", "at": self.env.now}
        pending_path.write_text(json.dumps(first), encoding="utf-8")
        queue = hw.PendingFailures(pending_path, max_age=86400)

        queue.append("scope-recall-wake.service", self.env.now + 1)

        entries, error = queue.load(self.env.now + 2)
        self.assertEqual(error, "")
        self.assertEqual(
            [entry["unit"] for entry in entries],
            ["note-indexer.service", "scope-recall-wake.service"],
        )
        self.assertEqual(len(pending_path.read_text(encoding="utf-8").splitlines()), 2)

    def test_pending_clear_quarantines_corrupt_json_instead_of_dropping_it(self):
        pending_path = Path(self.env.config["paths"]["pending"])
        broken = '{"unit": "note-indexer.service", "at": }\n'
        pending_path.write_text(broken, encoding="utf-8")
        queue = hw.PendingFailures(pending_path, max_age=86400)

        queue.clear(self.env.now)

        quarantined = list(pending_path.parent.glob(pending_path.name + ".corrupt-*"))
        self.assertEqual(len(quarantined), 1)
        self.assertIn(broken.encode("utf-8"), quarantined[0].read_bytes())
        self.assertEqual(pending_path.read_text(encoding="utf-8"), "")

    def test_dry_run_reports_invalid_utf8_pending_without_quarantining(self):
        pending_path = Path(self.env.config["paths"]["pending"])
        pending_path.write_bytes(b"\xff\xfe broken\n")

        notifier = RecordingNotifier()
        code, stdout, _ = run_watchdog(self.env, notifier, extra_args=("--dry-run",))

        self.assertEqual(code, 0)
        self.assertEqual(notifier.sent, [])
        self.assertEqual(pending_path.read_bytes(), b"\xff\xfe broken\n")
        self.assertEqual(list(pending_path.parent.glob(pending_path.name + ".corrupt-*")), [])
        self.assertIn("非法 UTF-8", json.loads(stdout)["body"])

    def test_dry_run_reports_corrupt_json_pending_without_quarantining(self):
        pending_path = Path(self.env.config["paths"]["pending"])
        broken = '{"unit": "note-indexer.service", "at": }\n'
        pending_path.write_text(broken, encoding="utf-8")

        notifier = RecordingNotifier()
        code, stdout, _ = run_watchdog(self.env, notifier, extra_args=("--dry-run",))

        self.assertEqual(code, 0)
        self.assertEqual(notifier.sent, [])
        self.assertEqual(pending_path.read_text(encoding="utf-8"), broken)
        self.assertEqual(list(pending_path.parent.glob(pending_path.name + ".corrupt-*")), [])
        self.assertIn("损坏的 JSON", json.loads(stdout)["body"])

    def test_expired_pending_failure_is_reported_before_cleanup(self):
        pending_path = Path(self.env.config["paths"]["pending"])
        pending_path.write_text(
            json.dumps({"unit": "note-indexer.service", "at": self.env.now - 90000}) + "\n",
            encoding="utf-8",
        )

        notifier = RecordingNotifier()
        code, _, _ = run_watchdog(self.env, notifier)
        self.assertEqual(code, 0)
        self.assertEqual(len(notifier.sent), 1)
        body = notifier.sent[0][1]
        self.assertIn("过期", body)
        self.assertIn("note-indexer", body)
        self.assertEqual(pending_path.read_text(encoding="utf-8"), "")

    def test_partial_delivery_failure_is_visible(self):
        self.env.systemd.props["note-indexer.service"].update(
            {"ActiveState": "failed", "Result": "failed", "ExecMainStatus": "1"}
        )

        class Channel:
            def __init__(self, name, fail):
                self.name = name
                self.fail = fail

            def send(self, subject, body):
                if self.fail:
                    raise hw.ChannelError("bad channel")

        notifier = hw.FanoutNotifier([Channel("good", False), Channel("bad", True)])
        code, _, stderr = run_watchdog(self.env, notifier)
        self.assertEqual(code, 0)
        self.assertIn("channel bad failed", stderr)

    def test_state_write_failure_is_visible(self):
        blocked = self.root / "blocked"
        _write(blocked, "not a directory")
        self.env.config["paths"]["state"] = str(blocked / "state.json")
        notifier = RecordingNotifier()
        code, _, stderr = run_watchdog(self.env, notifier)
        self.assertEqual(code, 1)
        self.assertIn("cannot write state", stderr)

    def test_log_write_failure_is_visible(self):
        blocked = self.root / "blocked-log"
        _write(blocked, "not a directory")
        self.env.config["paths"]["log"] = str(blocked / "watchdog.log")
        notifier = RecordingNotifier()
        code, _, stderr = run_watchdog(self.env, notifier)
        self.assertEqual(code, 1)
        self.assertIn("cannot write log", stderr)

    def test_gateway_heartbeat_and_pid_are_checked(self):
        heartbeat = Path(self.env.config["paths"]["heartbeat"])
        _write(heartbeat, json.dumps({"pid": 999, "updated_at": _iso(self.env.now - 600)}), self.env.now)
        notifier = RecordingNotifier()
        run_watchdog(self.env, notifier)
        body = notifier.sent[0][1]
        self.assertIn("心跳", body)
        self.assertIn("PID=999", body)

    def test_invalid_heartbeat_timestamp_is_reported(self):
        heartbeat = Path(self.env.config["paths"]["heartbeat"])
        _write(heartbeat, json.dumps({"pid": 100, "updated_at": "not-a-time"}), self.env.now)
        notifier = RecordingNotifier()
        run_watchdog(self.env, notifier)
        self.assertIn("updated_at 无法解析", notifier.sent[0][1])

    def test_corrupt_watchdog_state_alerts_and_is_replaced(self):
        state_path = Path(self.env.config["paths"]["state"])
        _write(state_path, "{broken", self.env.now)
        notifier = RecordingNotifier()
        run_watchdog(self.env, notifier)
        self.assertIn("状态文件无法读取", notifier.sent[0][1])
        self.assertIn("active", read_state(self.env))

    def test_malformed_log_cursor_is_reset_and_alerted(self):
        state_path = Path(self.env.config["paths"]["state"])
        _write(
            state_path,
            json.dumps(
                {
                    "rejections": {},
                    "log_cursors": {
                        "errors.log": {"inode": "bad", "offset": [], "size": -1}
                    },
                }
            ),
            self.env.now,
        )
        notifier = RecordingNotifier()
        code, _, _ = run_watchdog(self.env, notifier)
        self.assertEqual(code, 0)
        self.assertEqual(len(notifier.sent), 1)
        self.assertIn("日志游标", notifier.sent[0][1])
        state = read_state(self.env)
        self.assertNotIn("errors.log", state["log_cursors"])
        for cursor in state["log_cursors"].values():
            self.assertIsInstance(cursor, dict)

    def test_non_object_log_cursors_are_reset_without_crashing(self):
        state_path = Path(self.env.config["paths"]["state"])
        _write(state_path, json.dumps({"rejections": {}, "log_cursors": ["bad"]}), self.env.now)
        notifier = RecordingNotifier()
        code, _, _ = run_watchdog(self.env, notifier)
        self.assertEqual(code, 0)
        self.assertEqual(len(notifier.sent), 1)
        self.assertIn("log_cursors 不是 JSON 对象", notifier.sent[0][1])
        state = read_state(self.env)
        self.assertIsInstance(state["log_cursors"], dict)
        for cursor in state["log_cursors"].values():
            self.assertIsInstance(cursor, dict)

    def test_non_list_last_issue_codes_does_not_crash_recovery(self):
        state_path = Path(self.env.config["paths"]["state"])
        _write(
            state_path,
            json.dumps(
                {
                    "active_error": True,
                    "last_issue_codes": 123,
                    "rejections": {},
                    "log_cursors": {},
                }
            ),
            self.env.now,
        )
        notifier = RecordingNotifier()
        code, _, _ = run_watchdog(self.env, notifier)
        self.assertEqual(code, 0)
        self.assertEqual(len(notifier.sent), 1)
        self.assertIn("恢复", notifier.sent[0][0])

    def test_inactive_gateway_reports_once_without_heartbeat_noise(self):
        self.env.systemd.props["hermes-gateway.service"].update(
            {"ActiveState": "inactive", "SubState": "dead", "MainPID": "0"}
        )
        notifier = RecordingNotifier()
        run_watchdog(self.env, notifier)
        self.assertEqual(len(notifier.sent), 1)
        self.assertIn("hermes-gateway", notifier.sent[0][1])
        self.assertNotIn("心跳文件缺失", notifier.sent[0][1])

    def test_backup_age_and_missing_weekly_backup_are_reported(self):
        daily = Path(self.env.config["paths"]["backup_daily"]) / "memory-20261112.sqlite3"
        os.utime(daily, (self.env.now - 1000000, self.env.now - 1000000))
        for path in Path(self.env.config["paths"]["backup_weekly"]).glob("*"):
            path.unlink()
        notifier = RecordingNotifier()
        run_watchdog(self.env, notifier)
        body = notifier.sent[0][1]
        self.assertIn("每日备份", body)
        self.assertIn("每周备份", body)

    def test_missing_weekly_backup_is_not_reported_before_the_first_monday_slot(self):
        for path in Path(self.env.config["paths"]["backup_weekly"]).glob("*"):
            path.unlink()
        notifier = RecordingNotifier()
        code, stdout, stderr = run_watchdog(self.env, notifier)
        self.assertEqual(code, 0)
        self.assertEqual(notifier.sent, [])
        self.assertEqual(stdout, "")
        self.assertEqual(stderr, "")

    def test_old_weekly_backup_without_current_week_is_reported(self):
        weekly_dir = Path(self.env.config["paths"]["backup_weekly"])
        for path in weekly_dir.iterdir():
            path.unlink()
        old_now = self.env.now - 7 * 86400
        old_key = datetime.fromtimestamp(old_now).strftime("%G-W%V")
        write_backup_triple(weekly_dir, old_key, "weekly", old_now)

        notifier = RecordingNotifier()
        code, _, _ = run_watchdog(self.env, notifier)
        self.assertEqual(code, 0)
        self.assertEqual(len(notifier.sent), 1)
        body = notifier.sent[0][1]
        self.assertIn("本周", body)
        self.assertIn("没有每周备份", body)
        self.assertIn(old_key, body)

    def test_missing_weekly_backup_is_reported_on_monday_after_the_grace_period(self):
        local_now = datetime.fromtimestamp(self.env.now)
        days = (7 - local_now.weekday()) % 7
        if days == 0:
            days = 7
        monday = (local_now + timedelta(days=days)).replace(
            hour=6, minute=0, second=0, microsecond=0
        )
        slot = monday.replace(hour=3, minute=30)
        weekly_dir = Path(self.env.config["paths"]["backup_weekly"])
        for path in weekly_dir.iterdir():
            path.unlink()
        daily = Path(self.env.config["paths"]["backup_daily"]) / "memory-20261112.sqlite3"
        os.utime(daily, (slot.timestamp(), slot.timestamp()))

        notifier = RecordingNotifier()
        code, _, _ = run_watchdog(self.env, notifier, now=monday.timestamp())
        self.assertEqual(code, 0)
        self.assertEqual(len(notifier.sent), 1)
        body = notifier.sent[0][1]
        self.assertIn("本周", body)
        self.assertIn("没有每周备份", body)

    def test_small_weekly_backup_is_reported(self):
        config = json.loads(self.env.config_path.read_text(encoding="utf-8"))
        config["thresholds"]["backup_min_bytes"] = 10_000_000
        _write(self.env.config_path, json.dumps(config))

        notifier = RecordingNotifier()
        run_watchdog(self.env, notifier)
        body = notifier.sent[0][1]
        self.assertIn("最新每周备份", body)
        self.assertIn("可能被截断", body)

    def test_missing_daily_backup_is_reported(self):
        for path in Path(self.env.config["paths"]["backup_daily"]).glob("*"):
            path.unlink()
        notifier = RecordingNotifier()
        run_watchdog(self.env, notifier)
        self.assertIn("每日备份不存在", notifier.sent[0][1])

    def test_backup_manifest_size_mismatch_is_reported(self):
        manifest = Path(self.env.config["paths"]["backup_daily"]) / "manifest-20261112.json"
        payload = json.loads(manifest.read_text(encoding="utf-8"))
        payload["files"]["memory-20261112.sqlite3"]["size"] += 1
        _write(manifest, json.dumps(payload), self.env.now)

        notifier = RecordingNotifier()
        run_watchdog(self.env, notifier)
        self.assertIn("大小与 manifest 不一致", notifier.sent[0][1])

    def test_backup_manifest_user_version_mismatch_is_reported(self):
        directory = Path(self.env.config["paths"]["backup_daily"])
        write_backup_triple(
            directory, "20261112", "daily", self.env.now, source_user_version=0, db_user_version=999
        )

        notifier = RecordingNotifier()
        run_watchdog(self.env, notifier)
        body = notifier.sent[0][1]
        self.assertIn("user_version 不一致", body)
        self.assertIn("db=999", body)

    def test_backup_meta_generation_mismatch_is_reported(self):
        directory = Path(self.env.config["paths"]["backup_daily"])
        write_backup_triple(directory, "20261112", "daily", self.env.now, meta_generation=999)

        notifier = RecordingNotifier()
        run_watchdog(self.env, notifier)
        body = notifier.sent[0][1]
        self.assertIn("代次不一致", body)
        self.assertIn("meta=999", body)

    def test_missing_manifest_after_upgrade_is_an_error_not_legacy(self):
        directory = Path(self.env.config["paths"]["backup_daily"])
        write_backup_triple(directory, "20261111", "daily", self.env.now - 86400)
        (directory / "manifest-20261112.json").unlink()

        notifier = RecordingNotifier()
        run_watchdog(self.env, notifier)
        body = notifier.sent[0][1]
        self.assertIn("缺少同代 manifest", body)
        self.assertNotIn("legacy 格式", body)

    def test_legacy_backup_without_manifest_is_an_explicit_notice(self):
        directory = Path(self.env.config["paths"]["backup_daily"])
        for path in directory.iterdir():
            path.unlink()
        write_legacy_backup(directory, "20261112", self.env.now)

        notifier = RecordingNotifier()
        code, _, _ = run_watchdog(self.env, notifier)
        self.assertEqual(code, 0)
        self.assertEqual(len(notifier.sent), 1)
        self.assertIn("legacy 格式", notifier.sent[0][1])

    def test_legacy_backup_with_an_older_manifest_is_a_notice_not_an_error(self):
        directory = Path(self.env.config["paths"]["backup_daily"])
        write_backup_triple(directory, "20261111", "daily", self.env.now - 86400)
        for name in ("memory-20261112.sqlite3", "meta-20261112.tar.gz", "manifest-20261112.json"):
            (directory / name).unlink(missing_ok=True)
        write_legacy_backup(directory, "20261112", self.env.now)

        notifier = RecordingNotifier()
        code, _, _ = run_watchdog(self.env, notifier)
        self.assertEqual(code, 0)
        body = notifier.sent[0][1]
        self.assertIn("legacy 格式", body)
        self.assertNotIn("缺少同代 manifest", body)

    def test_backup_cron_failure_and_overdue_are_reported(self):
        jobs_path = Path(self.env.config["paths"]["cron_jobs"])
        jobs = json.loads(jobs_path.read_text(encoding="utf-8"))
        job = next(entry for entry in jobs["jobs"] if entry["id"] == "4d313aab436e")
        job.update(
            {
                "last_status": "error",
                "last_error": "disk full",
                "next_run_at": _iso(self.env.now - 10000),
            }
        )
        _write(jobs_path, json.dumps(jobs), self.env.now)

        notifier = RecordingNotifier()
        run_watchdog(self.env, notifier)
        body = notifier.sent[0][1]
        self.assertIn("共享库自动备份 cron 上次状态 error", body)
        self.assertIn("共享库自动备份 cron 已超过计划时间", body)

    def test_backup_cron_without_timestamps_is_reported(self):
        jobs_path = Path(self.env.config["paths"]["cron_jobs"])
        jobs = json.loads(jobs_path.read_text(encoding="utf-8"))
        job = next(entry for entry in jobs["jobs"] if entry["id"] == "4d313aab436e")
        job.pop("last_run_at", None)
        job.pop("next_run_at", None)
        _write(jobs_path, json.dumps(jobs), self.env.now)

        notifier = RecordingNotifier()
        code, _, _ = run_watchdog(self.env, notifier)
        self.assertEqual(code, 0)
        self.assertEqual(len(notifier.sent), 1)
        body = notifier.sent[0][1]
        self.assertIn("缺少可解析的 last_run_at", body)
        self.assertIn("缺少可解析的 next_run_at", body)

    def test_disabled_backup_cron_is_reported(self):
        jobs_path = Path(self.env.config["paths"]["cron_jobs"])
        jobs = json.loads(jobs_path.read_text(encoding="utf-8"))
        job = next(entry for entry in jobs["jobs"] if entry["id"] == "4d313aab436e")
        job["enabled"] = False
        _write(jobs_path, json.dumps(jobs), self.env.now)

        notifier = RecordingNotifier()
        run_watchdog(self.env, notifier)
        self.assertIn("共享库自动备份 cron 任务已停用", notifier.sent[0][1])

    def test_missing_backup_cron_job_is_reported(self):
        jobs_path = Path(self.env.config["paths"]["cron_jobs"])
        jobs = json.loads(jobs_path.read_text(encoding="utf-8"))
        jobs["jobs"] = [entry for entry in jobs["jobs"] if entry["id"] != "4d313aab436e"]
        _write(jobs_path, json.dumps(jobs), self.env.now)

        notifier = RecordingNotifier()
        run_watchdog(self.env, notifier)
        self.assertIn("cron 中找不到备份任务", notifier.sent[0][1])

    def test_weekly_report_and_cron_state_are_checked(self):
        for path in Path(self.env.config["paths"]["weekly_reports"]).glob("*.md"):
            path.unlink()
        jobs_path = Path(self.env.config["paths"]["cron_jobs"])
        jobs = json.loads(jobs_path.read_text(encoding="utf-8"))
        jobs["jobs"][0].update(
            {"last_status": "error", "last_error": "boom", "next_run_at": _iso(self.env.now - 10000)}
        )
        _write(jobs_path, json.dumps(jobs), self.env.now)
        notifier = RecordingNotifier()
        run_watchdog(self.env, notifier)
        body = notifier.sent[0][1]
        self.assertIn("没有记忆周检报告", body)
        self.assertIn("cron 上次状态", body)
        self.assertIn("超过计划时间", body)

    def test_unreadable_jobs_file_alerts(self):
        jobs_path = Path(self.env.config["paths"]["cron_jobs"])
        _write(jobs_path, "{not json", self.env.now)
        notifier = RecordingNotifier()
        run_watchdog(self.env, notifier)
        self.assertIn("cron 任务文件无法解析", notifier.sent[0][1])

    def test_missing_jobs_file_alerts_as_a_notice(self):
        Path(self.env.config["paths"]["cron_jobs"]).unlink()
        notifier = RecordingNotifier()
        run_watchdog(self.env, notifier)
        self.assertIn("cron 任务文件不存在", notifier.sent[0][1])

    def test_entry_write_stall_is_reported(self):
        database = Path(self.env.config["paths"]["memory_db"])
        connection = sqlite3.connect(database)
        try:
            connection.execute("DELETE FROM source_events")
            connection.execute(
                "INSERT INTO source_events VALUES (?, ?)",
                (_iso(self.env.now - 100000), "human_direct"),
            )
            connection.commit()
        finally:
            connection.close()
        notifier = RecordingNotifier()
        run_watchdog(self.env, notifier)
        self.assertIn("没有新对话条目", notifier.sent[0][1])

    def test_note_index_events_do_not_mask_a_conversation_entry_stall(self):
        database = Path(self.env.config["paths"]["memory_db"])
        connection = sqlite3.connect(database)
        try:
            connection.execute("DELETE FROM source_events")
            connection.execute(
                "INSERT INTO source_events VALUES (?, ?)",
                (_iso(self.env.now - 100000), "human_direct"),
            )
            connection.execute(
                "INSERT INTO source_events VALUES (?, ?)",
                (_iso(self.env.now - 10), "external_document"),
            )
            connection.execute(
                "INSERT INTO source_events VALUES (?, ?)",
                (_iso(self.env.now - 5), "memory_reinjection"),
            )
            connection.commit()
        finally:
            connection.close()
        notifier = RecordingNotifier()
        run_watchdog(self.env, notifier)
        self.assertIn("没有新对话条目", notifier.sent[0][1])

    def test_entry_scan_is_bounded_and_reports_a_window_miss(self):
        config = json.loads(self.env.config_path.read_text(encoding="utf-8"))
        config["thresholds"]["entry_scan_rows"] = 1
        _write(self.env.config_path, json.dumps(config))
        database = Path(self.env.config["paths"]["memory_db"])
        connection = sqlite3.connect(database)
        try:
            connection.execute("DELETE FROM source_events")
            connection.execute(
                "INSERT INTO source_events VALUES (?, ?)",
                (_iso(self.env.now - 100000), "human_direct"),
            )
            connection.execute(
                "INSERT INTO source_events VALUES (?, ?)",
                (_iso(self.env.now - 5), "external_document"),
            )
            connection.commit()
        finally:
            connection.close()
        notifier = RecordingNotifier()
        run_watchdog(self.env, notifier)
        self.assertIn("最近 1 行没有对话条目", notifier.sent[0][1])

    def test_missing_database_is_reported(self):
        Path(self.env.config["paths"]["memory_db"]).unlink()
        notifier = RecordingNotifier()
        run_watchdog(self.env, notifier)
        self.assertIn("共享记忆库不存在", notifier.sent[0][1])

    def test_capture_rejections_are_deduplicated_and_never_echoed(self):
        add_rejection(self.env, "hermes:s1:e1:tool:read@1@1")
        add_rejection(self.env, "hermes:s1:e1:tool:read@1@1", age=30)
        add_rejection(self.env, "hermes:s2:e2:tool:write@1@1 password=hunter2")
        notifier = RecordingNotifier()

        run_watchdog(self.env, notifier)
        self.assertEqual(len(notifier.sent), 1)
        subject, body = notifier.sent[0]
        self.assertIn("新增 2 条", body)
        self.assertIn(datetime.fromtimestamp(self.env.now).strftime("%m-%d %H:%M"), body)
        self.assertNotIn("hunter2", subject + body)
        self.assertEqual(len(read_state(self.env)["rejections"]), 2)

        run_watchdog(self.env, notifier, now=self.env.now + 60)
        self.assertEqual(len(notifier.sent), 1, "seen rejection keys are not reported again")

    def test_first_scan_bootstraps_rejections_without_alerting(self):
        config = json.loads(self.env.config_path.read_text(encoding="utf-8"))
        config["rejection_bootstrap"] = True
        _write(self.env.config_path, json.dumps(config))
        add_rejection(self.env, "hermes:old:e0:tool:read@1@1")
        notifier = RecordingNotifier()

        run_watchdog(self.env, notifier)
        self.assertEqual(notifier.sent, [], "the first scan records the existing backlog silently")
        self.assertEqual(len(read_state(self.env)["rejections"]), 1)

        add_rejection(self.env, "hermes:new:e1:tool:read@1@1")
        run_watchdog(self.env, notifier, now=self.env.now + 60)
        self.assertEqual(len(notifier.sent), 1)
        self.assertIn("新增 1 条", notifier.sent[0][1])

    def test_rejection_scan_is_incremental(self):
        add_rejection(self.env, "hermes:inc:e1:tool:read@1@1")
        notifier = RecordingNotifier()
        run_watchdog(self.env, notifier)
        self.assertIn("新增 1 条", notifier.sent[0][1])
        self.assertIn("log_cursors", read_state(self.env))

        add_rejection(self.env, "hermes:inc:e2:tool:read@1@1")
        run_watchdog(self.env, notifier, now=self.env.now + 60)
        self.assertEqual(len(notifier.sent), 2)
        self.assertIn("新增 1 条", notifier.sent[1][1])

    def test_rejection_scan_is_bounded_per_run(self):
        config = json.loads(self.env.config_path.read_text(encoding="utf-8"))
        config["thresholds"]["rejection_scan_max_lines"] = 1
        _write(self.env.config_path, json.dumps(config))
        add_rejection(self.env, "hermes:bound:e1:tool:read@1@1")
        add_rejection(self.env, "hermes:bound:e2:tool:read@1@1")
        notifier = RecordingNotifier()
        run_watchdog(self.env, notifier)
        self.assertIn("新增 1 条", notifier.sent[0][1])
        run_watchdog(self.env, notifier, now=self.env.now + 60)
        self.assertEqual(len(notifier.sent), 2)
        self.assertIn("新增 1 条", notifier.sent[1][1])

    def test_bounded_bootstrap_stays_pending_until_logs_are_fully_scanned(self):
        config = json.loads(self.env.config_path.read_text(encoding="utf-8"))
        config["rejection_bootstrap"] = True
        config["thresholds"]["rejection_scan_max_lines"] = 1
        _write(self.env.config_path, json.dumps(config))
        add_rejection(self.env, "hermes:pending:e1:tool:read@1@1")
        add_rejection(self.env, "hermes:pending:e2:tool:read@1@1")
        notifier = RecordingNotifier()

        run_watchdog(self.env, notifier)
        self.assertEqual(notifier.sent, [])
        self.assertTrue(read_state(self.env)["rejection_bootstrap_pending"])

        run_watchdog(self.env, notifier, now=self.env.now + 60)
        self.assertEqual(notifier.sent, [])
        self.assertFalse(read_state(self.env)["rejection_bootstrap_pending"])
        self.assertEqual(len(read_state(self.env)["rejections"]), 2)

        add_rejection(self.env, "hermes:pending:e3:tool:read@1@1")
        run_watchdog(self.env, notifier, now=self.env.now + 120)
        self.assertEqual(len(notifier.sent), 1)
        self.assertIn("新增 1 条", notifier.sent[0][1])

    def test_rejection_scan_is_bounded_by_bytes_per_run(self):
        config = json.loads(self.env.config_path.read_text(encoding="utf-8"))
        config["thresholds"]["rejection_scan_max_bytes"] = 200
        _write(self.env.config_path, json.dumps(config))
        add_rejection(self.env, "hermes:bytes:e1:tool:read@1@1")
        add_rejection(self.env, "hermes:bytes:e2:tool:read@1@1")
        notifier = RecordingNotifier()
        run_watchdog(self.env, notifier)
        self.assertIn("新增 1 条", notifier.sent[0][1])
        state = read_state(self.env)
        cursor = state["log_cursors"][self.env.config["paths"]["errors_log"]]
        self.assertLess(cursor["offset"], cursor["size"])
        run_watchdog(self.env, notifier, now=self.env.now + 60)
        self.assertEqual(len(notifier.sent), 2)
        self.assertIn("新增 1 条", notifier.sent[1][1])

    def test_first_send_failure_preserves_rejection_bootstrap(self):
        config = json.loads(self.env.config_path.read_text(encoding="utf-8"))
        config["rejection_bootstrap"] = True
        _write(self.env.config_path, json.dumps(config))
        add_rejection(self.env, "hermes:bootstrap:e1:tool:read@1@1")
        self.env.systemd.props["note-indexer.service"].update(
            {"ActiveState": "failed", "Result": "failed", "ExecMainStatus": "1"}
        )

        failing = FailingNotifier()
        code, _, _ = run_watchdog(self.env, failing)
        self.assertEqual(code, 1)
        self.assertNotIn("rejections", read_state(self.env))

        self.env.systemd.props["note-indexer.service"].update(
            {"ActiveState": "inactive", "Result": "success", "ExecMainStatus": "0"}
        )
        notifier = RecordingNotifier()
        code, _, _ = run_watchdog(self.env, notifier, now=self.env.now + 60)
        self.assertEqual(code, 0)
        self.assertEqual(len(notifier.sent), 1)
        self.assertIn("恢复", notifier.sent[0][0])
        self.assertNotIn("捕获拒绝", notifier.sent[0][1])
        self.assertEqual(len(read_state(self.env)["rejections"]), 1)

    def test_rotated_and_gzipped_logs_are_scanned(self):
        add_rejection(self.env, "hermes:rotated:e3:tool:read@1@1", name="errors.log.1")
        stamp = datetime.fromtimestamp(self.env.now).strftime("%Y-%m-%d %H:%M:%S")
        line = (
            f"{stamp},123 WARNING provider: scope-recall: not stored "
            f"(plaintext_secret_rejected): hermes:gzip:e4:tool:read@1@1\n"
        )
        with gzip.open(Path(self.env.config["paths"]["errors_log"]).with_name("errors.log.2.gz"), "wt") as handle:
            handle.write(line)
        notifier = RecordingNotifier()
        run_watchdog(self.env, notifier)
        self.assertIn("新增 2 条", notifier.sent[0][1])

    def test_one_failed_channel_does_not_block_the_other(self):
        class Channel:
            def __init__(self, name, fail):
                self.name = name
                self.fail = fail
                self.calls = 0

            def send(self, subject, body):
                self.calls += 1
                if self.fail:
                    raise hw.ChannelError("bot123:supersecret failed")

        good = Channel("good", False)
        bad = Channel("bad", True)
        notifier = hw.FanoutNotifier([bad, good])
        results = notifier.send("subject", "body")

        self.assertEqual(bad.calls, 1)
        self.assertEqual(good.calls, 1)
        self.assertEqual([result.ok for result in results], [False, True])
        self.assertNotIn("supersecret", results[0].error)

    def test_all_channels_failing_exits_nonzero_and_retries_rejections(self):
        add_rejection(self.env, "hermes:s9:e9:tool:read@1@1")
        failing = FailingNotifier()
        code, _, stderr = run_watchdog(self.env, failing)
        self.assertEqual(code, 1)
        self.assertIn("channel failing failed", stderr)
        self.assertEqual(read_state(self.env).get("rejections", {}), {})

        retry = RecordingNotifier()
        code, _, _ = run_watchdog(self.env, retry, now=self.env.now + 60)
        self.assertEqual(code, 0)
        self.assertEqual(len(retry.sent), 1, "an undelivered rejection alert is retried")
        self.assertEqual(len(read_state(self.env)["rejections"]), 1)

    def test_failure_trigger_reports_the_unit(self):
        self.env.systemd.props["note-indexer.service"].update(
            {"ActiveState": "failed", "Result": "failed", "ExecMainStatus": "1"}
        )
        notifier = RecordingNotifier()
        code, _, _ = run_watchdog(
            self.env,
            notifier,
            extra_args=("--trigger", "failure", "--unit", "note-indexer"),
        )
        self.assertEqual(code, 0)
        self.assertIn("触发 OnFailure", notifier.sent[0][1])

    def test_dry_run_prints_and_does_not_send_or_write_state(self):
        self.env.systemd.props["note-indexer.service"].update(
            {"ActiveState": "failed", "Result": "failed", "ExecMainStatus": "1"}
        )
        notifier = RecordingNotifier()
        code, stdout, _ = run_watchdog(self.env, notifier, extra_args=("--dry-run",))
        self.assertEqual(code, 0)
        self.assertEqual(notifier.sent, [])
        self.assertFalse(Path(self.env.config["paths"]["state"]).exists())
        payload = json.loads(stdout)
        self.assertEqual(payload["action"], "alert")
        self.assertIn("note-indexer", payload["body"])


class DeploymentTests(unittest.TestCase):
    def test_timer_keeps_persistent_and_install_seeds_the_stamp(self):
        timer = (ROOT / "deploy" / "systemd" / "hermes-health-watchdog.timer").read_text(encoding="utf-8")
        install = (ROOT / "deploy" / "install.sh").read_text(encoding="utf-8")
        readme = (ROOT / "deploy" / "README.md").read_text(encoding="utf-8")
        self.assertIn("Persistent=true", timer)
        self.assertIn("stamp-hermes-health-watchdog.timer", install)
        self.assertIn("run_systemctl enable hermes-health-watchdog.timer", install)
        self.assertIn("run_systemctl start hermes-health-watchdog.timer", install)
        self.assertIn("stamp", readme)

    def test_onfailure_dropins_use_the_template(self):
        for name in ("note-indexer.service.d", "scope-recall-wake.service.d"):
            path = ROOT / "deploy" / "systemd" / "dropins" / name
            files = list(path.glob("*.conf"))
            self.assertEqual(len(files), 1, name)
            self.assertIn("OnFailure=hermes-health-watchdog@%N.service", files[0].read_text(encoding="utf-8"))

    def test_gateway_has_no_onfailure_dropin_and_is_covered_by_the_probe(self):
        dropins = ROOT / "deploy" / "systemd" / "dropins"
        self.assertFalse((dropins / "hermes-gateway.service.d").exists())
        install = (ROOT / "deploy" / "install.sh").read_text(encoding="utf-8")
        rollback = (ROOT / "deploy" / "rollback.sh").read_text(encoding="utf-8")
        readme = (ROOT / "deploy" / "README.md").read_text(encoding="utf-8")
        self.assertNotIn("hermes-gateway.service.d", install)
        self.assertNotIn("hermes-gateway.service.d", rollback)
        self.assertIn("Restart=always", readme)
        self.assertIn("gateway 不使用 OnFailure", readme)
        self.assertIn("heartbeat", readme)

    def test_rollback_restores_dropins_and_resets_failed(self):
        rollback = (ROOT / "deploy" / "rollback.sh").read_text(encoding="utf-8")
        self.assertIn("restore_latest", rollback)
        self.assertIn("reset-failed", rollback)
        self.assertIn("note-indexer.service.d/10-onfailure.conf", rollback)
        self.assertNotIn("hermes-gateway.service.d/30-onfailure.conf", rollback)


class InstallRollbackTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.systemd = self.root / "etc/systemd/system"
        self.hermes = self.root / "root"
        self.bin = self.root / "bin"
        self.bin.mkdir(parents=True)
        self.systemctl_log = self.root / "systemctl.log"
        self.systemctl = self.bin / "systemctl"
        self.systemctl.write_text(
            "#!/bin/sh\n"
            "printf '%s\\n' \"$*\" >> \"$SYSTEMCTL_LOG\"\n"
            "case \"$1\" in is-active) exit 1;; esac\n"
            "exit 0\n",
            encoding="utf-8",
        )
        os.chmod(self.systemctl, 0o755)
        self.env = os.environ.copy()
        self.env.update(
            {
                "HEALTH_WATCHDOG_SYSTEMD_DIR": str(self.systemd),
                "HEALTH_WATCHDOG_ROOT": str(self.hermes),
                "HEALTH_WATCHDOG_SYSTEMCTL": str(self.systemctl),
                "HEALTH_WATCHDOG_TIMER_STAMP": str(
                    self.root / "var/lib/systemd/timers/stamp-test"
                ),
                "SYSTEMCTL_LOG": str(self.systemctl_log),
            }
        )

    def tearDown(self):
        self.temporary.cleanup()

    def run_script(self, script: Path, *args: str, env=None):
        return subprocess.run(
            ["bash", str(script), *args],
            cwd=str(ROOT),
            env=self.env if env is None else env,
            capture_output=True,
            text=True,
        )

    def _destdir_env(self, destdir: Path):
        env = os.environ.copy()
        for key in list(env):
            if key.startswith("HEALTH_WATCHDOG_"):
                env.pop(key)
        env["DESTDIR"] = str(destdir)
        env["PATH"] = f"{self.bin}:{env['PATH']}"
        env["SYSTEMCTL_LOG"] = str(self.systemctl_log)
        return env

    def _target(self) -> Path:
        return self.hermes / ".hermes/scripts/health_watchdog.py"

    def _write_target(self, content: str) -> Path:
        target = self._target()
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
        os.chmod(target, 0o755)
        return target

    def test_install_stages_verifies_and_replaces_each_file(self):
        target = self._write_target("old\n")
        result = self.run_script(ROOT / "deploy" / "install.sh", "--apply")

        self.assertEqual(result.returncode, 0, result.stderr)
        source = ROOT / "deploy" / "scripts" / "health_watchdog.py"
        self.assertEqual(target.read_text(encoding="utf-8"), source.read_text(encoding="utf-8"))
        self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o755)
        backups = list(target.parent.glob(target.name + ".bak-*"))
        self.assertEqual(len(backups), 1)
        self.assertEqual(backups[0].read_text(encoding="utf-8"), "old\n")
        config_target = self.hermes / ".hermes/health-watchdog.json"
        self.assertEqual(
            config_target.read_text(encoding="utf-8"),
            (ROOT / "deploy" / "health-watchdog.json").read_text(encoding="utf-8"),
        )
        self.assertTrue((self.systemd / "hermes-health-watchdog.timer").is_file())
        self.assertTrue(
            (self.systemd / "note-indexer.service.d" / "10-onfailure.conf").is_file()
        )
        self.assertEqual(list(target.parent.glob(".health-watchdog.install.*")), [])
        log = self.systemctl_log.read_text(encoding="utf-8")
        self.assertIn("daemon-reload", log)
        self.assertIn("enable hermes-health-watchdog.timer", log)
        self.assertIn("start hermes-health-watchdog.timer", log)

    def test_install_missing_source_aborts_before_changing_targets(self):
        deploy = self.root / "deploy-copy"
        shutil.copytree(ROOT / "deploy", deploy)
        (deploy / "systemd/dropins/note-indexer.service.d/10-onfailure.conf").unlink()
        target = self._write_target("old\n")

        result = self.run_script(deploy / "install.sh", "--apply")

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("missing source file", result.stderr)
        self.assertEqual(target.read_text(encoding="utf-8"), "old\n")
        self.assertEqual(list(target.parent.glob("*.bak-*")), [])

    def test_install_checksum_failure_leaves_the_old_file(self):
        real_sha = shutil.which("sha256sum")
        self.assertIsNotNone(real_sha)
        fake_bin = self.root / "fakebin"
        fake_bin.mkdir()
        fake_sha = fake_bin / "sha256sum"
        fake_sha.write_text(
            "#!/bin/sh\n"
            "case \"$1\" in\n"
            "  *.health-watchdog.install.*)\n"
            "    echo 0000000000000000000000000000000000000000000000000000000000000000\n"
            "    exit 0\n"
            "    ;;\n"
            "esac\n"
            f"exec {real_sha} \"$@\"\n",
            encoding="utf-8",
        )
        os.chmod(fake_sha, 0o755)
        env = self.env.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        target = self._write_target("old\n")

        result = self.run_script(ROOT / "deploy" / "install.sh", "--apply", env=env)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("staged checksum mismatch", result.stderr)
        self.assertEqual(target.read_text(encoding="utf-8"), "old\n")
        self.assertEqual(list(target.parent.glob(".health-watchdog.install.*")), [])

    def test_rollback_uses_backup_timestamp_not_mtime(self):
        target = self._write_target("current\n")
        older = Path(str(target) + ".bak-20261112T120000")
        newer = Path(str(target) + ".bak-20261112T120001")
        older.write_text("older\n", encoding="utf-8")
        newer.write_text("newer\n", encoding="utf-8")
        os.utime(older, (1_800_000_100, 1_800_000_100))
        os.utime(newer, (1_800_000_000, 1_800_000_000))

        result = self.run_script(ROOT / "deploy" / "rollback.sh", "--apply")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(target.read_text(encoding="utf-8"), "newer\n")
        self.assertFalse((self.hermes / ".hermes" / "health-watchdog.json").exists())
        self.assertFalse((self.systemd / "hermes-health-watchdog.timer").exists())
        log = self.systemctl_log.read_text(encoding="utf-8")
        self.assertIn("disable --now hermes-health-watchdog.timer", log)
        self.assertIn("stop hermes-health-watchdog.service", log)
        self.assertIn("daemon-reload", log)
        self.assertIn("reset-failed", log)

    def test_rollback_ignores_backups_without_a_timestamp(self):
        target = self._write_target("current\n")
        valid = Path(str(target) + ".bak-20261112T120000")
        invalid = Path(str(target) + ".bak-not-a-timestamp")
        valid.write_text("valid\n", encoding="utf-8")
        invalid.write_text("invalid\n", encoding="utf-8")
        os.utime(valid, (1_800_000_000, 1_800_000_000))
        os.utime(invalid, (1_800_000_100, 1_800_000_100))

        result = self.run_script(ROOT / "deploy" / "rollback.sh", "--apply")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(target.read_text(encoding="utf-8"), "valid\n")

    def test_rollback_checksum_failure_keeps_the_current_file_and_services(self):
        real_sha = shutil.which("sha256sum")
        self.assertIsNotNone(real_sha)
        fake_bin = self.root / "fakebin"
        fake_bin.mkdir()
        fake_sha = fake_bin / "sha256sum"
        fake_sha.write_text(
            "#!/bin/sh\n"
            "case \"$1\" in\n"
            "  *.bak-*) exit 1 ;;\n"
            "esac\n"
            f"exec {real_sha} \"$@\"\n",
            encoding="utf-8",
        )
        os.chmod(fake_sha, 0o755)
        env = self.env.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        target = self._write_target("current\n")
        backup = Path(str(target) + ".bak-20261112T120001")
        backup.write_text("backup\n", encoding="utf-8")

        result = self.run_script(ROOT / "deploy" / "rollback.sh", "--apply", env=env)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("cannot hash backup", result.stderr)
        self.assertEqual(target.read_text(encoding="utf-8"), "current\n")
        self.assertFalse(self.systemctl_log.exists())

    def test_install_dry_run_succeeds_with_no_temporary_files(self):
        result = self.run_script(ROOT / "deploy" / "install.sh")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("dry run", result.stdout)

    def test_rollback_dry_run_succeeds_with_no_backups(self):
        result = self.run_script(ROOT / "deploy" / "rollback.sh")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("dry run", result.stdout)

    def test_install_with_destdir_does_not_call_host_systemctl(self):
        destdir = self.root / "stage"
        env = self._destdir_env(destdir)

        result = self.run_script(ROOT / "deploy" / "install.sh", "--apply", env=env)

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(
            (destdir / "etc/systemd/system/hermes-health-watchdog.timer").is_file()
        )
        self.assertTrue(
            (destdir / "root/.hermes/scripts/health_watchdog.py").is_file()
        )
        self.assertFalse(self.systemctl_log.exists(), "DESTDIR must not touch host systemctl")

    def test_rollback_with_destdir_does_not_call_host_systemctl(self):
        destdir = self.root / "stage"
        script = destdir / "root/.hermes/scripts/health_watchdog.py"
        script.parent.mkdir(parents=True)
        script.write_text("current\n", encoding="utf-8")
        backup = Path(str(script) + ".bak-20261112T120000")
        backup.write_text("print('restored')\n", encoding="utf-8")
        env = self._destdir_env(destdir)

        result = self.run_script(ROOT / "deploy" / "rollback.sh", "--apply", env=env)

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(script.read_text(encoding="utf-8"), "print('restored')\n")
        self.assertFalse(self.systemctl_log.exists(), "DESTDIR must not touch host systemctl")

    def _assert_rollback_rejects(self, target: Path, content: str, message: str):
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("current\n", encoding="utf-8")
        backup = Path(str(target) + ".bak-20261112T120000")
        backup.write_text(content, encoding="utf-8")

        result = self.run_script(ROOT / "deploy" / "rollback.sh", "--apply")

        self.assertNotEqual(result.returncode, 0)
        self.assertIn(message, result.stderr)
        self.assertEqual(target.read_text(encoding="utf-8"), "current\n")
        self.assertFalse(self.systemctl_log.exists())

    def test_rollback_dry_run_rejects_a_corrupt_backup_without_changes(self):
        target = self._write_target("current\n")
        backup = Path(str(target) + ".bak-20261112T120000")
        backup.write_text("def broken(:\n", encoding="utf-8")

        result = self.run_script(ROOT / "deploy" / "rollback.sh")

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("invalid Python backup", result.stderr)
        self.assertEqual(target.read_text(encoding="utf-8"), "current\n")
        self.assertFalse(self.systemctl_log.exists())

    def test_rollback_restores_a_valid_dropin_backup(self):
        target = self.systemd / "note-indexer.service.d/10-onfailure.conf"
        target.parent.mkdir(parents=True)
        target.write_text("[Unit]\nOnFailure=old@%N.service\n", encoding="utf-8")
        backup = Path(str(target) + ".bak-20261112T120000")
        backup.write_text(
            "[Unit]\nOnFailure=hermes-health-watchdog@%N.service\n",
            encoding="utf-8",
        )

        result = self.run_script(ROOT / "deploy" / "rollback.sh", "--apply")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(target.read_text(encoding="utf-8"), backup.read_text(encoding="utf-8"))

    def test_rollback_rejects_a_corrupt_python_backup_before_stopping_services(self):
        self._assert_rollback_rejects(
            self._target(),
            "def broken(:\n",
            "invalid Python backup",
        )

    def test_rollback_rejects_a_corrupt_json_backup_before_stopping_services(self):
        self._assert_rollback_rejects(
            self.hermes / ".hermes/health-watchdog.json",
            "{not json",
            "invalid JSON backup",
        )

    def test_rollback_rejects_a_corrupt_unit_backup_before_stopping_services(self):
        self._assert_rollback_rejects(
            self.systemd / "hermes-health-watchdog.service",
            "[Service\nExecStart=/bin/true\n",
            "invalid unit backup",
        )

    def test_rollback_restarts_the_timer_when_a_restore_fails_after_stop(self):
        real_mv = shutil.which("mv")
        self.assertIsNotNone(real_mv)
        fake_bin = self.root / "fakebin"
        fake_bin.mkdir()
        fake_mv = fake_bin / "mv"
        fake_mv.write_text(
            "#!/bin/sh\n"
            "for arg in \"$@\"; do\n"
            "  case \"$arg\" in *health_watchdog.py) exit 1;; esac\n"
            "done\n"
            f"exec {real_mv} \"$@\"\n",
            encoding="utf-8",
        )
        os.chmod(fake_mv, 0o755)
        env = self.env.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        target = self._write_target("current\n")
        backup = Path(str(target) + ".bak-20261112T120000")
        backup.write_text("print('restored')\n", encoding="utf-8")

        result = self.run_script(ROOT / "deploy" / "rollback.sh", "--apply", env=env)

        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(target.read_text(encoding="utf-8"), "current\n")
        log = self.systemctl_log.read_text(encoding="utf-8")
        self.assertIn("disable --now hermes-health-watchdog.timer", log)
        self.assertIn("enable --now hermes-health-watchdog.timer", log)
        self.assertEqual(list(target.parent.glob(".health-watchdog.rollback.*")), [])


class ChannelTests(unittest.TestCase):
    def test_hermes_send_passes_the_body_on_stdin(self):
        calls = []

        def runner(command, **kwargs):
            calls.append((command, kwargs))
            return types.SimpleNamespace(returncode=0, stdout="", stderr="")

        channel = hw.HermesSendChannel("/bin/hermes", "telegram", runner=runner)
        channel.send("subject", "body")
        command, kwargs = calls[0]
        self.assertEqual(command[:2], ["/bin/hermes", "send"])
        self.assertIn("--file", command)
        self.assertEqual(kwargs["input"], "body")
        self.assertFalse(kwargs.get("shell", False))

    def test_hermes_send_errors_are_sanitized(self):
        def runner(command, **kwargs):
            return types.SimpleNamespace(returncode=1, stdout="", stderr="bot123:supersecret denied")

        channel = hw.HermesSendChannel("/bin/hermes", "telegram", runner=runner)
        with self.assertRaises(hw.ChannelError) as caught:
            channel.send("subject", "body")
        self.assertNotIn("supersecret", str(caught.exception))

    def test_smtp_channel_uses_the_env_file_and_mock_server(self):
        with tempfile.TemporaryDirectory() as temporary:
            env_path = Path(temporary) / "smtp.env"
            env_path.write_text(
                "SENDER=sender@example.test\n"
                "TO=to@example.test\n"
                "SMTP_HOST=smtp.example.test\n"
                "SMTP_PORT=465\n"
                "AUTH_CODE=auth-secret\n",
                encoding="utf-8",
            )
            sent = {}

            class FakeSMTP:
                def __init__(self, host, port, timeout, context=None):
                    sent["server"] = (host, port, timeout)
                    sent["context"] = context

                def login(self, user, password):
                    sent["login"] = (user, password)

                def send_message(self, message):
                    sent["message"] = message

                def quit(self):
                    sent["quit"] = True

            with mock.patch.object(hw.smtplib, "SMTP_SSL", FakeSMTP):
                hw.SmtpChannel(env_path).send("subject", "body")

            self.assertEqual(sent["server"], ("smtp.example.test", 465, 20))
            self.assertEqual(sent["login"], ("sender@example.test", "auth-secret"))
            self.assertEqual(sent["message"]["Subject"], "subject")
            self.assertEqual(sent["context"].verify_mode, ssl.CERT_REQUIRED)
            self.assertTrue(sent["quit"])

    def test_smtp_starttls_uses_a_verified_context(self):
        with tempfile.TemporaryDirectory() as temporary:
            env_path = Path(temporary) / "smtp.env"
            env_path.write_text(
                "SENDER=sender@example.test\n"
                "TO=to@example.test\n"
                "SMTP_HOST=smtp.example.test\n"
                "SMTP_PORT=587\n"
                "AUTH_CODE=auth-secret\n",
                encoding="utf-8",
            )
            sent = {}

            class FakeSMTP:
                def __init__(self, host, port, timeout):
                    sent["server"] = (host, port, timeout)

                def starttls(self, context=None):
                    sent["context"] = context

                def login(self, user, password):
                    sent["login"] = (user, password)

                def send_message(self, message):
                    sent["message"] = message

                def quit(self):
                    sent["quit"] = True

            with mock.patch.object(hw.smtplib, "SMTP", FakeSMTP):
                hw.SmtpChannel(env_path).send("subject", "body")

            self.assertEqual(sent["server"], ("smtp.example.test", 587, 20))
            self.assertEqual(sent["context"].verify_mode, ssl.CERT_REQUIRED)
            self.assertEqual(sent["login"], ("sender@example.test", "auth-secret"))
            self.assertTrue(sent["quit"])

    def test_missing_smtp_settings_are_reported_without_values(self):
        with tempfile.TemporaryDirectory() as temporary:
            env_path = Path(temporary) / "smtp.env"
            env_path.write_text("SENDER=sender@example.test\n", encoding="utf-8")
            with self.assertRaises(hw.ChannelError) as caught:
                hw.SmtpChannel(env_path).send("subject", "body")
            self.assertIn("SMTP 配置缺少", str(caught.exception))
            self.assertNotIn("sender@example.test", str(caught.exception))

    def test_parse_env_file_accepts_export_and_quotes(self):
        with tempfile.TemporaryDirectory() as temporary:
            env_path = Path(temporary) / "smtp.env"
            env_path.write_text(
                "export SENDER='a@example.test'\nTO=\"b@example.test\"\n# comment\n",
                encoding="utf-8",
            )
            values = hw.parse_env_file(env_path)
            self.assertEqual(values["SENDER"], "a@example.test")
            self.assertEqual(values["TO"], "b@example.test")


if __name__ == "__main__":
    unittest.main()
