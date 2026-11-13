from __future__ import annotations

import fcntl
import hashlib
import importlib.util
import io
import json
import os
import signal
import sqlite3
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
import unittest
from pathlib import Path

sys.dont_write_bytecode = True

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "shared_store_backup.py"


def _load_backup_module():
    spec = importlib.util.spec_from_file_location("shared_store_backup_under_test", SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


BACKUP = _load_backup_module()

KILL_WRAPPER = r'''
import importlib.util
import os
import signal
import sys
from pathlib import Path

sys.dont_write_bytecode = True
script = Path(os.environ["BACKUP_SCRIPT"])
spec = importlib.util.spec_from_file_location("backup_under_test", script)
module = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(module)
marker = Path(os.environ["BACKUP_TEST_MARKER"])
mode = os.environ["BACKUP_TEST_MODE"]

if mode == "staging":
    real = module._meta_tar_to_temp

    def paused(*args, **kwargs):
        result = real(*args, **kwargs)
        marker.write_text("staging", encoding="utf-8")
        os.kill(os.getpid(), signal.SIGSTOP)
        return result

    module._meta_tar_to_temp = paused
elif mode == "replace":
    real_replace = module.os.replace

    def replace(src, dst):
        real_replace(src, dst)
        if Path(dst).name.startswith("memory-") and not marker.exists():
            marker.write_text("replace", encoding="utf-8")
            os.kill(os.getpid(), signal.SIGSTOP)

    module.os.replace = replace
elif mode == "weekly-replace":
    real_replace = module.os.replace
    weekly_dir = Path(os.environ["BACKUP_TEST_WEEKLY"])

    def replace(src, dst):
        real_replace(src, dst)
        dst_path = Path(dst)
        if (
            dst_path.parent == weekly_dir
            and dst_path.name.startswith("memory-")
            and not marker.exists()
        ):
            marker.write_text("weekly-replace", encoding="utf-8")
            os.kill(os.getpid(), signal.SIGSTOP)

    module.os.replace = replace
else:
    raise SystemExit(f"unknown mode: {mode}")
raise SystemExit(module.main(sys.argv[1:]))
'''


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class Fixture:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.store = root / "store"
        self.backups = self.store / "backups" / "auto"
        self.daily = self.backups / "daily"
        self.weekly = self.backups / "weekly"
        self.lock = self.backups / ".backup.lock"
        self.src = self.store / "memory.sqlite3"
        self.meta_dir = root / "meta"
        self.notes = root / "notes"
        self.required = [
            "memories/MEMORY.md",
            "memories/USER.md",
            "installation.json",
            "runtime-config.json",
        ]
        self.meta_files: list[tuple[Path, str]] = []
        self._make_source()
        self._make_meta()

    def _make_source(self) -> None:
        self.store.mkdir(parents=True, exist_ok=True)
        con = sqlite3.connect(self.src)
        try:
            con.execute("PRAGMA journal_mode = WAL")
            con.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
            con.executemany("INSERT INTO items(value) VALUES (?)", [("one",), ("two",), ("three",)])
            con.execute("PRAGMA user_version = 1110")
            con.commit()
        finally:
            con.close()

    def _make_meta(self) -> None:
        self.meta_dir.mkdir(parents=True, exist_ok=True)
        files = {
            "MEMORY.md": "memory\n",
            "USER.md": "user\n",
            "installation.json": '{"installation": true}\n',
            "runtime-config.json": '{"runtime": true}\n',
            "scope-recall-runtime-config.json": '{"scope": true}\n',
            "note-index.json": '{"notes": true}\n',
        }
        for name, content in files.items():
            (self.meta_dir / name).write_text(content, encoding="utf-8")
        self.meta_files = [
            (self.meta_dir / "MEMORY.md", "memories/MEMORY.md"),
            (self.meta_dir / "USER.md", "memories/USER.md"),
            (self.meta_dir / "installation.json", "installation.json"),
            (self.meta_dir / "runtime-config.json", "runtime-config.json"),
            (self.meta_dir / "scope-recall-runtime-config.json", "scope-recall/runtime-config.json"),
            (self.meta_dir / "note-index.json", "note-index.json"),
        ]
        self.notes.mkdir()
        (self.notes / "a.md").write_text("note\n", encoding="utf-8")
        (self.notes / "sub").mkdir()
        (self.notes / "sub" / "b.md").write_text("note b\n", encoding="utf-8")

    def args(self, *, now: str = "2026-11-12T03:30:00", extra: tuple[str, ...] = ()) -> list[str]:
        args = [
            sys.executable,
            str(SCRIPT),
            "--store",
            str(self.store),
            "--backup-root",
            str(self.backups),
            "--lock-file",
            str(self.lock),
            "--notes-dir",
            str(self.notes),
            "--now",
            now,
            "--sqlite-timeout",
            "5",
            "--retry-delay",
            "0",
        ]
        for src, arc in self.meta_files:
            args.extend(["--meta-file", f"{src}:{arc}"])
        for arc in self.required:
            args.extend(["--required-meta", arc])
        args.extend(extra)
        return args

    def run(
        self, *, now: str = "2026-11-12T03:30:00", extra: tuple[str, ...] = (), timeout: int = 30
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            self.args(now=now, extra=extra),
            cwd=self.root,
            capture_output=True,
            text=True,
            timeout=timeout,
        )


class BackupHardeningTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="shared-store-backup-test-")
        self.root = Path(self._tmp.name)
        self.fx = Fixture(self.root)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def assert_success(self, result: subprocess.CompletedProcess[str]) -> None:
        self.assertEqual(result.returncode, 0, result.stderr)

    def _make_legacy_snapshot(
        self,
        directory: Path,
        key: str,
        *,
        generation: int | None = None,
    ) -> tuple[Path, Path]:
        """Write a snapshot in the pre-manifest format used by the old production script."""
        directory.mkdir(parents=True, exist_ok=True)
        db = directory / f"memory-{key}.sqlite3"
        src = sqlite3.connect(self.fx.src)
        out = sqlite3.connect(db)
        try:
            src.backup(out)
        finally:
            src.close()
            out.close()
        os.chmod(db, 0o600)

        meta = directory / f"meta-{key}.tar.gz"
        with tarfile.open(meta, "w:gz") as tf:
            for src_path, arc in self.fx.meta_files:
                tf.add(src_path, arcname=arc)
            for path in sorted(self.fx.notes.rglob("*.md")):
                tf.add(path, arcname="notes/" + path.relative_to(self.fx.notes).as_posix())
            if generation is not None:
                payload = json.dumps(
                    {"generation": generation, "kind": directory.name, "snapshot": db.name}
                ).encode("utf-8")
                info = tarfile.TarInfo(BACKUP.GENERATION_MEMBER)
                info.size = len(payload)
                info.mode = 0o600
                tf.addfile(info, io.BytesIO(payload))
        os.chmod(meta, 0o600)
        return db, meta

    def _start_until_marker(
        self,
        mode: str,
        marker: Path,
        *,
        now: str = "2026-11-12T03:30:00",
        weekly: Path | None = None,
    ) -> subprocess.Popen[str]:
        wrapper = self.root / "kill_wrapper.py"
        wrapper.write_text(KILL_WRAPPER, encoding="utf-8")
        env = os.environ.copy()
        env["BACKUP_SCRIPT"] = str(SCRIPT)
        env["BACKUP_TEST_MARKER"] = str(marker)
        env["BACKUP_TEST_MODE"] = mode
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        if weekly is not None:
            env["BACKUP_TEST_WEEKLY"] = str(weekly)
        process = subprocess.Popen(
            [sys.executable, str(wrapper), *self.fx.args(now=now)[2:]],
            cwd=self.root,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        deadline = time.monotonic() + 15
        while not marker.exists():
            if process.poll() is not None:
                stdout, stderr = process.communicate()
                self.fail(f"backup exited before {mode} marker: {stdout} {stderr}")
            if time.monotonic() > deadline:
                process.kill()
                stdout, stderr = process.communicate()
                self.fail(f"timed out waiting for {mode} marker: {stdout} {stderr}")
            time.sleep(0.02)
        return process

    def test_same_day_rerun_replaces_snapshot_atomically(self) -> None:
        first = self.fx.run()
        self.assert_success(first)
        db = self.fx.daily / "memory-20261112.sqlite3"
        meta = self.fx.daily / "meta-20261112.tar.gz"
        manifest = self.fx.daily / "manifest-20261112.json"
        old_db_hash = sha256(db)
        old_manifest = json.loads(manifest.read_text(encoding="utf-8"))
        self.assertEqual(old_manifest["files"][db.name]["sha256"], old_db_hash)

        con = sqlite3.connect(self.fx.src)
        try:
            con.execute("INSERT INTO items(value) VALUES ('four')")
            con.commit()
        finally:
            con.close()

        second = self.fx.run()
        self.assert_success(second)
        new_db_hash = sha256(db)
        self.assertNotEqual(new_db_hash, old_db_hash)
        new_manifest = json.loads(manifest.read_text(encoding="utf-8"))
        self.assertEqual(new_manifest["files"][db.name]["sha256"], new_db_hash)
        self.assertEqual(new_manifest["files"][meta.name]["sha256"], sha256(meta))
        self.assertEqual(len(list(self.fx.daily.glob("memory-*.sqlite3"))), 1)
        self.assertEqual(len(list(self.fx.daily.glob("meta-*.tar.gz"))), 1)
        self.assertEqual(len(list(self.fx.daily.glob("manifest-*.json"))), 1)
        self.assertEqual(list(self.fx.daily.glob(".*.tmp.*")), [])

        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        try:
            self.assertEqual(con.execute("SELECT COUNT(*) FROM items").fetchone()[0], 4)
        finally:
            con.close()

    def test_snapshot_reads_live_wal_database(self) -> None:
        con = sqlite3.connect(self.fx.src)
        try:
            con.execute("PRAGMA journal_mode = WAL")
            con.execute("INSERT INTO items(value) VALUES ('live')")
            con.commit()
            self.assert_success(self.fx.run())
        finally:
            con.close()

        db = self.fx.daily / "memory-20261112.sqlite3"
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        try:
            self.assertEqual(con.execute("SELECT COUNT(*) FROM items").fetchone()[0], 4)
        finally:
            con.close()

    def test_failure_preserves_old_snapshot_and_releases_lock(self) -> None:
        self.assert_success(self.fx.run())
        paths = [
            self.fx.daily / "memory-20261112.sqlite3",
            self.fx.daily / "meta-20261112.tar.gz",
            self.fx.daily / "manifest-20261112.json",
        ]
        before = {path.name: sha256(path) for path in paths}

        (self.fx.meta_dir / "USER.md").unlink()
        failed = self.fx.run()
        self.assertNotEqual(failed.returncode, 0)
        self.assertIn("USER.md", failed.stderr)
        for path in paths:
            self.assertTrue(path.exists(), path)
            self.assertEqual(sha256(path), before[path.name], path)
        self.assertEqual(list(self.fx.daily.glob(".*.tmp.*")), [])

        (self.fx.meta_dir / "USER.md").write_text("user\n", encoding="utf-8")
        retried = self.fx.run()
        self.assert_success(retried)

    def test_tar_validation_failure_preserves_old_snapshot(self) -> None:
        self.assert_success(self.fx.run())
        paths = [
            self.fx.daily / "memory-20261112.sqlite3",
            self.fx.daily / "meta-20261112.tar.gz",
            self.fx.daily / "manifest-20261112.json",
        ]
        before = {path.name: sha256(path) for path in paths}

        user = self.fx.meta_dir / "USER.md"
        user.unlink()
        user.symlink_to(self.fx.meta_dir / "MEMORY.md")
        failed = self.fx.run()
        self.assertNotEqual(failed.returncode, 0)
        self.assertIn("unsupported meta tar member", failed.stderr)
        for path in paths:
            self.assertTrue(path.exists(), path)
            self.assertEqual(sha256(path), before[path.name], path)
        self.assertEqual(list(self.fx.daily.glob(".*.tmp.*")), [])

    def test_corrupt_or_unsafe_tar_is_rejected(self) -> None:
        corrupt = self.root / "corrupt.tar.gz"
        corrupt.write_bytes(b"this is not a gzip stream")
        with self.assertRaises(BACKUP.BackupError):
            BACKUP.validate_meta_tar(corrupt, ["memories/MEMORY.md"])

        valid = self.root / "valid.tar.gz"
        with tarfile.open(valid, "w:gz") as tf:
            tf.add(self.fx.meta_dir / "MEMORY.md", arcname="memories/MEMORY.md")
        truncated = self.root / "truncated.tar.gz"
        data = valid.read_bytes()
        truncated.write_bytes(data[: len(data) // 2])
        with self.assertRaises(BACKUP.BackupError):
            BACKUP.validate_meta_tar(truncated, ["memories/MEMORY.md"])

        unsafe = self.root / "unsafe.tar.gz"
        with tarfile.open(unsafe, "w:gz") as tf:
            tf.add(self.fx.meta_dir / "MEMORY.md", arcname="../escape")
        with self.assertRaises(BACKUP.BackupError):
            BACKUP.validate_meta_tar(unsafe, [])

    def test_concurrent_lock_skips_without_waiting(self) -> None:
        self.fx.backups.mkdir(parents=True, exist_ok=True)
        with self.fx.lock.open("a+") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
            shared = self.fx.run(timeout=10)
            self.assertEqual(shared.returncode, BACKUP.LOCK_BUSY_EXIT, shared.stderr)
            self.assertIn("skipped", shared.stdout.lower())
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            started = time.monotonic()
            skipped = self.fx.run(timeout=10)
            elapsed = time.monotonic() - started
            self.assertEqual(skipped.returncode, BACKUP.LOCK_BUSY_EXIT, skipped.stderr)
            self.assertIn("skipped", skipped.stdout.lower())
            self.assertLess(elapsed, 5)
            self.assertEqual(list(self.fx.daily.glob("memory-*.sqlite3")), [])
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

        succeeded = self.fx.run()
        self.assert_success(succeeded)
        self.assertTrue((self.fx.daily / "memory-20261112.sqlite3").exists())

    def test_retry_once_is_bounded(self) -> None:
        calls: list[int] = []

        def flaky() -> str:
            calls.append(1)
            if len(calls) == 1:
                raise sqlite3.OperationalError("database is locked")
            return "ok"

        self.assertEqual(BACKUP.run_with_one_retry(flaky, delay=0), "ok")
        self.assertEqual(len(calls), 2)

        calls.clear()

        def always_busy() -> str:
            calls.append(1)
            raise sqlite3.OperationalError("database is busy")

        with self.assertRaises(sqlite3.OperationalError):
            BACKUP.run_with_one_retry(always_busy, delay=0)
        self.assertEqual(len(calls), 2)

        calls.clear()

        def permanent() -> str:
            calls.append(1)
            raise sqlite3.OperationalError("no such table: items")

        with self.assertRaises(sqlite3.OperationalError):
            BACKUP.run_with_one_retry(permanent, delay=0)
        self.assertEqual(len(calls), 1)

    def test_manifest_and_tar_are_readable(self) -> None:
        self.assert_success(self.fx.run())
        db = self.fx.daily / "memory-20261112.sqlite3"
        meta = self.fx.daily / "meta-20261112.tar.gz"
        manifest = json.loads((self.fx.daily / "manifest-20261112.json").read_text(encoding="utf-8"))

        self.assertEqual(manifest["schema_version"], 3)
        self.assertEqual(manifest["kind"], "daily")
        self.assertIsInstance(manifest["generation"], int)
        self.assertEqual(manifest["source_user_version"], 1110)
        self.assertEqual(manifest["snapshot"], db.name)
        for name, info in manifest["files"].items():
            path = self.fx.daily / name
            self.assertEqual(info["sha256"], sha256(path))
            self.assertEqual(info["size"], path.stat().st_size)
        self.assertEqual(manifest["meta"]["required"], sorted(self.fx.required))

        with tarfile.open(meta, "r:gz") as tf:
            names = set(tf.getnames())
        self.assertTrue(set(self.fx.required).issubset(names))
        self.assertIn("scope-recall/runtime-config.json", names)
        self.assertIn("note-index.json", names)
        self.assertIn("notes/a.md", names)
        self.assertIn("notes/sub/b.md", names)

    def test_weekly_snapshot_matches_daily(self) -> None:
        self.assert_success(self.fx.run(now="2026-11-09T03:30:00"))
        daily_db = self.fx.daily / "memory-20261109.sqlite3"
        daily_meta = self.fx.daily / "meta-20261109.tar.gz"
        weekly_db = self.fx.weekly / "memory-2026-W46.sqlite3"
        weekly_meta = self.fx.weekly / "meta-2026-W46.tar.gz"
        weekly_manifest = json.loads(
            (self.fx.weekly / "manifest-2026-W46.json").read_text(encoding="utf-8")
        )

        self.assertTrue(weekly_db.exists())
        self.assertTrue(weekly_meta.exists())
        self.assertEqual(sha256(weekly_db), sha256(daily_db))
        self.assertEqual(sha256(weekly_meta), sha256(daily_meta))
        self.assertEqual(weekly_manifest["files"][weekly_db.name]["sha256"], sha256(weekly_db))
        self.assertEqual(weekly_manifest["files"][weekly_meta.name]["sha256"], sha256(weekly_meta))
        self.assert_success(self.fx.run(extra=("--verify",)))

    def test_weekly_backfills_missed_monday_within_week(self) -> None:
        tuesday = self.fx.run(now="2026-11-10T03:30:00")
        self.assert_success(tuesday)
        self.assertIn("backfilled", tuesday.stdout)

        daily_db = self.fx.daily / "memory-20261110.sqlite3"
        weekly_db = self.fx.weekly / "memory-2026-W46.sqlite3"
        weekly_meta = self.fx.weekly / "meta-2026-W46.tar.gz"
        weekly_manifest_path = self.fx.weekly / "manifest-2026-W46.json"
        self.assertTrue(weekly_db.is_file())
        self.assertTrue(weekly_meta.is_file())
        self.assertTrue(weekly_manifest_path.is_file())
        self.assertEqual(sha256(weekly_db), sha256(daily_db))
        manifest = json.loads(weekly_manifest_path.read_text(encoding="utf-8"))
        self.assertEqual(manifest["kind"], "weekly")
        self.assertEqual(manifest["source_user_version"], 1110)
        self.assert_success(self.fx.run(extra=("--verify",)))

        weekly_hash = sha256(weekly_db)
        wednesday = self.fx.run(now="2026-11-11T03:30:00")
        self.assert_success(wednesday)
        self.assertIn("present", wednesday.stdout)
        self.assertEqual(sha256(weekly_db), weekly_hash)

        self.assert_success(self.fx.run(now="2026-11-16T03:30:00"))
        self.assertTrue((self.fx.weekly / "memory-2026-W47.sqlite3").is_file())
        self.assertTrue((self.fx.weekly / "manifest-2026-W47.json").is_file())
        self.assert_success(self.fx.run(extra=("--verify",)))

    def test_kill_during_weekly_replace_self_heals(self) -> None:
        self.assert_success(self.fx.run(now="2026-11-09T03:30:00"))
        weekly_db = self.fx.weekly / "memory-2026-W46.sqlite3"
        weekly_meta = self.fx.weekly / "meta-2026-W46.tar.gz"
        weekly_manifest = self.fx.weekly / "manifest-2026-W46.json"
        old_manifest = json.loads(weekly_manifest.read_text(encoding="utf-8"))

        con = sqlite3.connect(self.fx.src)
        try:
            con.execute("INSERT INTO items(value) VALUES ('weekly-heal')")
            con.commit()
        finally:
            con.close()

        marker = self.root / "weekly-replace.marker"
        process = self._start_until_marker(
            "weekly-replace",
            marker,
            now="2026-11-09T03:30:00",
            weekly=self.fx.weekly,
        )
        try:
            self.assertNotEqual(sha256(weekly_db), old_manifest["files"][weekly_db.name]["sha256"])
            self.assertEqual(sha256(weekly_meta), old_manifest["files"][weekly_meta.name]["sha256"])
        finally:
            process.kill()
            process.communicate(timeout=10)

        mixed = self.fx.run(extra=("--verify",))
        self.assertNotEqual(mixed.returncode, 0)
        self.assertIn("sha256 mismatch", mixed.stderr)

        healed = self.fx.run(now="2026-11-10T03:30:00")
        self.assert_success(healed)
        self.assertNotIn("present", healed.stdout)
        self.assertIn("repaired", healed.stdout)
        self.assert_success(self.fx.run(extra=("--verify",)))

        con = sqlite3.connect(f"file:{weekly_db}?mode=ro", uri=True)
        try:
            self.assertEqual(con.execute("SELECT COUNT(*) FROM items").fetchone()[0], 4)
        finally:
            con.close()

    def test_weekly_missing_meta_self_heals(self) -> None:
        self.assert_success(self.fx.run(now="2026-11-09T03:30:00"))
        weekly_meta = self.fx.weekly / "meta-2026-W46.tar.gz"
        weekly_meta.unlink()

        healed = self.fx.run(now="2026-11-10T03:30:00")
        self.assert_success(healed)
        self.assertNotIn("present", healed.stdout)
        self.assertIn("repaired", healed.stdout)
        self.assertTrue(weekly_meta.is_file())
        self.assert_success(self.fx.run(extra=("--verify",)))

    def test_weekly_mixed_generation_is_repaired(self) -> None:
        self.assert_success(self.fx.run(now="2026-11-09T03:30:00"))
        manifest_path = self.fx.weekly / "manifest-2026-W46.json"
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        payload["generation"] = 1 if payload["generation"] != 1 else 2
        manifest_path.write_text(json.dumps(payload), encoding="utf-8")

        healed = self.fx.run(now="2026-11-10T03:30:00")
        self.assert_success(healed)
        self.assertNotIn("present", healed.stdout)
        self.assertIn("repaired", healed.stdout)
        self.assert_success(self.fx.run(extra=("--verify",)))

    def test_legacy_weekly_is_not_replaced_by_daily(self) -> None:
        weekly_w46_db, weekly_w46_meta = self._make_legacy_snapshot(
            self.fx.weekly, "2026-W46"
        )
        w46_hashes = {path: sha256(path) for path in (weekly_w46_db, weekly_w46_meta)}

        con = sqlite3.connect(self.fx.src)
        try:
            con.execute("INSERT INTO items(value) VALUES ('after-w46-legacy')")
            con.commit()
        finally:
            con.close()

        tuesday = self.fx.run(now="2026-11-10T03:30:00")
        self.assert_success(tuesday)
        self.assertIn("present", tuesday.stdout)
        for path, digest in w46_hashes.items():
            self.assertEqual(sha256(path), digest, path)
        self.assertFalse((self.fx.weekly / "manifest-2026-W46.json").exists())

        weekly_w47_db, weekly_w47_meta = self._make_legacy_snapshot(
            self.fx.weekly, "2026-W47"
        )
        w47_hashes = {path: sha256(path) for path in (weekly_w47_db, weekly_w47_meta)}

        con = sqlite3.connect(self.fx.src)
        try:
            con.execute("INSERT INTO items(value) VALUES ('after-w47-legacy')")
            con.commit()
        finally:
            con.close()

        monday = self.fx.run(now="2026-11-16T03:30:00")
        self.assert_success(monday)
        self.assertIn("present", monday.stdout)
        for path, digest in w47_hashes.items():
            self.assertEqual(sha256(path), digest, path)
        self.assertFalse((self.fx.weekly / "manifest-2026-W47.json").exists())
        self.assert_success(self.fx.run(extra=("--verify",)))

        con = sqlite3.connect(f"file:{weekly_w47_db}?mode=ro", uri=True)
        try:
            self.assertEqual(con.execute("SELECT COUNT(*) FROM items").fetchone()[0], 4)
        finally:
            con.close()

    def test_mixed_legacy_weekly_must_gain_manifest(self) -> None:
        self.assert_success(self.fx.run(now="2026-11-02T03:30:00"))
        self.assertTrue((self.fx.weekly / "manifest-2026-W45.json").is_file())

        legacy_db, legacy_meta = self._make_legacy_snapshot(self.fx.weekly, "2026-W46")
        legacy_hash = sha256(legacy_db)

        result = self.fx.run(now="2026-11-09T03:30:00")
        self.assert_success(result)
        self.assertIn("repaired", result.stdout)
        self.assertTrue((self.fx.weekly / "manifest-2026-W46.json").is_file())
        self.assertNotEqual(sha256(legacy_db), legacy_hash)
        self.assertEqual(
            sha256(legacy_db), sha256(self.fx.daily / "memory-20261109.sqlite3")
        )
        self.assertTrue(legacy_meta.exists())
        self.assert_success(self.fx.run(extra=("--verify",)))

    def test_retention_keeps_daily_seven_and_weekly_four(self) -> None:
        dates = [
            "2026-10-05",
            "2026-10-12",
            "2026-10-19",
            "2026-10-26",
            "2026-11-02",
            "2026-11-09",
            "2026-11-10",
            "2026-11-11",
            "2026-11-12",
        ]
        for date in dates:
            self.assert_success(self.fx.run(now=f"{date}T03:30:00"))

        self.assertEqual(len(list(self.fx.daily.glob("memory-*.sqlite3"))), 7)
        self.assertEqual(len(list(self.fx.daily.glob("meta-*.tar.gz"))), 7)
        self.assertEqual(len(list(self.fx.daily.glob("manifest-*.json"))), 7)
        self.assertFalse((self.fx.daily / "memory-20261005.sqlite3").exists())
        self.assertTrue((self.fx.daily / "memory-20261112.sqlite3").exists())

        self.assertEqual(len(list(self.fx.weekly.glob("memory-*.sqlite3"))), 4)
        self.assertEqual(len(list(self.fx.weekly.glob("meta-*.tar.gz"))), 4)
        self.assertEqual(len(list(self.fx.weekly.glob("manifest-*.json"))), 4)
        self.assertFalse((self.fx.weekly / "memory-2026-W42.sqlite3").exists())
        self.assertTrue((self.fx.weekly / "memory-2026-W46.sqlite3").exists())

    def test_retention_limits_are_parameterized(self) -> None:
        for date in ("2026-11-02", "2026-11-09"):
            self.assert_success(
                self.fx.run(
                    now=f"{date}T03:30:00",
                    extra=("--keep-daily", "2", "--keep-weekly", "1"),
                )
            )
        self.assertEqual(len(list(self.fx.daily.glob("memory-*.sqlite3"))), 2)
        self.assertEqual(len(list(self.fx.weekly.glob("memory-*.sqlite3"))), 1)
        self.assertTrue((self.fx.weekly / "memory-2026-W46.sqlite3").exists())
        self.assertFalse((self.fx.weekly / "memory-2026-W45.sqlite3").exists())

    def test_verify_detects_mixed_generation_and_hash_damage(self) -> None:
        self.assert_success(self.fx.run())
        self.assert_success(self.fx.run(extra=("--verify",)))
        meta = self.fx.daily / "meta-20261112.tar.gz"
        manifest = self.fx.daily / "manifest-20261112.json"
        payload = json.loads(manifest.read_text(encoding="utf-8"))
        original_generation = payload["generation"]
        payload["generation"] = 1 if original_generation != 1 else 2
        manifest.write_text(json.dumps(payload), encoding="utf-8")

        mixed = self.fx.run(extra=("--verify",))
        self.assertNotEqual(mixed.returncode, 0)
        self.assertIn("mixed generation", mixed.stderr)

        payload["generation"] = original_generation
        manifest.write_text(json.dumps(payload), encoding="utf-8")
        self.assert_success(self.fx.run(extra=("--verify",)))

        meta.write_bytes(b"not a tar")
        damaged = self.fx.run(extra=("--verify",))
        self.assertNotEqual(damaged.returncode, 0)
        self.assertIn("sha256 mismatch", damaged.stderr)

    def test_verify_detects_manifest_identity_mismatch(self) -> None:
        self.assert_success(self.fx.run())
        manifest = self.fx.daily / "manifest-20261112.json"
        payload = json.loads(manifest.read_text(encoding="utf-8"))
        payload["kind"] = "weekly"
        payload["snapshot"] = "memory-wrong.sqlite3"
        manifest.write_text(json.dumps(payload), encoding="utf-8")
        result = self.fx.run(extra=("--verify",))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("kind mismatch", result.stderr)
        self.assertIn("snapshot mismatch", result.stderr)

    def test_verify_fails_when_meta_file_missing(self) -> None:
        self.assert_success(self.fx.run())
        meta = self.fx.daily / "meta-20261112.tar.gz"
        meta.unlink()
        result = self.fx.run(extra=("--verify",))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("missing", result.stderr)
        self.assertIn(meta.name, result.stderr)

    def test_verify_detects_manifest_meta_mismatch(self) -> None:
        self.assert_success(self.fx.run())
        manifest = self.fx.daily / "manifest-20261112.json"
        original = manifest.read_text(encoding="utf-8")

        payload = json.loads(original)
        payload["meta"]["members"] = [
            name for name in payload["meta"]["members"] if name != "notes/a.md"
        ]
        manifest.write_text(json.dumps(payload), encoding="utf-8")
        result = self.fx.run(extra=("--verify",))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("meta.members mismatch", result.stderr)

        payload = json.loads(original)
        payload["meta"]["required"] = payload["meta"]["required"] + ["notes/missing.md"]
        manifest.write_text(json.dumps(payload), encoding="utf-8")
        result = self.fx.run(extra=("--verify",))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("meta.required not in members", result.stderr)

        payload = json.loads(original)
        payload.pop("meta")
        manifest.write_text(json.dumps(payload), encoding="utf-8")
        result = self.fx.run(extra=("--verify",))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("invalid meta map", result.stderr)

        manifest.write_text(original, encoding="utf-8")
        self.assert_success(self.fx.run(extra=("--verify",)))

    def test_verify_legacy_rejects_missing_required_meta(self) -> None:
        _, meta = self._make_legacy_snapshot(self.fx.daily, "20261112")
        with tarfile.open(meta, "w:gz") as tf:
            tf.add(self.fx.meta_dir / "MEMORY.md", arcname="memories/MEMORY.md")
        result = self.fx.run(extra=("--verify",))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("meta tar missing required files", result.stderr)

    def test_verify_legacy_rejects_corrupt_database(self) -> None:
        db, _ = self._make_legacy_snapshot(self.fx.daily, "20261112")
        db.write_bytes(b"this is not a sqlite database")
        result = self.fx.run(extra=("--verify",))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("cannot read db", result.stderr)

    def test_snapshot_preserves_source_user_version(self) -> None:
        self.assert_success(self.fx.run())
        db = self.fx.daily / "memory-20261112.sqlite3"
        manifest = json.loads(
            (self.fx.daily / "manifest-20261112.json").read_text(encoding="utf-8")
        )
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        try:
            self.assertEqual(con.execute("PRAGMA user_version").fetchone()[0], 1110)
        finally:
            con.close()
        self.assertEqual(manifest["source_user_version"], 1110)
        self.assert_success(self.fx.run(extra=("--verify",)))

    def test_verify_treats_user_version_as_schema_not_generation(self) -> None:
        self.assert_success(self.fx.run())
        db = self.fx.daily / "memory-20261112.sqlite3"
        manifest_path = self.fx.daily / "manifest-20261112.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        replacement = manifest["generation"]
        if replacement == 1110:
            replacement = 1111
        con = sqlite3.connect(db)
        try:
            con.execute(f"PRAGMA user_version = {replacement}")
            con.commit()
        finally:
            con.close()

        result = self.fx.run(extra=("--verify",))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("user_version mismatch", result.stderr)
        self.assertNotIn("mixed generation", result.stderr)

        con = sqlite3.connect(db)
        try:
            con.execute("PRAGMA user_version = 1110")
            con.commit()
        finally:
            con.close()
        self.assert_success(self.fx.run())
        self.assert_success(self.fx.run(extra=("--verify",)))

    def test_verify_fails_on_missing_or_empty_backup_root(self) -> None:
        missing = self.fx.run(extra=("--verify",))
        self.assertNotEqual(missing.returncode, 0)
        self.assertIn("does not exist", missing.stderr)
        self.assertFalse(self.fx.backups.exists())

        self.fx.daily.mkdir(parents=True)
        self.fx.weekly.mkdir()
        empty = self.fx.run(extra=("--verify",))
        self.assertNotEqual(empty.returncode, 0)
        self.assertIn("no snapshots", empty.stderr)

    def test_verify_accepts_legacy_snapshot_without_manifest(self) -> None:
        self._make_legacy_snapshot(self.fx.daily, "20261112")
        result = self.fx.run(extra=("--verify",))
        self.assert_success(result)
        self.assertIn("1 snapshot", result.stdout)

    def test_verify_accepts_legacy_and_new_snapshots_together(self) -> None:
        legacy_db, _ = self._make_legacy_snapshot(self.fx.daily, "20261111")
        self.assert_success(self.fx.run())
        result = self.fx.run(extra=("--verify",))
        self.assert_success(result)
        self.assertIn("verify ok:", result.stdout)
        self.assertTrue(legacy_db.is_file())

    def test_verify_rejects_manifestless_latest_snapshot_when_manifests_exist(self) -> None:
        self.assert_success(self.fx.run())
        self._make_legacy_snapshot(self.fx.daily, "20261113")
        result = self.fx.run(extra=("--verify",))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("missing manifest", result.stderr)

    def test_verify_rejects_manifestless_snapshot_with_generation(self) -> None:
        self._make_legacy_snapshot(self.fx.daily, "20261112", generation=12345)
        result = self.fx.run(extra=("--verify",))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("missing manifest for generated snapshot", result.stderr)

    def test_verify_skips_cleanly_when_backup_lock_is_held(self) -> None:
        self.assert_success(self.fx.run())
        with self.fx.lock.open("a+") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            result = self.fx.run(extra=("--verify",))
            self.assertEqual(result.returncode, BACKUP.LOCK_BUSY_EXIT, result.stderr)
            self.assertIn("another run holds", result.stderr)
            self.assertNotIn("mixed generation", result.stderr)
            self.assertNotIn("sha256 mismatch", result.stderr)
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def test_verify_uses_shared_lock(self) -> None:
        self.assert_success(self.fx.run())
        with self.fx.lock.open("a+") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
            result = self.fx.run(extra=("--verify",))
            self.assert_success(result)
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def test_kill_during_staging_preserves_old_triple(self) -> None:
        self.assert_success(self.fx.run())
        db = self.fx.daily / "memory-20261112.sqlite3"
        before = sha256(db)
        marker = self.root / "staging.marker"
        process = self._start_until_marker("staging", marker)
        try:
            temps = list(self.fx.daily.glob(".*.tmp.*"))
            self.assertTrue(temps)
            for path in temps:
                if path.is_file():
                    self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600, path)
            self.assertEqual(sha256(db), before)
        finally:
            process.kill()
            process.communicate(timeout=10)
        self.assertEqual(sha256(db), before)
        self.assert_success(self.fx.run(extra=("--verify",)))
        self.assert_success(self.fx.run())
        self.assertEqual(list(self.fx.daily.glob(".*.tmp.*")), [])

    def test_kill_between_replaces_is_detected_by_manifest(self) -> None:
        self.assert_success(self.fx.run())
        db = self.fx.daily / "memory-20261112.sqlite3"
        meta = self.fx.daily / "meta-20261112.tar.gz"
        manifest = self.fx.daily / "manifest-20261112.json"
        old_manifest = json.loads(manifest.read_text(encoding="utf-8"))
        old_meta_hash = sha256(meta)
        old_manifest_hash = sha256(manifest)

        con = sqlite3.connect(self.fx.src)
        try:
            con.execute("INSERT INTO items(value) VALUES ('after-first-backup')")
            con.commit()
        finally:
            con.close()

        marker = self.root / "replace.marker"
        process = self._start_until_marker("replace", marker)
        try:
            self.assertNotEqual(
                sha256(db), old_manifest["files"][db.name]["sha256"]
            )
            self.assertEqual(sha256(meta), old_meta_hash)
            self.assertEqual(sha256(manifest), old_manifest_hash)
        finally:
            process.kill()
            process.communicate(timeout=10)

        mixed = self.fx.run(extra=("--verify",))
        self.assertNotEqual(mixed.returncode, 0)
        self.assertIn("sha256 mismatch", mixed.stderr)

        self.assert_success(self.fx.run())
        self.assert_success(self.fx.run(extra=("--verify",)))

    def test_permissions_are_private(self) -> None:
        self.assert_success(self.fx.run())
        for directory in (self.fx.backups, self.fx.daily, self.fx.weekly):
            self.assertEqual(stat.S_IMODE(directory.stat().st_mode), 0o700, directory)
        for path in (
            self.fx.daily / "memory-20261112.sqlite3",
            self.fx.daily / "meta-20261112.tar.gz",
            self.fx.daily / "manifest-20261112.json",
            self.fx.lock,
        ):
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600, path)

    def test_prune_failure_does_not_report_backup_failure(self) -> None:
        self.fx.daily.mkdir(parents=True, exist_ok=True)
        (self.fx.daily / "memory-20200101.sqlite3").mkdir()
        result = self.fx.run(extra=("--keep-daily", "1"))
        self.assert_success(result)
        self.assertIn("backup ok", result.stdout)
        self.assertIn("prune failed", result.stderr)
        self.assertTrue((self.fx.daily / "memory-20261112.sqlite3").is_file())

    def test_missing_source_fails_cleanly(self) -> None:
        self.fx.src.unlink()
        failed = self.fx.run()
        self.assertNotEqual(failed.returncode, 0)
        self.assertIn("missing", failed.stderr)
        self.assertEqual(list(self.fx.daily.glob(".*.tmp.*")), [])


if __name__ == "__main__":
    unittest.main()
