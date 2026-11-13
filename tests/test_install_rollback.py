from __future__ import annotations

import hashlib
import os
import shutil
import stat
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INSTALL = ROOT / "install.sh"
ROLLBACK = ROOT / "rollback.sh"
SOURCE = ROOT / "shared_store_backup.py"
OLD_SCRIPT = "#!/usr/bin/env python3\nprint('old')\n"
NEW_SCRIPT = SOURCE.read_text(encoding="utf-8")


def run_script(
    script: Path, *args: str, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(script), *args], capture_output=True, text=True, timeout=30, env=env
    )


class InstallRollbackTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="backup-install-test-")
        self.root = Path(self._tmp.name)
        self.source = self.root / "source.py"
        self.source.write_text(NEW_SCRIPT, encoding="utf-8")
        self.target_dir = self.root / "target"
        self.target_dir.mkdir()
        self.target = self.target_dir / "shared_store_backup.py"
        self.target.write_text(OLD_SCRIPT, encoding="utf-8")
        self.backup_dir = self.root / "backups"

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def args(self, *extra: str) -> tuple[str, ...]:
        return (
            "--source",
            str(self.source),
            "--target",
            str(self.target),
            "--backup-dir",
            str(self.backup_dir),
            *extra,
        )

    def assert_mode(self, path: Path, mode: int) -> None:
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), mode, path)

    def test_install_default_is_dry_run(self) -> None:
        result = run_script(INSTALL, *self.args())
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("DRY-RUN", result.stdout)
        self.assertEqual(self.target.read_text(encoding="utf-8"), OLD_SCRIPT)
        self.assertFalse(self.backup_dir.exists())

    @unittest.skipUnless(os.geteuid() == 0, "requires root")
    def test_install_apply_backs_up_and_installs(self) -> None:
        result = run_script(INSTALL, *self.args("--apply"))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.target.read_text(encoding="utf-8"), NEW_SCRIPT)
        self.assert_mode(self.target, 0o644)
        self.assert_mode(self.backup_dir, 0o700)
        backups = list(self.backup_dir.glob("shared_store_backup.py.*.bak"))
        self.assertEqual(len(backups), 1)
        self.assertEqual(backups[0].read_text(encoding="utf-8"), OLD_SCRIPT)
        self.assert_mode(backups[0], 0o600)

    @unittest.skipUnless(os.geteuid() == 0, "requires root")
    def test_install_replaces_target_with_rename_and_exact_content(self) -> None:
        old_inode = self.target.stat().st_ino
        result = run_script(INSTALL, *self.args("--apply"))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotEqual(self.target.stat().st_ino, old_inode)
        self.assertEqual(self.target.read_bytes(), self.source.read_bytes())
        self.assert_mode(self.target, 0o644)

    @unittest.skipUnless(os.geteuid() == 0, "requires root")
    def test_install_creates_missing_target(self) -> None:
        self.target.unlink()
        result = run_script(INSTALL, *self.args("--apply"))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.target.read_text(encoding="utf-8"), NEW_SCRIPT)
        self.assertEqual(list(self.backup_dir.glob("shared_store_backup.py.*.bak")), [])

    @unittest.skipUnless(os.geteuid() == 0, "requires root")
    def test_install_rejects_content_mismatch_without_touching_target(self) -> None:
        real_install = shutil.which("install")
        self.assertIsNotNone(real_install)
        tools = self.root / "tools"
        tools.mkdir()
        fake = tools / "install"
        fake.write_text(
            "#!/usr/bin/env bash\n"
            "set -euo pipefail\n"
            '"$REAL_INSTALL" "$@"\n'
            'last=""\n'
            'for arg in "$@"; do last="$arg"; done\n'
            'case "$last" in\n'
            '  */.shared_store_backup.py.install.*) printf "\\n# corrupt\\n" >> "$last" ;;\n'
            "esac\n",
            encoding="utf-8",
        )
        fake.chmod(0o755)
        env = os.environ.copy()
        env["PATH"] = f"{tools}:{env['PATH']}"
        env["REAL_INSTALL"] = real_install

        result = run_script(INSTALL, *self.args("--apply"), env=env)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("content mismatch", result.stderr)
        self.assertEqual(self.target.read_text(encoding="utf-8"), OLD_SCRIPT)
        self.assertEqual(list(self.backup_dir.glob("shared_store_backup.py.*.bak")), [])
        self.assertEqual(list(self.target_dir.glob(".shared_store_backup.py.install.*")), [])

    @unittest.skipUnless(os.geteuid() == 0, "requires root")
    def test_rollback_default_is_dry_run_and_apply_restores(self) -> None:
        installed = run_script(INSTALL, *self.args("--apply"))
        self.assertEqual(installed.returncode, 0, installed.stderr)
        installed_inode = self.target.stat().st_ino

        dry = run_script(
            ROLLBACK,
            "--target",
            str(self.target),
            "--backup-dir",
            str(self.backup_dir),
        )
        self.assertEqual(dry.returncode, 0, dry.stderr)
        self.assertIn("DRY-RUN", dry.stdout)
        self.assertEqual(self.target.read_text(encoding="utf-8"), NEW_SCRIPT)

        applied = run_script(
            ROLLBACK,
            "--apply",
            "--target",
            str(self.target),
            "--backup-dir",
            str(self.backup_dir),
        )
        self.assertEqual(applied.returncode, 0, applied.stderr)
        self.assertEqual(self.target.read_text(encoding="utf-8"), OLD_SCRIPT)
        self.assertNotEqual(self.target.stat().st_ino, installed_inode)
        self.assert_mode(self.target, 0o644)
        self.assert_mode(self.backup_dir, 0o700)
        backups = list(self.backup_dir.glob("shared_store_backup.py.*.bak"))
        self.assertEqual(len(backups), 2)
        contents = {path.read_text(encoding="utf-8") for path in backups}
        self.assertIn(OLD_SCRIPT, contents)
        self.assertIn(NEW_SCRIPT, contents)
        for path in backups:
            self.assert_mode(path, 0o600)

    @unittest.skipUnless(os.geteuid() == 0, "requires root")
    def test_second_rollback_does_not_restore_new_version(self) -> None:
        installed = run_script(INSTALL, *self.args("--apply"))
        self.assertEqual(installed.returncode, 0, installed.stderr)

        first = run_script(
            ROLLBACK,
            "--apply",
            "--target",
            str(self.target),
            "--backup-dir",
            str(self.backup_dir),
        )
        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertEqual(self.target.read_text(encoding="utf-8"), OLD_SCRIPT)

        second = run_script(
            ROLLBACK,
            "--apply",
            "--target",
            str(self.target),
            "--backup-dir",
            str(self.backup_dir),
        )
        self.assertNotEqual(second.returncode, 0)
        self.assertIn("no older backup", second.stderr)
        self.assertEqual(self.target.read_text(encoding="utf-8"), OLD_SCRIPT)
        self.assertNotEqual(self.target.read_text(encoding="utf-8"), NEW_SCRIPT)

    @unittest.skipUnless(os.geteuid() == 0, "requires root")
    def test_rollback_chain_never_reinstalls_newer_versions(self) -> None:
        version_a = "#!/usr/bin/env python3\nVERSION = 'a'\n"
        version_b = "#!/usr/bin/env python3\nVERSION = 'b'\n"
        version_c = "#!/usr/bin/env python3\nVERSION = 'c'\n"
        self.target.write_text(version_a, encoding="utf-8")
        for version in (version_b, version_c):
            self.source.write_text(version, encoding="utf-8")
            installed = run_script(INSTALL, *self.args("--apply"))
            self.assertEqual(installed.returncode, 0, installed.stderr)

        def rollback() -> subprocess.CompletedProcess[str]:
            return run_script(
                ROLLBACK,
                "--apply",
                "--target",
                str(self.target),
                "--backup-dir",
                str(self.backup_dir),
            )

        first = rollback()
        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertEqual(self.target.read_text(encoding="utf-8"), version_b)

        second = rollback()
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertEqual(self.target.read_text(encoding="utf-8"), version_a)

        third = rollback()
        self.assertNotEqual(third.returncode, 0)
        self.assertEqual(self.target.read_text(encoding="utf-8"), version_a)
        self.assertNotEqual(self.target.read_text(encoding="utf-8"), version_c)

    @unittest.skipUnless(os.geteuid() == 0, "requires root")
    def test_reinstall_after_rollback_clears_retired_marker(self) -> None:
        version_a = "#!/usr/bin/env python3\nVERSION = 'a'\n"
        version_b = "#!/usr/bin/env python3\nVERSION = 'b'\n"
        version_c = "#!/usr/bin/env python3\nVERSION = 'c'\n"
        self.target.write_text(version_a, encoding="utf-8")
        version_b_sha = hashlib.sha256(version_b.encode("utf-8")).hexdigest()

        def install(version: str) -> None:
            self.source.write_text(version, encoding="utf-8")
            result = run_script(INSTALL, *self.args("--apply"))
            self.assertEqual(result.returncode, 0, result.stderr)

        def rollback() -> subprocess.CompletedProcess[str]:
            return run_script(
                ROLLBACK,
                "--apply",
                "--target",
                str(self.target),
                "--backup-dir",
                str(self.backup_dir),
            )

        install(version_b)
        first = rollback()
        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertEqual(self.target.read_text(encoding="utf-8"), version_a)

        retired = self.backup_dir / ".rollback-retired"
        self.assertIn(version_b_sha, retired.read_text(encoding="utf-8").splitlines())

        install(version_b)
        retired_lines = (
            retired.read_text(encoding="utf-8").splitlines() if retired.exists() else []
        )
        self.assertNotIn(version_b_sha, retired_lines)

        install(version_c)
        second = rollback()
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertEqual(self.target.read_text(encoding="utf-8"), version_b)

    @unittest.skipUnless(os.geteuid() == 0, "requires root")
    def test_failed_rollback_does_not_record_retired(self) -> None:
        version_a = "#!/usr/bin/env python3\nVERSION = 'a'\n"
        version_b = "#!/usr/bin/env python3\nVERSION = 'b'\n"
        self.target.write_text(version_a, encoding="utf-8")
        self.source.write_text(version_b, encoding="utf-8")
        installed = run_script(INSTALL, *self.args("--apply"))
        self.assertEqual(installed.returncode, 0, installed.stderr)
        self.assertEqual(self.target.read_text(encoding="utf-8"), version_b)

        real_install = shutil.which("install")
        self.assertIsNotNone(real_install)
        tools = self.root / "tools"
        tools.mkdir()
        fake = tools / "install"
        fake.write_text(
            "#!/usr/bin/env bash\n"
            "set -euo pipefail\n"
            '"$REAL_INSTALL" "$@"\n'
            'last=""\n'
            'for arg in "$@"; do last="$arg"; done\n'
            'case "$last" in\n'
            '  */.shared_store_backup.py.rollback.*) printf "\\n# corrupt\\n" >> "$last" ;;\n'
            "esac\n",
            encoding="utf-8",
        )
        fake.chmod(0o755)
        env = os.environ.copy()
        env["PATH"] = f"{tools}:{env['PATH']}"
        env["REAL_INSTALL"] = real_install

        failed = run_script(
            ROLLBACK,
            "--apply",
            "--target",
            str(self.target),
            "--backup-dir",
            str(self.backup_dir),
            env=env,
        )
        self.assertNotEqual(failed.returncode, 0)
        self.assertIn("content mismatch", failed.stderr)
        self.assertEqual(self.target.read_text(encoding="utf-8"), version_b)
        self.assertFalse((self.backup_dir / ".rollback-retired").exists())

        restored = run_script(
            ROLLBACK,
            "--apply",
            "--target",
            str(self.target),
            "--backup-dir",
            str(self.backup_dir),
        )
        self.assertEqual(restored.returncode, 0, restored.stderr)
        self.assertEqual(self.target.read_text(encoding="utf-8"), version_a)
        retired = self.backup_dir / ".rollback-retired"
        self.assertIn(
            hashlib.sha256(version_b.encode("utf-8")).hexdigest(),
            retired.read_text(encoding="utf-8").splitlines(),
        )

    @unittest.skipUnless(os.geteuid() == 0, "requires root")
    def test_rollback_falls_back_from_corrupt_latest_backup(self) -> None:
        version_a = "#!/usr/bin/env python3\nVERSION = 'a'\n"
        version_b = "#!/usr/bin/env python3\nVERSION = 'b'\n"
        version_c = "#!/usr/bin/env python3\nVERSION = 'c'\n"
        self.target.write_text(version_a, encoding="utf-8")
        for version in (version_b, version_c):
            self.source.write_text(version, encoding="utf-8")
            installed = run_script(INSTALL, *self.args("--apply"))
            self.assertEqual(installed.returncode, 0, installed.stderr)

        backups = list(self.backup_dir.glob("shared_store_backup.py.*.bak"))
        by_content = {path.read_text(encoding="utf-8"): path for path in backups}
        self.assertIn(version_a, by_content)
        self.assertIn(version_b, by_content)
        by_content[version_b].write_text("def broken(:\n", encoding="utf-8")
        empty = self.backup_dir / "shared_store_backup.py.00000003.20261113T000000Z-0.bak"
        empty.write_bytes(b"")

        started = time.monotonic()
        rolled = run_script(
            ROLLBACK,
            "--apply",
            "--target",
            str(self.target),
            "--backup-dir",
            str(self.backup_dir),
        )
        elapsed = time.monotonic() - started
        self.assertEqual(rolled.returncode, 0, rolled.stderr)
        self.assertIn("skipping invalid backup", rolled.stderr)
        self.assertLess(elapsed, 10)
        self.assertEqual(self.target.read_text(encoding="utf-8"), version_a)

        by_content[version_a].write_text("def also_broken(:\n", encoding="utf-8")
        failed = run_script(
            ROLLBACK,
            "--apply",
            "--target",
            str(self.target),
            "--backup-dir",
            str(self.backup_dir),
        )
        self.assertNotEqual(failed.returncode, 0)
        self.assertIn("no older backup", failed.stderr)
        self.assertEqual(self.target.read_text(encoding="utf-8"), version_a)

    @unittest.skipUnless(os.geteuid() == 0, "requires root")
    def test_rollback_selection_ignores_mtime(self) -> None:
        version_a = "#!/usr/bin/env python3\nVERSION = 'a'\n"
        version_b = "#!/usr/bin/env python3\nVERSION = 'b'\n"
        version_c = "#!/usr/bin/env python3\nVERSION = 'c'\n"
        self.target.write_text(version_a, encoding="utf-8")

        self.source.write_text(version_b, encoding="utf-8")
        first = run_script(INSTALL, *self.args("--apply"))
        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertEqual(self.target.read_text(encoding="utf-8"), version_b)

        self.source.write_text(version_c, encoding="utf-8")
        second = run_script(INSTALL, *self.args("--apply"))
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertEqual(self.target.read_text(encoding="utf-8"), version_c)

        backups = list(self.backup_dir.glob("shared_store_backup.py.*.bak"))
        by_content = {path.read_text(encoding="utf-8"): path for path in backups}
        self.assertIn(version_a, by_content)
        self.assertIn(version_b, by_content)
        old_time = time.time() - 3600
        new_time = time.time() + 3600
        os.utime(by_content[version_a], (new_time, new_time))
        os.utime(by_content[version_b], (old_time, old_time))

        rolled = run_script(
            ROLLBACK,
            "--apply",
            "--target",
            str(self.target),
            "--backup-dir",
            str(self.backup_dir),
        )
        self.assertEqual(rolled.returncode, 0, rolled.stderr)
        self.assertEqual(self.target.read_text(encoding="utf-8"), version_b)

    def test_install_rejects_invalid_ast_without_touching_target(self) -> None:
        bad = self.root / "bad.py"
        bad.write_text("def broken(:\n", encoding="utf-8")
        result = run_script(
            INSTALL,
            "--apply",
            "--source",
            str(bad),
            "--target",
            str(self.target),
            "--backup-dir",
            str(self.backup_dir),
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("AST", result.stderr)
        self.assertEqual(self.target.read_text(encoding="utf-8"), OLD_SCRIPT)
        self.assertFalse(self.backup_dir.exists())

    def test_rollback_rejects_invalid_backup(self) -> None:
        bad = self.root / "bad.bak"
        bad.write_text("def broken(:\n", encoding="utf-8")
        result = run_script(
            ROLLBACK,
            "--apply",
            "--target",
            str(self.target),
            "--backup-dir",
            str(self.backup_dir),
            "--backup-file",
            str(bad),
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("AST", result.stderr)
        self.assertEqual(self.target.read_text(encoding="utf-8"), OLD_SCRIPT)

    @unittest.skipUnless(os.geteuid() == 0 and shutil.which("runuser"), "requires root and runuser")
    def test_apply_requires_root(self) -> None:
        self.root.chmod(0o755)
        self.source.chmod(0o644)
        self.target_dir.chmod(0o777)
        self.target.chmod(0o644)
        tools = self.root / "tools"
        tools.mkdir()
        tool = tools / "install.sh"
        shutil.copy2(INSTALL, tool)
        tool.chmod(0o755)

        result = subprocess.run(
            [
                "runuser",
                "-u",
                "nobody",
                "--",
                str(tool),
                "--apply",
                "--source",
                str(self.source),
                "--target",
                str(self.target),
                "--backup-dir",
                str(self.backup_dir),
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("requires root", result.stderr)
        self.assertEqual(self.target.read_text(encoding="utf-8"), OLD_SCRIPT)
        self.assertFalse(self.backup_dir.exists())


if __name__ == "__main__":
    unittest.main()
