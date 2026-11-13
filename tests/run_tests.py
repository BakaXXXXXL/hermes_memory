#!/usr/bin/env python3
"""Run the backup-hardening tests with the standard library only.

Examples:
    python3 tests/run_tests.py
    python3 tests/run_tests.py --scenario concurrent-lock --scenario corrupt-tar -v
    python3 tests/run_tests.py --scenario install-apply --scenario rollback -v
"""

from __future__ import annotations

import argparse
import sys
import unittest
from pathlib import Path

SCENARIOS = {
    "same-day-rerun": ("backup", "test_same_day_rerun_replaces_snapshot_atomically"),
    "live-wal": ("backup", "test_snapshot_reads_live_wal_database"),
    "failure-preserves-old": ("backup", "test_failure_preserves_old_snapshot_and_releases_lock"),
    "tar-validation-failure": ("backup", "test_tar_validation_failure_preserves_old_snapshot"),
    "corrupt-tar": ("backup", "test_corrupt_or_unsafe_tar_is_rejected"),
    "concurrent-lock": ("backup", "test_concurrent_lock_skips_without_waiting"),
    "retry": ("backup", "test_retry_once_is_bounded"),
    "manifest": ("backup", "test_manifest_and_tar_are_readable"),
    "weekly": ("backup", "test_weekly_snapshot_matches_daily"),
    "retention": ("backup", "test_retention_keeps_daily_seven_and_weekly_four"),
    "retention-overrides": ("backup", "test_retention_limits_are_parameterized"),
    "missing-source": ("backup", "test_missing_source_fails_cleanly"),
    "verify": ("backup", "test_verify_detects_mixed_generation_and_hash_damage"),
    "verify-identity": ("backup", "test_verify_detects_manifest_identity_mismatch"),
    "verify-user-version": ("backup", "test_verify_treats_user_version_as_schema_not_generation"),
    "verify-empty": ("backup", "test_verify_fails_on_missing_or_empty_backup_root"),
    "verify-legacy": ("backup", "test_verify_accepts_legacy_snapshot_without_manifest"),
    "verify-legacy-mixed": ("backup", "test_verify_accepts_legacy_and_new_snapshots_together"),
    "verify-legacy-generated": ("backup", "test_verify_rejects_manifestless_snapshot_with_generation"),
    "verify-legacy-latest": ("backup", "test_verify_rejects_manifestless_latest_snapshot_when_manifests_exist"),
    "verify-lock": ("backup", "test_verify_skips_cleanly_when_backup_lock_is_held"),
    "verify-shared-lock": ("backup", "test_verify_uses_shared_lock"),
    "user-version": ("backup", "test_snapshot_preserves_source_user_version"),
    "weekly-backfill": ("backup", "test_weekly_backfills_missed_monday_within_week"),
    "weekly-replace-kill": ("backup", "test_kill_during_weekly_replace_self_heals"),
    "weekly-missing-meta": ("backup", "test_weekly_missing_meta_self_heals"),
    "weekly-mixed-generation": ("backup", "test_weekly_mixed_generation_is_repaired"),
    "weekly-legacy": ("backup", "test_legacy_weekly_is_not_replaced_by_daily"),
    "weekly-mixed-legacy": ("backup", "test_mixed_legacy_weekly_must_gain_manifest"),
    "verify-meta-missing": ("backup", "test_verify_fails_when_meta_file_missing"),
    "verify-manifest-meta": ("backup", "test_verify_detects_manifest_meta_mismatch"),
    "verify-legacy-missing-meta": ("backup", "test_verify_legacy_rejects_missing_required_meta"),
    "verify-legacy-corrupt-db": ("backup", "test_verify_legacy_rejects_corrupt_database"),
    "prune-failure": ("backup", "test_prune_failure_does_not_report_backup_failure"),
    "kill-staging": ("backup", "test_kill_during_staging_preserves_old_triple"),
    "kill-mixed": ("backup", "test_kill_between_replaces_is_detected_by_manifest"),
    "permissions": ("backup", "test_permissions_are_private"),
    "install-dry-run": ("install", "test_install_default_is_dry_run"),
    "install-apply": ("install", "test_install_apply_backs_up_and_installs"),
    "install-atomic": ("install", "test_install_replaces_target_with_rename_and_exact_content"),
    "install-new-target": ("install", "test_install_creates_missing_target"),
    "install-content-check": ("install", "test_install_rejects_content_mismatch_without_touching_target"),
    "rollback": ("install", "test_rollback_default_is_dry_run_and_apply_restores"),
    "rollback-second": ("install", "test_second_rollback_does_not_restore_new_version"),
    "rollback-chain": ("install", "test_rollback_chain_never_reinstalls_newer_versions"),
    "rollback-retired-reinstall": ("install", "test_reinstall_after_rollback_clears_retired_marker"),
    "rollback-failure-retired": ("install", "test_failed_rollback_does_not_record_retired"),
    "rollback-corrupt-latest": ("install", "test_rollback_falls_back_from_corrupt_latest_backup"),
    "rollback-mtime": ("install", "test_rollback_selection_ignores_mtime"),
    "install-ast": ("install", "test_install_rejects_invalid_ast_without_touching_target"),
    "rollback-ast": ("install", "test_rollback_rejects_invalid_backup"),
    "install-root": ("install", "test_apply_requires_root"),
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Run shared_store_backup hardening tests.")
    parser.add_argument(
        "--scenario",
        action="append",
        choices=["all", *SCENARIOS],
        help="run one named scenario; repeat to select several (default: all)",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    tests_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(tests_dir))
    import test_install_rollback as install_tests
    import test_shared_store_backup as backup_tests

    modules = {"backup": backup_tests, "install": install_tests}
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    selected = args.scenario or ["all"]
    if "all" in selected:
        for module in modules.values():
            suite.addTests(loader.loadTestsFromModule(module))
    else:
        for scenario in selected:
            module_key, test_name = SCENARIOS[scenario]
            module = modules[module_key]
            class_name = "BackupHardeningTests" if module_key == "backup" else "InstallRollbackTests"
            suite.addTests(loader.loadTestsFromName(f"{class_name}.{test_name}", module))
    result = unittest.TextTestRunner(verbosity=2 if args.verbose else 1).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
