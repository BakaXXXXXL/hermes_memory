#!/usr/bin/env bash
# Remove the memory health watchdog and restore the newest installed backups.
# Without --apply this only prints the plan.
set -euo pipefail
export LC_ALL=C

APPLY=0
for arg in "$@"; do
  case "$arg" in
    --apply) APPLY=1 ;;
    -h|--help)
      echo "usage: $0 [--apply]"
      exit 0
      ;;
    *)
      echo "unknown argument: $arg" >&2
      exit 2
      ;;
  esac
done

PREFIX="${DESTDIR:-}"
SYSTEMD="${HEALTH_WATCHDOG_SYSTEMD_DIR:-$PREFIX/etc/systemd/system}"
HERMES_ROOT="${HEALTH_WATCHDOG_ROOT:-$PREFIX/root}"
if [ -n "${HEALTH_WATCHDOG_SYSTEMCTL+x}" ]; then
  SYSTEMCTL="$HEALTH_WATCHDOG_SYSTEMCTL"
elif [ -n "$PREFIX" ]; then
  SYSTEMCTL=""
else
  SYSTEMCTL="systemctl"
fi
UNITS=(
  "$SYSTEMD/hermes-health-watchdog.timer"
  "$SYSTEMD/hermes-health-watchdog.service"
  "$SYSTEMD/hermes-health-watchdog@.service"
  "$SYSTEMD/note-indexer.service.d/10-onfailure.conf"
  "$SYSTEMD/scope-recall-wake.service.d/10-onfailure.conf"
)
TARGETS=(
  "${UNITS[@]}"
  "$HERMES_ROOT/.hermes/scripts/health_watchdog.py"
  "$HERMES_ROOT/.hermes/health-watchdog.json"
)
TMP_FILES=()
TMP_DIRS=()
BACKUP_TARGETS=()
BACKUP_FILES=()
BACKUP_HASHES=()
STAGE_TARGETS=()
STAGE_TEMPS=()
STAGE_HASHES=()
SERVICES_STOPPED=0

cleanup() {
  local status=$?
  local tmp dir
  for tmp in "${TMP_FILES[@]:-}"; do
    [ -n "$tmp" ] || continue
    rm -f -- "$tmp" || true
  done
  for dir in "${TMP_DIRS[@]:-}"; do
    [ -n "$dir" ] || continue
    rm -rf -- "$dir" || true
  done
  if [ "$status" -ne 0 ] && [ "$SERVICES_STOPPED" -eq 1 ] && [ -n "$SYSTEMCTL" ]; then
    echo "rollback: failure after stopping services; attempting to restart hermes-health-watchdog.timer" >&2
    "$SYSTEMCTL" enable --now hermes-health-watchdog.timer >/dev/null 2>&1 || \
      "$SYSTEMCTL" start hermes-health-watchdog.timer >/dev/null 2>&1 || true
  fi
  return "$status"
}
trap cleanup EXIT

run() {
  if [ "$APPLY" -eq 1 ]; then
    "$@"
  else
    printf '+'
    printf ' %q' "$@"
    printf '\n'
  fi
}

run_systemctl() {
  if [ -z "$SYSTEMCTL" ]; then
    if [ -n "$PREFIX" ]; then
      printf '+ (DESTDIR=%s; systemctl skipped)' "$PREFIX"
    else
      printf '+ (systemctl disabled; skipped)'
    fi
    printf ' %q' "$@"
    printf '\n'
    return 0
  fi
  run "$SYSTEMCTL" "$@"
}

die() {
  echo "rollback: $*" >&2
  exit 1
}

sha256() {
  local digest
  if command -v sha256sum >/dev/null 2>&1; then
    digest="$(sha256sum "$1" | awk '{print $1}')" || return 1
  elif command -v shasum >/dev/null 2>&1; then
    digest="$(shasum -a 256 "$1" | awk '{print $1}')" || return 1
  else
    return 1
  fi
  [ -n "$digest" ] || return 1
  printf '%s\n' "$digest"
}

backup_id() {
  local name="${1##*.bak-}"
  [[ "$name" =~ ^[0-9]{8}T[0-9]{6,}$ ]] || return 1
  printf '%s\n' "$name"
}

select_latest() {
  local target="$1" latest="" latest_id="" candidate id
  for candidate in "$target".bak-*; do
    [ -f "$candidate" ] || continue
    [ -L "$candidate" ] && continue
    id="$(backup_id "$candidate")" || continue
    if [ -z "$latest" ] || [[ "$id" > "$latest_id" ]]; then
      latest="$candidate"
      latest_id="$id"
    fi
  done
  printf '%s' "$latest"
}

validate_python() {
  python3 -c 'import sys; compile(open(sys.argv[1], encoding="utf-8").read(), sys.argv[1], "exec")' "$1"
}

validate_json() {
  python3 -c 'import json, sys; json.load(open(sys.argv[1], encoding="utf-8"))' "$1"
}

validate_unit() {
  local target="$1" backup="$2" tmpdir wrapper status
  if command -v systemd-analyze >/dev/null 2>&1; then
    tmpdir="$(mktemp -d "${TMPDIR:-/tmp}/health-watchdog-unit.XXXXXX")"
    TMP_DIRS+=("$tmpdir")
    case "$target" in
      *.timer) wrapper="$tmpdir/validate.timer" ;;
      *) wrapper="$tmpdir/validate.service" ;;
    esac
    if [[ "$target" == *.conf ]]; then
      {
        printf '[Service]\nExecStart=/bin/true\n'
        cat -- "$backup"
      } > "$wrapper"
    else
      cp -- "$backup" "$wrapper"
    fi
    status=0
    systemd-analyze verify "$wrapper" >/dev/null 2>&1 || status=$?
    rm -rf -- "$tmpdir"
    return "$status"
  fi
  python3 - "$backup" <<'PY'
import re
import sys

path = sys.argv[1]
try:
    with open(path, encoding="utf-8") as handle:
        text = handle.read()
except (OSError, UnicodeError):
    raise SystemExit(1)
if not text.strip():
    raise SystemExit(1)
section = None
pending = ""
for raw in text.splitlines():
    line = raw.strip()
    if not line or line.startswith("#") or line.startswith(";"):
        continue
    if line.endswith("\\"):
        pending += line[:-1]
        continue
    line = pending + line
    pending = ""
    if line.startswith("["):
        if not line.endswith("]") or len(line) < 3:
            raise SystemExit(1)
        name = line[1:-1]
        if re.fullmatch(r"[A-Za-z0-9_.-]+", name) is None:
            raise SystemExit(1)
        section = name
        continue
    if section is None or "=" not in line:
        raise SystemExit(1)
    key = line.split("=", 1)[0].strip()
    if re.fullmatch(r"[A-Za-z0-9_.-]+", key) is None:
        raise SystemExit(1)
if pending or section is None:
    raise SystemExit(1)
PY
}

validate_backup() {
  local target="$1" backup="$2"
  case "$target" in
    *.py) validate_python "$backup" || die "invalid Python backup $backup" ;;
    *.json) validate_json "$backup" || die "invalid JSON backup $backup" ;;
    *.service|*.timer|*.conf) validate_unit "$target" "$backup" || die "invalid unit backup $backup" ;;
  esac
}

preflight_backups() {
  local target latest digest
  for target in "${TARGETS[@]}"; do
    latest="$(select_latest "$target")"
    if [ -n "$latest" ]; then
      [ -f "$latest" ] && [ ! -L "$latest" ] || die "invalid backup $latest"
      validate_backup "$target" "$latest"
      if [ "$APPLY" -eq 1 ]; then
        digest="$(sha256 "$latest")" || die "cannot hash backup $latest"
      else
        digest=""
      fi
    else
      digest=""
    fi
    BACKUP_TARGETS+=("$target")
    BACKUP_FILES+=("$latest")
    BACKUP_HASHES+=("$digest")
  done
}

stage_latest() {
  local target="$1" latest="$2" expected="$3"
  local parent tmp actual
  if [ -z "$latest" ]; then
    STAGE_TARGETS+=("$target")
    STAGE_TEMPS+=("")
    STAGE_HASHES+=("")
    return 0
  fi
  parent="$(dirname "$target")"
  mkdir -p "$parent"
  tmp="$(mktemp "$parent/.health-watchdog.rollback.XXXXXX")"
  TMP_FILES+=("$tmp")
  cp -a -- "$latest" "$tmp"
  validate_backup "$target" "$tmp" || die "invalid staged rollback for $target"
  actual="$(sha256 "$tmp")" || die "cannot hash staged rollback for $target"
  [ "$actual" = "$expected" ] || die "rollback checksum mismatch for $target"
  STAGE_TARGETS+=("$target")
  STAGE_TEMPS+=("$tmp")
  STAGE_HASHES+=("$expected")
}

restore_latest() {
  local target="$1" latest="$2" expected="${3:-}"
  if [ "$APPLY" -eq 0 ]; then
    if [ -z "$latest" ]; then
      run rm -f -- "$target"
    else
      run cp -a -- "$latest" "$target"
    fi
    return 0
  fi
  stage_latest "$target" "$latest" "$expected"
}

commit_staged() {
  local index target tmp expected actual
  for index in "${!STAGE_TARGETS[@]}"; do
    target="${STAGE_TARGETS[$index]}"
    tmp="${STAGE_TEMPS[$index]}"
    expected="${STAGE_HASHES[$index]}"
    if [ -z "$tmp" ]; then
      rm -f -- "$target"
      [ ! -e "$target" ] || die "cannot remove $target"
      continue
    fi
    mv -f -- "$tmp" "$target"
    actual="$(sha256 "$target")" || die "cannot hash restored $target"
    [ "$actual" = "$expected" ] || die "restored checksum mismatch for $target"
  done
}

preflight_backups

if [ "$APPLY" -eq 0 ]; then
  run_systemctl disable --now hermes-health-watchdog.timer || true
  run_systemctl stop hermes-health-watchdog.service || true
  for index in "${!BACKUP_TARGETS[@]}"; do
    restore_latest "${BACKUP_TARGETS[$index]}" "${BACKUP_FILES[$index]}"
  done
  run_systemctl daemon-reload
  run_systemctl reset-failed hermes-health-watchdog.service || true
  run_systemctl reset-failed hermes-health-watchdog.timer || true
  run_systemctl reset-failed 'hermes-health-watchdog@*' || true
  echo
  echo "dry run: nothing changed; re-run with --apply"
  exit 0
fi

# Stage and validate every restore before stopping a single service.  A bad
# backup or a failed copy therefore leaves the running watchdog untouched.
for index in "${!BACKUP_TARGETS[@]}"; do
  restore_latest "${BACKUP_TARGETS[$index]}" "${BACKUP_FILES[$index]}" "${BACKUP_HASHES[$index]}"
done

SERVICES_STOPPED=1
run_systemctl disable --now hermes-health-watchdog.timer || true
run_systemctl stop hermes-health-watchdog.service || true
commit_staged
run_systemctl daemon-reload
run_systemctl reset-failed hermes-health-watchdog.service || true
run_systemctl reset-failed hermes-health-watchdog.timer || true
run_systemctl reset-failed 'hermes-health-watchdog@*' || true
