#!/usr/bin/env bash
# Install the memory health watchdog.  Without --apply this only prints the plan.
set -euo pipefail
export LC_ALL=C

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APPLY=0
RUN_NOW=0
for arg in "$@"; do
  case "$arg" in
    --apply) APPLY=1 ;;
    --run-now) RUN_NOW=1 ;;
    -h|--help)
      echo "usage: $0 [--apply] [--run-now]"
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
TIMER_STAMP="${HEALTH_WATCHDOG_TIMER_STAMP:-$PREFIX/var/lib/systemd/timers/stamp-hermes-health-watchdog.timer}"
STAMP="$(date +%Y%m%dT%H%M%S%N)"
TMP_FILES=()
INSTALLED_SOURCES=()
INSTALLED_TARGETS=()

cleanup() {
  local status=$?
  local tmp
  for tmp in "${TMP_FILES[@]:-}"; do
    [ -n "$tmp" ] || continue
    rm -f -- "$tmp" || true
  done
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
  echo "install: $*" >&2
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

backup() {
  local target="$1" backup_path tmp target_hash tmp_hash
  [ -e "$target" ] || return 0
  backup_path="$target.bak-$STAMP"
  if [ "$APPLY" -eq 0 ]; then
    run cp -a "$target" "$backup_path"
    return 0
  fi
  tmp="$backup_path.tmp-$$"
  TMP_FILES+=("$tmp")
  cp -a "$target" "$tmp"
  target_hash="$(sha256 "$target")" || die "cannot hash $target"
  tmp_hash="$(sha256 "$tmp")" || die "cannot hash backup for $target"
  [ "$target_hash" = "$tmp_hash" ] || die "backup checksum mismatch for $target"
  mv -f "$tmp" "$backup_path"
}

install_file() {
  local mode="$1" source="$2" target="$3"
  local parent tmp source_hash tmp_hash target_hash
  [ -f "$source" ] || die "missing source file: $source"
  if [ "$APPLY" -eq 0 ]; then
    backup "$target"
    run install -D -m "$mode" "$source" "$target"
    return 0
  fi
  parent="$(dirname "$target")"
  mkdir -p "$parent"
  backup "$target"
  tmp="$(mktemp "$parent/.health-watchdog.install.XXXXXX")"
  TMP_FILES+=("$tmp")
  install -m "$mode" "$source" "$tmp"
  source_hash="$(sha256 "$source")" || die "cannot hash $source"
  tmp_hash="$(sha256 "$tmp")" || die "cannot hash staged copy for $target"
  [ "$source_hash" = "$tmp_hash" ] || die "staged checksum mismatch for $target"
  mv -f "$tmp" "$target"
  target_hash="$(sha256 "$target")" || die "cannot hash installed $target"
  if [ "$source_hash" != "$target_hash" ]; then
    if [ -e "$target.bak-$STAMP" ]; then
      cp -a "$target.bak-$STAMP" "$target" || true
    else
      rm -f "$target"
    fi
    die "installed checksum mismatch for $target"
  fi
  INSTALLED_SOURCES+=("$source")
  INSTALLED_TARGETS+=("$target")
}

verify_installed() {
  local index source_hash target_hash
  for index in "${!INSTALLED_TARGETS[@]}"; do
    source_hash="$(sha256 "${INSTALLED_SOURCES[$index]}")" || die "cannot hash ${INSTALLED_SOURCES[$index]}"
    target_hash="$(sha256 "${INSTALLED_TARGETS[$index]}")" || die "cannot hash ${INSTALLED_TARGETS[$index]}"
    [ "$source_hash" = "$target_hash" ] || die "installed checksum mismatch for ${INSTALLED_TARGETS[$index]}"
  done
}

SOURCES=(
  "$HERE/scripts/health_watchdog.py"
  "$HERE/health-watchdog.json"
  "$HERE/systemd/hermes-health-watchdog.service"
  "$HERE/systemd/hermes-health-watchdog.timer"
  "$HERE/systemd/hermes-health-watchdog@.service"
  "$HERE/systemd/dropins/note-indexer.service.d/10-onfailure.conf"
  "$HERE/systemd/dropins/scope-recall-wake.service.d/10-onfailure.conf"
)
for source in "${SOURCES[@]}"; do
  [ -f "$source" ] || die "missing source file: $source"
done

install_file 0755 "$HERE/scripts/health_watchdog.py" "$HERMES_ROOT/.hermes/scripts/health_watchdog.py"
install_file 0644 "$HERE/health-watchdog.json" "$HERMES_ROOT/.hermes/health-watchdog.json"
install_file 0644 "$HERE/systemd/hermes-health-watchdog.service" "$SYSTEMD/hermes-health-watchdog.service"
install_file 0644 "$HERE/systemd/hermes-health-watchdog.timer" "$SYSTEMD/hermes-health-watchdog.timer"
install_file 0644 "$HERE/systemd/hermes-health-watchdog@.service" "$SYSTEMD/hermes-health-watchdog@.service"
install_file 0644 "$HERE/systemd/dropins/note-indexer.service.d/10-onfailure.conf" \
  "$SYSTEMD/note-indexer.service.d/10-onfailure.conf"
install_file 0644 "$HERE/systemd/dropins/scope-recall-wake.service.d/10-onfailure.conf" \
  "$SYSTEMD/scope-recall-wake.service.d/10-onfailure.conf"

if [ "$APPLY" -eq 1 ] && [ "${#INSTALLED_TARGETS[@]}" -gt 0 ]; then
  verify_installed
fi

run_systemctl daemon-reload
run install -d -m 0755 "$(dirname "$TIMER_STAMP")"
if [ "$APPLY" -eq 1 ]; then
  if [ -z "$SYSTEMCTL" ] || ! "$SYSTEMCTL" is-active --quiet hermes-health-watchdog.timer; then
    # A fresh Persistent timer would catch up immediately; stamp it so the
    # first run lands on the next ten-minute boundary instead.
    touch "$TIMER_STAMP"
  fi
else
  run touch "$TIMER_STAMP"
fi
# With a non-empty $SYSTEMCTL this runs:
#   systemctl enable hermes-health-watchdog.timer
#   systemctl start hermes-health-watchdog.timer
run_systemctl enable hermes-health-watchdog.timer
run_systemctl start hermes-health-watchdog.timer
if [ "$RUN_NOW" -eq 1 ]; then
  run_systemctl start hermes-health-watchdog.service
fi

if [ "$APPLY" -eq 0 ]; then
  echo
  echo "dry run: nothing changed; re-run with --apply"
fi
