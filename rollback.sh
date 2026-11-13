#!/usr/bin/env bash
set -euo pipefail
umask 077

TARGET="/root/.hermes/scripts/shared_store_backup.py"
BACKUP_DIR="/root/.hermes/scripts/.backup-hardening"
BACKUP_FILE=""
APPLY=0
TMP=""

usage() {
    echo "usage: $0 [--apply] [--target PATH] [--backup-dir PATH] [--backup-file PATH]"
    echo "  default is dry-run; --apply requires root"
}

cleanup() {
    if [ -n "$TMP" ]; then
        rm -f -- "$TMP"
    fi
}
trap cleanup EXIT

while [ "$#" -gt 0 ]; do
    case "$1" in
        --apply)
            APPLY=1
            shift
            ;;
        --target)
            TARGET="${2:?--target requires a path}"
            shift 2
            ;;
        --backup-dir)
            BACKUP_DIR="${2:?--backup-dir requires a path}"
            shift 2
            ;;
        --backup-file)
            BACKUP_FILE="${2:?--backup-file requires a path}"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "ERROR: unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

RETIRED_FILE="$BACKUP_DIR/.rollback-retired"

ast_check() {
    python3 -c 'import ast, sys; ast.parse(open(sys.argv[1], encoding="utf-8").read(), filename=sys.argv[1])' "$1"
}

fsync_path() {
    python3 - "$1" <<'PY'
import os
import sys

path = sys.argv[1]
flags = os.O_RDONLY
if os.path.isdir(path):
    flags |= getattr(os, "O_DIRECTORY", 0)
fd = os.open(path, flags)
try:
    os.fsync(fd)
finally:
    os.close(fd)
PY
}

file_sha256() {
    python3 - "$1" <<'PY'
import hashlib
import sys

digest = hashlib.sha256()
with open(sys.argv[1], "rb") as handle:
    for chunk in iter(lambda: handle.read(1024 * 1024), b""):
        digest.update(chunk)
print(digest.hexdigest())
PY
}

backup_sequence() {
    local base="$1"
    local rest="${base#shared_store_backup.py.}"
    local first="${rest%%.*}"
    case "$first" in
        ''|*[!0-9]*) printf '0' ;;
        *) printf '%d' "$((10#$first))" ;;
    esac
}

retired_contains() {
    local sha="$1"
    [ -n "$sha" ] && [ -f "$RETIRED_FILE" ] && grep -qxF -- "$sha" "$RETIRED_FILE"
}

record_retired() {
    local sha="$1"
    [ -n "$sha" ] || return 0
    touch -- "$RETIRED_FILE"
    chmod 0600 -- "$RETIRED_FILE"
    if ! retired_contains "$sha"; then
        printf '%s\n' "$sha" >> "$RETIRED_FILE"
        fsync_path "$RETIRED_FILE"
    fi
}

next_backup_file() {
    local suffix="$1"
    local seq=0 path base first
    for path in "$BACKUP_DIR"/shared_store_backup.py.*.bak; do
        [ -f "$path" ] || continue
        base="$(basename -- "$path")"
        first="${base#shared_store_backup.py.}"
        first="${first%%.*}"
        case "$first" in
            ''|*[!0-9]*) continue ;;
        esac
        if [ "$((10#$first))" -gt "$seq" ]; then
            seq="$((10#$first))"
        fi
    done
    seq="$((seq + 1))"
    printf '%s/shared_store_backup.py.%08d.%s-%s.%s\n' \
        "$BACKUP_DIR" "$seq" "$(date -u +%Y%m%dT%H%M%SZ)" "$$" "$suffix"
}

enumerate_backups() {
    local current_sha="$1"
    local path base seq sha
    [ -d "$BACKUP_DIR" ] || return 0
    while IFS= read -r path; do
        [ -n "$path" ] || continue
        base="$(basename -- "$path")"
        case "$base" in
            *.safety.bak) continue ;;
        esac
        sha="$(file_sha256 "$path" 2>/dev/null)" || {
            echo "WARNING: skipping unreadable backup: $path" >&2
            continue
        }
        if [ -n "$current_sha" ] && [ "$sha" = "$current_sha" ]; then
            continue
        fi
        if retired_contains "$sha"; then
            continue
        fi
        seq="$(backup_sequence "$base")"
        printf '%08d\t%s\t%s\n' "$seq" "$base" "$path"
    done < <(find "$BACKUP_DIR" -maxdepth 1 -type f -name 'shared_store_backup.py.*.bak' -print 2>/dev/null)
}

select_backup_file() {
    local current_sha="$1"
    local line path
    local -a candidates=()
    mapfile -t candidates < <(enumerate_backups "$current_sha" | sort -r)
    for line in "${candidates[@]}"; do
        [ -n "$line" ] || continue
        path="${line#*$'\t'*$'\t'}"
        if [ -s "$path" ] && ast_check "$path" 2>/dev/null; then
            printf '%s\n' "$path"
            return 0
        fi
        echo "WARNING: skipping invalid backup: $path" >&2
    done
    return 0
}

verify_content() {
    local path="$1"
    local expected="$2"
    [ -f "$path" ] || { echo "ERROR: missing restored file: $path" >&2; return 1; }
    cmp -s -- "$BACKUP_FILE" "$path" || { echo "ERROR: content mismatch: $path" >&2; return 1; }
    local actual
    actual="$(file_sha256 "$path")"
    [ "$actual" = "$expected" ] || { echo "ERROR: sha256 mismatch: $path" >&2; return 1; }
    ast_check "$path"
}

CURRENT_SHA=""
if [ -f "$TARGET" ]; then
    CURRENT_SHA="$(file_sha256 "$TARGET")"
fi

if [ -z "$BACKUP_FILE" ]; then
    BACKUP_FILE="$(select_backup_file "$CURRENT_SHA")"
fi

if [ -z "$BACKUP_FILE" ] || [ ! -f "$BACKUP_FILE" ]; then
    echo "ERROR: no older backup script found in $BACKUP_DIR" >&2
    exit 1
fi
BACKUP_FILE="$(cd -- "$(dirname -- "$BACKUP_FILE")" && pwd)/$(basename -- "$BACKUP_FILE")"
ast_check "$BACKUP_FILE" || { echo "ERROR: backup failed AST validation: $BACKUP_FILE" >&2; exit 1; }
BACKUP_SHA="$(file_sha256 "$BACKUP_FILE")"

if [ "$APPLY" -eq 1 ] && [ "$(id -u)" -ne 0 ]; then
    echo "ERROR: --apply requires root" >&2
    exit 1
fi

if [ "$APPLY" -eq 0 ]; then
    echo "DRY-RUN rollback: no files will be changed"
    echo "  backup: $BACKUP_FILE"
    echo "  target: $TARGET"
    if [ -f "$TARGET" ]; then
        echo "  current target will be backed up before restore"
    fi
    echo "Re-run with --apply to roll back."
    exit 0
fi

install -d -m 0700 "$BACKUP_DIR"
chmod 0700 "$BACKUP_DIR"
TARGET_DIR="$(dirname -- "$TARGET")"
install -d -m 0755 "$TARGET_DIR"

REPLACED_SHA=""
if [ -f "$TARGET" ]; then
    REPLACED_SHA="$(file_sha256 "$TARGET")"
fi

TMP="$(mktemp "$TARGET_DIR/.shared_store_backup.py.rollback.XXXXXX")"
install -o root -g root -m 0644 "$BACKUP_FILE" "$TMP"
verify_content "$TMP" "$BACKUP_SHA"
fsync_path "$TMP"

if [ -f "$TARGET" ]; then
    current_backup="$(next_backup_file safety.bak)"
    cp -p -- "$TARGET" "$current_backup"
    chmod 0600 "$current_backup"
    echo "backed up current script: $current_backup"
fi

python3 - "$TMP" "$TARGET" <<'PY'
import os
import sys

os.replace(sys.argv[1], sys.argv[2])
PY
TMP=""
fsync_path "$TARGET_DIR"
verify_content "$TARGET" "$BACKUP_SHA"
record_retired "$REPLACED_SHA"
echo "rolled back: $TARGET <- $BACKUP_FILE"
