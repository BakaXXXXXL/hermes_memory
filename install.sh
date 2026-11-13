#!/usr/bin/env bash
set -euo pipefail
umask 077

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
SOURCE="$SCRIPT_DIR/shared_store_backup.py"
TARGET="/root/.hermes/scripts/shared_store_backup.py"
BACKUP_DIR="/root/.hermes/scripts/.backup-hardening"
APPLY=0
TMP=""

usage() {
    echo "usage: $0 [--apply] [--source PATH] [--target PATH] [--backup-dir PATH]"
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
        --source)
            SOURCE="${2:?--source requires a path}"
            shift 2
            ;;
        --target)
            TARGET="${2:?--target requires a path}"
            shift 2
            ;;
        --backup-dir)
            BACKUP_DIR="${2:?--backup-dir requires a path}"
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

clear_retired() {
    local sha="$1"
    [ -n "$sha" ] || return 0
    [ -f "$RETIRED_FILE" ] || return 0
    local tmp rc=0
    tmp="$(mktemp "$BACKUP_DIR/.rollback-retired.XXXXXX")"
    grep -vxF -- "$sha" "$RETIRED_FILE" > "$tmp" || rc=$?
    if [ "$rc" -gt 1 ]; then
        rm -f -- "$tmp"
        echo "ERROR: cannot read retired marker: $RETIRED_FILE" >&2
        return 1
    fi
    chmod 0600 -- "$tmp"
    mv -f -- "$tmp" "$RETIRED_FILE"
    fsync_path "$RETIRED_FILE"
    fsync_path "$BACKUP_DIR"
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

verify_content() {
    local path="$1"
    local expected="$2"
    [ -f "$path" ] || { echo "ERROR: missing installed file: $path" >&2; return 1; }
    cmp -s -- "$SOURCE" "$path" || { echo "ERROR: content mismatch: $path" >&2; return 1; }
    local actual
    actual="$(file_sha256 "$path")"
    [ "$actual" = "$expected" ] || { echo "ERROR: sha256 mismatch: $path" >&2; return 1; }
    ast_check "$path"
}

[ -f "$SOURCE" ] || { echo "ERROR: source not found: $SOURCE" >&2; exit 1; }
SOURCE="$(cd -- "$(dirname -- "$SOURCE")" && pwd)/$(basename -- "$SOURCE")"
ast_check "$SOURCE" || { echo "ERROR: source failed AST validation: $SOURCE" >&2; exit 1; }
SOURCE_SHA="$(file_sha256 "$SOURCE")"

if [ "$APPLY" -eq 1 ] && [ "$(id -u)" -ne 0 ]; then
    echo "ERROR: --apply requires root" >&2
    exit 1
fi

if [ "$APPLY" -eq 0 ]; then
    echo "DRY-RUN install: no files will be changed"
    echo "  source: $SOURCE"
    echo "  target: $TARGET"
    echo "  backup dir: $BACKUP_DIR"
    if [ -f "$TARGET" ]; then
        echo "  existing target will be backed up before replacement"
    else
        echo "  target does not exist yet; no old script to back up"
    fi
    echo "Re-run with --apply to install."
    exit 0
fi

install -d -m 0700 "$BACKUP_DIR"
chmod 0700 "$BACKUP_DIR"
TARGET_DIR="$(dirname -- "$TARGET")"
install -d -m 0755 "$TARGET_DIR"

TMP="$(mktemp "$TARGET_DIR/.shared_store_backup.py.install.XXXXXX")"
install -o root -g root -m 0644 "$SOURCE" "$TMP"
verify_content "$TMP" "$SOURCE_SHA"
fsync_path "$TMP"

if [ -f "$TARGET" ]; then
    backup="$(next_backup_file bak)"
    cp -p -- "$TARGET" "$backup"
    chmod 0600 "$backup"
    echo "backed up old script: $backup"
fi

python3 - "$TMP" "$TARGET" <<'PY'
import os
import sys

os.replace(sys.argv[1], sys.argv[2])
PY
TMP=""
fsync_path "$TARGET_DIR"
verify_content "$TARGET" "$SOURCE_SHA"
clear_retired "$SOURCE_SHA"
echo "installed: $TARGET"
