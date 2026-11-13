#!/usr/bin/env python3
"""共享记忆库自动备份（每日 03:30，由 cron 以 no-agent 脚本模式调用）。

做什么：
  * 用 sqlite3 backup API 生成一致性快照（WAL 安全，不直接 cp 写入中的库）
  * 每日快照：backups/auto/daily/memory-YYYYMMDD.sqlite3
  * 每周快照（周一）：backups/auto/weekly/memory-YYYY-Www.sqlite3
  * 同步备份小文件：MEMORY.md、USER.md、knowledge/notes/、installation.json、
    runtime-config.json、note-index.json → 同名 meta-*.tar.gz
  * 保留：daily 7 份、weekly 4 份；更老的自动删除（只删 auto/ 下的文件）
  * 每次对快照跑 PRAGMA quick_check，失败则退出码非 0

输出：3 行以内摘要；成功退出 0。
"""

import os
import sqlite3
import shutil
import sys
import tarfile
from datetime import datetime
from pathlib import Path

STORE = Path("/root/hermes-shared-store")
SRC = STORE / "memory.sqlite3"
BACKUPS = STORE / "backups" / "auto"
DAILY = BACKUPS / "daily"
WEEKLY = BACKUPS / "weekly"
META_FILES = [
    ("/root/.hermes/memories/MEMORY.md", "memories/MEMORY.md"),
    ("/root/.hermes/memories/USER.md", "memories/USER.md"),
    ("/root/hermes-shared-store/installation.json", "installation.json"),
    ("/root/hermes-shared-store/runtime-config.json", "runtime-config.json"),
    ("/root/.hermes/scope-recall/runtime-config.json", "scope-recall/runtime-config.json"),
    ("/root/.hermes/state/note-index.json", "note-index.json"),
]
KEEP_DAILY = 7
KEEP_WEEKLY = 4


def snapshot(dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    src = sqlite3.connect(f"file:{SRC}?mode=ro", uri=True)
    out = sqlite3.connect(dst)
    try:
        with out:
            src.backup(out)
    finally:
        src.close()
        out.close()
    os.chmod(dst, 0o600)
    con = sqlite3.connect(f"file:{dst}?mode=ro&immutable=1", uri=True)
    try:
        check = con.execute("PRAGMA quick_check").fetchone()[0]
    finally:
        con.close()
    # WAL sidecars of the snapshot are not part of the deliverable.
    for suffix in ("-wal", "-shm"):
        Path(str(dst) + suffix).unlink(missing_ok=True)
    if check != "ok":
        raise RuntimeError(f"quick_check failed: {check}")


def meta_tar(dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(dst, "w:gz") as tf:
        for path, arc in META_FILES:
            p = Path(path)
            if p.exists():
                tf.add(p, arcname=arc)
        notes = Path("/root/.hermes/knowledge/notes")
        for p in sorted(notes.rglob("*.md")):
            tf.add(p, arcname="notes/" + str(p.relative_to(notes)))
    os.chmod(dst, 0o600)


def prune(dirname: str, pattern: str, keep: int) -> int:
    files = sorted((BACKUPS / dirname).glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    removed = 0
    for p in files[keep:]:
        p.unlink(missing_ok=True)
        removed += 1
    return removed


def main() -> int:
    if not SRC.exists():
        print(f"ERROR: missing {SRC}", file=sys.stderr)
        return 1
    now = datetime.now()
    day = now.strftime("%Y%m%d")
    daily_db = DAILY / f"memory-{day}.sqlite3"
    daily_meta = DAILY / f"meta-{day}.tar.gz"
    snapshot(daily_db)
    meta_tar(daily_meta)
    size_mb = round(daily_db.stat().st_size / 1024 / 1024, 1)

    weekly_line = "weekly: skipped today"
    if now.weekday() == 0:  # Monday
        week = now.strftime("%G-W%V")
        weekly_db = WEEKLY / f"memory-{week}.sqlite3"
        weekly_meta = WEEKLY / f"meta-{week}.tar.gz"
        shutil.copy2(daily_db, weekly_db)
        shutil.copy2(daily_meta, weekly_meta)
        os.chmod(weekly_db, 0o600)
        os.chmod(weekly_meta, 0o600)
        prune("weekly", "memory-*.sqlite3", KEEP_WEEKLY)
        prune("weekly", "meta-*.tar.gz", KEEP_WEEKLY)
        weekly_line = f"weekly: {weekly_db.name}"

    removed = prune("daily", "memory-*.sqlite3", KEEP_DAILY) + prune("daily", "meta-*.tar.gz", KEEP_DAILY)
    print(f"backup ok: {daily_db} ({size_mb} MiB, quick_check ok)")
    print(f"{weekly_line}; pruned {removed} old daily/weekly files")
    print(f"total auto backups: {len(list(BACKUPS.rglob('*.sqlite3')))} sqlite, {len(list(BACKUPS.rglob('*.tar.gz')))} meta")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
