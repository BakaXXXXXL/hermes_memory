# Vector cleanup

`vector-cleanup` removes vectors from a store's companion whose SQLite subject can no longer be
read. SQLite is the authority: every vector is rebuildable, so the command deletes a vector,
never a source, a claim or a version.

It is a dry run by default. Without `--apply` it only reads the database and the companion and
prints what it would delete; `--manifest-out <file>` writes the full manifest that an apply
must present back unchanged.

Run it from outside any source tree, with the interpreter that runs the store's worker
(`scope-recall` is the console entry for the same command):

```bash
python -I -X utf8 -m scope_recall.maintenance.cli vector-cleanup \
    --database <root>/memory.sqlite3 --runtime-config <root>/runtime-config.json
```

`--runtime-config` supplies the companion's backend, directory, table, dimensions, identity and
embedding space. On a shared store use the shared worker's `runtime-config.json`; an entry's own
config names the same store. Without it, pass `--vector-dir`, `--backend`, `--table-name`,
`--dimensions`, `--agent-id` and `--installation-id` yourself.

## What it classifies

| Category | Meaning | Deleted by default |
|---|---|---|
| `gone` | The SQLite object, revision or block makes the vector unreadable in every mode. | yes |
| `history` | The vector is not current: its revision is suppressed or superseded. A history or as-of read may still reach the SQLite object, and an exact-reference history read can load the revision. | no; `--include-history` adds it |
| `live` | The vector is current. | never |

An id that cannot be parsed is counted as `unknown` and is never deleted. `--show-ids` adds the
delete ids to the printed summary. The manifest always carries every id and a
`classification_sha256` over the classification, so an apply can prove it is deleting exactly
what was reviewed.

## Apply

Stop every writer first: the gateways and MCP servers that capture or recall, and the store's
worker (`autostart pause`, or stop your own timer). A dry run is read-only and may run beside
them; an apply may not.

```bash
python -I -X utf8 -m scope_recall.maintenance.cli vector-cleanup \
    --database <root>/memory.sqlite3 --runtime-config <root>/runtime-config.json \
    --manifest-out <work>/cleanup-manifest.json
python -I -X utf8 -m scope_recall.maintenance.cli vector-cleanup \
    --database <root>/memory.sqlite3 --runtime-config <root>/runtime-config.json \
    --apply --expected-manifest <work>/cleanup-manifest.json \
    --backup-manifest <work>/cleanup-backup.json
```

`--apply` requires both files. `--expected-manifest` is the full manifest the dry run wrote;
`--backup-manifest` is a new path, and the command refuses an existing file. `--manifest-out`
cannot be used with `--apply`, and the two manifest paths must differ. Pass the same
`--include-history` to both runs; a manifest generated for the other delete set is refused with
`CATEGORIES_MISMATCH`. Before deleting, the apply reads the store and companion again and
refuses with `EXPECTED_MANIFEST_MISMATCH` (exit 3) if anything changed since the dry run; run the
dry run again. It also refuses with
`VECTOR_STORE_CHANGED` (exit 4) if the companion's id set changes between that comparison and
the delete.

The apply holds two write barriers: the truth database's writer lease and the companion's
physical write lock. It waits up to 30 s for the companion lock, then refuses with
`VECTOR_WRITE_LOCK_BUSY` (exit 3); a truth writer that holds the lease gives `WRITERS_ACTIVE`.
These locks exclude cooperating writers. They do not detect a process that ignores them, which
is why the writers must be stopped.

Apply is available only for an in-process LanceDB companion, which provides
`physical_write_lock`. The `sqlite-bruteforce` companion has no such lock, and Windows runs
LanceDB in a helper-process store that has none either; both are refused with
`VECTOR_WRITE_LOCK_UNAVAILABLE` rather than written without a barrier. The dry run works for
both, because it opens them read-only.

## The backup manifest is not a backup

The backup manifest is an audit list of ids and the classification it came from; it is not a
copy of the vectors. Every vector is rebuildable from SQLite, so a data backup is the store's
`backup` (database and companion), not this file. The result says
`backup_manifest_is_data_backup: false`.

## No compaction

The command never compacts and its result says `compacted: false`. Deleting rows leaves the
LanceDB fragments and manifests for the next compaction. A worker drain compacts when its
policy says so; to reclaim the space immediately, run one worker pass
(`python -I -X utf8 -m scope_recall.runtime.worker_entry --config <root>/runtime-config.json`)
and let its compaction run when due, or wait for the next drain.

## Exit codes and refusals

| Code | Meaning |
|---|---|
| 0 | The dry run or apply completed. |
| 2 | A refusal or input error: the database or companion is missing, incompatible or not the target's; a required file or lock is unavailable. |
| 3 | The expected manifest no longer matches, or the companion lock stayed busy. |
| 4 | The companion changed during the apply, or a delete did not complete. |

The printed JSON has `status`, `code` and `field` on a refusal. `--report-out <file>` writes the
full result to a new file. Output files are created exclusively with mode 0600 where the
filesystem supports it.
