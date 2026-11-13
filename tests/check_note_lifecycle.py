import importlib.util
import os
import sys
import tempfile
import time
import types
from pathlib import Path

MODULE = Path(__file__).resolve().parents[1]
ROOT = Path(
    os.environ.get("NOTE_INDEXER_CORE_ROOT", MODULE.parent / "integration" / "scope-recall-src")
).resolve()
for _name in list(sys.modules):
    if _name == "scope_recall" or _name.startswith("scope_recall."):
        del sys.modules[_name]
pkg = types.ModuleType("scope_recall")
pkg.__path__ = [str(ROOT)]
pkg.__file__ = str(ROOT / "__init__.py")
sys.modules["scope_recall"] = pkg

from scope_recall.contracts import InstanceBinding, TrustedContext
from scope_recall.core import MemoryCore
from scope_recall.core.capture import CaptureReceipt

spec = importlib.util.spec_from_file_location("note_indexer_under_test", MODULE / "note_indexer.py")
ni = importlib.util.module_from_spec(spec)
sys.modules["note_indexer_under_test"] = ni
spec.loader.exec_module(ni)

class Clock:
    now = "2026-11-12T12:00:00Z"
    def utc_now(self): return self.now
    def monotonic(self): return time.monotonic()

def make_env(tmp):
    scope = "TEST-scope"
    binding = InstanceBinding("TEST-agent", "TEST-installation", tmp / "store", frozenset({scope}), True)
    ctx = TrustedContext(binding, "TEST-session", frozenset({scope}), "external_document")
    core = MemoryCore(ni.core_config(binding), clock=Clock())
    core.initialize()
    return core, ctx, scope

def note_id(path):
    return "note-" + ni.hashlib.sha256(str(path).encode()).hexdigest()[:24]

def key(path, section, part=0):
    return ni.chunk_key(note_id(path), section, part)

def write(path, text):
    path.write_text(text, encoding="utf-8")

def run(core, ctx, scope, notes, state, now="2026-11-12T12:00:00Z"):
    return ni.index_notes(core, ctx, notes, state, scope_id=scope, apply=True, now=now)

def main():
    tmp = Path(tempfile.mkdtemp(prefix="nl-e2e-"))
    notes = tmp / "notes"; notes.mkdir()
    state = tmp / "state.json"
    core, ctx, scope = make_env(tmp)
    path = notes / "alpha.md"
    write(path, "# Alpha\n\n## A\n\nalpha one\n\n## B\n\nbeta one\n")

    s1 = run(core, ctx, scope, notes, state)
    assert s1["inserted"] == 3 and s1["notes"] == 1, s1
    k_a, k_b = key(path, 1), key(path, 2)
    assert core.source_by_event_key(ctx, k_a, 1).event["content"].endswith("alpha one")
    assert core.source_by_event_key(ctx, k_b, 1).event["content"].endswith("beta one")

    epoch_before = core.memory_epoch(ctx)
    write(path, "# Alpha\n\n## A\n\nalpha two\n\n## B\n\nbeta one\n")
    s2 = run(core, ctx, scope, notes, state)
    assert s2["inserted"] == 1 and s2["updated"] == 1, s2
    old_a = core.source_by_event_key(ctx, k_a, 1)
    new_a = core.source_by_event_key(ctx, k_a, 2)
    assert old_a.suppressed and not new_a.suppressed, (old_a.suppressed, new_a.suppressed)
    assert core.memory_epoch(ctx) > epoch_before
    assert core.memory_retracted_since(ctx, epoch_before)
    ref_a = new_a.ref
    hits2 = [h for h in core.search_sources(ctx, "alpha two", automatic=True) if h.ref == ref_a]
    assert [(h.revision, h.suppressed) for h in hits2] == [(2, False)], hits2

    # remove section B: whole chunk object is suppressed
    write(path, "# Alpha\n\n## A\n\nalpha two\n")
    s3 = run(core, ctx, scope, notes, state)
    assert s3["removed"] == 1, s3
    ref_b = core.source_by_event_key(ctx, k_b, 1).ref
    assert core.source_by_event_key(ctx, k_b, 1).suppressed
    assert not [h for h in core.search_sources(ctx, "beta one", automatic=True) if h.ref == ref_b]

    # a removed chunk can return as a new generation; retiring is not a permanent object block
    write(path, "# Alpha\n\n## A\n\nalpha two\n\n## B\n\nbeta two\n")
    s3b = run(core, ctx, scope, notes, state)
    assert s3b["inserted"] == 1, s3b
    returned = core.source_by_event_key(ctx, k_b, 2)
    assert returned is not None and not returned.suppressed, returned
    assert [h.revision for h in core.search_sources(ctx, "beta two", automatic=True) if h.ref == returned.ref] == [2]

    # rollback A to its first text: a new generation, never a duplicate of rev 1
    write(path, "# Alpha\n\n## A\n\nalpha one\n")
    s4 = run(core, ctx, scope, notes, state)
    assert s4["inserted"] == 1, s4
    assert core.source_by_event_key(ctx, k_a, 2).suppressed
    current = core.source_by_event_key(ctx, k_a, 3)
    assert current is not None and not current.suppressed and current.event["content"].endswith("alpha one")
    hits4 = [h for h in core.search_sources(ctx, "alpha one", automatic=True) if h.ref == ref_a]
    assert [(h.revision, h.suppressed) for h in hits4] == [(3, False)], hits4

    # rejected content is terminal and never counted as duplicate
    secret = "sk-" + "a" * 24
    write(path, "# Alpha\n\n## A\n\nalpha one\n\n## C\n\n" + secret + "\n")
    s5 = run(core, ctx, scope, notes, state)
    assert s5["rejected"] == 1 and s5["inserted"] == 0, s5
    k_c = key(path, 3)
    assert core.source_by_event_key(ctx, k_c, 1) is None
    s6 = run(core, ctx, scope, notes, state)
    assert s6["skipped_rejected"] == 1 and s6["rejected"] == 0, s6

    # conflict: a writer that starts at a revision the store already holds advances
    core2, ctx2, scope2 = make_env(tmp / "conflict")
    chunk = {"content": "conflict one", "hash": ni.compact_hash("conflict one")}
    ev = ni.chunk_event(path, chunk, "conflict-key", 1, Clock.now)
    first = core2.record_event(ctx2, ev, scope_id=scope2, remaining_seconds=10)
    assert first.disposition == "inserted"
    ev2 = dict(ev, content="conflict two")
    receipt = ni._record_with_retry(core2, ctx2, ev2, scope_id=scope2, revision=1)
    assert receipt.disposition == "inserted", receipt
    assert receipt.event_refs[0].revision == 2

    # unavailable is not a duplicate and leaves the state for the next pass
    core3, ctx3, scope3 = make_env(tmp / "unavailable")
    notes3 = tmp / "notes3"; notes3.mkdir()
    state3 = tmp / "state3.json"
    p3 = notes3 / "gamma.md"
    write(p3, "# Gamma\n\n## A\n\ngamma one\n")
    real_record = core3.record_event
    core3.record_event = lambda *a, **k: CaptureReceipt(
        "unavailable", (), "unknown", "unknown", "unknown", error_code="STORAGE_UNAVAILABLE"
    )
    s7 = run(core3, ctx3, scope3, notes3, state3)
    assert s7["unavailable"] == 2 and s7["duplicate"] == 0, s7
    assert ni.load_state(state3)["notes"][note_id(p3)]["chunks"] == {}
    core3.record_event = real_record
    s8 = run(core3, ctx3, scope3, notes3, state3)
    assert s8["inserted"] == 2, s8

    # legacy heading-hash key is retired as a whole object and removed from state
    core4, ctx4, scope4 = make_env(tmp / "legacy")
    notes4 = tmp / "notes4"; notes4.mkdir()
    state4 = tmp / "state4.json"
    p4 = notes4 / "legacy.md"
    write(p4, "# Legacy\n\n## A\n\nlegacy body\n")
    old_key = ni.chunk_key(note_id(p4), 1, 0) + ":deadbeefdeadbeefdead"
    legacy_event = ni.chunk_event(p4, {"content": "legacy old body", "hash": "x"}, old_key, 1, Clock.now)
    assert core4.record_event(ctx4, legacy_event, scope_id=scope4, remaining_seconds=10).disposition == "inserted"
    state4.write_text(
        ni.json.dumps({"notes": {note_id(p4): {"path": str(p4), "title": "Legacy", "chunks": {
            old_key: {"hash": "x", "revision": 1, "section": 1, "heading": "A"}
        }, "rejected": {}}}}, ensure_ascii=False),
        encoding="utf-8",
    )
    s9 = run(core4, ctx4, scope4, notes4, state4)
    assert s9["legacy_retired"] == 1 and s9["inserted"] == 2, s9
    legacy_ref = core4.source_by_event_key(ctx4, old_key, 1).ref
    assert core4.source(ctx4, legacy_ref, 1).suppressed
    assert old_key not in ni.load_state(state4)["notes"][note_id(p4)]["chunks"]

    print("ALL_NOTE_LIFECYCLE_CHECKS_PASSED")
    print(ni.json.dumps({"s1": s1, "s2": s2, "s3": s3, "s4": s4, "s5": s5, "s6": s6,
                         "s7": s7, "s8": s8, "s9": s9}, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
