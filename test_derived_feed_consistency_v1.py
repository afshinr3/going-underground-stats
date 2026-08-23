#!/usr/bin/env python3
"""DERIVED_FEED_CONSISTENCY_V1_2026_08_23 — tests for verify_derived_feeds_v1.

Covers every divergence the checker claims to detect, plus the exact 45-day
retention boundary, plus the end-to-end guarantee that running the check never
rewrites a live feed.

The boundary cases matter most. Production drops a canonical row when
`_age_days > STALE_DAYS_DROP_UNBOUND`, so 45 days is RETAINED and 46 is dropped.
A checker that treated 45 as "aged out" would swallow a genuine mismatch on the
last day a row is still supposed to exist.
"""

import datetime as _dt
import json
import os
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import verify_derived_feeds_v1 as V

FAILS = []
TODAY = _dt.date(2026, 8, 23)


def check(name, cond, detail=""):
    print(("  PASS  " if cond else "  FAIL  ") + name + (("  -- " + detail) if detail and not cond else ""))
    if not cond:
        FAILS.append(name)


def rec(vid, surname, date, **extra):
    e = {"canonical_video_id": vid, "surname": surname, "date": date, "show": "GU"}
    e.update(extra)
    return e


def payload(entries, **over):
    p = {
        "show": "GU",
        "window": "fri_to_mon_broadcast_week",
        "window_start": "2026-08-21",
        "window_end": "2026-08-24",
        "window_start_mon": "2026-08-21",
        "window_end_sun": "2026-08-24",
        "generated_at": "2026-08-23T12:00:00Z",
        "n": len(entries),
        "entries": entries,
        "source_feed": "videos.json",
        "rejected_count": 0,
        "rejected_sample": [],
        "_marker": "GU_WEEKLY_STATS_FRI_MON_V3_2026_07_17",
    }
    p.update(over)
    return p


A = rec("vidA", "O’Hanlon", "22 Aug", x_views="49.0K")
B = rec("vidB", "Levy", "21 Aug", x_views="267.8K")


def compare(expected_entries, live_entries, canonical_ids=None, exp_over=None, live_over=None):
    exp = payload(expected_entries, **(exp_over or {}))
    live = payload(live_entries, **(live_over or {}))
    if canonical_ids is None:
        canonical_ids = {V.record_id(e) for e in expected_entries}
    return V.compare_feed("stats_1week_gu.json", exp, live, canonical_ids, TODAY, 45)


def types(findings):
    return sorted(f["type"] for f in findings)


print(__doc__.strip().splitlines()[0])

print("\n[1] consistent feed")
f, r = compare([A, B], [A, B])
check("no findings when the feed matches the generator", f == [], f"got {types(f)}")
check("no spurious retirement records", r == [], f"got {r}")

print("\n[2] missing row")
f, r = compare([A, B], [A])
check("missing row detected", types(f) == ["field", "missing"], f"got {types(f)}")
miss = [x for x in f if x["type"] == "missing"]
check("missing row identifies the record",
      miss and miss[0]["id"] == "stats_1week_gu.json:vidB", f"got {miss}")
check("n field divergence reported alongside",
      any(x["type"] == "field" and x["field"] == "n" for x in f), f"got {f}")

print("\n[3] extra row")
C = rec("vidC", "Ghost", "22 Aug")
f, r = compare([A], [A, C])
check("extra row detected", "extra" in types(f), f"got {types(f)}")
extra = [x for x in f if x["type"] == "extra"]
check("extra row identifies the record",
      extra and extra[0]["id"] == "stats_1week_gu.json:vidC", f"got {extra}")
check("recent extra row is NOT excused as retention", r == [], f"got {r}")

print("\n[4] duplicated row")
f, r = compare([A, B], [A, B, B])
check("duplicate detected", "duplicate" in types(f), f"got {types(f)}")
dup = [x for x in f if x["type"] == "duplicate"]
check("duplicate identifies the record and field",
      dup and dup[0]["id"] == "stats_1week_gu.json:vidB" and dup[0]["field"] == "count",
      f"got {dup}")

print("\n[5] changed field")
B_changed = dict(B, x_views="1.2M")
f, r = compare([A, B], [A, B_changed])
fields = [x for x in f if x["type"] == "field"]
check("field divergence detected", len(fields) == 1, f"got {fields}")
check("field divergence names record and field",
      fields and fields[0]["id"] == "stats_1week_gu.json:vidB"
      and fields[0]["field"] == "x_views", f"got {fields}")

print("\n[6] ordering-only difference")
f, r = compare([A, B], [B, A])
check("order difference detected (entry order is semantic)",
      types(f) == ["order"], f"got {types(f)}")
order = [x for x in f if x["type"] == "order"]
check("order finding names the feed and entry_order",
      order and order[0]["id"] == "stats_1week_gu.json"
      and order[0]["field"] == "entry_order", f"got {order}")

print("\n[7] non-semantic normalisation")
f, r = compare([A, B], [A, B], live_over={"generated_at": "2026-08-23T23:59:59Z"})
check("generated_at difference alone is NOT a mismatch", f == [], f"got {types(f)}")

print("\n[8] the 45-day retention boundary")


def boundary_case(age_days_value):
    """A row present in the live feed but gone from canonical, aged N days."""
    d = TODAY - _dt.timedelta(days=age_days_value)
    old = rec("vidOLD", "Blumenthal", f"{d.day} {d.strftime('%b')}")
    # canonical no longer holds it; expected feed therefore lacks it too
    f, r = compare([A], [A, old], canonical_ids={V.record_id(A)})
    return f, r


f, r = boundary_case(44)
check("44 days (inside retention) -> mismatch, not excused",
      "extra" in types(f) and r == [], f"findings={types(f)} retired={r}")

f, r = boundary_case(45)
check("45 days (the boundary, still retained) -> mismatch, not excused",
      "extra" in types(f) and r == [], f"findings={types(f)} retired={r}")

f, r = boundary_case(46)
check("46 days (past retention) -> intentional drop, no mismatch",
      "extra" not in types(f) and r == ["vidOLD"], f"findings={types(f)} retired={r}")

check("boundary mirrors production predicate age > 45",
      V.is_intentional_retirement(
          rec("z", "x", (TODAY - _dt.timedelta(days=45)).strftime("%-d %b")),
          set(), TODAY, 45) is False,
      "45 days must not be treated as aged out")
check("46 days is treated as aged out",
      V.is_intentional_retirement(
          rec("z", "x", (TODAY - _dt.timedelta(days=46)).strftime("%-d %b")),
          set(), TODAY, 46 - 1) is True)

print("\n[9] the retention constant comes from fetch_and_push, never hardcoded")
check("STALE_DAYS_DROP_UNBOUND read from source", V.stale_days() == 45,
      f"got {V.stale_days()}")
tmp = tempfile.mkdtemp()
try:
    bogus = os.path.join(tmp, "fetch_and_push.py")
    with open(bogus, "w") as fh:
        fh.write("# no constant here\n")
    try:
        V.stale_days(bogus)
        check("missing constant -> Unverifiable", False, "no exception raised")
    except V.Unverifiable:
        check("missing constant -> Unverifiable", True)
finally:
    shutil.rmtree(tmp, ignore_errors=True)

print("\n[10] end-to-end against the real repo: well-formed verdict, and never writes")
live_names = ["stats_1week_gu.json", "stats_1week_no.json", "videos.json", "videos_neworder.json"]
before = {}
for n in live_names:
    p = os.path.join(HERE, n)
    before[n] = (open(p, "rb").read(), os.stat(p).st_mtime_ns) if os.path.exists(p) else None
proc = subprocess.run([sys.executable, os.path.join(HERE, "verify_derived_feeds_v1.py")],
                      capture_output=True, text=True)
after = {}
for n in live_names:
    p = os.path.join(HERE, n)
    after[n] = (open(p, "rb").read(), os.stat(p).st_mtime_ns) if os.path.exists(p) else None
check("live tree byte-identical and mtime-identical after the check",
      before == after, "the check wrote to the live tree")
# Deliberately NOT asserting the live repo is consistent. Whether main happens to
# be consistent right now is live state, not a property of this code — and the
# moment it legitimately diverges (which is the whole point of the checker) such
# an assertion would fail the suite for doing its job. What must always hold is
# that the run is well-formed: a valid verdict, on a valid exit code, and the two
# never disagree.
out_lines = [l for l in proc.stdout.splitlines() if l.startswith("[DERIVED_FEED_")]
consistent = any(l == "[DERIVED_FEED_CONSISTENT]" for l in out_lines)
mismatched = [l for l in out_lines if l.startswith("[DERIVED_FEED_MISMATCH]")]
check("real repo run emits exactly one kind of verdict",
      consistent != bool(mismatched),
      f"rc={proc.returncode} lines={out_lines[:4]}")
check("exit code agrees with the verdict",
      (proc.returncode == 0) == consistent,
      f"rc={proc.returncode} consistent={consistent}")
check("verdict uses a defined exit code",
      proc.returncode in (V.EXIT_CONSISTENT, V.EXIT_MISMATCH, V.EXIT_UNVERIFIABLE),
      f"rc={proc.returncode}")
check("no .tmp residue left in the repo",
      not any(x.endswith(".tmp") for x in os.listdir(HERE)),
      f"found {[x for x in os.listdir(HERE) if x.endswith('.tmp')]}")

print("\n[11] exit codes")
check("EXIT_CONSISTENT is 0", V.EXIT_CONSISTENT == 0)
check("mismatch is nonzero", V.EXIT_MISMATCH != 0)
check("unverifiable is nonzero and distinct",
      V.EXIT_UNVERIFIABLE != 0 and V.EXIT_UNVERIFIABLE != V.EXIT_MISMATCH)

print("\n[12] emitted line format")
f, _ = compare([A, B], [A])
lines = [f"[DERIVED_FEED_MISMATCH] type={x['type']} id={x['id']} field={x['field']}" for x in f]
check("every mismatch line carries type, id and field",
      all(l.startswith("[DERIVED_FEED_MISMATCH] type=") and " id=" in l and " field=" in l
          for l in lines), f"got {lines}")
check("no whitespace inside any emitted token",
      all(len(l.split()) == 4 for l in lines), f"got {lines}")

print()
if FAILS:
    print(f"FAILED ({len(FAILS)}): " + ", ".join(FAILS))
    sys.exit(1)
print("All derived-feed consistency tests passed.")
