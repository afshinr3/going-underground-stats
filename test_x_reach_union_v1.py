#!/usr/bin/env python3
"""TEST_X_REACH_UNION_V1 — prevent the Barnes X undercount from recurring.

The defect (2026-08-02)
-----------------------
The dashboard showed Barnes X reach = 86.4K against a true figure of 1,322,598
across 13 unique tweets. Two compounding causes, both structural:

  1. The X search term was the episode's guest STRING, which carries a role
     prefix -- "Trump's Ex-Lawyer Robert Barnes" -- and it was searched as an
     EXACT PHRASE. Only 1 of 13 Barnes tweets contained that literal.
  2. A surname search existed but was gated on `total == 0`. Because the phrase
     query returned something non-zero, the broad search NEVER RAN.

Cause 2 is the important one, and it generalises far past this episode: a gate
on "did we find anything" cannot detect "did we find everything". Any single
matching post suppresses the query that would have found the other twelve, so
the failure is silent and scales with how viral the episode was -- the better
the episode performed, the more reach was thrown away.

These tests assert the union semantics that fix it, using the REAL tweet ids and
view counts recorded for the Barnes episode.
"""
import asyncio, os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

FAIL, N = [], 0


def check(name, ok, detail=""):
    global N
    N += 1
    print("  %-62s %s %s" % (name, "PASS" if ok else "FAIL", detail))
    if not ok:
        FAIL.append(name)


# Real data: (tweet_id, views) for the 2026-08-01 Barnes episode, deduped max.
BARNES = [
    ("2083563023491563849", 796802), ("2083362453744722327", 235691),
    ("2083537273233059908", 102264), ("2083364857336991868", 45247),
    ("2083508243901645276", 37017),  ("2083481437731537056", 28003),
    ("2083498211939897851", 22279),  ("2083650663419187574", 18790),
    ("2083617574236069920", 15923),  ("2083151356773409082", 14628),
    ("2083152913472569660", 4717),   ("2083155640843907104", 797),
    ("2083364908318757313", 440),
]
TRUE_TOTAL = sum(v for _, v in BARNES)
# only this one reproduces the role-prefixed phrase verbatim
EXACT_PHRASE_HITS = [("2083362453744722327", 235691)]


def simulate(term_results, union):
    """Reproduce the production accumulation. union=False is the OLD zero-gated
    behaviour; union=True is X_REACH_UNION_V1."""
    ids = {}
    for results in term_results:
        if not union and ids:            # OLD: later terms skipped once anything found
            break
        for tid, v in results:
            if v > ids.get(tid, 0):
                ids[tid] = v
    return sum(ids.values()), len(ids)


def main():
    print("[TEST_X_REACH_UNION_V1]")
    print("\nT1  the OLD zero-gated logic reproduces the observed undercount")
    old_total, old_n = simulate([EXACT_PHRASE_HITS, BARNES], union=False)
    check("old logic stops after the phrase query", old_n == 1, "n=%d" % old_n)
    check("old logic loses the majority of reach",
          old_total < TRUE_TOTAL * 0.25, "%s of %s" % (f"{old_total:,}", f"{TRUE_TOTAL:,}"))

    print("\nT2  the UNION recovers the full reach")
    new_total, new_n = simulate([EXACT_PHRASE_HITS, BARNES], union=True)
    check("union finds every tweet", new_n == len(BARNES), "n=%d" % new_n)
    check("union total equals the true total", new_total == TRUE_TOTAL, f"{new_total:,}")
    check("union exceeds 1,000,000 for this episode", new_total > 1_000_000)
    check("recovered reach vs old", new_total >= old_total * 5,
          "%s -> %s" % (f"{old_total:,}", f"{new_total:,}"))

    print("\nT3  a tweet matching BOTH terms is counted ONCE (no double count)")
    dup_total, dup_n = simulate([EXACT_PHRASE_HITS, BARNES], union=True)
    check("overlapping match not double counted", dup_total == TRUE_TOTAL,
          "phrase hit also present in surname results")

    print("\nT4  max-views-per-id wins across repeated scrapes")
    snap_a = [("X1", 100), ("X2", 50)]
    snap_b = [("X1", 120), ("X2", 40)]
    t, n = simulate([snap_a, snap_b], union=True)
    # max per id: X1 -> max(100,120)=120, X2 -> max(50,40)=50, total 170.
    # (An earlier version of this test asserted 160, adding 120+40 -- the test
    # was wrong, not the code. Recorded because a wrong expectation here would
    # have been "fixed" by breaking the dedup that keeps reach accurate.)
    check("later-higher view count replaces earlier, per id",
          t == 170 and n == 2, "t=%d (120 + 50)" % t)

    print("\nT5  the production source still carries the union fix")
    src = open(os.path.join(HERE, "fetch_and_push.py"), errors="ignore").read()
    check("X_REACH_UNION_V1 marker present", "X_REACH_UNION_V1_20260802" in src)
    check("_return_ids accumulator exists", "_return_ids" in src)
    check("the zero-gated fallback is GONE",
          "if total == 0 and surname and len(surname) > 3:" not in src,
          "the `total == 0` gate was the defect")
    check("both full_name and surname are searched unconditionally",
          "_accum(full_name)" in src and "_accum(surname)" in src)

    print("\n%d assertions, %d failed" % (N, len(FAIL)))
    if FAIL:
        print("FAILED: " + ", ".join(FAIL))
        sys.exit(1)
    print("X REACH UNION HOLDS — a viral episode can no longer be truncated to "
          "its one canonical post.")


if __name__ == "__main__":
    main()
