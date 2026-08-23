#!/usr/bin/env python3
"""DERIVED_FEED_SIGNATURE_PARITY_V1_2026_08_23 — the publish decision and the
consistency check must ask the same question.

THE REGRESSION THIS GUARDS
--------------------------
Two code paths judged a weekly feed and disagreed:

  * m2_rumble_to_upstream_v1._weekly_entries_sig() decided whether a regenerated
    stats_1week_*.json was worth committing. It signed ONLY the in-window
    (surname, date) roster.
  * verify_derived_feeds_v1.compare_feed() decided whether the published feed
    still matched its canonical source. It compares everything except
    generated_at.

So a regenerated feed whose METRICS had moved but whose roster had not signed
IDENTICAL to the bridge. The bridge ran `git checkout --` on the fresh file,
kept the stale committed one, and committed the updated videos.json alongside
it. main then served a leaderboard that disagreed with its own source, and the
checker reported the divergence the bridge had just created:

    [DERIVED_FEED_MISMATCH] type=field id=stats_1week_gu.json:Eu0Phb99ipg field=_x_status
    [DERIVED_FEED_MISMATCH] type=field id=stats_1week_gu.json:Eu0Phb99ipg field=yt_views

canonical _x_status='UNMEASURED_NO_POSTS_FOUND' yt_views='1.4K'
published  _x_status='MEASURED'                 yt_views='1.3K'

The roster is not the content. Both paths now derive their verdict from
verify_derived_feeds_v1.feed_signature(), so "is this worth publishing" and
"does this match its source" are one question asked once.

ON RECORD ORDER
---------------
Entry order is treated as SEMANTIC by both paths, unchanged from the shipped
checker: _generate_weekly_stats() emits entries in canonical-source order with
no sort of its own, so a different order proves the feed did not come from the
current generator over the current source. What must NOT create a false
difference is SERIALISATION order — JSON key order, indentation, unicode
escaping — and that is asserted below.
"""

import datetime as _dt
import json
import os
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


def old_roster_sig(payload):
    """The pre-fix predicate, kept only to prove the defect was real."""
    return sorted((str(e.get("surname")), str(e.get("date")))
                  for e in (payload.get("entries") or []))


def rec(vid, surname, date, **extra):
    e = {"canonical_video_id": vid, "surname": surname, "date": date, "show": "GU"}
    e.update(extra)
    return e


def payload(entries, window=("2026-08-21", "2026-08-24"), **over):
    p = {
        "show": "GU",
        "window": "fri_to_mon_broadcast_week",
        "window_start": window[0], "window_end": window[1],
        "window_start_mon": window[0], "window_end_sun": window[1],
        "generated_at": "2026-08-23T13:00:54.607310Z",
        "n": len(entries), "entries": entries,
        "source_feed": "videos.json",
        "rejected_count": 0, "rejected_sample": [],
        "_marker": "GU_WEEKLY_STATS_FRI_MON_V3_2026_07_17",
    }
    p.update(over)
    return p


def findings_for(expected, live, canonical_ids=None, today=TODAY):
    if canonical_ids is None:
        canonical_ids = {V.record_id(e) for e in (expected.get("entries") or [])}
    f, r = V.compare_feed("stats_1week_gu.json", expected, live, canonical_ids, today, 45)
    return f, r


def agree(expected, live, canonical_ids=None, today=TODAY):
    """Both paths must reach the same verdict."""
    f, _ = findings_for(expected, live, canonical_ids, today)
    sig_differs = V.feed_signature(expected) != V.feed_signature(live)
    return (len(f) > 0), sig_differs


print(__doc__.strip().splitlines()[0])

# --- the real fixture, values exactly as observed on main -------------------
CANON_OK = rec("Eu0Phb99ipg", "O’Hanlon", "22 Aug",
               _x_status="UNMEASURED_NO_POSTS_FOUND", yt_views="1.4K", x_views="49.0K")
PUBLISHED_STALE = rec("Eu0Phb99ipg", "O’Hanlon", "22 Aug",
                      _x_status="MEASURED", yt_views="1.3K", x_views="49.0K")
CORRECT = payload([CANON_OK])
STALE = payload([PUBLISHED_STALE])

print("\n[1] the pre-fix divergent fixture")
check("old roster-only signature saw NO difference (the defect)",
      old_roster_sig(CORRECT) == old_roster_sig(STALE),
      "premise wrong: rosters already differed")
flagged, sig_differs = agree(CORRECT, STALE)
check("checker flags the stale feed", flagged)
check("new signature sees the difference", sig_differs)
check("both paths agree on the divergent fixture", flagged == sig_differs)
f, _ = findings_for(CORRECT, STALE)
check("divergence is reported per field",
      sorted(x["field"] for x in f) == ["_x_status", "yt_views"],
      f"got {[x['field'] for x in f]}")

print("\n[2] the corrected feed passes")
flagged, sig_differs = agree(CORRECT, CORRECT)
check("checker reports no findings", not flagged)
check("signatures match", not sig_differs)

print("\n[3] reordered equivalent records pass (serialisation is not content)")
reserialised = json.loads(json.dumps(CORRECT, sort_keys=True))
shuffled_keys = dict(reversed(list(CORRECT.items())))
shuffled_keys["entries"] = [dict(reversed(list(CORRECT["entries"][0].items())))]
check("key order does not change the signature",
      V.feed_signature(CORRECT) == V.feed_signature(shuffled_keys))
check("re-serialisation does not change the signature",
      V.feed_signature(CORRECT) == V.feed_signature(reserialised))
check("indent / ensure_ascii do not change the signature",
      V.feed_signature(json.loads(json.dumps(CORRECT, indent=4, ensure_ascii=True)))
      == V.feed_signature(CORRECT))
flagged, sig_differs = agree(CORRECT, shuffled_keys)
check("checker also sees key-reordered payload as equal", not flagged and not sig_differs)

print("\n[4] record reorder is content — and both paths say so together")
A = rec("vidA", "Alpha", "21 Aug")
B = rec("vidB", "Bravo", "22 Aug")
flagged, sig_differs = agree(payload([A, B]), payload([B, A]))
check("record reorder detected by the checker", flagged)
check("record reorder detected by the signature", sig_differs)
check("both paths agree on record reorder", flagged == sig_differs)

print("\n[5] missing / extra / duplicated / field-changed all fail, in both paths")
cases = {
    "missing":   (payload([A, B]), payload([A])),
    "extra":     (payload([A]),    payload([A, B])),
    "duplicate": (payload([A, B]), payload([A, B, B])),
    "changed":   (payload([A, B]), payload([A, dict(B, x_views="9.9M")])),
}
for name, (exp, live) in cases.items():
    flagged, sig_differs = agree(exp, live)
    check(f"{name}: checker fails", flagged)
    check(f"{name}: signature differs", sig_differs)
    check(f"{name}: both paths agree", flagged == sig_differs)

print("\n[6] an empty week cannot accidentally equal a populated week")
EMPTY = payload([])
check("empty vs populated signatures differ",
      V.feed_signature(EMPTY) != V.feed_signature(payload([A])))
flagged, sig_differs = agree(payload([A]), EMPTY)
check("checker flags the emptied feed", flagged)
check("both paths agree on empty vs populated", flagged == sig_differs)
check("empty week does not equal a DIFFERENT empty week (window is signed)",
      V.feed_signature(EMPTY)
      != V.feed_signature(payload([], window=("2026-08-14", "2026-08-17"))))
check("n is part of the signature",
      V.feed_signature(payload([A]))
      != V.feed_signature(payload([A], n=99)))

print("\n[7] weeks spanning month and year boundaries")
DEC = payload([rec("vidD", "Dec", "27 Dec"), rec("vidE", "Dec2", "28 Dec")],
              window=("2025-12-26", "2025-12-29"))
JAN = payload([rec("vidD", "Dec", "27 Dec"), rec("vidE", "Dec2", "28 Dec")],
              window=("2026-12-25", "2026-12-28"))
check("same records in different YEARS sign differently",
      V.feed_signature(DEC) != V.feed_signature(JAN))
flagged, sig_differs = agree(DEC, JAN)
check("checker flags the year difference", flagged)
check("both paths agree across the year boundary", flagged == sig_differs)

MONTH_SPAN = payload([rec("vidJ", "Jan", "31 Jan"), rec("vidF", "Feb", "1 Feb")],
                     window=("2026-01-30", "2026-02-02"))
check("a week spanning a month end signs stably",
      V.feed_signature(MONTH_SPAN) == V.feed_signature(json.loads(json.dumps(MONTH_SPAN))))
flagged, _ = agree(MONTH_SPAN, MONTH_SPAN)
check("month-spanning week compares equal to itself", not flagged)

# Year inference across the new year: on 5 Jan 2026, "28 Dec" is 2025, not 2026.
JAN5 = _dt.date(2026, 1, 5)
check("date in December resolves to the PREVIOUS year when read in January",
      V.parse_feed_date("28 Dec", JAN5) == _dt.date(2025, 12, 28),
      f"got {V.parse_feed_date('28 Dec', JAN5)}")
check("age across the year boundary is 8 days, not 363",
      V.age_days({"date": "28 Dec"}, JAN5) == 8,
      f"got {V.age_days({'date': '28 Dec'}, JAN5)}")
check("29 Feb on a non-leap year is rejected, not coerced",
      V.parse_feed_date("29 Feb", _dt.date(2026, 3, 5)) is None)

print("\n[8] the 45-day retention boundary, including across the year boundary")


def boundary(age, today=TODAY):
    d = today - _dt.timedelta(days=age)
    old = rec("vidOLD", "Old", f"{d.day} {d.strftime('%b')}")
    exp = payload([A])
    live = payload([A, old])
    f, r = V.compare_feed("stats_1week_gu.json", exp, live,
                          {V.record_id(A)}, today, 45)
    return f, r


f, r = boundary(44)
check("44 days -> mismatch (inside retention)", any(x["type"] == "extra" for x in f) and r == [])
f, r = boundary(45)
check("45 days -> mismatch (boundary: still retained)", any(x["type"] == "extra" for x in f) and r == [])
f, r = boundary(46)
check("46 days -> intentional retirement, no mismatch",
      not any(x["type"] == "extra" for x in f) and r == ["vidOLD"])

# same boundary, evaluated in January so the aged record sits in the prior year
JAN_TODAY = _dt.date(2026, 1, 10)
f, r = boundary(45, JAN_TODAY)
check("45 days across the year boundary -> still a mismatch",
      any(x["type"] == "extra" for x in f) and r == [], f"findings={f} retired={r}")
f, r = boundary(46, JAN_TODAY)
check("46 days across the year boundary -> still an intentional retirement",
      not any(x["type"] == "extra" for x in f) and r == ["vidOLD"], f"findings={f} retired={r}")

print("\n[9] documented exception: retention excusal is the ONE place the paths differ")
d = TODAY - _dt.timedelta(days=46)
aged = rec("vidOLD", "Old", f"{d.day} {d.strftime('%b')}")
exp, live = payload([A]), payload([A, aged])
f, r = V.compare_feed("stats_1week_gu.json", exp, live, {V.record_id(A)}, TODAY, 45)
check("the aged-out record itself is NOT reported as extra",
      not any(x["type"] == "extra" for x in f), f"got {f}")
check("it is recorded as an intentional retirement instead", r == ["vidOLD"], f"got {r}")
# The retention excusal suppresses the RECORD-level finding only. The feed's own
# n still disagrees with what the generator would produce, and that is reported
# — correctly: a feed still carrying a record 46 days stale is out of date, even
# though the record's absence from canonical is policy rather than loss.
check("the count divergence is still reported",
      [x["field"] for x in f if x["type"] == "field"] == ["n"], f"got {f}")
check("signature still differs, so the bridge republishes the tidier feed",
      V.feed_signature(exp) != V.feed_signature(live))
# This asymmetry is safe and deliberate: publishing the regenerated feed (which
# has dropped the aged record) is never worse than keeping the stale one.

print("\n[10] the bridge actually uses the shared definition")
import m2_rumble_to_upstream_v1 as BR
tmp = tempfile.mkdtemp()
try:
    p1 = os.path.join(tmp, "a.json")
    with open(p1, "w") as fh:
        json.dump(CORRECT, fh)
    check("bridge signature == checker signature for the same file",
          BR._weekly_entries_sig(p1) == V.feed_signature(p1))
    p2 = os.path.join(tmp, "b.json")
    with open(p2, "w") as fh:
        json.dump(STALE, fh)
    check("bridge distinguishes the stale feed it used to discard",
          BR._weekly_entries_sig(p1) != BR._weekly_entries_sig(p2))
    check("bridge returns None (unknown) for an unreadable feed",
          BR._weekly_entries_sig(os.path.join(tmp, "missing.json")) is None)
    check("bridge only ignores generated_at",
          BR._weekly_entries_sig(p1)
          == V.feed_signature(dict(CORRECT, generated_at="2099-01-01T00:00:00Z")))
finally:
    import shutil
    shutil.rmtree(tmp, ignore_errors=True)

print()
if FAILS:
    print(f"FAILED ({len(FAILS)}): " + ", ".join(FAILS))
    sys.exit(1)
print("All weekly-signature parity tests passed.")
