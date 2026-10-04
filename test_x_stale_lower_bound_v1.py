#!/usr/bin/env python3
"""X_STALE_IS_A_LOWER_BOUND_V1 — a carried-forward X figure must not render as current.

WHY THIS TEST EXISTS
--------------------
The X fetch in fetch_and_push.py has three outcomes. On success it writes `x_views` AND
stamps `_x_status='MEASURED'`. On no-posts it sets UNMEASURED_NO_POSTS_FOUND. On an
exception it sets FETCH_FAILED — and its own comment says:

    "A failed retrieval is the clearest possible unknown. It must never be able to reach
     the display as a number."

But neither failure branch clears `x_views`, and `sum_known_metrics` only asks whether the
field PARSES — it never reads `_x_status`. So a figure the run did not measure counted into
the episode total AND suppressed the "+" lower-bound marker, rendering identically to a
freshly measured one. On 2026-10-04 every one of the 13 New Order rows carried
`FETCH_FAILED:Exception` beside a real number; the display was correct only because the
hourly store-attribution bridge happened to refresh the values independently. The honesty
guard was doing nothing and nothing would have said so.

`_x_measured_iso` records when a figure was last genuinely measured, and
`stale_metric_fields` reports one measured too long ago. Views only grow, so a
carried-forward figure is a LOWER BOUND — it earns the same "+" an absent platform does.

THE TWO RULES THAT STOP THIS BECOMING NOISE
  1. A MISSING or unparseable stamp is NOT stale. Every historical row predates the stamp,
     so treating absence as staleness would mark all 33 rows at once, and an alarm that
     always fires is one nobody reads.
  2. Staleness NEVER changes a total. The value is real; only its confidence is reported.
"""
import datetime as d
import importlib.util
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
NOW = d.datetime(2026, 10, 5, 0, 0, 0, tzinfo=d.timezone.utc)
_results = []


def check(name, ok, detail=""):
    _results.append((name, bool(ok), detail))
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + (f"  — {detail}" if detail else ""))


def _mod():
    spec = importlib.util.spec_from_file_location("fap", os.path.join(HERE, "fetch_and_push.py"))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def main():
    m = _mod()
    skm = m.sum_known_metrics
    smf = getattr(m, "stale_metric_fields", None)
    if smf is None:
        # A clean finding, not a traceback: on a checkout predating
        # X_STALE_IS_A_LOWER_BOUND_V1 the guard simply does not exist, and that IS the
        # defect this test was written for.
        check("stale_metric_fields exists", False,
              "absent: a carried-forward X figure renders as a current measurement")
        print("\n  0 passed, 1 failed")
        return 1

    print("\n1  A MEASURED-RECENTLY FIGURE IS NOT FLAGGED")
    check("fresh stamp is current",
          smf({"x_views": "13.7K", "_x_measured_iso": "2026-10-04T23:40:00Z"}, now=NOW) == [],
          "20 minutes old")

    print("\n2  A CARRIED-FORWARD FIGURE IS FLAGGED AS A LOWER BOUND")
    check("stamp older than the window is stale",
          smf({"x_views": "13.7K", "_x_measured_iso": "2026-10-04T17:00:00Z"}, now=NOW)
          == ["x_views"], f"7h old, window is {m.X_STALE_AFTER_H}h")
    check("a days-old plateaued figure is stale",
          smf({"x_views": "1.3M", "_x_measured_iso": "2026-10-02T00:00:00Z"}, now=NOW)
          == ["x_views"])

    print("\n3  UNKNOWN AGE SAYS NOTHING — AN ALARM THAT ALWAYS FIRES IS ONE NOBODY READS")
    check("no stamp is NOT stale", smf({"x_views": "13.7K"}, now=NOW) == [],
          "every historical row predates the stamp")
    check("unparseable stamp is NOT stale",
          smf({"x_views": "13.7K", "_x_measured_iso": "garbage"}, now=NOW) == [],
          "never guess an age")
    _flagged, _rows = 0, 0
    for _f in ("videos.json", "videos_neworder.json"):
        _d = json.load(open(os.path.join(HERE, _f)))
        for _r in (_d if isinstance(_d, list) else _d.get("videos", _d)):
            _rows += 1
            if smf(_r, now=d.datetime.now(d.timezone.utc)):
                _flagged += 1
    check("unstamped live rows are not retroactively marked",
          _flagged == 0 and _rows > 0,
          f"{_flagged} of {_rows} live rows flagged; stamps accumulate from the next run")

    print("\n4  AN ALREADY-UNKNOWN FIELD IS NOT DOUBLE-REPORTED")
    check("sum_known_metrics owns the unknown case",
          smf({"x_views": "?", "_x_measured_iso": "2026-10-01T00:00:00Z"}, now=NOW) == [])

    print("\n5  STALENESS NEVER CHANGES A TOTAL")
    row = {"rumble_views": "813", "x_views": "13.7K", "yt_views": "9.3K", "ig_likes": "3.5K",
           "_x_measured_iso": "2026-10-02T00:00:00Z"}
    total, unknown = skm(row)
    check("total is identical with a stale stamp present",
          total == 813 + 13700 + 9300 + 3500 and unknown == [],
          f"total={total}, unknown={unknown}")

    print("\n6  A FAILED FETCH RECORDS WHY, NOT JUST ITS TYPE")
    src = open(os.path.join(HERE, "fetch_and_push.py")).read()
    check("the exception message reaches _x_status",
          "_emsg" in src and "FETCH_FAILED:{type(e).__name__}" in src,
          "'FETCH_FAILED:Exception' alone named no cause and became the dashboard tooltip")
    check("success stamps _x_measured_iso", "_x_measured_iso" in src)

    failed = [n for n, ok, _ in _results if not ok]
    print(f"\n  {len(_results) - len(failed)} passed, {len(failed)} failed")
    if failed:
        print("\n  A CARRIED-FORWARD X FIGURE CAN RENDER AS A CURRENT MEASUREMENT:")
        for n in failed:
            print(f"    - {n}")
        return 1
    print("\nALL CHECKS PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
