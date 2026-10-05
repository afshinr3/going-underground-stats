#!/usr/bin/env python3
"""EPISODE_NOT_DUPLICATED_V1 — one air date, one guest, one row.

WHY THIS EXISTS
---------------
`canonical_episode_id` is derived from the TITLE. Edit a published title and the next run
mints a SECOND identity for the same episode, so the feed carries it twice. Nothing
detected that: the ids differ, the video ids differ (one is null), and every existing
identity guard asks whether an episode DISAPPEARED, never whether it arrived twice.

Found on 2026-10-04 in videos.json, and it is still there:

    index 0   a5df3acf1214  3 Oct  HOH  "...NUKES Being Used Against Iran..."
              video doTi3weQTaM, yt_views 2.6K, pub_iso 2026-10-03T11:55:31Z
    index 19  443de1f604d7  3 Oct  HOH  "...NUCLEAR WEAPONS Being Used Against Iran..."
              no video bound, no yt_views, pub_iso 2026-10-03T01:00:29Z

Title similarity 0.939; identical rumble_views (2.5K) and x_views (167.3K). One episode,
counted twice — so the leaderboard renders Hoh twice and any feed-wide sum double-counts
about 170K of reach. The dashboard already carries a comment about New Order episodes
rendering twice for a different reason; this is the same harm by a different route.

`test_yt_attribution_v1` does fail on this, but on a SYMPTOM — three null metrics on the
row that never got its YouTube binding — which is why it read as a measurement problem for
however long it has been red. This guard names the cause.

THE REPAIR IS NOT AUTOMATED HERE, DELIBERATELY. Choosing which row survives is an
editorial call with published consequences: index 0 holds the YouTube binding and
yt_views, index 19 holds the earlier pub_iso and the X store-attribution marker. Merging
means choosing a pub_iso and an id. And videos.json is live data guarded by
test_episode_never_disappears_v1 and test_cleanup_never_deletes_bound_episode_v1 —
deleting a row by hand is exactly what those exist to prevent. So this reports, precisely,
and leaves the decision where it belongs.

A collision on (show, air date, surname) is the key, validated against the live feeds:
exactly one collision in 33 rows, and it is this one. Two genuinely distinct episodes with
the same guest on the same date for the same show would be a data-model change, and should
break this test so the decision is explicit rather than silent.
"""
import json
import os
import sys
from collections import defaultdict

ROOT = os.path.dirname(os.path.abspath(__file__))
FEEDS = ("videos.json", "videos_neworder.json")
_results = []


def check(name, ok, detail=""):
    _results.append((name, bool(ok), detail))
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + (f"  — {detail}" if detail else ""))


def _rows(fn):
    d = json.load(open(os.path.join(ROOT, fn)))
    return d if isinstance(d, list) else d.get("videos", d)


def _key(r):
    return (r.get("show"), str(r.get("date")),
            (r.get("canonical_surname_upper") or r.get("surname") or "").upper())


def main():
    print("\n1  NO AIR DATE CARRIES THE SAME GUEST TWICE")
    total = 0
    for fn in FEEDS:
        rows = _rows(fn)
        total += len(rows)
        groups = defaultdict(list)
        for i, r in enumerate(rows):
            groups[_key(r)].append((i, r))
        dupes = {k: v for k, v in groups.items() if len(v) > 1}
        if dupes:
            lines = []
            for (show, date, surname), members in dupes.items():
                for i, r in members:
                    lines.append(
                        f"{show} {date} {surname} idx={i} "
                        f"id={r.get('canonical_episode_id')} "
                        f"video={r.get('canonical_video_id') or 'UNBOUND'} "
                        f"pub={r.get('pub_iso')}")
            check(f"{fn} has no duplicate episode", False, "; ".join(lines))
        else:
            check(f"{fn} has no duplicate episode", True, f"{len(rows)} rows")

    print("\n2  IDENTITY FIELDS ARE STILL UNIQUE WHERE THEY CLAIM TO BE")
    for field in ("canonical_episode_id", "canonical_video_id"):
        seen = defaultdict(list)
        for fn in FEEDS:
            for r in _rows(fn):
                if r.get(field):
                    seen[r[field]].append(fn)
        clash = {k: v for k, v in seen.items() if len(v) > 1}
        check(f"{field} is unique across both feeds", not clash,
              "none repeated" if not clash else str(list(clash)[:3]))

    failed = [n for n, ok, _ in _results if not ok]
    print(f"\n  {len(_results) - len(failed)} passed, {len(failed)} failed  ({total} rows checked)")
    if failed:
        print("\n  AN EPISODE IS IN THE FEED TWICE — the leaderboard renders it twice and")
        print("  every feed-wide sum double-counts its reach:")
        for n in failed:
            print(f"    - {n}")
        print("\n  A title edit mints a new canonical_episode_id. Repair is an editorial")
        print("  choice (which pub_iso, which id, which row keeps the video binding), so")
        print("  this guard reports rather than deletes.")
        return 1
    print("\nALL CHECKS PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
