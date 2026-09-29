#!/usr/bin/env python3
"""FOLLOWERS_FAIL_CLOSED_V1_20260930 — a follower scrape that read nothing must not
publish "zero followers".

THE DEFECT THIS GUARDS
----------------------
followers.json was written only by the GitHub Action, unconditionally:

    followers = await fetch_x_followers([...])          # {} when the scrape fails
    total = sum(followers.values())                     # 0
    out = {"accounts": {h: followers.get(h) for h in ...},   # {null, null, null}
           "total": total, ...}
    json.dump(out, f, indent=2)                         # <-- published as fact

From 2026-09-27T07:25Z every run wrote that over a good file, and the X-GU / X-NO / X-AR
LaMetric apps showed "GU ?", "X TOTAL ?" and "STALE" for two and a half days. The last
real reading, 341,679, was overwritten seventeen times by a scrape that had read nothing.

WHY THE OBVIOUS DIAGNOSIS WAS WRONG
-----------------------------------
"followers: not found" reads exactly like expired cookies, and the repo even has a
refresher that syncs the Actions secret every six hours and reports success each time.
The cookies were never the problem. Measured 2026-09-30 with the same cookie file that
secret is synced from, running the Action's own code:

    from Afshin's Mac       x.com/GUnderground_TV -> "207.2K Followers", no login wall
    from the GitHub runner  same cookies, same code -> "not found", all three handles,
                            and every episode search failed x_search_results_never_rendered

Same session, different network: X blocks the datacenter IP. No secret rotation could fix
it, and the refresher would have gone on reporting success forever. Collection moved to
the Mac (x_followers_local_v1.py); CI is now fail-closed so it cannot answer a question
it is unable to ask.

Run: python3 test_followers_fail_closed_v1.py
"""
from __future__ import annotations

import json
import os
import re
import sys
import tempfile

ROOT = os.path.dirname(os.path.abspath(__file__))
PIPELINE = os.path.join(ROOT, "fetch_and_push.py")
BRIDGE = os.path.join(ROOT, "m2_rumble_to_upstream_v1.py")
COLLECTOR = "/Users/afshin/RumbleMonitor/x_followers_local_v1.py"
FOLLOWERS = os.path.join(ROOT, "followers.json")

FAILS, PASSES = [], []


def check(name, ok, detail=""):
    (PASSES if ok else FAILS).append(name)
    print(f"  {'PASS' if ok else 'FAIL'}  {name}{('  — ' + detail) if detail else ''}")


def main():
    src = open(PIPELINE, encoding="utf-8").read()

    # --- 1. the CI writer must be gated ------------------------------------------
    block = src[src.index('print("\\nFetching X follower counts...")'):]
    block = block[:block.index("# GU_WEEKLY_STATS_V1_2026_07_04")]
    check("ci_writer_is_gated",
          "FOLLOWERS_FAIL_CLOSED_V1" in block and "left untouched" in block,
          "the unconditional json.dump is gone")

    # A partial answer is the dangerous one: it looks valid. The gate must demand ALL.
    check("ci_requires_complete_coverage",
          re.search(r"if\s+len\(followers\)\s*<\s*len\(_f_handles\)", block) is not None,
          "CI writes only when every handle resolved, never a partial set")

    # --- 2. the Mac must own the file ---------------------------------------------
    bridge = open(BRIDGE, encoding="utf-8").read()
    check("bridge_collects_followers",
          "x_followers_local_v1" in bridge and "followers.json" in bridge,
          "the hourly bridge collects and publishes followers.json")

    check("collector_exists", os.path.exists(COLLECTOR), COLLECTOR)

    # --- 3. the collector carries forward rather than nulling ---------------------
    sys.path.insert(0, os.path.dirname(COLLECTOR))
    import x_followers_local_v1 as XFL

    good = {"accounts": {"afshinrattansi": 129293, "GUnderground_TV": 207292,
                         "NewOrder_TV": 5988}, "total": 342573, "updated": "x"}

    with tempfile.TemporaryDirectory() as td:
        path = os.path.join(td, "followers.json")

        # (a) nothing resolves -> the good file must survive byte for byte
        json.dump(good, open(path, "w"), indent=2)
        before = open(path).read()
        XFL.fetch_followers, real = (lambda: {}), XFL.fetch_followers
        changed, _ = XFL.write_followers(path)
        check("total_failure_leaves_file_untouched",
              open(path).read() == before and changed is False,
              "an unreadable source publishes nothing")

        # (b) one handle resolves -> the other two keep their last known values
        XFL.fetch_followers = lambda: {"GUnderground_TV": 207400}
        XFL.write_followers(path)
        got = json.load(open(path))
        check("partial_carries_previous_values",
              got["accounts"]["afshinrattansi"] == 129293
              and got["accounts"]["NewOrder_TV"] == 5988
              and got["accounts"]["GUnderground_TV"] == 207400,
              f"carried_forward={got.get('carried_forward')}")
        check("partial_total_counts_only_known",
              got["total"] == 129293 + 207400 + 5988,
              "no handle is silently treated as zero")
        check("partial_is_labelled",
              got.get("measured") == ["GUnderground_TV"]
              and sorted(got.get("carried_forward") or []) == ["NewOrder_TV", "afshinrattansi"],
              "a consumer can tell a partial answer from a complete one")

        XFL.fetch_followers = real

    # --- 4. the live file must not be a null set ---------------------------------
    live = json.load(open(FOLLOWERS))
    accs = live.get("accounts") or {}
    check("live_file_has_no_null_accounts",
          all(isinstance(v, int) for v in accs.values()),
          f"total={live.get('total'):,} accounts={ {k: v for k, v in accs.items()} }")
    check("live_total_is_not_zero", live.get("total", 0) > 0, str(live.get("total")))

    print(f"\n  {len(PASSES)} passed, {len(FAILS)} failed")
    if FAILS:
        print("\n  FOLLOWER INTEGRITY IS BROKEN — a scrape that read nothing can reach "
              "the display as a real zero:")
        for f in FAILS:
            print(f"    - {f}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
