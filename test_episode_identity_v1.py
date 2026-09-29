#!/usr/bin/env python3
"""EPISODE_IDENTITY_V1_20260930 — the O'Hanlon duplicate, and the display honesty it broke.

THE PRODUCTION FAILURE
----------------------
On 2026-09-30 the live GU feed published 19 rows for 18 real episodes. Michael O'Hanlon,
22 Aug, appeared twice:

    row A  canonical_video_id Eu0Phb99ipg   surname "O'Hanlon"  yt_views 2.2K
    row B  canonical_video_id absent        surname "Hanlon"    yt_views null
    BOTH   canonical_episode_id 6f96fd46bb55   AND   title hash 5544d97469b0

Both writers collapsed on an ordered precedence CHAIN, so row A keyed on its video id and
row B — which had none — fell through to the title hash. The rows agreed on TWO identifiers
and the chain consulted neither, because a stronger key existed on one side only. The guest
appeared twice in the LaMetric rotation, once with a total missing YouTube, and every
aggregate over that date counted its X, Rumble and IG figures twice.

WHY THE SUITE WAS GREEN
-----------------------
`ANY_SHARED_IDENTITY_MEANS_SAME_EPISODE_V1_20260817` had already diagnosed this precise
failure six weeks earlier — and fixed it only for the cloud's carry-forward test. Both
collapses stayed on the chain. Meanwhile test_episode_identity_regression_v1.py defined its
OWN collapse whose chain led with canonical_episode_id, so it merged this pair and passed
while the shipping code did not. A rule implemented once per caller is not a rule, and a
test that reimplements the logic tests itself.

Run: python3 test_episode_identity_v1.py
"""
from __future__ import annotations

import json
import os
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)
import episode_identity_v1 as EI

FAILS, PASSES = [], []


def check(name, ok, detail=""):
    (PASSES if ok else FAILS).append(name)
    print(f"  {'PASS' if ok else 'FAIL'}  {name}{('  — ' + detail) if detail else ''}")


# The two real rows, reduced to the fields that decide identity.
ROW_A = {"date": "22 Aug", "guest": "Michael O’Hanlon", "surname": "O’Hanlon",
         "title": "Afshin Rattansi CHALLENGES Ex-CIA Advisor on the Legacy of America’s Wars",
         "canonical_video_id": "Eu0Phb99ipg", "canonical_episode_id": "6f96fd46bb55",
         "yt_views": "2.2K", "x_views": "112.5K", "rumble_views": "3.3K", "ig_likes": "760"}
ROW_B = {"date": "22 Aug", "guest": "Michael O’Hanlon", "surname": "Hanlon",
         "title": "Afshin Rattansi CHALLENGES Ex-CIA Advisor on the Legacy of America’s Wars",
         "canonical_video_id": None, "canonical_episode_id": "6f96fd46bb55",
         "yt_views": None, "x_views": "112.5K", "rumble_views": "3.3K", "ig_likes": "760"}


def main():
    # --- 1. the exact failure, both orders ---------------------------------------
    for label, rows in (("fresh first", [ROW_A, ROW_B]), ("sparse first", [ROW_B, ROW_A])):
        out, merged = EI.collapse(rows)
        check(f"ohanlon_collapses_{label.replace(' ','_')}",
              len(out) == 1 and merged == 1, f"{len(rows)} rows -> {len(out)}")
        check(f"ohanlon_keeps_youtube_{label.replace(' ','_')}",
              out[0].get("yt_views") == "2.2K",
              "the measured 2.2K survives whichever row arrived first")

    # a stronger key on ONE side must not defeat a weaker one BOTH sides share
    check("shared_weak_identity_is_enough",
          bool(EI.identities(ROW_A) & EI.identities(ROW_B)),
          "agreement on any identifier means the same episode")

    # --- 2. identity is transitive ----------------------------------------------
    a = {"canonical_video_id": "V1", "title": "T one"}
    b = {"canonical_video_id": "V1", "canonical_episode_id": "E9"}   # joins a by video id
    c = {"canonical_episode_id": "E9", "x_views": "5.0K"}            # joins b by episode id
    out, _ = EI.collapse([a, b, c])
    check("identity_is_transitive", len(out) == 1 and out[0].get("x_views") == "5.0K",
          "A~B by video id and B~C by episode id makes one episode")

    # --- 3. the fallback must not merge distinct episodes ------------------------
    # Two real episodes, same surname, same date, different titles.
    d1 = {"surname": "SMITH", "date": "1 Jan", "title": "First episode"}
    d2 = {"surname": "SMITH", "date": "1 Jan", "title": "Second, unrelated episode"}
    out, _ = EI.collapse([d1, d2])
    check("surname_date_never_merges_distinct_titles", len(out) == 2,
          "surname|date speaks only when a row carries no real identifier")
    # ...and it DOES still rescue rows that carry nothing else
    e1 = {"surname": "JONES", "date": "2 Feb", "x_views": "1.0K"}
    e2 = {"surname": "JONES", "date": "2 Feb", "yt_views": "2.0K"}
    out, _ = EI.collapse([e1, e2])
    check("surname_date_still_rescues_identifierless_rows",
          len(out) == 1 and out[0].get("x_views") == "1.0K" and out[0].get("yt_views") == "2.0K")

    # --- 4. a measured value is never lost to an absent one ----------------------
    f1 = {"canonical_episode_id": "E1", "x_views": "11.7K"}
    f2 = {"canonical_episode_id": "E1", "x_views": None}
    for rows in ([f1, f2], [f2, f1]):
        out, _ = EI.collapse(rows)
        check("measured_beats_absent", out[0].get("x_views") == "11.7K")

    # --- 5. carry-forward markers: cleared on merge, kept when untouched ---------
    g1 = {"canonical_episode_id": "E2", "title": "G"}
    g2 = {"canonical_episode_id": "E2", "_carried_forward_iso": "t", "_carried_forward_reason": "r"}
    out, _ = EI.collapse([g1, g2])
    check("merge_clears_carry_forward_marker", "_carried_forward_iso" not in out[0],
          "a merged episode is no longer only a memory of a previous file")
    lone = {"canonical_episode_id": "E3", "_carried_forward_iso": "t"}
    out, _ = EI.collapse([lone])
    check("unmerged_carry_forward_keeps_marker", "_carried_forward_iso" in out[0],
          "provenance survives; test_episode_never_disappears_v1 depends on it")

    # --- 6. idempotence ---------------------------------------------------------
    once, _ = EI.collapse([ROW_A, ROW_B])
    twice, n2 = EI.collapse(once)
    check("collapse_is_idempotent", twice == once and n2 == 0)

    # --- 7. BOTH writers must call the shared rule ------------------------------
    for fn in ("fetch_and_push.py", "m2_rumble_to_upstream_v1.py"):
        src = open(os.path.join(ROOT, fn), encoding="utf-8").read()
        check(f"{fn}_delegates", "episode_identity_v1" in src,
              "a rule implemented once per caller is not a rule")

    # --- 8. the live feeds must be free of duplicates ---------------------------
    for fn in ("videos.json", "videos_neworder.json", "videos_health_v1.json"):
        d = json.load(open(os.path.join(ROOT, fn)))
        rows = d["episodes"] if isinstance(d, dict) else d
        collapsed, merged = EI.collapse(rows)
        check(f"live_{fn}_has_no_duplicates", merged == 0,
              f"{len(rows)} rows, {merged} would collapse")

    print(f"\n  {len(PASSES)} passed, {len(FAILS)} failed")
    if FAILS:
        print("\n  EPISODE IDENTITY IS BROKEN — one episode can reach the display twice, "
              "and every aggregate over its date double-counts it:")
        for f in FAILS:
            print(f"    - {f}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
