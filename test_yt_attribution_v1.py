#!/usr/bin/env python3
"""YT_ATTRIBUTION_V1_20260930 — YouTube numbers that were wrong, and the two reasons.

FAILURE 1: the exact video-id match never won
---------------------------------------------
YT_ATTRIB_BY_VIDEO_ID_V1_20260822 says "exact id match wins over surname tokens". The
control flow never honoured it: the surname pass ran unconditionally afterwards and, being
last, overwrote the exact figure. That matters because views_map[surname] is a SUM —
`episode_views[sn] = episode_views.get(sn, 0) + _v` over EVERY video whose title contains
the surname — so it is not one episode's view count at all.

Verified 2026-09-30 against youtube.com itself:

    guest       video id      published   actual
    Schiff      rE30fKtk4pk       1.1K       514
    Perkins     O3jo1u4MhPY       1.1K       837
    Sood        U_7Y7bkSXfk       1.1K     3,239
    Baharoon    F8hxaEtl9Y8        291       110
    Fernandez   XnOfgmdAELk         30       190

Three different episodes published the SAME 1.1K. The 2026-07-20 note in that same block
already describes this incident ("a Short containing 'Perkins' set yt_views on the main NO
episode") — the earlier fix guarded WHICH map the surname falls back to, not whether the
fallback runs at all.

FAILURE 2: the change-detector did not cover what it guarded
------------------------------------------------------------
`_health_episodes_sig` hashed only rumble_views and ig_likes, the two fields the bridge
wrote when it was written. A yt_views or x_views change produced an identical signature, so
the bridge called it churn and ran `git checkout -- videos_health_v1.json`, DISCARDING a
freshly regenerated, correct feed. Wilkerson/Fritz/Ellwood were filled with real YouTube
figures and the feed the displays read still said null, run after run.

Run: python3 test_yt_attribution_v1.py
"""
from __future__ import annotations

import json
import os
import re
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)

FAILS, PASSES = [], []


def check(name, ok, detail=""):
    (PASSES if ok else FAILS).append(name)
    print(f"  {'PASS' if ok else 'FAIL'}  {name}{('  — ' + detail) if detail else ''}")


def main():
    src = open(os.path.join(ROOT, "fetch_and_push.py"), encoding="utf-8").read()

    # --- 1. the exact-id match must suppress the surname fallback ----------------
    check("exact_id_sets_a_flag",
          "_yt_bound_exactly = True" in src,
          "the id branch records that it answered")
    check("surname_fallback_is_suppressed",
          re.search(r"if surname in yt and not _yt_bound_exactly:", src) is not None,
          "the summing surname map cannot overwrite an exact per-video figure")
    # ...and the suppression must NOT be a continue/return that skips ig_clips, which is
    # attributed by the SAME loop. (`if not surname: continue` above it is legitimate, so
    # the window checked is strictly between the yt branch and the ig_clips branch.)
    _between = src[src.index("if surname in yt and not _yt_bound_exactly:"):]
    _between = _between[:_between.index("if surname in ig_clips:")]
    check("ig_clips_still_reached",
          "continue" not in _between and "break" not in _between,
          "Instagram shares that loop and must not be skipped")

    # --- 2. the health signature must cover every metric ------------------------
    bsrc = open(os.path.join(ROOT, "m2_rumble_to_upstream_v1.py"), encoding="utf-8").read()
    sig = bsrc[bsrc.index("def _health_episodes_sig"):]
    sig = sig[:sig.index("def _videos_feed_degraded")]
    for field in ("rumble_views", "ig_likes", "yt_views", "x_views", "canonical_video_id"):
        check(f"sig_covers_{field}", f'"{field}"' in sig or f"'{field}'" in sig,
              "a detector blind to a field discards correct updates to it")

    # --- 3. the store attribution module ----------------------------------------
    import gu_yt_store_attribution_v1 as YSA
    check("store_module_matches_on_title_not_surname",
          "surname" not in YSA._index.__doc__.lower() if YSA._index.__doc__ else True,
          "the store holds two Wilkerson and two Fritz videos")

    videos = [{"id": "AAA", "show": "GU", "upload_date": "20260718", "views": 4369,
               "title": "Col. Larry Wilkerson: The World Is SLEEPWALKING"},
              {"id": "BBB", "show": "NO", "upload_date": "20260419", "views": 257,
               "title": "Col. Lawrence Wilkerson: Iran WON"}]
    idx = YSA._index(videos)
    row = {"title": "Col. Larry Wilkerson: The World Is SLEEPWALKING"}
    hits = idx.get(YSA.EI.norm_title_id(row["title"])) or []
    check("same_surname_different_videos_do_not_collide",
          len(hits) == 1 and hits[0]["id"] == "AAA",
          "matched by title; a surname needle would have had two candidates")

    # a multi-line social post as the title still finds its video by containment
    post_row = {"title": "NEW EPISODE OF NEW ORDER\n\nBRICS 'UP & RUNNING': How India Will "
                         "Navigate US-Iran Conflict & Tensions With Trump\n\nHow is India..."}
    store = [{"id": "CCC", "show": "NO", "upload_date": "20260628", "views": 138,
              "title": "BRICS ‘UP & RUNNING’: How India Will Navigate US-Iran "
                       "Conflict & Tensions With Trump"}]
    got = YSA._contained_match(post_row, store, "NO")
    check("multiline_post_title_matches_by_containment",
          len(got) == 1 and got[0]["id"] == "CCC",
          "the 28 Jun row carried a whole post and sat on a fabricated yt_views of 0")

    # --- 4. the live feeds: every platform measured, and no fabricated zero ------
    for fn in ("videos.json", "videos_neworder.json"):
        rows = json.load(open(os.path.join(ROOT, fn)))
        nulls = [(r.get("date"), k) for r in rows
                 for k in ("yt_views", "x_views", "rumble_views", "ig_likes")
                 if r.get(k) is None]
        check(f"live_{fn}_has_no_unmeasured_platform", not nulls,
              f"{len(rows)} episodes" if not nulls else str(nulls[:4]))
        zeros = [(r.get("date"), k) for r in rows
                 for k in ("yt_views", "x_views", "rumble_views", "ig_likes")
                 if str(r.get(k)).strip() in ("0", "0.0")]
        check(f"live_{fn}_has_no_fabricated_zero", not zeros,
              "an unmeasured platform is null, never 0" if not zeros else str(zeros[:4]))

    # --- 5. the health feed must agree with the source it summarises -------------
    h = json.load(open(os.path.join(ROOT, "videos_health_v1.json")))
    by_id = {}
    for fn in ("videos.json", "videos_neworder.json"):
        for r in json.load(open(os.path.join(ROOT, fn))):
            if r.get("canonical_episode_id"):
                by_id[r["canonical_episode_id"]] = r
    drift = []
    for e in h.get("episodes", []):
        r = by_id.get(e.get("canonical_episode_id"))
        if not r:
            continue
        for k in ("yt_views", "x_views", "rumble_views", "ig_likes"):
            hv, rv = (e.get("metrics") or {}).get(k), r.get(k)
            if isinstance(hv, dict):
                continue                      # structured N/A is a deliberate shape
            if hv != rv:
                drift.append((e.get("surname"), k, hv, rv))
    check("health_feed_matches_its_source", not drift,
          f"{len(h.get('episodes', []))} episodes" if not drift else str(drift[:4]))

    print(f"\n  {len(PASSES)} passed, {len(FAILS)} failed")
    if FAILS:
        print("\n  YOUTUBE ATTRIBUTION IS BROKEN — a summed surname total or a discarded "
              "regeneration can put a wrong number on the displays:")
        for f in FAILS:
            print(f"    - {f}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
