#!/usr/bin/env python3
"""YT_ATTRIB_BY_VIDEO_ID_V1_20260822 regression guard.

YouTube views were attributed by matching the guest's SURNAME as a token in the video
title. That fails whenever the title does not name the guest — which is exactly the class
of title that also broke guest parsing:

    Eu0Phb99ipg  "Afshin Rattansi CHALLENGES Ex-CIA Advisor on the Legacy of America's
                  Wars"                      535 views, attributed to NOBODY
    eyfC-IBPTOM  "Ex-World Bank Lead Economist Says WW3..."
                                           4,838 views, attributed to NOBODY
    TuFMmjBU3Vw  "Prof. John Mearsheimer Explains..."
                                          29,998 views, attached fine — the surname is
                                          in the title, and that is the whole difference

Measured against the live GU feed: the surname map held 2 entries, the id map 15.
The app showed YouTube "?" beside populated Rumble/X/IG columns.

THIS IS THE THIRD FAILURE OF SURNAME-TOKEN ATTRIBUTION. The second is recorded in
fetch_and_push's own docstring (Ünal/Maté were unmatchable by an ASCII-only regex).

Fix: match on the episode's own YouTube id — exact, and indifferent to the title. Where no
id is bound (Milanovic 2026-08-14 has canonical_video_id=None) fall back to FULL-STRING
title equality, which is not a token heuristic and cannot mis-attribute.

Offline: no network, no writes.

Run:  python3 test_yt_attrib_by_id_v1.py
"""
import re, sys
FAIL = []
def check(n, c, d=""):
    print(f"  {'PASS' if c else 'FAIL'}  {n}{('  -- ' + d) if d else ''}")
    if not c: FAIL.append(n)

import fetch_and_push as F
src = open("fetch_and_push.py").read()

print("\n1  The id map is built and returned")
check("by_video_id collected", "by_video_id[_vid]" in src)
check("video id parsed from the link", "[?&]v=(" in src)
check("id map returned as a third value", "return views_map, date_map, id_map" in src)
check("error path returns three values too", "return {}, {}, {}" in src)
check("caller unpacks three", "yt, yt_dates, yt_by_id = fetch_youtube_data(" in src)

print("\n2  Attribution prefers the exact id, then exact title")
check("id checked first", "_yid if (_yid and _yid in yt_by_id)" in src)
check("exact-title fallback exists", 'title::' in src)
check("title key is full-string, not tokens", "re.sub(r'\\s+', ' ', str(v.get('title')" in src)

print("\n3  Only EPISODE-class entries contribute (a Short must not claim an episode)")
blk = src[src.index("if _vid and _is_episode_class"):src.index("# METRIC_ATTRIB_V1_2026_07_20 — extract whole-word")]
check("id map gated on _is_episode_class", "_is_episode_class" in blk)
check("title map gated on _is_episode_class", blk.count("_is_episode_class") >= 2)

print("\n4  The overwrite discipline is preserved (never regress a real number to null)")
blk2 = src[src.index("YT_ATTRIB_BY_VIDEO_ID_V1_20260822 — exact id match"):]
blk2 = blk2[:blk2.index("_sn_candidates")]
check("only overwrites on a real value", "_new not in ('0', 0, '?', None)" in blk2)
check("fills when current is missing", "_cur in (None, '?', '', 0)" in blk2)

print("\n5  The surname path still exists as a fallback")
check("surname candidates still built", "_sn_candidates = [" in src)
check("surname map still consulted", "if surname in yt:" in src)

print("\n6  Title normalisation is stable")
norm = lambda t: re.sub(r'\s+', ' ', t).strip().lower()
a = "Ex-World Bank  Lead Economist Says WW3\n is Being Made More Likely"
b = "ex-world bank lead economist says ww3 is being made more likely"
check("whitespace/case normalise to the same key", norm(a) == norm(b), norm(a))

print("\n" + ("ALL CHECKS PASS" if not FAIL else f"{len(FAIL)} FAILED: {FAIL}"))
sys.exit(1 if FAIL else 0)
