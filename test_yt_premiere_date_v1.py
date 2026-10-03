#!/usr/bin/env python3
"""YT_PREMIERE_DATE_V1_20261004 — a premiere is dated by when it AIRS, not when it was
uploaded. No network; the cache is the contract.

THE REGRESSION THIS GUARDS (observed on the published feed, 2026-10-03)
----------------------------------------------------------------------
The New Order episode "Col. Lawrence Wilkerson: Trump Gave Netanyahu the GREEN LIGHT..."
showed as 2 Oct in videos_neworder.json, and therefore on the Android app, while it had
not aired at all. It was a scheduled premiere: uploaded 2026-10-02T07:12:53Z, airing
2026-10-04T06:30:00Z. YouTube's RSS feed lists a premiere exactly like a published video
and its <published> carries the UPLOAD time, with nothing saying the video has never been
seen. So it was admitted as a full EPISODE, dated two days early, sorted to the top as the
newest episode, and carried yt_views "0" with rumble_views and x_views null — which the
Android app renders as "?" thanks to the deliberate '?'-to-null hardening. One cause,
three symptoms, and the only wrong datum was the date.

WHY THE CACHE IS LOAD-BEARING: `date` is recomputed from `pub_iso` on every run and this
job runs every 15 minutes in CI. A probe that fails in CI would fall back to the RSS upload
date and revert the correction run after run — the same shape as the YouTube values that
this repo has already had reverted by a change detector that did not cover what it guarded.
So the premiere time is learned once, committed, and CI needs no network to honour it.

Run: python3 test_yt_premiere_date_v1.py
"""
import json
import os
import sys

D = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, D)
F, P = [], []


def check(n, ok, d=""):
    (P if ok else F).append(n)
    print("  %s  %s%s" % ("PASS" if ok else "FAIL", n, ("  — " + d) if d else ""))


src = open(os.path.join(D, "fetch_and_push.py"), encoding="utf-8").read()

# --- the mechanism is wired into the admission path -------------------------------
check("marker_present", "YT_PREMIERE_DATE_V1_20261004" in src)
check("helper_defined", "def _yt_premiere_iso(" in src)
check("cache_path_defined", "yt_premiere_cache_v1.json" in src)
check("admission_uses_premiere_date", '"pub_iso": _prem or _pub_norm' in src,
      "pub_iso must prefer the premiere time over the RSS upload time")
check("upload_time_retained", '"_yt_upload_iso": _pub_norm' in src,
      "the upload time must still be recorded, not discarded")

# the probe must fail OPEN: an unknown video keeps exactly what RSS said
i = src.index("def _yt_premiere_iso(")
j = src.index("def _fetch_youtube_full_episodes(")
helper = src[i:j]
check("probe_fails_open", "leaving RSS date untouched" in helper and "return None" in helper)
check("failure_is_not_cached", "do NOT cache a failure" in helper,
      "caching a network failure would make a transient error permanent")
check("probe_is_bounded", "age_d > 30" in helper,
      "must not probe the whole 15-entry RSS window every run")
check("datetime_scoped_locally", "import datetime as _dtm" in helper,
      "module-level `dt` is a function-local alias for the datetime CLASS; using it here "
      "would raise NameError, be swallowed by the except, and silently revert forever")

# --- the learned cache is committed and sane --------------------------------------
cpath = os.path.join(D, "yt_premiere_cache_v1.json")
check("cache_committed", os.path.exists(cpath))
if os.path.exists(cpath):
    cache = json.load(open(cpath))
    prem = {k: v for k, v in cache.items() if isinstance(v, dict)}
    for vid, rec in prem.items():
        check("cache_%s_has_premiere_iso" % vid[:6], bool(rec.get("premiere_iso")))
        check("cache_%s_premiere_after_upload" % vid[:6],
              (rec.get("premiere_iso") or "") > (rec.get("upload_iso") or ""),
              "a premiere airs after it is uploaded: %s vs %s"
              % (rec.get("premiere_iso"), rec.get("upload_iso")))

    # --- the published feeds agree with the cache ---------------------------------
    for feed in ("videos_neworder.json", "videos.json"):
        fp = os.path.join(D, feed)
        if not os.path.exists(fp):
            continue
        rows = json.load(open(fp))
        for r in rows:
            vid = r.get("canonical_video_id") or ""
            if vid in prem:
                want = prem[vid]["premiere_iso"]
                check("%s_%s_dated_by_air_time" % (feed.split(".")[0], vid[:6]),
                      r.get("pub_iso") == want,
                      "pub_iso %r should be the premiere time %r" % (r.get("pub_iso"), want))
                check("%s_%s_upload_time_kept" % (feed.split(".")[0], vid[:6]),
                      bool(r.get("_yt_upload_iso")))

# --- the specific episode that caused this ----------------------------------------
fp = os.path.join(D, "videos_neworder.json")
if os.path.exists(fp):
    rows = json.load(open(fp))
    wilk = [r for r in rows if (r.get("canonical_video_id") or "") == "-jJol9Qyxoo"]
    if wilk:
        check("wilkerson_not_dated_2_oct", wilk[0].get("date") != "2 Oct",
              "got %r" % wilk[0].get("date"))
        check("wilkerson_dated_4_oct", wilk[0].get("date") == "4 Oct",
              "got %r" % wilk[0].get("date"))

print("\n%d/%d passed" % (len(P), len(P) + len(F)))
sys.exit(1 if F else 0)
