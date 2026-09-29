#!/usr/bin/env python3
"""GU_YT_STORE_ATTRIBUTION_V1_20260930 — YouTube views from the COMPLETE local store.

WHAT WAS WRONG
--------------
`yt_views` came only from the cloud run, which discovers videos through the channel's RSS
feed. That feed serves roughly the 15 most recent uploads, so an episode that ages out of it
can never acquire a `canonical_video_id` — and without one, nothing ever measures its
YouTube figure again. The row survives only through EPISODE_UNION_NEVER_SHRINKS_V1, which
preserves the episode but cannot preserve a measurement that was never taken.

Measured 2026-09-30, the three oldest GU episodes, all carried-forward, all with
canonical_video_id absent and yt_views null:

    18 Jul  Lawrence Wilkerson   ->  WlIY7-ovBd8   4,369 views
    13 Jul  Dennis Fritz         ->  grMo8SmAQpU   1,111 views
     6 Jul  Tobias Ellwood       ->  SmH-aEMZyqg   1,493 views

All three were sitting in `~/RumbleMonitor/yt_2026.json` the whole time — the local store
written by scrape_2026_yt.py, which holds 90 videos back to 2025-12-22 and is refreshed
every six hours. The cloud was rediscovering a 15-item window while the complete list was
already on disk, exactly as it was for X before GU_X_STORE_ATTRIBUTION_V1.

HOW IT MATCHES
--------------
On the NORMALISED TITLE only, via episode_identity_v1.norm_title_id — the same hash the
episode-identity rule uses, so punctuation variants cannot fork a match — and the match must
be UNIQUE. Never on surname: the store holds two "Wilkerson" videos (18 Jul GU and 19 Apr
NO) and two "Fritz" videos (13 Jul and 31 Jan), so a surname needle would have picked
whichever came first. Show and air date are then required to agree as corroboration.

A row is only rewritten when the store yields a MEASURED number for an unambiguous match.
An unmeasurable row keeps whatever it had: unknown stays unknown, never 0.
"""
from __future__ import annotations

import json
import os
import sys
import time

REPO = os.path.dirname(os.path.abspath(__file__))
STORE = "/Users/afshin/RumbleMonitor/yt_2026.json"
MARKER = "GU_YT_STORE_ATTRIBUTION_V1"
FILES = (("videos.json", "GU"), ("videos_neworder.json", "NO"))
MAX_STORE_AGE_H = 24.0
DATE_WIN_DAYS = 3

if REPO not in sys.path:
    sys.path.insert(0, REPO)
import episode_identity_v1 as EI


def _air_yyyymmdd(row):
    """Air date as YYYYMMDD from pub_iso, else None. `date` ("18 Jul") carries no year."""
    iso = (row.get("pub_iso") or "").strip()
    if len(iso) >= 10:
        return iso[:4] + iso[5:7] + iso[8:10]
    return None


def _days_apart(a, b):
    try:
        ta = time.mktime(time.strptime(a, "%Y%m%d"))
        tb = time.mktime(time.strptime(b, "%Y%m%d"))
        return abs(ta - tb) / 86400.0
    except Exception:
        return None


def _index(store_videos):
    """normalised title hash -> list of store videos."""
    idx = {}
    for v in store_videos:
        h = EI.norm_title_id(v.get("title"))
        if h:
            idx.setdefault(h, []).append(v)
    return idx


def _norm_text(t):
    """The same folding norm_title_id applies, but kept as text so one string can be
    tested for containment in another."""
    import re as _re
    import unicodedata as _ud
    t = (t or "").strip()
    if not t:
        return ""
    t = _ud.normalize("NFKD", t)
    for a, b in (("\u2019", "'"), ("\u2018", "'"), ("\u201c", '"'), ("\u201d", '"'),
                 ("\u2013", "-"), ("\u2014", "-"), ("\u00a0", " ")):
        t = t.replace(a, b)
    return _re.sub(r"\s+", " ", t).strip().casefold()


def _contained_match(row, videos, show):
    """MULTILINE_TITLE_FALLBACK_V1_20260930 — some feed rows carry a whole social post as
    their `title`, not the video's title. The 28 Jun New Order row reads

        "NEW EPISODE OF NEW ORDER\n\nBRICS 'UP & RUNNING': How India Will Navigate
         US-Iran Conflict & Tensions With Trump-C. Uday Bhaskar\n\nHow is India ..."

    so its hash matches nothing, and the episode sat with a fabricated yt_views of "0" and
    no video id. The video's real title is a SUBSTRING of that block, which is evidence in
    itself rather than a heuristic: require the store title to appear verbatim (after the
    same folding) inside the row's, and require the hit to be unique within the show.
    """
    hay = _norm_text(row.get("title"))
    if len(hay) < 40:
        return []
    hits = []
    for v in videos:
        if (v.get("show") or "").upper() != show:
            continue
        needle = _norm_text(v.get("title"))
        if len(needle) >= 30 and needle in hay:
            hits.append(v)
    return hits


def reattribute(apply=False, max_age_h=MAX_STORE_AGE_H):
    if not os.path.exists(STORE):
        print(f"[{MARKER}] store missing: {STORE}", file=sys.stderr)
        return 1
    age_h = (time.time() - os.path.getmtime(STORE)) / 3600.0
    if age_h > max_age_h:
        print(f"[{MARKER}] store stale ({age_h:.1f}h > {max_age_h}h) — nothing rewritten",
              file=sys.stderr)
        return 1

    videos = (json.load(open(STORE)) or {}).get("videos_all") or []
    idx = _index(videos)
    total = 0

    for fname, show in FILES:
        path = os.path.join(REPO, fname)
        if not os.path.exists(path):
            continue
        rows = json.load(open(path)) or []
        changed = 0
        for r in rows:
            if not isinstance(r, dict):
                continue
            hits = idx.get(EI.norm_title_id(r.get("title"))) or []
            if not hits:
                hits = _contained_match(r, videos, show)
                if len(hits) == 1:
                    print(f"  SUBSTR {r.get('date')} {r.get('surname')}: row title is a "
                          f"multi-line post; matched {hits[0]['id']} by contained title")
            if len(hits) != 1:
                if r.get("yt_views") is None and hits:
                    print(f"  AMBIG {r.get('date')} {r.get('surname')}: {len(hits)} store "
                          f"videos share this title — left unchanged")
                continue
            v = hits[0]
            if (v.get("show") or "").upper() != show:
                print(f"  SHOW  {r.get('date')} {r.get('surname')}: store says "
                      f"{v.get('show')}, feed says {show} — left unchanged")
                continue
            air = _air_yyyymmdd(r)
            if air and v.get("upload_date"):
                gap = _days_apart(air, v["upload_date"])
                if gap is not None and gap > DATE_WIN_DAYS:
                    print(f"  DATE  {r.get('date')} {r.get('surname')}: store upload "
                          f"{v['upload_date']} is {gap:.0f}d from air {air} — left unchanged")
                    continue

            # If the row already knows its YouTube id, the store's match MUST be that same
            # video. Identical titles across two uploads would otherwise let the store
            # relabel a bound episode, which is the one way a title match can go wrong.
            bound = (r.get("canonical_video_id") or "").strip()
            if bound and bound != v["id"]:
                print(f"  IDDIFF {r.get('date')} {r.get('surname')}: row bound to {bound}, "
                      f"store title matches {v['id']} — left unchanged")
                continue

            views = v.get("views")
            if not isinstance(views, int):
                continue

            # Bind the identity as well as the number. A row with no canonical_video_id
            # cannot be measured again by anything, which is how these rows went dark.
            before_id = r.get("canonical_video_id")
            if not before_id:
                r["canonical_video_id"] = v["id"]
                r["canonical_video_url"] = f"https://www.youtube.com/watch?v={v['id']}"
                r.setdefault("link", r["canonical_video_url"])
                sp = r.setdefault("source_platform_ids", {})
                yt = sp.setdefault("youtube", [])
                if v["id"] not in yt:
                    yt.append(v["id"])

            old = r.get("yt_views")
            new = EI._fmt(views) if hasattr(EI, "_fmt") else _fmt(views)
            if old == new and before_id:
                continue
            r["yt_views"] = new
            r["_yt_status"] = MARKER
            print(f"  SET   {v['upload_date']} {str(r.get('surname'))[:22]:22} "
                  f"{str(old):>8} -> {new:>8}"
                  + ("" if before_id else f"  (bound {v['id']})"))
            changed += 1

        if apply and changed:
            tmp = path + ".tmp"
            with open(tmp, "w") as fh:
                json.dump(rows, fh, indent=2, ensure_ascii=False)
            os.replace(tmp, path)
        print(f"[{MARKER}] {fname}: {changed} row(s) "
              f"{'rewritten' if apply else 'would change'}")
        total += changed
    return 0


def _fmt(n):
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n/1_000:.1f}K"
    return str(int(n))


def main():
    apply = "--apply" in sys.argv
    max_age = MAX_STORE_AGE_H
    if "--max-age-h" in sys.argv:
        try:
            max_age = float(sys.argv[sys.argv.index("--max-age-h") + 1])
        except Exception:
            pass
    try:
        return reattribute(apply=apply, max_age_h=max_age)
    except Exception as e:                                          # noqa: BLE001
        print(f"[{MARKER}_ERR] {e!r}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
