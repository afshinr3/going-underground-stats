#!/usr/bin/env python3
"""GU_ANNOUNCEMENTS_IN_REPO_V1_20260823 — publish the show's own guest announcements
into THIS repo so the guest resolver works in CI, not only on the Mac.

WHY. gu_guest_from_posts_v1 recovers a guest name from the show's own X/IG
announcement when the episode title names only a role ("Ex-CIA Advisor"). It reads

    /Users/afshin/RumbleMonitor/x_2026.json     (2.5 MB)
    /Users/afshin/RumbleMonitor/ig_2026.json

Those are absolute paths on one laptop. The GitHub Actions runner that actually
regenerates videos.json has neither, so in CI the resolver found no posts, returned
None, and the repair pass fell through to BLANKING the guest. The 2026-08-22 episode
would have gone from "Ex-" to empty rather than to "Michael O'Hanlon" — a smaller
lie, still not the name, and every future role-titled episode would do the same.

WHAT THIS EXTRACTS, AND WHY IT IS SMALL. The resolver binds on exactly two
conditions: the post names the SHOW, and it uses the announcement phrase "joined
by". Posts failing either are already ignored, so keeping only the posts that pass
both loses the resolver nothing while turning 2.6 MB of scraped feed into a few KB.
This is a projection of the evidence, not a summary of it: the announcement text is
carried verbatim so the resolver applies its own unchanged rules to it.

NOT A CACHE OF ANSWERS. It stores the show's raw words, never a resolved name. The
binding decision stays in the resolver, where it is tested.

Run:  python3 gu_announcements_extract_v1.py
Then commit gu_announcements_v1.json if it changed.
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "gu_announcements_v1.json")

sys.path.insert(0, HERE)
import gu_guest_from_posts_v1 as R  # noqa: E402

MARKER = "GU_ANNOUNCEMENTS_IN_REPO_V1_20260823"


def extract():
    """Announcement posts per show, verbatim, deduped, oldest first."""
    out = {}
    for show, markers in R._SHOW_MARKERS.items():
        seen, rows = set(), []
        # Read the upstream scrape directly. If it is missing (i.e. we are already
        # running in CI) this yields nothing and the existing file is left alone --
        # a runner must never be able to blank the evidence it depends on.
        for text, when in R._load_posts_upstream(show):
            up = (text or "").upper()
            if not any(m in up for m in markers):
                continue
            if not R._JOINED.search(text or ""):
                continue
            key = (R._norm(text)[:160], when.isoformat())
            if key in seen:
                continue
            seen.add(key)
            rows.append({"text": text, "ts": when.isoformat()})
        rows.sort(key=lambda r: r["ts"])
        out[show] = rows
    return out


def main():
    if not (os.path.exists(R.X_FILE) or os.path.exists(R.IG_FILE)):
        print(f"[{MARKER}] upstream scrape not present; leaving {os.path.basename(OUT)} "
              f"untouched")
        return 0
    data = extract()
    total = sum(len(v) for v in data.values())
    if not total:
        print(f"[{MARKER}] no announcements matched; refusing to write an empty file")
        return 1
    payload = {"marker": MARKER,
               "note": "Verbatim announcement posts. Resolution happens in "
                       "gu_guest_from_posts_v1, never here.",
               "shows": data}
    prev = None
    if os.path.exists(OUT):
        try:
            prev = json.load(open(OUT))
        except Exception:
            prev = None
    if prev == payload:
        print(f"[{MARKER}] unchanged ({total} announcements)")
        return 0
    tmp = OUT + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=1)
    os.replace(tmp, OUT)
    print(f"[{MARKER}] wrote {total} announcements "
          + ", ".join(f"{k}={len(v)}" for k, v in data.items()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
