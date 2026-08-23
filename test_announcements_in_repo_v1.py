#!/usr/bin/env python3
"""GU_ANNOUNCEMENTS_IN_REPO_V1_20260823 regression guard.

THE DEFECT. gu_guest_from_posts_v1 recovers a guest name from the show's own
announcement when the title names only a role. Its evidence lived at two absolute
paths on one Mac:

    /Users/afshin/RumbleMonitor/x_2026.json
    /Users/afshin/RumbleMonitor/ig_2026.json

GitHub Actions is what actually regenerates videos.json, and the runner has
neither. So in CI _load_posts returned [], resolve_guest refused, and the caller's
repair pass blanked the guest. The 2026-08-22 episode would have gone from "Ex-"
to empty rather than to "Michael O'Hanlon", and every future role-titled episode
would have done the same — the local fix would have looked correct on the laptop
and silently degraded in the only place that ships.

WHAT IS LOCKED HERE
  1. the resolver works with the Mac-only scrape absent — this is the CI case
  2. it still refuses when there is genuinely no announcement (no new false names)
  3. the in-repo file is verbatim post text, never a cached answer
  4. the union is deduped, so a laptop run seeing both sources cannot double-count
  5. the extractor cannot blank the committed evidence when run without upstream

Offline: no network, and nothing in the repo is written.

Run:  python3 test_announcements_in_repo_v1.py
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import gu_guest_from_posts_v1 as R  # noqa: E402

FAIL = []


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}{('  -- ' + detail) if detail else ''}")
    if not cond:
        FAIL.append(name)


ANN_PATH = os.path.join(HERE, "gu_announcements_v1.json")
_BEFORE = (os.path.getmtime(ANN_PATH), open(ANN_PATH, "rb").read())

print("\n1  THE EVIDENCE IS COMMITTED AND WELL-FORMED")
doc = json.load(open(ANN_PATH))
check("marker present", doc.get("marker") == "GU_ANNOUNCEMENTS_IN_REPO_V1_20260823")
shows = doc.get("shows") or {}
check("both shows present", set(shows) == {"GU", "NO"}, str(sorted(shows)))
check("GU has announcements", len(shows.get("GU", [])) > 0, str(len(shows.get("GU", []))))
check("every row is verbatim text + timestamp",
      all(isinstance(r.get("text"), str) and r.get("ts") for v in shows.values() for r in v))

print("\n2  IT STORES THE SHOW'S WORDS, NOT A RESOLVED ANSWER")
# If a name were cached here, the resolver's tested binding rules would be bypassed
# and a wrong cached name could never be corrected by fixing the resolver.
for v in shows.values():
    for r in v:
        if set(r) - {"text", "ts"}:
            check("no answer fields on a row", False, str(sorted(r)))
            break
    else:
        continue
    break
else:
    check("rows carry only text+ts (no cached guest)", True)
check("every stored post really is an announcement",
      all(R._JOINED.search(r["text"]) for v in shows.values() for r in v))

print("\n3  THE CI CASE: the Mac-only scrape is absent")
_x, _ig = R.X_FILE, R.IG_FILE
R.X_FILE, R.IG_FILE = "/nonexistent/x.json", "/nonexistent/ig.json"
try:
    check("upstream really is unavailable", len(R._load_posts_upstream("GU")) == 0)
    check("but posts are still available", len(R._load_posts("GU")) > 0,
          str(len(R._load_posts("GU"))))
    n, ev = R.resolve_guest("2026-08-22T11:55:34Z", "GU")
    check("2026-08-22 resolves to Michael O'Hanlon",
          n is not None and "O" in n and "Hanlon" in n, repr(n))
    check("and cites the announcement that proves it",
          str(ev.get("announced_iso", "")).startswith("2026-08-21"), str(ev.get("announced_iso")))
    check("New Order 2026-08-23 resolves to David Monyae",
          R.resolve_guest("2026-08-23T06:30:06Z", "NO")[0] == "David Monyae")

    print("\n4  IT STILL REFUSES WHEN THERE IS NO ANNOUNCEMENT")
    check("2026-06-27 (no announcement) -> None",
          R.resolve_guest("2026-06-27T00:00:00Z", "GU")[0] is None)
    check("a date with no posts at all -> None",
          R.resolve_guest("2020-01-01T00:00:00Z", "GU")[0] is None)
    check("unparseable date -> None", R.resolve_guest("not-a-date", "GU")[0] is None)
finally:
    R.X_FILE, R.IG_FILE = _x, _ig

print("\n5  THE UNION IS DEDUPED (laptop sees both sources)")
if os.path.exists(R.X_FILE) or os.path.exists(R.IG_FILE):
    both = R._load_posts("GU")
    keys = [(R._norm(t)[:160], w.isoformat()) for t, w in both]
    check("no duplicate (text, timestamp) pairs", len(keys) == len(set(keys)),
          f"{len(keys)} rows, {len(set(keys))} unique")
    check("union is at least as large as either source alone",
          len(both) >= max(len(R._load_posts_upstream("GU")), len(R._load_posts_in_repo("GU"))))
else:
    check("upstream absent on this machine; union test not applicable", True)

print("\n6  THE EXTRACTOR CANNOT BLANK THE COMMITTED EVIDENCE")
import gu_announcements_extract_v1 as X  # noqa: E402
_xx, _ii = X.R.X_FILE, X.R.IG_FILE
X.R.X_FILE, X.R.IG_FILE = "/nonexistent/x.json", "/nonexistent/ig.json"
try:
    rc = X.main()
    check("running without upstream is a no-op, not a wipe", rc == 0)
    check("the file is byte-identical afterwards",
          open(ANN_PATH, "rb").read() == _BEFORE[1])
finally:
    X.R.X_FILE, X.R.IG_FILE = _xx, _ii

print("\n7  NOTHING IN THE REPO WAS MODIFIED BY THIS TEST")
check("gu_announcements_v1.json untouched",
      os.path.getmtime(ANN_PATH) == _BEFORE[0]
      and open(ANN_PATH, "rb").read() == _BEFORE[1])

print("\n" + ("ALL CHECKS PASS" if not FAIL else f"{len(FAIL)} FAILED: {FAIL}"))
sys.exit(1 if FAIL else 0)
