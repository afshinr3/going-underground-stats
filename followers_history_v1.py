#!/usr/bin/env python3
"""FOLLOWERS_HISTORY_V1_20261010 — build followers_history.json (one point per
day, last 35 days) from the git history of followers.json. Read by the Niblet
"X Followers" app via raw.githubusercontent.com. Runs every 3h from cron after
the M2 bridge commit; commits only when the file changes.
"""
import json, os, subprocess, sys
from datetime import datetime, timedelta, timezone

REPO = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(REPO, "followers_history.json")
ACCOUNTS = ["afshinrattansi", "GUnderground_TV"]
# IG count is scraped on the M2 Pro (RumbleMonitor/ig_follower_count_v1.py); only
# successful (ok) readings are recorded, one per day.
IG_COUNT = os.path.expanduser("~/RumbleMonitor/ig_follower_count.json")
DAYS = 35


def git(*a):
    return subprocess.run(["git", "-C", REPO, *a], capture_output=True, text=True, check=True).stdout


def main():
    since = (datetime.now(timezone.utc) - timedelta(days=DAYS)).strftime("%Y-%m-%d")
    log = git("log", f"--since={since}", "--format=%H %ct", "--", "followers.json").split("\n")
    daily = {}  # date -> {acct: count}; newest commit of each day wins
    for line in log:
        if not line.strip():
            continue
        sha, ts = line.split()
        day = datetime.fromtimestamp(int(ts), timezone.utc).strftime("%Y-%m-%d")
        if day in daily:
            continue
        try:
            accts = json.loads(git("show", f"{sha}:followers.json")).get("accounts", {})
        except Exception:
            continue
        row = {a: accts.get(a) for a in ACCOUNTS if isinstance(accts.get(a), int) and accts.get(a) > 0}
        if len(row) == len(ACCOUNTS):
            daily[day] = row
    days = sorted(daily)
    data = {
        "_marker": "FOLLOWERS_HISTORY_V1_20261010",
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "days": days,
        "accounts": {a: [daily[d][a] for d in days] for a in ACCOUNTS},
    }
    old = None
    if os.path.exists(OUT):
        try:
            old = json.load(open(OUT))
        except Exception:
            pass
    ig = dict((old or {}).get("instagram") or {"handle": "afshinrattansi", "history": {}})
    ig["history"] = dict(ig.get("history") or {})
    try:
        cur = json.load(open(IG_COUNT))
        if isinstance(cur.get("followers"), int) and cur["followers"] > 0:
            asof = datetime.fromtimestamp(cur["ts"], timezone.utc)
            if not ig.get("asof") or asof.strftime("%Y-%m-%dT%H:%M:%SZ") >= ig["asof"]:
                ig["count"] = cur["followers"]
                ig["asof"] = asof.strftime("%Y-%m-%dT%H:%M:%SZ")
                ig["history"][asof.strftime("%Y-%m-%d")] = cur["followers"]
    except Exception as e:
        print("ig read failed:", e)
    cutoff = (datetime.now(timezone.utc) - timedelta(days=DAYS)).strftime("%Y-%m-%d")
    ig["history"] = {d: c for d, c in sorted(ig["history"].items()) if d >= cutoff}
    data["instagram"] = ig
    if old and all(old.get(k) == data[k] for k in ("days", "accounts", "instagram")):
        print("unchanged"); return
    json.dump(data, open(OUT, "w"), indent=1)
    print(f"wrote {len(days)} days {days[0] if days else '-'}..{days[-1] if days else '-'}")
    if "--commit" in sys.argv:
        git("add", "followers_history.json")
        # pathspec commit: never sweeps in anything else that happens to be staged
        git("commit", "-m", "followers_history: daily X follower history (30d)", "--", "followers_history.json")
        git("push", "-q", "origin", "HEAD")
        print("pushed")


if __name__ == "__main__":
    main()
