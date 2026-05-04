#!/usr/bin/env python3
"""
Drop tracker — runs after each cloud fetch.

Maintains:
  peaks.json    — per-guest, per-platform all-time-max view count seen so far
  drops.md      — append-only log of every drop > 15% from peak (with timestamp)

This filters out scrape noise: numbers that just oscillate in a band don't
trigger a "drop" entry; only when the current value falls meaningfully
below the previously-seen ceiling do we flag it.
"""

import json
import os
import sys
from datetime import datetime

ROOT = os.path.dirname(os.path.abspath(__file__))
PEAKS_FILE = os.path.join(ROOT, "peaks.json")
DROPS_FILE = os.path.join(ROOT, "drops.md")
CURRENT_DROPS_FILE = os.path.join(ROOT, "drops_current.json")

PLATFORMS = ["x_views", "yt_views", "ig_likes", "rumble_views"]
DROP_THRESHOLD_PCT = 15  # only log drops larger than this


def parse_count(v):
    s = str(v or "0").replace(",", "").replace("?", "0").strip()
    if not s:
        return 0
    if s.upper().endswith("M"):
        try:
            return int(float(s[:-1]) * 1_000_000)
        except Exception:
            return 0
    if s.upper().endswith("K"):
        try:
            return int(float(s[:-1]) * 1_000)
        except Exception:
            return 0
    try:
        return int(float(s))
    except Exception:
        return 0


def fmt(n):
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n/1_000:.1f}K"
    return str(n)


def main():
    # Load peaks
    if os.path.exists(PEAKS_FILE):
        with open(PEAKS_FILE) as f:
            peaks = json.load(f)
    else:
        peaks = {}

    drops_today = []
    now = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    for show, data_file in [("GU", "videos.json"), ("NO", "videos_neworder.json")]:
        path = os.path.join(ROOT, data_file)
        if not os.path.exists(path):
            continue
        with open(path) as f:
            episodes = json.load(f)
        for ep in episodes:
            surname = ep.get("surname", "")
            if not surname:
                continue
            key = f"{show}:{surname}"
            ep_peaks = peaks.setdefault(key, {})
            for plat in PLATFORMS:
                cur_str = ep.get(plat, "?")
                cur = parse_count(cur_str)
                if cur <= 0:
                    continue
                prev_peak = ep_peaks.get(plat, 0)
                if cur > prev_peak:
                    ep_peaks[plat] = cur
                    ep_peaks[plat + "_at"] = now
                else:
                    pct_drop = 100.0 * (prev_peak - cur) / prev_peak if prev_peak else 0
                    if pct_drop >= DROP_THRESHOLD_PCT:
                        drops_today.append({
                            "ts": now,
                            "show": show,
                            "guest": surname,
                            "platform": plat.replace("_views", "").replace("_likes", "").upper(),
                            "peak": prev_peak,
                            "peak_at": ep_peaks.get(plat + "_at", "?"),
                            "current": cur,
                            "drop_pct": round(pct_drop, 1),
                        })

    # Save updated peaks
    with open(PEAKS_FILE, "w") as f:
        json.dump(peaks, f, indent=2)

    # Always overwrite drops_current.json — apps/devices consume this for alerts
    with open(CURRENT_DROPS_FILE, "w") as f:
        json.dump({"updated": now, "drops": drops_today}, f, indent=2)

    # Append to drops log only when there's something
    if drops_today:
        with open(DROPS_FILE, "a") as f:
            f.write(f"\n## {now}\n\n")
            f.write("| Show | Guest | Platform | Peak | Peak time | Now | Drop |\n")
            f.write("|------|-------|----------|------|-----------|-----|------|\n")
            for d in drops_today:
                f.write(f"| {d['show']} | {d['guest']} | {d['platform']} | "
                        f"{fmt(d['peak'])} | {d['peak_at'][:16]} | "
                        f"{fmt(d['current'])} | **-{d['drop_pct']}%** |\n")
        print(f"Logged {len(drops_today)} drops")
    else:
        print("No drops > {}%".format(DROP_THRESHOLD_PCT))


if __name__ == "__main__":
    main()
