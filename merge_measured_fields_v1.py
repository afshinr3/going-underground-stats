#!/usr/bin/env python3
"""MERGE_MEASURED_FIELDS_V1_20260917 — stop the cloud run erasing Rumble / IG numbers.

Two writers share videos.json / videos_neworder.json:
  * the GitHub Action (fetch_and_push.main_fetch) measures X + YouTube but cannot reach
    Rumble or Instagram, so it publishes rumble_views / ig_likes as null ("unmeasured");
  * the M2 bridge (m2_rumble_to_upstream_v1.py, cron :20) fills those two fields from the
    local scrapers and pushes.
The Action's push step did `git reset --hard origin/main` and then copied its own files over
the top, so every Action push silently reverted the bridge's numbers. On 2026-09-17 the bridge
wrote "Carlson rumble None -> 4.4K" at 00:20Z and the Action's 00:32Z push put it back to null
for every GU episode.

This runs in the Action AFTER the reset (working tree = origin/main, i.e. the bridge's latest)
and BEFORE commit. For each row the Action is publishing, a field this run left unmeasured
(null / "?" / "") is carried forward from the matching origin row when origin has a measured
value. A value the Action DID measure always wins. Rows are never added or removed. The weekly
and health feeds are then re-emitted from the merged files so the derived feeds agree.

Usage (from the repo root):  python3 merge_measured_fields_v1.py <dir_with_cloud_copies>
Cloud copies are named as the workflow saves them: gu_videos_json, gu_videos_neworder_json.
Never raises past main(); on any error the cloud copy is used unchanged (prior behaviour).
"""
import json
import os
import sys

FIELDS = ("rumble_views", "ig_likes")
UNMEASURED = (None, "", "?")
FILES = ("videos.json", "videos_neworder.json")


def _key_id(r):
    return r.get("canonical_video_id") or None


def _key_name(r):
    return ((r.get("surname") or "").strip().lower(), (r.get("date") or "").strip())


def merge_rows(cloud_rows, origin_rows):
    """Return (merged_rows, n_carried). Pure: no I/O."""
    by_id, by_name = {}, {}
    for o in origin_rows or []:
        if not isinstance(o, dict):
            continue
        if _key_id(o):
            by_id.setdefault(_key_id(o), o)
        if _key_name(o)[0]:
            by_name.setdefault(_key_name(o), o)
    n = 0
    for r in cloud_rows or []:
        if not isinstance(r, dict):
            continue
        o = (by_id.get(_key_id(r)) if _key_id(r) else None) or by_name.get(_key_name(r))
        if not o:
            continue
        for f in FIELDS:
            if r.get(f) in UNMEASURED and o.get(f) not in UNMEASURED:
                r[f] = o[f]
                n += 1
    return cloud_rows, n


def main(cloud_dir):
    total = 0
    for fn in FILES:
        src = os.path.join(cloud_dir, "gu_" + fn.replace(".", "_"))
        if not os.path.exists(src):
            continue
        try:
            cloud = json.load(open(src))
            origin = json.load(open(fn)) if os.path.exists(fn) else []
            merged, n = merge_rows(cloud, origin)
        except Exception as e:
            print(f"[MERGE_MEASURED_FIELDS_V1] {fn}: merge skipped ({e!r}); cloud copy used")
            merged, n = json.load(open(src)), 0
        with open(fn, "w") as fh:
            json.dump(merged, fh, indent=2, ensure_ascii=False)
        print(f"[MERGE_MEASURED_FIELDS_V1] {fn}: carried forward {n} measured field(s) from origin")
        total += n
    if total:
        try:
            import fetch_and_push as fp
            fp._generate_weekly_stats()
            fp._emit_videos_health_v1()
            print("[MERGE_MEASURED_FIELDS_V1] weekly + health feeds re-emitted from merged files")
        except Exception as e:
            print(f"[MERGE_MEASURED_FIELDS_V1] derived-feed re-emit failed: {e!r}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "/tmp"))
