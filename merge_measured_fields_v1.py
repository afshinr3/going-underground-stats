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
# EPISODE_CARRY_FORWARD_V1_20260918 — an episode must not vanish because ONE discovery run
# failed to find it. Measured 2026-09-18: videos.json held 15 GU rows while the published
# health feed held 18; Milanović (15 Aug), Hasan Ünal (7 Aug) and James Carden (11 Jul) were
# all live on YouTube and simply absent from that run's output, so the LaMetric guest frames
# skipped straight past them. Rows now survive a run that omits them; only a TOMBSTONE
# removes one, which is the "positive evidence" this rule requires.
TOMBSTONES = "gu_removed_episodes_v1.json"
UNMEASURED = (None, "", "?")
FILES = ("videos.json", "videos_neworder.json")


def _key_id(r):
    return r.get("canonical_video_id") or None


def _key_name(r):
    return ((r.get("surname") or "").strip().lower(), (r.get("date") or "").strip())


def _load_tombstones(path=TOMBSTONES):
    """{canonical_episode_id or 'surname|date'} deliberately removed. Absent file = none."""
    try:
        d = json.load(open(path))
        out = set(d.get("by_canonical_episode_id") or [])
        out |= set(d.get("by_surname_date") or [])
        return out
    except Exception:
        return set()


def _tombstoned(row, tombs):
    if not tombs:
        return False
    cid = row.get("canonical_episode_id")
    sn, dt = _key_name(row)
    return bool((cid and cid in tombs) or (sn and f"{sn}|{dt}" in tombs))


def _sort_key(r):
    """Newest first. pub_iso when present, else the bare '15 Aug' with the feed's year."""
    iso = str(r.get("pub_iso") or "")[:10]
    if len(iso) == 10 and iso[4] == "-":
        return iso
    import datetime as _dt
    for fmt in ("%d %b %Y", "%d %B %Y"):
        for yr in (_dt.date.today().year, _dt.date.today().year - 1):
            try:
                return _dt.datetime.strptime(f"{(r.get('date') or '').strip()} {yr}",
                                             fmt).date().isoformat()
            except ValueError:
                continue
    return "0000-00-00"


def carry_forward_rows(cloud_rows, origin_rows, tombs=None):
    """Append previously published rows this run omitted. Returns (rows, n_restored).

    Freshly discovered data always wins: a row present in BOTH sets is left as the cloud
    published it (the field-level merge above has already back-filled what this run could
    not measure). Pure: no I/O.
    """
    tombs = tombs or set()
    have_id = {_key_id(r) for r in cloud_rows if isinstance(r, dict) and _key_id(r)}
    have_name = {_key_name(r) for r in cloud_rows if isinstance(r, dict) and _key_name(r)[0]}
    restored = []
    for o in origin_rows or []:
        if not isinstance(o, dict):
            continue
        if (_key_id(o) and _key_id(o) in have_id) or (_key_name(o)[0] and _key_name(o) in have_name):
            continue
        if _tombstoned(o, tombs):
            print(f"[EPISODE_CARRY_FORWARD_V1] tombstoned, not restored: "
                  f"{o.get('date')} {o.get('surname')}")
            continue
        row = dict(o)
        row.setdefault("_carried_forward_v1", "EPISODE_CARRY_FORWARD_V1_20260918")
        restored.append(row)
    if not restored:
        return cloud_rows, 0
    out = list(cloud_rows) + restored
    out.sort(key=_sort_key, reverse=True)
    for r in restored:
        print(f"[EPISODE_CARRY_FORWARD_V1] restored {r.get('date')} "
              f"{r.get('guest') or r.get('surname')} (omitted by this run)")
    return out, len(restored)


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
            merged, n_rows = carry_forward_rows(merged, origin, _load_tombstones())
        except Exception as e:
            print(f"[MERGE_MEASURED_FIELDS_V1] {fn}: merge skipped ({e!r}); cloud copy used")
            merged, n, n_rows = json.load(open(src)), 0, 0
        with open(fn, "w") as fh:
            json.dump(merged, fh, indent=2, ensure_ascii=False)
        print(f"[MERGE_MEASURED_FIELDS_V1] {fn}: carried forward {n} measured field(s) "
              f"and {n_rows} whole episode row(s) from origin")
        total += n + n_rows
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
