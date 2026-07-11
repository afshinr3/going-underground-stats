"""Publish canonical GU episode state consumed by both Tidbyt and LaMetric.

GU_CANONICAL_EPISODE_V1_2026_07_11

Reads: /Users/afshin/going-underground-stats/videos.json (local M2 Pro clone)
Writes:
  1. /Users/afshin/going-underground-stats/canonical_episode_v1.json
  2. Patches videos.json in place to add canonical_guest_full_name on top-2 episodes
Backup videos.json to videos.json.bak.pre-canonical-episode-v1.<TS>

Rules encoded here:
- Canonical latest EPISODE identity comes from Rumble/RT-sourced episodes
  in videos.json (X clips and viral X reposts do NOT set canonical identity).
- Episodes are ranked by date (parsed as "DD MMM 2026"), most recent first.
- The top-2 dates that contain a resolvable full-name guest become the
  canonical "latest episode set" — displayed as ELLWOOD + CARDEN.
- Wilkerson-style old reposted clips must never surface as "latest".

Does NOT push to devices. Push is done by:
  - push84_lametric.py (already reads canonical_guest_full_name FIRST)
  - fetch_and_push.push_to_tidbyt() (reads videos.json surname/guest)
Both will see corrected values next cycle.
"""
import json, os, re, time, hashlib
from datetime import datetime, timezone

MARKER = "GU_CANONICAL_EPISODE_V1_2026_07_11"
ROOT = "/Users/afshin/going-underground-stats"
VIDEOS = os.path.join(ROOT, "videos.json")
OUT = os.path.join(ROOT, "canonical_episode_v1.json")

CANON = {
    "ellwood": "Tobias Ellwood",
    "carden": "James Carden",
    "wilkerson": "Lawrence Wilkerson",
    "kucinich": "Dennis Kucinich",
    "blumenthal": "Max Blumenthal",
    "trenin": "Dmitri Trenin",
    "keen": "Steve Keen",
    "weihua": "Chen Weihua",
    "shlaim": "Avi Shlaim",
    "weiwei": "Zhang Weiwei",
    "roberts": "Paul Craig Roberts",
    "mearsheimer": "John Mearsheimer",
    "sachs": "Jeffrey Sachs",
    "sood": "Vikram Sood",
    "hanke": "Steve Hanke",
    "olmert": "Ehud Olmert",
    "postol": "Theodore Postol",
    "bhaskar": "C. Uday Bhaskar",
    "sibal": "Kanwal Sibal",
    "bolton": "John Bolton",
    "wolff": "Richard Wolff",
    "pyne": "David Pyne",
    "kortunov": "Andrey Kortunov",
    "ben-menashe": "Ari Ben-Menashe",
    "bryant": "Wes Bryant",
}

MONTHS = {"Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
          "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12}


def date_key(d, year=2026):
    try:
        parts = (d or "").strip().split()
        if len(parts) < 2:
            return datetime.min.replace(tzinfo=timezone.utc)
        day = int(parts[0].lstrip("0") or "0")
        mo = MONTHS.get(parts[1][:3].capitalize())
        if mo is None:
            return datetime.min.replace(tzinfo=timezone.utc)
        return datetime(year, mo, day, tzinfo=timezone.utc)
    except Exception:
        return datetime.min.replace(tzinfo=timezone.utc)


def canonical_for_title(title, cur_guest, cur_surname):
    tl = (title or "").lower()
    cg = (cur_guest or "").lower()
    sn = (cur_surname or "").lower()
    for k, full in CANON.items():
        if k in tl or k in cg or k in sn:
            return full
    return None


def is_x_clip(entry):
    src = (entry.get("source") or entry.get("show") or "").lower()
    if "x.com" in src or "twitter" in src:
        return True
    url = (entry.get("url") or entry.get("link") or "").lower()
    if any(x in url for x in ("x.com/", "twitter.com/", "t.co/")):
        return True
    return False


def episode_hash(url_or_title):
    return hashlib.sha1((url_or_title or "").encode("utf-8")).hexdigest()[:12]


def main():
    with open(VIDEOS) as f:
        vids = json.load(f)

    ranked = []
    demoted_x = []
    for v in vids:
        title = v.get("title") or ""
        d = date_key(v.get("date"))
        if is_x_clip(v):
            demoted_x.append({"title": title[:80], "date": v.get("date"),
                              "reason": "x_clip_excluded_from_canonical_identity"})
            continue
        full_name = canonical_for_title(title, v.get("guest"), v.get("surname"))
        ranked.append((d, v, full_name))

    ranked.sort(key=lambda t: t[0], reverse=True)

    canonical_top2 = []
    for d, v, full_name in ranked:
        if len(canonical_top2) >= 2:
            break
        if not full_name:
            continue
        canonical_top2.append({
            "date_iso": d.strftime("%Y-%m-%d"),
            "date_short": v.get("date"),
            "title": (v.get("title") or "")[:200],
            "canonical_guest_full_name": full_name,
            "surname_upper": full_name.split()[-1].upper(),
            "source_hint": v.get("show", "GU"),
            "episode_id": episode_hash((v.get("url") or v.get("title") or "")[:200]),
        })

    latest_ep_hash = hashlib.sha1(
        ("|".join(g["canonical_guest_full_name"] for g in canonical_top2)
         + "|" + (canonical_top2[0]["date_iso"] if canonical_top2 else "")
        ).encode("utf-8")
    ).hexdigest()[:16]

    out = {
        "marker": MARKER,
        "published_iso": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "latest_episode_hash": latest_ep_hash,
        "canonical_top2_episodes": canonical_top2,
        "canonical_display_names_upper": [e["surname_upper"] for e in canonical_top2],
        "excluded_x_clips": demoted_x,
        "notes": [
            "Canonical identity source: Rumble-tagged rows in videos.json.",
            "X clips (x.com/twitter.com/t.co URLs) demoted to reach-only, "
            "never displayed as canonical latest.",
            "Displayed as: '<SURNAME_1> + <SURNAME_2>' by both Tidbyt and "
            "LaMetric consumers; see push84_lametric.clean_guest_name which "
            "already reads canonical_guest_full_name first.",
        ],
    }

    with open(OUT, "w") as f:
        json.dump(out, f, indent=2)

    ts_slug = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    backup_path = f"{VIDEOS}.bak.pre-canonical-episode-v1.{ts_slug}"
    if not os.path.exists(backup_path):
        with open(backup_path, "w") as f:
            json.dump(vids, f, indent=2)

    n_updated = 0
    top_titles = {e["title"] for e in canonical_top2}
    top_map = {e["title"]: e for e in canonical_top2}
    for v in vids:
        title = (v.get("title") or "")[:200]
        if title in top_titles:
            v["canonical_guest_full_name"] = top_map[title]["canonical_guest_full_name"]
            v["canonical_surname_upper"] = top_map[title]["surname_upper"]
            v["canonical_episode_id"] = top_map[title]["episode_id"]
            n_updated += 1

    with open(VIDEOS, "w") as f:
        json.dump(vids, f, indent=2)

    print(json.dumps({
        "wrote": OUT, "backup": backup_path,
        "videos_json_rows_updated": n_updated,
        "latest_episode_hash": latest_ep_hash,
        "canonical_top2": canonical_top2,
        "excluded_x_clips_count": len(demoted_x),
        "marker": MARKER,
    }, indent=2))


if __name__ == "__main__":
    main()
