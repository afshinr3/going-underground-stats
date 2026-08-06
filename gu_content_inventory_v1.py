#!/usr/bin/env python3
"""GU_CONTENT_INVENTORY_V1 — per-post content inventory for GU Stats.

WHY: episode totals counted exactly ONE canonical full-interview post per
platform. Clips were never in the inventory, so an episode whose reach is mostly
clips read far below its true figure. This builds the missing per-post layer.

SOURCES (all local, already-collected caches — no new scraping, and nothing here
touches SignalFlash, its fetch layer, or any production process):
  RumbleMonitor/x_2026.json       results.{GU,NO}.tweets_2026[]  (id, view_count)
  RumbleMonitor/ig_2026.json      posts_all[]                    (shortcode, views, likes)
  RumbleMonitor/yt_2026.json      videos_all[]                   (id, views)
  RumbleMonitor/rumble_2026.json  videos_all[]                   (url, views)

URLs are composed only from real platform IDs present in those caches. No URL is
invented. A post that cannot be measured is recorded with an explicit
unavailable status and a null value -- never zero.
"""
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

RM = Path.home() / "RumbleMonitor"
HERE = Path(__file__).resolve().parent
OUT = HERE / "gu_content_inventory_v1.json"

# Canonical accounts only. Anything published elsewhere is out of scope.
CANONICAL_ACCOUNTS = {
    "x":         {"GU": "GUnderground_TV", "NO": "neworder_TV"},
    "instagram": {"GU": "goingundergroundtv", "NO": "goingundergroundtv"},
    "youtube":   {"GU": "GoingUndergroundRT", "NO": "GoingUndergroundRT"},
    "rumble":    {"GU": "GoingUndergroundTV", "NO": "GoingUndergroundTV"},
}

# Episode identity comes from the published health snapshot, not from guesswork.
EPISODES = [
    {"episode_key": "mate_20260803", "guest": "Gabor Maté", "show": "GU",
     "match": ("maté", "mate", "gabor"), "pub_iso": "2026-08-03T22:18:20Z",
     "canonical_video_id": "PUw_r6rI5PY"},
    {"episode_key": "barnes_20260801", "guest": "Robert Barnes", "show": "GU",
     "match": ("barnes",), "pub_iso": "2026-08-01T21:22:19Z",
     "canonical_video_id": "N_ysv6Gh9Ac"},
    {"episode_key": "baharoon_20260802", "guest": "Mohammed Baharoon", "show": "NO",
     "match": ("baharoon",), "pub_iso": "2026-08-02T06:30:06Z",
     "canonical_video_id": "F8hxaEtl9Y8"},
]

# A clip is published around the broadcast. Bound the window so an unrelated later
# mention of the same guest is not silently absorbed into this episode.
WINDOW_BEFORE = timedelta(days=2)
WINDOW_AFTER = timedelta(days=30)


def _iso(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _now():
    return datetime.now(timezone.utc)


def _parse(s):
    if not s:
        return None
    s = str(s)
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S",
                "%a %b %d %H:%M:%S %z %Y", "%Y-%m-%dT%H:%M:%S.%f", "%Y%m%d"):
        try:
            d = datetime.strptime(s, fmt)
            return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def _matches(text, ep):
    t = (text or "").lower()
    return any(k in t for k in ep["match"])


def _in_window(dt, ep):
    if dt is None:
        return False
    pub = _parse(ep["pub_iso"])
    return (pub - WINDOW_BEFORE) <= dt <= (pub + WINDOW_AFTER)


def _measure(raw):
    """-> (value, status). Unknown never becomes 0."""
    if raw is None:
        return None, "unavailable"
    if isinstance(raw, bool):
        return None, "unavailable"
    if isinstance(raw, (int, float)):
        return int(raw), "measured"
    s = str(raw).strip()
    if s == "" or s in ("?", "N/A", "NA", "ERR", "ERROR"):
        return None, "unavailable"
    try:
        return int(float(s.replace(",", ""))), "measured"
    except ValueError:
        return None, "unavailable"


def _load(name):
    try:
        return json.loads((RM / name).read_text()), None
    except Exception as exc:
        return None, f"{name}: {exc}"


def build():
    errors = []
    items = []
    seen = {}          # (platform, content_id) -> episode_key, for dedup
    duplicates = []

    def add(ep, platform, account, content_id, url, pub_dt, ctype, metric,
            raw_value, measured_at):
        key = (platform, str(content_id))
        if key in seen:
            duplicates.append({"platform": platform, "content_id": str(content_id),
                               "first_episode": seen[key], "also_matched": ep["episode_key"]})
            return
        seen[key] = ep["episode_key"]
        value, status = _measure(raw_value)
        items.append({
            "episode_key": ep["episode_key"],
            "episode_guest": ep["guest"],
            "show": ep["show"],
            "platform": platform,
            "publishing_account": account,
            "canonical_url": url,
            "platform_content_id": str(content_id),
            "published_iso": _iso(pub_dt) if pub_dt else None,
            "content_type": ctype,
            "metric": metric,
            "measured_at_iso": measured_at,
            "value": value,
            "status": status,
        })

    # ---- X ----------------------------------------------------------------
    xd, err = _load("x_2026.json")
    if err:
        errors.append(err)
    else:
        measured_at = xd.get("last_successful_scrape_at") or xd.get("generated_at")
        for show, blob in (xd.get("results") or {}).items():
            handle = blob.get("handle") or CANONICAL_ACCOUNTS["x"].get(show)
            for tw in blob.get("tweets_2026") or []:
                dt = _parse(tw.get("created_dt") or tw.get("created_at"))
                for ep in EPISODES:
                    if ep["show"] != show or not _matches(tw.get("text"), ep) or not _in_window(dt, ep):
                        continue
                    add(ep, "x", handle, tw.get("id"),
                        f"https://x.com/{handle}/status/{tw.get('id')}", dt,
                        "clip_or_promo", "view_count", tw.get("view_count"), measured_at)

    # ---- Instagram --------------------------------------------------------
    igd, err = _load("ig_2026.json")
    if err:
        errors.append(err)
    else:
        measured_at = igd.get("generated_at")
        acct = CANONICAL_ACCOUNTS["instagram"]["GU"]
        for p in igd.get("posts_all") or []:
            ts = p.get("taken_at")
            dt = datetime.fromtimestamp(ts, timezone.utc) if isinstance(ts, (int, float)) else None
            for ep in EPISODES:
                if not _matches(p.get("caption"), ep) or not _in_window(dt, ep):
                    continue
                sc = p.get("shortcode")
                add(ep, "instagram", acct, p.get("id"),
                    f"https://www.instagram.com/p/{sc}/" if sc else None, dt,
                    "clip" if p.get("product_type") == "clips" else "post",
                    "views", p.get("views"), measured_at)

    # ---- YouTube ----------------------------------------------------------
    ytd, err = _load("yt_2026.json")
    if err:
        errors.append(err)
    else:
        measured_at = ytd.get("generated_at")
        acct = CANONICAL_ACCOUNTS["youtube"]["GU"]
        for v in ytd.get("videos_all") or []:
            dt = _parse(v.get("date_iso") or v.get("upload_date"))
            for ep in EPISODES:
                if not _matches(v.get("title"), ep) or not _in_window(dt, ep):
                    continue
                vid = v.get("id")
                add(ep, "youtube", acct, vid,
                    f"https://www.youtube.com/watch?v={vid}" if vid else None, dt,
                    "full_interview" if vid == ep["canonical_video_id"] else "clip",
                    "views", v.get("views"), measured_at)

    # ---- Rumble -----------------------------------------------------------
    rud, err = _load("rumble_2026.json")
    if err:
        errors.append(err)
    else:
        measured_at = rud.get("generated_at")
        acct = CANONICAL_ACCOUNTS["rumble"]["GU"]
        for v in rud.get("videos_all") or []:
            dt = _parse(v.get("date_iso"))
            for ep in EPISODES:
                if not _matches(v.get("title"), ep) or not _in_window(dt, ep):
                    continue
                url = v.get("url")
                cid = None
                if url:
                    m = re.search(r"/(v[0-9a-z]+)-", url)
                    cid = m.group(1) if m else url
                add(ep, "rumble", acct, cid, url, dt,
                    "full_interview" if "full" in (v.get("title") or "").lower() else "clip",
                    "views", v.get("views"), measured_at)

    # ---- roll-up: unknown is excluded and named, never counted as zero -----
    episodes = {}
    for ep in EPISODES:
        rows = [i for i in items if i["episode_key"] == ep["episode_key"]]
        by_type = {}
        for ctype in ("full_interview", "clip", "clip_or_promo", "post"):
            sub = [r for r in rows if r["content_type"] == ctype]
            known = [r["value"] for r in sub if r["status"] == "measured"]
            by_type[ctype] = {
                "n_items": len(sub),
                "n_measured": len(known),
                "n_unavailable": len(sub) - len(known),
                "sum_measured": sum(known) if known else (0 if sub else None),
            }
        unavailable = [r for r in rows if r["status"] != "measured"]
        all_known = [r["value"] for r in rows if r["status"] == "measured"]
        episodes[ep["episode_key"]] = {
            "guest": ep["guest"], "show": ep["show"], "pub_iso": ep["pub_iso"],
            "canonical_video_id": ep["canonical_video_id"],
            "n_items": len(rows),
            "by_content_type": by_type,
            "sum_measured_all": sum(all_known) if all_known else None,
            "n_unavailable": len(unavailable),
            "total_is_partial": bool(unavailable),
            "platforms_present": sorted({r["platform"] for r in rows}),
            "platforms_absent": sorted(set(CANONICAL_ACCOUNTS) - {r["platform"] for r in rows}),
        }

    out = {
        "marker": "GU_CONTENT_INVENTORY_V1",
        "generated_iso": _iso(_now()),
        "sources": {n: str(RM / n) for n in
                    ("x_2026.json", "ig_2026.json", "yt_2026.json", "rumble_2026.json")},
        "url_policy": "URLs composed only from platform IDs present in the caches; none invented.",
        "zero_policy": "unavailable/unmapped/unparseable -> value null + status, never 0.",
        "match_window": {"before_days": WINDOW_BEFORE.days, "after_days": WINDOW_AFTER.days},
        "canonical_accounts": CANONICAL_ACCOUNTS,
        "source_errors": errors,
        "duplicates_suppressed": duplicates,
        "episodes": episodes,
        "items": sorted(items, key=lambda r: (r["episode_key"], r["platform"],
                                              r["published_iso"] or "")),
    }
    OUT.write_text(json.dumps(out, indent=2, ensure_ascii=False))
    return out


if __name__ == "__main__":
    res = build()
    print(f"wrote {OUT}  items={len(res['items'])}  dupes_suppressed={len(res['duplicates_suppressed'])}")
    if res["source_errors"]:
        print("SOURCE ERRORS:", res["source_errors"])
    for k, e in res["episodes"].items():
        bt = e["by_content_type"]
        print(f"\n{e['guest']} ({e['show']})  items={e['n_items']}  partial={e['total_is_partial']}")
        print(f"  full_interview: n={bt['full_interview']['n_items']} sum={bt['full_interview']['sum_measured']}")
        print(f"  clip:           n={bt['clip']['n_items']} sum={bt['clip']['sum_measured']}")
        print(f"  clip_or_promo:  n={bt['clip_or_promo']['n_items']} sum={bt['clip_or_promo']['sum_measured']}")
        print(f"  platforms: present={e['platforms_present']} absent={e['platforms_absent']}")
        print(f"  unavailable items: {e['n_unavailable']}")
