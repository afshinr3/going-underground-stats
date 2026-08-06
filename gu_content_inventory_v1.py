#!/usr/bin/env python3
"""GU_CONTENT_INVENTORY_V2 — classified per-post content inventory for GU Stats.

V2 applies the operator's classification rules (2026-08-06) and fixes a matching
defect in V1.

MATCHING DEFECT FIXED: V1 matched guest keys as bare substrings, so "mate" hit
"underestimate" and pulled an unrelated Mearsheimer post into Maté's inventory.
Matching is now word-boundary anchored.

CLASSIFICATION (operator rules):
  FULL_INTERVIEW  canonical full-length NATIVE upload on an official account,
                  identity verified by MORE than title: canonical account +
                  platform content ID + publication timing. At most one per
                  episode per platform. Where a platform has no VERIFIED native
                  full interview, it is recorded N/A -- never zero.
  CLIP            native playable media carrying a substantive excerpt, identity
                  linked to the interview. Verified here via QUOTED DIALOGUE (an
                  operator-accepted method) on X, and via native-video media type
                  on Instagram.
  PROMO           link-only, stills/cards, generic announcements, "coming up"
                  trailers, promo montages, and reposts (RT) of already-counted
                  content. Recorded separately, EXCLUDED from clip totals.
  AMBIGUOUS       cannot be verified from available data. EXCLUDED from published
                  totals, exposed in the report. Never guessed.

HONEST LIMIT: these caches carry no media metadata, durations, transcripts or
fingerprints. So "contains native playable video" is NOT directly verifiable on
X, and duration is not verifiable anywhere. Anything that cannot be established
is AMBIGUOUS by rule, not by estimate.

Sources: RumbleMonitor/{x,ig,yt,rumble}_2026.json. No new scraping. URLs composed
only from real platform IDs. Unavailable -> null + status, never 0.
"""
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

RM = Path.home() / "RumbleMonitor"
HERE = Path(__file__).resolve().parent
OUT = HERE / "gu_content_inventory_v1.json"   # filename kept stable by operator request

CANONICAL_ACCOUNTS = {
    "x":         {"GU": "GUnderground_TV", "NO": "neworder_TV"},
    "instagram": {"GU": "goingundergroundtv", "NO": "goingundergroundtv"},
    "youtube":   {"GU": "GoingUndergroundRT", "NO": "GoingUndergroundRT"},
    "rumble":    {"GU": "GoingUndergroundTV", "NO": "GoingUndergroundTV"},
}

EPISODES = [
    {"episode_key": "mate_20260803", "guest": "Gabor Maté", "show": "GU",
     "match": (r"mat[ée]", r"gabor"), "pub_iso": "2026-08-03T22:18:20Z",
     "canonical_video_id": "PUw_r6rI5PY"},
    {"episode_key": "barnes_20260801", "guest": "Robert Barnes", "show": "GU",
     "match": (r"barnes",), "pub_iso": "2026-08-01T21:22:19Z",
     "canonical_video_id": "N_ysv6Gh9Ac"},
    {"episode_key": "baharoon_20260802", "guest": "Mohammed Baharoon", "show": "NO",
     "match": (r"baharoon",), "pub_iso": "2026-08-02T06:30:06Z",
     "canonical_video_id": "F8hxaEtl9Y8"},
]

WINDOW_BEFORE = timedelta(days=2)
WINDOW_AFTER = timedelta(days=30)

# Substantive excerpt: attributed dialogue in typographic or straight quotes,
# long enough not to be a strapline.
QUOTE_RE = re.compile(r"[‘'\"“]([^’'\"”]{40,})[’'\"”]")
# Future-tense / announcement -> the interview has not aired or is being trailed.
PROMO_RE = re.compile(
    r"\b(we'?ll be joined|will be joined|coming (up|soon)|tomorrow|tonight|"
    r"today'?s going underground|monday'?s|saturday'?s|"
    r"full (interview|episode) (out|now|available)|watch (the )?full|"
    r"subscribe|don'?t miss|catch (it|the)|premieres?)\b", re.I)
REPOST_RE = re.compile(r"^RT @", re.I)
# A post whose only payload is a link.
LINKONLY_RE = re.compile(r"^\s*(https?://\S+\s*)+$")


def _iso(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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
    """Word-boundary match. V1's substring match hit 'underestimate' for 'mate'."""
    t = (text or "").lower()
    return any(re.search(rf"\b{k}\b", t) for k in ep["match"])


def _in_window(dt, ep):
    if dt is None:
        return False
    pub = _parse(ep["pub_iso"])
    return (pub - WINDOW_BEFORE) <= dt <= (pub + WINDOW_AFTER)


def _measure(raw):
    if raw is None or isinstance(raw, bool):
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


def classify_x(text):
    """-> (classification, basis). Native-video presence is NOT in this cache, so
    anything without verifiable excerpt evidence is AMBIGUOUS, never assumed."""
    t = text or ""
    if REPOST_RE.search(t):
        return "PROMO", "repost (RT) of already-counted content"
    if LINKONLY_RE.match(t.strip()):
        return "PROMO", "link-only post"
    if PROMO_RE.search(t):
        return "PROMO", "announcement/trailer language (interview trailed, not excerpted)"
    # Episode-release posts ("NEW EPISODE OF ...") are how the full episode is
    # published natively on X. Canonical account, content ID and publication
    # timing all check out, but DURATION is not in this cache, so the rule
    # "verify with more than the title" cannot be satisfied. Rather than guess
    # either way, surface it as a candidate for operator confirmation.
    if re.search(r"\bnew episode of\b", t, re.I):
        return "AMBIGUOUS", ("candidate platform full interview (episode-release post on "
                             "canonical account); duration/transcript unavailable to verify")
    m = QUOTE_RE.search(t)
    if m:
        return "CLIP", f"quoted dialogue, {len(m.group(1))} chars"
    return "AMBIGUOUS", "no quoted excerpt and native media not verifiable from cache"


def classify_ig(post):
    mt, pt = post.get("media_type"), post.get("product_type")
    if mt == 2 and pt == "clips":
        return "CLIP", "native video reel (media_type=2, product_type=clips)"
    if mt == 2:
        return "CLIP", "native video (media_type=2)"
    if mt == 1:
        return "PROMO", "still image, not native playable media"
    if mt == 8:
        return "PROMO", "carousel container, not a single native video excerpt"
    return "AMBIGUOUS", f"unrecognised media_type={mt} product_type={pt}"


def _load(name):
    try:
        return json.loads((RM / name).read_text()), None
    except Exception as exc:
        return None, f"{name}: {exc}"


def build():
    errors, items, duplicates = [], [], []
    seen = {}

    def add(ep, platform, account, content_id, url, pub_dt, classification, basis,
            metric, raw_value, measured_at, scope="cumulative_to_measurement"):
        key = (platform, str(content_id))
        if key in seen:
            duplicates.append({"platform": platform, "content_id": str(content_id),
                               "first_episode": seen[key], "also_matched": ep["episode_key"]})
            return
        seen[key] = ep["episode_key"]
        value, status = _measure(raw_value)
        if classification == "AMBIGUOUS" and status == "measured":
            status = "ambiguous"          # measured but not publishable
        items.append({
            "episode_key": ep["episode_key"], "episode_guest": ep["guest"],
            "show": ep["show"], "platform": platform, "publishing_account": account,
            "canonical_url": url, "platform_content_id": str(content_id),
            "published_iso": _iso(pub_dt) if pub_dt else None,
            "classification": classification, "classification_basis": basis,
            "metric": metric, "metric_scope": scope,
            "measured_at_iso": measured_at, "value": value, "status": status,
        })

    # ---- X ----
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
                    cls, basis = classify_x(tw.get("text"))
                    add(ep, "x", handle, tw.get("id"),
                        f"https://x.com/{handle}/status/{tw.get('id')}", dt,
                        cls, basis, "view_count", tw.get("view_count"), measured_at)

    # ---- Instagram ----
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
                cls, basis = classify_ig(p)
                sc = p.get("shortcode")
                raw = p.get("views")
                # A carousel/still reports views 0 because the metric does not
                # apply -- that is unavailable, not a measured zero.
                if cls == "PROMO" and raw in (0, "0", None):
                    raw = None
                add(ep, "instagram", acct, p.get("id"),
                    f"https://www.instagram.com/p/{sc}/" if sc else None, dt,
                    cls, basis, "views", raw, measured_at)

    # ---- YouTube ----
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
                if vid == ep["canonical_video_id"]:
                    cls = "FULL_INTERVIEW"
                    basis = "canonical account + platform content ID + publication timing"
                else:
                    cls, basis = "CLIP", "native upload on canonical channel, not the canonical full ID"
                add(ep, "youtube", acct, vid,
                    f"https://www.youtube.com/watch?v={vid}" if vid else None, dt,
                    cls, basis, "views", v.get("views"), measured_at)

    # ---- Rumble ----
    ru, err = _load("rumble_2026.json")
    if err:
        errors.append(err)
    else:
        measured_at = ru.get("generated_at")
        acct = CANONICAL_ACCOUNTS["rumble"]["GU"]
        for v in ru.get("videos_all") or []:
            dt = _parse(v.get("date_iso"))
            for ep in EPISODES:
                if not _matches(v.get("title"), ep) or not _in_window(dt, ep):
                    continue
                url = v.get("url")
                m = re.search(r"/(v[0-9a-z]+)-", url or "")
                cid = m.group(1) if m else url
                # No duration/transcript in this cache, so a Rumble upload cannot
                # be VERIFIED as the canonical full interview. Rule: do not guess.
                add(ep, "rumble", acct, cid, url, dt, "AMBIGUOUS",
                    "native upload but duration/transcript unavailable to verify full vs excerpt",
                    "views", v.get("views"), measured_at)

    # ---- roll-up ----
    episodes = {}
    for ep in EPISODES:
        rows = [i for i in items if i["episode_key"] == ep["episode_key"]]
        buckets = {}
        for cls in ("FULL_INTERVIEW", "CLIP", "PROMO", "AMBIGUOUS"):
            sub = [r for r in rows if r["classification"] == cls]
            known = [r["value"] for r in sub if r["status"] == "measured"]
            any_val = [r["value"] for r in sub if r["value"] is not None]
            buckets[cls] = {
                "n_items": len(sub), "n_measured": len(known),
                "n_not_measured": len(sub) - len(known),
                "sum_measured": sum(known) if known else (None if not sub else 0),
                "excluded_reach_not_published": (sum(any_val) if any_val and not known else None),
                "platforms": sorted({r["platform"] for r in sub}),
            }
        # Full interview per platform: N/A where not verified, never zero.
        fi_by_platform = {}
        for plat in CANONICAL_ACCOUNTS:
            hit = [r for r in rows if r["classification"] == "FULL_INTERVIEW" and r["platform"] == plat]
            fi_by_platform[plat] = ({"value": hit[0]["value"], "status": hit[0]["status"],
                                     "url": hit[0]["canonical_url"]} if hit
                                    else {"value": None, "status": "N/A",
                                          "reason": "no verified native full interview on this platform"})
        fi = buckets["FULL_INTERVIEW"]["sum_measured"]
        cl = buckets["CLIP"]["sum_measured"]
        editorial = (fi or 0) + (cl or 0) if (fi is not None or cl is not None) else None
        episodes[ep["episode_key"]] = {
            "guest": ep["guest"], "show": ep["show"], "pub_iso": ep["pub_iso"],
            "canonical_video_id": ep["canonical_video_id"],
            "n_items": len(rows), "by_classification": buckets,
            "full_interview_by_platform": fi_by_platform,
            "editorial_total_measured": editorial,
            "promo_reach_measured": buckets["PROMO"]["sum_measured"],
            "excluded_ambiguous_reach": buckets["AMBIGUOUS"].get("excluded_reach_not_published"),
            "total_is_partial": bool(buckets["AMBIGUOUS"]["n_items"]
                                     or any(v["status"] == "N/A" for v in fi_by_platform.values())),
        }

    out = {
        "marker": "GU_CONTENT_INVENTORY_V2",
        "supersedes": "GU_CONTENT_INVENTORY_V1",
        "generated_iso": _iso(datetime.now(timezone.utc)),
        "metric_scope": "cumulative_to_measurement_timestamp",
        "scope_note": "Cumulative per-post totals. NOT comparable with the dashboard "
                      "'1 Week' rolling window and must never be summed into it.",
        "url_policy": "URLs composed only from platform IDs present in the caches; none invented.",
        "zero_policy": "unavailable/ambiguous/N-A -> value null + status, never 0.",
        "promo_policy": "PROMO reach reported separately and excluded from the editorial total.",
        "ambiguous_policy": "excluded from published totals, exposed here, never guessed.",
        "verification_limits": [
            "caches carry no media metadata: native-video presence is not directly verifiable on X",
            "no durations available: platform full-interview identity cannot be duration-verified",
            "no transcripts or fingerprints available for content-identity matching",
        ],
        "canonical_accounts": CANONICAL_ACCOUNTS,
        "source_errors": errors, "duplicates_suppressed": duplicates,
        "episodes": episodes,
        "items": sorted(items, key=lambda r: (r["episode_key"], r["classification"],
                                              r["platform"], r["published_iso"] or "")),
    }
    OUT.write_text(json.dumps(out, indent=2, ensure_ascii=False))
    return out


if __name__ == "__main__":
    r = build()
    print(f"wrote {OUT} items={len(r['items'])} dupes={len(r['duplicates_suppressed'])}")
    for k, e in r["episodes"].items():
        b = e["by_classification"]
        print(f"\n{e['guest']}  items={e['n_items']} partial={e['total_is_partial']}")
        for c in ("FULL_INTERVIEW", "CLIP", "PROMO", "AMBIGUOUS"):
            print(f"  {c:15s} n={b[c]['n_items']:3d} sum={b[c]['sum_measured']}")
        print(f"  editorial(full+clip)={e['editorial_total_measured']}  promo={e['promo_reach_measured']}")
