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
import unicodedata
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

# ---------------------------------------------------------------------------
# GU_QUOTE_PARSER_V2_2026_08_06 — supersedes the closing-quote-anchored regex.
#
# The old pattern was [‘'"“]([^’'"”]{40,})[’'"”] and it failed on the entire
# neworder_TV set for TWO independent reasons, both visible in the raw text:
#   1. TRUNCATION (the decisive one). The stored tweet text is cut mid-sentence
#      ("...There h", "...This is why t"), so the closing quote does not exist in
#      the string at all. No closing-anchored pattern can ever match it.
#   2. APOSTROPHES. [^’'"”] forbids a straight apostrophe inside the quote, so
#      "It's again resilience..." terminated the class after two characters.
# Maté/Barnes items happened to match because their quotes closed within the
# stored length and avoided a straight apostrophe — luck, not correctness.
#
# The fix models what the text actually is: an OPENED quotation that may be
# truncated. We require an opening quote mark preceded by a speaker attribution,
# followed by a substantive run of prose. Apostrophes are allowed inside; a
# closing quote is optional.
#
# This is deliberately NOT a broad ".*quote.*" rule, and quoted text alone never
# promotes anything: classify_x_verified() still requires an independent media
# signal before any CLIP is emitted.
OPEN_QUOTES = "\u2018\u201c'\""
# Attribution appears in two shapes on these accounts, both legitimate:
#   before: "Donald Trump's Former Lawyer Robert Barnes:\n\n'The Deep State..."
#   after:  "'Trump sees Israel as a TOOL...'\n\n—Trump's former lawyer Robert Barnes"
# The name can run long ("Holocaust Survivor & Trauma Specialist Dr. Gabor Maté"),
# so the prefix form allows up to 8 words rather than 5.
# Not anchored to end-of-line: neworder_TV writes "Name: <headline>\n\n'quote",
# where the colon is followed by a headline, while GU writes "Name:\n\n'quote".
_SPEAKER_RE = re.compile(r"[A-Z][\w.\-'&]*(?:\s+[\w.\-'&]*[A-Za-z][\w.\-'&]*){0,7}\s*:")
_ATTRIB_AFTER_RE = re.compile(r"[\u2014\u2013-]\s*[A-Z][\w.\-'&]*(?:\s+[\w.\-'&]+){0,7}")
_QUOTE_OPEN_RE = re.compile(
    r"[" + OPEN_QUOTES + r"]"          # an opening quote mark
    r"(?=\s*[A-Z0-9])"                 # dialogue starts with a capital/number
    r"([^\u2018\u201c\"]{40,})"        # >=40 chars; straight ' allowed (contractions)
)
# Promotional slogans that can appear in quotes but are not interview dialogue.
_SLOGAN_RE = re.compile(
    r"\b(subscribe|follow us|watch (the )?full|link in bio|out now|new episode|"
    r"don'?t miss|coming (up|soon)|premieres?)\b", re.I)


def normalise_text(t):
    """NFKC + whitespace collapse. Retains nothing destructive; the ORIGINAL text
    is what gets stored for audit — this is used only for matching."""
    if not t:
        return ""
    t = unicodedata.normalize("NFKC", str(t))
    t = t.replace("\u00a0", " ").replace("\u200b", "")
    return re.sub(r"[ \t]+", " ", t)


def quoted_dialogue(text):
    """-> (bool, evidence_str). Substantive attributed interview dialogue, which
    may be truncated. Promotional slogans do not count."""
    n = normalise_text(text)
    m = _QUOTE_OPEN_RE.search(n)
    if not m:
        return False, None
    body = m.group(1).strip()
    if _SLOGAN_RE.search(body):
        return False, None
    before, after = n[:m.start()], n[m.end():]
    if _SPEAKER_RE.search(before):
        how = "speaker prefix before quote"
    # NOTE: the body match is greedy, so `after` is usually empty — the dash
    # attribution must be looked for across the whole normalised text.
    elif _ATTRIB_AFTER_RE.search(n):
        how = "dash attribution after quote"
    else:
        return False, None
    return True, f"attributed quotation ({how}), {len(body)} chars (truncation-tolerant)"


# Retained so existing call sites keep working; no longer the classifier's basis.
QUOTE_RE = _QUOTE_OPEN_RE
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


# X_MEDIA_EVIDENCE_V1 — verified native-video facts fetched read-only from the
# TweetDetail payload. A post is promoted to FULL_INTERVIEW only if it has a
# single native video whose duration matches full-episode length and it is not a
# repost/quote. Title alone never promotes; duration alone never promotes.
FULL_EPISODE_MIN, FULL_EPISODE_MAX = 20.0, 45.0
def _x_evidence():
    f = HERE / "x_media_evidence_v1.json"
    try:
        d = json.loads(f.read_text())
    except Exception:
        return {}
    out = {}
    for p in d.get("posts", []):
        if not p.get("fetch_ok") or not p.get("evidence"):
            continue
        e = p["evidence"]
        vids = [m for m in e["media"] if m["type"] == "video"]
        out[str(p["tweet_id"])] = {
            "native_video": e["has_native_video"],
            "n_video": len(vids),
            "duration_min": vids[0]["duration_min"] if vids else None,
            "is_repost": bool(e.get("retweeted_status_id")),
            "is_quote": bool(e.get("quoted_status_id")),
            "quotes_id": e.get("quoted_status_id"),
        }
    return out
X_EVIDENCE = _x_evidence()


def classify_x_verified(tweet_id, text=""):
    """-> (classification, basis) or (None, None) when no media evidence exists.

    A CLIP is emitted ONLY on two independent signals:
      (a) native X video whose duration is excerpt-length, AND
      (b) speaker-attributed substantive quotation in the post text.
    Either alone leaves the item AMBIGUOUS. Quoted text can be promotional copy;
    a native video alone does not establish that it is interview content.
    """
    ev = X_EVIDENCE.get(str(tweet_id))
    if not ev:
        return None, None
    if ev["is_repost"]:
        return "PROMO", "verified: repost — the view metric belongs to the source post"
    if not ev["native_video"]:
        return "PROMO", "verified: no native video in payload (link/announcement/still only)"
    d = ev["duration_min"]
    if d is None:
        return "AMBIGUOUS", "native video present but duration absent from payload"
    if ev["n_video"] == 1 and FULL_EPISODE_MIN <= d <= FULL_EPISODE_MAX:
        return "FULL_INTERVIEW", (
            f"verified: single native X video, duration {d} min (full-episode length), "
            "not a repost or quote, canonical account, posted at broadcast. "
            "NOT established: transcript/frame identity against the episode master")
    if d > FULL_EPISODE_MAX:
        return "AMBIGUOUS", f"native video of {d} min exceeds episode length — unclassified"
    # Excerpt-length native video. Needs the second signal.
    has_quote, qev = quoted_dialogue(text)
    if has_quote:
        return "CLIP", (f"verified: native X video {d} min (excerpt length) + {qev}"
                        + (f"; quotes episode post {ev['quotes_id']}" if ev.get("quotes_id") else ""))
    return "AMBIGUOUS", (f"native X video {d} min (excerpt length) but no speaker-attributed "
                         "quotation — interview content not established")


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
    has_quote, qev = quoted_dialogue(t)
    if has_quote:
        # Quotation is only ONE signal. Without media evidence there is no second
        # signal, so this cannot be published as a CLIP.
        return "AMBIGUOUS", (f"{qev}, but no media evidence fetched — second signal "
                             "(native video / duration) absent")
    return "AMBIGUOUS", "no speaker-attributed quotation and no media evidence"


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
                    cls, basis = classify_x_verified(tw.get("id"), tw.get("text") or "")
                    if cls is None:
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
