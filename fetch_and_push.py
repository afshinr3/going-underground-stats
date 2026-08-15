#!/usr/bin/env python3
"""
Cloud-based stats fetcher — runs on GitHub Actions every 15 minutes.
Fetches X / YT / IG view counts for both Going Underground and New Order shows.

Outputs:
  videos.json            — Going Underground (15 latest, X handle GUnderground_TV, YT UCjY51YgQzYxD5kX-BNobpxA)
  videos_neworder.json   — New Order (latest, X handle NewOrder_TV, YT UC7FXwSQPOlq-eqXjpS3TL8g)

Pushes the GU animation to both Tidbyts.
"""

import asyncio
import base64
import io
import json
import os
import re
import sys
import urllib.parse
import urllib.request

import hashlib

import requests

import gu_parser
from PIL import Image, ImageDraw, ImageFont
from playwright.async_api import async_playwright

ROOT = os.path.dirname(os.path.abspath(__file__))

# CANONICAL_FIELD_EMISSION_V1_2026_07_11 --------------------------------------
# Source-of-truth CANON_MAP mirrored verbatim from gu_canonical_backfill_v2.py.
# Every emitted videos.json row carries three canonical fields so downstream
# consumers (Tidbyt renderer, push84_lametric.py) NEVER read the raw broken
# `extract_guest()` output (e.g. "Ukraine Proxy War" for Carden ep, or the
# truncated "Ex-UK Defence Minister Tobias " for Ellwood ep).
CANON_MAP = {
    "ellwood":     "Tobias Ellwood",
    "wilkerson":   "Lawrence Wilkerson",
    "kucinich":    "Dennis Kucinich",
    "pyne":        "David Pyne",
    "kortunov":    "Andrey Kortunov",
    "trenin":      "Dmitri Trenin",
    "blumenthal":  "Max Blumenthal",
    "mearsheimer": "John Mearsheimer",
    "shlaim":      "Avi Shlaim",
    "sibal":       "Kanwal Sibal",
    "bhaskar":     "C. Uday Bhaskar",
    "sood":        "Vikram Sood",
    "sachs":       "Jeffrey Sachs",
    "wolff":       "Richard Wolff",
    "bolton":      "John Bolton",
    "hanke":       "Steve Hanke",
    "keen":        "Steve Keen",
    "olmert":      "Ehud Olmert",
    "postol":      "Theodore Postol",
    "roberts":     "Paul Craig Roberts",
    "weihua":      "Chen Weihua",
    "weiwei":      "Zhang Weiwei",
    "ben-menashe": "Ari Ben-Menashe",
    "menashe":     "Ari Ben-Menashe",
    "bryant":      "Wes Bryant",
    "carden":      "James Carden",
    # LEGACY_GUEST_PREFIX_SCRUB_V1_2026_07_20 — surnames appearing in current
    # feeds without a CANON_MAP entry; adds full canonical name so the scrub
    # can restore "Former Economic Hitman John Perkins" -> "John Perkins".
    "perkins":     "John Perkins",
    "rickards":    "Jim Rickards",
    "sakwa":       "Richard Sakwa",
    "macgregor":   "Douglas Macgregor",
    "fritz":       "Dennis Fritz",
    "freeman":     "Chas Freeman",
    "flynn":       "Michael Flynn",
    "clark":       "Wesley Clark",
    "vallely":     "Paul Vallely",
    "astore":      "William J. Astore",
}
_CANON_BAD_PREFIXES = ("Ex-", "Former ", "Fmr ", "SLAMS ", "BLASTS ",
                       "REVEALS ", "EXPOSES ", "WARNS ", "'", "\u2018", "\u2019")



# YT_CONTENT_TYPE_V3_2026_07_17 — richer YT content-type classifier per operator directive.
def _yt_classify_content(link_href, title, description=""):
    """Return (content_type, confidence) using URL + title + description signals.
    Never returns "EPISODE" without a positive episode marker; the safe default
    is EPISODE_UNCLASSIFIED (still creates a new production but flags for audit)."""
    t = (title or "").upper()
    desc = (description or "").upper()
    lh = link_href or ""
    # SHORT — definitive URL
    if "/shorts/" in lh:
        return ("SHORT", "HIGH")
    # SHORT — hashtag markers
    if "#SHORTS" in t or "#SHORT" in t or "#SHORTS" in desc or "#SHORT" in desc:
        return ("SHORT", "HIGH")
    # EPISODE — canonical show markers
    #
    # EPISODE_MARKER_HOUSE_PHRASE_V1_2026_08_09 — the first two markers below are
    # phrases neither channel has ever actually published. Every real full episode
    # opens its description with "On this episode of <show>, we speak to ...", so
    # without that phrase here NO episode ever earned an EPISODE/HIGH verdict: they
    # all fell through to EPISODE_UNCLASSIFIED, and _v4_gate_unclassified only admits
    # a video whose canonical episode id is ALREADY in videos.json. A brand-new
    # episode never is, so the YouTube path could not create one at all — it could
    # only ever re-recognise episodes some other path had already inserted. The
    # 7 Aug Hasan Ünal episode sat in the audit queue for two days on that rule.
    #
    # Verified against the live feeds before adding: the phrase is present on all
    # four most recent Going Underground episodes and on the New Order episode of
    # 9 Aug, and absent from the New Order quote-clips, so it separates full
    # episodes from clips rather than merely admitting everything.
    for m in ("ON THIS EPISODE OF GOING UNDERGROUND",
              "ON THIS EPISODE OF NEW ORDER",
              "NEW EPISODE OF GOING UNDERGROUND",
              "NEW EPISODE OF NEW ORDER",
              "SPECIAL EPISODE OF",
              "SEASON FINALE EPISODE OF",
              "SEASON PREMIERE EPISODE OF"):
        if m in t or m in desc:
            return ("EPISODE", "HIGH")
    # CLIP — explicit markers
    if any(t.startswith(p) for p in ("CLIP:", "HIGHLIGHT:", "EXCERPT:")):
        return ("CLIP", "HIGH")
    for w in ("HIGHLIGHT REEL", "EXCERPT FROM", "CUT FROM", "PART 1 OF", "PART 2 OF"):
        if w in t:
            return ("CLIP", "HIGH")
    # Ambiguous — /watch?v= without positive markers; default to
    # EPISODE_UNCLASSIFIED so downstream can audit false negatives.
    return ("EPISODE_UNCLASSIFIED", "LOW")


# YT_CONTENT_TYPE_V4B_2026_07_17 — EPISODE_UNCLASSIFIED gating helper.
# EPISODE/HIGH creates automatically. SHORT/CLIP never create.
# EPISODE_UNCLASSIFIED matches to existing canonical episode ceid; if none,
# routes to audit queue and skips creation.
def _v4_gate_unclassified(ceid, title, link_href, pub, description,
                            cfn, emit_surname, emit_guest, surname_display,
                            short_date, root_dir):
    """Return True to allow emission, False to skip. Side-effect: writes to
    audit queue when unmatched. Never raises."""
    import json as _j, os as _o, datetime as _dt
    try:
        # Load known canonical episode IDs from both production files
        known = set()
        for _fname in ("videos.json", "videos_neworder.json"):
            _fp = _o.path.join(root_dir, _fname)
            if _o.path.exists(_fp):
                try:
                    for _e in _j.load(open(_fp)) or []:
                        _c = (_e or {}).get("canonical_episode_id")
                        if _c: known.add(_c)
                except Exception: pass
        if ceid and ceid in known:
            # Existing canonical episode; views attach via surname map elsewhere
            print("  V4_UNCLASSIFIED_MATCHED_EXISTING_CANONICAL ceid=" + str(ceid))
            return False
        # Route to audit queue
        row = {
            "iso_flagged": _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "marker": "YT_CONTENT_TYPE_V4B_2026_07_17",
            "reason": "EPISODE_UNCLASSIFIED_no_canonical_match",
            "title": title, "link_href": link_href, "pub": pub,
            "description": (description or "")[:500],
            "canonical_episode_id": ceid,
            "canonical_guest_full_name": cfn,
            "canonical_surname_upper": emit_surname,
            "extractor_guest": emit_guest,
            "extractor_surname": surname_display,
            "date_short": short_date,
        }
        _adir = _o.path.join(root_dir, "docs")
        try: _o.makedirs(_adir, exist_ok=True)
        except Exception: pass
        with open(_o.path.join(_adir, "audit_queue_unclassified_v1.jsonl"), "a") as _af:
            _af.write(_j.dumps(row, ensure_ascii=False) + "\n")
        print("  V4_UNCLASSIFIED_HELD_FOR_AUDIT title=" + title[:60])
    except Exception as _e:
        print("  V4_HELPER_ERR: " + str(_e))
    return False







def _canonical_from_title(title, cur_guest, cur_surname):
    """Return (canonical_full_name_or_None, canonical_surname_upper_or_None, episode_id).

    episode_id is ALWAYS returned — a deterministic 12-hex hash of the title
    (falls back to a hash of surname if title empty). Full name / surname
    are returned only when a CANON_MAP hit or a "clean-looking" current guest
    is available. Downstream consumers can then unambiguously choose canonical
    values over the broken extractor output.
    """
    t = (title or "").strip()
    tl = t.lower()
    canon = None
    # 1) Surname-substring scan on title
    for _sn, _cn in CANON_MAP.items():
        if _sn in tl:
            canon = _cn
            break
    # 2) Current guest field
    if not canon and cur_guest:
        cgl = cur_guest.lower()
        for _sn, _cn in CANON_MAP.items():
            if _sn in cgl:
                canon = _cn
                break
    # 3) Clean-looking current guest passes through as canonical
    if not canon and cur_guest and " " in cur_guest and not cur_guest.endswith(" "):
        if not any(cur_guest.startswith(p) for p in _CANON_BAD_PREFIXES):
            last = cur_guest.split()[-1]
            if not (last[:1].isupper() and last.endswith(
                    ("rat", "ing", "tio", "ion", "ent", "ies", "nes")) and len(last) < 12):
                canon = cur_guest
    # Deterministic episode id: 12 hex chars of sha1(title) — stable across runs.
    hash_src = t if t else (cur_guest or cur_surname or "")
    episode_id = hashlib.sha1(hash_src.encode("utf-8")).hexdigest()[:12]
    if canon:
        cs_upper = canon.split()[-1].upper()
    elif cur_surname:
        cs_upper = cur_surname.upper()
        canon = None  # do not fabricate a full name we do not know
    else:
        cs_upper = None
    return (canon, cs_upper, episode_id)
# ---------------------------------------------------------------------------

# CANONICAL_URL_BIND_V1_2026_07_20 --------------------------------------------
# URL-identity binding for videos_neworder.json / videos.json rows.
# A "canonical production" is identified by (channel_id, video_id) of its
# full-length EPISODE upload. Shorts and clips must NEVER establish a new
# production or override a production's canonical date. This block provides
# helpers to fetch the current RSS EPISODE-class set and bind rows to them.

import hashlib as _hashlib_v1  # already imported at module top; safe alias
_URL_BIND_CACHE = {}  # channel_id -> {video_id: metadata}



# UNKNOWN_IS_NOT_A_VALUE_V1_20260815 — "?" reached the production UI as a rendered placeholder
# and, worse, behaved like a measurement: a carried-forward Shidore row held x/yt/ig all "?" and
# those cells were not treated as empty, so they could not be filled from the complete copy.
# Unknown must be UNAVAILABLE internally, never a value and never zero.
_BLANK_VALUES = (None, "", "?", "-", "n/a", "N/A")


def _is_blank(v):
    return v in _BLANK_VALUES or (isinstance(v, str) and not v.strip())


def _norm_title_id(_r):
    """sha1 of a NORMALISED title: unicode punctuation folded, whitespace collapsed, casefolded.

    A title differing only by a curly apostrophe must not fork an episode into two identities.
    """
    import hashlib as _h
    import re as _re
    import unicodedata as _ud
    t = (_r.get('title') or '').strip()
    if not t:
        return ''
    t = _ud.normalize('NFKD', t)
    for a, b in (('\u2019', "'"), ('\u2018', "'"), ('\u201c', '"'), ('\u201d', '"'),
                 ('\u2013', '-'), ('\u2014', '-'), ('\u00a0', ' ')):
        t = t.replace(a, b)
    t = _re.sub(r'\s+', ' ', t).strip().casefold()
    return _h.sha1(t.encode('utf-8')).hexdigest()[:12]


def _canonical_episode_id_v2(channel_id, video_id):
    """Stable 16-hex-char SHA-256 hash of (channel_id + ':' + video_id).
    Cross-run stable; independent of title mutations or SHORT vs full URL.
    """
    if not channel_id or not video_id:
        return None
    return _hashlib_v1.sha256(
        f"{channel_id}:{video_id}".encode("utf-8")).hexdigest()[:16]


_VIDEO_ID_RE = re.compile(r"(?:watch\?v=|/shorts/|youtu\.be/|/embed/)([A-Za-z0-9_-]{11})")


def _extract_video_id(link_href):
    if not link_href:
        return None
    m = _VIDEO_ID_RE.search(link_href)
    return m.group(1) if m else None


def _iso_to_short(iso_str):
    """'2026-07-19T06:30:06+00:00' -> '19 Jul'. Returns '' on failure."""
    if not iso_str or len(iso_str) < 10:
        return ""
    try:
        from datetime import datetime as _dt
        d = _dt.strptime(iso_str[:10], "%Y-%m-%d")
        return d.strftime("%-d %b")
    except Exception:
        return ""


def _iso_normalise(iso_str):
    """Return ISO in 'YYYY-MM-DDTHH:MM:SSZ' form (UTC). Passes-through if
    already Z; strips '+00:00' offset."""
    if not iso_str:
        return ""
    s = str(iso_str)
    # RFC3339 with +00:00 -> replace with Z; leave other TZ offsets alone
    # (the RSS emits +00:00 for all Google-hosted channels).
    if s.endswith("+00:00"):
        s = s[:-6] + "Z"
    return s


def _fetch_youtube_full_episodes(channel_id):
    """Return dict {video_id: {title, pub_iso, link, channel_id, ceid_v2,
    content_type, confidence}} of the last 15 RSS entries whose classifier
    result is EPISODE or EPISODE_UNCLASSIFIED (i.e. NOT SHORT / NOT CLIP).

    In-process cached per channel_id so the URL-bind cleanup, metric
    attribution and discover paths all agree on the same source of truth
    within one main_fetch run.
    """
    if channel_id in _URL_BIND_CACHE:
        return _URL_BIND_CACHE[channel_id]
    out = {}
    try:
        req = urllib.request.Request(
            f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}",
            headers={"User-Agent": "Mozilla/5.0"})
        rss = urllib.request.urlopen(req, timeout=15).read().decode()
        entries = re.findall(
            r"<entry>.*?<yt:videoId>(.*?)</yt:videoId>.*?<title>(.*?)</title>.*?"
            r'<link rel="alternate" href="([^"]*)".*?<published>(.*?)</published>'
            r'(?:.*?<media:description[^>]*>(.*?)</media:description>)?',
            rss, re.DOTALL)
        for vid, title, link, pub, desc in entries:
            title = (title.replace("&amp;", "&")
                          .replace("&#39;", "'")
                          .replace("&quot;", '"'))
            ct, conf = _yt_classify_content(link, title, desc or "")
            if ct in ("SHORT", "CLIP"):
                continue
            out[vid] = {
                "video_id": vid,
                "title": title,
                "pub_iso": _iso_normalise(pub),
                "link": f"https://www.youtube.com/watch?v={vid}",
                "channel_id": channel_id,
                "canonical_episode_id_v2": _canonical_episode_id_v2(channel_id, vid),
                "content_type": ct,
                "confidence": conf,
            }
    except Exception as _e:
        print(f"  [URL_BIND] RSS fetch err channel={channel_id[-6:]}: {_e}",
              file=sys.stderr)
        # Empty dict => URL_BIND acts fail-open (no drops). Explicit fail-open
        # policy: never delete rows on transient network failure.
    _URL_BIND_CACHE[channel_id] = out
    return out


def _url_bind_title_match(row_title, ep_title):
    """Fuzzy title match — token-set overlap on words length>=4.
    Returns True if >= 3 shared long tokens or 60% token overlap.
    Used to backfill canonical_video_id for pre-V1 rows whose only anchor
    is the RSS title."""
    if not row_title or not ep_title:
        return False
    def _toks(s):
        return set(w.lower() for w in re.findall(r"[A-Za-z]{4,}", s or ""))
    a = _toks(row_title); b = _toks(ep_title)
    if not a or not b:
        return False
    inter = a & b
    small = min(len(a), len(b))
    ratio = len(inter) / max(small, 1)
    # URL_BIND_TITLE_MATCH_HARDENING_V1_20260725 — the bare ">=3 shared tokens"
    # shortcut false-matched DIFFERENT episodes that merely share 3 common topic
    # words (e.g. Levy vs Ben-Menashe both carry Israel/Iran/Israeli), which
    # corrupted one episode's identity by binding it to the other's YouTube video.
    # A real title match shares most of the shorter title's tokens, so gate the
    # >=3 shortcut behind a substantial overlap ratio as well.
    return ratio >= 0.6 or (len(inter) >= 3 and ratio >= 0.5)


def _url_bind_cleanup_and_backfill(cache, channel_id, root_dir, data_file_name):
    """CANONICAL_URL_BIND_V1_2026_07_20 — for each row in `cache`:
      (a) if row already has a canonical_video_id AND it exists in RSS episodes,
          just backfill any missing fields (pub_iso, canonical_video_url,
          canonical_episode_id_v2) from RSS episode metadata (RSS wins).
      (b) if row lacks canonical_video_id, try fuzzy title-match against RSS
          episodes; on hit, populate URL_BIND fields. On no hit AND row is
          older than STALE_DAYS days AND the RSS returned a non-empty set,
          DROP the row (audit-queue trail written).
      (c) SHORT/CLIP-titled rows that fuzzy-match no full-length episode are
          ALWAYS dropped, regardless of age (protects against pre-V3 phantoms).
    Returns (n_bound_backfill, n_dropped, dropped_rows) tuple.
    """
    import datetime as _dt, json as _j, os as _o
    STALE_DAYS_DROP_UNBOUND = 45   # rows older than 45d with no RSS match => drop
    rss = _fetch_youtube_full_episodes(channel_id)
    if not rss:
        # Fail-open: no RSS data, do nothing (never drop rows on transient failure)
        return (0, 0, [])
    # Index by video_id
    by_vid = rss  # already keyed by video_id
    kept = []
    dropped = []
    n_bound = 0
    now = _dt.datetime.utcnow()
    for row in cache:
        # UPCOMING rows are exempt; they get regenerated fresh each cycle
        if row.get("is_upcoming"):
            kept.append(row); continue
        # RUMBLE_FIRST_HONORIFIC_KEEP_V1_2026_08_10 — SECOND site of this rule.
        # This function owns the drop that actually removed the row; _keeps() below
        # carries an identical honorific test, so fixing only one leaves the bug live.
        # A Rumble-first episode has no canonical_video_id and cannot match the YT RSS
        # (it is not on YouTube yet), so it reached "Rule 1" and was dropped for opening
        # with an honorific — e.g. "Prof. John Mearsheimer Explains Why Iran War MUST
        # END" (Rumble 2026-08-09). The bridge re-injected it hourly; this deleted it
        # minutes later, every time. Exempt bridge-injected rows: they are not YouTube
        # rows and the bridge applies its own recency + attribution gates.
        if row.get("rumble_only_injected"):
            kept.append(row); continue
        rvid = row.get("canonical_video_id")
        title = row.get("title") or ""
        matched_ep = None
        if rvid and rvid in by_vid:
            matched_ep = by_vid[rvid]
        else:
            # Fuzzy title match
            for _vid, _ep in by_vid.items():
                if _url_bind_title_match(title, _ep["title"]):
                    matched_ep = _ep; break
        if matched_ep is not None:
            # Bind / backfill
            row["canonical_video_id"] = matched_ep["video_id"]
            row["canonical_video_url"] = matched_ep["link"]
            row["pub_iso"] = matched_ep["pub_iso"]
            row["canonical_episode_id_v2"] = matched_ep["canonical_episode_id_v2"]
            # CANONICAL_URL_BIND_V1_2026_07_20 — overwrite date + title from canonical
            # RSS entry so a phantom Short-derived row picks up the parent full
            # episodes date instead of the Shorts upload date. Root cause of the
            # Sakwa 17 Jul phantom that persisted post-V3 classifier.
            row["date"] = _iso_to_short(matched_ep["pub_iso"]) or row.get("date", "")
            row["title"] = matched_ep["title"]
            row["link"] = matched_ep["link"]
            # source_platform_ids — additive
            _spid = row.get("source_platform_ids") or {}
            if not isinstance(_spid, dict):
                _spid = {}
            _yt_ids = list(_spid.get("youtube") or [])
            if matched_ep["video_id"] not in _yt_ids:
                _yt_ids.append(matched_ep["video_id"])
            _spid["youtube"] = _yt_ids
            row["source_platform_ids"] = _spid
            n_bound += 1
            kept.append(row)
            continue
        # No match. Decide to drop.
        # Rule 1: title obviously indicates SHORT/CLIP -> always drop.
        _ct_guess, _ = _yt_classify_content(
            row.get("link") or row.get("canonical_video_url") or "",
            title, "")
        # If title starts with common non-episode Short/clip prefixes, drop
        _shorts_prefix_re = re.compile(
            r"^(?:Prof\.?|Dr\.?|Amb\.?|Sen\.?|Col\.?|Gen\.?|Fmr|Former|"
            r"Ex[\s\-]|Retired|Ret\.?)\s+", re.I)
        _looks_short_titled = (
            _ct_guess in ("SHORT", "CLIP")
            or bool(_shorts_prefix_re.match(title)) and " on " not in title
            and " for " not in title and " Says " not in title.replace(",", "")
        )
        # Rule 2: age gate for anything else
        _date_short = row.get("date") or ""
        _age_days = 9999
        try:
            _d = _dt.datetime.strptime(_date_short, "%d %b").replace(year=now.year)
            if _d > now:
                _d = _d.replace(year=now.year - 1)
            _age_days = (now - _d).days
        except Exception:
            pass
        _drop = _looks_short_titled or (_age_days > STALE_DAYS_DROP_UNBOUND)
        if _drop:
            dropped.append({
                "iso_flagged": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "marker": "CANONICAL_URL_BIND_V1_2026_07_20",
                "reason": ("SHORT_or_CLIP_titled_phantom"
                           if _looks_short_titled
                           else f"no_url_match_and_older_than_{STALE_DAYS_DROP_UNBOUND}d"),
                "channel_id": channel_id,
                "data_file": data_file_name,
                "row_guest": row.get("guest"),
                "row_surname": row.get("surname"),
                "row_date": _date_short,
                "row_title": title,
                "row_canonical_episode_id": row.get("canonical_episode_id"),
                "row_yt_content_type": row.get("_yt_content_type"),
            })
            continue
        # Otherwise keep (e.g. legacy real episode that's rolled off the 15-entry
        # RSS window and is <45d old). Do NOT hallucinate URL_BIND fields.
        kept.append(row)
    # Persist audit
    if dropped:
        try:
            _adir = _o.path.join(root_dir, "docs")
            _o.makedirs(_adir, exist_ok=True)
            _apath = _o.path.join(_adir, "audit_queue_url_bind_v1.jsonl")
            with open(_apath, "a") as _af:
                for _r in dropped:
                    _af.write(_j.dumps(_r, ensure_ascii=False) + "\n")
        except Exception as _e:
            print(f"  [URL_BIND] audit write err: {_e}", file=sys.stderr)
        print(f"  [URL_BIND] dropped {len(dropped)} phantom/stale row(s) from "
              f"{data_file_name}")
    return (n_bound, len(dropped), dropped)


def _url_bind_dedupe_by_canonical(cache):
    """CANONICAL_URL_BIND_V1_2026_07_20 — dedupe rows preferring stable
    canonical_video_id / canonical_episode_id_v2 keys. Metrics from
    duplicate rows keyed by same canonical are merged with last-writer-wins
    per platform, NEVER max-across-different-canonicals.
    Falls back to (canonical_episode_id, guest, date) for legacy rows lacking
    the new fields.
    """
    def _key(v):
        vid = v.get("canonical_video_id") or v.get("canonical_episode_id_v2")
        if vid: return ("v", vid)
        ceid = v.get("canonical_episode_id")
        if ceid: return ("c", ceid)
        return ("gd", v.get("guest",""), v.get("date",""))
    seen = {}
    order = []
    for v in cache:
        k = _key(v)
        if k not in seen:
            seen[k] = v
            order.append(k)
            continue
        prev = seen[k]
        # LAST-WRITER-WINS per platform metric (this is called after sort desc
        # by pub_iso, so the fresher row's metrics dominate).
        for field in ("rumble_views", "x_views", "yt_views", "ig_likes"):
            _new = v.get(field)
            if _new not in (None, "", "?"):
                prev[field] = _new
        # Preserve show / classifier metadata from later entries too
        for field in ("show", "_yt_content_type", "_yt_class_confidence",
                      "canonical_video_id", "canonical_video_url", "pub_iso",
                      "canonical_episode_id_v2", "source_platform_ids"):
            if not prev.get(field) and v.get(field):
                prev[field] = v[field]
    return [seen[k] for k in order]


def _url_bind_sort_by_pub_iso(cache):
    """CANONICAL_URL_BIND_V1_2026_07_20 — stable sort DESCENDING by pub_iso.
    Rows without pub_iso fall back to parsing short 'dd Mon' with current
    year; still no pub_iso => sink to the bottom of the ORDER of their
    original slot (preserve prior ordering for pre-V1 rows).
    """
    import datetime as _dt
    now = _dt.datetime.utcnow()
    def _sort_key(v):
        piso = v.get("pub_iso") or ""
        if piso and len(piso) >= 10:
            return piso
        d = v.get("date") or ""
        try:
            _d = _dt.datetime.strptime(d, "%d %b").replace(year=now.year)
            if _d > now:
                _d = _d.replace(year=now.year - 1)
            return _d.strftime("%Y-%m-%dT00:00:00Z")
        except Exception:
            # Anything unparseable sorts to the bottom (empty string < any date)
            return ""
    cache.sort(key=_sort_key, reverse=True)
    return cache


# --- end CANONICAL_URL_BIND_V1_2026_07_20 helpers ----------------------------


X_COOKIES = json.loads(os.environ.get("X_COOKIES_JSON", "[]"))
IG_COOKIES = json.loads(os.environ.get("IG_COOKIES_JSON", "[]"))

TIDBYT_DEVICES = [
    {"id": "winsomely-tidy-chic-roach-990",
     "key": os.environ.get("TIDBYT_KEY_1", "")},
    {"id": "totally-fantastic-cordial-jacamar-855",
     "key": os.environ.get("TIDBYT_KEY_2", "")},
]

SHOWS = [
    {
        "name": "Going Underground",
        "data_file": os.path.join(ROOT, "videos.json"),
        "x_handle": "GUnderground_TV",
        "yt_channel_id": "UCjY51YgQzYxD5kX-BNobpxA",
        "rumble_channel": "GoingUnderground",
    },
    {
        "name": "New Order",
        "data_file": os.path.join(ROOT, "videos_neworder.json"),
        "x_handle": "NewOrder_TV",
        "yt_channel_id": "UC7FXwSQPOlq-eqXjpS3TL8g",
        "rumble_channel": "NewOrderTV",
        "x_date_window": True,  # account too small for name search; use date-window fallback
    },
]


def parse_count(v):
    val = str(v or '0').replace(',', '').replace('?', '0')
    if val.upper().endswith('M'): return int(float(val[:-1]) * 1_000_000)
    if val.upper().endswith('K'): return int(float(val[:-1]) * 1_000)
    if val.replace('.', '').isdigit(): return int(float(val))
    return 0


# GU_UNKNOWN_IS_NULL_V2_2026_08_06 ------------------------------------------
# parse_count() maps None/'?'/garbage to 0, which is right for "render something"
# and wrong for "add it up": an unmeasured platform then contributes a hard 0 and
# the episode total is understated with no visible signal. These two helpers keep
# unknown separable from a source-confirmed zero.
UNKNOWN_METRIC_MARKERS = ('?', 'N/A', 'NA', 'ERR', 'ERROR', '')


def parse_count_opt(v):
    """-> int for a real measurement (including a genuine 0), None for unknown."""
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return int(v)
    if isinstance(v, dict):  # structured health object {status: N/A|ERROR, ...}
        st = str(v.get('status', '')).upper()
        return None if st in ('N/A', 'NA', 'ERR', 'ERROR', 'MISSING') else None
    s = str(v).strip()
    if s.upper() in UNKNOWN_METRIC_MARKERS:
        return None
    val = s.replace(',', '')
    try:
        if val.upper().endswith('M'):
            return int(float(val[:-1]) * 1_000_000)
        if val.upper().endswith('K'):
            return int(float(val[:-1]) * 1_000)
        return int(float(val))
    except (ValueError, TypeError):
        return None  # unparseable is unknown, never zero


def sum_known_metrics(entry, fields=('rumble_views', 'x_views', 'yt_views', 'ig_likes')):
    """-> (total_of_known, unknown_field_names). Unknown never adds 0 silently;
    the caller decides how to label a partial total."""
    total, unknown = 0, []
    for f in fields:
        n = parse_count_opt(entry.get(f))
        if n is None:
            unknown.append(f)
        else:
            total += n
    return total, unknown


def format_views(v):
    n = parse_count(v) if isinstance(v, str) else int(v)
    if n >= 1_000_000: return f"{n/1_000_000:.1f}M"
    if n >= 1_000: return f"{n/1_000:.1f}K"
    return str(n)


def _capitalised_tokens(text, min_len=4):
    """Lower-cased capitalised word tokens, in order of appearance. Unicode-aware.

    NON_ASCII_NAME_TOKENS_V1_2026_08_09 — replaces two ASCII-only classes:
    r'\\b[A-Z][a-zA-Z\\-]{3,}\\b' over YouTube titles and r'\\b[A-Z][a-z]{3,}\\b' over
    Instagram captions. Neither can match "Ünal" or "Maté" — the leading Ü is not in
    [A-Z], and "até" breaks the ASCII continuation — so those surnames never became
    tokens, never intersected known_surnames, and their episodes carried no yt_views
    at all. Ünal showed real Rumble, X and Instagram figures beside an empty YouTube
    column while its video sat on 5,658 views; every ASCII-named guest measured fine.

    str.isupper() is Unicode-aware, so the capitalisation rule that makes this a NAME
    filter (rather than an every-word filter) is preserved for every alphabet.
    """
    out = []
    for w in re.findall(r"[^\W\d_](?:[^\W\d_]|-)*", text or ""):
        if len(w) >= min_len and w[0].isupper():
            out.append(w.lower())
    return out


def fetch_youtube_data(channel_id, known_surnames=None):
    """Fetch view counts AND publish dates per surname from YouTube RSS.

    METRIC_ATTRIB_V1_2026_07_20 — hardened attribution logic.
    Previously this function walked EVERY capitalised token in EVERY RSS
    title and did views_map.setdefault(word.lower(), views). Consequences:
      1. First entry in feed (often a Short) established the view count
         for that surname; later main-episode data never overrode (setdefault).
      2. Any capitalised word longer than 3 chars (including topic words like
         "Ukraine", "Russia", "Pentagon") became a phantom "surname key".
      3. Shorts and Clips containing the guest's surname inherited THAT short's
         view count and attributed it to the main episode — cross-post
         inflation (July 2026 Perkins/Rickards incident).

    New rules (canonical-content-classification skill compliant):
      * Views attach to a surname ONLY when the title contains the surname
        as an isolated whole-word token.
      * When multiple entries mention the same surname, we sum the view counts
        for classified EPISODE entries and, SEPARATELY, sum views for
        SHORT/CLIP entries. `views_map[surname]` returns the EPISODE sum when
        available, else the SHORT/CLIP sum (metrics-only, per skill rule that
        SHORT/CLIP contribute METRICS but never chronology).
      * `date_map[surname]` is set ONLY from EPISODE-class entries.
      * If `known_surnames` is supplied, we restrict attribution to those.
        Otherwise we fall back to the old capitalised-word heuristic BUT still
        require whole-word match and skip topic stopwords.
    """
    _TOPIC_STOP = {
        'iran', 'israel', 'going', 'underground', 'order', 'ukraine', 'russia',
        'china', 'india', 'trump', 'biden', 'putin', 'netanyahu', 'gaza',
        'pentagon', 'nato', 'brics', 'centcom', 'hormuz', 'palestine', 'lebanon',
        'saudi', 'yemen', 'iraq', 'syria', 'europe', 'america', 'washington',
        'moscow', 'beijing', 'tehran', 'jerusalem', 'kyiv', 'nuclear', 'war',
        'peace', 'trade', 'economy', 'markets', 'sanctions', 'tariffs',
        'special', 'episode', 'clip', 'short', 'part', 'live', 'watch',
    }
    known_surnames = set(s.lower() for s in (known_surnames or []) if s)
    try:
        req = urllib.request.Request(
            f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}",
            headers={"User-Agent": "Mozilla/5.0"})
        rss = urllib.request.urlopen(req, timeout=15).read().decode()
        # YT_CONTENT_TYPE_V3_2026_07_17 — richer classifier; capture description too.
        entries = re.findall(
            r'<entry>.*?<title>(.*?)</title>.*?<link rel="alternate" href="([^"]*)".*?<published>(.*?)</published>.*?<media:description[^>]*>(.*?)</media:description>.*?<media:statistics views="(\d+)"',
            rss, re.DOTALL)
        # Fallback: entries where <media:description> is absent — retry with old regex
        if len(entries) == 0:
            _old_entries = re.findall(
                r'<entry>.*?<title>(.*?)</title>.*?<link rel="alternate" href="([^"]*)".*?<published>(.*?)</published>.*?<media:statistics views="(\d+)"',
                rss, re.DOTALL)
            entries = [(t, l, p, "", v) for (t, l, p, v) in _old_entries]
        episode_views = {}      # surname -> summed views across EPISODE entries
        short_clip_views = {}   # surname -> summed views across SHORT / CLIP entries
        date_map = {}           # surname -> ISO date (EPISODE only)
        for title, link_href, pub, description, views in entries:
            _content_type, _confidence = _yt_classify_content(link_href, title, description)
            _is_episode_class = _content_type in ("EPISODE", "EPISODE_UNCLASSIFIED")
            _is_short_or_clip = _content_type in ("SHORT", "CLIP")
            title = title.replace('&amp;', '&').replace('&#39;', "'")
            iso_date = pub[:10]
            try:
                _v = int(views)
            except Exception:
                _v = 0
            # METRIC_ATTRIB_V1_2026_07_20 — extract whole-word tokens ONLY.
            _tokens = set(_capitalised_tokens(title))
            # Parenthesised aliases (e.g. "(Prof. Steve Keen)") — split words.
            for pm in re.finditer(r'\(([^)]+)\)', title):
                for pw in pm.group(1).split():
                    pw = pw.strip('.,').lower()
                    if len(pw) > 3:
                        _tokens.add(pw)
            # Attribute to KNOWN surnames only when known list is supplied.
            _candidates = _tokens & known_surnames if known_surnames else {
                w for w in _tokens if w not in _TOPIC_STOP
            }
            for sn in _candidates:
                if _is_episode_class:
                    episode_views[sn] = episode_views.get(sn, 0) + _v
                    date_map.setdefault(sn, iso_date)
                elif _is_short_or_clip:
                    short_clip_views[sn] = short_clip_views.get(sn, 0) + _v
                else:
                    # Uncategorised — treat like episode for view attribution
                    # but do NOT set date (avoids date-drift from ambiguous items).
                    episode_views[sn] = episode_views.get(sn, 0) + _v
        # METRIC_ATTRIB_V1_2026_07_20 — final views_map prefers EPISODE totals;
        # falls back to SHORT/CLIP totals ONLY when the surname has no EPISODE
        # entry. This prevents the July-2026 Perkins/Rickards incident where
        # a Short containing "Perkins" set yt_views on the main NO episode.
        views_map = {}
        for sn, v in episode_views.items():
            views_map[sn] = format_views(v)
        for sn, v in short_clip_views.items():
            if sn not in views_map:
                views_map[sn] = format_views(v)
        return views_map, date_map
    except Exception as e:
        print(f"YouTube error for {channel_id}: {e}", file=sys.stderr)
        return {}, {}


def fetch_instagram_clips(known_surnames=None):
    """Fetch IG play counts from afshinrattansi profile (shared by both shows).

    METRIC_ATTRIB_V1_2026_07_20 — attribution hardened.
    Previously any capitalised 4+ char word in ANY caption was accumulated as
    a phantom "surname key", inflating IG numbers on unrelated episodes
    (July 2026 same 502 likes across Perkins short + Rickards short + R&AW
    Chief cross-contamination).

    New rules:
      * If `known_surnames` supplied, only accumulate to surnames that appear
        as a whole word in the caption AND are in the known list.
      * Otherwise apply a topic-stopword blocklist (broad geopolitical nouns).
      * A single IG post's play_count is attributed to AT MOST ONE surname
        (the first known-list match by position in caption). This prevents
        one clip's views inflating five different guest scores.
    """
    _TOPIC_STOP = {
        'iran', 'israel', 'going', 'underground', 'order', 'ukraine', 'russia',
        'china', 'india', 'trump', 'biden', 'putin', 'netanyahu', 'gaza',
        'pentagon', 'nato', 'brics', 'centcom', 'hormuz', 'palestine', 'lebanon',
        'saudi', 'yemen', 'iraq', 'syria', 'europe', 'america', 'washington',
        'moscow', 'beijing', 'tehran', 'jerusalem', 'kyiv', 'nuclear', 'war',
        'peace', 'trade', 'economy', 'markets', 'sanctions', 'tariffs',
        'special', 'episode', 'clip', 'short', 'part', 'live', 'watch',
    }
    known_surnames = set(s.lower() for s in (known_surnames or []) if s)
    if not IG_COOKIES:
        return {}
    try:
        cookies = {c['name']: c['value'] for c in IG_COOKIES}
        headers = {
            'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X)',
            'X-IG-App-ID': '936619743392459',
            'X-CSRFToken': cookies.get('csrftoken', ''),
            'Cookie': '; '.join(f'{k}={v}' for k, v in cookies.items()),
        }
        r = requests.get(
            'https://i.instagram.com/api/v1/users/web_profile_info/?username=afshinrattansi',
            headers=headers, timeout=15)
        user_id = r.json()['data']['user']['id']
        clips = {}
        max_id = ''
        for _ in range(5):
            url = f'https://i.instagram.com/api/v1/feed/user/{user_id}/?count=33'
            if max_id:
                url += f'&max_id={max_id}'
            r = requests.get(url, headers=headers, timeout=15)
            data = r.json()
            for item in data.get('items', []):
                caption = (item.get('caption') or {}).get('text', '') or ''
                play_count = item.get('play_count') or item.get('view_count') or item.get('like_count', 0)
                # METRIC_ATTRIB_V1_2026_07_20 — pick a SINGLE surname per clip.
                _picked = None
                for wl in _capitalised_tokens(caption):
                    if known_surnames:
                        if wl in known_surnames:
                            _picked = wl
                            break
                    else:
                        if wl not in _TOPIC_STOP:
                            _picked = wl
                            break
                if _picked:
                    clips[_picked] = clips.get(_picked, 0) + play_count
            if not data.get('more_available'):
                break
            max_id = data.get('next_max_id', '')
            if not max_id:
                break
        return {k: format_views(v) for k, v in clips.items()}
    except Exception as e:
        print(f"IG error: {e}", file=sys.stderr)
        return {}


# v5 2026-06-20 PATCHED — handles Ex-PM titles, possessive, generic-pre-name
def extract_guest(title):
    """Delegates to gu_parser — see GU_PARSER_SINGLE_SOURCE_V1_2026_08_09.

    This module used to carry its own near-identical copy of the parser, and the copy
    was the one that actually ran: gu_parser.py is the module covered by
    regression_tests_gu_titles.json and test_gu_parser.py, but nothing here imported
    it, so the corpus guarded code that was never executed and fixing the tested
    parser changed nothing in production. That is why the 7 Aug Hasan Unal episode
    stayed missing from videos.json after its parser bug had already been fixed.

    Checked before switching, across the 68 distinct titles in videos.json,
    videos_neworder.json, both YouTube feeds and the regression corpus: gu_parser is a
    strict superset here. It reads 5 titles this copy could not (Unal, Mate, Garcia,
    Melenchon, Freeman) and regresses none. The single pattern only this copy had,
    "<Name> <Verb> ..." for Blumenthal/Schiff, was ported into gu_parser first.
    """
    return gu_parser.extract_guest(title, source="fetch_and_push")


def _strip_role(name):
    # RANK_STRIP_V1_2026_07_20 — strip "Ret." / "Retired" then enlisted ranks first.
    name = re.sub(r'^(?:Ret|Retired)\.?\s+', '', name, flags=re.I).strip()
    name = re.sub(
        r'^(?:CMSGT|CSM|SGM|SMA|MSGT|MSG|SFC|SSG|SGT|CPL|PFC|PVT|SPC|'
        r'MSgt|GySgt|SSgt|TSgt|SrA|A1C|Amn|'
        r'PO1|PO2|PO3|CPO|SCPO|MCPO|ENS|LTJG|CDR|LCDR|RADM|VADM|'
        r'CW[0-9]|CWO|WO[0-9])\.?\s+',
        '', name).strip()
    # RANK_STRIP_V1_2026_07_20 — order matters: longer compound ranks first
    # (Lt. Col., Lt. Gen., Maj. Gen., Brig. Gen.) so "Lt. Col." isn't shredded to "Col.".
    name = re.sub(
        r'^(?:Lt\.?\s*Col\.?|Lt\.?\s*Gen\.?|Maj\.?\s*Gen\.?|Brig\.?\s*Gen\.?|'
        r'Vice\s+Adm\.?|Rear\s+Adm\.?)\s+',
        '', name, flags=re.I
    ).strip()
    name = re.sub(
        r'^(?:(?:Ex|Former|Fmr|Acting|Deputy|Senior|Chief|Head)[\s.-]*)*'
        r'(?:Israeli\s+|US\s+|UK\s+|British\s+|American\s+)?'
        r'(?:Intel\s+|Intelligence\s+)?(?:Acting\s+)?'
        r'(?:President|PM|Prime\s+Minister|Minister|Officer|Ambassador|Amb|MP|'
        r'Director|Head|Chief|Senator|Congressman|General|Gen|Admiral|Adm|Secretary|'
        r'Advisor|Analyst|Spokesperson|Editor|Professor|Commander|Colonel|Col|'
        r'Captain|Capt|Major|Maj|Lt|Sgt|Dr|Prof)\.?\s+',
        '', name, flags=re.I
    ).strip()
    return name


# GU_SURNAME_HARDENING_V1_2026_07_03
# Ported from /Users/afshin/RumbleMonitor/totals_pusher.py.
# Purpose: stop junk tokens like "Tru", "DEF" and "_R<date>" suffixes
# reaching the Android app via videos.json / videos_neworder.json.
_GU_JUNK_TOKENS = {"DEF", "TRU", "IRAN", "WAR", "NEWS", "LIVE", "WATCH",
                   "GU", "NO", "USA", "UN", "EU", "PM", "US", "UK",
                   "DES", "ST", "POWER", "LACKS"}

def _strip_r_date_suffix(s):
    """Strip cache-key suffixes like _R22Jun / _R8May. Broader than
    the legacy _R\\d{1,2}[A-Z][a-z]{2} regex."""
    if not s: return s
    return re.sub(r"_R[A-Za-z0-9]{2,10}$", "", str(s))

def _looks_valid_surname(s):
    """True iff s is a plausible surname. Rejects underscore/digit
    poisoning, ALL-CAPS junk fragments (DEF), and short truncations (Tru)."""
    if not s: return False
    s = s.strip().rstrip(".,?!:;’‘\"'")
    if "_" in s or any(ch.isdigit() for ch in s): return False
    if s.upper() in _GU_JUNK_TOKENS: return False
    return (len(s) >= 3 and s[0].isalpha()
            and not (s.isupper() and len(s) <= 4))




# GU_UPCOMING_EPISODE_MERGE_V1_2026_07_03 -----------------------------------
# Reads /Users/afshin/going-underground-stats/upcoming.json (list of entries).
# Prepends show-matching entries with is_upcoming=true to output list.
# Fail-open: if file missing or malformed, output is unchanged.
def _load_upcoming_for(show):
    """UPCOMING_STALE_FILTER_V1_2026_07_20 — drop entries whose date is more
    than STALE_DAYS in the past. Root cause of the 2026-07-20 dashboard bug
    where a 4-Jul Kucinich special was still labelled "UPCOMING" on 20-Jul
    because upcoming.json was never regenerated (gu_upcoming_auto_v1.py has
    a hardcoded M2-Pro-only path and never runs on GitHub Actions runner).
    """
    STALE_DAYS = 3
    try:
        p = os.path.join(ROOT, "upcoming.json")
        if not os.path.exists(p): return []
        with open(p) as _f: raw = json.load(_f)
        if not isinstance(raw, list): return []
        import datetime as _dt
        now = _dt.datetime.utcnow()
        out = []
        for it in raw:
            if not isinstance(it, dict): continue
            if str(it.get("show","")).upper() != show.upper(): continue
            # UPCOMING_STALE_FILTER_V1_2026_07_20 — parse "4 Jul" / "18 May"
            # against current year; if parsed date is > STALE_DAYS in the
            # past, treat as stale and skip.
            _date_short = str(it.get("date") or "").strip()
            if _date_short:
                try:
                    _d = _dt.datetime.strptime(_date_short, '%d %b').replace(year=now.year)
                    # If far in the future (>60d), likely wrong year — subtract.
                    if _d > now + _dt.timedelta(days=60):
                        _d = _d.replace(year=now.year - 1)
                    _age_days = (now - _d).days
                    if _age_days > STALE_DAYS:
                        print(f"  [UPCOMING_STALE] dropping {show} {_date_short!r} "
                              f"({_age_days}d old, > {STALE_DAYS}d)")
                        continue
                except Exception:
                    # Unparseable date — treat as stale to avoid pinning a permanent entry.
                    print(f"  [UPCOMING_STALE] dropping {show} unparseable date {_date_short!r}")
                    continue
            it = dict(it)
            it["is_upcoming"] = True
            out.append(it)
        return out
    except Exception:
        return []

# PUBLISH_GUARD_V1_2026_07_03 — refuse to publish videos.json if bad surname tokens present.
# Defense-in-depth on top of _looks_valid_surname. Called before writing any feed JSON.
_GU_PUBLISH_GUARD_BAD_LITERALS = {"Tru", "DEF", "DES", "St", "IRAN", "WAR", "NEWS",
                                   "LIVE", "WATCH", "GU", "NO", "USA", "UN", "EU",
                                   "PM", "US", "UK", "POWER", "LACKS"}

def _publish_guard_scan(videos_list, label):
    """Raise SystemExit(3) if any surname is a known bad token. Returns list of
    offenders (empty on clean)."""
    import re as _re_pg
    bad = []
    for v in videos_list or []:
        s = (v.get("surname") or "").strip()
        if not s:
            continue
        if s in _GU_PUBLISH_GUARD_BAD_LITERALS:
            bad.append((v.get("guest") or "", s, "literal_bad_token"))
            continue
        if _re_pg.search(r"_R[A-Za-z0-9]{2,10}$", s):
            bad.append((v.get("guest") or "", s, "R_date_suffix"))
            continue
        if s.isupper() and len(s) <= 4:
            bad.append((v.get("guest") or "", s, "allcaps_short"))
            continue
    if bad:
        print("[PUBLISH_GUARD_V1_FAILURE] label=" + label + " bad=" + repr(bad[:10]) + " n_total=" + str(len(bad)))
        raise SystemExit(3)
    print("[PUBLISH_GUARD_V1_OK] label=" + label + " n_checked=" + str(len(videos_list or [])))
    return []

def extract_surname(guest_name):
    """Get just the surname from a guest name. Returns None if guest_name is None/empty."""
    if not guest_name:
        return None
    name = guest_name.replace('(Jim) ', '').replace('Lt. Col. ', '').replace('Dr. ', '').replace('Prof. ', '').replace('Sgt. ', '')
    # Defensive: legacy data may have a "_R<date>" suffix from an older writer (see is_repeat
    # branch in legacy local auto_update.py). Strip it so downstream consumers (LaMetric,
    # GitHub Pages, APK) don't display it.
    # GU_SURNAME_HARDENING_V1_2026_07_03 - broader regex catches _R<alnum2..10>.
    name = _strip_r_date_suffix(name).strip()
    parts = name.strip().split()
    if not parts:
        return None
    last = parts[-1]
    if len(parts) >= 2 and parts[-2].endswith('-'):
        return parts[-2] + last
    if len(parts) >= 2 and '-' in parts[-1] and parts[-2][0].isupper():
        return last
    # GU_SURNAME_HARDENING_V1_2026_07_03 - reject junk-token surnames outright,
    # signalling to caller (discover_new_episodes) to skip this episode.
    if not _looks_valid_surname(last):
        return None
    return last


def discover_new_episodes(channel_id, data_file):
    """Use YouTube RSS to discover new episodes not yet in the data file."""
    from datetime import datetime as dt

    existing_surnames = set()
    if os.path.exists(data_file):
        with open(data_file) as f:
            cached = json.load(f)
        for c in cached:
            s = c.get('surname', '').lower()
            if s:
                existing_surnames.add(s)
    else:
        cached = []

    # Also track existing titles to avoid near-duplicate detection
    existing_titles = set()
    for c in cached:
        t = c.get('title', '').lower()[:40]
        if t:
            existing_titles.add(t)

    try:
        rss = urllib.request.urlopen(
            urllib.request.Request(
                f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}",
                headers={"User-Agent": "Mozilla/5.0"}),
            timeout=15).read().decode()
        # YT_CONTENT_TYPE_V3_2026_07_17 — richer classifier + capture description.
        # New-production creation is restricted to EPISODE / EPISODE_UNCLASSIFIED.
        # SHORT / CLIP contribute views via fetch_youtube_data (parser 1);
        # they must not create phantom production entries or set dates.
        entries = re.findall(
            r'<entry>.*?<title>(.*?)</title>.*?<link rel="alternate" href="([^"]*)".*?<published>(.*?)</published>.*?<media:description[^>]*>(.*?)</media:description>',
            rss, re.DOTALL)
        if len(entries) == 0:
            _old = re.findall(
                r'<entry>.*?<title>(.*?)</title>.*?<link rel="alternate" href="([^"]*)".*?<published>(.*?)</published>',
                rss, re.DOTALL)
            entries = [(t, l, p, "") for (t, l, p) in _old]
        new_eps = []
        for title_raw, link_href, pub, description in entries:
            # YT_CONTENT_TYPE_V3_2026_07_17 — content-type discrimination
            _content_type, _confidence = _yt_classify_content(link_href, title_raw, description)
            if _content_type in ("SHORT", "CLIP"):
                continue  # views-only; do not create new production
            title = title_raw.replace('&amp;', '&').replace('&#39;', "'").replace('&quot;', '"')
            title = title.replace('\u2018', "'").replace('\u2019', "'").replace('\u201c', '"').replace('\u201d', '"')
            if len(title) < 20:
                continue
            # Skip if title already tracked
            if title.lower()[:40] in existing_titles:
                continue
            guest = extract_guest(title)
            surname = extract_surname(guest)
            # Skip if extractor couldn't find a clean guest (returns None now instead of
            # title[:30]) — avoids "DES"/"St" garbage from the legacy truncation fallback.
            if not guest or not surname:
                print(f"  SKIP (unparseable): {title[:60]}...")
                continue
            # Validate: surname must be >1 char and not a common English word
            SKIP_WORDS = {'failure','decline','war','iran','israel','trump','target',
                          'hegemony','loser','crisis','threat','risk','end','new',
                          'order','going','underground','episode','interview',
                          'heated','challenge','relation','join','control',
                          'russia','china','hit','indi','rick','strait','hormuz',
                          'could','only','about','into','from','with','that',
                          'this','have','been','were','will','would','should',
                          'massacre','troops','pentagon','nuclear','bases','gulf',
                          'commander','challenged','former','centcom',
                          'in','of','on','at','by','to','an','is','it','or',
                          'action','missing','missing','brics','india','warns',
                          'global','south','west','east','world','power','trump'}
            # GU_SURNAME_HARDENING_V1_2026_07_03 - extra validity gate on top of SKIP_WORDS.
            if (len(surname) <= 2 or surname.lower() in SKIP_WORDS
                    or not _looks_valid_surname(surname)):
                continue
            if surname.lower() in existing_surnames:
                continue
            try:
                d = dt.strptime(pub[:10], '%Y-%m-%d')
                short_date = d.strftime('%-d %b')
            except Exception:
                short_date = ''
            # CANONICAL_FIELD_EMISSION_V1_2026_07_11 — canonicalise BEFORE emit.
            cfn, csu, ceid = _canonical_from_title(title, guest, surname)
            # Prefer canonical surname over extractor output when CANON_MAP hits.
            emit_guest = cfn or guest
            emit_surname = (csu or (surname.upper() if surname else None))
            # store the display-cased surname in `surname` for backward-compat
            # (Android reads `surname`); Tidbyt/LaMetric readers now prefer
            # canonical_surname_upper.
            surname_display = cfn.split()[-1] if cfn else surname
            # YT_CONTENT_TYPE_V4B_2026_07_17 — gate EPISODE_UNCLASSIFIED before creation
            if _content_type == "EPISODE_UNCLASSIFIED":
                _allow = _v4_gate_unclassified(ceid, title, link_href, pub,
                                                 description, cfn, emit_surname,
                                                 emit_guest, surname_display,
                                                 short_date, ROOT)
                if not _allow:
                    continue
            # CANONICAL_URL_BIND_V1_2026_07_20 — capture stable YT URL identity.
            _video_id = _extract_video_id(link_href)
            _ceid_v2 = _canonical_episode_id_v2(channel_id, _video_id) if _video_id else None
            new_eps.append({
                "guest": emit_guest,
                "surname": surname_display,
                "title": title,
                "rumble_views": "?", "x_views": "?", "date": short_date,
                "yt_views": "?", "ig_likes": "?",
                "canonical_guest_full_name": cfn or emit_guest,
                "canonical_surname_upper": emit_surname,
                "canonical_episode_id": ceid,
                # YT_CONTENT_TYPE_V3_2026_07_17 / YT_CONTENT_TYPE_V4B_2026_07_17 — classifier metadata
                "_yt_content_type": _content_type,
                "_yt_class_confidence": _confidence,
                # CANONICAL_URL_BIND_V1_2026_07_20 — url-based identity fields
                "canonical_video_id":  _video_id,
                "canonical_video_url": (f"https://www.youtube.com/watch?v={_video_id}"
                                         if _video_id else None),
                "pub_iso":             _iso_normalise(pub),
                "canonical_episode_id_v2": _ceid_v2,
                "source_platform_ids": {"youtube": [_video_id]} if _video_id else {},
                "link":                (f"https://www.youtube.com/watch?v={_video_id}"
                                         if _video_id else link_href),
            })
            existing_surnames.add((surname_display or "").lower())
            existing_titles.add(title.lower()[:40])
            print(f"  NEW: {emit_guest} ({short_date}) [canon={csu} id={ceid}]")
        if new_eps:
            cached = new_eps + cached
            with open(data_file, 'w') as f:
                json.dump(cached, f, indent=2)
            print(f"  Added {len(new_eps)} new episode(s)")
    except Exception as e:
        print(f"  Discovery error: {e}", file=sys.stderr)


async def _scrape_x_search(ctx, query):
    """Run a single X search query and return a list of (tweet_id, views) tuples.

    Tweet IDs are extracted from the /analytics link (the canonical anchor),
    so dedup by ID is reliable even when scroll-virtualization re-emits a
    tweet during the scroll loop.
    """
    encoded = urllib.parse.quote(query)
    page = await ctx.new_page()
    try:
        await page.goto(f'https://x.com/search?q={encoded}&f=live',
                        wait_until="domcontentloaded", timeout=20000)
        await page.wait_for_timeout(4000)
        # v2: detect login wall — if X redirected to login, cookies are invalid
        page_url = page.url
        page_title = await page.title()
        if 'login' in page_url.lower() or 'login' in page_title.lower() or 'sign in' in page_title.lower():
            raise Exception(f"X login wall: {page_title} ({page_url})")

        # X_SEARCH_WAIT_FOR_RESULTS_V1_20260813 — wait for the RESULTS, not for a clock.
        #
        # This slept a fixed 4s after domcontentloaded and then scrolled. X's search
        # panel loads lazily, so on a slow render the scroll loop ran against an empty
        # DOM and the function returned [] — indistinguishable from an episode with no
        # posts. That is the root of the non-determinism: measured directly, the query
        # from:… "Wilkerson" since:2026-07-18 returned 0 posts on one pass and 8 posts
        # totalling 526,101 views on the next, same session, minutes apart.
        #
        # Now it waits for a tweet article to exist. If none appears, X's own empty
        # state decides the verdict: "No results for" means genuinely nothing, anything
        # else means the page never rendered and is RAISED so the caller retries rather
        # than banking a zero.
        try:
            await page.wait_for_selector('article[data-testid="tweet"]', timeout=15000)
        except Exception:
            _body = ''
            try:
                _body = (await page.inner_text('body'))[:4000]
            except Exception:
                pass
            if re.search(r'No results for|not find any results|Try searching for something else',
                         _body, re.I):
                return []          # X states it plainly: genuinely no matching posts
            raise Exception("x_search_results_never_rendered")

        for _ in range(5):
            await page.evaluate("window.scrollBy(0, 2000)")
            await page.wait_for_timeout(1500)
        return await page.evaluate(r"""
            () => {
                var out = [];
                document.querySelectorAll('article[data-testid="tweet"]').forEach(t => {
                    var a = t.querySelector('a[href*="/analytics"]');
                    if (!a) return;
                    var href = a.getAttribute('href') || '';
                    var idMatch = href.match(/\/status\/(\d+)/);
                    if (!idMatch) return;
                    var m = (a.getAttribute('aria-label') || a.textContent || '').match(/([\d,.]+)\s*(?:view|View)/i);
                    if (!m) return;
                    out.push([idMatch[1], parseInt(m[1].replace(/,/g,''))]);
                });
                return out;
            }
        """)
    finally:
        await page.close()


# Bounded: X will not be argued with. Three passes recovered every case measured.
MAX_X_SEARCH_PASSES = 6


async def fetch_x_views_with_ctx(ctx, handles, full_name, since_date=None, _return_ids=None,
                                 _meta_out=None):
    """Fetch X tweet views using an existing playwright context (for parallel runs).

    Strategy:
      - For each handle, search `from:{handle} "{full_name}"` (quoted = exact phrase)
      - Native retweets stay with original author so no double-count from RTs.
      - Quote tweets contribute their own distinct view counts (correct).
      - Dedup by tweet ID across all handles.
    """
    if isinstance(handles, str):
        handles = [handles]
    date_filter = f' since:{since_date}' if since_date else ''
    phrase = f'"{full_name}"'
    seen_ids = {}
    # Run the per-handle queries concurrently within this episode
    async def one_handle(h):
        q = f'from:{h} {phrase}{date_filter}'
        try:
            return await _scrape_x_search(ctx, q)
        except Exception:
            return []

    # X_SEARCH_IS_NONDETERMINISTIC_V1_20260813 — ONE PASS IS A SAMPLE, NOT A MEASUREMENT.
    #
    # Measured directly, same session, same cookies, same query, minutes apart:
    #
    #   from:… "Wilkerson"        since:2026-07-18   pass 1: 0 posts        pass 2: 8 posts, 526,101 views
    #   from:… "James Carden"     since:2026-07-11   pass 1: 0 posts        pass 2: 2 posts, 343,753 views
    #   from:… "Lawrence Wilkerson" since:2026-07-18 pass 1: 5 / 267,993    pass 2: 6 / 461,793
    #
    # X's search panel loads lazily and intermittently returns partial or empty results.
    # So an empty result was never evidence of an episode with no posts, and — worse —
    # every NON-empty number in the table is a single draw from a distribution. The
    # working episodes are undercounts, not just the zeros.
    #
    # The accumulation is already union-by-id with max-views-per-id, which is monotone:
    # extra passes can only ever ADD tweets or RAISE a view count, never invent one. So
    # the honest procedure is to keep sampling until a pass contributes nothing new, and
    # to report whether it converged. Bounded, because X will not be argued with.
    _passes, _converged = 0, False
    for _attempt in range(MAX_X_SEARCH_PASSES):
        _passes += 1
        _before = (len(seen_ids), sum(seen_ids.values()))
        results_per_handle = await asyncio.gather(*[one_handle(h) for h in handles])
        for results in results_per_handle:
            for tweet_id, views in results:
                if views > seen_ids.get(tweet_id, 0):
                    seen_ids[tweet_id] = views
        # ZERO IS NEVER CONVERGENCE.
        #
        # The first version stopped as soon as two passes agreed — and two consecutive
        # EMPTY passes agree perfectly, so an episode with 526,101 views reported
        # "0 posts, converged" and looked like a settled measurement. Measured directly:
        # 'Wilkerson' returned 0 on one pass and 8 posts on the next, minutes apart, in
        # the same session. Agreement between two empty samples is the failure mode
        # itself, not evidence against it.
        #
        # A run that has found nothing therefore exhausts every pass before giving up,
        # and still reports UNMEASURED rather than a number.
        if not seen_ids:
            continue
        if (len(seen_ids), sum(seen_ids.values())) == _before and _attempt > 0:
            _converged = True
            break
    if _meta_out is not None:
        # Sampling quality travels on its OWN channel, never inside _return_ids: that
        # dict is {tweet_id: views} and every caller does sum(ids.values()), so a
        # metadata key in there would be a TypeError at best and a corrupted total at
        # worst. A caller that cannot tell a converged measurement from a truncated one
        # will publish both identically, so this has to be available — just not there.
        _meta_out["passes"] = max(_meta_out.get("passes", 0), _passes)
        _meta_out["converged"] = bool(_meta_out.get("converged", True) and _converged)
    # X_REACH_UNION_V1_20260802: when a caller passes _return_ids, merge into it
    # (max views per id) so several search terms can be UNIONed across calls
    # without double-counting a tweet that matches more than one term.
    if _return_ids is not None:
        for tweet_id, views in seen_ids.items():
            if views > _return_ids.get(tweet_id, 0):
                _return_ids[tweet_id] = views
    return sum(seen_ids.values()), len(seen_ids)



async def fetch_x_views(handles, full_name, since_date=None):
    """Standalone wrapper — opens its own browser. Used when called outside the
    shared-context loop. The parallel loop in update_show() uses
    fetch_x_views_with_ctx() to avoid spinning up a browser per episode."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            ctx = await browser.new_context()
            await ctx.add_cookies(X_COOKIES)
            return await fetch_x_views_with_ctx(ctx, handles, full_name, since_date)
        finally:
            await browser.close()


async def update_show(show, ig_clips):
    """Refresh a single show's data file."""
    print(f"\n=== {show['name']} ===")

    # Auto-discover new episodes from YouTube RSS
    print(f"Discovering new episodes...")
    discover_new_episodes(show['yt_channel_id'], show['data_file'])

    if not os.path.exists(show['data_file']):
        print(f"No {show['data_file']} — skipping", file=sys.stderr)
        return
    with open(show['data_file']) as f:
        cache = json.load(f)

    # ──────────────────────────────────────────────────────────────────
    # Normalise existing entries on load. The _R<date> suffix and any
    # title[:30]-truncated guest names were written by an older local
    # scraper (auto_update.py:282,767 in the legacy ~/RumbleMonitor branch).
    # Defensively clean them here so even if dirty data slips in from
    # any future writer it gets corrected on the next run.
    # ──────────────────────────────────────────────────────────────────
    # LEGACY_ROLE_SURNAME_SCRUB_V1_2026_07_20 — surnames that are actually
    # role words (Chief, Officer, Advisor, Minister, Hitman, Whistleblower,
    # Ambassador, President, Commander, General, Admiral, Analyst) are
    # ALWAYS wrong. Force re-derive from title via extract_guest + CANON_MAP;
    # if still bad, mark for drop. Root cause: 2026-07-18 initial import
    # carried "Former R&AW Chief" / "Former Economic Hitman John Perkins" /
    # "Former Pentagon Advisor Jim Rickards" whose extract_guest returns None
    # and normal `is_truncated` heuristic doesn't fire.
    _ROLE_SURNAMES = {
        'chief', 'officer', 'advisor', 'adviser', 'minister', 'hitman',
        'whistleblower', 'ambassador', 'president', 'commander', 'general',
        'admiral', 'analyst', 'director', 'secretary', 'senator', 'governor',
        'mayor', 'attorney', 'spokesperson', 'spokesman', 'spokeswoman',
        'strategist', 'economist', 'journalist', 'academic', 'professor',
    }
    # LEGACY_GUEST_PREFIX_SCRUB_V1_2026_07_20 — even when surname is a real
    # last name (Perkins/Rickards), the guest field may carry a bad role
    # prefix ("Former Economic Hitman John Perkins"). Rewrite via _strip_role
    # + CANON_MAP so consumers see "John Perkins" not "Former Economic Hitman
    # John Perkins". Also fix canonical_guest_full_name if it matches.
    _BAD_GUEST_PREFIX_RE = re.compile(
        r'^(?:Former|Ex|Fmr)\s+(?:[A-Z][A-Za-z\-]+\s+){0,3}',
        re.IGNORECASE,
    )
    _to_drop_idx = []
    _normalised = 0
    for _idx, v in enumerate(cache):
        orig_surname = v.get('surname', '') or ''
        # GU_SURNAME_HARDENING_V1_2026_07_03 - broader suffix strip.
        clean_surname = _strip_r_date_suffix(orig_surname).strip()
        if clean_surname != orig_surname:
            v['surname'] = clean_surname
            _normalised += 1
        # LEGACY_GUEST_PREFIX_SCRUB_V1_2026_07_20 — strip role prefix from
        # guest / canonical_guest_full_name when the last word matches surname.
        # Prefer CANON_MAP full name over lossy prefix strip so "Former
        # Economic Hitman John Perkins" -> "John Perkins", not "Perkins".
        _cur_guest = v.get('guest', '') or ''
        _cur_canon = v.get('canonical_guest_full_name', '') or ''
        _sn_now = (v.get('surname') or '').strip()
        _canon_full = CANON_MAP.get(_sn_now.lower()) if _sn_now else None
        for _field_name, _val in (('guest', _cur_guest), ('canonical_guest_full_name', _cur_canon)):
            if not _val or not _sn_now:
                continue
            if _BAD_GUEST_PREFIX_RE.match(_val) and _val.split()[-1].lower() == _sn_now.lower():
                # LEGACY_GUEST_PREFIX_SCRUB_V1_2026_07_20 — prefer canonical
                # full name from CANON_MAP; fallback to stripped tail.
                if _canon_full:
                    _clean = _canon_full
                else:
                    # Try to keep first name + surname: strip the "Former "
                    # + 1-2 role words but leave the last 2 capitalised tokens.
                    _stripped = _BAD_GUEST_PREFIX_RE.sub('', _val).strip()
                    _parts = _stripped.split()
                    _clean = ' '.join(_parts[-2:]) if len(_parts) >= 2 else _stripped
                if _clean and _clean != _val:
                    v[_field_name] = _clean
                    _normalised += 1
                    print(f"  [GUEST_PREFIX_SCRUB] {_field_name}: {_val[:50]!r} -> {_clean!r}")
        # LEGACY_ROLE_SURNAME_SCRUB_V1_2026_07_20 - detect role-word surnames.
        _is_role_surname = (v.get('surname', '') or '').lower() in _ROLE_SURNAMES
        # GU_SURNAME_HARDENING_V1_2026_07_03 - flag junk-token surnames for re-derive.
        _needs_rederive = not _looks_valid_surname(v.get('surname', '')) or _is_role_surname
        # If guest looks like a title-truncation artefact, re-derive from title
        guest = v.get('guest', '') or ''
        title = v.get('title', '') or ''
        is_truncated = (
            len(guest) >= 28 and (
                guest.endswith((':', ',', ' is', ' the', ' on', ' a', ' an', ' DES', ' St'))
                or (title.startswith(guest) and len(title) > len(guest) + 10)
            )
        )
        if (is_truncated or _needs_rederive) and title:
            new_guest = extract_guest(title)
            new_surname = extract_surname(new_guest) if new_guest else None
            # LEGACY_ROLE_SURNAME_SCRUB_V1_2026_07_20 — try CANON_MAP by title
            # ONLY (do not pass current guest — it may itself be junk that the
            # canonical helper would legitimise via its clean-looking-passthrough
            # branch, e.g. "R&AW Chief"). Only accept a canonical match backed
            # by CANON_MAP (positive canon full name).
            if not (new_guest and new_surname):
                _cfn, _cs_upper, _ = _canonical_from_title(title, "", "")
                if _cfn and _cs_upper:
                    new_guest = _cfn
                    new_surname = _cfn.split()[-1]
            if new_guest and new_surname:
                print(f"  [normalize] guest '{guest[:40]}...' -> '{new_guest}' (surname {new_surname})")
                v['guest'] = new_guest
                v['surname'] = new_surname
                _normalised += 1
            elif _is_role_surname:
                # LEGACY_ROLE_SURNAME_SCRUB_V1_2026_07_20 — role-word survivor
                # with no canonical match is legacy junk; drop it entirely.
                # We log the drop so operator can audit.
                print(f"  [ROLE_SURNAME_DROP] surname={orig_surname!r} guest={guest[:40]!r} "
                      f"title={title[:60]!r}")
                _to_drop_idx.append(_idx)
            elif _needs_rederive:
                # GU_SURNAME_HARDENING_V1_2026_07_03 - safe fallback: blank the surname
                # (Android falls back to guest/title) rather than shipping "Tru" / "DEF".
                print(f"  [normalize] blanking junk surname; title={title[:50]}")
                v['surname'] = ''
                _normalised += 1
    if _to_drop_idx:
        cache = [v for _i, v in enumerate(cache) if _i not in set(_to_drop_idx)]
        print(f"  [ROLE_SURNAME_DROP] removed {len(_to_drop_idx)} legacy role-word entries")
        _normalised += len(_to_drop_idx)
    if _normalised:
        print(f"  [normalize] cleaned {_normalised} legacy/truncated entries")

    # CANONICAL_URL_BIND_V1_2026_07_20 — clean up phantom Shorts-as-productions
    # rows and backfill canonical URL identity fields BEFORE dedupe.
    try:
        _channel_id = show.get('yt_channel_id') or ''
        _data_basename = os.path.basename(show['data_file'])
        _n_bound, _n_drop, _ = _url_bind_cleanup_and_backfill(
            cache, _channel_id, ROOT, _data_basename)
        # Filter out the dropped rows: reconstruct cache preserving order after
        # the audit trail was already persisted. _url_bind_cleanup_and_backfill
        # mutates rows in-place with new fields and returns dropped meta only.
        # Rebuild cache by keeping rows still present after re-checking.
        # The helper doesn't remove from cache directly (avoids re-index bugs);
        # we re-filter here using the same drop criteria — matched-any-ep gate.
        _rss = _fetch_youtube_full_episodes(_channel_id)
        if _rss:
            _by_vid = _rss
            def _keeps(row):
                if row.get('is_upcoming'):
                    return True
                # RUMBLE_FIRST_HONORIFIC_KEEP_V1_2026_08_10 — rows injected by the
                # local Rumble bridge are NOT YouTube rows: a Rumble-first episode
                # has no canonical_video_id and cannot match the YT RSS, because it
                # is not on YouTube yet. So it fell through to the YouTube-Shorts
                # heuristic below, which drops any title opening with an honorific
                # ("Prof.", "Dr.", "Col.", "Amb.", "Sen.", "Gen.", "Former", "Ex-").
                # That silently deleted every Rumble-first episode with an academic
                # or military guest title — e.g. "Prof. John Mearsheimer Explains Why
                # Iran War MUST END" (Rumble 2026-08-09), which the hourly bridge
                # re-injected and this filter re-deleted within minutes, every hour.
                # YouTube-sourced episodes are unaffected: they keep their own
                # canonical_video_id and return True on the check below. The bridge
                # applies its own recency + attribution gates before injecting.
                if row.get('rumble_only_injected'):
                    return True
                # CANONICAL_URL_BIND_V1_2026_07_20 — never drop rows that
                # already have any canonical_video_id (rolled-off-RSS preserve).
                if row.get('canonical_video_id'):
                    return True
                if (row.get('canonical_video_id') or '') in _by_vid:
                    return True
                # Try title match against remaining episodes
                _t = row.get('title') or ''
                for _ep in _by_vid.values():
                    if _url_bind_title_match(_t, _ep['title']):
                        return True
                # Fall back to age + short/clip-titled gates
                import datetime as _dt2
                now2 = _dt2.datetime.utcnow()
                _ds = row.get('date') or ''
                _age = 9999
                try:
                    _d = _dt2.datetime.strptime(_ds, '%d %b').replace(year=now2.year)
                    if _d > now2: _d = _d.replace(year=now2.year - 1)
                    _age = (now2 - _d).days
                except Exception:
                    pass
                _ct_g, _ = _yt_classify_content(
                    row.get('link') or row.get('canonical_video_url') or '',
                    _t, '')
                _shorts_prefix_re2 = re.compile(
                    r"^(?:Prof\.?|Dr\.?|Amb\.?|Sen\.?|Col\.?|Gen\.?|Fmr|Former|"
                    r"Ex[\s\-]|Retired|Ret\.?)\s+", re.I)
                _looks = (_ct_g in ('SHORT','CLIP')
                          or bool(_shorts_prefix_re2.match(_t)))
                if _looks or _age > 45:
                    return False
                return True
            cache = [r for r in cache if _keeps(r)]
        print(f"  [URL_BIND] bound/backfilled {_n_bound}, dropped {_n_drop} row(s)")
    except Exception as _e:
        print(f"  [URL_BIND] cleanup err (fail-open): {_e}", file=sys.stderr)

    # Dedupe on (guest, date) - merge view counts, keep highest per field
    def _to_num(x):
        x = str(x or '').replace(',', '').strip().upper()
        if not x or x == '?': return -1
        if x.endswith('K'): return float(x[:-1]) * 1000
        if x.endswith('M'): return float(x[:-1]) * 1_000_000
        try: return float(x)
        except Exception: return -1
    # CANONICAL_URL_BIND_V1_2026_07_20 — dedupe by canonical URL identity
    # (canonical_video_id or canonical_episode_id_v2) when present. Metrics
    # merge as last-writer-wins per platform WITHIN a canonical, so a Short
    # cannot inflate the full episode's metrics across canonical boundaries.
    _pre_len = len(cache)
    cache = _url_bind_dedupe_by_canonical(cache)
    if len(cache) != _pre_len:
        print(f"  [URL_BIND_DEDUPE] deduped {_pre_len - len(cache)} canonical duplicates")
    # Legacy (guest, date) fallback dedupe for any rows still lacking canonical
    # fields (safety net; should be rare after URL_BIND cleanup).
    _seen = {}
    _deduped_cache = []
    for v in cache:
        if v.get('canonical_video_id') or v.get('canonical_episode_id_v2'):
            _deduped_cache.append(v); continue
        key = (v.get('guest', ''), v.get('date', ''))
        if key not in _seen:
            _seen[key] = v
            _deduped_cache.append(v)
            continue
        prev = _seen[key]
        for field in ('rumble_views', 'x_views', 'yt_views', 'ig_likes'):
            if _to_num(v.get(field, '?')) > _to_num(prev.get(field, '?')):
                prev[field] = v.get(field, '?')
        if 'show' in v and 'show' not in prev:
            prev['show'] = v['show']
    if len(_deduped_cache) != len(cache):
        print(f"  [normalize] deduped {len(cache) - len(_deduped_cache)} legacy (guest, date) duplicates")
        cache = _deduped_cache

    # METRIC_ATTRIB_V1_2026_07_20 — pass known surnames so YT views attach
    # only to real guests. Falls back to topic-stopword heuristic if empty.
    _known = {
        (v.get('canonical_surname_upper') or v.get('surname') or '').lower()
        for v in cache if (v.get('surname') or v.get('canonical_surname_upper'))
    }
    _known.discard('')
    yt, yt_dates = fetch_youtube_data(show['yt_channel_id'], known_surnames=_known)

    # Helper: convert "25 Apr" or "21 Mar" to ISO YYYY-MM-DD using current year
    from datetime import datetime
    def short_to_iso(short):
        if not short: return None
        try:
            d = datetime.strptime(short, '%d %b').replace(year=datetime.now().year)
            # If date is in the future, it must be last year
            if d > datetime.now():
                d = d.replace(year=datetime.now().year - 1)
            return d.strftime('%Y-%m-%d')
        except Exception:
            return None

    # Parallelize X scraping across episodes using a single shared browser.
    # Each episode opens its own playwright page via _scrape_x_search; we cap
    # concurrent pages with a semaphore to avoid OOM on the runner.
    eligible = [v for v in cache
                if v.get('surname', '').lower() and (v.get('guest') or '').strip()]
    handles = [show['x_handle'], 'afshinrattansi']
    MAX_CONCURRENT_EPISODES = 4
    use_date_window = show.get('x_date_window', False)

    # Build per-episode date windows (for small-account shows: since=episode_date, until=next_episode_date)
    date_windows = {}
    if use_date_window:
        sorted_ep = sorted(eligible,
                           key=lambda v: yt_dates.get(v.get('surname','').lower()) or short_to_iso(v.get('date','')) or '9999')
        for i, v in enumerate(sorted_ep):
            s = yt_dates.get(v.get('surname','').lower()) or short_to_iso(v.get('date',''))
            u = None
            if i + 1 < len(sorted_ep):
                nxt = sorted_ep[i + 1]
                u = yt_dates.get(nxt.get('surname','').lower()) or short_to_iso(nxt.get('date',''))
            date_windows[v.get('surname','')] = (s, u)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            ctx = await browser.new_context()
            await ctx.add_cookies(X_COOKIES)
            # X_VIEWS_CACHE_FALLBACK_V1_2026_07_04 -----------------------------
            # Read RumbleMonitor/x_2026.json (fresh ~15min per health-guard).
            # X_CACHE_PATH_PORTABILITY_V1_20260813 — this was a single hardcoded path
            # under /Users/afshin. fetch_and_push.py runs in GITHUB ACTIONS, where that
            # path cannot exist, so the cache fallback silently returned (0, 0) on every
            # cloud run and the pipeline then treated "the fallback is unavailable"
            # identically to "the fallback found nothing". Candidates are tried in order
            # and the outcome is recorded, so an unavailable cache is visible instead of
            # looking like a measurement of zero.
            _X_CACHE_CANDIDATES = [
                os.environ.get("X_CACHE_PATH"),
                os.path.join(os.path.dirname(os.path.abspath(__file__)), "x_2026.json"),
                "/Users/afshin/RumbleMonitor/x_2026.json",
            ]
            _X_CACHE_DATA = {"loaded": False, "results": {}, "available": None, "path": None}
            def _x_from_cache(surname, show_code):
                """Sum view_count of tweets in the given show whose text mentions
                the guest's surname (case-insensitive). Returns (total, count)."""
                if not surname: return 0, 0
                if not _X_CACHE_DATA["loaded"]:
                    _X_CACHE_DATA["loaded"] = True
                    for _cand in _X_CACHE_CANDIDATES:
                        if not _cand or not os.path.exists(_cand):
                            continue
                        try:
                            with open(_cand) as _f: _xc = json.load(_f)
                            _X_CACHE_DATA["results"] = _xc.get("results") or {}
                            _X_CACHE_DATA["available"] = True
                            _X_CACHE_DATA["path"] = _cand
                            break
                        except Exception as _e_cache:
                            print(f"  [X_CACHE] unreadable {_cand}: {_e_cache}",
                                  file=sys.stderr)
                    if _X_CACHE_DATA["available"] is None:
                        _X_CACHE_DATA["available"] = False
                        print("  [X_CACHE] UNAVAILABLE — no candidate path exists; the "
                              "cache fallback contributes nothing this run (this is NOT "
                              "evidence of zero views)", file=sys.stderr)
                if not _X_CACHE_DATA["available"]:
                    return 0, 0
                _show_data = (_X_CACHE_DATA["results"].get(show_code) or {})
                _tweets = _show_data.get("tweets_2026") or []
                sn_lower = surname.lower()
                _total = 0; _n = 0
                for _t in _tweets:
                    _txt = (_t.get("text") or "").lower()
                    if sn_lower not in _txt: continue
                    try: _vc = int(_t.get("view_count") or 0)
                    except Exception: _vc = 0
                    _total += _vc; _n += 1
                return _total, _n

            def _show_code_for(_show):
                """Map show dict → 'GU'/'NO' for x_2026.json lookup."""
                _n = str(_show.get("name") or "").lower()
                if "going underground" in _n: return "GU"
                if "new order" in _n: return "NO"
                return "?"

            sem = asyncio.Semaphore(MAX_CONCURRENT_EPISODES)

            async def process_episode(v):
                full_name = (v.get('guest') or '').strip()
                surname = v.get('surname', '').strip()
                since = yt_dates.get(surname.lower()) or short_to_iso(v.get('date', ''))
                async with sem:
                    try:
                        # X_REACH_UNION_V1_20260802 — ROOT CAUSE OF THE BARNES UNDERCOUNT.
                        #
                        # `full_name` is the episode's guest STRING, which carries a role
                        # prefix: "Trump's Ex-Lawyer Robert Barnes". fetch_x_views_with_ctx
                        # searches it as an EXACT PHRASE, so it only matches posts
                        # reproducing that whole literal. For the 2026-08-01 Barnes episode
                        # exactly 1 of 13 Barnes tweets did.
                        #
                        # The surname search existed but was gated on `total == 0`. Because
                        # the phrase matched something non-zero, the broad search NEVER RAN,
                        # and 12 tweets carrying ~1.09M of 1.32M views were silently dropped.
                        # A gate on "did we find anything" cannot detect "did we find
                        # everything" -- that is the whole defect in one line.
                        #
                        # Fix: run BOTH queries always and UNION by tweet id. Dedup is
                        # already max-views-per-id inside fetch_x_views_with_ctx, so a tweet
                        # matching both queries is counted once, and native RTs still stay
                        # with their original author. No totals are patched or hardcoded.
                        ids, _meta = {}, {}

                        async def _accum(term):
                            if not term:
                                return
                            t_, _c = await fetch_x_views_with_ctx(
                                ctx, handles, term, since_date=since, _return_ids=ids,
                                _meta_out=_meta)

                        await _accum(full_name)
                        if surname and len(surname) > 3:
                            await _accum(surname)
                        total, count = sum(ids.values()), len(ids)
                        # Fallback 2 (x_date_window shows): sum handle tweets in episode date window
                        if total == 0 and use_date_window:
                            win_since, win_until = date_windows.get(surname, (since, None))
                            if win_since:
                                # Try show handle first, then afshinrattansi mentioning show name
                                for fb_q in [
                                    f'from:{show["x_handle"]} since:{win_since}' + (f' until:{win_until}' if win_until else ''),
                                    f'from:afshinrattansi "{show["name"]}" since:{win_since}' + (f' until:{win_until}' if win_until else ''),
                                ]:
                                    results = await _scrape_x_search(ctx, fb_q)
                                    if results:
                                        total = sum(vv for _, vv in results)
                                        count = len(results)
                                        print(f"  {surname}: date-window fallback ({count} tweets, q={fb_q[:60]})")
                                        break
                        # X_VIEWS_CACHE_FALLBACK_V1_2026_07_04 — cache fallback
                        if total == 0 and surname:
                            _cache_total, _cache_n = _x_from_cache(surname, _show_code_for(show))
                            if _cache_total > 0:
                                total = _cache_total; count = _cache_n
                                print(f"  {surname}: cache-fallback ({count} tweets, X:{format_views(total)})")
                        if total > 0:
                            v['x_views'] = format_views(total)
                            # Stamp the SUCCESS too. Setting _x_status only on failure
                            # left a stale UNMEASURED_NO_POSTS_FOUND sitting next to a
                            # freshly measured number on almost every row — the value
                            # was right and the label said it had never been measured.
                            v['_x_status'] = ('MEASURED' if _meta.get('converged', True)
                                              else 'MEASURED_LOWER_BOUND')
                            v['_x_passes'] = _meta.get('passes')
                            print(f"  {surname}: {count} tweets, X:{v['x_views']}"
                                  f" [{_meta.get('passes')} passes,"
                                  f" {'converged' if _meta.get('converged') else 'floor'}]")
                        else:
                            # X_UNMEASURED_IS_NOT_ZERO_V1_20260813 — ROOT CAUSE OF THE
                            # BLUMENTHAL "X 0".
                            #
                            # This branch used to write the string '0' whenever every X
                            # query came back empty. That is a claim the pipeline is not
                            # entitled to make: finding no posts means WE FOUND NOTHING, not
                            # that the episode reached nobody. Five of fourteen GU episodes
                            # shipped X "0" on that basis — Blumenthal, Wilkerson, Carden,
                            # Kucinich, Ben-Menashe — while the authoritative X cache held
                            # 16 Blumenthal posts totalling 515,643 views. The dashboard then
                            # summed the fabricated 0 into the episode total, so a 21K total
                            # was presented as complete when the largest platform was simply
                            # missing.
                            #
                            # GU_UNKNOWN_IS_NULL_V2_2026_08_06 had already established the
                            # correct rule and applied it to every metric — but it ran LATER
                            # in the file and only rewrites '?', so this line got there first
                            # and destroyed the marker it was looking for. The V2 comment
                            # even says it "mirrors the existing X pattern": the X path was
                            # the TEMPLATE for the bad rule and was the one place never
                            # migrated off it.
                            #
                            # Unknown now stays unknown all the way to the renderer.
                            v['_x_status'] = 'UNMEASURED_NO_POSTS_FOUND'
                            print(f"  {surname}: X UNMEASURED (no posts matched; "
                                  f"NOT recorded as zero)", file=sys.stderr)
                    except Exception as e:
                        # A failed retrieval is the clearest possible unknown. It must never
                        # be able to reach the display as a number.
                        print(f"  {surname}: X error {e}", file=sys.stderr)
                        v['_x_status'] = f'FETCH_FAILED:{type(e).__name__}'


            await asyncio.gather(*[process_episode(v) for v in eligible])
        finally:
            await browser.close()

    # METRIC_ATTRIB_V1_2026_07_20 — attribute using canonical surname first,
    # fall back to raw surname. Only OVERWRITE when the new value is
    # non-null AND non-'0' (avoid regressing a real number to null when the
    # scraper misses one cycle — root cause of the 2026-07-20 yt_views wipe).
    for v in cache:
        _sn_candidates = [
            (v.get('canonical_surname_upper') or '').lower(),
            (v.get('surname') or '').lower(),
        ]
        for surname in _sn_candidates:
            if not surname:
                continue
            if surname in yt:
                _new = yt[surname]
                _cur = v.get('yt_views')
                # Only overwrite if new is a positive number OR the current is
                # missing/empty. Preserves a real number when scraper misses.
                if _new and _new not in ('0', 0, '?', None):
                    v['yt_views'] = _new
                elif _cur in (None, '?', '', 0):
                    v['yt_views'] = _new
            if surname in ig_clips:
                _new = ig_clips[surname]
                _cur = v.get('ig_likes')
                if _new and _new not in ('0', 0, '?', None):
                    v['ig_likes'] = _new
                # GU_UNKNOWN_IS_NULL_V2_2026_08_06 — string '0' added deliberately.
                # The old '?' -> '0' scrubber wrote a STRING zero, which did not
                # match this tuple (int 0 != '0'), so a field corrupted once could
                # never be refilled: the zero was sticky for the life of the cache.
                # Treating it as refillable does not invent a value -- it lets the
                # next cycle RE-MEASURE. If Instagram genuinely reports 0, 0 is
                # written back; if it cannot be measured, it stays null.
                elif _cur in (None, '?', '', 0, '0'):
                    v['ig_likes'] = _new
            # Only need to attribute once per entry.
            break

    # GU_UPCOMING_WIRE_V2_2026_07_03 -----------------------------------------
    # 1. Strip stale upcoming (they get re-prepended fresh each cycle)
    cache = [v for v in cache if not v.get("is_upcoming")]
    # 2. Compute show_code from data_file basename
    _basename = os.path.basename(show['data_file'])
    _show_code = "GU" if _basename == "videos.json" else ("NO" if _basename == "videos_neworder.json" else "?")
    # 3. Prepend fresh upcoming for this show
    _upcoming = _load_upcoming_for(_show_code)
    if _upcoming:
        cache = _upcoming + cache
        print(f"  [UPCOMING] prepended {len(_upcoming)} entry/entries for {_show_code}")
    # 4. PUBLISH_GUARD_V1 active — refuse write on bad tokens
    try:
        _guard_ok = _publish_guard_scan(cache, _show_code)
    except NameError:
        _guard_ok = True  # guard fn missing → fail-open with warning
        print(f"  [PUBLISH_GUARD] fn missing — fail-open", file=sys.stderr)
    if _guard_ok is False:
        print(f"  [PUBLISH_GUARD] BLOCKED write for {_show_code} — bad tokens detected", file=sys.stderr)
        return
    # GU_STATS_SHOW_ATTRIBUTION_V1_2026_07_04 — normalize show field per-entry
    # so downstream 1-week filter and Android tab attribution are reliable.
    _norm_count = 0
    if _show_code in ("GU", "NO"):
        for _v in cache:
            if _v.get("show") != _show_code:
                _v["show"] = _show_code
                _norm_count += 1
    if _norm_count:
        print(f"  [SHOW_NORMALIZE] set show={_show_code!r} on {_norm_count} entries")
    # CANONICAL_PUBLISH_V1_2026_07_10 v2 — self-contained canonical resolution.
    # Populates canonical_guest_full_name and rewrites `guest` (Android reads it)
    # when the current value is a bad-truncation. Uses an inline surname->canonical
    # map so this pass works even when known_guests_v1.json / resolve_guest_identity
    # helpers are not present on this branch.
    _CANON_MAP = {
        # surname_lowercase: "Canonical Full Name"
        "ellwood":     "Tobias Ellwood",
        "wilkerson":   "Lawrence Wilkerson",
        "kucinich":    "Dennis Kucinich",
        "pyne":        "David Pyne",
        "kortunov":    "Andrey Kortunov",
        "trenin":      "Dmitri Trenin",
        "blumenthal":  "Max Blumenthal",
        "mearsheimer": "John Mearsheimer",
        "shlaim":      "Avi Shlaim",
        "sibal":       "Kanwal Sibal",
        "bhaskar":     "C. Uday Bhaskar",
        "sood":        "Vikram Sood",
        "sachs":       "Jeffrey Sachs",
        "wolff":       "Richard Wolff",
        "bolton":      "John Bolton",
        "hanke":       "Steve Hanke",
        "keen":        "Steve Keen",
        "olmert":      "Ehud Olmert",
        "postol":      "Theodore Postol",
        "roberts":     "Paul Craig Roberts",
        "weihua":      "Chen Weihua",
        "weiwei":      "Zhang Weiwei",
        "ben-menashe": "Ari Ben-Menashe",
        "menashe":     "Ari Ben-Menashe",
        "bryant":      "Wes Bryant",
        "carden":      "James Carden",
        # LEGACY_GUEST_PREFIX_SCRUB_V1_2026_07_20 — kept in sync with top-level CANON_MAP.
        "perkins":     "John Perkins",
        "rickards":    "Jim Rickards",
        "sakwa":       "Richard Sakwa",
        "macgregor":   "Douglas Macgregor",
        "fritz":       "Dennis Fritz",
        "freeman":     "Chas Freeman",
        "flynn":       "Michael Flynn",
        "clark":       "Wesley Clark",
        "vallely":     "Paul Vallely",
        "astore":      "William J. Astore",
    }
    _BAD_PREFIXES = ("Ex-", "Former ", "Fmr ", "SLAMS ", "BLASTS ", "REVEALS ", "EXPOSES ", "WARNS ", "'")
    _canon_resolved = 0
    _canon_unchanged = 0
    for _v in cache:
        try:
            _title = _v.get("title") or ""
            _cur_guest = (_v.get("guest") or "").strip()
            _title_low = _title.lower()
            _canon = None
            # 1) Surname substring scan against inline canonical map
            for _sn_low, _cn in _CANON_MAP.items():
                if _sn_low in _title_low:
                    _canon = _cn
                    break
            # 2) If not matched by title, try current guest field
            if not _canon and _cur_guest:
                _cg_low = _cur_guest.lower()
                for _sn_low, _cn in _CANON_MAP.items():
                    if _sn_low in _cg_low:
                        _canon = _cn
                        break
            # 3) If not matched but current guest looks well-formed (2+ words,
            #    no bad prefix, no trailing truncation), treat it as canonical.
            if not _canon and _cur_guest and " " in _cur_guest and not _cur_guest.endswith(" "):
                if not any(_cur_guest.startswith(p) for p in _BAD_PREFIXES):
                    _last = _cur_guest.split()[-1]
                    if not (_last[:1].isupper() and _last.endswith(("rat","ing","tio","ion","ent","ies","nes")) and len(_last) < 12):
                        _canon = _cur_guest
            if _canon:
                _v["canonical_guest_full_name"] = _canon
                # Overwrite guest when current is bad-truncation.
                _bad = False
                if not _cur_guest: _bad = True
                elif _cur_guest.endswith(" "): _bad = True
                elif any(_cur_guest.startswith(p) for p in _BAD_PREFIXES): _bad = True
                else:
                    _last = _cur_guest.split()[-1] if _cur_guest.split() else ""
                    if _last[:1].isupper() and _last.endswith(("rat","ing","tio","ion","ent","ies","nes")) and len(_last) < 12:
                        _bad = True
                if _bad or _cur_guest != _canon:
                    _v["guest"] = _canon
                    # Rewrite surname too so downstream extractors don't re-read stale
                    # value like "War" (Carden) or "Minister" (Ellwood).
                    _v["surname"] = _canon.split()[-1]
                _canon_resolved += 1
            else:
                _v.setdefault("canonical_guest_full_name", None)
                _canon_unchanged += 1
            # CANONICAL_FIELD_EMISSION_V1_2026_07_11 — always populate
            # canonical_surname_upper and canonical_episode_id so the Tidbyt
            # renderer and push84_lametric.py never fall back to raw `surname`.
            _cs_source = _canon or _v.get("canonical_guest_full_name") or _v.get("surname") or ""
            if _cs_source:
                _last_word = _cs_source.split()[-1] if " " in _cs_source else _cs_source
                _v["canonical_surname_upper"] = _last_word.upper()
            else:
                _v.setdefault("canonical_surname_upper", None)
            _hash_src = _title if _title else _cs_source
            _v["canonical_episode_id"] = hashlib.sha1(
                _hash_src.encode("utf-8")).hexdigest()[:12]
        except Exception:
            _v.setdefault("canonical_guest_full_name", None)
            _v.setdefault("canonical_surname_upper", None)
            _v.setdefault("canonical_episode_id", None)
            _canon_unchanged += 1
    # GU_NO_QUESTION_MARK_V1_2026_07_13 --------------------------------
    # Defense-in-depth: never leave literal '?' in rumble_views/yt_views/ig_likes/x_views.
    # Mirrors the existing X pattern (line 852/855). The dashboard (docs/index.html)
    # ALSO defensively renders '?' as 'N/A' as of GU_HEALTH_MIGRATION_V1_2026_07_13
    # -- belt-and-braces so downstream Android / Tidbyt / LaMetric consumers that lack
    # the client-side renderer do not display '?' either.
    # GU_UNKNOWN_IS_NULL_V2_2026_08_06 — supersedes GU_NO_QUESTION_MARK_V1.
    #
    # The previous rule rewrote '?' -> '0'. '?' is this pipeline's own marker for
    # UNKNOWN (see the new-episode defaults ~line 1158), so that rewrite destroyed
    # the distinction between "the source reported zero" and "we never measured
    # it". Gabor Maté shipped ig_likes "0" and rumble_views "0" purely because no
    # Instagram or Rumble content was mapped to that episode -- the dashboard then
    # rendered a literal 0 and counted it as 0 in the episode total, while
    # yt_views (which was a real null) correctly rendered N/A. Same condition,
    # two representations, one of them silently wrong.
    #
    # Unknown now stays null all the way through cache -> health -> render.
    # A source-confirmed 0 is a real int/str 0 and is left untouched.
    _qm_normalized = 0
    for _v in cache:
        for _f in ('rumble_views', 'yt_views', 'ig_likes', 'x_views'):
            if _v.get(_f) == '?':
                _v[_f] = None
                _qm_normalized += 1
    if _qm_normalized:
        print(f"  [GU_UNKNOWN_IS_NULL_V2] preserved {_qm_normalized} unknown '?' -> null fields")
    print(f"  [CANONICAL_PUBLISH_V1_v2 + CANONICAL_FIELD_EMISSION_V1_2026_07_11] resolved={_canon_resolved} unchanged={_canon_unchanged}")
    # CANONICAL_URL_BIND_V1_2026_07_20 — sort by pub_iso desc (freshest first)
    try:
        cache = _url_bind_sort_by_pub_iso(cache)
    except Exception as _e_sort:
        print(f"  [URL_BIND_SORT] fail-open: {_e_sort}", file=sys.stderr)
    # EPISODE_UNION_NEVER_SHRINKS_V1_20260814 — an episode that existed cannot vanish
    # because ONE upstream fetch came back short.
    #
    # Measured from this repo's own commit history: since 2026-08-13T00:07 local, the cloud
    # run has REPLACED the inventory roughly every few hours, each time losing
    # {Ünal, Ben-Menashe, Carden, Fritz} and introducing June episodes (Olmert, Keen,
    # Postol) plus parser debris ("Israel’s", "DEF"). The local Rumble/IG bridge then
    # restored them on its next pass. So Ünal oscillated in and out of the published stats
    # all day, and whether it was visible depended purely on which job committed last.
    #
    # The trigger is structural: the whole URL_BIND cleanup — including the 45-day age
    # filter — sits behind `if _rss:`, so a failed or partial YouTube feed skips it and the
    # run emits a different, older inventory instead of preserving the known one. That is
    # the same shape as the 38-day-old rates universe: a refresh that can SHRINK coverage
    # when its source is unavailable.
    #
    # The rule is therefore the same one used there: a refresh may ADD, and may UPDATE, but
    # it may never silently DROP. Any episode present in the previous file and absent from
    # this run is carried forward with its last known metrics and logged. Genuine removals
    # (upcoming rows that aired, explicit dedupe) still work because they rewrite or replace
    # the row rather than omitting it — and anything re-added is visible in the log rather
    # than happening quietly.
    try:
        import datetime as _dt      # local: module scope does not carry this alias
        _prev_path = show['data_file']
        _prev = json.load(open(_prev_path)) if os.path.exists(_prev_path) else []
        def _ep_key(_r):
            # EPISODE_KEY_MUST_BE_PRESENT_ON_EVERY_ROW_V1_20260815 — this chain started with
            # `canonical_video_id`, which a CARRIED-FORWARD row does not carry. So the fresh
            # copy of an episode keyed on its video id while the carried-forward copy of the
            # SAME episode fell through to `surname|date`, the two keys never matched, and the
            # union appended a second copy every run. Measured on the live file: 30 rows for
            # 18 real episodes, 11 surnames duplicated, which is what put two Mearsheimer and
            # two Maté rows on the dashboard with different view counts.
            #
            # `canonical_episode_id` is present on 30/30 rows and groups every duplicate pair
            # correctly with ZERO collisions across distinct titles, so it leads the chain.
            # It also absorbs parser debris: the bad-surname row "Israel’s" carries the same
            # episode id as the Fritz episode it was mis-parsed from, so keying on it merges
            # the debris away instead of publishing it as a separate guest.
            # STRONGEST_IDENTITY_FIRST_V1_20260815 — canonical_episode_id is sha1(TITLE),
            # and a title is a MUTABLE STRING. Production proof: the same New Order episode
            # appeared twice on 13 Aug as 10f843fd1fef and ab8e6b132e80, because one copy
            # spelled it "America’s Missteps" (U+2019) and the other "America's Missteps"
            # (U+0027). One character, two identities, two rows on the phone.
            #
            # Both copies carried the SAME canonical_video_id (CfY37_DnbIM) and the same
            # pub_iso. A platform content id is real identity; a hash of a title is a proxy for
            # it. So the video id leads the chain now, and the title hash — which remains the
            # fallback for Rumble-first rows that have no video id yet — is taken over a
            # NORMALISED title so punctuation variants cannot fork an episode again.
            return (str(_r.get('canonical_video_id') or '').strip()
                    or _norm_title_id(_r)
                    or str(_r.get('canonical_episode_id') or '').strip()
                    or str(_r.get('canonical_episode_id_v2') or '').strip()
                    or f"{str(_r.get('surname') or '').upper()}|{str(_r.get('date') or '')}")
        _now_keys = {_ep_key(r) for r in cache}
        _readded = []
        for _r in _prev:
            if not isinstance(_r, dict):
                continue
            if _ep_key(_r) in _now_keys:
                continue
            if _r.get('is_upcoming'):
                continue          # an upcoming row legitimately disappears once it airs
            _r = dict(_r)
            _r['_carried_forward_iso'] = _dt.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            _r['_carried_forward_reason'] = (
                'absent from this run’s feed; retained under EPISODE_UNION_NEVER_SHRINKS_V1 '
                'because a known episode must not disappear on one failed lookup')
            cache.append(_r)
            _now_keys.add(_ep_key(_r))     # a carried-forward row is now PRESENT; a second
                                           # copy of the same episode must not also be added
            _readded.append(f"{_r.get('surname')}|{_r.get('date')}")

        # EPISODE_COLLAPSE_V1_20260815 — repair as well as prevent. The key fix above stops new
        # duplicates being created, but the published file already carried 30 rows for 18
        # episodes, and those rows arrive here through `_prev`. Collapse any remaining rows that
        # share an episode key, preferring the FRESH row over a carried-forward one and filling
        # each missing metric from the copies being discarded — never letting an absent value
        # overwrite a measured one, which is the METRIC_NEVER_REGRESSES rule one level up.
        # IDENTITY_BACKFILL_BEFORE_KEYING_V1_20260815 — EPISODE_COLLAPSE_V1 merges on
        # `_ep_key`, but a CARRIED-FORWARD row has NO canonical_episode_id, so it falls through
        # the chain to `surname|date` while the fresh copy of the SAME episode keys on its id.
        # Two keys, so the collapse cannot see one episode and both survive. Measured in
        # production at 16:01Z: Milanovic id=fd9388b8783d with X 11.7K beside Milanovic
        # id=None carried-forward with X missing; Shidore doubled the same way on 13 Aug.
        #
        # The id is deterministic from the TITLE and every copy of an episode carries the same
        # title, so it is computed for any row missing one BEFORE keys are taken.
        #
        # This patch was written earlier today and LOST TWICE: once to a `git reset --hard`
        # resolving a merge conflict, once to `git pull --rebase --autostash` in the local
        # bridge, which stashes uncommitted work. Both times the pipeline was then "verified"
        # by reading an intermediate JSON that looked clean. It is committed in the same
        # operation as the edit now.
        import hashlib as _hashlib_backfill
        for _r in cache:
            if isinstance(_r, dict) and not _r.get('canonical_episode_id'):
                _t = (_r.get('title') or '').strip()
                if _t:
                    _r['canonical_episode_id'] = _hashlib_backfill.sha1(
                        _t.encode('utf-8')).hexdigest()[:12]
        _collapsed, _seen_idx = [], {}
        for _r in cache:
            _k = _ep_key(_r)
            if _k not in _seen_idx:
                _seen_idx[_k] = len(_collapsed)
                _collapsed.append(_r)
                continue
            _keep = _collapsed[_seen_idx[_k]]
            _drop = _r
            # prefer the row that is NOT carried forward; if both or neither, keep the first
            if _keep.get('_carried_forward_iso') and not _drop.get('_carried_forward_iso'):
                _keep, _drop = _drop, _keep
            for _f, _v in _drop.items():
                if _is_blank(_keep.get(_f)) and not _is_blank(_v):
                    _keep[_f] = _v
            # never publish a placeholder: an unknown stays absent rather than rendering "?"
            for _f in list(_keep):
                if isinstance(_keep[_f], str) and _keep[_f].strip() in ('?', '-', 'n/a', 'N/A'):
                    _keep[_f] = None
            _collapsed[_seen_idx[_k]] = _keep
        if len(_collapsed) != len(cache):
            print(f"  [EPISODE_COLLAPSE_V1] {len(cache)} rows -> {len(_collapsed)} unique "
                  f"episodes for {os.path.basename(show['data_file'])}")
        cache = _collapsed
        # METRIC_NEVER_REGRESSES_TO_UNKNOWN_V1_20260814 — the same rule, one level down.
        #
        # GU_UNKNOWN_IS_NULL_V2 correctly stopped a failed lookup becoming a fake ZERO. But
        # it let a failed lookup overwrite a MEASURED value with null, which destroys
        # evidence just as effectively. Measured across today's commits, six episodes lost
        # a real number this way — Ünal 419.0K, Carden 126.8K, Ben-Menashe 134.7K and three
        # others — each replaced by null on a later run whose X search happened to fail.
        #
        # A measurement is a fact that was true when taken. A later failure to reproduce it
        # is information about the SEARCH, not about the episode. So unknown may fill an
        # empty field and may never replace a filled one.
        _prev_by_key = {}
        for _r in _prev:
            if isinstance(_r, dict):
                _prev_by_key[_ep_key(_r)] = _r
        _restored = []
        for _r in cache:
            _old = _prev_by_key.get(_ep_key(_r))
            if not _old:
                continue
            for _f in ('x_views', 'yt_views', 'rumble_views', 'ig_likes'):
                _new_v, _old_v = _r.get(_f), _old.get(_f)
                _new_unknown = _new_v in (None, '', '?', 'None')
                _old_known = _old_v not in (None, '', '?', 'None', '0')
                if _new_unknown and _old_known:
                    _r[_f] = _old_v
                    _r[f'_{_f}_retained_iso'] = _dt.datetime.now(_dt.UTC).strftime('%Y-%m-%dT%H:%M:%SZ')
                    _r[f'_{_f}_retained_reason'] = (
                        'this run could not measure it; the previous MEASURED value is kept '
                        'under METRIC_NEVER_REGRESSES_TO_UNKNOWN_V1')
                    _restored.append(f"{_r.get('surname')}.{_f}={_old_v}")
        if _restored:
            print(f"  [METRIC_RETAIN] kept {len(_restored)} measured value(s) a failed "
                  f"lookup would have erased: {', '.join(_restored[:8])}", file=sys.stderr)

        if _readded:
            print(f"  [EPISODE_UNION] carried forward {len(_readded)} episode(s) missing "
                  f"from this run: {', '.join(_readded)}", file=sys.stderr)
        else:
            print(f"  [EPISODE_UNION] no episode lost this run ({len(cache)} total)")
    except Exception as _e_union:
        # Fail OPEN on the union check itself, but say so — silently shrinking is the
        # failure this exists to prevent, so a broken guard must be visible.
        print(f"  [EPISODE_UNION] GUARD FAILED, inventory not protected this run: "
              f"{_e_union}", file=sys.stderr)

    with open(show['data_file'], 'w') as f:
        json.dump(cache, f, indent=2)
    print(f"Saved {len(cache)} entries to {show['data_file']}")
    # ---- end GU_UPCOMING_WIRE_V2 ----


BAD_SURNAMES = {'Co', 'C', 'J', 'Relation', 'Hit', 'Indi', 'a', 'Rick',
                'Iran', 'Russia', 'China', 'Commander', 'Former', 'Centcom'}


def cleanup_json(data_file):
    """Remove entries with known bad surnames from a JSON data file."""
    if not os.path.exists(data_file):
        return
    with open(data_file) as f:
        data = json.load(f)
    before = len(data)
    data = [v for v in data if v.get('surname', '') not in BAD_SURNAMES
            and len(v.get('surname', '')) > 1]
    if len(data) < before:
        with open(data_file, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"  Cleaned {before - len(data)} bad entries from {os.path.basename(data_file)}")


async def fetch_x_followers(handles):
    """Fetch follower counts for the given X handles. Returns dict handle -> int."""
    out = {}
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            ctx = await browser.new_context()
            await ctx.add_cookies(X_COOKIES)
            for handle in handles:
                page = await ctx.new_page()
                try:
                    await page.goto(f'https://x.com/{handle}',
                                    wait_until='domcontentloaded', timeout=20000)
                    await page.wait_for_timeout(4000)
                    raw = await page.evaluate(r"""
                        () => {
                            var els = document.querySelectorAll('a[href$="/verified_followers"], a[href$="/followers"]');
                            for (var i=0; i<els.length; i++) {
                                var label = els[i].getAttribute('aria-label') || '';
                                var m = label.match(/([\d,.]+\s*[KMB]?)\s*Follower/i);
                                if (m) return m[1];
                                var txt = els[i].innerText;
                                var m2 = txt.match(/([\d,.]+[KMB]?)\s*Followers?/i);
                                if (m2) return m2[1];
                            }
                            return null;
                        }
                    """)
                    if raw:
                        out[handle] = parse_count(raw.replace(' ', ''))
                        print(f"  @{handle} followers: {raw} ({out[handle]:,})")
                    else:
                        print(f"  @{handle} followers: not found", file=sys.stderr)
                except Exception as e:
                    print(f"  @{handle} error: {e}", file=sys.stderr)
                finally:
                    await page.close()
        finally:
            await browser.close()
    return out



# GU_WEEKLY_STATS_V1_2026_07_04 -----------------------------------------------
# THIS_WEEK_IS_LAST_3_EPISODES_V1_20260814 — "This week" is a COUNT, not a date range.
#
# Operator directive: "This week" must mean the 2 most recent Going Underground episodes
# plus the 1 most recent New Order episode, sorted together by publication date, newest
# first. The previous Fri->Mon window emptied itself on a date rollover — measured live at
# 2026-08-14T00:00Z the window advanced to 14-17 Aug and reported n=0 while three perfectly
# good episodes sat in the inventory. A card that goes blank because the calendar turned
# over is telling the reader about the clock, not about the programme.
#
# Selection is by PUBLICATION DATE from the canonical inventory, never by which file a row
# happens to sit in, and it carries the measured stats already preserved by
# METRIC_NEVER_REGRESSES_TO_UNKNOWN_V1 — a failed lookup neither removes an episode nor
# replaces a measurement.
#
# SHORTAGE IS EXPLICIT AND NEVER SUBSTITUTED. If a show has fewer than its quota, the card
# shows what exists and says which show is short by how many. It never fills a GU slot with
# a New Order episode: the two programmes are not interchangeable, and quietly padding the
# count would misreport whose reach it is.
THIS_WEEK_QUOTA = (("GU", "videos.json", 2), ("NO", "videos_neworder.json", 1))


def _generate_this_week():
    import datetime as _dt3, re as _re3
    _MONS = {"jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
             "jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12}
    _today = _dt3.date.today()

    def _pub_date(v):
        """Publication date. pub_iso is canonical and unambiguous; the short date is a
        fallback and its year is inferred, never assumed forward of today."""
        iso = v.get("pub_iso")
        if iso:
            try:
                return _dt3.date.fromisoformat(str(iso)[:10]), "pub_iso"
            except Exception:
                pass
        m = _re3.match(r"(\d+)\s+([A-Za-z]+)", str(v.get("date") or ""))
        if not m:
            return None, "unparseable"
        try:
            d, mon = int(m.group(1)), _MONS.get(m.group(2)[:3].lower())
            if not mon:
                return None, "unparseable"
            c = _dt3.date(_today.year, mon, d)
            if c > _today:
                c = _dt3.date(_today.year - 1, mon, d)
            return c, "short_date"
        except Exception:
            return None, "unparseable"

    picked, shortages, undated = [], [], []
    for _code, _src, _want in THIS_WEEK_QUOTA:
        try:
            rows = json.load(open(os.path.join(ROOT, _src)))
        except Exception as e:
            shortages.append({"show": _code, "wanted": _want, "got": 0,
                              "reason": f"inventory_unreadable:{type(e).__name__}"})
            continue
        dated = []
        for v in rows:
            if v.get("is_upcoming"):
                continue          # not published yet
            d, how = _pub_date(v)
            if d is None:
                undated.append({"show": _code, "surname": v.get("surname")})
                continue
            dated.append((d, how, v))
        dated.sort(key=lambda t: t[0], reverse=True)
        take = dated[:_want]
        for d, how, v in take:
            e = dict(v)
            e["_this_week_pub_date"] = d.isoformat()
            e["_this_week_date_source"] = how
            e["_this_week_show"] = _code
            picked.append((d, e))
        if len(take) < _want:
            shortages.append({"show": _code, "wanted": _want, "got": len(take),
                              "reason": "fewer_published_episodes_than_quota"})

    picked.sort(key=lambda t: t[0], reverse=True)
    entries = [e for _d, e in picked]
    payload = {
        "_marker": "THIS_WEEK_IS_LAST_3_EPISODES_V1_20260814",
        "selection": "most_recent_published_episodes_by_quota",
        "quota": {c: w for c, _s, w in THIS_WEEK_QUOTA},
        "sort": "publication_date_desc_across_shows",
        "generated_at": _dt3.datetime.now(_dt3.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "n": len(entries),
        "n_expected": sum(w for _c, _s, w in THIS_WEEK_QUOTA),
        "complete": not shortages,
        "shortages": shortages,
        "undated_excluded": undated,
        "note": ("count-based, not a date window: a calendar rollover cannot empty this. "
                 "Shortages are reported, never padded from the other programme."),
        "entries": entries,
    }
    try:
        _tmp = os.path.join(ROOT, "stats_this_week_v1.json.tmp")
        with open(_tmp, "w") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
        os.replace(_tmp, os.path.join(ROOT, "stats_this_week_v1.json"))
        _desc = ", ".join(f"{e.get('_this_week_show')}:{e.get('surname')}" for e in entries)
        print(f"  [THIS_WEEK] {len(entries)}/{payload['n_expected']} episodes -> {_desc}"
              + (f"  SHORT: {shortages}" if shortages else ""))
    except Exception as e:
        print(f"  [THIS_WEEK] write failed: {e}", file=sys.stderr)
    return payload


# Generate stats_1week_gu.json + stats_1week_no.json with last-completed-week
# semantics. Runs at end of main_fetch(). Fail-open: if source unreadable,
# still publish payload with n=0 and reason field.
def _generate_weekly_stats():
    import datetime as _dt2, re as _re2
    _MONS = {"jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
             "jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12}
    # GU_WEEKLY_STATS_FRI_MON_V3_2026_07_17 — operator directive: each week
    # should include the most recent shows in the Fri→Mon broadcast window,
    # which matches Going Underground's actual publishing cadence better than
    # a rolling 7-day arithmetic window. Behaviour:
    #   - Today is Fri..Sun  → window = THIS Friday through the following Monday
    #   - Today is Mon       → window = LAST Friday through today (Monday)
    #   - Today is Tue..Thu  → window = LAST Fri through LAST Mon (most-recent complete Fri-Mon)
    _today = _dt2.date.today()
    _dow = _today.weekday()   # 0=Mon .. 6=Sun
    if _dow >= 4:             # Fri (4), Sat (5), Sun (6)
        _window_start = _today - _dt2.timedelta(days=_dow - 4)   # this Friday
    elif _dow == 0:           # Mon
        _window_start = _today - _dt2.timedelta(days=3)          # last Friday
    else:                     # Tue..Thu → last complete Fri..Mon
        _window_start = _today - _dt2.timedelta(days=_dow + 3)   # last Friday
    _window_end = _window_start + _dt2.timedelta(days=3)         # +3 days = Monday
    # Legacy names kept for compat with downstream refs in the same function
    _monday_last_week = _window_start
    _sunday_last_week = _window_end

    def _parse_dmy(dstr):
        m = _re2.match(r"(\d+)\s+([A-Za-z]+)", dstr or "")
        if not m: return None
        try:
            d = int(m.group(1))
            mon = _MONS.get(m.group(2)[:3].lower())
            if not mon: return None
            c = _dt2.date(_today.year, mon, d)
            if c > _today: c = _dt2.date(_today.year - 1, mon, d)
            return c
        except Exception:
            return None

    for _src, _out, _code in [("videos.json", "stats_1week_gu.json", "GU"),
                               ("videos_neworder.json", "stats_1week_no.json", "NO")]:
        _srcp = os.path.join(ROOT, _src); _outp = os.path.join(ROOT, _out)
        _entries = []
        try:
            with open(_srcp) as _f: _entries = json.load(_f)
        except Exception: _entries = []
        _filtered = []
        _rejected = []
        for _v in _entries:
            _s = _v.get("show")
            if _s != _code:
                _rejected.append({"surname": _v.get("surname"), "reason": "show_mismatch", "got": _s})
                continue
            _pd = _parse_dmy(_v.get("date"))
            if not _pd:
                _rejected.append({"surname": _v.get("surname"), "reason": "date_unparseable"})
                continue
            if not (_monday_last_week <= _pd <= _sunday_last_week):
                continue
            _filtered.append(_v)
        _payload = {
            "show": _code,
            "window": "fri_to_mon_broadcast_week",
            "window_start": _window_start.isoformat(),
            "window_end":   _window_end.isoformat(),
            # legacy field names for backward compat (now Fri/Mon in name AND semantics)
            "window_start_mon": _window_start.isoformat(),
            "window_end_sun":   _window_end.isoformat(),
            "generated_at":     _dt2.datetime.utcnow().isoformat() + "Z",
            "n": len(_filtered),
            "entries": _filtered,
            "source_feed":      _src,
            "rejected_count":   len(_rejected),
            "rejected_sample":  _rejected[:5],
            "_marker": "GU_WEEKLY_STATS_FRI_MON_V3_2026_07_17",
        }
        try:
            with open(_outp, "w") as _f: json.dump(_payload, _f, indent=2)
            print(f"[WEEKLY_STATS] {_out} n={len(_filtered)} window={_window_start} (Fri) to {_window_end} (Mon) fri_to_mon")
        except Exception as _e:
            print(f"[WEEKLY_STATS_ERR] {_out}: {_e}")



def _health_metric(value, status=None):
    """Raw metric -> what the dashboard should render.

    A measured value passes through unchanged. Anything UNKNOWN becomes the structured
    health object ({status:'N/A', reason}) that docs/index.html already renders as a grey
    N/A with the reason as a tooltip, and that `totalParts()` already excludes from the
    episode total while marking it partial.

    The point is that UNKNOWN must be self-describing by the time it leaves this file. A
    bare null travels fine through JSON and then every downstream consumer — Android,
    Tidbyt, LaMetric, a spreadsheet — is free to coerce it to 0. A dict cannot be summed
    by accident.
    """
    if value is None or str(value).strip() in ("", "?", "None", "null"):
        return {"status": "N/A", "reason": status or "UNMEASURED"}
    return value


def _emit_videos_health_v1():
    """VIDEOS_HEALTH_V1_EMIT_V1_2026_07_20 — first-class artefact for dashboard
    + downstream LaMetric consumers. Schema is BOTH the dashboard-consumed shape
    ({iso, episodes:[...]} with metrics{}) AND additive {gu:[top5], no:[top5],
    last_updated} preferred by the operator's prompt. Both live in the same JSON
    (additive keys). Never raises."""
    import datetime as _dt
    try:
        _iso = _dt.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        _out = {'iso': _iso, 'last_updated': _iso, 'episodes': [], 'gu': [], 'no': []}
        for _fname, _show_code, _bucket in [('videos.json', 'GU', 'gu'),
                                             ('videos_neworder.json', 'NO', 'no')]:
            _fp = os.path.join(ROOT, _fname)
            if not os.path.exists(_fp):
                continue
            try:
                with open(_fp) as _f:
                    _rows = json.load(_f) or []
            except Exception:
                _rows = []
            _clean = []
            for _r in _rows:
                if not isinstance(_r, dict):
                    continue
                _guest = (_r.get('canonical_guest_full_name') or _r.get('guest') or '').strip()
                _clean.append({
                    'guest': _guest,
                    'surname': (_r.get('canonical_surname_upper') or _r.get('surname') or '').strip(),
                    'title': (_r.get('title') or '').strip(),
                    'date': (_r.get('date') or '').strip(),
                    'show': _show_code,
                    'link': (_r.get('link') or _r.get('canonical_video_url') or '').strip(),
                    'canonical_episode_id': _r.get('canonical_episode_id'),
                    # CANONICAL_URL_BIND_V1_2026_07_20 — additive URL_BIND fields
                    'canonical_video_id':      _r.get('canonical_video_id'),
                    'canonical_video_url':     _r.get('canonical_video_url'),
                    'canonical_episode_id_v2': _r.get('canonical_episode_id_v2'),
                    'pub_iso':                 _r.get('pub_iso'),
                    'source_platform_ids':     _r.get('source_platform_ids') or {},
                    'metrics': {
                        'rumble_views': _r.get('rumble_views'),
                        'yt_views':     _r.get('yt_views'),
                        # X_UNMEASURED_IS_NOT_ZERO_V1_20260813 — an unmeasured platform is
                        # emitted as the structured health object the dashboard already
                        # understands ({status:'N/A', reason}), so the reason survives all
                        # the way to the tooltip instead of being flattened to a bare null
                        # that every consumer is free to reinterpret as zero.
                        'x_views':      _health_metric(_r.get('x_views'),
                                                       _r.get('_x_status')),
                        'ig_likes':     _r.get('ig_likes'),
                    },
                })
            _out['episodes'].extend(_clean)
            _out[_bucket] = _clean[:5]
        with open(os.path.join(ROOT, 'videos_health_v1.json'), 'w') as _f:
            json.dump(_out, _f, indent=2, ensure_ascii=False)
        print(f"[VIDEOS_HEALTH_V1] emitted iso={_iso} gu_top5={len(_out['gu'])} no_top5={len(_out['no'])} total_episodes={len(_out['episodes'])}")
    except Exception as _e:
        print(f"[VIDEOS_HEALTH_V1_ERR] {_e}")


async def main_fetch():
    # METRIC_ATTRIB_V1_2026_07_20 — build the union of known surnames across
    # both shows before scraping IG, so attribution is anchored to real guests.
    _known_union = set()
    for _show in SHOWS:
        _dpath = _show['data_file']
        if os.path.exists(_dpath):
            try:
                with open(_dpath) as _f:
                    for _v in json.load(_f) or []:
                        _sn = (_v.get('canonical_surname_upper') or _v.get('surname') or '').lower()
                        if _sn:
                            _known_union.add(_sn)
            except Exception:
                pass
    ig_clips = fetch_instagram_clips(known_surnames=_known_union)
    print(f"IG clips found for {len(ig_clips)} surnames (from {len(_known_union)} known)")
    for show in SHOWS:
        await update_show(show, ig_clips)
        cleanup_json(show['data_file'])

    # Fetch X follower counts for all three accounts
    print("\nFetching X follower counts...")
    followers = await fetch_x_followers(['afshinrattansi', 'GUnderground_TV', 'NewOrder_TV'])
    total = sum(followers.values())
    out = {
        "accounts": {h: followers.get(h) for h in ['afshinrattansi', 'GUnderground_TV', 'NewOrder_TV']},
        "total": total,
        "updated": __import__('datetime').datetime.utcnow().isoformat() + 'Z',
    }
    with open(os.path.join(ROOT, 'followers.json'), 'w') as f:
        json.dump(out, f, indent=2)
    print(f"Total X followers: {total:,}")

    # GU_WEEKLY_STATS_V1_2026_07_04 — publish per-show last-completed-week stats
    try:
        _generate_weekly_stats()
        _generate_this_week()
    except Exception as _e_ws:
        print(f"[WEEKLY_STATS_ERR] {_e_ws}")


    # VIDEOS_HEALTH_V1_EMIT_V1_2026_07_20 — first-class artefact for dashboard + LaMetric
    try:
        _emit_videos_health_v1()
    except Exception as _e_vh:
        print(f"[VIDEOS_HEALTH_V1_CALL_ERR] {_e_vh}")


def push_to_tidbyt():
    """Build animation from Going Underground data and push to both Tidbyts."""
    with open(SHOWS[0]['data_file']) as f:
        cache = json.load(f)

    sorted_eps = []
    for v in cache[:15]:
        # GU_UNKNOWN_IS_NULL_V2 — unknown platforms are excluded, not counted as 0.
        total, _unknown_fields = sum_known_metrics(v)
        if _unknown_fields:
            print(f"  [GU_PARTIAL_TOTAL] {v.get('surname','?')} {v.get('date','')}: "
                  f"total excludes unmeasured {','.join(_unknown_fields)}")
        # TIDBYT_CANONICAL_PREF_V1_2026_07_11 — prefer canonical_surname_upper so
        # broken extractor output (e.g. "War" for Carden ep, "Minister" for
        # Ellwood ep) never reaches the Tidbyt pixmap.
        name = v.get('canonical_surname_upper') or v.get('surname', '?')
        date = v.get('date', '')
        label = f"{name} {date}" if date else name
        if total >= 1_000_000: t = f"{total/1_000_000:.1f}M"
        elif total >= 1_000: t = f"{total/1_000:.0f}K"
        else: t = str(total)
        sorted_eps.append((label, t))

    WIDTH, HEIGHT = 64, 32
    try:
        font_name = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf", 9)
        font_num = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf", 14)
    except Exception:
        font_name = ImageFont.load_default()
        font_num = ImageFont.load_default()

    def draw_crisp(img, x, y, text, color, font):
        mask = Image.new("L", img.size, 0)
        ImageDraw.Draw(mask).text((x, y), text, fill=255, font=font)
        mask = mask.point(lambda p: 255 if p > 100 else 0)
        overlay = Image.new("RGB", img.size, color)
        img.paste(overlay, mask=mask)

    # First frame: combined X follower count header
    followers_total_str = ""
    try:
        with open(os.path.join(ROOT, 'followers.json')) as f:
            ft = json.load(f).get('total', 0)
        if ft >= 1_000_000: followers_total_str = f"{ft/1_000_000:.2f}M"
        elif ft >= 1_000: followers_total_str = f"{ft/1_000:.1f}K"
        else: followers_total_str = str(ft)
    except Exception:
        pass

    # Load active drops (if any) — alert frames go first, in red
    drops = []
    try:
        with open(os.path.join(ROOT, 'drops_current.json')) as f:
            drops = json.load(f).get('drops', [])
    except Exception:
        pass

    frames = []
    for d in drops[:5]:  # cap at 5 alert frames so animation isn't too long
        alert = Image.new("RGB", (WIDTH, HEIGHT), (60, 0, 0))
        line1 = f"{d['guest'][:9]} {d['platform']}"
        line2 = f"-{d['drop_pct']:.0f}%"
        lw = font_name.getbbox(line1)[2]
        draw_crisp(alert, max(0, (WIDTH - lw) // 2), 0, line1, (255, 60, 60), font_name)
        nw = font_num.getbbox(line2)[2]
        draw_crisp(alert, (WIDTH - nw) // 2, 13, line2, (255, 80, 80), font_num)
        frames.append(alert)

    if followers_total_str:
        hdr = Image.new("RGB", (WIDTH, HEIGHT), (10, 0, 0))
        lbl = "X FOLLOWERS"
        lw = font_name.getbbox(lbl)[2]
        draw_crisp(hdr, max(0, (WIDTH - lw) // 2), 0, lbl, (255, 255, 255), font_name)
        nw = font_num.getbbox(followers_total_str)[2]
        draw_crisp(hdr, (WIDTH - nw) // 2, 13, followers_total_str, (0, 255, 0), font_num)
        frames.append(hdr)

    for name, total in sorted_eps[:15]:
        img = Image.new("RGB", (WIDTH, HEIGHT), (10, 0, 0))
        d = name[:12]
        nw = font_name.getbbox(d)[2]
        draw_crisp(img, max(0, (WIDTH - nw) // 2), 0, d, (255, 255, 255), font_name)
        nw2 = font_num.getbbox(total)[2]
        draw_crisp(img, (WIDTH - nw2) // 2, 13, total, (0, 255, 0), font_num)
        frames.append(img)

    palette_img = Image.new("P", (1, 1))
    palette_img.putpalette([10,0,0, 255,255,255, 0,255,0, 0,0,0] + [0]*(256-4)*3)
    pframes = [f.quantize(palette=palette_img, dither=Image.Dither.NONE) for f in frames]
    buf = io.BytesIO()
    pframes[0].save(buf, format="GIF", save_all=True, append_images=pframes[1:],
                    duration=1000, loop=0)
    image_data = base64.b64encode(buf.getvalue()).decode()

    for dev in TIDBYT_DEVICES:
        if not dev['key']:
            continue
        try:
            r = requests.post(
                f"https://api.tidbyt.com/v0/devices/{dev['id']}/push",
                headers={"Authorization": f"Bearer {dev['key']}",
                         "Content-Type": "application/json"},
                json={"image": image_data, "installationID": "GUstats", "background": False},
                timeout=10)
            print(f"Tidbyt {dev['id'][:10]}: {r.status_code}")
        except Exception as e:
            print(f"Tidbyt {dev['id'][:10]}: {e}", file=sys.stderr)


def main():
    asyncio.run(main_fetch())
    push_to_tidbyt()


if __name__ == "__main__":
    main()
