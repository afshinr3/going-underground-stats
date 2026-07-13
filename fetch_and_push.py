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
}
_CANON_BAD_PREFIXES = ("Ex-", "Former ", "Fmr ", "SLAMS ", "BLASTS ",
                       "REVEALS ", "EXPOSES ", "WARNS ", "'", "\u2018", "\u2019")


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


def format_views(v):
    n = parse_count(v) if isinstance(v, str) else int(v)
    if n >= 1_000_000: return f"{n/1_000_000:.1f}M"
    if n >= 1_000: return f"{n/1_000:.1f}K"
    return str(n)


def fetch_youtube_data(channel_id):
    """Fetch view counts AND publish dates per surname from YouTube RSS."""
    try:
        req = urllib.request.Request(
            f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}",
            headers={"User-Agent": "Mozilla/5.0"})
        rss = urllib.request.urlopen(req, timeout=15).read().decode()
        entries = re.findall(
            r'<entry>.*?<title>(.*?)</title>.*?<published>(.*?)</published>.*?<media:statistics views="(\d+)"',
            rss, re.DOTALL)
        views_map = {}     # surname -> view count string
        date_map = {}      # surname -> ISO date string (YYYY-MM-DD)
        for title, pub, views in entries:
            title = title.replace('&amp;', '&').replace('&#39;', "'")
            iso_date = pub[:10]
            for w in re.findall(r'\b[A-Z][a-z]+(?:-[A-Z][a-z]+)?\b', title):
                if len(w) > 3 and w.lower() not in ('iran', 'israel', 'going', 'underground', 'order'):
                    views_map.setdefault(w.lower(), format_views(views))
                    date_map.setdefault(w.lower(), iso_date)
            m = re.search(r'\(([^)]+)\)', title)
            if m:
                for w in m.group(1).split():
                    w = w.strip('.,')
                    if len(w) > 3:
                        views_map.setdefault(w.lower(), format_views(views))
                        date_map.setdefault(w.lower(), iso_date)
        return views_map, date_map
    except Exception as e:
        print(f"YouTube error for {channel_id}: {e}", file=sys.stderr)
        return {}, {}


def fetch_instagram_clips():
    """Fetch IG play counts from afshinrattansi profile (shared by both shows)."""
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
                for word in re.findall(r'\b[A-Z][a-z]{3,}\b', caption):
                    clips[word.lower()] = clips.get(word.lower(), 0) + play_count
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
    title = title.strip()
    title = title.replace('\u2018', "'").replace('\u2019', "'").replace('\u201c', '"').replace('\u201d', '"')
    title = re.sub(r'^[\W_]+', '', title).strip()

    # ===== v5 PATCH A: Ex-{Nationality} {Role} {Name} ... =====
    ex_role_match = re.match(
        r'^(?:Ex|Former|Fmr)[\s\-]+'
        r'(?:(?:Israeli|US|UK|British|American|EU|French|German|Russian|Chinese|Iranian|'
        r'Saudi|Indian|Pakistani|Turkish|Egyptian|Iraqi|Syrian|Palestinian|Lebanese|'
        r'Jordanian|Greek|Italian|Spanish|Dutch|Brazilian|Mexican|Canadian|Australian|'
        r'Japanese|Korean|Thai|Filipino|Indonesian|Vietnamese|African|European)\s+)?'
        r'(?:President|PM|Prime\s+Minister|Minister|Officer|Ambassador|Amb|Director|Head|'
        r'Chief|Senator|Congressman|Congresswoman|MP|General|Admiral|Secretary|Advisor|'
        r'Adviser|Analyst|Spokesperson|Editor|Professor|Commander|Colonel|Captain|Major|'
        r'Sgt\.?|Lt\.?\s*Col\.?|Dr\.?|Prof\.?|VP|Vice\s+President|Deputy|CEO|CFO|'
        r'Mayor|Governor|Judge|Justice)\s+'
        r'([A-Z][a-z]+(?:\s+[A-Z][a-zA-Z\-]+){1,2}?)'
        r'(?=\s+(?:[A-Z]{2,}|on|in|of|at|for|with|to|from|by|and|or|but|'
        r'Explains|Says|Argues|Discusses|Talks|Reveals|Warns|Why|How|What|When|Where|'
        r'Who|That|Which|will|could|would|should|is|are|was|were|has|have|had|tells|'
        r'told|shares|gives)\b|[\'\":,.\-——–]|\s*$)',
        title
    )
    if ex_role_match:
        return ex_role_match.group(1).strip()

    # ===== Existing v4 honorific pattern + PATCH B: ' added to lookahead =====
    honorific_match = re.match(
        r'^(?:Prof|Dr|Mr|Mrs|Ms|Sir|Lady|Sen|Rep|Ambassador|Amb|Col|Gen|Lt|Capt|Maj|'
        r'Hon|Rabbi|Imam|Rev|Sgt|Baroness|Lord)\.?\s+'
        r'([A-Z][a-z]+(?:\s+[A-Z][a-zA-Z\-]+){1,2}?)'
        r'(?=\s+(?:on|in|of|at|for|with|to|from|by|and|or|but|Explains|Says|Argues|'
        r'Discusses|Talks|Reveals|Warns|Why|How|What|When|Where|Who|That|Which|will|'
        r'could|would|should|is|are|was|were|has|have|had|tells|told|shares|gives)\b'
        r"|[\'\":,.\-——–]|\s*$)",      # ← added ' for possessive (Mearsheimer's …)
        title
    )
    if honorific_match:
        return honorific_match.group(1).strip()

    # ===== Parenthesised guest (unchanged) =====
    paren = re.search(r'\(([^)]+)\)\s*$', title)
    if paren:
        guest = paren.group(1).strip()
        guest = _strip_role(guest)
        if guest and len(guest) > 3:
            return guest
        return paren.group(1).strip()

    # ===== "Name on Topic" (unchanged) =====
    name_on = re.match(r'^(?:\S+\'s\s+)?([A-Z][a-z]+(?:\s+[A-Z][a-z-]+)+)\s+on\s+', title)
    if name_on:
        return name_on.group(1)

    # ===== Dash separator + PATCH C: Amb. now stripped =====
    dash_match = re.split(r'\s*[–—]\s*|\s+-\s+|-\s+(?=[A-Z](?:[a-z]|x-|ormer))', title)
    if len(dash_match) >= 2:
        guest = dash_match[-1].strip()
        guest = _strip_role(guest)
        if guest and len(guest) > 3:
            return guest
        return dash_match[-1].strip()

    # ===== Colon-prefixed name (unchanged shape, plus PATCH D below) =====
    colon_match = re.match(r'^([^:]{2,40}):\s+(.*)', title)
    if colon_match:
        cand = colon_match.group(1).strip()
        rest = colon_match.group(2).strip()
        cand = _strip_role(cand)
        is_all_caps = cand == cand.upper() and len(cand) > 2
        looks_like_name = bool(re.match(r"^[A-Z][a-zA-Z\.'\-]+(?:\s+[A-Z][a-zA-Z\.'\-]+){0,3}$", cand))
        if looks_like_name and not is_all_caps and 3 < len(cand) <= 40:
            return cand
        name_on_after = re.match(
            r'^(?:(?:Prof|Dr|Lt\.?\s*Col|Sgt|Mr|Mrs|Ms|Sir|Amb)\.?\s+)?'
            r'([A-Z][a-z]+(?:\s+[A-Z][a-zA-Z\-]+){1,3})\s+on\s+',
            rest
        )
        if name_on_after:
            return name_on_after.group(1).strip()
        name_verb_after = re.match(
            r'^(?:(?:Prof|Dr|Mr|Mrs|Ms|Sir|Lady|Sen|Rep|Ambassador|Amb|Col|Gen|Lt|Capt|'
            r'Maj|Hon|Rabbi|Imam|Rev|Sgt)\.?\s+)?'
            r'([A-Z][a-z]+(?:\s+[A-Z][a-zA-Z\-]+){1,2}?)'
            r'(?=\s+(?:explains|says|argues|discusses|talks|reveals|warns|tells|told|'
            r'shares|gives|will|could|would|should|is|are|was|were|has|have|had)\b)',
            rest, flags=re.IGNORECASE
        )
        if name_verb_after:
            return name_verb_after.group(1).strip()
        # ===== PATCH D: "{generic descriptor(s)} {Name} {ALL-CAPS-VERB}..." after colon =====
        # Handles "War on Iran: Pentagon Whistleblower Wes Bryant SLAMS..." where the
        # name is preceded by 1–3 descriptor words rather than an honorific.
        generic_pre_name = re.match(
            r'^(?:[A-Z][a-zA-Z]+\s+){1,3}'                       # 1–3 descriptor words (Pentagon Whistleblower …)
            r'([A-Z][a-z]+(?:\s+[A-Z][a-zA-Z\-]+){1,2}?)'        # captured name
            r'(?=\s+(?:[A-Z]{2,}|explains|says|argues|reveals|warns|tells|on)\b)',
            rest
        )
        if generic_pre_name:
            return generic_pre_name.group(1).strip()
    # EXTRACT_GUEST_NAME_VERB_V1_2026_07_04 — GU title pattern "<Guest Full Name> <Verb> ..."
    # Handles: "Max Blumenthal Reveals Why ...", "Steve Keen Warns ...", etc.
    name_verb_leading = re.match(
        r"^([A-Z][a-z]+(?:\s+[A-Z][a-zA-Z\-']+){1,3}?)\s+"
        r"(?:Reveals|Explains|Says|Argues|Discusses|Talks|Warns|Slams|Analyses|Analyzes|"
        r"Tells|Shares|Challenges|Confirms|Predicts|Claims|Believes|Uncovers|Exposes|"
        r"Details|Describes|Debates|Comments|Reports|Breaks)\b",
        title
    )
    if name_verb_leading:
        _cand = name_verb_leading.group(1).strip()
        # Skip host name (Afshin Rattansi)
        if _cand.lower() not in ("afshin rattansi", "afshin"):
            return _cand
    return None


def _strip_role(name):
    name = re.sub(
        r'^(?:(?:Ex|Former|Fmr|Acting|Deputy|Senior|Chief|Head)[\s.-]*)*'
        r'(?:Israeli\s+|US\s+|UK\s+|British\s+|American\s+)?'
        r'(?:Intel\s+|Intelligence\s+)?(?:Acting\s+)?'
        r'(?:President|PM|Prime\s+Minister|Minister|Officer|Ambassador|Amb|MP|'
        r'Director|Head|Chief|Senator|Congressman|General|Admiral|Secretary|'
        r'Advisor|Analyst|Spokesperson|Editor|Professor|Commander|Colonel|Captain|'
        r'Major|Sgt\.?|Lt\.?\s*Col\.?|Dr\.?|Prof\.?)\.?\s+',
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
    try:
        p = os.path.join(ROOT, "upcoming.json")
        if not os.path.exists(p): return []
        with open(p) as _f: raw = json.load(_f)
        if not isinstance(raw, list): return []
        out = []
        for it in raw:
            if not isinstance(it, dict): continue
            if str(it.get("show","")).upper() != show.upper(): continue
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
        entries = re.findall(
            r'<entry>.*?<title>(.*?)</title>.*?<published>(.*?)</published>',
            rss, re.DOTALL)
        new_eps = []
        for title_raw, pub in entries:
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
            new_eps.append({
                "guest": emit_guest,
                "surname": surname_display,
                "title": title,
                "rumble_views": "?", "x_views": "?", "date": short_date,
                "yt_views": "?", "ig_likes": "?",
                "canonical_guest_full_name": cfn or emit_guest,
                "canonical_surname_upper": emit_surname,
                "canonical_episode_id": ceid,
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


async def fetch_x_views_with_ctx(ctx, handles, full_name, since_date=None):
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
    results_per_handle = await asyncio.gather(*[one_handle(h) for h in handles])
    for results in results_per_handle:
        for tweet_id, views in results:
            if views > seen_ids.get(tweet_id, 0):
                seen_ids[tweet_id] = views
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
    _normalised = 0
    for v in cache:
        orig_surname = v.get('surname', '') or ''
        # GU_SURNAME_HARDENING_V1_2026_07_03 - broader suffix strip.
        clean_surname = _strip_r_date_suffix(orig_surname).strip()
        if clean_surname != orig_surname:
            v['surname'] = clean_surname
            _normalised += 1
        # GU_SURNAME_HARDENING_V1_2026_07_03 - flag junk-token surnames for re-derive.
        _needs_rederive = not _looks_valid_surname(v.get('surname', ''))
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
            if new_guest and new_surname:
                print(f"  [normalize] guest '{guest[:40]}…' -> '{new_guest}' (surname {new_surname})")
                v['guest'] = new_guest
                v['surname'] = new_surname
                _normalised += 1
            elif _needs_rederive:
                # GU_SURNAME_HARDENING_V1_2026_07_03 - safe fallback: blank the surname
                # (Android falls back to guest/title) rather than shipping "Tru" / "DEF".
                print(f"  [normalize] blanking junk surname; title={title[:50]}")
                v['surname'] = ''
                _normalised += 1
    if _normalised:
        print(f"  [normalize] cleaned {_normalised} legacy/truncated entries")

    # Dedupe on (guest, date) - merge view counts, keep highest per field
    def _to_num(x):
        x = str(x or '').replace(',', '').strip().upper()
        if not x or x == '?': return -1
        if x.endswith('K'): return float(x[:-1]) * 1000
        if x.endswith('M'): return float(x[:-1]) * 1_000_000
        try: return float(x)
        except Exception: return -1
    _seen = {}
    _deduped_cache = []
    for v in cache:
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
        print(f"  [normalize] deduped {len(cache) - len(_deduped_cache)} (guest, date) duplicates")
        cache = _deduped_cache

    yt, yt_dates = fetch_youtube_data(show['yt_channel_id'])

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
            _X_CACHE_PATH = "/Users/afshin/RumbleMonitor/x_2026.json"
            _X_CACHE_DATA = {"loaded": False, "results": {}}
            def _x_from_cache(surname, show_code):
                """Sum view_count of tweets in the given show whose text mentions
                the guest's surname (case-insensitive). Returns (total, count)."""
                if not surname: return 0, 0
                try:
                    if not _X_CACHE_DATA["loaded"]:
                        with open(_X_CACHE_PATH) as _f: _xc = json.load(_f)
                        _X_CACHE_DATA["results"] = _xc.get("results") or {}
                        _X_CACHE_DATA["loaded"] = True
                except Exception:
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
                        total, count = await fetch_x_views_with_ctx(
                            ctx, handles, full_name, since_date=since)
                        # Fallback 1: search by surname only
                        if total == 0 and surname and len(surname) > 3:
                            total, count = await fetch_x_views_with_ctx(
                                ctx, handles, surname, since_date=since)
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
                            print(f"  {surname}: {count} tweets, X:{v['x_views']}")
                        else:
                            # Never render '?'; use deterministic 0 fallback
                            if v.get('x_views') == '?': v['x_views'] = '0'
                    except Exception as e:
                        print(f"  {surname}: X error {e}", file=sys.stderr)
                        if v.get('x_views') == '?': v['x_views'] = '0'  # never leave '?'


            await asyncio.gather(*[process_episode(v) for v in eligible])
        finally:
            await browser.close()

    for v in cache:
        surname = v.get('surname', '').lower()
        if not surname:
            continue
        if surname in yt:
            v['yt_views'] = yt[surname]
        if surname in ig_clips:
            v['ig_likes'] = ig_clips[surname]

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
    _qm_normalized = 0
    for _v in cache:
        for _f in ('rumble_views', 'yt_views', 'ig_likes', 'x_views'):
            if _v.get(_f) == '?':
                _v[_f] = '0'
                _qm_normalized += 1
    if _qm_normalized:
        print(f"  [GU_NO_QUESTION_MARK_V1] normalized {_qm_normalized} '?' -> '0' fields")
    print(f"  [CANONICAL_PUBLISH_V1_v2 + CANONICAL_FIELD_EMISSION_V1_2026_07_11] resolved={_canon_resolved} unchanged={_canon_unchanged}")
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
# Generate stats_1week_gu.json + stats_1week_no.json with last-completed-week
# semantics. Runs at end of main_fetch(). Fail-open: if source unreadable,
# still publish payload with n=0 and reason field.
def _generate_weekly_stats():
    import datetime as _dt2, re as _re2
    _MONS = {"jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
             "jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12}
    # GU_WEEKLY_STATS_ROLLING7D_V2_2026_07_04 — rolling 7d per show
    _today = _dt2.date.today()
    _window_start = _today - _dt2.timedelta(days=7)
    _window_end   = _today
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
            "window": "rolling_last_7d",
            "window_start": _window_start.isoformat(),
            "window_end":   _window_end.isoformat(),
            # legacy field names for backward compat
            "window_start_mon": _window_start.isoformat(),
            "window_end_sun":   _window_end.isoformat(),
            "generated_at":     _dt2.datetime.utcnow().isoformat() + "Z",
            "n": len(_filtered),
            "entries": _filtered,
            "source_feed":      _src,
            "rejected_count":   len(_rejected),
            "rejected_sample":  _rejected[:5],
            "_marker": "GU_WEEKLY_STATS_ROLLING7D_V2_2026_07_04",
        }
        try:
            with open(_outp, "w") as _f: json.dump(_payload, _f, indent=2)
            print(f"[WEEKLY_STATS] {_out} n={len(_filtered)} window={_window_start} to {_window_end} (rolling 7d)")
        except Exception as _e:
            print(f"[WEEKLY_STATS_ERR] {_out}: {_e}")

async def main_fetch():
    ig_clips = fetch_instagram_clips()
    print(f"IG clips found for {len(ig_clips)} surnames")
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
    except Exception as _e_ws:
        print(f"[WEEKLY_STATS_ERR] {_e_ws}")


def push_to_tidbyt():
    """Build animation from Going Underground data and push to both Tidbyts."""
    with open(SHOWS[0]['data_file']) as f:
        cache = json.load(f)

    sorted_eps = []
    for v in cache[:15]:
        total = sum(parse_count(v.get(k)) for k in ['rumble_views','x_views','yt_views','ig_likes'])
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
