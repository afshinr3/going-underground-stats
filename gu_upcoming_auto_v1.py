#!/usr/bin/env python3
"""GU_UPCOMING_AUTO_V1_2026_07_04 — auto-discover promoted UPCOMING GU episode
from @afshinrattansi + @GUnderground_TV latest tweets via existing X GraphQL
auth (~/RumbleMonitor/x_cookies.json).

Also emits X_SCRAPE_HEALTH_STATUS to /tmp/x_scrape_health.json:
- writes generated_at + newest_tweet_iso
- STALE_CACHE_WARN if newest tweet age > 48h despite fresh generated_at
- LOG_LEVEL_FAIL if latest fetch returned zero new tweets

Pattern for upcoming episode:
  - Text mentions "episode of Going Underground" OR "special episode"
  - AND contains "will be joined by" OR "@<handle>" user mention
  - AND references future day: "Saturday", "Sunday", "Tomorrow", explicit date
Extracts:
  guest    = first user_mention.name that's not @afshinrattansi/@GUnderground_TV/@NewOrder_TV
  title    = first meaningful line of tweet text, stripped of URLs, up to ~140 chars
  date     = resolved to "<D> <Mon>" e.g. "4 Jul"

Writes upcoming.json ONLY if discovery succeeded. Never invents.
"""
import os, sys, json, re, datetime as dt

sys.path.insert(0, "/Users/afshin/RumbleMonitor")
from x_graphql_scraper import (
    _load_cookies, _get_user_id, _make_request, _get_auth_headers,
    _extract_tweets_from_response, DEFAULT_HASHES,
)
from scrape_2026_x import fetch_page as _sc_fetch_page

UPCOMING_PATH = "/Users/afshin/going-underground-stats/upcoming.json"
X_2026_PATH = "/Users/afshin/RumbleMonitor/x_2026.json"
HEALTH_PATH = "/tmp/x_scrape_health.json"

HANDLES_TO_SCAN = ["afshinrattansi", "GUnderground_TV"]
# Exclude self-mentions when picking guest
EXCLUDE_MENTIONS = {"afshinrattansi", "GUnderground_TV", "neworder_TV", "NewOrder_TV",
                    "Afshin_Rattansi"}

FEATURES_JSON = json.dumps({
    "rweb_tipjar_consumption_enabled": True,
    "responsive_web_graphql_exclude_directive_enabled": True,
    "verified_phone_label_enabled": False,
    "creator_subscriptions_tweet_preview_api_enabled": True,
    "responsive_web_graphql_timeline_navigation_enabled": True,
    "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
    "communities_web_enable_tweet_community_results_fetch": True,
    "c9s_tweet_anatomy_moderator_badge_enabled": True,
    "articles_preview_enabled": True,
    "responsive_web_edit_tweet_api_enabled": True,
    "graphql_is_translatable_rweb_tweet_is_translatable_enabled": True,
    "view_counts_everywhere_api_enabled": True,
    "longform_notetweets_consumption_enabled": True,
    "responsive_web_twitter_article_tweet_consumption_enabled": True,
    "freedom_of_speech_not_reach_fetch_enabled": True,
    "standardized_nudges_misinfo": True,
    "tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled": True,
    "longform_notetweets_rich_text_read_enabled": True,
    "longform_notetweets_inline_media_enabled": True,
    "profile_label_improvements_pcf_label_in_post_enabled": True,
    "rweb_video_screen_enabled": False,
    "premium_content_api_read_enabled": False,
    "responsive_web_grok_analyze_button_fetch_trends_enabled": False,
    "responsive_web_grok_analyze_post_followups_enabled": True,
    "responsive_web_jetfuel_frame": False,
    "responsive_web_grok_share_attachment_enabled": True,
    "responsive_web_grok_analysis_button_from_backend": True,
    "creator_subscriptions_quote_tweet_preview_enabled": False,
    "responsive_web_grok_image_annotation_enabled": True,
    "responsive_web_enhance_cards_enabled": False,
}, separators=(",", ":"))
FT_JSON = json.dumps({"withArticlePlainText": False}, separators=(",", ":"))


def _fetch_latest_tweets(user_id, cookies, page_size=40):
    # Reuse proven scrape_2026_x.fetch_page (handles auth, URL-encoding, hash rotation)
    tweets, _cursor = _sc_fetch_page(user_id, cookies, cursor=None)
    return tweets or []


UPCOMING_PATTERNS = [
    r"episode of Going Underground",
    r"special episode of Going Underground",
    r"special episode",
    r"NEW EPISODE",
]

DAY_KEYWORDS = {
    "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
    "friday": 4, "saturday": 5, "sunday": 6,
    "today": None, "tomorrow": None,
}


def _resolve_date_from_text(text, tweet_dt):
    """Resolve future-day reference in text to actual date. Returns date or None."""
    tl = text.lower()
    # 1. Explicit "MMM DD" or "DD MMM" — try to parse
    for pat in [r"(\d{1,2})\s*(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)",
                r"(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\s*(\d{1,2})"]:
        m = re.search(pat, tl)
        if m:
            g1, g2 = m.group(1), m.group(2)
            if g1.isdigit():
                day = int(g1); mon_name = g2
            else:
                day = int(g2); mon_name = g1
            mon = {"jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
                   "jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12}[mon_name]
            return dt.date(tweet_dt.year, mon, day)
    # 2. "tomorrow"
    if "tomorrow" in tl:
        return (tweet_dt + dt.timedelta(days=1)).date()
    # 3. "today"
    if "today" in tl and "tomorrow" not in tl:
        return tweet_dt.date()
    # 4. day of week (find next occurrence)
    for kw, wd in DAY_KEYWORDS.items():
        if wd is None: continue
        if kw in tl:
            base = tweet_dt.date()
            offset = (wd - base.weekday()) % 7
            if offset == 0: offset = 7  # if today is that day, mean next week
            # But if tweet says "today Saturday" and tweet was made Saturday, keep 0
            if base.weekday() == wd and "today" in tl: offset = 0
            return base + dt.timedelta(days=offset)
    return None


def _extract_upcoming(tweets):
    """Scan tweets for UPCOMING GU episode. Return dict or None."""
    candidates = []
    for t in tweets:
        text = t.get("text") or t.get("full_text") or ""
        if not text: continue
        tl = text.lower()
        # Must reference Going Underground + upcoming pattern
        if "going underground" not in tl: continue
        matched = any(re.search(p, text, re.I) for p in UPCOMING_PATTERNS)
        if not matched: continue
        # Extract @handles from text (fresh scraper returns 4-field struct, no entities)
        at_handles = re.findall(r"@([A-Za-z0-9_]+)", text)
        guest_handle = None
        for h in at_handles:
            if h in EXCLUDE_MENTIONS: continue
            # Skip obvious non-guest handles
            if h.lower() in ("rt", "afshinrattansi"): continue
            guest_handle = h
            break
        # Convert handle to human name: "Dennis_Kucinich" -> "Dennis Kucinich"
        guest_name = None
        if guest_handle:
            guest_name = guest_handle.replace("_", " ").strip()
        else:
            # Text-based fallback for tweets without @mention
            m = re.search(r"joined by\s+(?:veteran\s+|former\s+|current\s+)?(?:Congressman\s+|Senator\s+|Prof\.?\s+|Dr\.?\s+|Amb\.?\s+|Ambassador\s+|Rep\.?\s+|President\s+)?([A-Z][a-zA-Z\-']+(?:\s+[A-Z][a-zA-Z\-']+){1,3})",
                          text)
            if m: guest_name = m.group(1)
        if not guest_name: continue
        # Parse tweet timestamp
        ct = t.get("created_at") or ""
        try:
            tweet_dt = dt.datetime.strptime(ct, "%a %b %d %H:%M:%S %z %Y")
        except Exception:
            tweet_dt = dt.datetime.now(dt.timezone.utc)
        # Resolve date reference
        ep_date = _resolve_date_from_text(text, tweet_dt)
        if not ep_date: continue
        if ep_date < tweet_dt.date(): continue
        # Detect RT — deprioritize
        is_rt = text.startswith("RT ")
        candidates.append({
            "tweet_id": t.get("id"),
            "tweet_created_at": ct,
            "guest": guest_name,
            "guest_handle": guest_handle,
            "ep_date": ep_date,
            "text": text,
            "is_rt": is_rt,
        })
    if not candidates: return None
    # Prefer: non-RT first, then latest tweet_created_at, then latest ep_date
    candidates.sort(key=lambda c: (not c["is_rt"], c["tweet_created_at"], c["ep_date"].isoformat()), reverse=True)
    return candidates[0]


def _format_upcoming_entry(c):
    """Format as GU upcoming.json record."""
    ep_date = c["ep_date"]
    mon_names = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    date_str = f"{ep_date.day} {mon_names[ep_date.month-1]}"
    # GU_AUTO_SURNAME_CLEAN_V1_2026_07_04 — no _R<date> suffix (triggers PUBLISH_GUARD reject)
    surname_first = c["guest"].split()[-1]
    surname_field = surname_first
    # Title: extract first meaningful sentence of tweet, strip URLs
    text = c["text"]
    text = re.sub(r"https?://\S+", "", text).strip()
    # Take first line
    title = text.split("\n\n")[0].strip()[:180]
    return {
        "show": "GU",
        "date": date_str,
        "guest": c["guest"],
        "surname": surname_field,
        "title": title,
        "is_upcoming": True,
        "_source_tweet_id": str(c["tweet_id"]),
    }


def _write_health(status, **fields):
    payload = {
        "checked_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "status": status,
        **fields,
    }
    try:
        with open(HEALTH_PATH, "w") as f: json.dump(payload, f, indent=2)
    except Exception: pass
    print(json.dumps(payload))


def _scrape_health_check():
    """Detect fresh generated_at + stale newest tweet condition."""
    try:
        d = json.load(open(X_2026_PATH))
    except Exception as e:
        return {"cache_readable": False, "err": str(e)[:120]}
    ga = d.get("generated_at")
    newest_iso = None
    for show in d.get("results", {}).values():
        for t in show.get("tweets_2026", []):
            ct = t.get("created_dt") or t.get("created_at")
            if not ct: continue
            try:
                tdt = dt.datetime.fromisoformat(ct) if "T" in ct else dt.datetime.strptime(ct, "%a %b %d %H:%M:%S %z %Y")
                if newest_iso is None or tdt.isoformat() > newest_iso:
                    newest_iso = tdt.isoformat()
            except Exception:
                continue
    now = dt.datetime.now(dt.timezone.utc)
    ga_dt = None
    try: ga_dt = dt.datetime.fromisoformat(ga)
    except Exception: pass
    newest_age_h = None
    if newest_iso:
        try:
            newest_dt = dt.datetime.fromisoformat(newest_iso)
            newest_age_h = round((now - newest_dt).total_seconds() / 3600, 1)
        except Exception: pass
    stale = newest_age_h is not None and newest_age_h > 48
    return {
        "cache_readable": True,
        "generated_at": ga,
        "generated_at_age_h": round((now - ga_dt).total_seconds() / 3600, 1) if ga_dt else None,
        "newest_tweet_iso": newest_iso,
        "newest_tweet_age_h": newest_age_h,
        "stale_cache_warn": stale,
    }


def main():
    print("=== GU_UPCOMING_AUTO_V1 ===")
    # 1. Health check on x_2026.json
    health = _scrape_health_check()
    print(f"HEALTH: {json.dumps(health, indent=2)}")
    # 2. Fresh scrape via GraphQL
    cookies = _load_cookies()
    if not cookies:
        _write_health("FAIL_NO_COOKIES", **health, upcoming_discovered=False)
        sys.exit(2)
    print(f"cookies_loaded={len(cookies)}")
    all_tweets = []
    n_by_handle = {}
    for handle in HANDLES_TO_SCAN:
        uid = _get_user_id(handle, cookies)
        if not uid:
            print(f"handle_resolve_failed: {handle}")
            n_by_handle[handle] = 0
            continue
        tweets = _fetch_latest_tweets(uid, cookies, page_size=40)
        n_by_handle[handle] = len(tweets)
        print(f"  @{handle}: {len(tweets)} tweets fetched")
        all_tweets.extend(tweets)
    if not all_tweets:
        _write_health("FAIL_ZERO_FRESH_TWEETS", n_by_handle=n_by_handle, **health,
                      upcoming_discovered=False)
        sys.exit(3)
    # 3. Deduplicate by id
    by_id = {}
    for t in all_tweets:
        by_id[t.get("id")] = t
    all_tweets = list(by_id.values())
    # Sort by created_at desc
    def _sort_key(t):
        ct = t.get("created_at") or ""
        try:
            return dt.datetime.strptime(ct, "%a %b %d %H:%M:%S %z %Y").isoformat()
        except Exception:
            return ""
    all_tweets.sort(key=_sort_key, reverse=True)
    fresh_newest = _sort_key(all_tweets[0]) if all_tweets else None
    print(f"fresh_scrape_newest_iso={fresh_newest}  total_unique={len(all_tweets)}")
    # 4. Extract upcoming
    upcoming = _extract_upcoming(all_tweets)
    if not upcoming:
        _write_health("NO_UPCOMING_MATCH", n_by_handle=n_by_handle,
                      fresh_newest=fresh_newest, tweets_examined=len(all_tweets),
                      **health, upcoming_discovered=False)
        print("NO_UPCOMING_EPISODE_DETECTED")
        sys.exit(0)
    # 5. Format + write
    entry = _format_upcoming_entry(upcoming)
    print(f"DISCOVERED: {json.dumps(entry, indent=2)}")
    with open(UPCOMING_PATH, "w") as f:
        json.dump([entry], f, indent=2)
    _write_health("UPCOMING_DISCOVERED", n_by_handle=n_by_handle,
                  fresh_newest=fresh_newest, tweets_examined=len(all_tweets),
                  entry_source_tweet_id=upcoming["tweet_id"],
                  entry_guest=entry["guest"], entry_date=entry["date"],
                  **health)
    return entry


if __name__ == "__main__":
    main()
