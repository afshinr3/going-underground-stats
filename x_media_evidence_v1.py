#!/usr/bin/env python3
"""X_MEDIA_EVIDENCE_V1 — read-only raw TweetDetail fetch for classification proof.

Resolves whether the three unresolved episode-release posts carry a NATIVE X
video and, if so, its duration — the evidence the cached collector never kept
(_fetch_tweet_detail returns only an int view count).

READ-ONLY. Reuses the already-authorised local X session via x_graphql_scraper's
own cookie loader, rate-limit guard and request path. Never prints, logs or
persists cookies, bearer tokens, CSRF headers or full auth headers: only the
redacted media evidence below is written.

Retains per post: id, account, created_at, full text, media[] (type, media id,
duration_millis, variant bitrates/content types, aspect ratio), quoted/retweeted
source id, view metric + its definition, and the fetch timestamp.
"""
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path.home() / "RumbleMonitor"))
import x_graphql_scraper as X  # noqa: E402

HERE = Path(__file__).resolve().parent
OUT = HERE / "x_media_evidence_v1.json"
INV = HERE / "gu_content_inventory_v1.json"

# X's own definition, for the record.
VIEW_METRIC_DEF = ("views.count from the TweetDetail GraphQL payload = X 'Views' "
                   "(impressions of the post, counted once per user per post; for "
                   "video posts it is post impressions, NOT video plays or "
                   "completed views, and NOT unique reach)")


def _iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _candidates():
    inv = json.loads(INV.read_text())
    out = []
    for i in inv["items"]:
        if i["platform"] == "x" and "candidate platform full" in i["classification_basis"]:
            out.append({"episode_key": i["episode_key"], "guest": i["episode_guest"],
                        "tweet_id": i["platform_content_id"],
                        "cached_view_count": i["value"]})
    return out


def _raw_tweet_detail(tweet_id, cookies):
    """Same request _fetch_tweet_detail issues, but returns the raw body."""
    if X._is_rate_limited():
        return None, "rate_limited_locally"
    headers = X._get_auth_headers(cookies)
    hashes = X._get_endpoint_hashes()
    variables = json.dumps({
        "focalTweetId": tweet_id, "with_rux_injections": False,
        "rankingMode": "Relevance", "includePromotedContent": True,
        "withCommunity": True, "withQuickPromoteEligibilityTweetFields": True,
        "withBirdwatchNotes": True, "withVoice": True,
    }, separators=(',', ':'))
    features = json.dumps({
        "rweb_tipjar_consumption_enabled": True,
        "responsive_web_graphql_exclude_directive_enabled": True,
        "verified_phone_label_enabled": False,
        "creator_subscriptions_tweet_preview_api_enabled": True,
        "responsive_web_graphql_timeline_navigation_enabled": True,
        "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
        "communities_web_enable_tweet_community_results_fetch": True,
        "c9s_tweet_anatomy_moderator_badge_enabled": True,
        "articles_preview_enabled": True, "responsive_web_edit_tweet_api_enabled": True,
        "graphql_is_translatable_rweb_tweet_is_translatable_enabled": True,
        "view_counts_everywhere_api_enabled": True,
        "longform_notetweets_consumption_enabled": True,
        "responsive_web_twitter_article_tweet_consumption_enabled": True,
        "freedom_of_speech_not_reach_fetch_enabled": True,
        "standardized_nudges_misinfo": True,
        "tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled": True,
        "longform_notetweets_rich_text_read_enabled": True,
        "longform_notetweets_inline_media_enabled": True,
        "responsive_web_enhance_cards_enabled": False,
    }, separators=(',', ':'))
    ft = json.dumps({"withArticleRichContentState": True, "withArticlePlainText": False,
                     "withGrokAnalyze": False, "withDisallowedReplyControls": False},
                    separators=(',', ':'))
    import urllib.parse
    hash_id = hashes.get("TweetDetail", X.DEFAULT_HASHES["TweetDetail"])
    url = ("https://x.com/i/api/graphql/%s/TweetDetail?variables=%s&features=%s&fieldToggles=%s"
           % (hash_id, urllib.parse.quote(variables), urllib.parse.quote(features),
              urllib.parse.quote(ft)))
    status, body, resp_headers = X._make_request(url, headers)
    if status == 429:
        X._record_rate_limit(resp_headers)
        return None, "http_429_rate_limited"
    if status in (401, 403):
        return None, f"http_{status}_auth_failure"
    if status != 200 or not body:
        return None, f"http_{status}_or_empty_body"
    X._record_success()
    return body, None


def _find_tweet(payload, tweet_id):
    """Locate the focal tweet's legacy object anywhere in the response tree."""
    found = []

    def walk(o):
        if isinstance(o, dict):
            lg = o.get("legacy")
            if isinstance(lg, dict) and str(lg.get("id_str") or "") == str(tweet_id):
                found.append(o)
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    walk(payload)
    return found[0] if found else None


def _media_evidence(node):
    lg = node.get("legacy") or {}
    ent = lg.get("extended_entities") or lg.get("entities") or {}
    media = []
    for m in ent.get("media") or []:
        vi = m.get("video_info") or {}
        media.append({
            "media_id": m.get("id_str"),
            "type": m.get("type"),                     # photo | video | animated_gif
            "duration_millis": vi.get("duration_millis"),
            "duration_min": round((vi.get("duration_millis") or 0) / 60000.0, 2) or None,
            "aspect_ratio": vi.get("aspect_ratio"),
            "variants": [{"content_type": v.get("content_type"), "bitrate": v.get("bitrate")}
                         for v in (vi.get("variants") or [])],
            "expanded_url": m.get("expanded_url"),
        })
    views = (node.get("views") or {})
    return {
        "created_at": lg.get("created_at"),
        "full_text": lg.get("full_text"),
        "account_screen_name": (((node.get("core") or {}).get("user_results") or {})
                                .get("result", {}).get("legacy", {}) or {}).get("screen_name"),
        "media": media,
        "has_native_video": any(m["type"] in ("video", "animated_gif") for m in media),
        "quoted_status_id": lg.get("quoted_status_id_str"),
        "retweeted_status_id": ((lg.get("retweeted_status_result") or {})
                                .get("result", {}).get("rest_id")),
        "is_quote_status": lg.get("is_quote_status"),
        "view_count": views.get("count"),
        "view_count_state": views.get("state"),
        "view_metric_definition": VIEW_METRIC_DEF,
    }


def main():
    cookies = X._load_cookies()
    if not cookies:
        print("NO_LOCAL_X_SESSION"); return 2
    results = []
    for c in _candidates():
        body, err = _raw_tweet_detail(c["tweet_id"], cookies)
        rec = dict(c)
        rec["fetched_at_iso"] = _iso()
        if err:
            rec.update({"fetch_ok": False, "blocker": err, "evidence": None})
        else:
            try:
                payload = json.loads(body)
            except Exception as e:
                rec.update({"fetch_ok": False, "blocker": f"unparseable_json: {e}", "evidence": None})
                results.append(rec); continue
            node = _find_tweet(payload, c["tweet_id"])
            if not node:
                rec.update({"fetch_ok": False, "blocker": "focal_tweet_not_in_payload",
                            "evidence": None})
            else:
                rec.update({"fetch_ok": True, "blocker": None, "evidence": _media_evidence(node)})
        results.append(rec)

    out = {
        "marker": "X_MEDIA_EVIDENCE_V1",
        "generated_iso": _iso(),
        "read_only": True,
        "secrets_policy": "no cookies, bearer/CSRF tokens or auth headers are captured or stored",
        "view_metric_definition": VIEW_METRIC_DEF,
        "posts": results,
    }
    OUT.write_text(json.dumps(out, indent=2, ensure_ascii=False))
    print(f"wrote {OUT}")
    for r in results:
        if not r["fetch_ok"]:
            print(f"  {r['guest']:18s} {r['tweet_id']}  BLOCKED: {r['blocker']}")
            continue
        e = r["evidence"]
        durs = [m["duration_min"] for m in e["media"] if m["duration_min"]]
        print(f"  {r['guest']:18s} {r['tweet_id']}  @{e['account_screen_name']}  "
              f"native_video={e['has_native_video']}  media={len(e['media'])}  "
              f"durations_min={durs}  views={e['view_count']}  "
              f"quoted={e['quoted_status_id']} rt={e['retweeted_status_id']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
