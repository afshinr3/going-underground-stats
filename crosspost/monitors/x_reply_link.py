"""SCHEDULED_EPISODE_LINK_V1_20261016 -- the episode link the source account posts in its OWN reply.

Operator 2026-10-16: the Facebook post said "follow our Rumble channel, link below in the replies 👇 ... Watch the
full interview https://rumble.com/c/GoingUnderground" -- it should point to the scheduled Rumble episode. On X, GU
puts the episode URL in a self-reply; the relay only ever saw the parent post, so every platform got the channel
link (and a "link below in the replies" line that means nothing off X). This reads the post's conversation the same
way XCookieMonitor reads the timeline (headless browser + the web app's own TweetDetail response) and returns the
first rumble.com/v... URL in a reply by the SAME author. None when there is none (the channel link is the fallback).
"""
from __future__ import annotations

import re

RUMBLE_EPISODE = re.compile(r"https?://(?:www\.)?rumble\.com/v[0-9a-z]+-[^\s\"'<>]+", re.I)


def _walk(node, out):
    if isinstance(node, dict):
        if "legacy" in node and isinstance(node["legacy"], dict) and node.get("rest_id"):
            out.append(node)
        for v in node.values():
            _walk(v, out)
    elif isinstance(node, list):
        for v in node:
            _walk(v, out)


def rumble_links_from_detail(payloads, tweet_id: str, author_id: str | None = None):
    found = []
    for pl in payloads:
        nodes = []
        _walk(pl, nodes)
        if author_id is None:   # the parent post's own author -- never accept another user's link
            author_id = next((n["legacy"].get("user_id_str") for n in nodes if n.get("rest_id") == str(tweet_id)), None)
            if author_id is None:
                continue
        for n in nodes:
            lg = n["legacy"]
            uid = lg.get("user_id_str")
            if lg.get("in_reply_to_status_id_str") != str(tweet_id) and n.get("rest_id") != str(tweet_id):
                continue
            if author_id and uid != author_id:
                continue
            for u in (lg.get("entities") or {}).get("urls") or []:
                for cand in (u.get("expanded_url"), u.get("unwound_url"), u.get("url")):
                    if cand and RUMBLE_EPISODE.match(cand):
                        found.append((n.get("rest_id"), cand.split("?")[0]))
            for m in RUMBLE_EPISODE.finditer(lg.get("full_text") or ""):
                found.append((n.get("rest_id"), m.group(0)))
    # replies first, the parent itself last
    found.sort(key=lambda x: x[0] == str(tweet_id))
    return [u for _, u in found]


def episode_link(handle: str, tweet_id: str, cookies: list[dict], author_id: str | None = None) -> str | None:
    from playwright.sync_api import sync_playwright
    payloads = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1280, "height": 2000})
        ctx.add_cookies(cookies)
        page = ctx.new_page()

        def on_response(resp):
            if resp.status == 200 and "TweetDetail" in resp.url:
                try:
                    payloads.append(resp.json())
                except Exception:
                    pass
        page.on("response", on_response)
        try:
            page.goto(f"https://x.com/{handle}/status/{tweet_id}", wait_until="domcontentloaded", timeout=60_000)
            try:
                page.wait_for_selector('article[data-testid="tweet"]', timeout=30_000)
            except Exception:
                pass
            for _ in range(20):
                if payloads:
                    break
                page.wait_for_timeout(500)
            page.mouse.wheel(0, 3000)
            page.wait_for_timeout(2000)
        finally:
            ctx.close()
            browser.close()
    links = rumble_links_from_detail(payloads, tweet_id, author_id)
    return links[0] if links else None
