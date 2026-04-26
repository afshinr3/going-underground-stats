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

import requests
from PIL import Image, ImageDraw, ImageFont
from playwright.async_api import async_playwright

ROOT = os.path.dirname(os.path.abspath(__file__))

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
    },
    {
        "name": "New Order",
        "data_file": os.path.join(ROOT, "videos_neworder.json"),
        "x_handle": "NewOrder_TV",
        "yt_channel_id": "UC7FXwSQPOlq-eqXjpS3TL8g",
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


async def fetch_x_views(handle, surname, since_date=None):
    """Fetch X tweet views for surname. If since_date given (YYYY-MM-DD), only count tweets on/after that date."""
    # Use X's built-in date filter so collisions with older same-surname guests are excluded
    query = f'from:{handle} {surname}'
    if since_date:
        query += f' since:{since_date}'
    encoded = urllib.parse.quote(query)
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            ctx = await browser.new_context()
            await ctx.add_cookies(X_COOKIES)
            page = await ctx.new_page()
            url = f'https://x.com/search?q={encoded}&f=live'
            await page.goto(url, wait_until="domcontentloaded", timeout=20000)
            await page.wait_for_timeout(4000)
            for _ in range(5):
                await page.evaluate("window.scrollBy(0, 2000)")
                await page.wait_for_timeout(1500)
            views = await page.evaluate(r"""
                () => {
                    var out = [];
                    document.querySelectorAll('article[data-testid="tweet"]').forEach(t => {
                        var a = t.querySelector('a[href*="/analytics"]');
                        if (a) {
                            var m = (a.getAttribute('aria-label') || a.textContent || '').match(/([\d,.]+)\s*(?:view|View)/i);
                            if (m) out.push(parseInt(m[1].replace(/,/g,'')));
                        }
                    });
                    return out;
                }
            """)
            return sum(views), len(views)
        finally:
            await browser.close()


async def update_show(show, ig_clips):
    """Refresh a single show's data file."""
    if not os.path.exists(show['data_file']):
        print(f"No {show['data_file']} — skipping", file=sys.stderr)
        return
    with open(show['data_file']) as f:
        cache = json.load(f)

    print(f"\n=== {show['name']} ===")
    yt, yt_dates = fetch_youtube_data(show['yt_channel_id'])

    for v in cache:
        surname = v.get('surname', '').lower()
        if not surname:
            continue
        # Use the YouTube publish date as a filter to exclude older same-surname guests
        since = yt_dates.get(surname)
        try:
            total, count = await fetch_x_views(show['x_handle'], surname, since_date=since)
            if total > 0:
                v['x_views'] = format_views(total)
                print(f"  {v['surname']}: {count} tweets since {since or 'any'}, X:{v['x_views']}")
        except Exception as e:
            print(f"  {v['surname']}: X error {e}", file=sys.stderr)
        if surname in yt:
            v['yt_views'] = yt[surname]
        if surname in ig_clips:
            v['ig_likes'] = ig_clips[surname]

    with open(show['data_file'], 'w') as f:
        json.dump(cache, f, indent=2)
    print(f"Saved {len(cache)} entries to {show['data_file']}")


async def main_fetch():
    ig_clips = fetch_instagram_clips()
    print(f"IG clips found for {len(ig_clips)} surnames")
    for show in SHOWS:
        await update_show(show, ig_clips)


def push_to_tidbyt():
    """Build animation from Going Underground data and push to both Tidbyts."""
    with open(SHOWS[0]['data_file']) as f:
        cache = json.load(f)

    sorted_eps = []
    for v in cache[:15]:
        total = sum(parse_count(v.get(k)) for k in ['rumble_views','x_views','yt_views','ig_likes'])
        name = v.get('surname', '?')
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

    frames = []
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
