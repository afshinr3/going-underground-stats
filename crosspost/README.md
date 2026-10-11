# crosspost: X video → Telegram / Instagram / LinkedIn / Rednote

This watches **@Gunderground_tv** and **@neworder_tv** on X. When either one posts a native video, it
downloads the highest-bitrate MP4 and rewrites the caption. Then it republishes the video to Afshin
Rattansi's Telegram channel, Instagram (Reels), LinkedIn and Rednote (Xiaohongshu).

```
crosspost/
├── config/settings.py        account → Rumble mapping, caption limits, all credentials (env)
├── monitors/x_monitor.py     X API v2 timeline (or RSS + yt-dlp), rate limits, token refresh, download
├── transformers/caption.py   CTA stripping, link expansion, show-specific CTA, per-platform truncation
├── publishers/               telegram.py · instagram.py · linkedin.py · rednote.py
├── db/state.py               SQLite: seen posts, per-platform delivery + retry counts, cursors
├── main.py                   runner (daemon / --once / --dry-run), JSON logs, temp cleanup
├── tests/test_pipeline.py    stdlib unittest suite
└── deploy/                   systemd unit, macOS launchd plist  (+ Dockerfile, docker-compose.yml)
```

## What it does each cycle

1. **Discover.** For each handle it fetches tweets newer than the stored `since_id`. On the very
   first run it only looks back `INITIAL_LOOKBACK_HOURS`, so old posts are not posted again. A post
   is kept only if its *own* attachments include media of type `video`. That rules out text-only
   posts, images, GIFs, retweets and replies to other accounts. Quote posts that carry their own
   clip are kept, since that is the usual GU format. New posts go into SQLite with
   `tweet_id` as the primary key, so nothing is processed twice.
2. **Download** the highest-`bit_rate` `video/mp4` variant to `temp/<tweet_id>.mp4`. If that
   fails, it falls back to yt-dlp.
3. **Transform** the caption:
   - Unescape `&amp;`. Expand `t.co` links to their real URLs.
   - Drop the t.co links that only point at the attached video or at the quoted post.
   - Remove the trailing *"Watch the full interview in the quoted post below. 👇"* and close
     variants ("quote post", "quoted tweet", "See the full episode below", ⬇️, stray 👇, a
     trailing t.co). Hashtags that follow the CTA are kept.
   - Append the CTA for the show:
     - `@Gunderground_tv` → `Watch the full interview 👇` + `https://rumble.com/c/GoingUnderground`
     - `@neworder_tv` → `Watch the full interview 👇` + `https://rumble.com/c/NewOrderTV`

     The URL is written as plain text, not `[url](url)` markdown, because none of the four
     platforms renders markdown.
   - Fit the caption to each platform's limit: Telegram 1,024 · Instagram 2,200 · LinkedIn 3,000 ·
     Rednote 1,000. Length is counted in UTF-16 units, so an emoji counts as 2. When the caption
     is too long, the body is cut at a word boundary and ends in `…`. **The CTA is never cut.**
4. **Publish** to each enabled platform and record the result per platform. If some platforms
   fail, only those are retried on later cycles, up to `MAX_PUBLISH_ATTEMPTS`. The video stays in
   `temp/` until every platform has either succeeded or used up its attempts. Then it is deleted.

## Setup

```bash
cd crosspost
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
playwright install chromium          # x.com cookie monitor and Rednote playwright mode
brew install ffmpeg  # or apt install ffmpeg — optional; gives Telegram correct aspect/duration
cp .env.example .env                 # fill in credentials (see below)

python -m unittest discover -s tests # 20 offline tests
python main.py --dry-run             # discover + print every platform's caption; posts nothing
python main.py --once --platforms telegram   # first real run against one platform
python main.py                       # daemon, polls every POLL_INTERVAL_SECONDS (default 600)
```

### Credentials

| Platform | What you need |
|---|---|
| **X** | `X_BEARER_TOKEN` from developer.x.com. **The Free tier cannot read user timelines.** You need Basic or higher. Two other options: OAuth 2.0 (`X_CLIENT_ID`/`X_CLIENT_SECRET`/`X_REFRESH_TOKEN`, refreshed automatically, with the rotated refresh token saved in the DB), `X_COOKIES_FILE`/`X_COOKIES_JSON`, a logged-in x.com cookie export read with headless Chromium the same way `fetch_and_push.py` reads X, so no paid API is needed, or `X_RSS_URL_TEMPLATE`, which points at a self-hosted RSSHub/Nitter and needs yt-dlp. |
| **Telegram** | Create a bot with @BotFather, add it as an **admin** of the channel, then set `TELEGRAM_CHANNEL_ID` (`@name` or `-100…`). The cloud Bot API rejects uploads over 50 MB. For larger files, run [telegram-bot-api](https://github.com/tdlib/telegram-bot-api) locally and set `TELEGRAM_API_BASE`. |
| **Instagram** | The IG account must be Business or Creator and linked to a Facebook Page. You need a Meta app with `instagram_basic` and `instagram_content_publish`, plus a long-lived token, which lasts 60 days: refresh it before then. `INSTAGRAM_USER_ID` is the IG user id. Uploads use Meta's **resumable upload**, so the video never needs public hosting. Reels must be 3 s–15 min long. |
| **LinkedIn** | You need a LinkedIn app with the Community Management API (organization page) or "Share on LinkedIn" (`w_member_social`, personal profile). Set `LINKEDIN_AUTHOR_URN` to `urn:li:person:…` or `urn:li:organization:…`. Tokens last 60 days. Uploads use the multipart `rest/videos` API, then `rest/posts`. Hashtags become real LinkedIn hashtags. |
| **Rednote** | There is no official API. **Playwright mode** drives creator.xiaohongshu.com with a saved session: run `python -m publishers.rednote login` once on a machine with a screen and scan the QR code. Copy the resulting `config/rednote_cookies.json` to the server. **Webhook mode** posts the video, title and caption to `REDNOTE_WEBHOOK_URL` for a self-hosted connector or a manual-approval queue. Titles are capped at 20 characters, taken from the first sentence. It is disabled by default until you have logged in. |

## Running unattended

- **Alerts.** A Telegram DM goes to `ALERT_TELEGRAM_CHAT_ID` (`alerts/notifier.py`) when:
  - X can't be read
  - a platform rejects a post
  - a post is given up on (with its link)
  - a platform is enabled but not configured
  - a cycle crashes
  - checks resume after a long gap

  Each alert includes the fix. A persisting problem repeats at most every 12 h, and a ✅ message
  follows when it clears.
- **Credential upkeep** (`alerts/tokens.py`, every 6 h): the Instagram token is renewed
  automatically. LinkedIn tokens, X cookies and the Rednote login get a warning 7 days before
  they expire. `python main.py --check-credentials` prints every expiry date.
- **Dead-man's switch.** `HEALTHCHECK_URL` is pinged after every cycle. If pings stop (Mac
  asleep, agent dead), healthchecks.io alerts you.

## Deploy

- **Docker Compose** (any Linux box or VPS): `docker compose up -d --build`. State and the Rednote
  session are stored in `./data`.
- **systemd**: clone to `/opt/gu-crosspost`, create a venv, install `deploy/gu-crosspost.service`.
- **macOS (e.g. the M2 Pro)**: `deploy/com.gu.crosspost.plist` with launchd.
- **cron instead of a daemon**: `*/10 * * * * cd …/crosspost && .venv/bin/python main.py --once`.

Only run **one** instance per state DB. The SQLite file is what prevents double posting.

## Caveats

- Meta, LinkedIn and Xiaohongshu all review apps and accounts that post automatically. Use the
  accounts' own apps and tokens, and expect Xiaohongshu to occasionally ask for a captcha or a
  new login. The publisher fails clearly and saves a screenshot in `temp/` when that happens.
- Whether reposting a show's clips is allowed depends on who holds the rights. Make sure the
  destination accounts are entitled to the footage.
