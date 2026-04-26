# Going Underground stats — cloud edition

Runs on GitHub Actions every 15 minutes.

- Pulls X / YouTube / Instagram view counts for the latest 15 GU episodes
- Commits updated `videos.json` to this repo (raw URL = data feed for iOS/Android apps)
- Pushes a 15-frame animation to both Tidbyts

## Required secrets (set in repo Settings → Secrets → Actions)
- `X_COOKIES_JSON` — JSON array exported from your Chrome's `.x.com` cookies
- `IG_COOKIES_JSON` — same for `.instagram.com`
- `TIDBYT_KEY_1` — bearer token for `winsomely-tidy-chic-roach-990`
- `TIDBYT_KEY_2` — bearer token for `totally-fantastic-cordial-jacamar-855`

## Public data feed
After the first run, your apps point at:
`https://raw.githubusercontent.com/<user>/<repo>/main/videos.json`

LaMetric pushes are local-only and stay on the Mac (LaMetric API only listens on LAN).
