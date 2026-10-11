# Getting the credentials (do this at the M2 Pro)

Everything goes in `~/gu-crosspost/crosspost/.env`. Never paste a value into chat. Do them in this
order: after the first two, a live test post is already possible.

## 1. X source: x.com cookies (5 min)
1. In Chrome, log in to x.com as any account that can see both profiles. A secondary account is
   safest.
2. Install the **Cookie-Editor** extension. On x.com, open it and choose **Export → JSON**.
3. Save the export as `~/gu-crosspost/crosspost/config/x_cookies.json`. That path is git-ignored.
   Then run `chmod 600` on it.
4. In `.env`, set `X_COOKIES_FILE=./config/x_cookies.json`.
5. Check: `.venv/bin/python main.py --dry-run` should print recent video posts and their captions.
   It shows nothing if neither account has posted a video since install, because installation set
   a "from now" baseline.

When the cookies expire, the log says "X login wall". Export them again.

## 2. Telegram (5 min)
1. In Telegram, message **@BotFather**, send `/newbot` and follow the prompts. It gives you a
   **token**.
2. In your channel: **Admins → Add admin →** the bot, with *Post messages* allowed.
3. In `.env`, set `TELEGRAM_BOT_TOKEN=` to the token. Set `TELEGRAM_CHANNEL_ID=` to `@yourchannel`
   for a public channel. For a private channel, use the `-100…` id: forward a channel post to
   @userinfobot to get it.
4. Test: `.venv/bin/python main.py --once --platforms telegram` (it posts only when a new video
   exists).

## 3. Alerts to your phone (2 min, uses the bot from step 2)
1. In Telegram, open your bot and send it any message, e.g. "hi".
2. Run `.venv/bin/python -m alerts.notifier whoami`. Copy the `ALERT_TELEGRAM_CHAT_ID=…` line it
   prints into `.env`.
3. Run `.venv/bin/python -m alerts.notifier test`. You should get a test message.
4. Optional, but recommended because the Mac can sleep: set up a dead-man's switch.
   - Create a free check at healthchecks.io. Set the period to 10 min and the grace to 1 h.
   - Connect its Telegram integration.
   - Put the ping URL in `HEALTHCHECK_URL=`.
   - If the Mac stops checking, healthchecks.io alerts you itself.

What you'll get alerts for:
- an X login problem
- a post that failed, with the link so you can post it by hand
- a platform that isn't configured
- a token expiring within 7 days
- a long gap with no checks

You also get a ✅ message when each problem clears.

## 4. Instagram (≈15 min, one-off)
Requirement: the account must be **Professional** (Creator or Business). This route, "Instagram
API with Instagram Login", needs **no Facebook Page**, and the token renews itself.
1. Go to developers.facebook.com → **Create app** → use case *Manage messaging & content on
   Instagram*.
2. Open **Instagram → API setup with Instagram login** and click **Add account**. Log in as your IG
   account.
3. Click **Generate token** and copy it. It's long-lived.
4. The same screen shows the account's **Instagram user ID**.
5. In `.env`, set:
   - `INSTAGRAM_GRAPH_HOST=graph.instagram.com`
   - `INSTAGRAM_USER_ID=`
   - `INSTAGRAM_ACCESS_TOKEN=`
6. Check it: run `.venv/bin/python main.py --check-credentials`.

The app stays in Development mode, which is fine for posting to your own account. Renewal is
automatic. If renewal ever fails, an alert tells you.

## 5. LinkedIn: personal profile (≈10 min)
1. Go to linkedin.com/developers → **Create app**.
   - It has to be associated with a LinkedIn Page.
   - Any page you admin works. If you have none, create a placeholder page; it only owns the app.
2. Under **Products**, add **Share on LinkedIn** and **Sign In with LinkedIn using OpenID
   Connect**. Both are instant.
3. Go to **Auth → OAuth 2.0 tools → Create token**.
   - Tick `w_member_social`, `openid` and `profile`.
   - Approve it, then copy the token.
   - Note the **expiry date** it shows.
4. Get your member id by running
   `curl -s -H "Authorization: Bearer TOKEN" https://api.linkedin.com/v2/userinfo`.
   The id is the `"sub"` value.
5. In `.env`, set:
   - `LINKEDIN_ACCESS_TOKEN=`
   - `LINKEDIN_AUTHOR_URN=urn:li:person:<sub>`
   - `LINKEDIN_TOKEN_EXPIRES_AT=YYYY-MM-DD`

LinkedIn doesn't let self-serve apps renew tokens. You'll get an alert 7 days before expiry; then
repeat step 3, which takes about 2 minutes every 60 days.

## 6. Rednote: optional, last
1. Run `.venv/bin/python -m publishers.rednote login`.
2. Scan the QR code with the Xiaohongshu app. The session saves itself.
3. In `.env`, set `REDNOTE_ENABLED=true`.

## Go live
0. Keep the Mac awake while it's plugged in: **System Settings → Battery → Options →
   "Prevent automatic sleeping on power adapter when the display is off"**. The
   service only runs while you're logged in. With the lid shut and on battery, nothing gets
   checked; when it wakes, it catches up and tells you how long the gap was.
1. Run `bash deploy/install_macos.sh --start`.
2. Logs are in `~/Library/Logs/gu-crosspost.log`.
3. To turn off one platform, set `<PLATFORM>_ENABLED=false` in `.env`. Then run
   `launchctl kickstart -k gui/$(id -u)/com.gu.crosspost`.
