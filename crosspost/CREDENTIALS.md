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

## 3. Instagram (20–30 min, one-off)
Requirements: the IG account must be **Professional** (Creator or Business) and linked to a
**Facebook Page**. You can do both in the IG app: Settings → Account type / Linked accounts.
1. Go to developers.facebook.com → **Create app** → type *Business*. Add the **Instagram Graph
   API** product.
2. Open **Graph API Explorer**, select the app and ask for these permissions:
   `instagram_basic`, `instagram_content_publish`, `pages_show_list`, `pages_read_engagement`.
   Then **Generate token**.
3. Make it long-lived (60 days) with the **Access Token Debugger → Extend**.
4. Get the IG user id: in Explorer, run `me/accounts?fields=instagram_business_account`. The
   number under `instagram_business_account.id` is the id.
5. In `.env`, set `INSTAGRAM_USER_ID=` and `INSTAGRAM_ACCESS_TOKEN=`.

While the app is in Development mode, posting works for accounts that hold a role on the app,
which yours does. Put a reminder in your calendar to extend the token every ~50 days.

## 4. LinkedIn (20 min + possible approval wait)
1. Go to linkedin.com/developers → **Create app**. It must be tied to a LinkedIn Page; your own is
   fine.
2. Under **Products**, add **Share on LinkedIn** to post as you. To post as a company page, add
   **Community Management API** instead; that one needs LinkedIn's approval.
3. Use **Auth → OAuth 2.0 tools → Create token** with the scopes `w_member_social` and `openid`
   and `profile` (for a page, use `w_organization_social`). Copy the token.
4. Author URN:
   - For yourself: `urn:li:person:<sub>`. Get `<sub>` from
     `curl -H "Authorization: Bearer TOKEN" https://api.linkedin.com/v2/userinfo` (the `sub`
     field).
   - For a page: `urn:li:organization:<number in the page admin URL>`.
5. In `.env`, set `LINKEDIN_ACCESS_TOKEN=` and `LINKEDIN_AUTHOR_URN=`. The token lasts 60 days.

## 5. Rednote: optional, last
1. Run `.venv/bin/python -m publishers.rednote login`.
2. Scan the QR code with the Xiaohongshu app. The session saves itself.
3. In `.env`, set `REDNOTE_ENABLED=true`.

## Go live
1. Run `bash deploy/install_macos.sh --start`.
2. Logs are in `~/Library/Logs/gu-crosspost.log`.
3. To turn off one platform, set `<PLATFORM>_ENABLED=false` in `.env`. Then run
   `launchctl kickstart -k gui/$(id -u)/com.gu.crosspost`.
