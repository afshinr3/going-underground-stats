"""Rednote / Xiaohongshu (小红书) publisher.

Xiaohongshu has no public posting API, so two interchangeable modes are offered:

  REDNOTE_MODE=playwright (default)
      Drives the official Creator Center (creator.xiaohongshu.com) in headless Chromium
      with a saved logged-in session. Create the session once on a machine with a
      display:   python -m publishers.rednote login
      (scan the QR code with the Xiaohongshu app; the session is saved to
      REDNOTE_COOKIES_FILE). Sessions typically last weeks; re-run `login` when the
      publisher reports it has been logged out. Selectors below are the fragile part —
      if the Creator Center UI changes, update SELECTORS only.

  REDNOTE_MODE=webhook
      POSTs the video + title + caption as multipart/form-data to REDNOTE_WEBHOOK_URL,
      for use with a self-hosted connector (e.g. xiaohongshu-mcp, social-auto-upload, an
      n8n/Make flow, or a human-in-the-loop queue). Expects a 2xx and optionally JSON
      {"id": "...", "url": "..."}.

Note: outbound links are not clickable on Xiaohongshu and link-heavy notes can be
down-ranked; the Rumble URL is kept as plain text so the CTA stays consistent.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

from transformers.caption import build_title

from .base import PublishError, Publisher, PublishResult

# The international domain: this Mac (UAE) is routed to creator.rednote.com and the operator's
# login lives there; the same profile is NOT logged in on creator.xiaohongshu.com (2026-10-12).
PUBLISH_URL = "https://creator.rednote.com/publish/publish?source=official&from=tab_switch"
LOGIN_URL = "https://creator.rednote.com/login"

SELECTORS = {
    "video_tab": "text=上传视频",
    "file_input": "input[type='file']",
    "upload_done": "text=/上传成功|重新上传|替换视频/",
    "title": "input[placeholder*='标题']",
    "body": "div.ql-editor, #post-textarea, div[contenteditable='true']",
    # creator.rednote.com: the red bottom-bar 发布 is not a <button>; exact text so it can never
    # hit 发布笔记 (sidebar), 暂存离开 (save draft) or 定时发布 (scheduled)
    "publish_button": "text=/^\\s*发布\\s*$/",
    "published": "text=/发布成功|已发布/",
    "login_marker": "text=/扫码登录|短信登录|登录/",
}


def _click_publish(page) -> None:
    """Click the red 发布 button.

    On creator.rednote.com the bottom bar is <xhs-publish-btn> with a CLOSED shadow root, so
    neither text nor role selectors can see 发布 or 暂存离开 (save draft). The bar is screenshotted
    and the click goes to the centre of its red region -- the white 暂存离开 can never be hit.
    Falls back to the text selector on layouts that expose a normal button."""
    import io

    from PIL import Image

    bar = page.locator("xhs-publish-btn").first
    if bar.count():
        bar.scroll_into_view_if_needed()
        bb = bar.bounding_box()
        im = Image.open(io.BytesIO(bar.screenshot())).convert("RGB")
        y = im.height // 2
        reds = [x for x in range(im.width)
                if (lambda c: c[0] > 200 and c[1] < 90 and c[2] < 120)(im.getpixel((x, y)))]
        if not bb or len(reds) < 20:
            raise PublishError("publish bar found but no red 发布 button in it")
        page.mouse.click(bb["x"] + (min(reds) + max(reds)) / 2 * bb["width"] / im.width,
                         bb["y"] + bb["height"] / 2)
        return
    page.locator(SELECTORS["publish_button"]).last.click()


def _load_storage(path: Path) -> dict:
    """Accept either a Playwright storage_state file or a bare cookie list (browser export)."""
    data = json.loads(path.read_text())
    if isinstance(data, list):
        cookies = []
        for c in data:
            cookies.append({
                "name": c["name"], "value": c["value"],
                "domain": c.get("domain", ".xiaohongshu.com"), "path": c.get("path", "/"),
                "expires": c.get("expirationDate", c.get("expires", -1)),
                "httpOnly": c.get("httpOnly", False), "secure": c.get("secure", True),
                "sameSite": {"no_restriction": "None", "lax": "Lax", "strict": "Strict"}.get(
                    str(c.get("sameSite", "")).lower(), "Lax"),
            })
        return {"cookies": cookies, "origins": []}
    return data


class RednotePublisher(Publisher):
    name = "rednote"

    def validate(self) -> None:
        if self.s.rednote_mode == "webhook":
            if not self.s.rednote_webhook_url:
                raise PublishError("REDNOTE_WEBHOOK_URL is required in webhook mode")
            return
        if not self.s.rednote_cookies_file.exists():
            raise PublishError(
                f"no Rednote session at {self.s.rednote_cookies_file}; run `python -m publishers.rednote login`"
            )
        try:
            import playwright  # noqa: F401
        except ImportError as e:
            raise PublishError("playwright not installed (pip install playwright)") from e

    def publish(self, video: Path, caption: str, post: dict) -> PublishResult:
        title = build_title(post)
        if self.s.rednote_chinese:
            # operator 2026-10-12: Rednote captions in Simplified Chinese; English if translation fails
            from config.settings import CAPTION_LIMITS, REDNOTE_TITLE_LIMIT
            from transformers.translate import caption_zh, title_zh
            limit = CAPTION_LIMITS["rednote"]
            title = title_zh(caption, REDNOTE_TITLE_LIMIT) or title
            zh = caption_zh(caption, limit)
            if zh:
                # operator 2026-10-12: Chinese first, then the original English; cut at the limit
                both = zh + "\n\n" + caption
                caption = both if len(both) <= limit else both[: limit - 1] + "…"
        if self.s.rednote_mode == "webhook":
            return self._publish_webhook(video, title, caption, post)
        return self._publish_playwright(video, title, caption)

    # ------------------------------------------------------------------ webhook
    def _publish_webhook(self, video: Path, title: str, caption: str, post: dict) -> PublishResult:
        headers = {}
        if self.s.rednote_webhook_token:
            headers["Authorization"] = f"Bearer {self.s.rednote_webhook_token}"
        with open(video, "rb") as f:
            r = self.http.post(
                self.s.rednote_webhook_url,
                headers=headers,
                data={"title": title, "content": caption, "source_url": post.get("permalink", ""),
                      "tweet_id": post["tweet_id"]},
                files={"video": (video.name, f, "video/mp4")},
                timeout=1800,
            )
        if r.status_code >= 300:
            raise PublishError(f"webhook {r.status_code}: {r.text[:300]}")
        try:
            body = r.json()
        except ValueError:
            body = {}
        return PublishResult(self.name, body.get("id"), body.get("url"))

    # ------------------------------------------------------------------ playwright
    def _publish_playwright(self, video: Path, title: str, caption: str) -> PublishResult:
        from playwright.sync_api import TimeoutError as PWTimeout
        from playwright.sync_api import sync_playwright

        with sync_playwright() as pw:
            # 2026-10-12: a session injected into another browser is rejected (401 -> login) and
            # then revoked; Xiaohongshu also bounces Playwright's bundled Chromium. So posting
            # reuses the SAME real-Chrome profile the operator logged in with (`login`).
            if self.s.rednote_profile_dir.exists():
                ctx = pw.chromium.launch_persistent_context(
                    str(self.s.rednote_profile_dir), channel="chrome", headless=self.s.rednote_headless,
                    locale="zh-CN", viewport={"width": 1400, "height": 1000})
                browser = None
            else:
                browser = pw.chromium.launch(channel="chrome", headless=self.s.rednote_headless)
                ctx = browser.new_context(storage_state=_load_storage(self.s.rednote_cookies_file),
                                          locale="zh-CN", viewport={"width": 1400, "height": 1000})
            page = ctx.new_page()
            try:
                page.goto(PUBLISH_URL, wait_until="domcontentloaded", timeout=60_000)
                page.wait_for_timeout(10_000)  # the auth check redirects a few seconds after load
                if "login" in page.url:
                    raise PublishError("Rednote session expired — re-run `python -m publishers.rednote login`")
                try:
                    page.locator(SELECTORS["video_tab"]).first.click(timeout=5000)
                except PWTimeout:
                    pass  # video is the default tab
                page.locator(SELECTORS["file_input"]).first.set_input_files(str(video))
                page.locator(SELECTORS["upload_done"]).first.wait_for(timeout=15 * 60_000)

                page.locator(SELECTORS["title"]).first.fill(title)
                body = page.locator(SELECTORS["body"]).first
                body.click()
                # type line by line so the editor keeps line breaks
                for i, line in enumerate(caption.split("\n")):
                    if i:
                        page.keyboard.press("Enter")
                    if line:
                        page.keyboard.insert_text(line)
                page.keyboard.press("Escape")  # dismiss hashtag/topic suggestion popups
                page.wait_for_timeout(1500)

                _click_publish(page)
                try:
                    page.locator(SELECTORS["published"]).first.wait_for(timeout=120_000)
                except PWTimeout:
                    if "publish/success" not in page.url:
                        shot = self.s.temp_dir / "rednote_failure.png"
                        page.screenshot(path=str(shot), full_page=True)
                        raise PublishError(f"no publish confirmation (screenshot: {shot})")
                if browser is not None:  # the persistent profile keeps its own cookies
                    self.s.rednote_cookies_file.write_text(json.dumps(ctx.storage_state()))
                return PublishResult(self.name, None, None)
            finally:
                ctx.close()
                if browser is not None:
                    browser.close()


def login(settings) -> None:
    """Open a visible browser, let the operator scan the QR code, save the session."""
    from playwright.sync_api import sync_playwright

    # 2026-10-12: waiting for the URL to lose "login" never fired although the operator had
    # logged in, so the session was lost at the timeout. Logged-in is now detected from the
    # session cookies, and the session is saved every few seconds once they exist.
    auth_names = ("web_session", "galaxy_creator_session_id", "access-token-creator.xiaohongshu.com",
                  "customer-sso-sid", "customerClientId")
    import time as _t

    # v3: log in inside the dedicated real-Chrome profile that posting reuses, starting from the
    # publish page so whichever domain Rednote routes this Mac to (xiaohongshu.com or rednote.com)
    # is the one logged in. Done = the publish page with its upload input, stable for 20 s.
    settings.rednote_profile_dir.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as pw:
        ctx = pw.chromium.launch_persistent_context(
            str(settings.rednote_profile_dir), channel="chrome", headless=False, locale="zh-CN",
            viewport={"width": 1400, "height": 1000})
        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        page.goto(PUBLISH_URL)
        print("Log in to Rednote in the Chrome window (scan the QR code)...", flush=True)
        ok_since, deadline = None, _t.time() + 15 * 60
        while _t.time() < deadline:
            page.wait_for_timeout(3000)
            on_publish = "/publish/publish" in page.url and "login" not in page.url \
                and page.locator("input[type='file']").count() > 0
            if on_publish:
                ok_since = ok_since or _t.time()
                if _t.time() - ok_since > 20:
                    break
            else:
                ok_since = None
                if "login" not in page.url and "/publish/publish" not in page.url:
                    page.goto(PUBLISH_URL)  # logged in somewhere else: go to the upload page
        if ok_since:
            settings.rednote_cookies_file.parent.mkdir(parents=True, exist_ok=True)
            settings.rednote_cookies_file.write_text(json.dumps(ctx.storage_state()))
            print("Logged in on %s; profile saved in %s" % (page.url.split("/")[2], settings.rednote_profile_dir),
                  flush=True)
        ctx.close()
        if not ok_since:
            print("No login detected within 15 minutes", flush=True)
            sys.exit(1)


if __name__ == "__main__":  # python -m publishers.rednote login
    from config.settings import load_settings

    if sys.argv[1:] == ["login"]:
        login(load_settings())
    else:
        print("usage: python -m publishers.rednote login")
