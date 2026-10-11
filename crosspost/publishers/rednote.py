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

PUBLISH_URL = "https://creator.xiaohongshu.com/publish/publish?source=official&from=tab_switch"
LOGIN_URL = "https://creator.xiaohongshu.com/login"

SELECTORS = {
    "video_tab": "text=上传视频",
    "file_input": "input[type='file']",
    "upload_done": "text=/上传成功|重新上传|替换视频/",
    "title": "input[placeholder*='标题']",
    "body": "div.ql-editor, #post-textarea, div[contenteditable='true']",
    "publish_button": "button:has-text('发布'):not(:has-text('定时'))",
    "published": "text=/发布成功|已发布/",
    "login_marker": "text=/扫码登录|短信登录|登录/",
}


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

        storage = _load_storage(self.s.rednote_cookies_file)
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=self.s.rednote_headless)
            ctx = browser.new_context(storage_state=storage, locale="zh-CN", viewport={"width": 1400, "height": 1000})
            page = ctx.new_page()
            try:
                page.goto(PUBLISH_URL, wait_until="domcontentloaded", timeout=60_000)
                page.wait_for_timeout(3000)
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

                page.locator(SELECTORS["publish_button"]).last.click()
                try:
                    page.locator(SELECTORS["published"]).first.wait_for(timeout=120_000)
                except PWTimeout:
                    if "publish/success" not in page.url:
                        shot = self.s.temp_dir / "rednote_failure.png"
                        page.screenshot(path=str(shot), full_page=True)
                        raise PublishError(f"no publish confirmation (screenshot: {shot})")
                # refresh the stored session so cookie rotation doesn't log us out
                self.s.rednote_cookies_file.write_text(json.dumps(ctx.storage_state()))
                return PublishResult(self.name, None, None)
            finally:
                ctx.close()
                browser.close()


def login(settings) -> None:
    """Open a visible browser, let the operator scan the QR code, save the session."""
    from playwright.sync_api import sync_playwright

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False)
        ctx = browser.new_context(locale="zh-CN")
        page = ctx.new_page()
        page.goto(LOGIN_URL)
        print("Log in to the Xiaohongshu Creator Center in the browser window (scan the QR code)...")
        page.wait_for_url(lambda u: "login" not in u, timeout=5 * 60_000)
        page.wait_for_timeout(3000)
        settings.rednote_cookies_file.parent.mkdir(parents=True, exist_ok=True)
        settings.rednote_cookies_file.write_text(json.dumps(ctx.storage_state()))
        print(f"Saved session to {settings.rednote_cookies_file}")
        browser.close()


if __name__ == "__main__":  # python -m publishers.rednote login
    from config.settings import load_settings

    if sys.argv[1:] == ["login"]:
        login(load_settings())
    else:
        print("usage: python -m publishers.rednote login")
