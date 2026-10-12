"""Facebook PERSONAL PROFILE publisher (browser automation).

Meta's Graph API cannot post to personal profiles (publish_actions was removed in 2018), and
Reels published to Instagram through the API are never auto-shared to Facebook. The operator
chose (2026-10-12) to have the clip posted to his own profile by driving facebook.com in
headless Chromium with his logged-in session, accepting that automating a personal account is
against Facebook's terms and can get the account checkpointed or restricted.

Session: export facebook.com cookies with the Cookie-Editor extension (Export -> JSON) to
FB_COOKIES_FILE (default config/fb_cookies.json, git-ignored), same as the X cookies. Or run
`python -m publishers.facebook_profile login` on the Mac's screen. Check it without posting:
`python -m publishers.facebook_profile check`.

Flow: home feed -> "What's on your mind" -> Photo/video -> attach the clip -> caption ->
(Next) -> Post -> wait for the composer to close. SELECTORS hold every UI-dependent string
(English UI); when Facebook changes its composer, fix them there. Any login wall or checkpoint
raises PublishError immediately -- it never tries to get past one.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

from .base import PublishError, Publisher, PublishResult

HOME = "https://www.facebook.com/"

SELECTORS = {
    "composer": re.compile(r"what'?s on your mind", re.I),
    "photo_video": re.compile(r"^photo/video$", re.I),
    "dialog_title": re.compile(r"create post", re.I),
    "next": re.compile(r"^next$", re.I),
    "post": re.compile(r"^post$", re.I),
    "login_marker": "input[name='email'], input[name='pass'], form[action*='login']",
}
WALL = re.compile(r"/(login|checkpoint|two_step_verification|recover)", re.I)


def _load_storage(path: Path) -> dict:
    """Playwright storage_state, or a Cookie-Editor export (list of cookies)."""
    data = json.loads(path.read_text())
    if isinstance(data, list):
        return {"cookies": [{
            "name": c["name"], "value": c["value"],
            "domain": c.get("domain", ".facebook.com"), "path": c.get("path", "/"),
            "expires": c.get("expirationDate", c.get("expires", -1)),
            "httpOnly": c.get("httpOnly", False), "secure": c.get("secure", True),
            "sameSite": {"no_restriction": "None", "lax": "Lax", "strict": "Strict"}.get(
                str(c.get("sameSite", "")).lower(), "Lax"),
        } for c in data], "origins": []}
    return data


def _open(pw, settings, headless=True):
    browser = pw.chromium.launch(headless=headless)
    ctx = browser.new_context(storage_state=_load_storage(settings.fb_cookies_file), locale="en-GB",
                              viewport={"width": 1400, "height": 1000})
    return browser, ctx


def _assert_logged_in(page) -> None:
    if WALL.search(page.url) or page.locator(SELECTORS["login_marker"]).count():
        raise PublishError("Facebook session expired or checkpointed -- export fresh facebook.com cookies "
                           "(Cookie-Editor -> Export JSON) to FB_COOKIES_FILE; nothing was posted")


class FacebookProfilePublisher(Publisher):
    name = "facebook_profile"

    def validate(self) -> None:
        if not self.s.fb_cookies_file.exists():
            raise PublishError(f"no Facebook session at {self.s.fb_cookies_file}; export facebook.com cookies there")
        try:
            import playwright  # noqa: F401
        except ImportError as e:
            raise PublishError("playwright not installed") from e

    def publish(self, video: Path, caption: str, post: dict) -> PublishResult:
        from playwright.sync_api import TimeoutError as PWTimeout
        from playwright.sync_api import sync_playwright

        with sync_playwright() as pw:
            browser, ctx = _open(pw, self.s, headless=self.s.fb_headless)
            page = ctx.new_page()
            try:
                page.goto(HOME, wait_until="domcontentloaded", timeout=60_000)
                page.wait_for_timeout(4000)
                _assert_logged_in(page)

                page.get_by_role("button", name=SELECTORS["composer"]).first.click(timeout=20_000)
                dialog = page.get_by_role("dialog").filter(has_text=SELECTORS["dialog_title"]).last
                dialog.wait_for(timeout=20_000)
                try:
                    dialog.get_by_role("button", name=SELECTORS["photo_video"]).first.click(timeout=5000)
                except PWTimeout:
                    pass  # some layouts show the drop zone straight away
                dialog.locator("input[type='file']").first.set_input_files(str(video))

                box = dialog.locator("div[role='textbox'][contenteditable='true']").first
                box.click()
                for i, line in enumerate(caption.split("\n")):
                    if i:
                        page.keyboard.press("Shift+Enter")
                    if line:
                        page.keyboard.insert_text(line)
                page.wait_for_timeout(1500)

                # newer composers put audience/extras behind "Next"
                nxt = dialog.get_by_role("button", name=SELECTORS["next"])
                if nxt.count():
                    nxt.first.click()
                    page.wait_for_timeout(2000)
                    dialog = page.get_by_role("dialog").last
                btn = dialog.get_by_role("button", name=SELECTORS["post"]).last
                # the button stays disabled until the upload is attached
                for _ in range(180):
                    if btn.get_attribute("aria-disabled") != "true":
                        break
                    page.wait_for_timeout(5000)
                else:
                    raise PublishError("Post button never enabled (upload stuck)")
                btn.click()
                # the composer closes once Facebook has accepted the post; the video keeps
                # processing server-side after that
                page.get_by_role("dialog").filter(has_text=SELECTORS["dialog_title"]).wait_for(
                    state="detached", timeout=20 * 60_000)
                _assert_logged_in(page)
                self.s.fb_cookies_file.write_text(json.dumps(ctx.storage_state()))  # keep rotated cookies
                return PublishResult(self.name, None, None)
            except PublishError:
                raise
            except Exception as e:
                shot = self.s.temp_dir / "facebook_profile_failure.png"
                try:
                    page.screenshot(path=str(shot), full_page=True)
                except Exception:
                    pass
                raise PublishError(f"facebook composer step failed ({type(e).__name__}: {str(e)[:200]}); "
                                   f"screenshot {shot}") from e
            finally:
                ctx.close()
                browser.close()


def check(settings) -> int:
    """Read-only: is the saved session logged in, and can the composer be found? Posts nothing."""
    from playwright.sync_api import sync_playwright
    with sync_playwright() as pw:
        browser, ctx = _open(pw, settings)
        page = ctx.new_page()
        try:
            page.goto(HOME, wait_until="domcontentloaded", timeout=60_000)
            page.wait_for_timeout(5000)
            try:
                _assert_logged_in(page)
            except PublishError as e:
                print("NOT LOGGED IN:", e)
                return 1
            found = page.get_by_role("button", name=SELECTORS["composer"]).count()
            print("logged in: yes | composer found:", bool(found), "| url:", page.url)
            if not found:
                shot = settings.temp_dir / "facebook_profile_check.png"
                page.screenshot(path=str(shot))
                print("screenshot:", shot)
            return 0 if found else 2
        finally:
            ctx.close()
            browser.close()


def login(settings) -> None:
    """Visible browser on the Mac: log in by hand, then the session is saved."""
    from playwright.sync_api import sync_playwright
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False)
        ctx = browser.new_context(locale="en-GB")
        page = ctx.new_page()
        page.goto(HOME)
        print("Log in to Facebook in the browser window (incl. any 2FA)...")
        page.wait_for_function("() => !document.querySelector(\"input[name='pass']\")", timeout=10 * 60_000)
        page.wait_for_timeout(5000)
        settings.fb_cookies_file.parent.mkdir(parents=True, exist_ok=True)
        settings.fb_cookies_file.write_text(json.dumps(ctx.storage_state()))
        print("saved", settings.fb_cookies_file)
        browser.close()


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from config.settings import load_settings
    cmd = sys.argv[1] if len(sys.argv) > 1 else "check"
    s = load_settings()
    if cmd == "login":
        login(s)
    else:
        sys.exit(check(s))
