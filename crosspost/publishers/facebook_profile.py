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
import os
import re
import sys
from pathlib import Path

from .base import PublishError, Publisher, PublishResult

HOME = "https://www.facebook.com/"

SELECTORS = {
    "composer": re.compile(r"what'?s on your mind", re.I),
    # no literal "/" in these: it breaks Playwright's selector parser
    "photo_video": re.compile(r"^(photo.video|add photos or videos)", re.I),
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


# Login and posting MUST launch Chrome identically: cookies are encrypted per launch config
# (normal Chrome uses the real Keychain, which automated Chrome can't read -- measured
# 2026-10-12), and Facebook refuses to log in when Chrome advertises automation. So the
# automation banner/flag is removed and the same options are used everywhere.
_LAUNCH = dict(channel="chrome", locale="en-GB", viewport={"width": 1400, "height": 1000},
               ignore_default_args=["--enable-automation"],
               args=["--disable-blink-features=AutomationControlled"])


def _profile(settings) -> Path:
    return settings.db_path.parent / "fb_chrome_profile"


def _open(pw, settings, headless=True):
    """Same real-Chrome profile for login and posting: sessions are browser-bound (the Rednote
    QR session was revoked when replayed in another browser, 2026-10-12)."""
    prof = _profile(settings)
    if prof.exists():
        ctx = pw.chromium.launch_persistent_context(str(prof), headless=headless, **_LAUNCH)
        return None, ctx
    browser = pw.chromium.launch(channel="chrome", headless=headless)
    ctx = browser.new_context(storage_state=_load_storage(settings.fb_cookies_file), locale="en-GB",
                              viewport={"width": 1400, "height": 1000})
    return browser, ctx


def _assert_logged_in(page) -> None:
    if WALL.search(page.url) or page.locator(SELECTORS["login_marker"]).count():
        raise PublishError("Facebook session expired or checkpointed -- export fresh facebook.com cookies "
                           "(Cookie-Editor -> Export JSON) to FB_COOKIES_FILE; nothing was posted")


def _dismiss_popups(page) -> None:
    """Close interstitials that grey out the feed ("Remember password", cookie/notification
    prompts). Only harmless dismiss buttons are pressed -- never anything that posts."""
    for name in ("Not now", "Close", "Not Now", "Later"):
        try:
            b = page.get_by_role("button", name=name, exact=True)
            if b.count():
                b.first.click(timeout=2000)
                page.wait_for_timeout(800)
        except Exception:
            pass
    page.keyboard.press("Escape")


def _composer(page):
    btn = page.get_by_role("button", name=SELECTORS["composer"])
    return btn if btn.count() else page.get_by_text(SELECTORS["composer"])


ACTIVITY = "https://www.facebook.com/me/allactivity?category_key=MANAGEPOSTSPHOTOSANDVIDEOS"


def _norm(t: str) -> str:
    return " ".join(re.sub(r"[^a-z0-9]+", " ", t.lower()).split())


def _wait_until_listed(ctx, caption: str, temp_dir: Path, minutes: int = 20) -> bool:
    """Proof of posting: the caption's opening words show up in the activity log ("Your posts,
    photos and videos"). Emoji are dropped on both sides (the log renders flags as images)."""
    head = _norm(next((l for l in caption.split("\n") if _norm(l)), ""))[:40]
    if not head:
        return True
    chk = ctx.new_page()
    try:
        for _ in range(minutes * 2):
            chk.wait_for_timeout(30_000)
            try:
                chk.goto(ACTIVITY, wait_until="domcontentloaded", timeout=60_000)
                chk.wait_for_timeout(8000)
                if head in _norm(chk.inner_text("body")):
                    return True
            except Exception:
                pass
        chk.screenshot(path=str(temp_dir / "facebook_profile_not_listed.png"))
        return False
    finally:
        chk.close()


class FacebookProfilePublisher(Publisher):
    name = "facebook_profile"

    def validate(self) -> None:
        if not (_profile(self.s).exists() or self.s.fb_cookies_file.exists()):
            raise PublishError("no Facebook session: run `python -m publishers.facebook_profile login` on the Mac")
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
                _dismiss_popups(page)

                _composer(page).first.click(timeout=20_000)
                # Facebook shows Create Post either as a dialog or as a full page (both seen on
                # 2026-10-12), so everything is found page-wide by label, never via the dialog.
                page.get_by_text(SELECTORS["dialog_title"]).first.wait_for(timeout=20_000)
                page.wait_for_timeout(1500)

                # 1. caption FIRST: on 2026-10-12 typing after the upload lost the first line
                #    (the preview stole focus mid-typing)
                box = page.get_by_role("textbox", name=SELECTORS["composer"]).last
                box.click(timeout=15_000)
                page.keyboard.press("Meta+A")  # Facebook keeps unsent drafts: start from an empty box
                page.keyboard.press("Backspace")
                # the editor applies the clear asynchronously: without this pause it wiped the
                # first typed line (the headline went missing on 2026-10-12)
                page.wait_for_timeout(800)
                for i, line in enumerate(caption.split("\n")):
                    if i:
                        page.keyboard.press("Shift+Enter")
                    if line:
                        page.keyboard.insert_text(line)
                page.wait_for_timeout(1500)
                head = next((l.strip() for l in caption.split("\n") if l.strip()), "")[:40]
                typed = page.get_by_role("textbox", name=SELECTORS["composer"]).last.inner_text()
                if head and head not in typed.replace("\u00a0", " "):
                    raise PublishError("caption check failed: the first line is not in the composer (box has %r); "
                                       "nothing posted" % typed[:80])

                # 2. video through the button's own file chooser (a page-wide input[type=file] hit the
                #    wrong field on the full-page layout and the post went out as text + link)
                videos_before = page.locator("video").count()
                try:
                    with page.expect_file_chooser(timeout=10_000) as fc:
                        page.get_by_role("button", name=SELECTORS["photo_video"]).last.click(timeout=10_000)
                    fc.value.set_files(str(video))
                except PWTimeout:
                    raise PublishError("could not open the photo/video picker; nothing posted")
                for _ in range(60):  # never post without the clip
                    if page.locator("video").count() > videos_before:
                        break
                    page.wait_for_timeout(2000)
                else:
                    raise PublishError("video preview never appeared in the composer; nothing posted")

                # some composers put audience/extras behind "Next"
                nxt = page.get_by_role("button", name=SELECTORS["next"])
                if nxt.count():
                    nxt.last.click()
                    page.wait_for_timeout(2000)
                btn = page.get_by_role("button", name=SELECTORS["post"]).last
                # the button stays disabled until the upload is attached
                for _ in range(180):
                    if btn.get_attribute("aria-disabled") != "true":
                        break
                    page.wait_for_timeout(5000)
                else:
                    raise PublishError("Post button never enabled (upload stuck)")
                if os.environ.get("FB_DRY_RUN"):
                    page.screenshot(path=str(self.s.temp_dir / "facebook_profile_dryrun.png"))
                    raise PublishError("dry run: stopped before Post (nothing posted)")
                btn.click()
                # accepted = the composer (its "What's on your mind" box) is gone; the video keeps
                # processing server-side after that
                page.get_by_role("textbox", name=SELECTORS["composer"]).last.wait_for(
                    state="detached", timeout=20 * 60_000)
                _assert_logged_in(page)
                # 2026-10-13: four "published" clips never appeared -- the composer closes while the
                # video is still uploading and closing the browser killed it. Success now means the
                # post is in the operator's own activity log; this tab stays open meanwhile.
                if not _wait_until_listed(ctx, caption, self.s.temp_dir):
                    raise PublishError("composer closed but the post never appeared in the activity log "
                                       "within 20 min (upload lost?)")
                if browser is not None:
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
                if browser is not None:
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
            _dismiss_popups(page)
            found = _composer(page).count()
            print("logged in: yes | composer found:", bool(found), "| url:", page.url)
            if not found:
                shot = settings.temp_dir / "facebook_profile_check.png"
                page.screenshot(path=str(shot))
                print("screenshot:", shot)
            return 0 if found else 2
        finally:
            ctx.close()
            if browser is not None:
                browser.close()


def login(settings) -> None:
    """Visible real Chrome (dedicated profile) on the Mac: log in by hand; done when the feed
    has been logged in (c_user cookie, no password box) for 15 s. 15-minute window."""
    import time as _t
    from playwright.sync_api import sync_playwright
    prof = _profile(settings)
    prof.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as pw:
        ctx = pw.chromium.launch_persistent_context(str(prof), headless=False, **_LAUNCH)
        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        page.goto(HOME)
        print("Log in to Facebook in the Chrome window (incl. any 2FA)...", flush=True)
        ok_since, deadline = None, _t.time() + 15 * 60
        while _t.time() < deadline:
            page.wait_for_timeout(3000)
            logged = any(c["name"] == "c_user" for c in ctx.cookies("https://www.facebook.com")) \
                and not WALL.search(page.url) and page.locator("input[name='pass']").count() == 0
            ok_since = (ok_since or _t.time()) if logged else None
            if ok_since and _t.time() - ok_since > 15:
                break
        ctx.close()
        if not ok_since:
            print("No Facebook login detected within 15 minutes", flush=True)
            sys.exit(1)
        print("Facebook logged in; profile saved in", prof, flush=True)


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from config.settings import load_settings
    cmd = sys.argv[1] if len(sys.argv) > 1 else "check"
    s = load_settings()
    if cmd == "login":
        login(s)
    else:
        sys.exit(check(s))
