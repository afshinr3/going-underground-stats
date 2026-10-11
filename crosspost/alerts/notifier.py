"""Operator alerts: a Telegram DM to Afshin when something needs a human.

  * alert(key, text): sends once, then at most every ALERT_REPEAT_HOURS while the same
    problem persists (keyed in the state DB, so restarts don't re-spam).
  * resolve(key, text): sends a "fixed" note only if that key had alerted.
  * Optional dead-man's switch: HEALTHCHECK_URL (e.g. a free healthchecks.io check) is
    pinged after every cycle. If the Mac sleeps or the agent dies, healthchecks.io itself
    sends the alert, which a process that isn't running can't do.

Setup: message the bot once from your own Telegram account, then run
    python -m alerts.notifier whoami     # prints your chat id → ALERT_TELEGRAM_CHAT_ID
    python -m alerts.notifier test       # sends a test alert
"""
from __future__ import annotations

import logging
import re
import sys
import time

import requests

from config.settings import Settings
from db.state import StateStore

log = logging.getLogger("alerts")

# Map raw errors to what Afshin should actually do (section numbers refer to CREDENTIALS.md).
_HINTS = [
    (re.compile(r"X login wall|cookies expired", re.I), "X cookies expired: export fresh x.com cookies (CREDENTIALS.md §1)."),
    (re.compile(r"instagram.*(190|OAuthException|expired|Session has)|Error validating access token", re.I | re.S),
     "Instagram token expired or revoked: generate a new one (CREDENTIALS.md §4)."),
    (re.compile(r"linkedin.*\b401\b|REVOKED_ACCESS_TOKEN|EXPIRED_ACCESS_TOKEN", re.I | re.S),
     "LinkedIn token expired: create a new one in the LinkedIn developer portal (CREDENTIALS.md §5)."),
    (re.compile(r"telegram.*(\b401\b|\b403\b|Unauthorized|not enough rights|chat not found)", re.I | re.S),
     "Telegram bot was removed or lost admin rights in the channel (CREDENTIALS.md §2)."),
    (re.compile(r"rednote.*(session expired|login)", re.I | re.S),
     "Rednote logged out: run `python -m publishers.rednote login` (CREDENTIALS.md §6)."),
]


def hint_for(text: str) -> str | None:
    for rx, hint in _HINTS:
        if rx.search(text):
            return hint
    return None


class Notifier:
    def __init__(self, settings: Settings, state: StateStore):
        self.s = settings
        self.state = state

    @property
    def enabled(self) -> bool:
        return bool(self.s.telegram_bot_token and self.s.alert_telegram_chat_id)

    def send(self, text: str) -> bool:
        if not self.enabled:
            log.warning("alert (no ALERT_TELEGRAM_CHAT_ID set)", extra={"text": text})
            return False
        try:
            r = requests.post(
                f"{self.s.telegram_api_base.rstrip('/')}/bot{self.s.telegram_bot_token}/sendMessage",
                data={"chat_id": self.s.alert_telegram_chat_id, "text": text[:4000],
                      "disable_web_page_preview": "true"},
                timeout=20,
            )
            ok = r.ok and r.json().get("ok")
            if not ok:
                log.error("alert send failed", extra={"status": r.status_code, "body": r.text[:200]})
            return bool(ok)
        except Exception as e:  # alerts must never break the pipeline
            log.error("alert send failed", extra={"error": str(e)})
            return False

    def alert(self, key: str, text: str) -> None:
        k = f"alert:{key}"
        last = float(self.state.get(k, "0") or 0)
        if time.time() - last < self.s.alert_repeat_hours * 3600:
            return
        hint = hint_for(text)
        msg = f"⚠️ crosspost: {text}" + (f"\n\n→ {hint}" if hint else "")
        log.warning("alert", extra={"key": key, "text": text})
        if self.send(msg) or not self.enabled:
            self.state.set(k, str(time.time()))

    def resolve(self, key: str, text: str) -> None:
        k = f"alert:{key}"
        if float(self.state.get(k, "0") or 0) > 0:
            self.state.set(k, "0")
            self.send(f"✅ crosspost: {text}")

    def heartbeat(self, ok: bool = True) -> None:
        if not self.s.healthcheck_url:
            return
        try:
            requests.get(self.s.healthcheck_url.rstrip("/") + ("" if ok else "/fail"), timeout=10)
        except Exception as e:
            log.info("healthcheck ping failed", extra={"error": str(e)})


def _whoami(s: Settings) -> None:
    r = requests.get(f"{s.telegram_api_base.rstrip('/')}/bot{s.telegram_bot_token}/getUpdates", timeout=20).json()
    seen = {}
    for u in r.get("result", []):
        chat = (u.get("message") or {}).get("chat") or {}
        if chat.get("type") == "private":
            seen[chat["id"]] = f"@{chat.get('username', '?')} {chat.get('first_name', '')}"
    if not seen:
        print("No private messages yet: send the bot any message from your Telegram, then re-run.")
    for cid, who in seen.items():
        print(f"ALERT_TELEGRAM_CHAT_ID={cid}    # {who}")


if __name__ == "__main__":
    from config.settings import load_settings

    s = load_settings()
    cmd = sys.argv[1:] or ["test"]
    if cmd == ["whoami"]:
        _whoami(s)
    elif cmd == ["test"]:
        ok = Notifier(s, StateStore(":memory:")).send("✅ crosspost test alert: alerts reach you.")
        print("sent" if ok else "NOT sent: check TELEGRAM_BOT_TOKEN and ALERT_TELEGRAM_CHAT_ID")
    else:
        print("usage: python -m alerts.notifier [whoami|test]")
