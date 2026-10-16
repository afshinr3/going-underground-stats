"""Credential upkeep, run every TOKEN_CHECK_HOURS from the main loop.

  Instagram  auto-renewed; the new token is stored in the state DB and overrides .env.
             graph.instagram.com (Instagram Login): ig_refresh_token, no app secret needed.
             graph.facebook.com (Facebook Login): fb_exchange_token, needs META_APP_ID/SECRET;
             without them, expiry is only read (debug_token) so you still get a warning.
  LinkedIn   self-serve apps get no refresh tokens, so this only warns. Expiry comes from
             LINKEDIN_TOKEN_EXPIRES_AT (shown when you create the token), else the
             introspection API (LINKEDIN_CLIENT_ID/SECRET), else first-seen + 60 days.
  X cookies  warns before the auth_token cookie in the export expires.
  Rednote    warns before the saved web_session cookie expires.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from config.settings import Settings
from db.state import StateStore

from .notifier import Notifier

log = logging.getLogger("alerts.tokens")
DAY = 86400


def _h(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()[:12]


def _fmt(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%d %b %Y")


def apply_overrides(s: Settings, state: StateStore) -> None:
    """Use the most recently auto-renewed Instagram token, unless .env holds a newer manual one."""
    stored = state.get("token:instagram")
    env_hash = state.get("token:instagram:env_hash")
    if s.instagram_access_token and _h(s.instagram_access_token) != env_hash:
        # .env changed since we last renewed: the operator pasted a fresh token; adopt it.
        state.set("token:instagram:env_hash", _h(s.instagram_access_token))
        state.set("token:instagram", s.instagram_access_token)
        state.set("token:instagram:expires", "0")
        return
    if stored:
        s.instagram_access_token = stored
    # THREADS_TOKEN_RENEWAL_V1_20261016: same pattern -- a renewed Threads token lives in state; a new .env value wins.
    t_stored = state.get("token:threads")
    t_env_hash = state.get("token:threads:env_hash")
    if s.threads_access_token and _h(s.threads_access_token) != t_env_hash:
        state.set("token:threads:env_hash", _h(s.threads_access_token))
        state.set("token:threads", s.threads_access_token)
        state.set("token:threads:expires", "0")
        return
    if t_stored:
        s.threads_access_token = t_stored


# --------------------------------------------------------------------------- Instagram
def _instagram(s: Settings, state: StateStore) -> float | None:
    """Renew if possible; return expiry epoch (0 = never expires, None = unknown)."""
    tok = s.instagram_access_token
    if not tok:
        return None
    # stored expiry: 0 = unknown yet, -1 = never expires, else epoch seconds
    exp = float(state.get("token:instagram:expires", "0") or 0)
    due = exp == 0 or (exp > 0 and exp - time.time() < 30 * DAY)

    if s.instagram_graph_host == "graph.instagram.com":
        if due:
            r = requests.get("https://graph.instagram.com/refresh_access_token",
                             params={"grant_type": "ig_refresh_token", "access_token": tok}, timeout=30)
            if r.ok and r.json().get("access_token"):
                d = r.json()
                _store_ig(s, state, d["access_token"], time.time() + int(d.get("expires_in", 60 * DAY)))
                log.info("instagram token renewed")
            else:
                log.warning("instagram refresh failed", extra={"status": r.status_code, "body": r.text[:200]})
        return float(state.get("token:instagram:expires", "0") or 0) or None

    # Facebook Login host
    if due and s.meta_app_id and s.meta_app_secret:
        r = requests.get(f"https://graph.facebook.com/{s.meta_graph_version}/oauth/access_token",
                         params={"grant_type": "fb_exchange_token", "client_id": s.meta_app_id,
                                 "client_secret": s.meta_app_secret, "fb_exchange_token": tok}, timeout=30)
        if r.ok and r.json().get("access_token"):
            d = r.json()
            _store_ig(s, state, d["access_token"], time.time() + int(d.get("expires_in", 60 * DAY)))
            log.info("instagram token renewed")
        else:
            log.warning("instagram exchange failed", extra={"status": r.status_code, "body": r.text[:200]})
    # Read the real expiry (works with the token itself for the app's own developer).
    app_tok = f"{s.meta_app_id}|{s.meta_app_secret}" if s.meta_app_id and s.meta_app_secret else s.instagram_access_token
    r = requests.get(f"https://graph.facebook.com/{s.meta_graph_version}/debug_token",
                     params={"input_token": s.instagram_access_token, "access_token": app_tok}, timeout=30)
    if r.ok:
        data = r.json().get("data") or {}
        if not data.get("is_valid", True):
            raise RuntimeError("instagram token is invalid (OAuthException 190)")
        e = float(data.get("expires_at") or 0)
        state.set("token:instagram:expires", str(e or -1))
        return e or None  # 0 from Meta → never expires
    return None


def _store_ig(s: Settings, state: StateStore, token: str, expires: float) -> None:
    s.instagram_access_token = token
    state.set("token:instagram", token)
    state.set("token:instagram:expires", str(expires))


# --------------------------------------------------------------------------- Threads
def _threads(s: Settings, state: StateStore) -> float | None:
    """THREADS_TOKEN_RENEWAL_V1_20261016: long-lived Threads tokens last 60 days and can be refreshed once they are
    >= 24 h old (th_refresh_token, no app secret). Refresh when < 30 days remain or the expiry is not yet known."""
    tok = s.threads_access_token
    if not tok:
        return None
    exp = float(state.get("token:threads:expires", "0") or 0)
    if exp == 0 or exp - time.time() < 30 * DAY:
        r = requests.get("https://graph.threads.net/refresh_access_token",
                         params={"grant_type": "th_refresh_token", "access_token": tok}, timeout=30)
        if r.ok and r.json().get("access_token"):
            d = r.json()
            s.threads_access_token = d["access_token"]
            state.set("token:threads", d["access_token"])
            state.set("token:threads:expires", str(time.time() + int(d.get("expires_in", 60 * DAY))))
            log.info("threads token renewed")
        else:
            log.warning("threads refresh failed", extra={"status": r.status_code, "body": r.text[:200]})
            if exp == 0:   # first sight, refresh not yet allowed (< 24 h old): assume a fresh 60-day token
                key = f"token_seen:threads:{_h(tok)}"
                if not state.get(key):
                    state.set(key, str(time.time()))
                return float(state.get(key)) + 60 * DAY
    return float(state.get("token:threads:expires", "0") or 0) or None


# --------------------------------------------------------------------------- LinkedIn
def _linkedin(s: Settings, state: StateStore) -> float | None:
    tok = s.linkedin_access_token
    if not tok:
        return None
    if s.linkedin_token_expires_at:
        return datetime.fromisoformat(s.linkedin_token_expires_at).replace(tzinfo=timezone.utc).timestamp()
    if s.linkedin_client_id and s.linkedin_client_secret:
        r = requests.post("https://www.linkedin.com/oauth/v2/introspectToken",
                          data={"client_id": s.linkedin_client_id, "client_secret": s.linkedin_client_secret,
                                "token": tok}, timeout=30)
        if r.ok:
            d = r.json()
            if d.get("active") is False:
                raise RuntimeError("linkedin token 401 EXPIRED_ACCESS_TOKEN (introspection: inactive)")
            if d.get("expires_at"):
                return float(d["expires_at"])
    key = f"token_seen:linkedin:{_h(tok)}"
    if not state.get(key):
        state.set(key, str(time.time()))
    return float(state.get(key)) + 60 * DAY


# --------------------------------------------------------------------------- cookie files
def _cookie_expiry(path: Path | None, name: str) -> float | None:
    if not path or not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
    except ValueError:
        return None
    cookies = data.get("cookies", []) if isinstance(data, dict) else data
    for c in cookies if isinstance(cookies, list) else []:
        if isinstance(c, dict) and c.get("name") == name:
            e = c.get("expires", c.get("expirationDate"))
            return float(e) if isinstance(e, (int, float)) and e > 0 else None
    return None


# --------------------------------------------------------------------------- entry point
def maintain(s: Settings, state: StateStore, notifier: Notifier, force: bool = False) -> dict:
    """Renew / check every credential; alert when one expires within ALERT_EXPIRY_DAYS."""
    apply_overrides(s, state)
    last = float(state.get("token_check:last", "0") or 0)
    if not force and time.time() - last < s.token_check_hours * 3600:
        return {}
    state.set("token_check:last", str(time.time()))

    x_file = os.environ.get("X_COOKIES_FILE", "").strip()
    checks = {
        "instagram": (lambda: _instagram(s, state)) if "instagram" in s.enabled_platforms() else None,
        "linkedin": (lambda: _linkedin(s, state)) if "linkedin" in s.enabled_platforms() else None,
        "threads": (lambda: _threads(s, state)) if "threads" in s.enabled_platforms() else None,
        "x_cookies": (lambda: _cookie_expiry(Path(x_file).expanduser(), "auth_token")) if x_file else None,
        "rednote": (lambda: _cookie_expiry(s.rednote_cookies_file, "web_session"))
        if "rednote" in s.enabled_platforms() and s.rednote_mode == "playwright" else None,
    }
    labels = {"instagram": "Instagram token", "linkedin": "LinkedIn token", "threads": "Threads token",
              "x_cookies": "X login cookies", "rednote": "Rednote login"}
    out = {}
    for name, fn in checks.items():
        if fn is None:
            continue
        try:
            exp = fn()
        except Exception as e:
            notifier.alert(f"token:{name}", f"{labels[name]} check failed: {e}")
            continue
        out[name] = exp
        if not exp:  # unknown or never expires
            notifier.resolve(f"expiry:{name}", f"{labels[name]} renewed.")
            continue
        days = (exp - time.time()) / DAY
        if days <= s.alert_expiry_days:
            when = "has EXPIRED" if days <= 0 else f"expires in {days:.0f} day(s) ({_fmt(exp)})"
            notifier.alert(f"expiry:{name}", f"{labels[name]} {when}. Renew it: see CREDENTIALS.md.")
        else:
            notifier.resolve(f"expiry:{name}", f"{labels[name]} renewed, now valid until {_fmt(exp)}.")
    log.info("credential check", extra={k: (_fmt(v) if v else v) for k, v in out.items()})
    return out
