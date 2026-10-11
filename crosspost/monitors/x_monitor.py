"""X (Twitter) extraction: find new posts with native video and download the best MP4.

Two discovery back-ends, selected by env:
  * XApiMonitor (default): official X API v2 user timeline. Needs X_BEARER_TOKEN
    (app-only, never expires) or X_CLIENT_ID + X_REFRESH_TOKEN (OAuth 2.0 user
    context, refreshed automatically and the rotated refresh token persisted to the
    state DB). Honours x-rate-limit-reset on HTTP 429.
  * RssMonitor: any RSS feed of the account (self-hosted RSSHub/Nitter), set
    X_RSS_URL_TEMPLATE="https://rsshub.example.com/twitter/user/{handle}". Each item is
    probed with yt-dlp to decide whether it carries a native video.

Every post is normalised to the dict shape consumed by the rest of the pipeline:
  {tweet_id, handle, created_at, text, urls, quoted_ids, video_url, video_bitrate, permalink}
"""
from __future__ import annotations

import base64
import json
import logging
import os
import re
import shutil
import subprocess
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

from config.settings import SOURCE_ACCOUNTS, Settings
from db.state import StateStore

log = logging.getLogger("monitor.x")

API = "https://api.x.com/2"
TOKEN_URL = "https://api.x.com/2/oauth2/token"
UA = "gu-crosspost/1.0"


class XAuthError(RuntimeError):
    pass


def best_mp4_variant(media: dict) -> tuple[str, int] | None:
    """Highest-bitrate MP4 variant of a video media object (HLS .m3u8 variants ignored)."""
    mp4s = [
        v for v in media.get("variants") or []
        if v.get("content_type") == "video/mp4" and v.get("url")
    ]
    if not mp4s:
        return None
    best = max(mp4s, key=lambda v: v.get("bit_rate") or 0)
    return best["url"], int(best.get("bit_rate") or 0)


# =========================================================================== X API v2
class XApiMonitor:
    def __init__(self, settings: Settings, state: StateStore):
        self.s = settings
        self.state = state
        self.session = requests.Session()
        self.session.headers["User-Agent"] = UA
        self._access_token: str | None = None

    # ------------------------------------------------------------------ auth
    def _token(self) -> str:
        if self.s.x_client_id and (self.state.get("x_refresh_token") or self.s.x_refresh_token):
            if not self._access_token or time.time() >= float(self.state.get("x_access_expiry", "0")):
                self._refresh()
            return self._access_token  # type: ignore[return-value]
        if self.s.x_bearer_token:
            return self.s.x_bearer_token
        raise XAuthError("Set X_BEARER_TOKEN, or X_CLIENT_ID + X_REFRESH_TOKEN, or X_RSS_URL_TEMPLATE")

    def _refresh(self) -> None:
        refresh = self.state.get("x_refresh_token") or self.s.x_refresh_token
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {"grant_type": "refresh_token", "refresh_token": refresh, "client_id": self.s.x_client_id}
        if self.s.x_client_secret:  # confidential client → HTTP Basic
            basic = base64.b64encode(f"{self.s.x_client_id}:{self.s.x_client_secret}".encode()).decode()
            headers["Authorization"] = f"Basic {basic}"
        r = requests.post(TOKEN_URL, data=data, headers=headers, timeout=30)
        if r.status_code != 200:
            raise XAuthError(f"X token refresh failed: {r.status_code} {r.text[:300]}")
        tok = r.json()
        self._access_token = tok["access_token"]
        self.state.set("x_access_expiry", str(time.time() + int(tok.get("expires_in", 7200)) - 120))
        if tok.get("refresh_token"):  # X rotates refresh tokens; the old one is now dead
            self.state.set("x_refresh_token", tok["refresh_token"])
        log.info("x token refreshed")

    # ------------------------------------------------------------------ http
    def _get(self, path: str, params: dict | None = None, _retry: int = 0) -> dict:
        r = self.session.get(
            f"{API}{path}", params=params, timeout=30, headers={"Authorization": f"Bearer {self._token()}"}
        )
        if r.status_code == 429:
            reset = int(r.headers.get("x-rate-limit-reset", time.time() + 60))
            wait = max(5, min(reset - time.time() + 2, 15 * 60))
            log.warning("x rate limited; sleeping", extra={"seconds": int(wait), "path": path})
            time.sleep(wait)
            return self._get(path, params, _retry)
        if r.status_code == 401 and _retry == 0 and self.s.x_client_id:
            self.state.set("x_access_expiry", "0")
            self._access_token = None
            return self._get(path, params, 1)
        if r.status_code >= 500 and _retry < 3:
            time.sleep(2 ** (_retry + 1))
            return self._get(path, params, _retry + 1)
        if r.status_code != 200:
            raise RuntimeError(f"X API {path} -> {r.status_code}: {r.text[:500]}")
        remaining = r.headers.get("x-rate-limit-remaining")
        if remaining is not None and int(remaining) <= 1:
            log.info("x rate limit nearly exhausted", extra={"path": path, "reset": r.headers.get("x-rate-limit-reset")})
        return r.json()

    def _user_id(self, handle: str) -> str:
        key = f"x_user_id:{handle.lower()}"
        cached = self.state.get(key)
        if cached:
            return cached
        uid = self._get(f"/users/by/username/{handle}")["data"]["id"]
        self.state.set(key, uid)
        return uid

    # ------------------------------------------------------------------ discovery
    def fetch_new(self, handle: str) -> list[dict]:
        uid = self._user_id(handle)
        since_key = f"x_since_id:{handle.lower()}"
        params = {
            "max_results": max(5, min(self.s.x_max_results, 100)),
            "exclude": "retweets",
            "expansions": "attachments.media_keys,referenced_tweets.id",
            "media.fields": "type,variants,duration_ms,preview_image_url",
            "tweet.fields": "created_at,entities,note_tweet,referenced_tweets,attachments,in_reply_to_user_id",
        }
        since = self.state.get(since_key)
        if since:
            params["since_id"] = since
        else:
            start = datetime.now(timezone.utc) - timedelta(hours=self.s.initial_lookback_hours)
            params["start_time"] = start.strftime("%Y-%m-%dT%H:%M:%SZ")

        data = self._get(f"/users/{uid}/tweets", params)
        tweets = data.get("data") or []
        if tweets:
            self.state.set(since_key, max(tweets, key=lambda t: int(t["id"]))["id"])
        media_by_key = {m["media_key"]: m for m in (data.get("includes") or {}).get("media", [])}

        out = []
        for t in sorted(tweets, key=lambda t: int(t["id"])):
            post = self._normalise(t, handle, uid, media_by_key)
            if post:
                out.append(post)
        return out

    @staticmethod
    def _normalise(t: dict, handle: str, uid: str, media_by_key: dict) -> dict | None:
        refs = t.get("referenced_tweets") or []
        if any(r["type"] == "retweeted" for r in refs):
            return None
        # Replies to *other* accounts are conversation, not content; self-threads are fine.
        if t.get("in_reply_to_user_id") and t["in_reply_to_user_id"] != uid:
            return None
        videos = [
            media_by_key[k] for k in (t.get("attachments") or {}).get("media_keys", [])
            if media_by_key.get(k, {}).get("type") == "video"   # native video only; GIFs excluded
        ]
        if not videos:
            return None
        variant = best_mp4_variant(videos[0])
        if not variant:
            return None
        note = t.get("note_tweet") or {}
        text = note.get("text") or t.get("text", "")
        entities = note.get("entities") or t.get("entities") or {}
        # media URLs live in the base entities even for note tweets
        urls = list(entities.get("urls") or []) + [
            u for u in (t.get("entities") or {}).get("urls", []) if u.get("media_key")
        ]
        return {
            "tweet_id": t["id"],
            "handle": handle,
            "created_at": t.get("created_at"),
            "text": text,
            "urls": urls,
            "quoted_ids": [r["id"] for r in refs if r["type"] == "quoted"],
            "video_url": variant[0],
            "video_bitrate": variant[1],
            "permalink": f"https://x.com/{handle}/status/{t['id']}",
        }


# =========================================================================== RSS + yt-dlp
class RssMonitor:
    STATUS_RE = re.compile(r"(?:twitter|x)\.com/([^/]+)/status/(\d+)", re.I)

    def __init__(self, settings: Settings, state: StateStore, template: str):
        self.s = settings
        self.state = state
        self.template = template

    def fetch_new(self, handle: str) -> list[dict]:
        r = requests.get(self.template.format(handle=handle), timeout=30, headers={"User-Agent": UA})
        r.raise_for_status()
        root = ET.fromstring(r.content)
        out = []
        for item in root.iter("item"):
            link = (item.findtext("link") or "") + " " + (item.findtext("guid") or "")
            m = self.STATUS_RE.search(link)
            if not m or m.group(1).lower() != handle.lower():  # retweets carry another author
                continue
            tweet_id = m.group(2)
            if self.state.is_known(tweet_id):
                continue
            info = ytdlp_info(f"https://x.com/{handle}/status/{tweet_id}")
            if not info or not _ytdlp_has_native_video(info):
                self.state.record_post({"tweet_id": tweet_id, "handle": handle}, status="skipped")
                continue
            out.append({
                "tweet_id": tweet_id,
                "handle": handle,
                "created_at": datetime.fromtimestamp(info.get("timestamp") or time.time(), timezone.utc).isoformat(),
                "text": info.get("description") or info.get("title") or "",
                "urls": [],
                "quoted_ids": [],
                "video_url": None,  # resolved by yt-dlp at download time
                "video_bitrate": 0,
                "permalink": f"https://x.com/{handle}/status/{tweet_id}",
            })
        return out


def _ytdlp_has_native_video(info: dict) -> bool:
    entries = info.get("entries") or [info]
    return any(
        (e.get("vcodec") not in (None, "none") or any(f.get("vcodec") not in (None, "none") for f in e.get("formats") or []))
        and not e.get("is_gif")
        for e in entries
    )


def ytdlp_info(url: str) -> dict | None:
    if not shutil.which("yt-dlp"):
        log.error("yt-dlp not installed")
        return None
    p = subprocess.run(["yt-dlp", "-J", "--no-warnings", url], capture_output=True, text=True, timeout=120)
    if p.returncode != 0:
        log.info("yt-dlp found no media", extra={"url": url, "stderr": p.stderr[-300:]})
        return None
    return json.loads(p.stdout)


# =========================================================================== download
def download_video(post: dict, settings: Settings) -> Path:
    """Download the highest-resolution MP4 to TEMP_DIR/<tweet_id>.mp4."""
    dest = settings.temp_dir / f"{post['tweet_id']}.mp4"
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    tmp = dest.with_suffix(".part")
    if post.get("video_url"):
        try:
            with requests.get(post["video_url"], stream=True, timeout=60, headers={"User-Agent": UA}) as r:
                r.raise_for_status()
                size = 0
                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(1024 * 1024):
                        size += len(chunk)
                        if size > settings.max_video_bytes:
                            raise RuntimeError(f"video exceeds MAX_VIDEO_BYTES ({settings.max_video_bytes})")
                        f.write(chunk)
            os.replace(tmp, dest)
            return dest
        except Exception as e:
            tmp.unlink(missing_ok=True)
            if not settings.x_use_ytdlp_fallback:
                raise
            log.warning("direct download failed; trying yt-dlp", extra={"tweet_id": post["tweet_id"], "error": str(e)})
    if not shutil.which("yt-dlp"):
        raise RuntimeError("no video_url and yt-dlp is not installed")
    p = subprocess.run(
        ["yt-dlp", "--no-warnings", "--no-playlist", "-f", "bv*[ext=mp4]+ba[ext=m4a]/b[ext=mp4]/bv*+ba/b",
         "--merge-output-format", "mp4", "-o", str(dest), post["permalink"]],
        capture_output=True, text=True, timeout=900,
    )
    if p.returncode != 0 or not dest.exists():
        raise RuntimeError(f"yt-dlp download failed: {p.stderr[-500:]}")
    return dest


def build_monitor(settings: Settings, state: StateStore):
    template = os.environ.get("X_RSS_URL_TEMPLATE", "").strip()
    if template and not (settings.x_bearer_token or settings.x_refresh_token):
        return RssMonitor(settings, state, template)
    return XApiMonitor(settings, state)


def handles() -> list[str]:
    return [a.handle for a in SOURCE_ACCOUNTS.values()]
