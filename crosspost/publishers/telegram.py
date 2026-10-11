"""Telegram channel publisher (Bot API `sendVideo`).

The bot must be an admin of the channel with "Post messages" rights. Captions are
already fitted to 1,024 chars by the transformer. The public Bot API caps uploads at
50 MB; set TELEGRAM_API_BASE to a self-hosted telegram-bot-api server for up to 2 GB.
"""
from __future__ import annotations

import subprocess
import shutil
from pathlib import Path

from .base import PublishError, Publisher, PublishResult

CLOUD_LIMIT = 50 * 1024 * 1024


def _probe(video: Path) -> dict:
    """width/height/duration via ffprobe, so Telegram shows the right aspect ratio."""
    if not shutil.which("ffprobe"):
        return {}
    p = subprocess.run(
        ["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries",
         "stream=width,height:format=duration", "-of", "default=nw=1", str(video)],
        capture_output=True, text=True, timeout=60,
    )
    out = dict(line.split("=", 1) for line in p.stdout.splitlines() if "=" in line)
    meta = {}
    for k in ("width", "height"):
        if out.get(k, "").isdigit():
            meta[k] = int(out[k])
    try:
        meta["duration"] = int(float(out["duration"]))
    except (KeyError, ValueError):
        pass
    return meta


class TelegramPublisher(Publisher):
    name = "telegram"

    def validate(self) -> None:
        if not (self.s.telegram_bot_token and self.s.telegram_channel_id):
            raise PublishError("TELEGRAM_BOT_TOKEN and TELEGRAM_CHANNEL_ID are required")

    def publish(self, video: Path, caption: str, post: dict) -> PublishResult:
        size = video.stat().st_size
        if size > CLOUD_LIMIT and "api.telegram.org" in self.s.telegram_api_base:
            raise PublishError(
                f"video is {size / 1e6:.1f} MB; cloud Bot API limit is 50 MB — set TELEGRAM_API_BASE "
                "to a local telegram-bot-api server"
            )
        url = f"{self.s.telegram_api_base.rstrip('/')}/bot{self.s.telegram_bot_token}/sendVideo"
        data = {
            "chat_id": self.s.telegram_channel_id,
            "caption": caption,
            "supports_streaming": "true",
            **{k: str(v) for k, v in _probe(video).items()},
        }
        with open(video, "rb") as f:
            r = self.http.post(url, data=data, files={"video": (video.name, f, "video/mp4")}, timeout=900)
        body = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
        if not body.get("ok"):
            raise PublishError(f"sendVideo failed: {r.status_code} {body.get('description') or r.text[:300]}")
        msg = body["result"]
        chat = msg.get("chat", {})
        link = f"https://t.me/{chat['username']}/{msg['message_id']}" if chat.get("username") else None
        return PublishResult(self.name, str(msg["message_id"]), link)
