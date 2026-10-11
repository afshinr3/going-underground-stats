"""Central configuration: account mappings, platform limits and credentials.

Everything secret comes from the environment (or a local `.env`, loaded if
python-dotenv is installed). Nothing here should ever be committed with real values.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

try:  # optional dependency; plain env vars work without it
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
except ImportError:  # pragma: no cover
    pass

BASE_DIR = Path(__file__).resolve().parent.parent


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _bool(name: str, default: bool = False) -> bool:
    v = _env(name)
    if not v:
        return default
    return v.lower() in {"1", "true", "yes", "on"}


def _int(name: str, default: int) -> int:
    v = _env(name)
    return int(v) if v else default


# --------------------------------------------------------------------------- sources
@dataclass(frozen=True)
class SourceAccount:
    handle: str          # without "@", case-insensitive on X
    label: str           # human-readable show name
    rumble_url: str      # CTA destination appended to every caption


SOURCE_ACCOUNTS: dict[str, SourceAccount] = {
    "gunderground_tv": SourceAccount(
        handle="Gunderground_tv",
        label="Going Underground",
        rumble_url="https://rumble.com/c/GoingUnderground",
    ),
    "neworder_tv": SourceAccount(
        handle="neworder_tv",
        label="New Order",
        rumble_url="https://rumble.com/c/NewOrderTV",
    ),
}


def source_for(handle: str) -> SourceAccount:
    try:
        return SOURCE_ACCOUNTS[handle.lstrip("@").lower()]
    except KeyError as e:
        raise KeyError(f"Unknown source handle: {handle!r}") from e


# --------------------------------------------------------------------------- limits
# Hard caption/character limits per destination (enforced by transformers.caption.fit_caption).
CAPTION_LIMITS = {
    "telegram": 1024,
    "instagram": 2200,
    "linkedin": 3000,
    "rednote": 1000,
}
REDNOTE_TITLE_LIMIT = 20  # Xiaohongshu note titles are capped at 20 characters


# --------------------------------------------------------------------------- settings
@dataclass
class Settings:
    # runtime
    poll_interval_seconds: int = field(default_factory=lambda: _int("POLL_INTERVAL_SECONDS", 600))
    temp_dir: Path = field(default_factory=lambda: Path(_env("TEMP_DIR") or BASE_DIR / "temp"))
    db_path: Path = field(default_factory=lambda: Path(_env("DB_PATH") or BASE_DIR / "data" / "state.sqlite3"))
    log_level: str = field(default_factory=lambda: _env("LOG_LEVEL", "INFO"))
    log_json: bool = field(default_factory=lambda: _bool("LOG_JSON", True))
    max_attempts: int = field(default_factory=lambda: _int("MAX_PUBLISH_ATTEMPTS", 5))
    # Only consider posts newer than this many hours on first run (avoids back-posting history).
    initial_lookback_hours: int = field(default_factory=lambda: _int("INITIAL_LOOKBACK_HOURS", 6))
    max_video_bytes: int = field(default_factory=lambda: _int("MAX_VIDEO_BYTES", 1024 * 1024 * 1024))

    # X / Twitter
    x_bearer_token: str = field(default_factory=lambda: _env("X_BEARER_TOKEN"))
    x_client_id: str = field(default_factory=lambda: _env("X_CLIENT_ID"))
    x_client_secret: str = field(default_factory=lambda: _env("X_CLIENT_SECRET"))
    x_refresh_token: str = field(default_factory=lambda: _env("X_REFRESH_TOKEN"))
    x_max_results: int = field(default_factory=lambda: _int("X_MAX_RESULTS", 20))
    x_use_ytdlp_fallback: bool = field(default_factory=lambda: _bool("X_USE_YTDLP_FALLBACK", True))

    # Telegram
    telegram_enabled: bool = field(default_factory=lambda: _bool("TELEGRAM_ENABLED", True))
    telegram_bot_token: str = field(default_factory=lambda: _env("TELEGRAM_BOT_TOKEN"))
    telegram_channel_id: str = field(default_factory=lambda: _env("TELEGRAM_CHANNEL_ID"))
    # Point at a self-hosted Bot API server to lift the 50 MB cloud upload cap to 2 GB.
    telegram_api_base: str = field(default_factory=lambda: _env("TELEGRAM_API_BASE", "https://api.telegram.org"))

    # Instagram (Meta Graph API)
    instagram_enabled: bool = field(default_factory=lambda: _bool("INSTAGRAM_ENABLED", True))
    instagram_user_id: str = field(default_factory=lambda: _env("INSTAGRAM_USER_ID"))
    instagram_access_token: str = field(default_factory=lambda: _env("INSTAGRAM_ACCESS_TOKEN"))
    meta_graph_version: str = field(default_factory=lambda: _env("META_GRAPH_VERSION", "v21.0"))
    # graph.facebook.com (Facebook Login, needs a linked FB Page) or
    # graph.instagram.com (Instagram Login, Professional account only, no Page needed)
    instagram_graph_host: str = field(default_factory=lambda: _env("INSTAGRAM_GRAPH_HOST", "graph.instagram.com"))
    meta_app_id: str = field(default_factory=lambda: _env("META_APP_ID"))
    meta_app_secret: str = field(default_factory=lambda: _env("META_APP_SECRET"))
    instagram_share_to_feed: bool = field(default_factory=lambda: _bool("INSTAGRAM_SHARE_TO_FEED", True))

    # LinkedIn
    linkedin_enabled: bool = field(default_factory=lambda: _bool("LINKEDIN_ENABLED", True))
    linkedin_access_token: str = field(default_factory=lambda: _env("LINKEDIN_ACCESS_TOKEN"))
    # urn:li:person:XXXX (personal profile) or urn:li:organization:NNNN (company page)
    linkedin_author_urn: str = field(default_factory=lambda: _env("LINKEDIN_AUTHOR_URN"))
    linkedin_api_version: str = field(default_factory=lambda: _env("LINKEDIN_API_VERSION", "202409"))
    linkedin_token_expires_at: str = field(default_factory=lambda: _env("LINKEDIN_TOKEN_EXPIRES_AT"))  # YYYY-MM-DD
    linkedin_client_id: str = field(default_factory=lambda: _env("LINKEDIN_CLIENT_ID"))
    linkedin_client_secret: str = field(default_factory=lambda: _env("LINKEDIN_CLIENT_SECRET"))

    # Alerts (Telegram DM to the operator) + dead-man's switch
    alert_telegram_chat_id: str = field(default_factory=lambda: _env("ALERT_TELEGRAM_CHAT_ID"))
    alert_repeat_hours: float = field(default_factory=lambda: float(_env("ALERT_REPEAT_HOURS") or 12))
    alert_expiry_days: int = field(default_factory=lambda: _int("ALERT_EXPIRY_DAYS", 7))
    token_check_hours: float = field(default_factory=lambda: float(_env("TOKEN_CHECK_HOURS") or 6))
    stale_gap_hours: float = field(default_factory=lambda: float(_env("STALE_GAP_HOURS") or 3))
    discovery_fail_threshold: int = field(default_factory=lambda: _int("DISCOVERY_FAIL_THRESHOLD", 3))
    healthcheck_url: str = field(default_factory=lambda: _env("HEALTHCHECK_URL"))

    # Rednote / Xiaohongshu
    rednote_enabled: bool = field(default_factory=lambda: _bool("REDNOTE_ENABLED", False))
    rednote_mode: str = field(default_factory=lambda: _env("REDNOTE_MODE", "playwright"))  # playwright | webhook
    rednote_cookies_file: Path = field(
        default_factory=lambda: Path(_env("REDNOTE_COOKIES_FILE") or BASE_DIR / "config" / "rednote_cookies.json")
    )
    rednote_headless: bool = field(default_factory=lambda: _bool("REDNOTE_HEADLESS", True))
    rednote_webhook_url: str = field(default_factory=lambda: _env("REDNOTE_WEBHOOK_URL"))
    rednote_webhook_token: str = field(default_factory=lambda: _env("REDNOTE_WEBHOOK_TOKEN"))

    def enabled_platforms(self) -> list[str]:
        out = []
        if self.telegram_enabled:
            out.append("telegram")
        if self.instagram_enabled:
            out.append("instagram")
        if self.linkedin_enabled:
            out.append("linkedin")
        if self.rednote_enabled:
            out.append("rednote")
        return out


def load_settings() -> Settings:
    s = Settings()
    s.temp_dir.mkdir(parents=True, exist_ok=True)
    s.db_path.parent.mkdir(parents=True, exist_ok=True)
    return s
