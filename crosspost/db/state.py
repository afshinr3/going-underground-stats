"""SQLite state store.

Two guarantees:
  * a tweet is never published twice to the same platform (per-platform rows), and
  * a partial failure retries only the platforms that failed, up to MAX_PUBLISH_ATTEMPTS.
"""
from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS posts (
    tweet_id     TEXT PRIMARY KEY,
    handle       TEXT NOT NULL,
    created_at   TEXT,
    text         TEXT,
    video_url    TEXT,
    status       TEXT NOT NULL DEFAULT 'pending',   -- pending | done | skipped | failed
    first_seen   TEXT NOT NULL,
    updated_at   TEXT NOT NULL,
    payload      TEXT                                  -- raw normalized post JSON
);
CREATE TABLE IF NOT EXISTS deliveries (
    tweet_id     TEXT NOT NULL,
    platform     TEXT NOT NULL,
    status       TEXT NOT NULL,                        -- success | failed
    attempts     INTEGER NOT NULL DEFAULT 0,
    remote_id    TEXT,
    last_error   TEXT,
    updated_at   TEXT NOT NULL,
    PRIMARY KEY (tweet_id, platform)
);
CREATE TABLE IF NOT EXISTS kv (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class StateStore:
    def __init__(self, path: Path | str):
        self.path = str(path)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(SCHEMA)

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def close(self) -> None:
        self._conn.close()

    # ------------------------------------------------------------------ kv
    def get(self, key: str, default: str | None = None) -> str | None:
        row = self._conn.execute("SELECT value FROM kv WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default

    def set(self, key: str, value: str) -> None:
        with self._tx() as c:
            c.execute(
                "INSERT INTO kv(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

    # ------------------------------------------------------------------ posts
    def is_known(self, tweet_id: str) -> bool:
        return self._conn.execute("SELECT 1 FROM posts WHERE tweet_id=?", (tweet_id,)).fetchone() is not None

    def record_post(self, post: dict, status: str = "pending") -> bool:
        """Insert a newly seen post. Returns False if it was already known."""
        with self._tx() as c:
            cur = c.execute(
                """INSERT OR IGNORE INTO posts
                   (tweet_id, handle, created_at, text, video_url, status, first_seen, updated_at, payload)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (
                    post["tweet_id"],
                    post["handle"],
                    post.get("created_at"),
                    post.get("text"),
                    post.get("video_url"),
                    status,
                    _now(),
                    _now(),
                    json.dumps(post, ensure_ascii=False),
                ),
            )
            return cur.rowcount == 1

    def set_post_status(self, tweet_id: str, status: str) -> None:
        with self._tx() as c:
            c.execute("UPDATE posts SET status=?, updated_at=? WHERE tweet_id=?", (status, _now(), tweet_id))

    def pending_posts(self) -> list[dict]:
        rows = self._conn.execute(
            "SELECT payload FROM posts WHERE status='pending' ORDER BY created_at ASC"
        ).fetchall()
        return [json.loads(r["payload"]) for r in rows]

    # ------------------------------------------------------------------ deliveries
    def delivery(self, tweet_id: str, platform: str) -> dict | None:
        row = self._conn.execute(
            "SELECT * FROM deliveries WHERE tweet_id=? AND platform=?", (tweet_id, platform)
        ).fetchone()
        return dict(row) if row else None

    def platforms_remaining(self, tweet_id: str, platforms: list[str], max_attempts: int) -> list[str]:
        out = []
        for p in platforms:
            d = self.delivery(tweet_id, p)
            if d is None or (d["status"] != "success" and d["attempts"] < max_attempts):
                out.append(p)
        return out

    def platforms_exhausted(self, tweet_id: str, platforms: list[str], max_attempts: int) -> list[str]:
        out = []
        for p in platforms:
            d = self.delivery(tweet_id, p)
            if d and d["status"] != "success" and d["attempts"] >= max_attempts:
                out.append(p)
        return out

    def mark_delivery(
        self, tweet_id: str, platform: str, ok: bool, remote_id: str | None = None, error: str | None = None
    ) -> None:
        with self._tx() as c:
            c.execute(
                """INSERT INTO deliveries(tweet_id, platform, status, attempts, remote_id, last_error, updated_at)
                   VALUES (?,?,?,1,?,?,?)
                   ON CONFLICT(tweet_id, platform) DO UPDATE SET
                     status=excluded.status,
                     attempts=deliveries.attempts+1,
                     remote_id=COALESCE(excluded.remote_id, deliveries.remote_id),
                     last_error=excluded.last_error,
                     updated_at=excluded.updated_at""",
                (tweet_id, platform, "success" if ok else "failed", remote_id, (error or "")[:2000] or None, _now()),
            )
