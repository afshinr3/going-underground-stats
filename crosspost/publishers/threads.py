"""Threads publisher (Threads API, graph.threads.net).

Added 2026-10-16 on operator request ("it should cross post on threads").

WHY THIS LOOKS LIKE instagram.py's FALLBACK PATH AND NOT ITS MAIN PATH.
Instagram tries a resumable byte upload first and only falls back to a public `video_url`.
The Threads API has **no resumable upload at all** -- a container is created from a URL Meta
fetches itself -- so the public-serving route is the ONLY route here, not a fallback. That is
the same machinery instagram.py already hardened on this host, so it is REUSED rather than
rewritten (`_shrink`, `SERVE_MAX_BYTES`): Meta fetches through the M2's tunnel at ~0.2 MB/s and
abandons after ~2 min, so anything over ~26 MB is re-encoded to 720p first. The X original is
never modified and the served copy is deleted afterwards whatever happens.

Flow:
  1. POST /{threads-user-id}/threads  media_type=VIDEO video_url=... text=...  -> creation id
  2. poll GET /{creation-id}?fields=status,error_message until FINISHED (ERROR/EXPIRED -> fail)
  3. POST /{threads-user-id}/threads_publish  creation_id=...
  4. GET /{media-id}?fields=permalink

Credentials (a Threads token is NOT an Instagram token, even on the same Meta app):
  THREADS_USER_ID, THREADS_ACCESS_TOKEN, scopes `threads_basic` + `threads_content_publish`.

TEXT LIMIT IS 500 CHARACTERS -- by far the tightest destination here (Telegram 1024, IG 2200).
`fit_caption` truncates the body and never the CTA, so the show CTA always survives.

NOT YET VALIDATED AGAINST A LIVE TOKEN. Built from the documented API shape; no Threads
credentials exist on this host yet, so `validate()` makes the relay skip Threads with one
config alert instead of failing a run. The first real publish is the proof.
"""
from __future__ import annotations

import secrets
import shutil
import time
from pathlib import Path

from .base import PublishError, Publisher, PublishResult
from .instagram import SERVE_MAX_BYTES, _shrink


class ThreadsPublisher(Publisher):
    name = "threads"

    @property
    def graph(self) -> str:
        return f"https://{self.s.threads_graph_host}/{self.s.threads_api_version}"

    def validate(self) -> None:
        if not (self.s.threads_user_id and self.s.threads_access_token):
            raise PublishError("THREADS_USER_ID and THREADS_ACCESS_TOKEN are required")
        if not self.s.threads_public_media_base:
            raise PublishError(
                "THREADS_PUBLIC_MEDIA_BASE (or INSTAGRAM_PUBLIC_MEDIA_BASE) is required: the "
                "Threads API has no byte upload and can only fetch a public video_url")

    def _check(self, r, what: str) -> dict:
        try:
            body = r.json()
        except ValueError:
            body = {}
        if r.status_code >= 400 or "error" in body:
            err = body.get("error", {}) if isinstance(body, dict) else {}
            raise PublishError(f"{what}: {r.status_code} {err.get('message') or r.text[:300]}")
        return body

    def publish(self, video: Path, caption: str, post: dict) -> PublishResult:
        token = self.s.threads_access_token
        uid = self.s.threads_user_id

        public = self.s.temp_dir / "public"
        public.mkdir(parents=True, exist_ok=True)
        name = secrets.token_urlsafe(32) + ".mp4"
        served = public / name
        if video.stat().st_size > SERVE_MAX_BYTES:
            _shrink(video, served)
        else:
            shutil.copyfile(video, served)
        try:
            r = self.http.post(
                f"{self.graph}/{uid}/threads",
                data={
                    "media_type": "VIDEO",
                    "video_url": self.s.threads_public_media_base + name,
                    "text": caption,
                    "access_token": token,
                },
                timeout=60,
            )
            creation_id = self._check(r, "create threads container")["id"]
            return self._finish(creation_id, uid, token)
        finally:
            served.unlink(missing_ok=True)

    def _finish(self, creation_id: str, uid: str, token: str) -> PublishResult:
        # Polled every 30 s, not faster: on 2026-10-12 a 10 s poll exhausted this account's
        # hourly Meta Graph quota and failed uploads that had already finished. A quota error
        # means "ask again later", never "failed".
        def status():
            try:
                s = self._check(
                    self.http.get(
                        f"{self.graph}/{creation_id}",
                        params={"fields": "status,error_message", "access_token": token},
                        timeout=30,
                    ),
                    "container status",
                )
            except PublishError as e:
                if "request limit" in str(e).lower():
                    time.sleep(120)
                    return False
                raise
            code = str(s.get("status") or "").upper()
            if code in {"ERROR", "EXPIRED"}:
                raise PublishError(f"container {code}: {s.get('error_message')}")
            return code in {"FINISHED", "PUBLISHED"}

        self.wait_until(status, timeout=1800, interval=30, what="threads video processing")

        r = self.http.post(
            f"{self.graph}/{uid}/threads_publish",
            data={"creation_id": creation_id, "access_token": token},
            timeout=60,
        )
        media_id = self._check(r, "threads_publish")["id"]
        link = None
        try:
            link = self._check(
                self.http.get(f"{self.graph}/{media_id}",
                              params={"fields": "permalink", "access_token": token}, timeout=30),
                "permalink",
            ).get("permalink")
        except PublishError:
            pass
        return PublishResult(self.name, media_id, link)
