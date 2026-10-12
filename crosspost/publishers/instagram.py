"""Instagram Reels publisher (Instagram Graph API, Content Publishing).

Flow — uses *resumable upload* so no public video URL / hosting is needed:
  1. POST /{ig-user-id}/media  media_type=REELS upload_type=resumable caption=...
     → container id + upload uri
  2. POST https://rupload.facebook.com/ig-api-upload/{ver}/{container-id}  (raw bytes)
  3. poll GET /{container-id}?fields=status_code until FINISHED (ERROR → fail)
  4. POST /{ig-user-id}/media_publish  creation_id={container-id}

VIDEO_URL FALLBACK (2026-10-12): on graph.instagram.com this account answers step 1 with
"The parameter video_url is required" on every Graph version (v21-v24 probed), so resumable
upload never worked here -- 0 of 4 Reels published. When Meta says that, the clip is copied to
temp/public/<random 43 chars>.mp4, which the M2 relay serves publicly at
INSTAGRAM_PUBLIC_MEDIA_BASE for the few minutes Meta needs to fetch it, the container is
created with video_url, and the file is deleted afterwards whatever happens.

Requires an Instagram Professional (Business/Creator) account and a long-lived token:
  * INSTAGRAM_GRAPH_HOST=graph.instagram.com — "Instagram API with Instagram Login",
    scopes instagram_business_basic + instagram_business_content_publish; no FB Page needed.
  * INSTAGRAM_GRAPH_HOST=graph.facebook.com — Facebook Login, account linked to a Page,
    scopes instagram_basic + instagram_content_publish.
alerts/tokens.py renews the token automatically before its 60 days run out.
Reels must be 3s–15min, 9:16 recommended; Meta rejects other specs at step 3.
"""
from __future__ import annotations

import secrets
import time
import shutil
from pathlib import Path

from .base import PublishError, Publisher, PublishResult


SERVE_MAX_BYTES = 20 * 1024 * 1024


def _shrink(src: Path, dest: Path) -> None:
    """720p H.264 at <=1 Mbit/s video + 96 kbit/s AAC, faststart, keeping the aspect ratio."""
    import subprocess
    ffmpeg = shutil.which("ffmpeg") or "/opt/homebrew/bin/ffmpeg"
    cmd = [ffmpeg, "-y", "-loglevel", "error", "-i", str(src),
           "-vf", "scale='if(gt(iw,ih),-2,720)':'if(gt(iw,ih),720,-2)'",
           "-c:v", "libx264", "-preset", "veryfast", "-crf", "26", "-maxrate", "1000k", "-bufsize", "2000k",
           "-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a", "96k", "-movflags", "+faststart", str(dest)]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
    if r.returncode != 0 or not dest.exists() or dest.stat().st_size == 0:
        dest.unlink(missing_ok=True)
        raise PublishError(f"could not shrink {src.name} for Instagram: {r.stderr[-300:]}")


class InstagramPublisher(Publisher):
    name = "instagram"

    @property
    def graph(self) -> str:
        return f"https://{self.s.instagram_graph_host}/{self.s.meta_graph_version}"

    def validate(self) -> None:
        if not (self.s.instagram_user_id and self.s.instagram_access_token):
            raise PublishError("INSTAGRAM_USER_ID and INSTAGRAM_ACCESS_TOKEN are required")

    def _check(self, r, what: str) -> dict:
        try:
            body = r.json()
        except ValueError:
            body = {}
        if r.status_code >= 400 or "error" in body:
            err = body.get("error", {})
            raise PublishError(f"{what}: {r.status_code} {err.get('message') or r.text[:300]}")
        return body

    def publish(self, video: Path, caption: str, post: dict) -> PublishResult:
        token = self.s.instagram_access_token
        uid = self.s.instagram_user_id

        base = {
            "media_type": "REELS",
            "caption": caption,
            "share_to_feed": "true" if self.s.instagram_share_to_feed else "false",
            "access_token": token,
        }
        # 1. create container (resumable first)
        r = self.http.post(f"{self.graph}/{uid}/media", data=dict(base, upload_type="resumable"), timeout=60)
        try:
            container_id = self._check(r, "create container")["id"]
        except PublishError as e:
            if "video_url is required" not in str(e):
                raise
            return self._publish_via_url(video, base, uid, token)

        # 2. upload bytes
        size = video.stat().st_size
        with open(video, "rb") as f:
            r = self.http.post(
                f"https://rupload.facebook.com/ig-api-upload/{self.s.meta_graph_version}/{container_id}",
                headers={"Authorization": f"OAuth {token}", "offset": "0", "file_size": str(size)},
                data=f,
                timeout=900,
            )
        up = self._check(r, "upload")
        if up.get("success") is False:
            raise PublishError(f"upload rejected: {up}")

        return self._finish(container_id, uid, token)

    def _publish_via_url(self, video: Path, base: dict, uid: str, token: str) -> PublishResult:
        public = self.s.temp_dir / "public"
        public.mkdir(parents=True, exist_ok=True)
        name = secrets.token_urlsafe(32) + ".mp4"
        served = public / name
        # Meta fetches video_url through the M2's Funnel at ~0.2 MB/s (measured 2026-10-12 under
        # host load) and abandons the fetch after ~2 min: every clip <=26 MB published, the 40 and
        # 45 MB ones came back "container ERROR". Large clips are re-encoded to 720p at a capped
        # bitrate first; the X original is untouched and small clips are served as-is.
        if video.stat().st_size > SERVE_MAX_BYTES:
            _shrink(video, served)
        else:
            shutil.copyfile(video, served)
        try:
            r = self.http.post(
                f"{self.graph}/{uid}/media",
                data=dict(base, video_url=self.s.instagram_public_media_base + name),
                timeout=60,
            )
            container_id = self._check(r, "create container (video_url)")["id"]
            return self._finish(container_id, uid, token)
        finally:
            served.unlink(missing_ok=True)

    def _finish(self, container_id: str, uid: str, token: str) -> PublishResult:
        # 3. wait for processing. Polled every 30 s, and a quota error ("Application request
        # limit reached") only means "ask again later": on 2026-10-12 polling every 10 s across
        # several clips exhausted the account's hourly Graph quota and failed finished uploads.
        def status():
            try:
                s = self._check(
                    self.http.get(
                        f"{self.graph}/{container_id}",
                        params={"fields": "status_code,status", "access_token": token},
                        timeout=30,
                    ),
                    "container status",
                )
            except PublishError as e:
                if "request limit" in str(e).lower():
                    time.sleep(120)
                    return False
                raise
            code = s.get("status_code")
            if code == "ERROR" or code == "EXPIRED":
                raise PublishError(f"container {code}: {s.get('status')}")
            return code == "FINISHED"

        self.wait_until(status, timeout=1800, interval=30, what="instagram video processing")

        # 4. publish
        r = self.http.post(
            f"{self.graph}/{uid}/media_publish",
            data={"creation_id": container_id, "access_token": token},
            timeout=60,
        )
        media_id = self._check(r, "media_publish")["id"]
        link = None
        try:
            link = self._check(
                self.http.get(f"{self.graph}/{media_id}", params={"fields": "permalink", "access_token": token}, timeout=30),
                "permalink",
            ).get("permalink")
        except PublishError:
            pass
        return PublishResult(self.name, media_id, link)
