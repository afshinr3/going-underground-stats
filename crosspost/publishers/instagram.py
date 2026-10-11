"""Instagram Reels publisher (Instagram Graph API, Content Publishing).

Flow — uses *resumable upload* so no public video URL / hosting is needed:
  1. POST /{ig-user-id}/media  media_type=REELS upload_type=resumable caption=...
     → container id + upload uri
  2. POST https://rupload.facebook.com/ig-api-upload/{ver}/{container-id}  (raw bytes)
  3. poll GET /{container-id}?fields=status_code until FINISHED (ERROR → fail)
  4. POST /{ig-user-id}/media_publish  creation_id={container-id}

Requires an Instagram Professional (Business/Creator) account and a long-lived token:
  * INSTAGRAM_GRAPH_HOST=graph.instagram.com — "Instagram API with Instagram Login",
    scopes instagram_business_basic + instagram_business_content_publish; no FB Page needed.
  * INSTAGRAM_GRAPH_HOST=graph.facebook.com — Facebook Login, account linked to a Page,
    scopes instagram_basic + instagram_content_publish.
alerts/tokens.py renews the token automatically before its 60 days run out.
Reels must be 3s–15min, 9:16 recommended; Meta rejects other specs at step 3.
"""
from __future__ import annotations

from pathlib import Path

from .base import PublishError, Publisher, PublishResult


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

        # 1. create container
        r = self.http.post(
            f"{self.graph}/{uid}/media",
            data={
                "media_type": "REELS",
                "upload_type": "resumable",
                "caption": caption,
                "share_to_feed": "true" if self.s.instagram_share_to_feed else "false",
                "access_token": token,
            },
            timeout=60,
        )
        container_id = self._check(r, "create container")["id"]

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

        # 3. wait for processing
        def status():
            s = self._check(
                self.http.get(
                    f"{self.graph}/{container_id}",
                    params={"fields": "status_code,status", "access_token": token},
                    timeout=30,
                ),
                "container status",
            )
            code = s.get("status_code")
            if code == "ERROR" or code == "EXPIRED":
                raise PublishError(f"container {code}: {s.get('status')}")
            return code == "FINISHED"

        self.wait_until(status, timeout=900, interval=10, what="instagram video processing")

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
