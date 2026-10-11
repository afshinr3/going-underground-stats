"""LinkedIn video post publisher (Community Management / Posts API, versioned `rest/`).

Flow:
  1. POST /rest/videos?action=initializeUpload  → upload instructions (byte ranges), video URN
  2. PUT each byte range to its uploadUrl, collecting the ETag of each part
  3. POST /rest/videos?action=finalizeUpload  with the ETags
  4. poll GET /rest/videos/{urn} until status == AVAILABLE
  5. POST /rest/posts  with content.media.id = video URN

Token scopes: w_member_social (person author) or w_organization_social (organization
author, token holder must be a page admin). Tokens last 60 days — refresh before then.
"""
from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import quote

from transformers.caption import build_title

from .base import PublishError, Publisher, PublishResult

API = "https://api.linkedin.com/rest"

# Characters with meaning in LinkedIn's "little text" commentary format.
_RESERVED = re.compile(r"([\\|{}@\[\]()<>#*_~])")
_HASHTAG = re.compile(r"(?<![\w\\])#(\w+)")


def to_little_text(text: str) -> str:
    """Escape reserved chars but keep #hashtags as real hashtags."""
    placeholders: dict[str, str] = {}

    def _keep(m: re.Match) -> str:
        key = f"\x00{len(placeholders)}\x00"
        placeholders[key] = "{hashtag|\\#|" + m.group(1) + "}"
        return key

    text = _HASHTAG.sub(_keep, text)
    text = _RESERVED.sub(r"\\\1", text)
    for k, v in placeholders.items():
        text = text.replace(k, v)
    return text


class LinkedInPublisher(Publisher):
    name = "linkedin"

    def validate(self) -> None:
        if not (self.s.linkedin_access_token and self.s.linkedin_author_urn):
            raise PublishError("LINKEDIN_ACCESS_TOKEN and LINKEDIN_AUTHOR_URN are required")

    def _headers(self, json_body: bool = True) -> dict:
        h = {
            "Authorization": f"Bearer {self.s.linkedin_access_token}",
            "LinkedIn-Version": self.s.linkedin_api_version,
            "X-Restli-Protocol-Version": "2.0.0",
        }
        if json_body:
            h["Content-Type"] = "application/json"
        return h

    def _check(self, r, what: str):
        if r.status_code >= 400:
            raise PublishError(f"{what}: {r.status_code} {r.text[:400]}")
        return r

    def publish(self, video: Path, caption: str, post: dict) -> PublishResult:
        size = video.stat().st_size
        owner = self.s.linkedin_author_urn

        # 1. initialize
        r = self._check(
            self.http.post(
                f"{API}/videos?action=initializeUpload",
                headers=self._headers(),
                json={"initializeUploadRequest": {
                    "owner": owner, "fileSizeBytes": size, "uploadCaptions": False, "uploadThumbnail": False,
                }},
                timeout=60,
            ),
            "initializeUpload",
        )
        val = r.json()["value"]
        video_urn = val["video"]
        upload_token = val.get("uploadToken", "")

        # 2. upload parts (LinkedIn splits into ~4 MB ranges)
        etags = []
        with open(video, "rb") as f:
            for part in val["uploadInstructions"]:
                first, last = int(part["firstByte"]), int(part["lastByte"])
                f.seek(first)
                chunk = f.read(last - first + 1)
                pr = self._check(
                    self.http.put(
                        part["uploadUrl"],
                        data=chunk,
                        headers={"Authorization": f"Bearer {self.s.linkedin_access_token}",
                                 "Content-Type": "application/octet-stream"},
                        timeout=300,
                    ),
                    f"upload part {first}-{last}",
                )
                etag = pr.headers.get("etag") or pr.headers.get("ETag")
                if not etag:
                    raise PublishError("upload part returned no ETag")
                etags.append(etag)

        # 3. finalize
        self._check(
            self.http.post(
                f"{API}/videos?action=finalizeUpload",
                headers=self._headers(),
                json={"finalizeUploadRequest": {
                    "video": video_urn, "uploadToken": upload_token, "uploadedPartIds": etags,
                }},
                timeout=60,
            ),
            "finalizeUpload",
        )

        # 4. wait until AVAILABLE
        def ready():
            st = self._check(
                self.http.get(f"{API}/videos/{quote(video_urn, safe='')}", headers=self._headers(False), timeout=30),
                "video status",
            ).json().get("status")
            if st == "PROCESSING_FAILED":
                raise PublishError("LinkedIn video processing failed")
            return st == "AVAILABLE"

        self.wait_until(ready, timeout=900, interval=10, what="linkedin video processing")

        # 5. create post
        r = self._check(
            self.http.post(
                f"{API}/posts",
                headers=self._headers(),
                json={
                    "author": owner,
                    "commentary": to_little_text(caption),
                    "visibility": "PUBLIC",
                    "distribution": {
                        "feedDistribution": "MAIN_FEED",
                        "targetEntities": [],
                        "thirdPartyDistributionChannels": [],
                    },
                    "content": {"media": {"title": build_title(post, limit=200), "id": video_urn}},
                    "lifecycleState": "PUBLISHED",
                    "isReshareDisabledByAuthor": False,
                },
                timeout=60,
            ),
            "create post",
        )
        post_urn = r.headers.get("x-restli-id") or r.headers.get("x-linkedin-id")
        link = f"https://www.linkedin.com/feed/update/{post_urn}/" if post_urn else None
        return PublishResult(self.name, post_urn, link)
