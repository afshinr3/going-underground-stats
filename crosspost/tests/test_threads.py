"""Threads publisher — added 2026-10-16 on operator request.

No network: the HTTP session is replaced with a recorder. These pin the API SHAPE (container
-> status -> publish -> permalink), the mandatory public video_url, the 500-character limit,
and that a missing token skips Threads instead of breaking a run.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import CAPTION_LIMITS, Settings  # noqa: E402
from publishers import REGISTRY, PublishError  # noqa: E402
from publishers.threads import ThreadsPublisher  # noqa: E402
from transformers.caption import build_caption, cta_for  # noqa: E402


class _Resp:
    def __init__(self, body, status=200):
        self._b, self.status_code, self.text = body, status, str(body)

    def json(self):
        return self._b


class _Http:
    """Records calls and answers the Threads flow in order."""

    def __init__(self):
        self.calls = []
        self._status_calls = 0

    def post(self, url, **kw):
        self.calls.append(("POST", url, kw.get("data") or {}))
        if url.endswith("/threads"):
            return _Resp({"id": "CONTAINER1"})
        if url.endswith("/threads_publish"):
            return _Resp({"id": "MEDIA1"})
        return _Resp({"error": {"message": "unexpected post"}}, 400)

    def get(self, url, **kw):
        self.calls.append(("GET", url, kw.get("params") or {}))
        if url.endswith("CONTAINER1"):
            self._status_calls += 1
            return _Resp({"status": "IN_PROGRESS"} if self._status_calls < 2
                         else {"status": "FINISHED"})
        if url.endswith("MEDIA1"):
            return _Resp({"permalink": "https://www.threads.net/@gu/post/XYZ"})
        return _Resp({"error": {"message": "unexpected get"}}, 400)


def _settings(**over):
    s = Settings()
    s.threads_user_id = over.get("uid", "9999")
    s.threads_access_token = over.get("token", "TOK")
    s.threads_public_media_base = over.get("base", "https://relay.example/public/")
    return s


class Wiring(unittest.TestCase):
    def test_registered_as_a_destination(self):
        self.assertIn("threads", REGISTRY)
        self.assertIs(REGISTRY["threads"], ThreadsPublisher)

    def test_enabled_platforms_includes_threads(self):
        s = _settings()
        s.threads_enabled = True
        self.assertIn("threads", s.enabled_platforms())

    def test_threads_caption_limit_is_500(self):
        self.assertEqual(CAPTION_LIMITS["threads"], 500)

    def test_missing_token_is_a_clean_skip_not_a_crash(self):
        s = _settings(uid="", token="")
        with self.assertRaises(PublishError) as e:
            ThreadsPublisher(s).validate()
        self.assertIn("THREADS_USER_ID", str(e.exception))

    def test_missing_public_base_is_refused_because_there_is_no_byte_upload(self):
        s = _settings(base="")
        with self.assertRaises(PublishError) as e:
            ThreadsPublisher(s).validate()
        self.assertIn("video_url", str(e.exception))


class Flow(unittest.TestCase):
    def setUp(self):
        self.s = _settings()
        self.p = ThreadsPublisher(self.s)
        self.http = _Http()
        self.p.http = self.http
        self.p.wait_until = lambda check, **kw: self.p.__class__.wait_until(
            check, timeout=5, interval=0, what="test")
        self.clip = self.s.temp_dir / "t.mp4"
        self.clip.parent.mkdir(parents=True, exist_ok=True)
        self.clip.write_bytes(b"\0" * 2048)
        self.addCleanup(self.clip.unlink)

    def test_publish_flow_and_permalink(self):
        r = self.p.publish(self.clip, "Clip.\n\nWatch the latest episode of New Order 👇", {})
        self.assertEqual(r.platform, "threads")
        self.assertEqual(r.remote_id, "MEDIA1")
        self.assertEqual(r.url, "https://www.threads.net/@gu/post/XYZ")
        create = next(c for c in self.http.calls if c[0] == "POST" and c[1].endswith("/threads"))
        self.assertEqual(create[2]["media_type"], "VIDEO")
        self.assertTrue(create[2]["video_url"].startswith("https://relay.example/public/"))
        self.assertIn("New Order", create[2]["text"])

    def test_served_copy_is_deleted_afterwards(self):
        self.p.publish(self.clip, "Clip.", {})
        leftovers = list((self.s.temp_dir / "public").glob("*.mp4"))
        self.assertEqual(leftovers, [], "the publicly served copy must not be left behind")

    def test_container_error_raises(self):
        class Bad(_Http):
            def get(self, url, **kw):
                if url.endswith("CONTAINER1"):
                    return _Resp({"status": "ERROR", "error_message": "bad codec"})
                return super().get(url, **kw)
        self.p.http = Bad()
        with self.assertRaises(PublishError) as e:
            self.p.publish(self.clip, "Clip.", {})
        self.assertIn("bad codec", str(e.exception))


class CaptionFit(unittest.TestCase):
    def test_cta_survives_the_500_char_limit(self):
        """The tightest destination: the body is truncated, the show CTA never is."""
        for handle, show in (("Gunderground_tv", "Going Underground"), ("neworder_tv", "New Order")):
            cap = build_caption({"handle": handle, "text": "word " * 400}, "threads")
            self.assertLessEqual(len(cap), 500)
            self.assertIn(f"Watch the latest episode of {show}", cap)
            self.assertTrue(cap.endswith(cta_for(handle).split("\n")[-1]))


if __name__ == "__main__":
    unittest.main(verbosity=2)
