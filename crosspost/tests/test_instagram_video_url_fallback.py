"""Instagram: when Meta refuses resumable upload ("video_url is required"), publish via a
temporary public URL, and always delete the served copy afterwards."""
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from publishers.base import PublishError
from publishers.instagram import InstagramPublisher


class Resp:
    def __init__(self, status, body):
        self.status_code, self._b, self.text = status, body, str(body)

    def json(self):
        return self._b


class FakeHTTP:
    def __init__(self, create_error="The parameter video_url is required", fail_url_create=False):
        self.calls, self.create_error, self.fail_url_create, self.served_seen = [], create_error, fail_url_create, None

    def post(self, url, data=None, **kw):
        self.calls.append(("POST", url, dict(data or {}) if isinstance(data, dict) else "<bytes>"))
        if url.endswith("/media") and data.get("upload_type") == "resumable":
            return Resp(400, {"error": {"message": self.create_error}})
        if url.endswith("/media") and "video_url" in data:
            name = data["video_url"].rsplit("/", 1)[1]
            self.served_seen = (self.pub_dir / name).exists()
            if self.fail_url_create:
                return Resp(400, {"error": {"message": "boom"}})
            return Resp(200, {"id": "C1"})
        if url.endswith("/media_publish"):
            return Resp(200, {"id": "M1"})
        raise AssertionError(url)

    def get(self, url, params=None, **kw):
        if params.get("fields") == "status_code,status":
            return Resp(200, {"status_code": "FINISHED"})
        return Resp(200, {"permalink": "https://instagram.com/reel/x"})


def make(tmp, http):
    s = SimpleNamespace(instagram_graph_host="graph.instagram.com", meta_graph_version="v21.0",
                        instagram_user_id="U", instagram_access_token="T", instagram_share_to_feed=True,
                        temp_dir=Path(tmp), instagram_public_media_base="https://example.test/media/")
    p = InstagramPublisher.__new__(InstagramPublisher)
    p.s, p.http = s, http
    http.pub_dir = Path(tmp) / "public"
    return p


class T(unittest.TestCase):
    def _video(self, tmp):
        v = Path(tmp) / "123.mp4"
        v.write_bytes(b"\x00" * 100)
        return v

    def test_falls_back_to_video_url_and_cleans_up(self):
        with tempfile.TemporaryDirectory() as tmp:
            http = FakeHTTP()
            res = make(tmp, http).publish(self._video(tmp), "cap", {})
            self.assertEqual(res.remote_id, "M1")
            self.assertTrue(http.served_seen, "clip must be served while Meta fetches it")
            urls = [c[2].get("video_url") for c in http.calls if isinstance(c[2], dict) and "video_url" in c[2]]
            self.assertEqual(len(urls), 1)
            self.assertTrue(urls[0].startswith("https://example.test/media/") and len(urls[0].rsplit("/", 1)[1]) >= 36)
            self.assertEqual(list((Path(tmp) / "public").iterdir()), [], "served copy must be deleted")

    def test_served_copy_deleted_even_on_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            http = FakeHTTP(fail_url_create=True)
            with self.assertRaises(PublishError):
                make(tmp, http).publish(self._video(tmp), "cap", {})
            self.assertEqual(list((Path(tmp) / "public").iterdir()), [])

    def test_other_errors_are_not_masked(self):
        with tempfile.TemporaryDirectory() as tmp:
            http = FakeHTTP(create_error="Invalid OAuth access token")
            with self.assertRaises(PublishError):
                make(tmp, http).publish(self._video(tmp), "cap", {})
            self.assertFalse(any("video_url" in (c[2] if isinstance(c[2], dict) else {}) for c in http.calls))


if __name__ == "__main__":
    unittest.main()
