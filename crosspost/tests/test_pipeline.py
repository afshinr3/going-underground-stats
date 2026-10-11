"""Stdlib-only tests: python -m unittest discover -s tests (run from crosspost/)."""
from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db.state import StateStore  # noqa: E402
from monitors.x_monitor import XApiMonitor, best_mp4_variant, parse_user_tweets  # noqa: E402
from publishers.linkedin import to_little_text  # noqa: E402
from transformers.caption import (  # noqa: E402
    build_caption, build_title, clean_text, fit_caption, strip_trailing_cta, utf16_len,
)

GU_CTA = "Watch the full interview 👇\nhttps://rumble.com/c/GoingUnderground"
NO_CTA = "Watch the full interview 👇\nhttps://rumble.com/c/NewOrderTV"


class CaptionTests(unittest.TestCase):
    def test_strips_canonical_cta(self):
        t = "Jeffrey Sachs: NATO lied about expansion.\n\nWatch the full interview in the quoted post below. 👇"
        self.assertEqual(strip_trailing_cta(t), "Jeffrey Sachs: NATO lied about expansion.")

    def test_strips_inline_and_variants(self):
        for tail in [
            " Watch the full interview in the quoted post below 👇",
            "\nwatch the FULL interview in the quote post below.👇👇",
            "\n👇 See the full episode below",
            "\nWatch the full interview in the quoted tweet below. ⬇️",
            "\nWatch the full interview here 👇 https://t.co/AbC123",
        ]:
            with self.subTest(tail=tail):
                self.assertEqual(strip_trailing_cta("Big claim." + tail), "Big claim.")

    def test_keeps_hashtags_after_cta(self):
        t = "Gaza ceasefire talks.\nWatch the full interview in the quoted post below. 👇\n#Gaza #Palestine"
        self.assertEqual(strip_trailing_cta(t), "Gaza ceasefire talks.\n#Gaza #Palestine")

    def test_does_not_strip_mid_text_mentions(self):
        t = "He said you should watch the full interview below before judging.\nThen he left."
        self.assertEqual(strip_trailing_cta(t), t)

    def test_expands_links_and_drops_media_and_quote(self):
        raw = "Read more https://t.co/aaa &amp; more\nWatch the full interview in the quoted post below. 👇 https://t.co/qqq https://t.co/mmm"
        urls = [
            {"url": "https://t.co/aaa", "expanded_url": "https://example.com/story"},
            {"url": "https://t.co/qqq", "expanded_url": "https://x.com/Gunderground_tv/status/999"},
            {"url": "https://t.co/mmm", "expanded_url": "https://x.com/Gunderground_tv/status/1/video/1", "media_key": "7_1"},
        ]
        self.assertEqual(clean_text(raw, urls, {"999"}), "Read more https://example.com/story & more")

    def test_build_caption_routes_by_handle(self):
        gu = {"handle": "Gunderground_tv", "text": "Clip.\n\nWatch the full interview in the quoted post below. 👇"}
        no = {"handle": "neworder_tv", "text": "Clip.\n\nWatch the full interview in the quoted post below. 👇"}
        self.assertEqual(build_caption(gu), f"Clip.\n\n{GU_CTA}")
        self.assertEqual(build_caption(no), f"Clip.\n\n{NO_CTA}")

    def test_telegram_truncation_keeps_cta(self):
        post = {"handle": "neworder_tv", "text": ("word " * 400) + "\nWatch the full interview in the quoted post below. 👇"}
        cap = build_caption(post, "telegram")
        self.assertLessEqual(utf16_len(cap), 1024)
        self.assertTrue(cap.endswith(NO_CTA))
        self.assertIn("…", cap)

    def test_fit_caption_noop_when_short(self):
        self.assertEqual(fit_caption("hi", "cta", 100), "hi\n\ncta")

    def test_title(self):
        post = {"handle": "Gunderground_tv", "text": "#BREAKING Sachs on Ukraine. More text here."}
        self.assertEqual(build_title(post), "Sachs on Ukraine.")
        self.assertLessEqual(len(build_title({"handle": "Gunderground_tv", "text": "x" * 50})), 20)


class LinkedInTests(unittest.TestCase):
    def test_little_text(self):
        self.assertEqual(
            to_little_text("Hi (all) @you #Gaza"),
            "Hi \\(all\\) \\@you {hashtag|\\#|Gaza}",
        )


class MonitorTests(unittest.TestCase):
    MEDIA = {
        "3_1": {"media_key": "3_1", "type": "photo"},
        "7_1": {"media_key": "7_1", "type": "video", "variants": [
            {"content_type": "application/x-mpegURL", "url": "https://v/pl.m3u8"},
            {"content_type": "video/mp4", "bit_rate": 832000, "url": "https://v/480.mp4"},
            {"content_type": "video/mp4", "bit_rate": 2176000, "url": "https://v/720.mp4"},
        ]},
        "16_1": {"media_key": "16_1", "type": "animated_gif", "variants": [
            {"content_type": "video/mp4", "bit_rate": 0, "url": "https://v/gif.mp4"}]},
    }

    def norm(self, **t):
        base = {"id": "100", "text": "hello"}
        base.update(t)
        return XApiMonitor._normalise(base, "Gunderground_tv", "42", self.MEDIA)

    def test_best_variant(self):
        self.assertEqual(best_mp4_variant(self.MEDIA["7_1"]), ("https://v/720.mp4", 2176000))

    def test_video_post_accepted(self):
        p = self.norm(attachments={"media_keys": ["7_1"]},
                      referenced_tweets=[{"type": "quoted", "id": "55"}])
        self.assertEqual(p["video_url"], "https://v/720.mp4")
        self.assertEqual(p["quoted_ids"], ["55"])

    def test_filters(self):
        self.assertIsNone(self.norm())                                              # text only
        self.assertIsNone(self.norm(attachments={"media_keys": ["3_1"]}))           # image
        self.assertIsNone(self.norm(attachments={"media_keys": ["16_1"]}))          # gif
        self.assertIsNone(self.norm(attachments={"media_keys": ["7_1"]},
                                    referenced_tweets=[{"type": "retweeted", "id": "1"}]))
        self.assertIsNone(self.norm(attachments={"media_keys": ["7_1"]}, in_reply_to_user_id="9"))
        self.assertIsNotNone(self.norm(attachments={"media_keys": ["7_1"]}, in_reply_to_user_id="42"))

    def test_note_tweet_text_preferred(self):
        p = self.norm(attachments={"media_keys": ["7_1"]}, note_tweet={"text": "long form"})
        self.assertEqual(p["text"], "long form")


def _gql_tweet(tid, screen="Gunderground_tv", media_type="video", **legacy):
    lg = {"id_str": tid, "user_id_str": "42", "full_text": "Clip text https://t.co/med",
          "created_at": "Sun Oct 11 10:00:00 +0000 2026",
          "entities": {"urls": [], "media": [{"url": "https://t.co/med", "expanded_url": "https://x.com/x/status/1/video/1"}]},
          "extended_entities": {"media": [{"type": media_type, "video_info": {"variants": [
              {"content_type": "video/mp4", "bitrate": 256000, "url": "https://v/lo.mp4"},
              {"content_type": "video/mp4", "bitrate": 2176000, "url": "https://v/hi.mp4"}]}}]}}
    lg.update(legacy)
    return {"entryId": f"tweet-{tid}", "content": {"itemContent": {"tweet_results": {"result": {
        "__typename": "Tweet", "rest_id": tid,
        "core": {"user_results": {"result": {"legacy": {"screen_name": screen}}}}, "legacy": lg}}}}}


class CookieMonitorParseTests(unittest.TestCase):
    def test_parse_user_tweets(self):
        wrapped = _gql_tweet("5")
        res = wrapped["content"]["itemContent"]["tweet_results"]["result"]
        wrapped["content"]["itemContent"]["tweet_results"]["result"] = {
            "__typename": "TweetWithVisibilityResults", "tweet": res}
        payload = {"data": {"user": {"result": {"timeline": {"timeline": {"instructions": [{"entries": [
            _gql_tweet("3", quoted_status_id_str="99"),
            _gql_tweet("4", media_type="photo"),
            _gql_tweet("6", screen="someoneelse"),
            _gql_tweet("7", retweeted_status_result={"result": {}}),
            _gql_tweet("8", in_reply_to_user_id_str="77"),
            wrapped,
        ]}]}}}}}}
        posts = parse_user_tweets(payload, "Gunderground_tv")
        self.assertEqual([p["tweet_id"] for p in posts], ["3", "5"])
        self.assertEqual(posts[0]["video_url"], "https://v/hi.mp4")
        self.assertEqual(posts[0]["quoted_ids"], ["99"])
        self.assertTrue(posts[0]["created_at"].startswith("2026-10-11T10:00:00"))
        self.assertEqual(build_caption(posts[0]).split("\n")[0], "Clip text")


class StateTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = StateStore(os.path.join(self.tmp.name, "s.db"))

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def test_dedup_and_retry_accounting(self):
        post = {"tweet_id": "1", "handle": "Gunderground_tv", "text": "x", "created_at": "2026-01-01"}
        self.assertTrue(self.db.record_post(post))
        self.assertFalse(self.db.record_post(post))
        plats = ["telegram", "linkedin"]
        self.db.mark_delivery("1", "telegram", ok=True, remote_id="m1")
        self.db.mark_delivery("1", "linkedin", ok=False, error="boom")
        self.assertEqual(self.db.platforms_remaining("1", plats, 2), ["linkedin"])
        self.db.mark_delivery("1", "linkedin", ok=False, error="boom")
        self.assertEqual(self.db.platforms_remaining("1", plats, 2), [])
        self.assertEqual(self.db.platforms_exhausted("1", plats, 2), ["linkedin"])
        self.assertEqual(self.db.delivery("1", "telegram")["remote_id"], "m1")


if __name__ == "__main__":
    unittest.main()
