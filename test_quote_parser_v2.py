#!/usr/bin/env python3
"""Regression guards for GU_QUOTE_PARSER_V2 (Baharoon quoted-dialogue defect).

Old rule: [‘'"“]([^’'"”]{40,})[’'"”] — closing-quote anchored. It failed the whole
neworder_TV set for two independent reasons: the stored text is TRUNCATED so no
closing quote exists, and [^'] forbade apostrophes inside the quote ("It's").
"""
import json, sys, unittest
from pathlib import Path
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import gu_content_inventory_v1 as G

BAHAROON = ("Mohammed Baharoon: The world is moving from a POLAR order to a NETWORKED order\n\n"
            "'It's again resilience of supply routes, but I think the bilateral relationships "
            "cannot achieve their full potential if they remain bilateral. There h")
BARNES_DASH = ("‘The only person who could compete with Lindsey Graham for causing MASS DEATH "
               "around the world is ANTHONY FAUCI.’ \n\n—Trump’s Former Lawyer Robert Barnes")
BARNES_PREFIX = ("Donald Trump’s Former Lawyer Robert Barnes:\n\n‘The Deep State is DEEPLY "
                 "INTERTWINED with the Israel Lobby.\n\nAnd that's a great success of the Israel Lo")
MATE = ("Holocaust Survivor & Trauma Specialist Dr. Gabor Maté:\n\n‘Benjamin Netanyahu is one of "
        "the most dishonest, brutal, hypocritical politicians ever to tread the earth.’")
TRAILER = ("SATURDAY’S GOING UNDERGROUND:\n\nWe’re joined by Donald Trump’s former lawyer "
           "Robert Barnes. \n\nWhat is behind Donald Trump continuing to push on with his")
SLOGAN = ("Going Underground: ‘Subscribe to our channel and watch the full interview right now "
          "today for more of this’")


class TestQuoteDetection(unittest.TestCase):
    def test_straight_quote_with_apostrophe_and_truncation(self):
        """The exact defect: straight quote, contraction, no closing quote."""
        ok, ev = G.quoted_dialogue(BAHAROON)
        self.assertTrue(ok, "truncated straight-quote dialogue must be detected")
        self.assertIn("truncation-tolerant", ev)

    def test_curly_quotes(self):
        self.assertTrue(G.quoted_dialogue(MATE)[0])

    def test_dash_attribution_after_quote(self):
        self.assertTrue(G.quoted_dialogue(BARNES_DASH)[0])

    def test_speaker_prefix_before_quote(self):
        self.assertTrue(G.quoted_dialogue(BARNES_PREFIX)[0])

    def test_multiline_quotation(self):
        self.assertTrue(G.quoted_dialogue(BARNES_PREFIX)[0])  # contains \n\n inside

    def test_trailer_copy_is_not_dialogue(self):
        self.assertFalse(G.quoted_dialogue(TRAILER)[0])

    def test_quoted_promotional_slogan_is_not_dialogue(self):
        self.assertFalse(G.quoted_dialogue(SLOGAN)[0])

    def test_unattributed_quote_is_not_dialogue(self):
        self.assertFalse(G.quoted_dialogue(
            "‘Some long sentence of text with no attribution anywhere at all here’")[0])

    def test_short_quote_is_not_substantive(self):
        self.assertFalse(G.quoted_dialogue("Robert Barnes: ‘Too short.’")[0])

    def test_malformed_and_empty(self):
        for t in (None, "", "‘’", ":::", "\x00\x01"):
            self.assertFalse(G.quoted_dialogue(t)[0], repr(t))

    def test_unicode_normalisation_applied(self):
        self.assertEqual(G.normalise_text("A B​C"), "A BC")
        self.assertIn("fi", G.normalise_text("ﬁn"))  # NFKC ligature


class TestSecondSignalRequired(unittest.TestCase):
    """Quotation alone must never publish a CLIP."""

    def test_text_only_path_never_emits_clip(self):
        cls, _ = G.classify_x(BAHAROON)
        self.assertEqual(cls, "AMBIGUOUS")

    def test_excerpt_video_without_quote_is_ambiguous(self):
        G.X_EVIDENCE["TEST_NOQUOTE"] = {"native_video": True, "n_video": 1,
                                        "duration_min": 2.0, "is_repost": False, "is_quote": False}
        self.assertEqual(G.classify_x_verified("TEST_NOQUOTE", "no dialogue here")[0], "AMBIGUOUS")

    def test_quote_plus_excerpt_video_is_clip(self):
        G.X_EVIDENCE["TEST_CLIP"] = {"native_video": True, "n_video": 1, "duration_min": 2.0,
                                     "is_repost": False, "is_quote": False}
        self.assertEqual(G.classify_x_verified("TEST_CLIP", BAHAROON)[0], "CLIP")

    def test_no_native_video_is_promo(self):
        G.X_EVIDENCE["TEST_NOVID"] = {"native_video": False, "n_video": 0, "duration_min": None,
                                      "is_repost": False, "is_quote": False}
        self.assertEqual(G.classify_x_verified("TEST_NOVID", BAHAROON)[0], "PROMO")

    def test_repost_is_promo_regardless_of_quote(self):
        G.X_EVIDENCE["TEST_RT"] = {"native_video": True, "n_video": 1, "duration_min": 2.0,
                                   "is_repost": True, "is_quote": False}
        self.assertEqual(G.classify_x_verified("TEST_RT", BAHAROON)[0], "PROMO")

    def test_full_episode_duration_is_full_interview(self):
        G.X_EVIDENCE["TEST_FULL"] = {"native_video": True, "n_video": 1, "duration_min": 27.5,
                                     "is_repost": False, "is_quote": False}
        self.assertEqual(G.classify_x_verified("TEST_FULL", "anything")[0], "FULL_INTERVIEW")


class TestNoBroadRuleAndNoHardcoding(unittest.TestCase):
    def test_no_baharoon_ids_hardcoded_in_classifier(self):
        src = (HERE / "gu_content_inventory_v1.py").read_text()
        for tid in ("2083531586033877006", "2083870718773891314", "2083950310092054556"):
            self.assertNotIn(tid, src, "content IDs must not be hardcoded")

    def test_earlier_regression_cannot_recur(self):
        """The reverted attempt collapsed clips by mis-anchoring. Pin the shape."""
        inv = json.loads((HERE / "gu_content_inventory_v1.json").read_text())
        eps = inv["episodes"]
        self.assertGreater(eps["mate_20260803"]["by_classification"]["CLIP"]["sum_measured"], 500_000)
        self.assertGreater(eps["barnes_20260801"]["by_classification"]["CLIP"]["sum_measured"], 1_000_000)
        self.assertGreater(eps["baharoon_20260802"]["by_classification"]["CLIP"]["sum_measured"], 500_000)

    def test_control_totals_are_not_in_the_source(self):
        src = (HERE / "gu_content_inventory_v1.py").read_text()
        for n in ("710261", "668191", "784401", "1327819", "617558"):
            self.assertNotIn(n, src)


if __name__ == "__main__":
    unittest.main(verbosity=2)
