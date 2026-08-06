#!/usr/bin/env python3
"""GU_UNKNOWN_IS_NULL_V2 — regression guards for the Maté Instagram-zero defect.

The defect: `GU_NO_QUESTION_MARK_V1` rewrote '?' -> '0' in fetch_and_push.py.
'?' is this pipeline's marker for UNKNOWN, so an unmapped platform shipped as a
hard "0": rendered as a literal 0 and counted as 0 in the episode total, while a
real null on the same episode correctly rendered N/A. Gabor Maté's Instagram was
never measured -- no Instagram content is mapped to that episode at all.

These drive the shipped module and the shipped dashboard source; they do not
re-implement the logic, and they do not assert the operator's verified totals
(those are control data for scope, not fixtures to code against).
"""
import importlib.util
import json
import re
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent


def _mod():
    spec = importlib.util.spec_from_file_location("fetch_and_push", HERE / "fetch_and_push.py")
    m = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(m)
    except SystemExit:
        pass
    return m


class TestExplicitZeroSurvives(unittest.TestCase):
    """A source-confirmed 0 must stay 0. Only unknown becomes null."""

    def test_int_zero_is_a_measurement(self):
        m = _mod()
        self.assertEqual(m.parse_count_opt(0), 0)
        self.assertIsNotNone(m.parse_count_opt(0))

    def test_string_zero_is_a_measurement(self):
        m = _mod()
        self.assertEqual(m.parse_count_opt("0"), 0)

    def test_explicit_zero_contributes_to_total(self):
        m = _mod()
        total, unknown = m.sum_known_metrics(
            {"rumble_views": "0", "x_views": "100", "yt_views": "0", "ig_likes": "0"})
        self.assertEqual(total, 100)
        self.assertEqual(unknown, [])   # nothing unknown -- all four measured


class TestUnknownNeverBecomesZero(unittest.TestCase):
    def test_none_is_unknown(self):
        self.assertIsNone(_mod().parse_count_opt(None))

    def test_question_mark_is_unknown(self):
        self.assertIsNone(_mod().parse_count_opt("?"))

    def test_na_and_error_are_unknown(self):
        m = _mod()
        for v in ("N/A", "NA", "ERR", "ERROR", ""):
            self.assertIsNone(m.parse_count_opt(v), v)

    def test_structured_health_objects_are_unknown(self):
        m = _mod()
        self.assertIsNone(m.parse_count_opt({"status": "N/A", "reason": "no mapping"}))
        self.assertIsNone(m.parse_count_opt({"status": "ERROR", "reason": "429"}))

    def test_malformed_value_is_unknown_not_zero(self):
        m = _mod()
        for v in ("abc", "--", "NaN", "twelve"):
            self.assertIsNone(m.parse_count_opt(v), v)

    def test_unknown_is_excluded_from_total_and_reported(self):
        m = _mod()
        total, unknown = m.sum_known_metrics(
            {"rumble_views": None, "x_views": "50.7K", "yt_views": None, "ig_likes": None})
        self.assertEqual(total, 50_700)
        self.assertEqual(sorted(unknown), ["ig_likes", "rumble_views", "yt_views"])

    def test_scrubber_no_longer_writes_string_zero(self):
        """The exact line that caused the defect."""
        src = (HERE / "fetch_and_push.py").read_text()
        self.assertIn("GU_UNKNOWN_IS_NULL_V2", src)
        self.assertNotIn("_v[_f] = '0'", src)
        self.assertIn("_v[_f] = None", src)


class TestEpisodeFixtures(unittest.TestCase):
    """The three operator-named episodes, as their published records actually are."""

    MATE = {"guest": "Gabor Maté", "rumble_views": None, "x_views": "50.7K",
            "yt_views": None, "ig_likes": None,
            "source_platform_ids": {"youtube": ["PUw_r6rI5PY"]}}
    BARNES = {"guest": "Robert Barnes", "rumble_views": None, "x_views": "101.5K",
              "yt_views": "3.0K", "ig_likes": None,
              "source_platform_ids": {"youtube": ["N_ysv6Gh9Ac"]}}
    BAHAROON = {"guest": "Mohammed Baharoon", "rumble_views": "428", "x_views": "536.1K",
                "yt_views": "83", "ig_likes": "218",
                "source_platform_ids": {"youtube": ["F8hxaEtl9Y8"]}}

    def test_mate_is_partial_not_zero(self):
        total, unknown = _mod().sum_known_metrics(self.MATE)
        self.assertEqual(total, 50_700)
        self.assertIn("ig_likes", unknown)
        self.assertIn("rumble_views", unknown)
        self.assertIn("yt_views", unknown)

    def test_mate_has_no_instagram_mapping_at_all(self):
        """Proves the zero was never a measurement: nothing IG is mapped."""
        self.assertNotIn("instagram", self.MATE["source_platform_ids"])

    def test_barnes_is_partial(self):
        total, unknown = _mod().sum_known_metrics(self.BARNES)
        self.assertEqual(total, 104_500)
        self.assertEqual(sorted(unknown), ["ig_likes", "rumble_views"])

    def test_baharoon_is_fully_measured(self):
        total, unknown = _mod().sum_known_metrics(self.BAHAROON)
        self.assertEqual(unknown, [])
        self.assertEqual(total, 428 + 536_100 + 83 + 218)


class TestPartialTotalIsNotSilent(unittest.TestCase):
    def test_a_partial_total_is_distinguishable_from_a_complete_one(self):
        m = _mod()
        partial, u1 = m.sum_known_metrics({"rumble_views": None, "x_views": "100",
                                           "yt_views": None, "ig_likes": None})
        complete, u2 = m.sum_known_metrics({"rumble_views": "0", "x_views": "100",
                                            "yt_views": "0", "ig_likes": "0"})
        self.assertEqual(partial, complete)      # same number...
        self.assertNotEqual(u1, u2)              # ...but not the same claim
        self.assertTrue(u1 and not u2)


class TestDashboardSource(unittest.TestCase):
    """Guards on docs/index.html — the renderer is the last place a null can be
    flattened back into a 0."""

    def setUp(self):
        self.src = (HERE / "docs" / "index.html").read_text()

    def test_countOpt_exists_and_returns_null_for_unknown(self):
        self.assertIn("function countOpt", self.src)
        self.assertIn("// unparseable is unknown, never 0", self.src)
        self.assertIn("Number.isFinite(n) ? n : null", self.src)

    def test_totals_use_partial_aware_formatter(self):
        self.assertIn("function fmtTotal", self.src)
        self.assertIn("${fmtTotal(e)}", self.src)

    def test_no_silent_empty_array_fallbacks_remain(self):
        self.assertNotIn(".catch(()=>[])", self.src)
        self.assertNotIn(".catch(()=>({drops:[]}))", self.src)

    def test_conflict_markers_are_detected(self):
        self.assertIn("unresolved merge conflict", self.src)

    def test_stale_snapshot_is_surfaced(self):
        self.assertIn("STALE_AFTER_HOURS", self.src)
        self.assertIn("may be stale", self.src)

    def test_source_errors_are_rendered(self):
        self.assertIn("function renderSourceErrors", self.src)
        self.assertIn("not authoritative", self.src)


class TestPublishedDataHasNoConflictMarkers(unittest.TestCase):
    def test_repo_data_files_are_parseable(self):
        for name in ("videos.json", "videos_neworder.json"):
            p = HERE / name
            if not p.exists():
                continue
            txt = p.read_text()
            self.assertFalse(re.search(r"^<{7}|^>{7}|^={7}", txt, re.M),
                             f"{name} contains unresolved conflict markers")
            json.loads(txt)  # must parse


if __name__ == "__main__":
    unittest.main(verbosity=2)
