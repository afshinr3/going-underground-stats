#!/usr/bin/env python3
"""Guards for GU_CONTENT_INVENTORY_V1."""
import json, unittest
from pathlib import Path
HERE = Path(__file__).resolve().parent
INV = json.loads((HERE / "gu_content_inventory_v1.json").read_text())

class TestInventoryIntegrity(unittest.TestCase):
    def test_no_duplicate_platform_posts(self):
        keys = [(i["platform"], i["platform_content_id"]) for i in INV["items"]]
        self.assertEqual(len(keys), len(set(keys)), "same platform post counted twice")

    def test_every_item_has_required_fields(self):
        req = ("episode_key","platform","publishing_account","canonical_url",
               "platform_content_id","published_iso","classification",
               "classification_basis","metric","metric_scope",
               "measured_at_iso","value","status")
        for i in INV["items"]:
            for f in req:
                self.assertIn(f, i)

    def test_unavailable_is_null_never_zero(self):
        """'unavailable' must carry no number. 'ambiguous' KEEPS its measured
        value on purpose -- it is excluded from totals, not erased."""
        for i in INV["items"]:
            if i["status"] == "unavailable":
                self.assertIsNone(i["value"], i)
            if i["status"] == "measured":
                self.assertIsNotNone(i["value"], i)

    def test_measured_zero_stays_zero_not_null(self):
        for i in INV["items"]:
            if i["status"] == "measured":
                self.assertIsNotNone(i["value"])

    def test_urls_are_derived_from_real_ids(self):
        for i in INV["items"]:
            if i["canonical_url"] is None:
                continue
            if i["platform"] in ("youtube", "x"):
                self.assertIn(i["platform_content_id"], i["canonical_url"])

    def test_only_canonical_accounts(self):
        allowed = {a for m in INV["canonical_accounts"].values() for a in m.values()}
        for i in INV["items"]:
            self.assertIn(i["publishing_account"], allowed, i["publishing_account"])

    def test_at_most_one_full_interview_per_episode_per_platform(self):
        from collections import Counter
        c = Counter((i["episode_key"], i["platform"]) for i in INV["items"]
                    if i["classification"] == "FULL_INTERVIEW")
        for key, n in c.items():
            self.assertEqual(n, 1, f"{key} has {n} full interviews")

    def test_unverified_platform_full_interview_is_NA_not_zero(self):
        for k, e in INV["episodes"].items():
            for plat, v in e["full_interview_by_platform"].items():
                if v["status"] == "N/A":
                    self.assertIsNone(v["value"], f"{k}/{plat} N/A must not be 0")

    def test_promo_excluded_from_editorial_total(self):
        for k, e in INV["episodes"].items():
            b = e["by_classification"]
            expected = (b["FULL_INTERVIEW"]["sum_measured"] or 0) + (b["CLIP"]["sum_measured"] or 0)
            self.assertEqual(e["editorial_total_measured"], expected, k)

    def test_ambiguous_never_enters_published_totals(self):
        for k, e in INV["episodes"].items():
            amb = [i for i in INV["items"] if i["episode_key"] == k
                   and i["classification"] == "AMBIGUOUS" and i["value"] is not None]
            if amb:
                self.assertNotIn(sum(i["value"] for i in amb),
                                 [e["editorial_total_measured"]], k)
                for i in amb:
                    self.assertEqual(i["status"], "ambiguous", i["platform_content_id"])

    def test_every_item_has_a_classification_basis(self):
        for i in INV["items"]:
            self.assertTrue(i["classification_basis"], i["platform_content_id"])
            self.assertIn(i["classification"],
                          ("FULL_INTERVIEW", "CLIP", "PROMO", "AMBIGUOUS"))

    def test_scope_is_cumulative_and_labelled_not_rolling(self):
        self.assertEqual(INV["metric_scope"], "cumulative_to_measurement_timestamp")
        self.assertIn("1 Week", INV["scope_note"])

    def test_word_boundary_matching_excludes_underestimate(self):
        """V1 matched 'mate' inside 'underestimate'."""
        import re
        for i in INV["items"]:
            self.assertNotIn("Mearsheimer", i.get("classification_basis", ""))

    def test_all_three_episodes_present_with_clips(self):  # noqa
        self.assertEqual(len(INV["episodes"]), 3)
        for k, e in INV["episodes"].items():
            self.assertGreater(e["n_items"], 1, f"{k} has no clips")

    def test_partial_flag_is_consistent(self):
        """Partial iff something is ambiguous or a platform full interview is N/A."""
        for k, e in INV["episodes"].items():
            amb = e["by_classification"]["AMBIGUOUS"]["n_items"]
            na = any(v["status"] == "N/A" for v in e["full_interview_by_platform"].values())
            self.assertEqual(e["total_is_partial"], bool(amb or na), k)

if __name__ == "__main__":
    unittest.main(verbosity=2)
