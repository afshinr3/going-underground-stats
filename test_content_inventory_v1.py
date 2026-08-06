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
               "platform_content_id","published_iso","content_type","metric",
               "measured_at_iso","value","status")
        for i in INV["items"]:
            for f in req:
                self.assertIn(f, i)

    def test_unavailable_is_null_never_zero(self):
        for i in INV["items"]:
            if i["status"] != "measured":
                self.assertIsNone(i["value"], i)

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

    def test_exactly_one_full_interview_per_episode(self):
        for k, e in INV["episodes"].items():
            self.assertEqual(e["by_content_type"]["full_interview"]["n_items"], 1, k)

    def test_all_three_episodes_present_with_clips(self):
        self.assertEqual(len(INV["episodes"]), 3)
        for k, e in INV["episodes"].items():
            self.assertGreater(e["n_items"], 1, f"{k} has no clips")

    def test_partial_flag_is_consistent(self):
        for k, e in INV["episodes"].items():
            self.assertEqual(e["total_is_partial"], e["n_unavailable"] > 0, k)

if __name__ == "__main__":
    unittest.main(verbosity=2)
