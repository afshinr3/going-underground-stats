#!/usr/bin/env python3
"""EPISODE_CARRY_FORWARD_V1 guard — no network, no pushes.

The defect: on 2026-09-18 a cloud run published 15 GU rows while the previously published
set had 18. Milanović (15 Aug), Hasan Ünal (7 Aug) and James Carden (11 Jul) were live on
YouTube and simply missing from that run, so the LaMetric guest frames skipped them.
"""
import json, os, sys, tempfile, unittest
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import merge_measured_fields_v1 as M


def _row(sn, date, vid=None, **kw):
    r = {"surname": sn, "guest": sn, "date": date, "canonical_video_id": vid,
         "canonical_episode_id": (vid or sn) + "_cid", "x_views": "100.0K",
         "rumble_views": "1.0K", "ig_likes": None}
    r.update(kw)
    return r


class T(unittest.TestCase):
    def setUp(self):
        # last successfully published set: 18 rows
        self.published = [_row(f"Guest{i}", f"{i} Aug", vid=f"v{i}") for i in range(1, 19)]
        # this run discovered only 15 of them — rows 16,17,18 are missing
        self.cloud = [dict(r) for r in self.published[:15]]

    def test_1_omitted_episode_survives_the_publish(self):
        merged, n = M.carry_forward_rows(self.cloud, self.published)
        self.assertEqual(n, 3)
        self.assertEqual(len(merged), 18)
        for sn in ("Guest16", "Guest17", "Guest18"):
            self.assertIn(sn, [r["surname"] for r in merged], f"{sn} disappeared")
        self.assertTrue(all(r.get("_carried_forward_v1")
                            for r in merged if r["surname"] in ("Guest16", "Guest17", "Guest18")))

    def test_2_fresh_data_wins_for_episodes_in_both_sets(self):
        self.cloud[0]["x_views"] = "999.9K"
        merged, _ = M.carry_forward_rows(self.cloud, self.published)
        row = [r for r in merged if r["surname"] == "Guest1"][0]
        self.assertEqual(row["x_views"], "999.9K")
        self.assertEqual(len([r for r in merged if r["surname"] == "Guest1"]), 1)

    def test_3_tombstone_is_the_only_removal(self):
        tombs = {"v18_cid"}
        merged, n = M.carry_forward_rows(self.cloud, self.published, tombs)
        self.assertEqual(n, 2)
        self.assertNotIn("Guest18", [r["surname"] for r in merged])

    def test_4_matches_by_name_when_video_id_is_absent(self):
        pub = [_row("Milanović", "15 Aug", vid=None)]
        merged, n = M.carry_forward_rows([], pub)
        self.assertEqual((n, merged[0]["surname"]), (1, "Milanović"))
        merged2, n2 = M.carry_forward_rows([_row("Milanović", "15 Aug", vid=None)], pub)
        self.assertEqual((n2, len(merged2)), (0, 1))

    def test_5_field_merge_still_runs(self):
        cloud = [_row("Guest1", "1 Aug", vid="v1", rumble_views=None)]
        pub = [_row("Guest1", "1 Aug", vid="v1", rumble_views="4.6K")]
        merged, n = M.merge_rows(cloud, pub)
        self.assertEqual((merged[0]["rumble_views"], n), ("4.6K", 1))

    def test_6_newest_first_ordering(self):
        pub = [_row("Old", "1 Jul", vid="vO", pub_iso="2026-07-01T00:00:00Z")]
        cloud = [_row("New", "15 Sep", vid="vN", pub_iso="2026-09-15T00:00:00Z")]
        merged, _ = M.carry_forward_rows(cloud, pub)
        self.assertEqual([r["surname"] for r in merged], ["New", "Old"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
