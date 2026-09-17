#!/usr/bin/env python3
"""GU_X_STORE_ATTRIBUTION_V1 guard — no network, no pushes.

1 the repair map is the STABLE snapshot, never the health feed. Sourcing it from the health
  feed is what broke this mid-fix on 2026-09-18: the feed was regenerated from rows whose
  guest read "Afshin Rattansi CHALLENGES Ex-", the good name was lost, and the next run
  attributed under surname "Ex-" (31 unrelated posts, 831.6K) and "Israel's" (154, 4.4M).
2 a name ending in "on" (Carlson, Pilkington, Ayalon) is NOT treated as a broken guest.
3 a title-fragment guest IS treated as broken.
4 thinly-supported attributions never overwrite an existing measured value.
"""
import json, os, sys, unittest
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import gu_x_store_attribution_v1 as X


class T(unittest.TestCase):
    def test_1_name_map_is_stable_snapshot_not_health_feed(self):
        self.assertTrue(X.NAME_MAP.endswith("gu_guest_names_v1.json"))
        src = open(X.__file__).read()
        fn = src.split("def _guest_carry_forward")[1].split("def ")[0]
        self.assertIn("NAME_MAP", fn)
        self.assertNotIn("HEALTH", fn, "repair map must not read the regenerated health feed")
        m = json.load(open(X.NAME_MAP)).get("by_canonical_episode_id") or {}
        self.assertGreater(len(m), 10)
        self.assertTrue(all("Afshin Rattansi" not in v for v in m.values()))

    def test_2_real_names_not_flagged_broken(self):
        for good in ("Tucker Carlson", "Philip Pilkington", "Ami Ayalon", "Branko Milanović",
                     "John Mearsheimer", "Ken Silva"):
            self.assertFalse(X._looks_broken(good), good)

    def test_3_title_fragments_flagged_broken(self):
        for bad in ("Afshin Rattansi CHALLENGES Ex-", "CMSGT. Dennis Fritz: Israel’s",
                    "of Israel’s Shin Bet Ami Ayalon", ""):
            self.assertTrue(X._looks_broken(bad), bad)

    def test_4_thin_support_threshold_present(self):
        self.assertGreaterEqual(X.MIN_SUPPORT_TWEETS, 3)
        self.assertIn("MIN_SUPPORT", open(X.__file__).read())


if __name__ == "__main__":
    unittest.main(verbosity=2)
