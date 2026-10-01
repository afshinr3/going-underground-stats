"""NAME_TRUST_IN_MERGE_V1_20261001 + UNCONDITIONAL_COLLAPSE_BEFORE_WRITE_V1_20261001.

The duplicate O'Hanlon row survived in the CLOUD publisher's output on
2026-09-30 20:55 and 2026-10-01 00:36 — 19 rows for 18 episodes — even though
the shared union-identity rule had landed in both writers on 2026-09-30 03:22.
Two defects, both covered here:

1. fetch_and_push.update_show's collapse sits inside a ~190-line try whose
   `except` FAILS OPEN, while the write that follows is outside it. Any
   exception in that block skipped the collapse and published the duplicate.
2. episode_identity_v1._merge kept whichever row came FIRST. Row order is not
   stable (`cached = new_eps + cached`, then a pub_iso sort the poisoned row
   cannot participate in), so the collapse could keep the FRAGMENT — one row,
   confidently wrong, no longer detectable as a duplicate.

These tests call the real functions. test_episode_identity_regression_v1 once
defined its OWN collapse, whose chain happened to merge this very pair, and
stayed green while production shipped the bug — a test that reimplements the
logic tests itself.
"""
import json
import os
import re
import unittest

import episode_identity_v1 as EI

HERE = os.path.dirname(os.path.abspath(__file__))

# The two rows exactly as the cloud published them in videos.json @ 8fcc31e2.
TITLE = ("Afshin Rattansi CHALLENGES Ex-CIA Advisor on the Legacy of "
         "America’s Wars: Success or Failure?")
GOOD = {
    "guest": "Michael O’Hanlon", "surname": "O’Hanlon", "date": "22 Aug",
    "pub_iso": "2026-08-22T11:55:34Z", "canonical_video_id": "Eu0Phb99ipg",
    "canonical_episode_id": "6f96fd46bb55", "title": TITLE,
    "yt_views": "2.2K", "x_views": "590.3K",
}
POISONED = {
    "guest": "Afshin Rattansi CHALLENGES Ex-", "surname": "Ex-", "date": "22 Aug",
    "pub_iso": None, "canonical_video_id": None,
    "canonical_episode_id": "6f96fd46bb55", "title": TITLE,
}


class NameTrustTest(unittest.TestCase):

    def test_the_pair_really_shares_identity(self):
        """If this stops holding, the fixture no longer reproduces the bug."""
        shared = EI.identities(GOOD) & EI.identities(POISONED)
        self.assertIn("canonical_episode_id=6f96fd46bb55", shared)
        self.assertEqual(len(shared), 2, f"expected two shared ids, got {shared}")

    def test_poisoned_row_is_recognised(self):
        self.assertTrue(EI.name_is_poisoned(POISONED))
        self.assertFalse(EI.name_is_poisoned(GOOD))

    def test_collapse_is_order_independent(self):
        """The real defect: which row survived depended on feed order."""
        for order in ([GOOD, POISONED], [POISONED, GOOD]):
            out, merged = EI.collapse([dict(r) for r in order])
            self.assertEqual(len(out), 1)
            self.assertEqual(merged, 1)
            self.assertEqual(out[0]["guest"], "Michael O’Hanlon")
            self.assertEqual(out[0]["surname"], "O’Hanlon")

    def test_merge_keeps_the_good_rows_identifiers_and_metrics(self):
        out, _ = EI.collapse([dict(POISONED), dict(GOOD)])
        self.assertEqual(out[0]["canonical_video_id"], "Eu0Phb99ipg")
        self.assertEqual(out[0]["pub_iso"], "2026-08-22T11:55:34Z")
        self.assertEqual(out[0]["yt_views"], "2.2K")

    def test_poisoned_row_never_fills_a_blank_name(self):
        """A fragment must not become the name just because the good row lacks one."""
        nameless = dict(GOOD, guest="", surname="")
        out, merged = EI.collapse([nameless, dict(POISONED)])
        self.assertEqual(merged, 1)
        self.assertNotIn("CHALLENGES", out[0].get("guest") or "")
        self.assertNotEqual((out[0].get("surname") or "").strip(), "Ex-")

    def test_collapse_is_idempotent(self):
        once, m1 = EI.collapse([dict(POISONED), dict(GOOD)])
        twice, m2 = EI.collapse(once)
        self.assertEqual(m1, 1)
        self.assertEqual(m2, 0, "a second pass must merge nothing")
        self.assertEqual(len(twice), 1)

    def test_no_false_positives_on_this_shows_real_rows(self):
        """Two rules were rejected for flagging correct rows; keep it that way.

        This show titles episodes "Tucker Carlson: We Are on the Brink...", so a
        correct guest is routinely a prefix of its own headline.
        """
        for guest, surname in [
                ("Tucker Carlson", "Carlson"), ("Peter Schiff", "Schiff"),
                ("John Perkins", "Perkins"), ("Michael O’Hanlon", "O’Hanlon"),
                ("Robert Harward", "Harward"), ("Hasan Ünal", "Ünal"),
                ("Gabor Maté", "Maté"), ("Jeffrey Sachs", "Sachs")]:
            row = {"guest": guest, "surname": surname,
                   "title": f"{guest}: Why The West Is FINISHED And What Comes Next"}
            self.assertFalse(EI.name_is_poisoned(row),
                             f"{guest!r} must not be treated as a fragment")

    def test_live_feeds_have_no_poisoned_rows(self):
        for fname in ("videos.json", "videos_neworder.json"):
            path = os.path.join(HERE, fname)
            if not os.path.exists(path):
                continue
            with open(path) as f:
                rows = json.load(f)
            bad = [(r.get("guest"), r.get("surname"))
                   for r in rows if EI.name_is_poisoned(r)]
            self.assertEqual(bad, [], f"{fname} carries fragment names: {bad}")

    def test_role_surname_is_a_fragment(self):
        """Sourced from gu_guest_from_posts_v1._ROLE_WORDS so the two cannot drift."""
        self.assertTrue(EI.name_is_poisoned(
            {"guest": "Deputy Commander", "surname": "Commander", "title": "x"}))


class UnconditionalCollapseTest(unittest.TestCase):
    """The collapse must not sit behind a fail-open guard it does not control."""

    def setUp(self):
        with open(os.path.join(HERE, "fetch_and_push.py")) as f:
            self.src = f.read()

    def test_collapse_runs_after_the_failopen_except(self):
        failopen = self.src.index("except Exception as _e_union:")
        write = self.src.index("with open(show['data_file'], 'w') as f:")
        between = self.src[failopen:write]
        self.assertIn("_EI_pw.collapse(cache)", between,
                      "a collapse must run between the fail-open guard and the write")

    def test_discovery_collapses_before_its_own_write(self):
        i = self.src.index("cached = new_eps + cached")
        j = self.src.index("json.dump(cached, f, indent=2)")
        self.assertIn("_EI_dn.collapse(cached)", self.src[i:j],
                      "discover_new_episodes is the other publisher of this file")

    def test_pre_write_collapse_has_its_own_guard(self):
        """It must not be able to take the write down with it."""
        i = self.src.index("_EI_pw.collapse(cache)")
        tail = self.src[i:i + 900]
        self.assertIn("except Exception as _e_pw", tail)


if __name__ == "__main__":
    unittest.main(verbosity=2)
