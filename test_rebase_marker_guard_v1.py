"""REBASE_MARKER_GUARD_V1_20261001 — regression test for the 2026-10-01 outage.

The GU LaMetric read "SOURCE UNAVAILABLE" for hours while canonical_gu_pusher_v2
reported http=201 and n_full=0. One transient `port 22: Operation timed out` left
local main divergent; the bridge's unchecked `pull --rebase` then conflicted on
videos_health_v1.json, left CONFLICT MARKERS in it and stopped on a detached HEAD.
The pusher reads that file directly, so json.load raised and _load_episodes
returned (age, [], False) — the fail-closed frame. It repeated every hour because
`checkout -- <file>` on an unmerged path RE-CREATES the markers.

Each test builds a throwaway git repo with a real conflicting rebase and asserts
on behaviour, never on log text.
"""
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest

SRC = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                   "m2_rumble_to_upstream_v1.py")


def _load():
    spec = importlib.util.spec_from_file_location("m2_guard_under_test", SRC)
    m = importlib.util.module_from_spec(spec)
    sys.modules["m2_guard_under_test"] = m
    spec.loader.exec_module(m)
    return m


def _run(repo, *args):
    return subprocess.run(["git", "-C", repo, *args],
                          capture_output=True, text=True)


def _feed(n_eps, iso, rumble=None):
    eps = []
    for i in range(n_eps):
        eps.append({"guest": f"G{i}", "canonical_episode_id": f"id{i}",
                    "pub_iso": "2026-09-01T00:00:00Z",
                    "metrics": {"rumble_views": rumble, "yt_views": "1K",
                                "x_views": "2K", "ig_likes": "3K"}})
    return json.dumps({"iso": iso, "last_updated": iso, "episodes": eps},
                      indent=2) + "\n"


class _Repo:
    """A repo whose local branch and 'cloud' both rewrote the same feed lines."""

    def __init__(self, tmp, filename="videos_health_v1.json"):
        self.path = os.path.join(tmp, "repo")
        self.filename = filename
        os.makedirs(self.path)
        _run(self.path, "init", "-q", "-b", "main")
        _run(self.path, "config", "user.email", "t@t.t")
        _run(self.path, "config", "user.name", "t")
        self.write(_feed(2, "2026-09-30T00:00:00Z"))
        _run(self.path, "add", "-A")
        _run(self.path, "commit", "-q", "-m", "base")
        base = _run(self.path, "rev-parse", "HEAD").stdout.strip()

        # the "cloud" line of history
        _run(self.path, "checkout", "-q", "-b", "cloud")
        self.write(_feed(2, "2026-10-01T00:36:07Z"))
        _run(self.path, "add", "-A")
        _run(self.path, "commit", "-q", "-m", "Update stats 00:36")
        self.cloud = _run(self.path, "rev-parse", "HEAD").stdout.strip()

        # the local bridge commit, touching the same lines
        _run(self.path, "checkout", "-q", "main")
        _run(self.path, "reset", "-q", "--hard", base)
        self.write(_feed(2, "2026-09-30T15:22:17Z", rumble="2.8K"))
        _run(self.path, "add", "-A")
        _run(self.path, "commit", "-q", "-m", "Rumble+IG bridge")

    def write(self, text):
        with open(os.path.join(self.path, self.filename), "w") as f:
            f.write(text)

    def read(self):
        with open(os.path.join(self.path, self.filename)) as f:
            return f.read()

    def start_conflicting_rebase(self):
        r = _run(self.path, "rebase", self.cloud)
        assert r.returncode != 0, "fixture must produce a real conflict"
        return r


class RebaseMarkerGuardTest(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.m = _load()

    def _bind(self, repo):
        self.m.REPO = repo.path

    def test_fixture_really_leaves_markers(self):
        """Guard against a fixture that silently stops reproducing the bug."""
        with tempfile.TemporaryDirectory() as tmp:
            r = _Repo(tmp)
            r.start_conflicting_rebase()
            self.assertIn("<<<<<<<", r.read())
            self._bind(r)
            self.assertTrue(self.m._in_rebase())

    def test_resolver_leaves_no_markers_and_file_parses(self):
        with tempfile.TemporaryDirectory() as tmp:
            r = _Repo(tmp)
            r.start_conflicting_rebase()
            self._bind(r)
            self.assertEqual(self.m._resolve_managed_conflicts(), "resolved")
            self.assertFalse(self.m._in_rebase())
            self.assertNotIn("<<<<<<<", r.read())
            json.loads(r.read())                      # must parse

    def test_resolver_reattaches_head(self):
        with tempfile.TemporaryDirectory() as tmp:
            r = _Repo(tmp)
            r.start_conflicting_rebase()
            self._bind(r)
            self.m._resolve_managed_conflicts()
            self.m._ensure_on_branch()
            self.assertEqual(
                _run(r.path, "symbolic-ref", "-q", "HEAD").returncode, 0,
                "a detached HEAD makes every push fail")

    def test_safe_discard_refuses_on_unmerged_path(self):
        """A churn-revert on an unmerged path fails silently; the guard says so.

        Measured on git 2.50.1: `git checkout -- <unmerged>` does NOT rewrite
        markers, it refuses with rc=1 and leaves the file untouched. _git()
        ignores returncode, so the intended discard simply never happened and
        the log was silent about it.
        """
        with tempfile.TemporaryDirectory() as tmp:
            r = _Repo(tmp)
            r.start_conflicting_rebase()
            self._bind(r)
            r.write(_feed(2, "clean"))
            raw = _run(r.path, "checkout", "--", r.filename)
            self.assertNotEqual(raw.returncode, 0,
                                "git must refuse an unmerged checkout")
            self.assertIn("unmerged", (raw.stderr or "").lower())
            self.assertIn("clean", r.read(), "the file must be left untouched")
            # the guard declines up front rather than issuing a doomed command
            self.assertFalse(self.m._safe_discard(
                os.path.join(r.path, r.filename)))
            self.assertNotIn("<<<<<<<", r.read())

    def test_safe_discard_works_on_a_merged_path(self):
        """The guard must not break the churn suppression it wraps."""
        with tempfile.TemporaryDirectory() as tmp:
            r = _Repo(tmp)
            self._bind(r)
            committed = r.read()
            r.write(_feed(2, "9999-01-01T00:00:00Z"))
            self.assertTrue(self.m._safe_discard(
                os.path.join(r.path, r.filename)))
            self.assertEqual(r.read(), committed)

    def test_verify_no_markers_repairs_a_broken_feed(self):
        with tempfile.TemporaryDirectory() as tmp:
            r = _Repo(tmp)
            r.start_conflicting_rebase()
            self._bind(r)
            self.m._resolve_managed_conflicts()
            # simulate any writer leaving a torn file behind
            r.write("{\n<<<<<<< HEAD\n  \"iso\": 1\n=======\n  \"iso\": 2\n>>>>>>> x\n}\n")
            self.assertEqual(self.m._verify_no_markers(), [])
            self.assertNotIn("<<<<<<<", r.read())
            json.loads(r.read())

    def test_verify_reports_what_it_cannot_fix(self):
        """A feed git has never seen cannot be restored — say so, don't pass."""
        with tempfile.TemporaryDirectory() as tmp:
            r = _Repo(tmp, filename="videos_health_v1.json")
            self._bind(r)
            with open(os.path.join(r.path, "stats_1week_gu.json"), "w") as f:
                f.write("{ torn")                     # untracked + unparseable
            self.assertIn("stats_1week_gu.json", self.m._verify_no_markers())

    def test_unowned_conflict_is_not_guessed_at(self):
        with tempfile.TemporaryDirectory() as tmp:
            r = _Repo(tmp, filename="some_other_file.json")
            r.start_conflicting_rebase()
            self._bind(r)
            self.assertEqual(self.m._resolve_managed_conflicts(), "foreign")
            self.assertTrue(self.m._in_rebase(), "a fresh unowned rebase is left alone")

    def test_clean_repo_is_a_noop(self):
        with tempfile.TemporaryDirectory() as tmp:
            r = _Repo(tmp)
            self._bind(r)
            self.assertEqual(self.m._resolve_managed_conflicts(), "clean")
            self.assertEqual(self.m._verify_no_markers(), [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
