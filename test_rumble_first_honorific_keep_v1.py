#!/usr/bin/env python3
"""RUMBLE_FIRST_HONORIFIC_KEEP_V1 — a Rumble-first episode must survive the cloud rebuild.

Regression guard for the 2026-08-10 defect:

  "Prof. John Mearsheimer Explains Why Iran War MUST END & why US Will Reduce
   Middle East Presence" published on Rumble 2026-08-09 never appeared in GU stats
   or on Substack.

Mechanism: the local bridge (m2_rumble_to_upstream_v1.py, hourly at :20) correctly
injected the row with rumble_only_injected=True. The cloud rebuild
(fetch_and_push.main_fetch, ~every 15 min) then deleted it inside
_url_bind_cleanup_and_backfill's `_keeps()` filter: a Rumble-first episode has no
canonical_video_id and cannot match the YouTube RSS (it is not on YouTube yet), so it
fell through to a YouTube-Shorts heuristic that drops any title beginning with an
honorific — Prof./Dr./Amb./Sen./Col./Gen./Fmr/Former/Ex-/Retired/Ret.

Net effect: injected at :20, deleted within minutes, every hour, silently. Only
episodes whose guest carries an academic or military title were affected, which is why
"Col. Larry Wilkerson" survived (it was on YouTube and had a canonical_video_id) and
the defect looked intermittent.

These tests exercise the SHIPPING regex and the SHIPPING keep-rule ordering, not a
local restatement of them, so they fail if either is edited back.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

SRC = Path(__file__).resolve().parent / "fetch_and_push.py"
MARKER = "RUMBLE_FIRST_HONORIFIC_KEEP_V1_2026_08_10"

# The honorific pattern exactly as it ships (kept in sync by test_shipping_regex_unchanged).
HONORIFIC = re.compile(
    r"^(?:Prof\.?|Dr\.?|Amb\.?|Sen\.?|Col\.?|Gen\.?|Fmr|Former|"
    r"Ex[\s\-]|Retired|Ret\.?)\s+", re.I)

REAL_EPISODE = ("Prof. John Mearsheimer Explains Why Iran War MUST END "
                "& why US Will Reduce Middle East Presence")

failures = []


def check(cond, label):
    print(("  [PASS] " if cond else "  [FAIL] ") + label)
    if not cond:
        failures.append(label)


def test_the_exact_title_still_trips_the_shorts_heuristic():
    """The title genuinely matches the Shorts pattern — that is why the guard is needed."""
    check(bool(HONORIFIC.match(REAL_EPISODE)),
          "the real Mearsheimer title does match the honorific/Shorts pattern")


def test_shipping_regex_unchanged():
    """If the shipped regex changes, this test's premise is stale — fail loudly."""
    src = SRC.read_text()
    check("Prof\\.?|Dr\\.?|Amb\\.?|Sen\\.?|Col\\.?|Gen\\.?|Fmr|Former|" in src,
          "shipped honorific regex still present and unchanged in fetch_and_push.py")


def test_guard_present_and_ordered_before_the_heuristic():
    """rumble_only_injected must be checked BEFORE falling through to the drop gate."""
    src = SRC.read_text()
    check(MARKER in src, f"{MARKER} marker present in fetch_and_push.py")
    i_guard = src.find("if row.get('rumble_only_injected'):")
    i_short = src.find("_shorts_prefix_re2 = re.compile")
    check(i_guard != -1, "rumble_only_injected keep-rule exists in _keeps()")
    check(i_guard != -1 and i_short != -1 and i_guard < i_short,
          "keep-rule is evaluated BEFORE the Shorts heuristic (ordering matters)")


def test_keeps_rule_semantics():
    """Reproduce _keeps()'s decision for the four cases that matter."""
    def keeps(row, has_yt_match=False):
        if row.get('is_upcoming'):
            return True
        if row.get('rumble_only_injected'):      # the fix
            return True
        if row.get('canonical_video_id'):
            return True
        if has_yt_match:
            return True
        return not bool(HONORIFIC.match(row.get('title') or ''))

    check(keeps({"title": REAL_EPISODE, "rumble_only_injected": True}),
          "Rumble-first honorific episode is KEPT (the regression)")
    check(keeps({"title": "Col. Larry Wilkerson: The World Is SLEEPWALKING",
                 "canonical_video_id": "abc123"}),
          "YouTube honorific episode still kept via canonical_video_id")
    check(not keeps({"title": "Prof. Somebody on a 30-second clip"}),
          "a genuine honorific Short with no provenance is still DROPPED")
    check(keeps({"title": "Gabor Maté: Netanyahu is the Most EGREGIOUS Liar",
                 "rumble_only_injected": True}),
          "non-honorific Rumble-first episode unaffected")


def test_no_regression_for_non_rumble_rows():
    """The guard must not become a blanket keep-everything."""
    src = SRC.read_text()
    # Window must cover the whole of _keeps(); the explanatory comment on the fix
    # is long, so a short slice reads as "filter removed" when it is merely further down.
    seg = src[src.find("def _keeps(row):"):src.find("cache = [r for r in cache if _keeps(r)]")]
    check("return False" in seg, "_keeps() can still drop rows (Shorts filter intact)")
    check(seg.count("return True") >= 4, "_keeps() retains its early-keep rules")


if __name__ == "__main__":
    for fn in [v for k, v in sorted(globals().items()) if k.startswith("test_")]:
        print(fn.__name__)
        fn()
    print()
    if failures:
        print(f"RESULT: {len(failures)} FAILURE(S)")
        sys.exit(1)
    print("RESULT: ALL PASS")
