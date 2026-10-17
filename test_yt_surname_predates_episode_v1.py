#!/usr/bin/env python3
"""YT_SURNAME_PREDATES_EPISODE_V1_20261017 — a returning guest's old episode is not the new one.

Tucker Carlson 16 Oct (Rumble-first, not yet on YouTube) published yt_views 90.5K. That is the
11 Sep Carlson video (JUHl-tQRN7g, 90,527 views), matched by surname. fetch_and_push now clears
the figure and marks the row when the newest YouTube episode carrying the surname predates the
row by more than 7 days; merge_measured_fields_v1 must then NOT carry the stale origin value back.
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import merge_measured_fields_v1 as M  # noqa: E402

MARK = "YT_SURNAME_PREDATES_EPISODE_V1"


def test_merge_does_not_restore_cleared_predating_figure():
    origin = [{"guest": "Tucker Carlson", "surname": "Carlson", "date": "16 Oct", "yt_views": "90.5K"}]
    cloud = [{"guest": "Tucker Carlson", "surname": "Carlson", "date": "16 Oct", "yt_views": None,
              "_yt_status": MARK}]
    rows, _ = M.merge_rows(cloud, origin)
    assert rows[0]["yt_views"] is None


def test_merge_still_carries_forward_unmarked_rows():
    origin = [{"guest": "Kristinn Hrafnsson", "surname": "Hrafnsson", "date": "10 Oct", "yt_views": "1.9K"}]
    cloud = [{"guest": "Kristinn Hrafnsson", "surname": "Hrafnsson", "date": "10 Oct", "yt_views": None}]
    rows, _ = M.merge_rows(cloud, origin)
    assert rows[0]["yt_views"] == "1.9K"


def test_marker_matches_writer():
    src = open(os.path.join(HERE, "fetch_and_push.py")).read()
    assert 'YT_SURNAME_PREDATES_EPISODE = "%s"' % MARK in src
    assert "_yt_predates and not _yt_bound_exactly" in src


if __name__ == "__main__":
    for k, f in list(globals().items()):
        if k.startswith("test_"):
            f()
            print("ok", k)
