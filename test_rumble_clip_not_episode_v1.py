#!/usr/bin/env python3
"""RUMBLE_ONLY_REQUIRES_VIDEO_URL_V1_20261017 — a Rumble clip must not become an episode.

"Tucker Carlson: Trump's Betrayal of His Voters is the GREATEST BETRAYAL…" had no video url in the
channel listing. The bridge injected it as a new 16 Oct episode; it is a clip of the 11 Sep Carlson
interview, so the feed showed a phantom episode with "X ?". Guards:
  1. the bridge refuses to inject a url-less Rumble item;
  2. a tombstone removes a row whether it arrives from origin OR from this run.
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import merge_measured_fields_v1 as M  # noqa: E402


def test_bridge_refuses_urlless_item():
    import datetime
    import m2_rumble_to_upstream_v1 as B
    now = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=22)
    vids = [{"show": "GU", "title": "Tucker Carlson: Trump’s Betrayal of His Voters is the GREATEST BETRAYAL",
             "date_iso": now.strftime("%Y-%m-%dT%H:%M:%S"), "views": 1150}]
    videos, changes = [], []
    assert B._inject_rumble_only(videos, "GU", vids, "videos.json", changes) == 0
    assert videos == []


def test_bridge_still_injects_item_with_url():
    import datetime
    import m2_rumble_to_upstream_v1 as B
    now = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=5)
    vids = [{"show": "GU", "title": "Significant Possibility of NUKES Being Used Against Iran— Ex-State Dept. Official Matthew Hoh",
             "date_iso": now.strftime("%Y-%m-%dT%H:%M:%S"), "views": 900,
             "url": "https://rumble.com/v7g9qai-significant-possibility.html"}]
    videos, changes = [], []
    assert B._inject_rumble_only(videos, "GU", vids, "videos.json", changes) == 1
    assert videos[0]["title"].startswith("Significant")


def test_tombstone_drops_cloud_side_row():
    row = {"surname": "Carlson", "date": "16 Oct", "title": "x"}
    out, n = M.carry_forward_rows([dict(row)], [], tombs={"carlson|16 Oct"})
    assert out == [] and n == 1


def test_live_tombstone_lists_the_clip():
    import json
    d = json.load(open(os.path.join(HERE, "gu_removed_episodes_v1.json")))
    assert "carlson|16 Oct" in d["by_surname_date"]


if __name__ == "__main__":
    for k, f in list(globals().items()):
        if k.startswith("test_"):
            f()
            print("ok", k)
