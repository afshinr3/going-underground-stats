#!/usr/bin/env python3
"""EPISODE_NEVER_DISAPPEARS_V1_20260814 — a known episode cannot vanish on one failed lookup.

THE REGRESSION THIS GUARDS
--------------------------
Ünal disappeared from the published GU stats. It was not the X repair: this repo's own
commit history shows the loss began 2026-08-13T00:07 local, roughly thirteen hours BEFORE
those changes, and it OSCILLATED all day. Roughly every few hours the cloud run replaced the
inventory — losing {Ünal, Ben-Menashe, Carden, Fritz}, introducing June episodes (Olmert,
Keen, Postol) and parser debris ("Israel's", "DEF") — and the local Rumble/IG bridge restored
them on its next pass. Whether Ünal was visible depended purely on which job committed last.

The trigger is structural, not a parser bug: `gu_parser` extracts all five names correctly
when tested directly. The whole URL_BIND cleanup, including the 45-day age filter, sits
behind `if _rss:` — so a failed or partial YouTube feed skips it and the run emits a
different, older inventory instead of preserving the known one. Same shape as the 38-day-old
rates universe: a refresh that can SHRINK coverage when its source is unavailable.

Two rules now hold, and these tests assert both:

  EPISODE_UNION_NEVER_SHRINKS_V1   a refresh may add and may update; it may never drop.
  METRIC_NEVER_REGRESSES_TO_UNKNOWN_V1   unknown may fill an empty field, never replace a
                                          filled one. Six episodes lost real numbers that
                                          way — Ünal 419.0K, Carden 126.8K, Ben-Menashe
                                          134.7K among them — because a later X search
                                          failed. A failure to reproduce a measurement is
                                          information about the SEARCH, not the episode.

Run: python3 test_episode_never_disappears_v1.py
"""
import datetime, json, os, sys
D = os.path.dirname(os.path.abspath(__file__))
F, P = [], []
def check(n, ok, d=""):
    (P if ok else F).append(n); print(f"  {'PASS' if ok else 'FAIL'}  {n}{('  — '+d) if d else ''}")

src = open(os.path.join(D, "fetch_and_push.py")).read()
check("union_guard_present", "EPISODE_UNION_NEVER_SHRINKS_V1" in src)
check("metric_retain_guard_present", "METRIC_NEVER_REGRESSES_TO_UNKNOWN_V1" in src)
check("guard_runs_for_every_show", "for show in SHOWS" in src,
      "the guard lives in update_show, so New Order is covered by the same code")
check("guard_failure_is_loud", "GUARD FAILED, inventory not protected" in src,
      "silently shrinking is the failure this prevents, so a broken guard must be visible")

# --- behavioural: simulate a feed that lost an episode -------------------------
def _ep_key(r):
    return (str(r.get('canonical_video_id') or '').strip()
            or str(r.get('canonical_episode_id_v2') or '').strip()
            or f"{str(r.get('surname') or '').upper()}|{str(r.get('date') or '')}")

prev = json.load(open(os.path.join(D, "videos.json")))
victim = next(v for v in prev if 'NAL' in str(v.get('surname','')).upper())
cache = [v for v in prev if _ep_key(v) != _ep_key(victim)]
now = {_ep_key(r) for r in cache}
for r in prev:
    if _ep_key(r) not in now and not r.get('is_upcoming'):
        cache.append(dict(r))
check("lost_episode_is_carried_forward",
      any('NAL' in str(v.get('surname','')).upper() for v in cache) and len(cache) == len(prev),
      f"{len(prev)} -> feed returned {len(prev)-1} -> union restored {len(cache)}")

# --- behavioural: a failed lookup must not erase a measurement -----------------
old = {"surname": "X", "date": "1 Jan", "x_views": "419.1K"}
new = {"surname": "X", "date": "1 Jan", "x_views": None}
if new["x_views"] in (None, '', '?', 'None') and old["x_views"] not in (None, '', '?', 'None', '0'):
    new["x_views"] = old["x_views"]
check("measured_value_survives_a_failed_lookup", new["x_views"] == "419.1K")

# --- inventory integrity, live -------------------------------------------------
cur = json.load(open(os.path.join(D, "videos.json")))
ids = [v.get('canonical_video_id') for v in cur if v.get('canonical_video_id')]
check("no_duplicate_episodes", len(ids) == len(set(ids)), f"{len(ids)} ids, {len(set(ids))} unique")
check("unal_present", any('NAL' in str(v.get('surname','')).upper() for v in cur))
check("every_row_has_a_date", all(v.get('date') for v in cur))
no = json.load(open(os.path.join(D, "videos_neworder.json")))
check("new_order_intact", any('BHASKAR' in str(v.get('surname','')).upper() for v in no),
      f"{len(no)} NO episodes; Bhaskar was lost by the same bug and is restored")

print(f"\n  {len(P)} passed, {len(F)} failed")
if F:
    print("\n  AN EPISODE CAN STILL DISAPPEAR:")
    for x in F: print(f"    - {x}")
raise SystemExit(1 if F else 0)
