#!/usr/bin/env python3
"""CLEANUP_NEVER_DELETES_A_BOUND_EPISODE_V1_2026_08_23.

THE REGRESSION THIS GUARDS
--------------------------
On 2026-08-23 the 22 Aug episode vanished from videos.json and nobody could say
why. The URL_BIND audit — the one artifact built to explain drops — was empty,
and the run log positively asserted the inventory was intact:

    [normalize] blanking unresolvable guest/surname; title=Afshin Rattansi
                CHALLENGES Ex-CIA Advisor on the Legacy of America's W...
    [EPISODE_UNION] no episode lost this run (18 total)
    Saved 18 entries to .../videos.json
    Cleaned 1 bad entries from videos.json          <-- the episode, deleted
                                                        after the save

Sequence: the extractor could not find a guest in a title that names only the
host, so the normalizer blanked guest/surname. The EPISODE_UNION guard then
certified the inventory and update_show() saved 18 rows. cleanup_json() runs
AFTER that save, outside the guard, and deletes any row whose surname is <= 1
character. The blanked row failed that test and 17 rows were committed.

The cost was not just the row. It came back two runs later once the name
resolved, but as a FRESH episode: x_views 507.4K was gone, remeasured at 48.5K.

The rule: a name we cannot resolve yet is a naming problem, not evidence the
episode does not exist. A row bound to a real video keeps its place and its
metrics until the name resolves.
"""
import json, os, sys, tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_and_push

FAILS = []


def check(name, cond, detail=""):
    print(("  PASS  " if cond else "  FAIL  ") + name + (("  -- " + detail) if detail and not cond else ""))
    if not cond:
        FAILS.append(name)


# The row exactly as it stood when it was deleted: bound to a real YouTube
# video, metrics accumulated, name blanked by the normalizer this run.
OHANLON_BLANKED = {
    "guest": "", "surname": "", "date": "22 Aug",
    "title": "Afshin Rattansi CHALLENGES Ex-CIA Advisor on the Legacy of America's Wars",
    "canonical_video_id": "Eu0Phb99ipg",
    "canonical_episode_id": "gu-2026-08-22-eu0phb99ipg",
    "canonical_episode_id_v2": "Eu0Phb99ipg",
    "x_views": "507.4K", "rumble_views": "1.6K", "ig_likes": "284",
}
# A Rumble-first episode: real, but not on YouTube yet, so no canonical_video_id.
RUMBLE_FIRST_BLANKED = {
    "guest": "", "surname": "", "date": "21 Aug", "title": "Some Rumble-first episode",
    "rumble_only_injected": True, "rumble_views": "2.1K",
}
# Genuine legacy junk: bad surname, no identity of any kind. Still deletable.
JUNK_BAD_SURNAME = {"guest": "Iran", "surname": "Iran", "date": "3 Jun", "title": "debris"}
JUNK_SHORT_SURNAME = {"guest": "C", "surname": "C", "date": "4 Jun", "title": "debris"}
GOOD = {"guest": "Daniel Levy", "surname": "Levy", "date": "20 Aug",
        "title": "Daniel Levy on Gaza", "canonical_video_id": "abc123"}


def run_cleanup(rows):
    fd, path = tempfile.mkstemp(suffix="_videos.json")
    os.close(fd)
    try:
        with open(path, "w") as f:
            json.dump(rows, f)
        fetch_and_push.cleanup_json(path)
        with open(path) as f:
            return json.load(f)
    finally:
        os.unlink(path)


print(__doc__.strip().splitlines()[0])
print("\n[1] the exact 2026-08-23 loss cannot happen again")
out = run_cleanup([OHANLON_BLANKED, GOOD])
ids = [r.get("canonical_video_id") for r in out]
check("blanked-name episode bound to a real video survives cleanup",
      "Eu0Phb99ipg" in ids, f"rows kept: {ids}")
check("no row is lost at all in that case", len(out) == 2, f"got {len(out)}")
kept = [r for r in out if r.get("canonical_video_id") == "Eu0Phb99ipg"]
check("its accumulated metrics survive with it",
      bool(kept) and kept[0].get("x_views") == "507.4K",
      f"x_views={kept[0].get('x_views') if kept else None}")

print("\n[2] a Rumble-first episode with no YouTube id is equally protected")
out = run_cleanup([RUMBLE_FIRST_BLANKED, GOOD])
check("rumble_only_injected row survives a blank surname", len(out) == 2, f"got {len(out)}")

print("\n[3] genuine junk is still removed — the original purpose still holds")
out = run_cleanup([JUNK_BAD_SURNAME, JUNK_SHORT_SURNAME, GOOD])
surnames = [r.get("surname") for r in out]
check("BAD_SURNAMES row removed", "Iran" not in surnames, f"kept: {surnames}")
check("single-character surname removed", "C" not in surnames, f"kept: {surnames}")
check("the good row is untouched", surnames == ["Levy"], f"kept: {surnames}")

print("\n[4] mixed batch: junk goes, bound episodes stay")
out = run_cleanup([OHANLON_BLANKED, JUNK_BAD_SURNAME, RUMBLE_FIRST_BLANKED, GOOD])
check("2 junk-free real episodes + good row remain", len(out) == 3, f"got {len(out)}")
check("only the junk row was dropped",
      all(r.get("surname") != "Iran" for r in out), f"kept: {[r.get('surname') for r in out]}")

print("\n[5] the old predicate would have deleted the episode (documents the bug)")
old_would_keep = (OHANLON_BLANKED.get("surname", "") not in fetch_and_push.BAD_SURNAMES
                  and len(OHANLON_BLANKED.get("surname", "")) > 1)
check("old len(surname) > 1 rule did delete this row", not old_would_keep,
      "old rule would have kept it — premise of this test is wrong")

print()
if FAILS:
    print(f"FAILED ({len(FAILS)}): " + ", ".join(FAILS))
    sys.exit(1)
print("All cleanup-guard tests passed.")
