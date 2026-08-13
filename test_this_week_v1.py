#!/usr/bin/env python3
"""THIS_WEEK_IS_LAST_3_EPISODES_V1_20260814 — "This week" is a count, not a date range.

WHY THE CHANGE
--------------
The old card selected on a Fri->Mon broadcast window, and a date rollover emptied it.
Measured live at 2026-08-14T00:00Z the window advanced to 14-17 Aug and reported n=0 while
three perfectly good episodes sat in the inventory. A card that blanks because the calendar
turned over is reporting the clock, not the programme.

"This week" now means the 2 most recent Going Underground episodes plus the 1 most recent
New Order episode, sorted together by publication date, newest first.

WHAT THESE TESTS PROTECT
------------------------
  * a rollover can never empty it — selection is by COUNT off the inventory;
  * the quota is per programme and shortages are STATED, never padded from the other show,
    because the two are not interchangeable and quietly filling a GU slot with a New Order
    episode would misreport whose reach it is;
  * publication date decides order, taken from canonical pub_iso where present;
  * measured stats travel with the entries — a failed lookup neither removes an episode nor
    replaces a measurement (see METRIC_NEVER_REGRESSES_TO_UNKNOWN_V1);
  * the app reads the published artefact rather than recomputing the rule, so the card and
    the JSON cannot drift apart.

Run: python3 test_this_week_v1.py
"""
import datetime, json, os, sys
D = os.path.dirname(os.path.abspath(__file__))
F, P = [], []
def check(n, ok, d=""):
    (P if ok else F).append(n); print(f"  {'PASS' if ok else 'FAIL'}  {n}{('  — '+d) if d else ''}")

tw = json.load(open(os.path.join(D, "stats_this_week_v1.json")))
ent = tw["entries"]

check("selection_is_count_not_window",
      tw.get("selection") == "most_recent_published_episodes_by_quota"
      and "window_start" not in tw,
      "no date range can empty it")
check("quota_is_2_GU_and_1_NO", tw.get("quota") == {"GU": 2, "NO": 1}, str(tw.get("quota")))
shows = [e.get("_this_week_show") for e in ent]
check("composition_matches_quota",
      shows.count("GU") <= 2 and shows.count("NO") <= 1,
      f"GU={shows.count('GU')} NO={shows.count('NO')}")
check("never_substitutes_across_shows",
      not (shows.count("NO") > tw["quota"]["NO"] or shows.count("GU") > tw["quota"]["GU"]),
      "a GU slot is never filled with a New Order episode")

dates = [e.get("_this_week_pub_date") for e in ent]
check("sorted_newest_first_across_shows", dates == sorted(dates, reverse=True), str(dates))
check("every_entry_is_dated", all(dates), "an undated row is excluded, never guessed")
check("uses_canonical_pub_iso_where_present",
      any(e.get("_this_week_date_source") == "pub_iso" for e in ent),
      str({e.get('surname'): e.get('_this_week_date_source') for e in ent}))

check("shortage_is_explicit",
      ("shortages" in tw and "complete" in tw
       and tw["complete"] == (len(tw["shortages"]) == 0)),
      f"complete={tw['complete']} shortages={tw['shortages']}")
check("count_is_reported_against_expectation",
      tw.get("n") == len(ent) and tw.get("n_expected") == 3,
      f"n={tw.get('n')} of {tw.get('n_expected')}")

# measured stats must travel with the entries, and unknown must stay unknown
bad_zero = [e.get("surname") for e in ent
            for f in ("x_views", "yt_views", "rumble_views", "ig_likes")
            if str(e.get(f)).strip() == "0"]
check("no_fabricated_zero_in_the_card", not bad_zero, str(bad_zero))
check("entries_carry_their_metrics",
      all(any(e.get(f) is not None for f in
              ("x_views", "yt_views", "rumble_views", "ig_likes")) for e in ent))

# the app must consume the artefact, not reimplement the rule
page = open(os.path.join(D, "docs", "index.html"), encoding="utf-8").read()
check("app_reads_the_published_artefact", "stats_this_week_v1.json" in page)
check("app_matches_by_identity", "thisWeekKey" in page and "__thisWeekKeys" in page)
check("app_states_a_shortage", "This Week is short" in page,
      "the reader is told which show is short and by how much")
check("app_falls_back_loudly", "falling back to the legacy date window" in page,
      "an unavailable artefact must not silently show a different set")
check("tab_label_describes_the_rule", "latest 2 GU + 1 NO" in page)

print(f"\n  {len(P)} passed, {len(F)} failed")
if F:
    print("\n  THIS WEEK IS WRONG:")
    for x in F: print(f"    - {x}")
raise SystemExit(1 if F else 0)
