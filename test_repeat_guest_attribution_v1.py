#!/usr/bin/env python3
"""REPEAT_GUEST_ATTRIBUTION_V1 — a recurring guest's X clips must belong to ONE episode.

WHY THIS EXISTS
---------------
Guests come back. Measured on the live feeds: Mearsheimer on GU 10 Aug and on New Order
27 Sep; Wilkerson on GU 18 Jul and New Order 4 Oct. Their X figures differ by a lot
(480.5K vs 709.6K; 1.1M vs 13.7K), so attributing one appearance's clips to the other is
not a rounding error.

gu_x_store_attribution matches on OWN HANDLE + GUEST SURNAME IN THE TEXT inside a window
of air-3d..air+21d, deduped by tweet id. The surname does not distinguish two appearances
by the same person -- only the DATE does. So if two appearances' windows OVERLAP, a clip
in the overlap is claimed by both episodes under the same rule, and nothing downstream can
tell which interview it was about. The +21d tail is long: two appearances 22 days apart
already overlap.

Today no pair overlaps (the closest real gap is 47 days), so this passes. It is here to
fire on the booking that creates the ambiguity, before the figures are published, rather
than after someone notices a guest's numbers look wrong.

THE DATE FIELD CANNOT DO THIS ALONE. `date` is the display string and carries no year
("18 Jul"), and pub_iso is absent on 4 of 33 rows. An undated row is therefore compared
against its partner under EVERY plausible year, and ambiguity is reported unless the
episodes are far enough apart under ALL of them -- never assuming the convenient year.

LIMITATION, STATED RATHER THAN DISCOVERED LATER: the authoritative window constants live
in gu_x_attribution_v1.py, which is host-side and not in this repo. The values below mirror
the window as documented in gu_x_store_attribution_v1's own docstring. If the real module
ever disagrees, this guard is wrong in a knowable direction and these two numbers are the
single place to correct.
"""
import datetime as d
import json
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import episode_identity_v1 as EI

ROOT = os.path.dirname(os.path.abspath(__file__))
FEEDS = ("videos.json", "videos_neworder.json")
WINDOW_BEFORE_D = 3       # mirrors gu_x_store_attribution_v1: "window air-3d..air+21d"
WINDOW_AFTER_D = 21
MONTHS = {m: i + 1 for i, m in enumerate(
    ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"))}
_results = []


def check(name, ok, detail=""):
    _results.append((name, bool(ok), detail))
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + (f"  — {detail}" if detail else ""))


def _rows():
    out = []
    for fn in FEEDS:
        d_ = json.load(open(os.path.join(ROOT, fn)))
        for r in (d_ if isinstance(d_, list) else d_.get("videos", d_)):
            out.append((fn, r))
    return out


def _surname(r):
    return str(r.get("canonical_surname_upper") or r.get("surname") or "").strip().upper()


def air_dates(r, year_hints):
    """Every plausible air date for this row. One entry when pub_iso gives the year."""
    pub = str(r.get("pub_iso") or "").strip()
    if len(pub) >= 10:
        try:
            return [d.date.fromisoformat(pub[:10])]
        except ValueError:
            pass
    parts = str(r.get("date") or "").split()
    if len(parts) != 2 or parts[1] not in MONTHS:
        return []
    day, mon = int(parts[0]), MONTHS[parts[1]]
    out = []
    for y in sorted(year_hints):
        for yy in (y - 1, y, y + 1):
            try:
                dt = d.date(yy, mon, day)
            except ValueError:
                continue
            if dt not in out:
                out.append(dt)
    return out


def overlaps(a, b):
    """True if two air dates' attribution windows intersect."""
    a0, a1 = a - d.timedelta(days=WINDOW_BEFORE_D), a + d.timedelta(days=WINDOW_AFTER_D)
    b0, b1 = b - d.timedelta(days=WINDOW_BEFORE_D), b + d.timedelta(days=WINDOW_AFTER_D)
    return a0 <= b1 and b0 <= a1


def main():
    rows = _rows()
    years = {int(str(r.get("pub_iso"))[:4]) for _, r in rows
             if str(r.get("pub_iso") or "")[:4].isdigit()} or {d.date.today().year}
    by = defaultdict(list)
    for fn, r in rows:
        sn = _surname(r)
        if sn:
            by[sn].append((fn, r))
    repeats = {k: v for k, v in by.items() if len(v) > 1}

    print(f"\n1  RECURRING GUESTS ON THE LIVE FEEDS  ({len(repeats)} found)")
    for sn, members in sorted(repeats.items()):
        shown = ", ".join(f"{r.get('date')} x={r.get('x_views')}" for _, r in members)
        print(f"     {sn}: {shown}")

    print("\n2  NO TWO APPEARANCES SHARE AN ATTRIBUTION WINDOW")
    clashes, evaluated = [], []
    for sn, members in sorted(repeats.items()):
        for i in range(len(members)):
            for j in range(i + 1, len(members)):
                ri, rj = members[i][1], members[j][1]
                # The same episode twice is a DUPLICATE, not two appearances. Ask the
                # canonical rule rather than re-deciding it here -- the Hoh pair carries
                # two DIFFERENT episode ids, which is the entire defect, so comparing ids
                # would have mistaken a duplicate for a repeat booking. collapse() also
                # keeps this guard correct as that rule evolves. test_episode_identity_v1
                # owns the duplicate itself.
                if len(EI.collapse([ri, rj])[0]) == 1:
                    continue
                di, dj = air_dates(ri, years), air_dates(rj, years)
                if not di or not dj:
                    clashes.append(f"{sn}: {ri.get('date')} / {rj.get('date')} — air date "
                                   f"unparseable, overlap cannot be ruled out")
                    continue
                # the closest the two appearances could be under ANY plausible year
                min_gap = min(abs((b - a).days) for a in di for b in dj)
                evaluated.append((sn, min_gap))
                bad = [(a, b) for a in di for b in dj if overlaps(a, b)]
                if bad:
                    a, b = bad[0]
                    gap = abs((b - a).days)
                    clashes.append(f"{sn}: {a} and {b} are {gap}d apart — windows overlap, "
                                   f"so a clip naming {sn} belongs to both")
    if clashes:
        for c in clashes:
            check("attribution windows do not overlap", False, c)
    else:
        # Only pairs actually evaluated as two APPEARANCES, and the closest they could be
        # under any plausible year -- not an arbitrary year pick, and never a duplicate
        # pair that was skipped above (reporting that as a 0d gap would read as a 0d gap
        # having passed).
        best = {}
        for sn, g in evaluated:
            best[sn] = min(g, best.get(sn, g))
        detail = (", ".join(f"{sn} {g}d" for sn, g in sorted(best.items()))
                  or "no repeat bookings to compare")
        check("attribution windows do not overlap", True,
              f"closest possible gaps: {detail} (window is -{WINDOW_BEFORE_D}d..+{WINDOW_AFTER_D}d, "
              f"so {WINDOW_BEFORE_D + WINDOW_AFTER_D + 1}d or less overlaps)")

    print("\n3  YEAR-STAMP COVERAGE FOR RECURRING GUESTS  (reported, not asserted)")
    # Deliberately NOT a failure. Check 2 already decides ambiguity, and it does so by
    # testing an undated row against EVERY plausible year -- so a missing pub_iso only
    # matters when some interpretation actually overlaps, which check 2 catches. Failing
    # here as well would make this guard red for a benign gap, and an alarm that always
    # fires is one nobody reads.
    undated = [f"{_surname(r)} {r.get('date')}" for _, r in rows
               if _surname(r) in repeats and not str(r.get("pub_iso") or "").strip()]
    print(f"     {'all year-stamped' if not undated else ', '.join(undated) + ' — year inferred, not known'}")

    failed = [n for n, ok, _ in _results if not ok]
    print(f"\n  {len(_results) - len(failed)} passed, {len(failed)} failed")
    if failed:
        print("\n  A RECURRING GUEST'S X VIEWS MAY BE ATTRIBUTED TO THE WRONG INTERVIEW.")
        print("  Only the date separates two appearances; the surname cannot.")
        return 1
    print("\nALL CHECKS PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
