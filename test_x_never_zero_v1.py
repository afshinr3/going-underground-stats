#!/usr/bin/env python3
"""X_NEVER_ZERO_V1_20260813 — a failed or unmatched X measurement must never become 0.

THE DEFECT THIS GUARDS
----------------------
`fetch_and_push.py` wrote the string '0' into `x_views` whenever every X query came back
empty, and again whenever the fetch raised:

    else:
        if v.get('x_views') == '?': v['x_views'] = '0'      # <-- fabricated
    except Exception as e:
        if v.get('x_views') == '?': v['x_views'] = '0'      # <-- fabricated

Five of fourteen GU episodes shipped a fabricated zero. The Blumenthal episode displayed

    Rumble 3.9K · X 0 · YouTube 15.4K · Instagram 2.1K · Total 21K

while the authoritative X cache held 16 Blumenthal posts totalling 515,643 views, and a
re-measurement through the pipeline's own search returned 283,793. The total was presented
as whole while the largest platform was simply missing.

WHY IT SURVIVED A FIX THAT WAS ALREADY WRITTEN
----------------------------------------------
GU_UNKNOWN_IS_NULL_V2_2026_08_06 had already established the correct rule and applied it
to every metric — but it runs LATER in the same file and only rewrites '?'. The X path
got there first and destroyed the very marker V2 looks for. Its comment even records that
it "mirrors the existing X pattern": the X branch was the TEMPLATE for the discarded rule
and the one place never migrated off it. A fix applied to the general case can leave the
original special case running.

Run: python3 test_x_never_zero_v1.py
"""
from __future__ import annotations

import json
import os
import re
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
PIPELINE = os.path.join(ROOT, "fetch_and_push.py")
DATA = [("GU", os.path.join(ROOT, "videos.json")),
        ("NO", os.path.join(ROOT, "videos_neworder.json"))]
HEALTH = os.path.join(ROOT, "videos_health_v1.json")

FAILS, PASSES = [], []


def check(name, ok, detail=""):
    (PASSES if ok else FAILS).append(name)
    print(f"  {'PASS' if ok else 'FAIL'}  {name}{('  — ' + detail) if detail else ''}")


def main():
    src = open(PIPELINE, encoding="utf-8").read()

    # --- 1. the coercion must not exist in any form --------------------------------
    coercions = re.findall(r"""\[\s*['"]x_views['"]\s*\]\s*=\s*['"]0['"]""", src)
    check("no_x_views_assigned_zero", not coercions,
          f"{len(coercions)} literal assignment(s) of '0' to x_views"
          if coercions else "the fabrication site is gone")
    # The general rule must still be the one in force.
    check("unknown_preserved_as_null", "GU_UNKNOWN_IS_NULL_V2" in src,
          "'?' is normalised to null, not to a number")

    # --- 2. no shipped episode carries a fabricated zero ----------------------------
    # The pipeline only ever writes x_views when total > 0, so a literal 0 cannot be a
    # measurement — it can only be a fabrication.
    for show, path in DATA:
        if not os.path.exists(path):
            continue
        rows = json.load(open(path))
        bad = [r.get("surname") for r in rows
               if str(r.get("x_views")).strip() in ("0", "0.0")]
        check(f"{show}_no_zero_x_views", not bad,
              f"{len(rows)} episodes; " + (f"FABRICATED ZEROS: {bad}" if bad else "none"))
        # An unknown must be null AND say why.
        unk = [r for r in rows if r.get("x_views") is None]
        unlabelled = [r.get("surname") for r in unk if not r.get("_x_status")]
        check(f"{show}_unknown_x_is_labelled", not unlabelled,
              f"{len(unk)} unknown, " + (f"unlabelled: {unlabelled}" if unlabelled
                                         else "each carries _x_status"))

    # --- 3. the display artefact makes unknown unsummable ---------------------------
    if os.path.exists(HEALTH):
        h = json.load(open(HEALTH))
        eps = h.get("episodes") or []
        zeros = [e.get("surname") for e in eps
                 if str((e.get("metrics") or {}).get("x_views")).strip() in ("0", "0.0")]
        check("health_no_zero_x", not zeros,
              f"{len(eps)} episodes; " + (f"ZEROS: {zeros}" if zeros else "none"))
        na = [e for e in eps
              if isinstance((e.get("metrics") or {}).get("x_views"), dict)]
        check("health_unknown_is_structured", all(
            m.get("status") == "N/A" and m.get("reason")
            for m in [(e.get("metrics") or {}).get("x_views") for e in na]),
            f"{len(na)} unmeasured rendered as {{status:'N/A', reason}} — a dict cannot "
            "be summed by accident, a bare null can")

    # --- 4. aggregation excludes unknown rather than adding zero --------------------
    os.environ.setdefault("X_COOKIES_JSON", "[]")
    sys.path.insert(0, ROOT)
    try:
        import fetch_and_push as FP
        total, unknown = FP.sum_known_metrics(
            {"rumble_views": "3.9K", "yt_views": "15.4K", "x_views": None,
             "ig_likes": "2.1K"})
        check("unknown_excluded_from_total", unknown == ["x_views"] and total == 21400,
              f"total={total} unknown={unknown} — the caller is told the total is partial")
        t2, u2 = FP.sum_known_metrics(
            {"rumble_views": "3.9K", "yt_views": "15.4K", "x_views": "283.8K",
             "ig_likes": "2.1K"})
        check("measured_x_included", not u2 and t2 > 300000,
              f"total={t2:,} with X measured — the same episode, complete")
    except Exception as e:                                            # noqa: BLE001
        check("aggregation_importable", False, str(e)[:120])

    # --- 5. the renderer marks a partial total --------------------------------------
    idx = os.path.join(ROOT, "docs", "index.html")
    if os.path.exists(idx):
        page = open(idx, encoding="utf-8").read()
        check("renderer_marks_partial_total",
              "fmtTotal(" in page and "unmeasured and excluded from this total" in page,
              "a total built on incomplete data is marked, never shown as if whole")

    print(f"\n  {len(PASSES)} passed, {len(FAILS)} failed")
    if FAILS:
        print("\n  X MEASUREMENT INTEGRITY IS BROKEN — a missing platform can reach the "
              "display as a real zero:")
        for f in FAILS:
            print(f"    - {f}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
