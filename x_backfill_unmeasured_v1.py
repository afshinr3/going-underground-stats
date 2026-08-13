#!/usr/bin/env python3
"""X_BACKFILL_UNMEASURED_V1_20260813 — re-measure episodes whose X reach was fabricated as 0.

WHAT WENT WRONG
---------------
`fetch_and_push.py` wrote the string '0' into `x_views` whenever every X query came back
empty, and again whenever the fetch raised. Finding no posts is not the same as an episode
reaching nobody, so five of fourteen GU episodes shipped a number that was never measured:

    Blumenthal 29 Jun · Wilkerson 18 Jul · Carden 11 Jul · Kucinich 4 Jul · Ben-Menashe 22 Jun

The authoritative X cache (`RumbleMonitor/x_2026.json`, 3,022 GU posts) contains 16
Blumenthal posts totalling 515,643 views, so the zero was demonstrably false rather than
merely unverified.

WHY A SEPARATE SCRIPT, AND WHY IT USES THE PIPELINE'S OWN FUNCTION
------------------------------------------------------------------
The published numbers on the working episodes come from ONE estimator: the union of an
exact-phrase search on the guest string and a broad surname search, deduped max-views-per
tweet id (`fetch_x_views_with_ctx`). Any other estimator produces a different number — the
cache's own surname-sum gives Blumenthal 515,643 and Mearsheimer 1.1M against a published
67.5K — so backfilling with a different method would make the affected episodes
incomparable with the rest of the table while looking like a fix.

This therefore imports the pipeline's function and runs the SAME query union. It is a
re-measurement, not a reconstruction.

DISCIPLINE
----------
  * writes ONLY episodes whose current x_views is a fabricated '0' or null, never a real one;
  * a measurement that comes back empty stays UNKNOWN — this script cannot write a zero
    either, which is the whole point;
  * every write records provenance and the ISO of the measurement;
  * --dry-run prints and writes nothing.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time

REPO = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, REPO)
VIDEOS = {"GU": os.path.join(REPO, "videos.json"),
          "NO": os.path.join(REPO, "videos_neworder.json")}
COOKIES = "/Users/afshin/RumbleMonitor/x_cookies.json"

# A value that is a fabricated zero, or an honest unknown. Both are re-measurable. Anything
# else is a real measurement and is never touched by this script.
def _is_unmeasured(val):
    if val is None:
        return True
    s = str(val).strip().lower()
    return s in ("0", "0.0", "?", "", "none", "null", "n/a")


async def main_async(args):
    os.environ.setdefault("X_COOKIES_JSON", open(COOKIES).read())
    import fetch_and_push as FP
    from playwright.async_api import async_playwright

    targets = []
    for show, path in VIDEOS.items():
        if args.show and show != args.show:
            continue
        try:
            rows = json.load(open(path))
        except Exception as e:
            print(f"  {show}: {path} unreadable: {e}")
            continue
        for v in rows:
            if _is_unmeasured(v.get("x_views")):
                targets.append((show, path, v))
    if not targets:
        print("  nothing to re-measure — no episode carries a fabricated or unknown x_views")
        return 0

    print(f"  {len(targets)} episode(s) to re-measure:")
    for show, _p, v in targets:
        print(f"    {show:<3} {v.get('surname'):<14} {v.get('date'):<8} "
              f"current x_views={v.get('x_views')!r}")
    if args.dry_run:
        print("  --dry-run: nothing written")
        return 0

    results = {}
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            ctx = await browser.new_context()
            await ctx.add_cookies(FP.X_COOKIES)
            # WARM-UP. A cold context loses its first queries: in the probe that first
            # recovered Wilkerson, one long-lived context returned 0 on its opening
            # search and 8 posts on a later identical one. Whatever X is doing on first
            # contact — session establishment, an interstitial, lazy bundle load — it
            # costs the first episode in every run. One throwaway navigation pays it
            # once instead of charging it to whichever guest happens to sort first.
            try:
                _w = await ctx.new_page()
                await _w.goto("https://x.com/home", wait_until="domcontentloaded",
                              timeout=20000)
                await _w.wait_for_timeout(3000)
                await _w.close()
                print("  context warmed")
            except Exception as _e:
                print(f"  warm-up failed ({type(_e).__name__}) — continuing")
            for show, _p, v in targets:
                full_name = (v.get("guest") or "").strip()
                surname = (v.get("surname") or "").strip()
                # The pipeline derives `since` from a "29 Jun" short date via a nested
                # helper that is not importable. `pub_iso` is the canonical publish
                # timestamp on the same record and is strictly better: no year inference,
                # no ambiguity. Falls back to the short date only if pub_iso is absent.
                since = None
                if v.get("pub_iso"):
                    since = str(v["pub_iso"])[:10]
                elif v.get("date"):
                    try:
                        import datetime as _dt
                        _d = _dt.datetime.strptime(v["date"], "%d %b").replace(
                            year=_dt.datetime.now().year)
                        if _d > _dt.datetime.now():
                            _d = _d.replace(year=_d.year - 1)
                        since = _d.strftime("%Y-%m-%d")
                    except Exception:
                        since = None
                # Same handle list the pipeline builds at line 1532: the show handle plus
                # the presenter's own account, because episodes are promoted from both.
                _show = next((s for s in FP.SHOWS
                              if ("going underground" in s["name"].lower()) == (show == "GU")),
                             None)
                handles = [_show["x_handle"], "afshinrattansi"] if _show \
                    else args.handles.split(",")
                ids, meta = {}, {}
                try:
                    await FP.fetch_x_views_with_ctx(ctx, handles, full_name,
                                                    since_date=since, _return_ids=ids,
                                                    _meta_out=meta)
                    if surname and len(surname) > 3:
                        await FP.fetch_x_views_with_ctx(ctx, handles, surname,
                                                        since_date=since, _return_ids=ids,
                                                        _meta_out=meta)
                except Exception as e:                                    # noqa: BLE001
                    print(f"    {surname}: FETCH_FAILED {type(e).__name__}: {str(e)[:90]}")
                    results[(show, surname)] = ("FETCH_FAILED", 0, 0)
                    continue
                total, n = sum(ids.values()), len(ids)
                # A search that never converged is a truncated sample, not a
                # measurement. It is recorded as such rather than published as a number.
                if total > 0 and not meta.get("converged", True):
                    # The union is monotone — extra passes only ADD tweets or RAISE a
                    # view count — so a non-converged run undercounts and can never
                    # overcount. That is a floor, which is a real measurement, and far
                    # more useful than discarding three confirmed posts as "unknown".
                    # It is labelled so nobody mistakes it for an exact count.
                    status = "MEASURED_LOWER_BOUND"
                elif total > 0:
                    status = "MEASURED"
                else:
                    status = "UNMEASURED_NO_POSTS_FOUND"
                results[(show, surname)] = (status, total, n)
                print(f"    {surname}: {n} posts, {total:,} views  "
                      f"[{meta.get('passes')} passes, "
                      f"{'converged' if meta.get('converged') else 'NOT converged'}] -> {status}")
        finally:
            await browser.close()

    iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    written = 0
    for show, path in VIDEOS.items():
        rows = json.load(open(path))
        changed = False
        for v in rows:
            key = (show, (v.get("surname") or "").strip())
            if key not in results:
                continue
            status, total, n = results[key]
            # MONOTONE WRITES ONLY.
            #
            # The union across passes is a LOWER BOUND, and X's search is unreliable
            # enough that consecutive runs disagree about which episode it fails on:
            # one run measured Carden at 410,114 and Wilkerson at nothing, the next
            # measured Wilkerson at 425,391 and Carden at nothing. The first version of
            # this script wrote every result unconditionally, so the second run ERASED
            # a real 410,114 measurement and replaced it with unknown.
            #
            # A floor may only ever be raised. A new sample that is lower than what is
            # already published carries no information — both are floors, and the higher
            # one is the better floor. This mirrors METRIC_ATTRIB_V1 in the pipeline,
            # which for the same reason only overwrites with a non-null, non-zero value.
            _prev = FP.parse_count_opt(v.get("x_views")) if hasattr(FP, "parse_count_opt") else None
            if status in ("MEASURED", "MEASURED_LOWER_BOUND") and (
                    _prev is None or total > _prev):
                v["x_views"] = FP.format_views(total)
                v["_x_status"] = status
                v["_x_measured_iso"] = iso
                v["_x_provenance"] = f"X_BACKFILL_UNMEASURED_V1 phrase+surname union, {n} posts"
                written += 1
            elif _prev is not None:
                # A worse sample than what is already published. Keep the better floor
                # and record that this attempt did not improve it.
                v["_x_last_attempt_iso"] = iso
                v["_x_last_attempt_status"] = status
                print(f"    {v.get('surname')}: kept published {v.get('x_views')} "
                      f"(this run: {total:,}) — a floor is never lowered")
            else:
                # STILL UNKNOWN. Explicitly null, never 0 — that is the defect being fixed.
                v["x_views"] = None
                v["_x_status"] = status
                v["_x_measured_iso"] = iso
            changed = True
        if changed:
            tmp = path + ".tmp"
            json.dump(rows, open(tmp, "w"), indent=2)
            os.replace(tmp, path)
            print(f"  wrote {path}")
    print(f"  {written} episode(s) now carry a measured X value")
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--show", default=None, help="GU or NO; default both")
    ap.add_argument("--handles", default="GUnderground_TV,afshinrattansi")
    return asyncio.run(main_async(ap.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
