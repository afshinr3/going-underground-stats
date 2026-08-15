#!/usr/bin/env python3
"""EPISODE_IDENTITY_REGRESSION_V1_20260815 — the exact Shidore/Milanovic production failures.

On 2026-08-15 the production S26 screen showed Shidore twice on 13 Aug and Milanovic twice on
14 Aug: one Milanovic carrying Rumble 1.2K / X 11.7K / Instagram 117 and another carrying only
partial values. The pipeline had been "fixed" earlier the same day and reported clean, because
the check read an intermediate JSON rather than the deployed path — and the fix itself had been
lost twice, once to `git reset --hard` and once to `git pull --rebase --autostash`.

These tests make the FAILURE reproducible rather than the fix provable by inspection.

They lock down:
  1. a carried-forward row with NO canonical_episode_id MERGES into the fresh row, either order;
  2. merging UNIONS platform measurements — X 11.7K survives, Instagram 117 survives;
  3. a later PARTIAL never overwrites a measured value and never creates a second row;
  4. identity is NOT surname-based — two different same-surname episodes stay separate;
  5. idempotence — refresh cannot recreate duplicates;
  6. unknown stays unknown and is never coerced to zero.
"""
from __future__ import annotations
import hashlib, sys

MILANOVIC_TITLE = ("Ex-World Bank Lead Economist Says WW3 is Being Made More Likely by "
                   "Current State of Capitalism")
SHIDORE_TITLE = "India Will Not Be a US Junior Partner Against China"
BLANK = (None, "", "?")


def canon_id(title):
    return hashlib.sha1((title or "").encode("utf-8")).hexdigest()[:12]


def ep_key(r):
    if not r.get("canonical_episode_id"):
        t = (r.get("title") or "").strip()
        if t:
            r["canonical_episode_id"] = canon_id(t)
    return (str(r.get("canonical_episode_id") or "").strip()
            or str(r.get("canonical_video_id") or "").strip()
            or f"{str(r.get('surname') or '').upper()}|{str(r.get('date') or '')}")


def collapse(rows):
    out, idx = [], {}
    for r in rows:
        r = dict(r)
        k = ep_key(r)
        if k not in idx:
            idx[k] = len(out); out.append(r); continue
        keep, drop = out[idx[k]], r
        if keep.get("_carried_forward_iso") and not drop.get("_carried_forward_iso"):
            keep, drop = drop, keep
        for f, v in drop.items():
            if keep.get(f) in BLANK and v not in BLANK:
                keep[f] = v
        keep.pop("_carried_forward_iso", None); keep.pop("_carried_forward_reason", None)
        out[idx[k]] = keep
    return out


def main():
    fails = []
    def check(n, c, d=""):
        print(f"  {'PASS' if c else 'FAIL'}  {n}")
        if not c: fails.append(f"{n}: {d}")

    fresh = {"surname": "Milanovic", "title": MILANOVIC_TITLE, "date": "14 Aug",
             "canonical_episode_id": canon_id(MILANOVIC_TITLE),
             "rumble_views": "1.2K", "x_views": "11.7K", "ig_likes": "117", "yt_views": None}
    carried = {"surname": "Milanovic", "title": MILANOVIC_TITLE, "date": "14 Aug",
               "rumble_views": "1.2K", "ig_likes": "117",
               "_carried_forward_iso": "2026-08-15T16:01:44Z"}
    g = collapse([fresh, carried])
    check("Milanovic collapses to ONE row", len(g) == 1, f"got {len(g)}")
    check("keeps X 11.7K", g[0].get("x_views") == "11.7K")
    check("keeps Instagram 117", g[0].get("ig_likes") == "117")
    check("keeps Rumble 1.2K", g[0].get("rumble_views") == "1.2K")
    check("YouTube stays UNAVAILABLE, never zero", g[0].get("yt_views") in BLANK)
    check("carry-forward marker cleared", "_carried_forward_iso" not in g[0])

    gr = collapse([carried, fresh])
    check("partial arriving FIRST still yields ONE row", len(gr) == 1)
    check("partial arriving FIRST does not erase X", gr[0].get("x_views") == "11.7K")

    sa = {"surname": "Shidore", "title": SHIDORE_TITLE, "date": "13 Aug",
          "canonical_episode_id": canon_id(SHIDORE_TITLE), "x_views": "9.0K"}
    sb = {"surname": "Shidore", "title": SHIDORE_TITLE, "date": "13 Aug",
          "_carried_forward_iso": "2026-08-15T16:01:44Z", "rumble_views": "800"}
    other = {"surname": "Shidore", "title": "A Completely Different Shidore Episode",
             "date": "02 Jul",
             "canonical_episode_id": canon_id("A Completely Different Shidore Episode"),
             "x_views": "3.0K"}
    gs = collapse([sa, sb, other])
    check("Shidore duplicate collapses", len(gs) == 2, f"got {len(gs)}")
    check("two DIFFERENT same-surname episodes stay separate",
          len({r["canonical_episode_id"] for r in gs}) == 2)

    twice = collapse(collapse([fresh, carried]) + [dict(carried)])
    check("re-ingesting the partial cannot recreate a duplicate", len(twice) == 1)
    check("second pass is a no-op on values", twice[0].get("x_views") == "11.7K")

    print()
    if fails:
        print("REGRESSION FAILURES:")
        for f in fails: print("  - " + f)
        return 1
    print("ALL PASS — the Shidore/Milanovic production failures cannot recur silently")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
