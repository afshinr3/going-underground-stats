#!/usr/bin/env python3
"""GU_X_STORE_ATTRIBUTION_V1_20260918 — per-episode X views from the COMPLETE local store.

WHAT WAS WRONG
--------------
The Action measures each episode's X reach with a LIVE search (`fetch_x_episode_engagement`),
which is pagination- and rate-limited, so it publishes whatever one truncated pass happened to
see. Measured 2026-09-18 against the local store (`x_2026.json`, complete and dated):

    guest         published   store attribution   error
    Carlson 1     684.3K      1,482.8K            -54 %
    Silva         106.1K        216.7K            -51 %
    Pilkington    205.5K        424.2K            -52 %
    Milanović     N/A           166.6K            missing entirely
    O'Hanlon      531.8K        112.3K            +374 %   <- opposite direction, see below

The O'Hanlon row is the other failure mode: its guest parsed as "Afshin Rattansi CHALLENGES
Ex-" (surname "Ex-"), and a two-character needle matches unrelated tweets, so that row is
over-counted while every correctly-named row is roughly halved.

WHAT THIS DOES
--------------
Re-attributes x_views for every row from `gu_x_attribution_v1.attribute()` — own handle only,
guest surname must appear in the text, window air-3d..air+21d, deduped by tweet id. That module
already exists and is the project's stated method; nothing here re-implements it.

  * A row is only rewritten when the store yields a MEASURED number. Unmeasurable rows keep
    whatever they had — an unknown count stays unknown rather than becoming 0.
  * A broken guest name is repaired by canonical_episode_id from the last known-good health
    feed before attribution, so the "Ex-" class of row is measured under its real surname.
    No name is invented: if no carry-forward exists, the row is skipped and reported.
  * The store must be fresh (default <= 24 h) or nothing is rewritten at all.

Run from the repo root:  python3 gu_x_store_attribution_v1.py [--apply] [--max-age-h N]
Without --apply it prints the diff and writes nothing. Never raises past main().
"""
from __future__ import annotations

import json
import os
import sys
import time

REPO = os.path.dirname(os.path.abspath(__file__))
RUMBLE = "/Users/afshin/RumbleMonitor"
STORE = os.path.join(RUMBLE, "x_2026.json")
HEALTH = os.path.join(REPO, "videos_health_v1.json")
# GUEST_NAME_MAP: a STABLE snapshot of operator-correct names. The health feed must never be
# the repair source: regenerating it from a feed whose guest parsed as "Afshin Rattansi
# CHALLENGES Ex-" overwrites the good name, and the next run then attributes under the
# two-character surname "Ex-" (31 unrelated posts, 831.6K) or "Israel's" (154 posts, 4.4M).
# Observed here on 2026-09-18 and reverted; the map exists so it cannot recur.
NAME_MAP = os.path.join(REPO, "gu_guest_names_v1.json")
MARKER = "GU_X_STORE_ATTRIBUTION_V1"
FILES = (("videos.json", "GU"), ("videos_neworder.json", "NO"))
MAX_STORE_AGE_H = 24.0
MIN_SUPPORT_TWEETS = 3

if RUMBLE not in sys.path:
    sys.path.insert(0, RUMBLE)


def _fmt(n):
    """K/M formatting matching the existing feeds (684.3K, 1.5M)."""
    n = int(n)
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(n)


def _air_iso(row):
    """Rows carry pub_iso (ISO) or a bare '22 Aug'. Year comes from pub_iso when present."""
    iso = str(row.get("pub_iso") or "")[:10]
    if len(iso) == 10 and iso[4] == "-":
        return iso
    import datetime as _dt
    d = str(row.get("date") or "").strip()
    if not d:
        return None
    for fmt in ("%d %b %Y", "%d %B %Y"):
        for yr in (_dt.date.today().year, _dt.date.today().year - 1):
            try:
                return _dt.datetime.strptime(f"{d} {yr}", fmt).date().isoformat()
            except ValueError:
                continue
    return None


def _guest_carry_forward():
    """canonical_episode_id -> guest, from the stable name map (never the health feed)."""
    try:
        return json.load(open(NAME_MAP)).get("by_canonical_episode_id") or {}
    except Exception:
        return {}


def _looks_broken(guest):
    """A guest field that is a title fragment, not a person."""
    g = (guest or "").strip()
    if not g or len(g) > 28:
        return True
    low = g.lower()
    # Trailing JUNK WORD, not a trailing substring: "Carlson", "Pilkington" and "Ayalon"
    # all end in "on" and are perfectly good names.
    if "afshin rattansi" in low or low.split()[-1] in {"ex-", "ex", "the", "on", "of", "&", "with"}:
        return True
    import gu_x_attribution_v1 as ATT
    sn = ATT.surname_of(g)
    return not sn or len(sn) < 4


def reattribute(apply=False, max_age_h=MAX_STORE_AGE_H):
    import gu_x_attribution_v1 as ATT
    if not os.path.exists(STORE):
        print(f"[{MARKER}] store missing: {STORE}", file=sys.stderr)
        return 1
    age_h = (time.time() - os.path.getmtime(STORE)) / 3600.0
    if age_h > max_age_h:
        print(f"[{MARKER}] store stale ({age_h:.1f}h > {max_age_h}h) — nothing rewritten",
              file=sys.stderr)
        return 1
    store = json.load(open(STORE))
    carry = _guest_carry_forward()
    changed_total = 0
    for fname, show in FILES:
        path = os.path.join(REPO, fname)
        if not os.path.exists(path):
            continue
        rows = json.load(open(path)) or []
        changed = 0
        for r in rows:
            if not isinstance(r, dict):
                continue
            guest = (r.get("canonical_guest_full_name") or r.get("guest") or "").strip()
            repaired = None
            if _looks_broken(guest):
                repaired = carry.get(r.get("canonical_episode_id") or "")
                if not repaired:
                    print(f"  SKIP  {r.get('date')} {guest!r}: unparseable guest, "
                          f"no carry-forward for {r.get('canonical_episode_id')}")
                    continue
                guest = repaired
            air = _air_iso(r)
            if not air:
                print(f"  SKIP  {guest}: no air date")
                continue
            views, n, detail = ATT.attribute(guest, show, air, store=store)
            if views is None:
                print(f"  keep  {air} {guest}: {detail}")
                continue
            # MIN_SUPPORT: the store method requires the surname IN the post text, so a
            # guest whose clips never name him is undercounted (Vikram Sood: 1 tweet, 229.1K
            # vs 759.1K published). Rewriting on one or two posts would swap an over-count
            # for an under-count, so thinly-supported rows are reported and left alone —
            # unless the row has no measured value at all, where any measurement beats none.
            before = r.get("x_views")
            if n < MIN_SUPPORT_TWEETS and before not in (None, "", "?"):
                print(f"  THIN  {air} {guest:22s} {str(before):>9} vs {_fmt(views):>9} "
                      f"({n} tweet(s)) — left unchanged, needs review")
                continue
            after = _fmt(views)
            if before == after and r.get("_x_status") == MARKER:
                continue
            print(f"  SET   {air} {guest:22s} {str(before):>9} -> {after:>9} "
                  f"({n} tweets{', guest repaired' if repaired else ''})")
            if apply:
                r["x_views"] = after
                r["_x_status"] = MARKER
                r["_x_store_tweets"] = n
                if repaired:
                    # Persist the repaired name too: the health feed (and therefore the
                    # LaMetric guest frames) renders `guest`, so measuring O'Hanlon's row
                    # correctly while still displaying "Ex-" fixes only half the defect.
                    # The name is carried forward from the last known-good feed by
                    # canonical_episode_id — never invented, never parsed afresh here.
                    r.setdefault("_x_guest_repaired_from", r.get("guest"))
                    r["guest"] = repaired
                    r["canonical_guest_full_name"] = repaired
                    _sn = ATT.surname_of(repaired)
                    if _sn:
                        r["surname"] = _sn
                        r["canonical_surname_upper"] = _sn.upper()
            changed += 1
        if apply and changed:
            tmp = path + ".tmp"
            with open(tmp, "w") as fh:
                json.dump(rows, fh, indent=2, ensure_ascii=False)
            os.replace(tmp, path)
        print(f"[{MARKER}] {fname}: {changed} row(s) {'rewritten' if apply else 'would change'}")
        changed_total += changed
    return 0 if (changed_total or True) else 1


def main():
    apply = "--apply" in sys.argv
    max_age = MAX_STORE_AGE_H
    if "--max-age-h" in sys.argv:
        try:
            max_age = float(sys.argv[sys.argv.index("--max-age-h") + 1])
        except Exception:
            pass
    try:
        return reattribute(apply=apply, max_age_h=max_age)
    except Exception as e:                                          # noqa: BLE001
        print(f"[{MARKER}_ERR] {e!r}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
