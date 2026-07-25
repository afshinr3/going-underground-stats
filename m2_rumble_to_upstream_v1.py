#!/usr/bin/env python3
# m2_rumble_to_upstream_v1.py
# REBUILT 2026-07-24 — marker M2_RUMBLE_IG_BRIDGE_V2_20260724
# =============================================================================
# Focused local bridge: inject Rumble views + Instagram likes into the committed
# videos.json, preserving EVERY cloud-provided field (X/YT/canonical/etc). Only
# rumble_views and ig_likes are ever written.
#
# WHY THIS EXISTS: the GU pipeline is two-writer. The cloud GitHub Action
# (runs-on: ubuntu-latest, every 15 min) live-scrapes X + YT and commits
# videos.json, but has NO Rumble or Instagram source. This local bridge is the
# only path for those two metrics. The original m2_rumble_to_upstream_v1.py was
# deleted ~2026-06-27 (never committed to git, unrecoverable), and its :20
# hourly cron has been failing silently ever since -> rumble_views / ig_likes
# stuck at "0". This is a faithful reconstruction:
#   * Rumble join logic copied verbatim from RumbleMonitor/auto_update.py.
#   * Instagram matching reuses RumbleMonitor/ig_matcher_v2.match_episode (import).
#
# SAFETY: writes ONLY rumble_views + ig_likes; json.dump(..., indent=2) matches
# the cloud writer byte-for-byte so the diff is minimal and does not fight the
# 15-min cloud commits. git pull --rebase --autostash before writing.
#
# Usage: python3 m2_rumble_to_upstream_v1.py [--dry-run]
# =============================================================================
import datetime
import json
import os
import re
import subprocess
import sys

REPO = "/Users/afshin/going-underground-stats"
RUMBLE = "/Users/afshin/RumbleMonitor/rumble_2026.json"
VIDEOS = os.path.join(REPO, "videos.json")            # GU
VIDEOS_NO = os.path.join(REPO, "videos_neworder.json")  # New Order
TARGET_FILES = [VIDEOS, VIDEOS_NO]
SHOW_OF = {VIDEOS: "GU", VIDEOS_NO: "NO"}
DRY = "--dry-run" in sys.argv
MARKER = "M2_RUMBLE_IG_BRIDGE_V2_20260724"
# RUMBLE_ONLY_EPISODE_INJECT_V1_20260725 — inject episodes that exist on Rumble
# but not yet on YouTube/X (the cloud's only sources), so Rumble-first shows
# (e.g. this-week's episode) reach videos.json + the 1-week tab + Substack the
# same day instead of waiting for the YouTube upload. Bounded to recent episodes
# only; the cloud preserves these rows and dedupes by title when YT later lands.
INJECT_MARKER = "RUMBLE_ONLY_EPISODE_INJECT_V1_20260725"
RECENCY_DAYS_INJECT = 10  # only inject Rumble-only episodes newer than this

sys.path.insert(0, "/Users/afshin/RumbleMonitor")
sys.path.insert(0, REPO)  # for fetch_and_push guest extractor (import-safe: __main__-guarded)
os.environ.setdefault("X_COOKIES_JSON", "[]")
os.environ.setdefault("IG_COOKIES_JSON", "[]")
import ig_matcher_v2 as IGM  # noqa: E402  (report-only module, import-safe)
try:
    import fetch_and_push as _FP  # noqa: E402  (cloud module; __main__-guarded, safe to import)
except Exception:
    _FP = None

# Non-surname tokens (copied verbatim from auto_update.py surname-fallback).
_STOP = {"going", "underground", "episode", "video", "with", "from", "reveals",
         "trump", "biden", "israel", "russia", "china", "ukraine", "gaza",
         "nato", "putin", "afshin", "rattansi", "order", "clip", "part", "live",
         "full", "over", "into", "what", "when", "where", "about", "after",
         "before"}


def _rumble_join_key(_title):
    _t = str(_title or "")
    for _p in ("NEW EPISODE OF GOING UNDERGROUND", "NEW EPISODE OF NEW ORDER"):
        if _t.upper().startswith(_p):
            _t = _t[len(_p):]
            break
    _t = _t.replace("\n", " ").strip()
    return re.sub(r"[^a-z0-9]", "", _t.lower())[:40]


def _episode_surname_tokens(_ep):
    _tokens = []
    for _fld in (_ep.get("guest") or "", _ep.get("title") or ""):
        _s = str(_fld)
        for _p in ("NEW EPISODE OF GOING UNDERGROUND", "NEW EPISODE OF NEW ORDER"):
            if _s.upper().startswith(_p):
                _s = _s[len(_p):]
                break
        _s = _s.replace("\n", " ").strip()
        for _t in re.findall(r"[A-Za-z]{4,}", _s):
            _tokens.append(_t.lower())
    _seen, _out = set(), []
    for _t in _tokens:
        if _t not in _seen:
            _seen.add(_t)
            _out.append(_t)
    return _out


def _fmt(v):
    v = int(v)
    if v >= 1_000_000:
        return f"{v/1e6:.1f}M"
    if v >= 1_000:
        return f"{v/1e3:.1f}K"
    return str(v)


def _pub_iso_sort_key(v):
    """Same ordering the cloud uses (_url_bind_sort_by_pub_iso): pub_iso desc,
    falling back to 'dd Mon'. Keeps an injected Rumble-first episode at the top
    instead of appended last."""
    piso = v.get("pub_iso") or ""
    if piso and len(piso) >= 10:
        return piso
    d = v.get("date") or ""
    m = re.match(r"(\d+)\s+([A-Za-z]+)", d)
    if m:
        mon = {mn: i for i, mn in enumerate(
            ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep",
             "Oct", "Nov", "Dec"])}.get(m.group(2)[:3].title())
        if mon:
            now = datetime.datetime.now(datetime.timezone.utc)
            yr = now.year
            try:
                if datetime.date(yr, mon, int(m.group(1))) > now.date():
                    yr -= 1
            except Exception:
                pass
            return f"{yr}-{mon:02d}-{int(m.group(1)):02d}T00:00:00Z"
    return ""


def _build_rumble_maps():
    d = json.load(open(RUMBLE))
    vids = d.get("videos_2026") or d.get("videos_all") or []
    exact, surname = {}, {}
    for _v in vids:
        _k = _rumble_join_key(_v.get("title"))
        if _k:
            exact.setdefault(_k, _v.get("views", 0))
        for _tok in re.findall(r"[A-Za-z]{4,}", str(_v.get("title") or "")):
            _tl = _tok.lower()
            if _tl in _STOP:
                continue
            surname.setdefault(_tl, _v.get("views", 0))
    return exact, surname, vids


def _clean_guest_surname(title):
    """Derive a clean (guest, surname) from a Rumble title, reusing the cloud's
    own extractor + the same LEGACY_GUEST_PREFIX_SCRUB the cloud applies, so an
    injected row needs no cloud-side correction. Returns ("","") if it cannot
    confidently attribute — caller must skip injection in that case."""
    if _FP is None:
        return "", ""
    try:
        g = (_FP.extract_guest(title) or "").strip()
        sn = (_FP.extract_surname(g) if g else "") or ""
        sn = sn.strip()
        # cloud's LEGACY_GUEST_PREFIX_SCRUB: strip "Ex/Former/Fmr <role> " prefix
        # when the guest's last token is the surname (e.g. "Ex-Israeli Negotiator
        # Daniel Levy" -> "Daniel Levy").
        if sn and re.match(r"^(?:Former|Ex|Fmr)\b", g, re.IGNORECASE) \
                and g.split() and g.split()[-1].lower() == sn.lower():
            g = " ".join(g.split()[-2:])
        # prefer a positive CANON_MAP full name when available
        try:
            cfn, csu, _ = _FP._canonical_from_title(title, "", "")
            if cfn and csu:
                g, sn = cfn, cfn.split()[-1]
        except Exception:
            pass
        return g.strip(), sn.strip()
    except Exception:
        return "", ""


def _inject_rumble_only(videos, show_code, vids, fname, changes):
    """Append recent Rumble-only episodes (present on Rumble, absent from the
    YouTube/X-sourced feed) as well-formed rows. Fail-safe: skips any episode
    without a reliable date or a confident guest attribution. Returns count."""
    existing = set()
    for _ep in videos:
        _t = (_ep.get("title") or "").lower()[:40]
        if _t:
            existing.add(_t)
    now = datetime.datetime.now(datetime.timezone.utc)
    added = 0
    for rv in vids:
        if (rv.get("show") or "") != show_code:
            continue
        title = (rv.get("title") or "").strip()
        if not title:
            continue
        key = title.lower()[:40]
        if key in existing:
            continue
        diso = str(rv.get("date_iso") or "")
        try:
            dt = datetime.datetime.fromisoformat(
                diso.replace("Z", "").split(".")[0]).replace(tzinfo=datetime.timezone.utc)
        except Exception:
            continue  # undated -> never inject (would fail the 1-week date filter anyway)
        if (now - dt) > datetime.timedelta(days=RECENCY_DAYS_INJECT):
            continue  # only Rumble-FIRST (recent); do not backfill rolled-off history
        guest, surname = _clean_guest_surname(title)
        if not surname:
            continue  # never inject a row we cannot attribute (public feed)
        row = {
            "guest": guest or surname,
            "surname": surname,
            "title": title,
            "date": f"{dt.day} {dt.strftime('%b')}",   # "24 Jul" — parseable by weekly filter
            "rumble_views": _fmt(rv.get("views") or 0),
            "show": show_code,
            "canonical_guest_full_name": guest or surname,
            "pub_iso": dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "rumble_only_injected": True,        # provenance so the cloud/operator can audit
            "rumble_prov": "m2_rumble_only_v3",
        }
        videos.append(row)
        existing.add(key)
        changes.append((fname, surname, "EP+", None, f'{row["date"]} {title[:34]}'))
        added += 1
    return added


def _git(*args, check=False):
    return subprocess.run(["git", "-C", REPO, *args],
                          capture_output=True, text=True, check=check)


def _weekly_entries_sig(path):
    """Content signature of a stats_1week_*.json — the in-window (surname, date)
    set, ignoring the always-changing generated_at timestamp. None if unreadable."""
    try:
        d = json.load(open(path))
        return sorted((str(e.get("surname")), str(e.get("date")))
                      for e in (d.get("entries") or []))
    except Exception:
        return None


def _process_file(path, exact, surname, vids, posts, changes):
    """Inject rumble_views + ig_likes into existing rows AND append recent
    Rumble-only episodes missing from the feed. Returns basename if it changed
    (and was written), else None."""
    fname = os.path.basename(path)
    if not os.path.exists(path):
        return None
    videos = json.load(open(path))
    n0 = len(changes)
    # RUMBLE_ONLY_EPISODE_INJECT_V1_20260725 — add Rumble-first episodes first so
    # their rumble_views are already correct (they carry it) and IG matching below
    # can also apply to them in the same pass.
    show_code = SHOW_OF.get(path)
    if show_code:
        _inject_rumble_only(videos, show_code, vids, fname, changes)
    for ep in videos:
        # ---- Rumble (exact key, surname fallback) ----
        v = exact.get(_rumble_join_key(ep.get("title")))
        via = ""
        if not v:
            for _tok in _episode_surname_tokens(ep):
                if _tok in surname:
                    v, via = surname[_tok], " (surname)"
                    break
        if v and v != 0:
            newr = _fmt(v)
            if str(ep.get("rumble_views")) != newr:
                changes.append((fname, ep.get("surname"), "rumble", ep.get("rumble_views"), newr + via))
                ep["rumble_views"] = newr

        # ---- Instagram (reuse ig_matcher_v2.match_episode) ----
        try:
            r = IGM.match_episode(ep, posts)
            likes = int(r.get("total_likes") or 0)
            if likes > 0:
                newi = _fmt(likes)
                if str(ep.get("ig_likes")) != newi:
                    changes.append((fname, ep.get("surname"), "ig", ep.get("ig_likes"), newi))
                    ep["ig_likes"] = newi
        except Exception as _e:
            pass  # fail-open: never let one episode's IG match abort the bridge

    if len(changes) > n0 and not DRY:
        videos.sort(key=_pub_iso_sort_key, reverse=True)  # surface newest (incl. injected) at top
        with open(path, "w") as fh:
            json.dump(videos, fh, indent=2)  # indent=2 + ensure_ascii=True == cloud writer
        return fname
    return fname if len(changes) > n0 else None


def main():
    if not DRY:
        _git("pull", "--rebase", "--autostash")  # sync with cloud first

    exact, surname, vids = _build_rumble_maps()
    posts = IGM.load_posts()

    changes = []
    changed_files = []
    for path in TARGET_FILES:
        cf = _process_file(path, exact, surname, vids, posts, changes)
        if cf and not DRY:
            changed_files.append(cf)

    print(f"[{MARKER}] {len(changes)} field update(s){' (DRY-RUN, no write/push)' if DRY else ''}")
    for fn, s, f, old, new in changes:
        print(f"  {fn:22s} {str(s):16s} {f:7s} {old!r} -> {new!r}")

    if DRY:
        return 0

    # RUMBLE_ONLY_EPISODE_INJECT_V1_20260725 — regenerate the 1-week tab from the
    # updated feed EVERY run (not just when an episode was injected this cycle):
    # the tab can be stale even with no feed change, because the cloud's own
    # _generate_weekly_stats() runs only at the END of main_fetch(), AFTER the
    # flaky X-follower / IG scrapes; when those fail in CI (CF / rate-limit)
    # main_fetch aborts and stats_1week_*.json freezes (observed stuck >40min
    # while videos.json kept updating). The bridge closes that loop
    # deterministically. Same generator -> identical output, so it never fights
    # the cloud. Commit weekly files ONLY when their in-window entries actually
    # change (ignore the always-moving generated_at) to avoid per-run churn.
    if _FP is not None:
        WEEKLY = ("stats_1week_gu.json", "stats_1week_no.json")
        _sig_before = {w: _weekly_entries_sig(os.path.join(REPO, w)) for w in WEEKLY}
        try:
            _FP._generate_weekly_stats()  # rewrites both weekly files from the feed
            for _wf in WEEKLY:
                _p = os.path.join(REPO, _wf)
                if _weekly_entries_sig(_p) != _sig_before.get(_wf):
                    if _wf not in changed_files:
                        changed_files.append(_wf)      # entries changed -> commit
                else:
                    _git("checkout", "--", _wf)        # only timestamp moved -> discard churn
            print(f"  [{INJECT_MARKER}] weekly stats regenerated; "
                  f"committing={[w for w in WEEKLY if w in changed_files]}")
        except Exception as _e_ws:
            print(f"  [{INJECT_MARKER}] weekly-stats regen skipped: {_e_ws}")

    if not changed_files:
        print(f"[{MARKER}] no committable changes")
        return 0

    for cf in changed_files:
        _git("add", cf)
    _now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    _git("commit", "-m", f"Rumble+IG bridge: M2 local->upstream {_now}")
    p = _git("push")
    print(f"  push rc={p.returncode} {(p.stderr or p.stdout or '')[-160:].strip()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
