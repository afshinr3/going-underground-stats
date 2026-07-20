#!/usr/bin/env python3
"""Regression tests for CANONICAL_URL_BIND_V1_2026_07_20.

Runs the four required test cases documented in
regression_tests_gu_titles.json under 'canonical_url_bind_test_cases':
  1. sakwa_real_date_not_17_jul
  2. true_17_jul_guest_present
  3. short_does_not_inherit_date
  4. cross_production_metric_isolation

Each test loads the FRESH VERSION of fetch_and_push.py, monkey-patches its
_fetch_youtube_full_episodes to return the synthetic RSS entries, and then
exercises the URL_BIND helpers (cleanup, dedupe, sort) against a synthetic
cache to assert the expected outcomes.

Exit code 0 = all pass, 1 = any fail.
"""
import copy, json, os, re, sys, types, hashlib, importlib.util

REPO = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, REPO)

# Stub playwright so fetch_and_push imports cleanly.
sys.modules.setdefault("playwright", types.ModuleType("playwright"))
sys.modules.setdefault("playwright.async_api", types.ModuleType("playwright.async_api"))
sys.modules["playwright.async_api"].async_playwright = lambda: None


def load_fap():
    spec = importlib.util.spec_from_file_location("fap", os.path.join(REPO, "fetch_and_push.py"))
    fap = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(fap)
    return fap


def synth_rss_to_episode_dict(entries, channel_id, fap):
    """Return dict {video_id: {title, pub_iso, link, ...}} filtered to
    EPISODE / EPISODE_UNCLASSIFIED per the classifier."""
    out = {}
    for e in entries:
        ct, conf = fap._yt_classify_content(e["link"], e["title"], "")
        # Trust url_kind hint too — synthetic tests always mark accurately.
        if e.get("url_kind") in ("SHORT", "CLIP"): continue
        if ct in ("SHORT", "CLIP"): continue
        out[e["video_id"]] = {
            "video_id":   e["video_id"],
            "title":      e["title"],
            "pub_iso":    e["pub_iso"],
            "link":       e["link"],
            "channel_id": channel_id,
            "canonical_episode_id_v2": fap._canonical_episode_id_v2(channel_id, e["video_id"]),
            "content_type": ct, "confidence": conf,
        }
    return out


def patch_rss(fap, channel_id, entries):
    """Monkey-patch _fetch_youtube_full_episodes to return synthetic entries."""
    ep = synth_rss_to_episode_dict(entries, channel_id, fap)
    fap._URL_BIND_CACHE[channel_id] = ep
    return ep


def _run_bind_and_filter(fap, cache, channel_id):
    """Reproduce the URL_BIND cleanup + outer filter used in update_show()."""
    fap._url_bind_cleanup_and_backfill(cache, channel_id, "/tmp", "test_neworder.json")
    rss = fap._fetch_youtube_full_episodes(channel_id)
    def _keeps(row):
        if row.get("is_upcoming"): return True
        if (row.get("canonical_video_id") or "") in rss: return True
        for _ep in rss.values():
            if fap._url_bind_title_match(row.get("title") or "", _ep["title"]):
                return True
        import datetime as _dt2
        now2 = _dt2.datetime.utcnow()
        _ds = row.get("date") or ""
        _age = 9999
        try:
            _d = _dt2.datetime.strptime(_ds, "%d %b").replace(year=now2.year)
            if _d > now2: _d = _d.replace(year=now2.year - 1)
            _age = (now2 - _d).days
        except Exception: pass
        _ct_g, _ = fap._yt_classify_content(
            row.get("link") or row.get("canonical_video_url") or "",
            row.get("title") or "", "")
        _shorts_prefix_re2 = re.compile(
            r"^(?:Prof\.?|Dr\.?|Amb\.?|Sen\.?|Col\.?|Gen\.?|Fmr|Former|"
            r"Ex[\s\-]|Retired|Ret\.?)\s+", re.I)
        _looks = (_ct_g in ("SHORT","CLIP")
                  or bool(_shorts_prefix_re2.match(row.get("title") or "")))
        if _looks or _age > 45: return False
        return True
    cache = [r for r in cache if _keeps(r)]
    cache = fap._url_bind_dedupe_by_canonical(cache)
    cache = fap._url_bind_sort_by_pub_iso(cache)
    return cache


def _assert(cond, msg):
    if not cond:
        raise AssertionError(msg)


def _iso_to_short(iso):
    from datetime import datetime as _dt
    d = _dt.strptime(iso[:10], "%Y-%m-%d")
    return d.strftime("%-d %b")


def test_sakwa_real_date_not_17_jul(fap, tc):
    """Given RSS with full-length Sakwa (May 17) + Sakwa Short (Jul 17),
    ensure any pre-existing 'Sakwa 17 Jul' row gets dropped."""
    ch = tc["channel_id"]
    patch_rss(fap, ch, tc["synthetic_rss_entries"])
    # Cache seeded with pre-existing PHANTOM 17-Jul Sakwa row (like current bug)
    cache = [{
        "guest": "Richard Sakwa", "surname": "Sakwa",
        "title": "Prof. Richard Sakwa: The US-led Unipolar Order is in TWILIGHT, the West is going BESERK.",
        "date": "17 Jul", "yt_views": "865", "x_views": "32.2K",
        "rumble_views": "0", "ig_likes": "0",
        "canonical_guest_full_name": "Richard Sakwa",
        "canonical_surname_upper": "SAKWA",
        "canonical_episode_id": hashlib.sha1(b"phantomtitle").hexdigest()[:12],
        "show": "NO",
    }]
    cache = _run_bind_and_filter(fap, cache, ch)
    # Must not have any row with date '17 Jul' or title containing 'BESERK'.
    for r in cache:
        _assert(r.get("date") != "17 Jul",
                f"phantom Sakwa '17 Jul' row still present: {r}")
        _assert("BESERK" not in (r.get("title") or ""),
                f"Sakwa Short title survived: {r}")
    # If a Sakwa row exists at all, its canonical_video_id must be BS9TmtR_Ehw
    for r in cache:
        if "sakwa" in (r.get("surname") or "").lower():
            _assert(r.get("canonical_video_id") == "BS9TmtR_Ehw",
                    f"Sakwa row has wrong canonical_video_id: {r}")


def test_true_17_jul_guest_present(fap, tc):
    """Given a real full-length 17 Jul episode with 'Alex Ryder', ensure
    videos_neworder.json has that row."""
    ch = tc["channel_id"]
    ep_meta = patch_rss(fap, ch, tc["synthetic_rss_entries"])
    # Cache seeded EMPTY (so URL_BIND won't try to backfill a stub) — instead
    # we simulate what discover_new_episodes would produce.
    vid = "TESTFULL17JUL"
    ep = ep_meta[vid]
    ceid_v2 = fap._canonical_episode_id_v2(ch, vid)
    new_row = {
        "guest": "Alex Ryder", "surname": "Ryder",
        "title": ep["title"], "date": _iso_to_short(ep["pub_iso"]),
        "yt_views": "?", "x_views": "?", "rumble_views": "?", "ig_likes": "?",
        "canonical_guest_full_name": "Alex Ryder",
        "canonical_surname_upper": "RYDER",
        "canonical_episode_id": hashlib.sha1(ep["title"].encode()).hexdigest()[:12],
        "_yt_content_type": ep["content_type"],
        "_yt_class_confidence": ep["confidence"],
        "canonical_video_id": vid,
        "canonical_video_url": ep["link"],
        "pub_iso": ep["pub_iso"],
        "canonical_episode_id_v2": ceid_v2,
        "source_platform_ids": {"youtube": [vid]},
        "link": ep["link"],
        "show": "NO",
    }
    cache = [new_row]
    cache = _run_bind_and_filter(fap, cache, ch)
    _assert(len(cache) == 1, f"expected 1 row, got {len(cache)}: {cache}")
    r = cache[0]
    _assert(r.get("date") == "17 Jul", f"expected date '17 Jul', got {r.get('date')!r}")
    _assert(r.get("guest") == "Alex Ryder", f"guest mismatch: {r.get('guest')!r}")
    _assert(r.get("canonical_video_id") == vid, f"vid mismatch: {r.get('canonical_video_id')!r}")


def test_short_does_not_inherit_date(fap, tc):
    """Sakwa Short on 17 Jul must NOT override the May full episode's date
    and must NOT create a new row."""
    ch = tc["channel_id"]
    patch_rss(fap, ch, tc["synthetic_rss_entries"])
    # Cache: seed the parent Sakwa full-length row correctly
    parent = {
        "guest": "Richard Sakwa", "surname": "Sakwa",
        "title": "Prof. Richard Sakwa: We Are Witnessing the TWILIGHT of US Unipolarity and the Political West",
        "date": "17 May", "yt_views": "5000", "x_views": "100",
        "rumble_views": "0", "ig_likes": "0",
        "canonical_video_id": "BS9TmtR_Ehw",
        "canonical_video_url": "https://www.youtube.com/watch?v=BS9TmtR_Ehw",
        "pub_iso": "2026-05-17T06:30:06Z",
        "canonical_episode_id_v2": fap._canonical_episode_id_v2(ch, "BS9TmtR_Ehw"),
        "canonical_episode_id": hashlib.sha1(b"parent").hexdigest()[:12],
        "canonical_guest_full_name": "Richard Sakwa",
        "canonical_surname_upper": "SAKWA",
        "source_platform_ids": {"youtube": ["BS9TmtR_Ehw"]},
        "show": "NO",
    }
    # Phantom "Sakwa 17 Jul" row (created from the Short by legacy code)
    phantom = {
        "guest": "Richard Sakwa", "surname": "Sakwa",
        "title": "Prof. Richard Sakwa: The US-led Unipolar Order is in TWILIGHT, the West is going BESERK.",
        "date": "17 Jul", "yt_views": "865", "x_views": "32.2K",
        "rumble_views": "0", "ig_likes": "0",
        "canonical_guest_full_name": "Richard Sakwa",
        "canonical_surname_upper": "SAKWA",
        "canonical_episode_id": hashlib.sha1(b"phantom-short").hexdigest()[:12],
        "show": "NO",
    }
    cache = [parent, phantom]
    cache = _run_bind_and_filter(fap, cache, ch)
    # After processing:
    #  - phantom must be dropped OR merged into parent (last-writer-wins).
    #  - Parent must survive with 17 May date and BS9TmtR_Ehw canonical_video_id.
    _assert(any(r.get("canonical_video_id") == "BS9TmtR_Ehw" and r.get("date") == "17 May"
                for r in cache),
            f"Parent Sakwa 17 May row missing: {cache}")
    for r in cache:
        _assert(r.get("date") != "17 Jul",
                f"phantom Sakwa 17 Jul row still present: {r}")


def test_cross_production_metric_isolation(fap, tc):
    """Perkins full episode's x_views must not appear in Sakwa's row and
    vice versa. Different video_ids = different canonicals = isolated."""
    ch = tc["channel_id"]
    patch_rss(fap, ch, tc["synthetic_rss_entries"])
    perkins = {
        "guest": "John Perkins", "surname": "Perkins",
        "title": tc["synthetic_rss_entries"][0]["title"],
        "date": "12 Jul",
        "yt_views": tc["seeded_metrics"]["PERKINSFULL12"]["yt_views"],
        "x_views":  tc["seeded_metrics"]["PERKINSFULL12"]["x_views"],
        "rumble_views": "0", "ig_likes": "0",
        "canonical_video_id": "PERKINSFULL12",
        "canonical_video_url": "https://www.youtube.com/watch?v=PERKINSFULL12",
        "pub_iso": "2026-07-12T06:30:06Z",
        "canonical_episode_id_v2": fap._canonical_episode_id_v2(ch, "PERKINSFULL12"),
        "canonical_episode_id": hashlib.sha1(b"perkins").hexdigest()[:12],
        "canonical_guest_full_name": "John Perkins",
        "canonical_surname_upper": "PERKINS",
        "source_platform_ids": {"youtube": ["PERKINSFULL12"]},
        "show": "NO",
    }
    sakwa = {
        "guest": "Richard Sakwa", "surname": "Sakwa",
        "title": tc["synthetic_rss_entries"][1]["title"],
        "date": "17 May",
        "yt_views": tc["seeded_metrics"]["BS9TmtR_Ehw"]["yt_views"],
        "x_views":  tc["seeded_metrics"]["BS9TmtR_Ehw"]["x_views"],
        "rumble_views": "0", "ig_likes": "0",
        "canonical_video_id": "BS9TmtR_Ehw",
        "canonical_video_url": "https://www.youtube.com/watch?v=BS9TmtR_Ehw",
        "pub_iso": "2026-05-17T06:30:06Z",
        "canonical_episode_id_v2": fap._canonical_episode_id_v2(ch, "BS9TmtR_Ehw"),
        "canonical_episode_id": hashlib.sha1(b"sakwa").hexdigest()[:12],
        "canonical_guest_full_name": "Richard Sakwa",
        "canonical_surname_upper": "SAKWA",
        "source_platform_ids": {"youtube": ["BS9TmtR_Ehw"]},
        "show": "NO",
    }
    cache = [perkins, sakwa]
    cache = _run_bind_and_filter(fap, cache, ch)
    _assert(len(cache) == 2, f"expected 2 rows, got {len(cache)}: {cache}")
    by_vid = {r["canonical_video_id"]: r for r in cache}
    _assert("PERKINSFULL12" in by_vid, "Perkins row missing")
    _assert("BS9TmtR_Ehw" in by_vid, "Sakwa row missing")
    _assert(by_vid["PERKINSFULL12"]["x_views"] == tc["expected_perkins_x_views"],
            f"Perkins x_views drifted: got {by_vid['PERKINSFULL12']['x_views']!r}")
    _assert(by_vid["BS9TmtR_Ehw"]["x_views"] == tc["expected_sakwa_x_views"],
            f"Sakwa x_views drifted: got {by_vid['BS9TmtR_Ehw']['x_views']!r}")
    _assert(by_vid["PERKINSFULL12"]["yt_views"] == tc["expected_perkins_yt_views"],
            f"Perkins yt_views drifted: got {by_vid['PERKINSFULL12']['yt_views']!r}")
    _assert(by_vid["BS9TmtR_Ehw"]["yt_views"] == tc["expected_sakwa_yt_views"],
            f"Sakwa yt_views drifted: got {by_vid['BS9TmtR_Ehw']['yt_views']!r}")


TESTS = {
    "sakwa_real_date_not_17_jul": test_sakwa_real_date_not_17_jul,
    "true_17_jul_guest_present": test_true_17_jul_guest_present,
    "short_does_not_inherit_date": test_short_does_not_inherit_date,
    "cross_production_metric_isolation": test_cross_production_metric_isolation,
}


def main():
    with open(os.path.join(REPO, "regression_tests_gu_titles.json")) as f:
        data = json.load(f)
    cases = data.get("canonical_url_bind_test_cases") or []
    fap = load_fap()
    n_pass = 0; n_fail = 0
    for tc in cases:
        # Reset URL_BIND cache per test to avoid cross-contamination
        fap._URL_BIND_CACHE.clear()
        name = tc["name"]
        fn = TESTS.get(name)
        if not fn:
            print(f"  SKIP {name}: no runner registered")
            continue
        try:
            fn(fap, tc)
            print(f"  PASS {name}")
            n_pass += 1
        except Exception as e:
            print(f"  FAIL {name}: {e}")
            n_fail += 1
    print(f"\n{n_pass} passed, {n_fail} failed")
    sys.exit(0 if n_fail == 0 else 1)


if __name__ == "__main__":
    main()
