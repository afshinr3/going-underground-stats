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
import time

REPO ="/Users/afshin/going-underground-stats"
RUMBLE = "/Users/afshin/RumbleMonitor/rumble_2026.json"
VIDEOS = os.path.join(REPO, "videos.json")            # GU
VIDEOS_NO = os.path.join(REPO, "videos_neworder.json")  # New Order
TARGET_FILES = [VIDEOS, VIDEOS_NO]
SHOW_OF = {VIDEOS: "GU", VIDEOS_NO: "NO"}
DRY = "--dry-run" in sys.argv
MARKER = "M2_RUMBLE_IG_BRIDGE_V2_20260724"

# SCOPED_VIDEO_DATE_V1_20260926 — which Rumble date provenances may overwrite a published date.
#
# This gate used to be `!= "rumble_video_page_time_datetime"`, and that one string was written by
# scrape_2026_rumble for EVERY successful lookup — including the ones that read the recommendation
# rail instead of the video. That is how the 2026-09-25 Joe Kent episode was published as "20 Sep":
# a sidebar video's publish instant, wearing the provenance this bridge trusts above all others.
#
# The legacy string is deliberately NOT trusted any more. It is unfalsifiable — a rail date and a
# real one are indistinguishable under it — and every fresh scrape now stamps a specific, scoped
# source instead. Trusting it "for continuity" would keep exactly the records that need re-deriving.
TRUSTED_RUMBLE_DATE_PROVS = frozenset({
    "rumble_scoped_time_page_date_agrees",      # <time> in the video header + page date chip agree
    "rumble_ldjson_uploaddate_page_date_agrees",  # VideoObject uploadDate + page date chip agree
    "rumble_scoped_time_only",                  # <time> scoped to the video, no chip to cross-check
    "rumble_ldjson_uploaddate_only",            # VideoObject for THIS url, no chip to cross-check
    "rumble_page_date_chip_day_only",           # the video's own date chip, day precision
})
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
try:
    import verify_derived_feeds_v1 as _VDF  # noqa: E402  (stdlib-only, import-safe)
except Exception:
    _VDF = None

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
    """Tokens that may identify this episode's GUEST in the Rumble surname map.

    LOOSE_TOKEN_RUMBLE_MISATTRIB_V1_20260822 — this used to tokenise the whole TITLE as
    well as the guest, returning every 4+ letter word. The Rumble map is keyed the same
    way, so it holds 442 keys including 'world', 'must', 'prof', 'again', 'against',
    'action', 'alliance'. Any episode could therefore inherit any Rumble video's view
    count through one shared ordinary word.

    Measured 2026-08-22: Prof. David Monyae's episode — which has NO Rumble video at all,
    the show had not gone live — matched the token 'david' and was credited **6,250
    Rumble views** belonging to an unrelated video. The app displayed "Rumble 6.2K" for
    something that did not exist.

    Only the guest's SURNAME can identify the guest. Restricting to it costs nothing:
    13 of the 14 current episodes match by exact title key and never reach this fallback,
    and the fourteenth was the fabricated one.
    """
    _tokens = []
    _sn_fields = (
        str(_ep.get("surname") or ""),
        str(_ep.get("canonical_surname_upper") or ""),
    )
    for _f in _sn_fields:
        for _t in re.findall(r"[A-Za-z]{4,}", _f):
            _tokens.append(_t.lower())
    # Last token of the guest's full name is a surname too ("Prof. David Monyae").
    _g = str(_ep.get("canonical_guest_full_name") or _ep.get("guest") or "").strip()
    _gw = re.findall(r"[A-Za-z]{4,}", _g.split("\n")[0])
    if _gw:
        _tokens.append(_gw[-1].lower())
    _seen, _out = set(), []
    for _t in _tokens:
        if _t not in _seen:
            _seen.add(_t)
            _out.append(_t)
    return _out


# SURNAME_SCOPE_V1_20260926 — a surname match must also be the SAME SHOW and the SAME WEEK.
#
# WHAT HAPPENED. The New Order episode "Prof. John Mearsheimer: GREAT FEAR in the US that China
# will be the World's MOST POWERFUL Country" had not been published anywhere — its YouTube id
# premieres in 18 hours and it has no Rumble video at all — yet GU Stats showed it with 4.0K
# Rumble views. Those 4,010 views belong to "Prof. John Mearsheimer Explains Why Iran War MUST
# END", a GOING UNDERGROUND episode from 2026-07-28. The surname fallback matched on the token
# "mearsheimer" alone, and the map had thrown away the other three Mearsheimer videos.
#
# The 2026-08-22 fix narrowed the fallback from every 4+ letter title word to the guest's surname.
# That killed the 'david'/'world' class of collision but not this one: the same guest, on the other
# show, two months earlier, is still a surname match. Recurring guests are the norm on both shows,
# so the loose version was always going to attribute an unpublished episode sooner or later.
#
# THE RULE. A fallback match must be the same show AND within SURNAME_MATCH_MAX_DAYS of the
# episode's own published date, and it must be the ONLY candidate that qualifies. Ambiguity is a
# refusal: leaving rumble_views alone shows nothing, while guessing shows a number the operator
# cannot tell from a measurement ([[regenerated_artifact_is_not_a_repair_source]]).
SURNAME_MATCH_MAX_DAYS = 7


def _ep_published_date(_ep):
    """The episode's own date, from pub_iso. None when absent."""
    _raw = str(_ep.get("pub_iso") or "").replace("Z", "")
    if not _raw:
        return None
    try:
        return datetime.datetime.fromisoformat(_raw.split(".")[0]).date()
    except Exception:
        return None


def _scoped_surname_match(_ep, _cands, _show_code):
    """(video | None, why). Same show, within SURNAME_MATCH_MAX_DAYS, and unambiguous."""
    _ep_d = _ep_published_date(_ep)
    if _ep_d is None:
        return None, "episode_has_no_pub_iso"
    _ok = []
    for _v in _cands or []:
        if _show_code and str(_v.get("show") or "") != str(_show_code):
            continue
        try:
            _vd = datetime.datetime.fromisoformat(
                str(_v.get("date_iso") or "").split(".")[0]).date()
        except Exception:
            continue
        if abs((_vd - _ep_d).days) <= SURNAME_MATCH_MAX_DAYS:
            _ok.append(_v)
    if not _ok:
        return None, ("no_same_show_video_within_%dd_of_%s (%d other-show/other-date candidate(s))"
                      % (SURNAME_MATCH_MAX_DAYS, _ep_d, len(_cands or [])))
    if len(_ok) > 1:
        return None, ("ambiguous_%d_candidates_within_%dd:%s"
                      % (len(_ok), SURNAME_MATCH_MAX_DAYS,
                         [str(x.get("date_iso"))[:10] for x in _ok]))
    return _ok[0], "same_show_%s" % str(_ok[0].get("date_iso"))[:10]


def _episode_all_word_tokens_LEGACY(_ep):
    """Retained for reference only — this is the loose behaviour that mis-attributed."""
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
            # SURNAME_SCOPE_V1_20260926 — keep EVERY candidate, not the first. A surname is
            # not unique across the archive: John Mearsheimer has four Rumble videos, two on
            # each show, spread over nine months. setdefault kept whichever the iteration
            # reached first and threw the rest away, so the caller could not tell an
            # unambiguous match from a coin toss between four.
            surname.setdefault(_tl, []).append(_v)
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


# RUMBLE_GUEST_OVERRIDE_V1_20260815 — attribution for episodes whose TITLE omits the guest.
# Keyed on the Rumble video id, which is stable and unique; a title substring is accepted as a
# fallback. This is deliberately a small explicit table rather than a heuristic: guessing a
# guest name onto a public feed is the thing the caller refuses to do, and an override is an
# operator asserting a fact, not the parser inventing one.
RUMBLE_GUEST_OVERRIDES = {
    "v7e5t4c": ("Branko Milanovic", "Milanovic"),
}


def _guest_override(url, title):
    u = str(url or "")
    for vid, pair in RUMBLE_GUEST_OVERRIDES.items():
        if vid and vid in u:
            return pair
    return None


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
        # UNATTRIBUTABLE_RUMBLE_EPISODE_V1_20260815 — the bare `continue` below was a SILENT
        # drop, and it is how the Branko Milanovic episode went missing from the dashboard on
        # the day it aired. GU titles do not always name the guest: this one is
        # "Ex-World Bank Lead Economist Says WW3 is Being Made More Likely by Current State of
        # Capitalism" (rumble v7e5t4c, 2026-08-14), which contains no surname at all, so
        # `_clean_guest_surname` returned "" and the episode vanished with no log line, no
        # counter and no alert. It is Rumble-FIRST, so YouTube and X had nothing either and
        # every downstream consumer was blind to it.
        #
        # Refusing to publish a guessed name on a public feed is CORRECT and is kept. What was
        # wrong is doing it invisibly. Two changes: an operator-maintainable override supplies
        # the attribution where the title cannot, and any remaining unattributable episode is
        # REPORTED rather than dropped in silence.
        if not surname:
            _ov = _guest_override(rv.get("url") or "", title)
            if _ov:
                guest, surname = _ov
                print(f"  [{INJECT_MARKER}] attributed via override: {surname} <- {title[:60]}")
            else:
                print(f"  [{INJECT_MARKER}] UNATTRIBUTABLE Rumble-first episode, NOT injected: "
                      f"{dt.date()} {title[:70]} url={rv.get('url')}")
                changes.append((fname, "?UNATTRIBUTED", "EP?", None,
                                f"{dt.day} {dt.strftime('%b')} {title[:34]}"))
                continue
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
    """Content signature of a stats_1week_*.json. None if unreadable.

    DERIVED_FEED_SIGNATURE_PARITY_V1_2026_08_23 — this used to sign only the
    in-window (surname, date) roster. That made every metric inside an entry
    invisible to the publish decision: when a regenerated feed carried moved
    view counts but the same roster, it signed IDENTICAL and the freshly
    generated file was thrown away by `git checkout --` while the updated
    videos.json was committed. main then served a weekly feed disagreeing with
    its own canonical source — observed 2026-08-23 on Eu0Phb99ipg
    (_x_status MEASURED vs UNMEASURED_NO_POSTS_FOUND, yt_views 1.3K vs 1.4K)
    and KhzHRLRRuQE.

    The roster is not the content. This now delegates to the consistency
    checker's feed_signature(), so the decision "is this feed worth publishing"
    and the check "does this feed match its source" are the SAME question asked
    once. generated_at is still ignored — that is what it was always meant to
    ignore, and it is still the only thing normalised away.
    """
    if _VDF is None:
        return None          # unknown, never "unchanged" — see caller
    return _VDF.feed_signature(path)



# BOTH_WRITERS_MUST_COLLAPSE_V1_20260815 — two processes publish these files: the cloud Action
# (fetch_and_push) and this local bridge. The cloud collapse was fixed and proven — Milanovic
# went to a single row through a real cycle — while Shidore stayed DOUBLED at 2 rows in
# videos_neworder.json, both carrying the SAME canonical_video_id CfY37_DnbIM. GU shrank 19->18
# and NO stayed at 9, which is the signature of a second writer republishing a stale pair after
# the first writer had merged it.
#
# A dedupe that lives in only one of two writers is not a dedupe. The same identity rule is
# applied here, immediately before the file is written, so whichever process publishes last
# still publishes one row per episode.
_BLANKS = (None, "", "?", "-", "n/a", "N/A")


def _blankish(v):
    return v in _BLANKS or (isinstance(v, str) and not v.strip())


def _norm_title_id_local(title):
    import hashlib as _h
    import re as _re
    import unicodedata as _ud
    t = (title or "").strip()
    if not t:
        return ""
    t = _ud.normalize("NFKD", t)
    for a, b in (("\u2019", "'"), ("\u2018", "'"), ("\u201c", '"'), ("\u201d", '"'),
                 ("\u2013", "-"), ("\u2014", "-"), ("\u00a0", " ")):
        t = t.replace(a, b)
    t = _re.sub(r"\s+", " ", t).strip().casefold()
    return _h.sha1(t.encode("utf-8")).hexdigest()[:12]


def _episode_key_local(r):
    """DEPRECATED — kept only because other call sites may still reference it.
    Identity is a SET, not a chain; see episode_identity_v1. Do not add callers."""
    return (str(r.get("canonical_video_id") or "").strip()
            or _norm_title_id_local(r.get("title"))
            or str(r.get("canonical_episode_id") or "").strip()
            or "%s|%s" % (str(r.get("surname") or "").upper(), r.get("date")))


def _collapse_local(rows):
    """SHARED_EPISODE_IDENTITY_V1_20260930 — delegates to episode_identity_v1.collapse.

    This used to key on `_episode_key_local`, an ordered precedence chain, so two rows
    matched only when they resolved at the SAME level of it. Measured 2026-09-30 on the
    live GU feed: Michael O'Hanlon 22 Aug published TWICE, one row keyed on its
    canonical_video_id and the other — which had none — on the title hash, while BOTH
    carried the same canonical_episode_id 6f96fd46bb55 and the same title hash. A shared,
    unambiguous identity was present and the chain never consulted it, because a stronger
    key existed on one side only. 19 rows for 18 episodes: the guest appeared twice in the
    LaMetric rotation, once with a total missing YouTube (2.2K), and every aggregate over
    that date double-counted its X, Rumble and IG figures.

    ANY_SHARED_IDENTITY_MEANS_SAME_EPISODE_V1_20260817 had already diagnosed and fixed
    precisely this — but only for the cloud's carry-forward test. Both collapses stayed on
    the chain. The rule now lives in ONE module that both writers call, so a future fix
    cannot land on one publisher and miss the other.
    """
    import episode_identity_v1 as _EI
    out, merged = _EI.collapse(rows)
    if merged:
        print(f"  [{_EI.MARKER}] collapsed {len(rows)} row(s) -> {len(out)} unique "
              f"episode(s) ({merged} duplicate row(s) merged)", flush=True)
    return out


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
    # `vids` is the raw LIST of Rumble videos; `exact` maps join-key -> views only,
    # so neither carries the full record. Build a join-key -> record map here.
    _by_key = {}
    for _rv in (vids or []):
        _k = _rumble_join_key(_rv.get("title"))
        if _k:
            _by_key.setdefault(_k, _rv)

    # RUMBLE_CANONICAL_DATE_RESTAMP_V1_20260826 — for rows this bridge owns
    # (rumble_only_injected), Rumble is canonical for the DATE as well as the views.
    # Those rows were stamped from the channel listing's rounded relative string
    # ("2 days ago" -> now-2d), which drifts with the scrape clock and was routinely a
    # day out: the 2026-08-24 Ken Silva episode was stored as "23 Aug". The scraper now
    # records the exact publish instant from each video page; re-stamp the stored row
    # from it so a row created under the old estimate is corrected instead of frozen.
    # Only ever touches rows the bridge itself injected, and reports every change.
    for ep in videos:
        if not ep.get("rumble_only_injected"):
            continue
        # Re-adjudicate identity through the CLOUD's own function (never a private
        # copy — a second implementation is how these two writers drift apart).
        # Rows injected before SURNAME_IS_NOT_AN_IDENTITY_V1 carry a guest bound on
        # surname alone; the ingest gate cannot reach an existing row.
        if _FP is not None and hasattr(_FP, "_readjudicate_carried_guest"):
            _before = ep.get("guest")
            try:
                if _FP._readjudicate_carried_guest(ep, show_code):
                    changes.append((fname, ep.get("surname"), "guest",
                                    _before, ep.get("guest")))
            except Exception as _e_id:
                print(f"  [identity-repair] {ep.get('surname')}: {_e_id!r}",
                      file=sys.stderr, flush=True)
        rv = _by_key.get(_rumble_join_key(ep.get("title")))
        if not rv or not rv.get("date_iso"):
            continue
        if str(rv.get("date_prov") or "") not in TRUSTED_RUMBLE_DATE_PROVS:
            continue                      # only trust a SCOPED, cross-checked timestamp
        try:
            dt = datetime.datetime.fromisoformat(
                str(rv["date_iso"]).replace("Z", "").split(".")[0]
            ).replace(tzinfo=datetime.timezone.utc)
        except Exception:
            continue
        new_date = f"{dt.day} {dt.strftime('%b')}"
        new_piso = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        if ep.get("date") != new_date or ep.get("pub_iso") != new_piso:
            changes.append((fname, ep.get("surname"), "date",
                            f'{ep.get("date")}/{ep.get("pub_iso")}',
                            f'{new_date}/{new_piso}'))
            ep["date"] = new_date
            ep["pub_iso"] = new_piso
            # Carry the SOURCE's provenance, not a frozen string: "which source said so" is the
            # whole value of this field, and flattening it is what hid the rail date.
            ep["date_prov"] = str(rv.get("date_prov"))

    for ep in videos:
        # ---- Rumble (exact key, surname fallback) ----
        v = exact.get(_rumble_join_key(ep.get("title")))
        via = ""
        if not v:
            for _tok in _episode_surname_tokens(ep):
                _cands = surname.get(_tok)
                if not _cands:
                    continue
                _hit, _why = _scoped_surname_match(ep, _cands, show_code)
                if _hit is not None:
                    v, via = _hit.get("views", 0), " (surname:%s)" % _why
                    break
                if _why:
                    print(f"  [surname-join] {ep.get('surname')}: refused — {_why}", flush=True)
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

    # COLLAPSE_IS_ITS_OWN_REASON_TO_WRITE_V1_20260930 — the collapse used to run only
    # inside the `len(changes) > n0` branch below, so a duplicate could only ever be
    # repaired as a SIDE EFFECT of some unrelated metric moving. A feed whose numbers are
    # all current but whose rows are doubled is therefore never rewritten, and the
    # duplicate is permanent: the O'Hanlon pair survived every hourly run because the
    # episode is from 22 August and nothing about it changes any more. A repair must be
    # its own reason to write.
    _dupes_merged = 0
    if not DRY:
        _pre = len(videos)
        videos = _collapse_local(videos)
        _dupes_merged = _pre - len(videos)

    if (len(changes) > n0 or _dupes_merged) and not DRY:
        videos.sort(key=_pub_iso_sort_key, reverse=True)  # surface newest (incl. injected) at top
        with open(path, "w", encoding="utf-8") as fh:
            # ASCII_PARITY_WITH_CLOUD_WRITER_V1_20260930 — the old comment here claimed
            # ensure_ascii=True matched the cloud writer. It does not: the cloud's FINAL
            # writer for these feeds is merge_measured_fields_v1.py:155, which dumps with
            # ensure_ascii=False. So every title carrying a smart quote (7 of them across
            # the two feeds) was written back escaped as \u2019 here and literal there.
            # Each bridge run therefore rewrote lines that held identical DATA, and the
            # hourly rebase hit a conflict on every one of them. On 2026-09-29 that
            # conflict aborted mid-rebase and left conflict markers in videos.json and
            # videos_neworder.json; the next run could not parse them, regenerated
            # videos_health_v1.json as episodes:[], and the LaMetric /lametric app — which
            # reads that local file — lost all five episode frames. Match the cloud
            # writer's encoding so identical data produces identical bytes.
            json.dump(videos, fh, indent=2, ensure_ascii=False)
        if _dupes_merged:
            print(f"  [{MARKER}] {fname}: {_dupes_merged} duplicate row(s) merged away",
                  flush=True)
        return fname
    return fname if len(changes) > n0 else None


def _health_episodes_sig(path):
    """Content signature of the health feed, ignoring the always-moving iso /
    last_updated. None when it cannot be read — the caller treats unknown as
    changed and publishes, rather than discarding a fresh feed on a failed read."""
    try:
        d = json.load(open(path))
        eps = d.get("episodes") or []
        return json.dumps(
            [[e.get("title"), e.get("date"), e.get("guest"), e.get("surname"),
              (e.get("metrics") or {}).get("rumble_views"),
              (e.get("metrics") or {}).get("ig_likes")] for e in eps],
            sort_keys=True, ensure_ascii=False)
    except Exception:
        return None


def _videos_feed_degraded(path):
    """DEGRADED_FEED_HEALTH_GUARD_V1_20260913 — True when the GU feed on disk is the legacy
    X-only shape: no (non-injected) row carries `yt_views` or `canonical_video_id`. An external
    writer (commits as "GU Stats Bot" from a +0400 host, not the GitHub Action) intermittently
    pushes that shape; regenerating the health feed from it nulled YouTube on every GU episode
    (health yt_ok 23 -> 9, 26 -> 24 episodes, 2026-09-13 01:20Z and 03:20-06:20Z). Keeping the
    last health feed is stale-but-measured; regenerating from this shape is fresh-but-UNKNOWN.
    Unreadable/empty -> False (prior behaviour: let the emitter run)."""
    try:
        rows = [r for r in (json.load(open(path)) or [])
                if isinstance(r, dict) and not r.get("rumble_only_injected")]
        if not rows:
            return False
        return not any(("yt_views" in r) or ("canonical_video_id" in r) for r in rows)
    except Exception:
        return False


def _recover_interrupted_rebase(stale_s=900):
    """INTERRUPTED_REBASE_RECOVERY_V1_20260913 — a `pull --rebase` interrupted on 2026-09-11
    18:20Z left the repo mid-rebase on a detached HEAD. Every later pull failed, every push
    failed ("You are not currently on a branch"), 28 hourly commits never went live and the
    local videos_health_v1.json that every LaMetric pusher reads froze for ~30h. A rebase dir
    older than `stale_s` is not in progress, it is abandoned: abort it (restores the branch,
    re-applies the autostash) so this run syncs normally. Everything here is recomputed hourly
    from local truth, so nothing unique is dropped. Never raises."""
    try:
        gd = os.path.join(REPO, ".git")
        for d in ("rebase-merge", "rebase-apply"):
            p = os.path.join(gd, d)
            age = time.time() - os.path.getmtime(p) if os.path.isdir(p) else 0
            if age > stale_s:
                r = _git("rebase", "--abort")
                print(f"  [INTERRUPTED_REBASE_RECOVERY_V1] stale {d} ({int(age)}s) aborted "
                      f"rc={r.returncode}",
                      file=sys.stderr, flush=True)
    except Exception as _e:
        print(f"  [INTERRUPTED_REBASE_RECOVERY_V1] check failed: {_e!r}", file=sys.stderr, flush=True)


def main():
    if not DRY:
        _recover_interrupted_rebase()
        _git("pull", "--rebase", "--autostash")  # sync with cloud first

    exact, surname, vids = _build_rumble_maps()
    posts = IGM.load_posts()

    changes = []
    changed_files = []
    # FILE_ISOLATION_V1_20260826 — one unreadable target file used to abort the whole
    # run. videos_neworder.json carried unresolved `git stash pop` conflict markers,
    # so json.load raised and main() died AFTER videos.json had been mutated on disk
    # but BEFORE the commit/push, every hour. The Rumble-only episode was written
    # locally and never published, while the cloud kept committing videos.json — a
    # feed whose refresh timestamp advances while an episode silently never arrives.
    # Each file is now isolated: a failure is reported loudly, the other files still
    # publish, and the process exits NON-ZERO so the failure cannot pass as success.
    failed_files = []
    for path in TARGET_FILES:
        try:
            cf = _process_file(path, exact, surname, vids, posts, changes)
        except Exception as e:
            failed_files.append((os.path.basename(path), repr(e)[:200]))
            print(f"[{MARKER}] ERROR processing {os.path.basename(path)}: {e!r}",
                  file=sys.stderr, flush=True)
            continue
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
    # UNREADABLE_FEED_MUST_NOT_REGENERATE_DERIVED_V1_20260930 — the derived feeds
    # (weekly stats + videos_health_v1.json) are summaries OF the target feeds. When a
    # target feed fails to parse it is reported in failed_files and skipped above, but
    # the regenerators below were still run: they read the same unreadable file, found
    # no episodes, and wrote episodes:[] / n:0 over a previously correct summary. That
    # is a read failure published as the fact "there are no episodes". On 2026-09-29 an
    # aborted rebase left conflict markers in both feeds and this path emptied the
    # health feed, which is what the LaMetric /lametric app reads — the display lost
    # every episode frame while the live GitHub copy was still intact. A feed we could
    # not read tells us nothing about its contents: keep the last good summary.
    if _FP is not None and failed_files:
        print(f"  [{INJECT_MARKER}] derived feeds NOT regenerated: "
              f"{len(failed_files)} source feed(s) unreadable "
              f"({', '.join(fn for fn, _ in failed_files)}) — keeping last good summary",
              file=sys.stderr, flush=True)
    if _FP is not None and not failed_files:
        WEEKLY = ("stats_1week_gu.json", "stats_1week_no.json")
        _sig_before = {w: _weekly_entries_sig(os.path.join(REPO, w)) for w in WEEKLY}
        try:
            _FP._generate_weekly_stats()  # rewrites both weekly files from the feed
            for _wf in WEEKLY:
                _p = os.path.join(REPO, _wf)
                _sig_now, _sig_was = _weekly_entries_sig(_p), _sig_before.get(_wf)
                # A signature we could not compute is UNKNOWN. Treating unknown as
                # "unchanged" would discard a freshly generated feed on the strength
                # of a failed read — absence reported as success, which is the exact
                # shape of every silent loss in this pipeline. Publish instead.
                if _sig_now is None or _sig_was is None or _sig_now != _sig_was:
                    if _wf not in changed_files:
                        changed_files.append(_wf)      # content changed -> commit
                else:
                    _git("checkout", "--", _wf)        # only generated_at moved -> discard churn
            print(f"  [{INJECT_MARKER}] weekly stats regenerated; "
                  f"committing={[w for w in WEEKLY if w in changed_files]}")
        except Exception as _e_ws:
            print(f"  [{INJECT_MARKER}] weekly-stats regen skipped: {_e_ws}")

        # X_STORE_ATTRIBUTION_REAPPLY_V1_20260918 — the Action measures each episode's X
        # reach with ONE live search, which is pagination/rate limited, so it republishes
        # roughly half the real number every run (Carlson 684.3K vs 1.48M in the complete
        # local store; Silva 106.1K vs 216.7K) and over-counts any row whose guest parsed
        # as a title fragment (surname "Ex-" -> 531.8K vs 112.3K). This bridge owns the
        # complete store, so it re-attributes from it here — after the pull, before the
        # health feed is regenerated from these files. Rows the store cannot measure, and
        # rows supported by fewer than 3 posts, are left exactly as the Action published
        # them. Idempotent: a second run rewrites nothing.
        try:
            import gu_x_store_attribution_v1 as _XSA
            _XSA.reattribute(apply=True)
            for _f in ("videos.json", "videos_neworder.json"):
                if _f not in changed_files:
                    changed_files.append(_f)
        except Exception as _e_xsa:
            print(f"  [{INJECT_MARKER}] X store re-attribution skipped: {_e_xsa!r}",
                  file=sys.stderr, flush=True)

        # X_FOLLOWERS_LOCAL_V1_20260930 — followers.json is collected HERE, not in CI.
        # X blocks the GitHub runner's IP: measured 2026-09-30, the same cookies read
        # "207.2K Followers" from this Mac and "not found" for all three handles in the
        # Action, which wrote nulls over the good file every run from 2026-09-27T07:25Z
        # and left the X-GU / X-NO / X-AR LaMetric apps showing "?" for two and a half
        # days. The cloud writer is now fail-closed (FOLLOWERS_FAIL_CLOSED_V1) and this
        # bridge owns the file, exactly as it already owns the X post store above.
        try:
            import x_followers_local_v1 as _XFL
            _f_changed, _ = _XFL.write_followers()
            if _f_changed and "followers.json" not in changed_files:
                changed_files.append("followers.json")
        except Exception as _e_xfl:
            print(f"  [{INJECT_MARKER}] X follower collection skipped: {_e_xfl!r}",
                  file=sys.stderr, flush=True)

        # HEALTH_FEED_REGEN_V1_20260826 — videos_health_v1.json is the feed the
        # DASHBOARD PREFERS (videos.json is only its fallback), and it is emitted
        # exclusively by the cloud's main_fetch. So a Rumble-only episode could be
        # correct in videos.json and still never render: the 2026-08-24 episode was
        # live in videos.json while the health feed showed 25 episodes without it,
        # and that feed's `last_updated` is the very timestamp the operator watches
        # advance. Regenerate it from the same emitter (never a private copy) right
        # after the feed is updated. Commit only when the EPISODE CONTENT changes;
        # `iso`/`last_updated` move every run and would otherwise cause churn.
        HEALTH = "videos_health_v1.json"
        _h_before = _health_episodes_sig(os.path.join(REPO, HEALTH))
        try:
            if _videos_feed_degraded(VIDEOS):   # DEGRADED_FEED_HEALTH_GUARD_V1_20260913
                raise RuntimeError("DEGRADED_FEED_HEALTH_GUARD_V1: videos.json is the legacy "
                                   "X-only shape (no yt_views/canonical_video_id) — keeping the "
                                   "previous health feed rather than nulling YouTube")
            _FP._emit_videos_health_v1()
            _h_now = _health_episodes_sig(os.path.join(REPO, HEALTH))
            if _h_now is None or _h_before is None or _h_now != _h_before:
                if HEALTH not in changed_files:
                    changed_files.append(HEALTH)
                print(f"  [{INJECT_MARKER}] health feed regenerated; committing")
            else:
                _git("checkout", "--", HEALTH)   # only timestamps moved
        except Exception as _e_h:
            print(f"  [{INJECT_MARKER}] health-feed regen FAILED: {_e_h!r}",
                  file=sys.stderr, flush=True)

    if failed_files:
        print(f"[{MARKER}] {len(failed_files)} target file(s) FAILED and were skipped:",
              file=sys.stderr, flush=True)
        for fn, err in failed_files:
            print(f"    {fn}: {err}", file=sys.stderr, flush=True)

    if not changed_files:
        print(f"[{MARKER}] no committable changes")
        return 2 if failed_files else 0

    for cf in changed_files:
        _git("add", cf)
    _now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    _git("commit", "-m", f"Rumble+IG bridge: M2 local->upstream {_now}")
    p = _git("push")
    print(f"  push rc={p.returncode} {(p.stderr or p.stdout or '')[-160:].strip()}")
    if p.returncode != 0:
        print(f"[{MARKER}] PUSH FAILED — changes are committed locally but NOT live",
              file=sys.stderr, flush=True)
        return 3
    return 2 if failed_files else 0


if __name__ == "__main__":
    sys.exit(main())
