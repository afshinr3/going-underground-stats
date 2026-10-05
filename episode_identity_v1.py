#!/usr/bin/env python3
"""EPISODE_IDENTITY_V1_20260930 — one episode-identity rule, shared by both writers.

WHAT WAS WRONG
--------------
Both publishers collapsed duplicate rows with an ordered PRECEDENCE CHAIN:

    canonical_video_id  or  normalised-title hash  or  canonical_episode_id  or  surname|date

Two rows only match when they resolve at the SAME level of that chain, so a row that
carries MORE identity information than its twin gets a different key and both survive.
Measured 2026-09-30 on the live GU feed — Michael O'Hanlon, 22 Aug, published twice:

    row A  canonical_video_id Eu0Phb99ipg  -> key "Eu0Phb99ipg"     YT 2.2K, surname O'Hanlon
    row B  canonical_video_id absent       -> key "5544d97469b0"    YT null, surname Hanlon
    BOTH   canonical_episode_id 6f96fd46bb55  AND  title hash 5544d97469b0

The two rows agreed on TWO identifiers and the chain consulted neither, because a stronger
key existed on one side only. 19 published GU rows for 18 real episodes: the guest appeared
twice in the LaMetric rotation, once with a total missing YouTube, and every aggregate that
touched that date counted its X, Rumble and IG figures twice.

`ANY_SHARED_IDENTITY_MEANS_SAME_EPISODE_V1_20260817` had already diagnosed exactly this and
fixed it — but only for the cloud's CARRY-FORWARD test. The cloud's collapse and the local
bridge's `_collapse_local` were both left on the chain. A fix applied to one caller of a
rule is not a fix to the rule.

THE RULE
--------
Two rows are the same episode if they agree on ANY identity field. Identity is a SET, never
a precedence chain, and agreement is transitive: A~B through a video id and B~C through a
title hash makes A, B and C one episode.

`surname|date` stays a LAST RESORT, emitted only when a row carries no real identifier.
Promoting it to a full member would be unsafe here: the guest parser has historically
fabricated surnames ("Ex-", "Israel's"), and two mis-parsed rows sharing a fabricated
surname on one date would then merge two genuinely different episodes.
"""
from __future__ import annotations

import hashlib
import re
import unicodedata

MARKER = "EPISODE_IDENTITY_V1"
BLANKS = (None, "", "?", "-", "n/a", "N/A")
PLACEHOLDERS = ("?", "-", "n/a", "N/A")


def is_blank(v):
    return v in BLANKS or (isinstance(v, str) and not v.strip())


def norm_title_id(title):
    """sha1 of a NORMALISED title: unicode punctuation folded, whitespace collapsed,
    casefolded — so "America's" (U+2019) and "America's" (U+0027) are one episode."""
    t = (title or "").strip()
    if not t:
        return ""
    t = unicodedata.normalize("NFKD", t)
    for a, b in (("’", "'"), ("‘", "'"), ("“", '"'), ("”", '"'),
                 ("–", "-"), ("—", "-"), (" ", " ")):
        t = t.replace(a, b)
    t = re.sub(r"\s+", " ", t).strip().casefold()
    return hashlib.sha1(t.encode("utf-8")).hexdigest()[:12]


def identities(row):
    """Every identifier this row carries, namespaced so two fields cannot collide."""
    out = set()
    for f in ("canonical_video_id", "canonical_episode_id", "canonical_episode_id_v2"):
        v = str(row.get(f) or "").strip()
        if v:
            out.add(f"{f}={v}")
    t = norm_title_id(row.get("title"))
    if t:
        out.add(f"title={t}")
    for pid_list in (row.get("source_platform_ids") or {}).values():
        for pid in (pid_list or []):
            if pid:
                out.add(f"platform={pid}")
    if not out:
        # No real identifier at all. Only here is surname|date allowed to speak.
        out.add(f"fallback={str(row.get('surname') or '').upper()}|{row.get('date') or ''}")
    return out


def _norm_text(t):
    """The same folding norm_title_id uses, without the hash."""
    t = (t or "").strip()
    if not t:
        return ""
    t = unicodedata.normalize("NFKD", t)
    for a, b in (("\u2019", "'"), ("\u2018", "'"), ("\u201c", '"'), ("\u201d", '"'),
                 ("\u2013", "-"), ("\u2014", "-"), ("\u00a0", " ")):
        t = t.replace(a, b)
    return re.sub(r"\s+", " ", t).strip().casefold()


def _role_words():
    """Sourced from gu_guest_from_posts_v1 so the two cannot drift. Never raises."""
    try:
        import gu_guest_from_posts_v1 as _P
        return set(_P._ROLE_WORDS)
    except Exception:
        return set()


# Fields that must never be filled from a row whose name is evidently not a name.
NAME_FIELDS = ("guest", "surname",
               "canonical_guest_full_name", "canonical_surname_upper")


def name_is_poisoned(row):
    """True when this row's guest/surname is evidently a headline fragment.

    NAME_TRUST_IN_MERGE_V1_20261001. _merge kept whichever row appeared FIRST and
    only filled its blanks. That is order-dependent, and row order is not stable:
    discover_new_episodes does `cached = new_eps + cached` and the cache is later
    sorted by pub_iso, which the poisoned row does not carry. Measured on the
    published 19-row feed: collapsing the O'Hanlon pair good-row-first keeps
    "Michael O'Hanlon"; junk-row-first keeps "Afshin Rattansi CHALLENGES Ex-" /
    surname "Ex-" — strictly worse than the duplicate it replaces, because it is
    one row, confidently wrong, and no longer detectable as a dupe.

    The tests are deliberately narrow and self-evidencing. Two tempting rules were
    measured and REJECTED against all 36 distinct rows in this feed's history:

    - "the guest is a leading slice of its own title" flags THREE correct rows,
      because this show titles episodes "Tucker Carlson: We Are on the Brink...".
      A correct guest is very often a prefix of its own headline.
    - `gu_parser._looks_like_name` is wrong in both directions here: it ACCEPTS
      "Afshin Rattansi CHALLENGES Ex-" and REJECTS "Michael O'Hanlon", whose
      apostrophe is U+2019.

    What actually distinguishes the fragments is the cut itself:
      1. The value ends in "-" — by construction half a word. "Ex-" reached the
         leaderboard as the top episode of the week at 510K reach.
      2. The guest carries headline punctuation (":"), as in
         "CMSGT. Dennis Fritz: Israel's ". A person's name does not.
      3. The surname is a ROLE, not a family name (sourced from
         gu_guest_from_posts_v1._ROLE_WORDS so the two cannot drift).
    Verified over those 36 rows: exactly the two known fragments are flagged and
    nothing else — see test_episode_identity_name_trust_v1.py.
    """
    guest = (row.get("guest") or row.get("canonical_guest_full_name") or "").strip()
    if guest:
        if guest.endswith("-"):
            return True
        if ":" in guest:
            return True
    sn = (row.get("surname") or "").strip().rstrip(".,?!:;\u2019\u2018\"'")
    if sn:
        if sn.endswith("-"):
            return True
        if sn.rstrip("-").casefold() in _role_words():
            return True
    return False


def _merge(keep, drop):
    """Fill keep's blanks from drop. Prefers the row that is NOT carried forward; a
    measured value is never overwritten by an absent one (METRIC_NEVER_REGRESSES).

    The carry-forward markers are cleared HERE, on a row that was actually merged: the
    episode is no longer only a memory of a previous file, so the marker would be a lie.
    They must NOT be cleared from a row that was never merged — a genuinely carried-forward
    episode keeps its provenance, which is what test_episode_never_disappears_v1 checks.
    """
    # NAME_TRUST_IN_MERGE_V1_20261001 — decide which row SURVIVES on evidence,
    # not on which happened to come first. A name that is a slice of its own
    # headline loses to one that is not; see name_is_poisoned.
    k_bad, d_bad = name_is_poisoned(keep), name_is_poisoned(drop)
    if k_bad and not d_bad:
        keep, drop = drop, keep
        k_bad, d_bad = d_bad, k_bad
    elif k_bad == d_bad and keep.get("_carried_forward_iso") and not drop.get("_carried_forward_iso"):
        keep, drop = drop, keep
    for f, v in drop.items():
        if is_blank(keep.get(f)) and not is_blank(v):
            # A poisoned donor may still supply metrics and ids, but never a name:
            # filling a blank guest with a headline fragment is how the fragment
            # got onto the leaderboard in the first place.
            if d_bad and f in NAME_FIELDS:
                continue
            keep[f] = v
    keep.pop("_carried_forward_iso", None)
    keep.pop("_carried_forward_reason", None)
    return keep


# RE_MINTED_IDENTITY_V1_20261005 ---------------------------------------------------
# canonical_episode_id is derived from the TITLE. Edit a published title and the next run
# mints a NEW id, a NEW title hash, and binds the video to the NEW row -- so the previous
# row shares no identity with it and `identities()` cannot see one episode. The
# surname|date fallback that would have caught it is suppressed by design whenever a row
# carries any other identifier ("Only here is surname|date allowed to speak"), and that
# restriction is itself guarded by test_episode_identity_v1.
#
# Measured on the live GU feed 2026-10-04: Matthew Hoh 3 Oct present TWICE --
# a5df3acf1214 BOUND to video doTi3weQTaM carrying the live YouTube title ("...NUKES..."),
# and 443de1f604d7 UNBOUND carrying the superseded title ("...NUCLEAR WEAPONS..."),
# retained by EPISODE_UNION_NEVER_SHRINKS_V1 because it looked like an episode that had
# vanished. Two correct mechanisms colliding: the never-shrink guard cannot tell "this
# episode disappeared" from "this episode's identity changed". Identical rumble_views and
# x_views on both rows, so every aggregate over that date double-counted and the
# leaderboard rendered Hoh twice.
#
# THE EVIDENCE THAT SEPARATES THE TWO CASES IS THE VENUE BINDING, NOT THE AIR DATE.
# Two rows sharing a guest and a date with NEITHER bound are the case
# test_episode_identity_v1 case 3 forbids merging -- they may be two real episodes and
# nothing says otherwise. Two rows bound to DIFFERENT videos are certainly two episodes.
# Only the asymmetric pair -- exactly one bound -- is the signature of an identity that was
# re-minted while the episode stayed put, because a second real episode would have its own
# binding. So this requires that asymmetry and merges nothing else.


def venue_ids(row):
    """Venue bindings this row carries: the YouTube id plus any platform ids."""
    out = set()
    v = str(row.get("canonical_video_id") or "").strip()
    if v:
        out.add(v)
    for pid_list in (row.get("source_platform_ids") or {}).values():
        for pid in (pid_list or []):
            if pid:
                out.add(str(pid))
    return out


def air_date_key(row):
    """SURNAME|air date, or None when either part is missing -- never a half key.

    YEAR_AWARE_AIR_DATE_V1_20261005. `date` is the display string and carries NO YEAR
    ("18 Jul", "3 Oct"), and GUESTS RECUR -- Mearsheimer appears on GU 10 Aug and on New
    Order 27 Sep; Wilkerson on GU 18 Jul and New Order 4 Oct. A year-blind key would make
    one guest's 18 Jul 2026 interview collide with their 18 Jul 2027 one, and this key
    exists to AUTHORISE a merge, so a collision here conflates two real episodes and their
    view counts. pub_iso carries the year, so it leads.

    The short-date fallback is NAMESPACED, so a year-aware key can never compare equal to
    a year-blind one. pub_iso is absent on 4 of 33 live rows (Wilkerson 18 Jul, Fritz
    13 Jul, Ellwood 6 Jul, Bhaskar 28 Jun), and for those the honest answer is that the
    year is unknown. The cost is that a re-minted pair where only ONE row has pub_iso is
    left unmerged -- a visible duplicate, which is the conservative failure. The dangerous
    failure is merging two distinct episodes, and that is now impossible on a year
    mismatch.
    """
    sn = str(row.get("canonical_surname_upper") or row.get("surname") or "").strip().upper()
    if not sn or sn in PLACEHOLDERS:
        return None
    pub = str(row.get("pub_iso") or "").strip()
    if len(pub) >= 10 and pub[4] == "-" and pub[7] == "-":
        return f"{sn}|{pub[:10]}"                 # year-aware: SURNAME|2026-10-03
    dt = str(row.get("date") or "").strip()
    if not dt or dt in PLACEHOLDERS:
        return None
    return f"{sn}|undated:{dt}"                   # year UNKNOWN; never equal to the above


def is_re_minted_pair(a, b):
    """True only for the asymmetric bound/unbound pair on one air date."""
    ka, kb = air_date_key(a), air_date_key(b)
    if not ka or ka != kb:
        return False
    va, vb = venue_ids(a), venue_ids(b)
    return bool(va) != bool(vb)          # exactly one side bound


def collapse(rows):
    """Collapse rows that share ANY identity. Returns (collapsed_rows, n_merged).

    Order of first appearance is preserved. A row matching several existing groups
    merges those groups together, so identity is transitive.
    """
    groups, by_id = [], {}
    for r in rows:
        if not isinstance(r, dict):
            continue
        r = dict(r)
        ids = identities(r)
        hits = sorted({by_id[i] for i in ids if i in by_id})
        if not hits:
            # RE_MINTED_IDENTITY_V1_20261005 — the primary rule found nothing. Before
            # accepting a new episode, check for the one case it structurally cannot see:
            # the same episode under a re-minted, title-derived identity. Requires the
            # bound/unbound asymmetry, so it can never merge two rows that merely share a
            # guest and a date.
            for gi, g in enumerate(groups):
                if g is None or not is_re_minted_pair(g["row"], r):
                    continue
                hits = [gi]
                break
        if not hits:
            by_id.update({i: len(groups) for i in ids})
            groups.append({"row": r, "ids": set(ids)})
            continue
        tgt = hits[0]
        groups[tgt]["row"] = _merge(groups[tgt]["row"], r)
        groups[tgt]["ids"] |= ids
        for gi in hits[1:]:                      # transitive: fold the other groups in
            groups[tgt]["row"] = _merge(groups[tgt]["row"], groups[gi]["row"])
            groups[tgt]["ids"] |= groups[gi]["ids"]
            groups[gi] = None
        for i in groups[tgt]["ids"]:
            by_id[i] = tgt

    out = [g["row"] for g in groups if g is not None]
    for r in out:                                # never publish a placeholder
        for f in list(r):
            if isinstance(r[f], str) and r[f].strip() in PLACEHOLDERS:
                r[f] = None
    n_rows = sum(1 for r in rows if isinstance(r, dict))
    return out, n_rows - len(out)
