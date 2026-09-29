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


def _merge(keep, drop):
    """Fill keep's blanks from drop. Prefers the row that is NOT carried forward; a
    measured value is never overwritten by an absent one (METRIC_NEVER_REGRESSES).

    The carry-forward markers are cleared HERE, on a row that was actually merged: the
    episode is no longer only a memory of a previous file, so the marker would be a lie.
    They must NOT be cleared from a row that was never merged — a genuinely carried-forward
    episode keeps its provenance, which is what test_episode_never_disappears_v1 checks.
    """
    if keep.get("_carried_forward_iso") and not drop.get("_carried_forward_iso"):
        keep, drop = drop, keep
    for f, v in drop.items():
        if is_blank(keep.get(f)) and not is_blank(v):
            keep[f] = v
    keep.pop("_carried_forward_iso", None)
    keep.pop("_carried_forward_reason", None)
    return keep


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
