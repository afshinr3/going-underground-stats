#!/usr/bin/env python3
"""GU_GUEST_FROM_POSTS_V1_20260822 — recover a guest name from the show's OWN posts
when the video title does not contain one.

THE RECURRING FAILURE THIS ENDS
-------------------------------
Some episode titles identify the guest only by ROLE and name nobody but the host:

    "Afshin Rattansi CHALLENGES Ex-CIA Advisor on the Legacy of America's Wars..."

`gu_parser.extract_guest` correctly returns None for these (FALLTHROUGH_NO_MATCH) --
there is no name in the string to find. But downstream a legacy `title[:30]` truncation
wrote the guest as **'Afshin Rattansi CHALLENGES Ex-'** and the surname as **'Ex-'**, and
that is what reached the leaderboard: the top episode of the week, 753K reach, labelled
"Ex-". Measured 2026-08-22; the parser had already logged 12 rejections of that title.

The name was never missing from our data -- only from the TITLE. The show announces every
guest on X and Instagram:

    "SATURDAY'S GOING UNDERGROUND:  We're joined by Dr. Michael O'Hanlon, former member
     of the CIA External Advisory Board and Pentagon Defense Policy Board."
                                            -- @GUnderground_TV, 2026-08-21T12:57:58Z

18 X posts and 4 IG captions named him. Nothing looked. This module looks.

DESIGN RULES
------------
* **Never invent.** Returns None unless a name is found in the show's own post text.
  A refusal is a valid answer; fabricating one is what caused the defect.
* **Time-bound.** Only posts within +/- WINDOW_H of the episode's publish time are
  considered, so a previous week's guest cannot claim this week's episode. Same principle
  as the ownership time-match: evidence must belong to THIS episode.
* **Show-scoped.** GU posts resolve GU episodes, NO posts resolve NO episodes.
* **Corroboration wins.** Candidates are ranked by how many distinct posts name them, so a
  single stray capitalised phrase loses to the name the show repeated all week.
* **Fail-soft.** Any read or parse error yields None; the caller keeps its existing
  behaviour (skip the episode) rather than gaining a wrong name.
"""
import json
import os
import re
from datetime import datetime, timedelta, timezone

MARKER = "GU_GUEST_FROM_POSTS_V1_20260822"

X_FILE = "/Users/afshin/RumbleMonitor/x_2026.json"
IG_FILE = "/Users/afshin/RumbleMonitor/ig_2026.json"

# Announcements run AHEAD of the episode (measured: -18.7h to -25.5h across 2026).
# A symmetric window let the FOLLOWING week's announcement claim an episode
# (2026-08-07 Ünal resolved as 'Mearsheimer' from a post 47.2h away). Bound it to
# the announcement lead time, asymmetric, with a small forward allowance.
WINDOW_BEFORE_H = 40
WINDOW_AFTER_H = 6
WINDOW_H = 48   # legacy arg default; see resolve_guest
MIN_POSTS = 1          # a single explicit "joined by" announcement is enough

_HON = r"(?:Dr\.?|Prof\.?|Professor|Mr\.?|Ms\.?|Mrs\.?|Sir|Lord|Baroness|Amb\.?|Gen\.?|Col\.?)"
# A person's name: 2-4 capitalised tokens, allowing O'Hanlon, Al Mulla, Maté, Ünal.
_NAME = r"[A-ZÀ-Þ][\w’'\-]+(?:\s+[A-ZÀ-Þ][\w’'\-]+){1,3}"

# THE SHOW'S OWN ANNOUNCEMENT CONVENTION, and only that.
#
# A first draft scored every capitalised-name pattern inside a +/-48h window. Back-tested
# against 12 known-good episodes it got 10 WRONG -- because the GU account announces BOTH
# shows, so a New Order guest inside the window outscored the Going Underground one
# (2026-08-17 Pilkington resolved as "Shidore", 2026-08-10 Mearsheimer as "Fernandez").
# Time proximity alone does not bind a post to an episode.
#
# Two conditions now bind it, and both are the show's own explicit convention:
#   1. the post names the SHOW ("GOING UNDERGROUND" / "NEW ORDER")
#   2. the post uses the announcement phrase "joined by <Name>"
# Among those, the announcement CLOSEST in time to the episode wins -- announcements run
# roughly a day ahead (Pilkington -20h, O'Hanlon -23h), so nearest-in-time is unambiguous.
# NOTE: re.I must NOT span the name group -- it defeats the capitalisation rule in
# _NAME and matched "at", "of", "the", "world-renowned" as guests in the back-test.
# Scoped inline flags keep "joined by"/honorifics case-insensitive and the NAME strict.
# The announcement clause takes three shapes, all seen live in 2026:
#   name FIRST   "joined by Dr. Michael O'Hanlon, former member of the CIA..."
#   name LAST    "joined by Donald Trump's former lawyer Robert Barnes."
#   honorific-late "joined by Holocaust survivor and ... specialist Dr. Gabor Maté"
# So: cut the clause at the first comma / newline / period, then inside it prefer the
# name that follows an honorific; failing that take the LAST name-shaped token run,
# because every descriptive form puts the person at the end.
#
# NOTE: re.I must NOT span the name group -- it defeats the capitalisation rule in
# _NAME and matched "at", "of", "the", "world-renowned" as guests in the back-test.
_JOINED = re.compile(r"(?i:(?:we(?:’|')?(?:re|ll be)\s+)?joined by)\s+(.+)")
_HON_NAME = re.compile(rf"(?i:{_HON})\s+({_NAME})")
_ANY_NAME = re.compile(_NAME)
_POSSESSIVE = re.compile(r"(?:’|')s$")

_ROLE_WORDS = {
    "lead", "economist", "advisor", "adviser", "chief", "officer", "minister",
    "ambassador", "president", "commander", "general", "admiral", "analyst",
    "director", "secretary", "senator", "governor", "mayor", "attorney", "lawyer",
    "whistleblower", "hitman", "strategist", "journalist", "academic", "professor",
    "specialist", "survivor", "member", "board", "policy", "defense", "defence",
    "research", "bank", "world", "former", "deputy", "acting", "head", "editor",
    "correspondent", "author", "historian", "expert", "veteran", "colonel",
}


def _is_person_name(cand):
    """Does this look like a PERSON, not an institution or a headline fragment?

    Back-test false positives this rejects, both from real announcements:
      "World Bank’s Research"  -- a possessive token; the guest was Milanovic
      "US-Iran MoU"            -- internal capitals; the guest was Fritz

    Rule: 2-4 tokens; no possessive token; and within a token an uppercase letter may
    appear only at the start or straight after an apostrophe/hyphen -- which admits
    O’Hanlon, Al-Sisi and Maté while excluding MoU, US-Iran and NATO-style fragments.
    """
    toks = _norm(cand).split()
    if not 2 <= len(toks) <= 4:
        return False
    # A role title is not a person. "joined by the World Bank's former Lead Economist
    # Branko Milanovic" yielded "Lead Economist" once possessives were rejected -- shaped
    # exactly like a name. Mirrors _ROLE_SURNAMES in fetch_and_push.py.
    if any(t.lower().strip(".,") in _ROLE_WORDS for t in toks):
        return False
    for t in toks:
        if _POSSESSIVE.search(t):
            return False
        if not t[:1].isupper():
            return False
        for i, ch in enumerate(t[1:], start=1):
            if ch.isupper() and t[i - 1] not in ("’", "'", "-"):
                return False
    return True


def _name_from_clause(text):
    """Guest name from a 'joined by ...' announcement, or None."""
    m = _JOINED.search(text or "")
    if not m:
        return None
    # Split on comma/newline ONLY. Splitting on "." truncated "Dr. Michael O'Hanlon"
    # to "Dr" and silently refused every honorific-first announcement.
    clause = re.split(r"[,\n]", m.group(1))[0].strip()
    if not clause:
        return None
    hon = _HON_NAME.findall(clause)
    if hon:
        cand = _norm(hon[-1])
        return cand if _is_person_name(cand) and not _blocked(cand) else None
    names = [_norm(x) for x in _ANY_NAME.findall(clause)]
    names = [n for n in names if _is_person_name(n) and not _blocked(n)]
    return names[-1] if names else None


_SHOW_MARKERS = {"GU": ("GOING UNDERGROUND",), "NO": ("NEW ORDER",)}

# Never accept these as a guest: the host, the shows, and role words that survive
# a capitalised-token match.
_BLOCK = {
    "afshin rattansi", "afshin", "going underground", "new order",
    "united states", "middle east", "white house", "al qaeda", "new episode",
    "saturday going", "former cia", "ex cia", "the us", "us israeli",
}


def _norm(s):
    return re.sub(r"\s+", " ", (s or "").strip()).strip(".,:;’'\"")


def _blocked(name):
    low = _norm(name).lower().replace("’", "'")
    if low in _BLOCK:
        return True
    # a name whose tokens are all uppercase acronyms is a headline, not a person
    return all(t.isupper() for t in _norm(name).split())


def _load_posts(show):
    """(text, datetime) for every post by this show. Fail-soft to []."""
    out = []
    try:
        with open(X_FILE) as fh:
            res = (json.load(fh).get("results") or {}).get(show) or {}
        for t in (res.get("tweets_2026") or []):
            dt = t.get("created_dt")
            if not dt:
                continue
            try:
                out.append((t.get("text") or "",
                            datetime.fromisoformat(dt.replace("Z", "+00:00"))))
            except Exception:
                continue
    except Exception:
        pass
    try:
        with open(IG_FILE) as fh:
            ig = json.load(fh)
        for p in _iter_ig(ig, show):
            out.append(p)
    except Exception:
        pass
    return out


def _iter_ig(node, show, _depth=0):
    """IG schema varies; walk it defensively for caption+timestamp pairs."""
    if _depth > 6:
        return
    if isinstance(node, dict):
        cap = node.get("caption")
        ts = node.get("taken_at_dt") or node.get("taken_at") or node.get("created_dt")
        if isinstance(cap, str) and ts:
            try:
                if isinstance(ts, (int, float)):
                    yield cap, datetime.fromtimestamp(ts, timezone.utc)
                else:
                    yield cap, datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
            except Exception:
                pass
        for v in node.values():
            yield from _iter_ig(v, show, _depth + 1)
    elif isinstance(node, list):
        for v in node:
            yield from _iter_ig(v, show, _depth + 1)


def resolve_guest(pub_iso, show="GU", window_h=WINDOW_H, posts=None):
    """Best-supported guest name from the show's own posts around `pub_iso`, or None.

    Returns (name, evidence_dict) on success, (None, evidence_dict) on refusal. The
    evidence is always returned so a refusal can be audited as readily as a hit.
    """
    ev = {"marker": MARKER, "show": show, "pub_iso": pub_iso,
          "window_h": window_h, "n_posts_in_window": 0, "candidates": {}}
    try:
        pub = datetime.fromisoformat(str(pub_iso).replace("Z", "+00:00"))
    except Exception:
        ev["refused"] = "unparseable_pub_iso"
        return None, ev

    allp = posts if posts is not None else _load_posts(show)
    lo, hi = pub - timedelta(hours=WINDOW_BEFORE_H), pub + timedelta(hours=WINDOW_AFTER_H)
    markers = _SHOW_MARKERS.get(show, ())
    other = tuple(m for k, ms in _SHOW_MARKERS.items() if k != show for m in ms)

    hits = []          # (abs_time_delta, name, when)
    n = 0
    for text, when in allp:
        if when is None or not (lo <= when <= hi):
            continue
        n += 1
        body = re.sub(r"^RT @\w+:\s*", "", text or "")
        up = body.upper()
        # must name THIS show, and must not name the other one (a post can trail both)
        if markers and not any(m in up for m in markers):
            continue
        if other and any(m in up for m in other) and not any(
                up.index(m) < min(up.index(o) for o in other if o in up)
                for m in markers if m in up):
            continue
        cand = _name_from_clause(body)
        if not cand:
            continue
        hits.append((abs((when - pub).total_seconds()), cand, when))

    ev["n_posts_in_window"] = n
    ev["n_announcements"] = len(hits)
    if not hits:
        ev["refused"] = "no_show_announcement_in_window"
        return None, ev
    hits.sort()
    ev["candidates"] = {c: round(d / 3600.0, 1) for d, c, _ in hits[:5]}   # name -> hours away
    ev["selected"] = hits[0][1]
    ev["announced_iso"] = hits[0][2].isoformat()
    ev["hours_from_episode"] = round(hits[0][0] / 3600.0, 1)
    return hits[0][1], ev


def surname_of(full_name):
    """Last token of a resolved name, keeping O'Hanlon / Al Mulla intact."""
    if not full_name:
        return None
    toks = _norm(full_name).split()
    if not toks:
        return None
    if len(toks) >= 3 and toks[-2].lower() in ("al", "el", "van", "von", "de", "da", "bin"):
        return f"{toks[-2]} {toks[-1]}"
    return toks[-1]


if __name__ == "__main__":
    import sys
    iso = sys.argv[1] if len(sys.argv) > 1 else "2026-08-22T11:55:34Z"
    show = sys.argv[2] if len(sys.argv) > 2 else "GU"
    name, ev = resolve_guest(iso, show)
    print(json.dumps({"name": name, "surname": surname_of(name), "evidence": ev},
                     indent=1, ensure_ascii=False))
