#!/usr/bin/env python3
"""GU_GUEST_FROM_DESCRIPTION_V1_20260928 — recover a guest name from the episode's OWN
YouTube description when the title does not contain one.

THE FAILURE THIS ENDS
---------------------
YouTube caps a title at 100 characters. The show writes titles that end with the guest,
so the cap silently amputates exactly the part we need:

    YouTube (99 chars) "Americans Pay $2 BILLION A DAY for Iran War: Afshin Rattansi
                        Challenges Ex-Deputy CENTCOM Commander"
    the show's own X post, full (131 chars)
                       "... Challenges Ex-Deputy CENTCOM Commander Vice Admiral Robert
                        Harward"

`gu_parser.extract_guest` correctly returns None -- there is no name in the string.
`gu_guest_from_posts_v1` also refused: it binds only on the announcement phrase
"joined by <Name>", and the 2026-09-28 announcement used the headline shape instead.
So the 2026-09-28 Robert Harward episode was rejected on EVERY pipeline cycle from
07:06Z (30+ rejections in parser_rejections.jsonl) and never reached videos.json,
while the episode was live on YouTube and Rumble.

The name was never missing from our data. The show opens EVERY episode description with
its own convention, naming the guest explicitly:

    "On this episode of Going Underground, we speak to Vice Admiral Robert Harward,
     former Deputy Commander of US CENTCOM and Executive Vice President of Shield AI."

The description already travels with the entry in the YouTube RSS feed the pipeline
fetches (<media:description>), so this needs no new network source and works unchanged
in GitHub Actions. It also generalises the two manual workarounds this class had
accumulated: GUEST_BY_VIDEO_ID (one hand-written row per episode, "verified from the
video description") and CANON_MAP.

WHY THIS IS BETTER EVIDENCE THAN THE POSTS RESOLVER
---------------------------------------------------
The description is EPISODE-SCOPED. It travels in the same feed entry as the video id,
so there is no time window to tune and no possibility of a neighbouring episode's guest
claiming this one -- the class of defect that made the posts resolver asymmetric
(2026-08-07 Ünal resolved as 'Mearsheimer' from a post 47.2h away). It is consulted
BEFORE the posts resolver for that reason.

DESIGN RULES (same discipline as gu_guest_from_posts_v1)
--------------------------------------------------------
* **Never invent.** Returns None unless the show's own opener names someone. A refusal
  is a valid answer.
* **Convention-bound.** Only the "we speak to / we are joined by" opener is read, not
  any capitalised phrase in a 1,000-character description. A first draft that scored
  every name run in the description picked up the interviewer, the politicians
  discussed, and "Al Qaeda".
* **Rank-stripped, then person-checked.** "Vice Admiral Robert Harward" -> "Robert
  Harward". Person-ness is delegated to gu_guest_from_posts_v1._is_person_name, which
  already rejects roles, organisations, possessives and acronym runs.
* **Fail-soft.** Any parse error yields None; the caller keeps its existing behaviour.
"""
import re

import gu_guest_from_posts_v1 as _POSTS

MARKER = "GU_GUEST_FROM_DESCRIPTION_V1_20260928"

# The show's opener, in every shape seen across the 2026 feeds of both channels.
#   "On this episode of Going Underground, we speak to X"
#   "On this episode of New Order, we are joined by X"
#   "we spoke to X" / "we talk to X" / "we're joined by X"
_OPENER = re.compile(
    r"(?i:we(?:\s*(?:’|')?\s*(?:re|are|ll|will)\b)?\s*(?:be\s+)?"
    r"(?:speak(?:ing)?|spoke|talk(?:ing)?|sat\s+down)\s+(?:to|with)|"
    r"we(?:\s*(?:’|')?\s*(?:re|are|ll|will)\b)?\s*(?:be\s+)?joined\s+by)\s+(.+)"
)

# Military / civil ranks and honorifics that PRECEDE a name. Stripped before the
# person-check, because gu_guest_from_posts_v1._ROLE_WORDS deliberately treats
# "admiral"/"commander"/"colonel" as role words and would reject the whole run.
_RANK = re.compile(
    r"^(?:"
    r"Vice\s+Adm(?:iral)?\.?|Rear\s+Adm(?:iral)?\.?|Adm(?:iral)?\.?|"
    r"Lt\.?\s*Gen\.?|Maj\.?\s*Gen\.?|Brig\.?\s*Gen\.?|Lt\.?\s*Col\.?|"
    r"Colonel|Col\.?|General|Gen\.?|Major|Maj\.?|Captain|Capt\.?|"
    r"Commander|Cmdr\.?|Sergeant|Sgt\.?|CMSGT\.?|MSGT\.?|"
    r"Professor|Prof\.?|Doctor|Dr\.?|Mr\.?|Mrs\.?|Ms\.?|Sir|Dame|Lord|Lady|Baroness|"
    r"Ambassador|Amb\.?|Senator|Sen\.?|Congressman|Congresswoman|Rep\.?|"
    r"Rabbi|Imam|Rev\.?|Hon\.?|Ret(?:ired)?\.?"
    r")\s+", re.IGNORECASE)

_NAME_RUN = re.compile(_POSTS._NAME)

# The same rank vocabulary, searched ANYWHERE in the clause rather than only at its
# head, so "former Director of Shin Bet Admiral Ami Ayalon" yields "Ami Ayalon".
_RANK_INSIDE = re.compile(_RANK.pattern.replace("^", "", 1), re.IGNORECASE)

# A role noun directly abutting the name: "former Israeli negotiator Daniel Levy".
# Sourced from gu_guest_from_posts_v1._ROLE_WORDS so the two modules cannot drift.
_ROLE_ANCHOR = re.compile(
    r"\b(?:%s)\s+" % "|".join(sorted(re.escape(w) for w in _POSTS._ROLE_WORDS)),
    re.IGNORECASE)


def _strip_ranks(text):
    """Peel every leading rank/honorific: 'Ret. Vice Admiral Robert Harward' -> the name."""
    prev = None
    out = (text or "").strip()
    while out and out != prev:
        prev = out
        out = _RANK.sub("", out, count=1).strip()
    return out


def _anchored_candidates(clause):
    """Name runs ANCHORED to the opener or to a rank -- never a floating capitalised run.

    THE RULE, and why it is this narrow. A first draft took the LAST name run in the
    clause (the shape gu_guest_from_posts_v1 uses for "joined by <role> <Name>"). In a
    DESCRIPTION that is unsafe: the opener runs on into the interview summary, so
    "we speak to former Prime Minister of Israel Ehud Olmert" offers the run
    "Israel Ehud Olmert", which is name-shaped and would be published as a person.

    A guest is therefore only read where the show's own grammar puts one:
      1. immediately after the opener, once ranks are peeled ("we speak to
         [Vice Admiral] Robert Harward", "we speak to Dr. Michael O'Hanlon"), or
      2. immediately after a rank anywhere in the clause ("... Shin Bet Admiral
         Ami Ayalon"), or
      3. immediately after a role noun ("... former Israeli negotiator Daniel Levy").
         IMMEDIATELY is what keeps this safe: "Prime Minister of Israel Ehud Olmert"
         puts "of" between the role and the run, so it still refuses.
    Anything else is refused. That costs recall on descriptive openers with no rank,
    which is the correct trade: those episodes are resolvable from the title, and a
    wrong name is unrecoverable once it reaches the leaderboard.
    """
    out = []
    head = _NAME_RUN.match(_strip_ranks(clause))
    if head:
        out.append(head.group(0))
    for m in _RANK_INSIDE.finditer(clause):
        tail = _NAME_RUN.match(clause[m.end():].strip())
        if tail:
            out.append(tail.group(0))
    for m in _ROLE_ANCHOR.finditer(clause):
        tail = _NAME_RUN.match(clause[m.end():].strip())
        if tail:
            out.append(tail.group(0))
    return out


def guest_from_description(description):
    """Guest name from the episode's own description, or None.

    Returns (name, evidence_dict). The evidence is always returned so a refusal can be
    audited as readily as a hit.
    """
    ev = {"marker": MARKER, "opener_found": False, "clause": None, "candidates": []}
    text = (description or "").replace("&amp;", "&")
    if not text.strip():
        ev["refused"] = "empty_description"
        return None, ev

    m = _OPENER.search(text)
    if not m:
        ev["refused"] = "no_opener_phrase"
        return None, ev
    ev["opener_found"] = True

    # Cut the clause at the first comma / newline ONLY. The guest's role follows the
    # comma ("Robert Harward, former Deputy Commander of US CENTCOM"), and reading past
    # it is how a role becomes a name. Splitting on ". " as well truncated
    # "Dr. Michael O'Hanlon" to "Dr" and refused every honorific-first opener -- the
    # identical bug gu_guest_from_posts_v1._name_from_clause carries a comment about.
    # A name run cannot span a full stop anyway, so the sentence end needs no split.
    clause = re.split(r"[,\n]", m.group(1))[0].strip()
    ev["clause"] = clause[:120]
    if not clause:
        ev["refused"] = "empty_clause"
        return None, ev

    candidates = _anchored_candidates(clause)
    ev["candidates"] = candidates[:5]

    for cand in candidates:
        name = _POSTS._norm(cand)
        if name and _POSTS._is_person_name(name) and not _POSTS._blocked(name):
            ev["selected"] = name
            return name, ev

    ev["refused"] = "no_person_name_in_clause"
    return None, ev


def surname_of(full_name):
    return _POSTS.surname_of(full_name)
