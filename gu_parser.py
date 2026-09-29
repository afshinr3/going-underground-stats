"""gu_parser.py — shared guest/surname extraction for the Going Underground / New Order pipelines.

2026-06-20 v1: single source of truth for `extract_guest`, `extract_surname`, `_strip_role`.
Consumed by:
  - going-underground-stats/fetch_and_push.py    (upstream X/YT/IG scrape; GH Actions)
  - going-underground-stats/episode_cluster.py   (M2 canonical cluster registry)
  - going_underground_book_rebuild/auto_update.py (M3 Substack draft pipeline)

No duplicate parser logic anywhere. To update parsing rules: edit THIS file +
add a regression case to regression_tests_gu_titles.json, then push. M3 syncs
via a curl-from-raw.githubusercontent step at the top of its cron job.

ANTI-SILENT-FAILURE (Layer 3): when extract_guest returns None, a structured
rejection record is appended to parser_rejections.jsonl. The drift monitor
watches that file and alerts on every entry.
"""
import re, json, os, datetime
from pathlib import Path

# Rejection log path — env-overridable for tests; default writes alongside
# fetch_and_push.py in the going-underground-stats repo.
REJECTIONS_PATH = os.environ.get(
    "GU_PARSER_REJECTIONS_PATH",
    str(Path(__file__).resolve().parent / "parser_rejections.jsonl"),
)
PARSER_VERSION = "v5_2026_06_20"


def _now_iso():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _emit_rejection(title, paths_tried, source):
    """Append a structured rejection record. Best-effort, never raises."""
    try:
        # Heuristic: "rejected tokens" = title words split, the first 5 capitalised words.
        # Useful for debugging: lets a human see what the parser stared at.
        toks = re.findall(r"\b[A-Z][A-Za-z\-\.\']+\b", title)[:6]
        rec = {
            "iso": _now_iso(),
            "parser_version": PARSER_VERSION,
            "title": title,
            "regex_paths_tried": paths_tried,
            "candidate_capitalised_tokens": toks,
            "source": source,
        }
        with open(REJECTIONS_PATH, "a") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception:
        pass  # never let logging failure block discovery


def _strip_role(name):
    """Strip leading role/honorific tokens from a candidate name.

    RANK_STRIP_V1_2026_07_20 — added US enlisted ranks (CMSGT, MSGT, SFC, SSG,
    SGM, CSM, PFC, CPL, MSgt, GySgt, SSgt, TSgt, SrA, A1C, Amn, PO1, PO2, PO3,
    CPO, SCPO, MCPO, ENS, LTJG, CDR, LCDR, RADM, VADM) and "Ret." prefix.
    Fixes CMSGT-Fritz name leak (2026-07-13 episode → guest="CMSGT. Dennis Fritz").
    """
    # RANK_STRIP_V1_2026_07_20 — strip "Ret." / "Retired" leading token first.
    name = re.sub(r'^(?:Ret|Retired)\.?\s+', '', name, flags=re.I).strip()
    # RANK_STRIP_V1_2026_07_20 — enlisted-rank strip runs first (dedicated pass).
    # These are ALL-CAPS abbreviations that were slipping through the officer-only
    # honorific list (which knew Col/Gen/Lt/Capt/Maj but not enlisted ranks).
    name = re.sub(
        r'^(?:CMSGT|CSM|SGM|SMA|MSGT|MSG|SFC|SSG|SGT|CPL|PFC|PVT|SPC|'
        r'MSgt|GySgt|SSgt|TSgt|SrA|A1C|Amn|'
        r'PO1|PO2|PO3|CPO|SCPO|MCPO|ENS|LTJG|CDR|LCDR|RADM|VADM|'
        r'CW[0-9]|CWO|WO[0-9])\.?\s+',
        '', name).strip()
    # RANK_STRIP_V1_2026_07_20 — order matters: longer compound ranks first
    # (Lt. Col., Lt. Gen., Maj. Gen., Brig. Gen.) so "Lt. Col." isn't shredded to "Col.".
    name = re.sub(
        r'^(?:Lt\.?\s*Col\.?|Lt\.?\s*Gen\.?|Maj\.?\s*Gen\.?|Brig\.?\s*Gen\.?|'
        r'Vice\s+Adm\.?|Rear\s+Adm\.?)\s+',
        '', name, flags=re.I
    ).strip()
    name = re.sub(
        r'^(?:(?:Ex|Former|Fmr|Acting|Deputy|Senior|Chief|Head)[\s.-]*)*'
        r'(?:Israeli\s+|US\s+|UK\s+|British\s+|American\s+)?'
        r'(?:Intel\s+|Intelligence\s+)?(?:Acting\s+)?'
        r'(?:President|PM|Prime\s+Minister|Minister|Officer|Ambassador|Amb|MP|'
        r'Director|Head|Chief|Senator|Congressman|General|Gen|Admiral|Adm|Secretary|'
        r'Advisor|Analyst|Spokesperson|Editor|Professor|Commander|Colonel|Col|'
        r'Captain|Capt|Major|Maj|Lt|Sgt|Dr|Prof)\.?\s+',
        '', name, flags=re.I
    ).strip()
    return name


# ROLE_STRIP_REPEAT_V1_2026_09_28 — `_strip_role` is ONE pass over a chain of alternatives,
# so it stops at the first token it cannot name. Real GU billing stacks three or four role
# tokens: "Ex-Deputy CENTCOM Commander Vice Admiral Robert Harward" defeated it at token two
# ("CENTCOM" is in no list, so the role branch never matched and the whole string came back
# unchanged). Three gaps, fixed together because any one alone still leaves the name buried:
#   * the chain must REPEAT until the string stops changing;
#   * spelled-out compound ranks — it knew "Vice Adm." but not "Vice Admiral";
#   * ALL-CAPS commands/agencies sitting in the role slot (CENTCOM, NATO, CIA, IDF...).
# The ALL-CAPS strip is bounded deliberately: 2-8 characters, and only when named text
# follows, so it can never consume the last token or an initialised surname. A real personal
# name is not ALL-CAPS in this corpus and `_looks_like_name()` rejects one that is.
_ALLCAPS_ROLE_ORG = re.compile(
    r'^(?:CENTCOM|AFRICOM|SOUTHCOM|NORTHCOM|EUCOM|INDOPACOM|SOCOM|STRATCOM|'
    r'NATO|CIA|FBI|NSA|DIA|DoD|DOD|UN|EU|IDF|MI5|MI6|GCHQ|FCO|FCDO|DHS|ICE|'
    r'IAEA|OPCW|WHO|IMF|USAID|DNI|JCS|NSC)\s+(?=\S)')
_SPELLED_COMPOUND_RANK = re.compile(
    r'^(?:Vice\s+Admiral|Rear\s+Admiral|Lieutenant\s+(?:Colonel|General|Commander)|'
    r'Major\s+General|Brigadier\s+General|Master\s+Sergeant|Staff\s+Sergeant|'
    r'Chief\s+Master\s+Sergeant|Command\s+Sergeant\s+Major|Sergeant\s+Major)\s+',
    re.I)


def _strip_role_full(name):
    """`_strip_role` applied until the string is stable. Never returns None."""
    if not name:
        return name
    prev = None
    guard = 0
    while name and name != prev and guard < 8:
        prev = name
        guard += 1
        name = _SPELLED_COMPOUND_RANK.sub('', name).strip()
        name = _ALLCAPS_ROLE_ORG.sub('', name).strip()
        name = _strip_role(name)
    return name


# TRAILING_NAME_V1_2026_09_28 — read the billing from the RIGHT, not the left.
#
# Stripping role tokens left-to-right needs a complete list of role NOUNS, and it will
# never have one: "Ex-Deputy CENTCOM Commander Vice Admiral Robert Harward" deadlocks
# because "Ex-Deputy " wants a role noun after it and "CENTCOM" is in no list, while
# "Ex-Israeli Negotiator Daniel Levy" and "Ex-UK Defence Minister Tobias Ellwood" each
# fail on one unlisted noun. The person's name is always the TAIL of a billing phrase, so
# walk in from the right and stop at the first token that is not a plausible personal-name
# token. An unknown role noun then costs a rejection, never a wrong name — the failure
# direction that matters, because a fabricated surname is what put 4.4M invented views
# into a "measured" feed.
_ROLE_STOP_WORDS = {
    # ranks and honorifics
    "admiral", "general", "colonel", "commander", "captain", "major", "lieutenant",
    "sergeant", "corporal", "private", "brigadier", "marshal", "vice", "rear",
    "professor", "prof", "doctor", "dr", "sir", "dame", "lord", "lady", "rabbi",
    "imam", "reverend", "rev", "mr", "mrs", "ms", "hon", "ret", "retired",
    # offices and billing nouns
    "president", "premier", "chancellor", "minister", "secretary", "ambassador",
    "envoy", "negotiator", "adviser", "advisor", "analyst", "officer", "official",
    "director", "chief", "head", "deputy", "senator", "congressman", "congresswoman",
    "mp", "mep", "governor", "mayor", "spokesperson", "spokesman", "spokeswoman",
    "editor", "journalist", "correspondent", "author", "economist", "historian",
    "lawyer", "attorney", "judge", "inspector", "whistleblower", "candidate",
    "leader", "member", "founder", "chairman", "ceo", "banker", "diplomat",
    "commissioner", "prosecutor", "researcher", "scientist", "psychologist",
    "philosopher", "activist", "veteran", "agent", "operative", "strategist",
    # nationality / department adjectives that sit in the billing slot
    "israeli", "palestinian", "american", "british", "russian", "chinese", "iranian",
    "ukrainian", "german", "french", "italian", "spanish", "turkish", "saudi",
    "egyptian", "iraqi", "syrian", "lebanese", "indian", "pakistani", "afghan",
    "japanese", "korean", "canadian", "australian", "dutch", "greek", "brazilian",
    "mexican", "venezuelan", "cuban", "european", "western", "defence", "defense",
    "foreign", "national", "intelligence", "intel", "military", "army", "navy",
    "air", "force", "state", "treasury", "justice", "interior", "senior", "acting",
    "former", "ex", "fmr", "un", "nato", "eu",
}


def _is_name_token(tok):
    """One token that could belong to a personal name in this corpus."""
    core = tok.strip(".,'’-")
    if len(core) < 2 or not core[0].isupper():
        return False
    if not all(ch.isalpha() or ch in ".'’-" for ch in core):
        return False
    # ALL-CAPS is a command, agency or shouted headline word here, never a person.
    if core.isupper():
        return False
    if core.lower() in _ROLE_STOP_WORDS:
        return False
    # "Ex-Deputy", "Ex-Israeli" — a hyphen compound whose head is a role prefix.
    if "-" in core and core.split("-")[0].lower() in _ROLE_STOP_WORDS:
        return False
    return True


def _trailing_person_name(phrase, max_tokens=3):
    """Last 2-3 name-like tokens of a billing phrase, or None.

    Requires at least TWO tokens on purpose: a lone trailing surname is too weak an anchor
    to distinguish "…Challenges Ex-Deputy CENTCOM Commander" from a real name.
    """
    if not phrase:
        return None
    toks = [t for t in re.split(r'\s+', phrase.strip()) if t]
    picked = []
    for tok in reversed(toks):
        if len(picked) >= max_tokens or not _is_name_token(tok):
            break
        picked.insert(0, tok.strip(",."))
    if len(picked) < 2:
        return None

    # A NAME IS ATTACHED TO A BILLING, NOT TO PROSE. Caught by replaying the rejection log
    # against this very function: "Afshin Rattansi CHALLENGES Ex-CIA Advisor on the Legacy
    # of America's Wars" names no guest, and the right-to-left walk happily returned
    # "America's Wars" -- a fabricated guest, and cluster_id "wars_<date>". The tell is the
    # token in front of it: a billing phrase reaches the name through a ROLE word
    # ("...Admiral Robert Harward", "...ambassador Chas Freeman"), whereas headline prose
    # reaches it through an ordinary connective ("...Legacy of America's Wars"). So the
    # preceding token must be absent, or a role word. Everything else is prose and is
    # refused -- the whole point of this path is that it may find nothing.
    _before_idx = len(toks) - len(picked) - 1
    if _before_idx >= 0:
        _before = toks[_before_idx].strip(".,'’-").lower()
        if _before.split("-")[0] not in _ROLE_STOP_WORDS \
                and _before not in _ROLE_STOP_WORDS:
            return None

    cand = " ".join(picked)
    return cand if _looks_like_name(cand) else None


# NAME_PARTICLE_V1_2026_09_28 — closed list; middle positions only. See _looks_like_name.
_NAME_PARTICLES = {"da", "de", "del", "della", "der", "den", "di", "do", "dos", "das",
                   "du", "van", "von", "bin", "ibn", "al", "el", "la", "le", "ter",
                   "ten", "af", "y", "e", "st"}


_NVL_VERBS = ("Reveals", "Explains", "Says", "Argues", "Discusses", "Talks", "Warns",
              "Slams", "Analyses", "Analyzes", "Tells", "Shares", "Challenges", "Confirms",
              "Predicts", "Claims", "Believes", "Uncovers", "Exposes", "Details",
              "Describes", "Debates", "Comments", "Reports", "Breaks")
# Title case AND the shouted form, longest first so "Analyses" cannot be clipped by a prefix.
_NVL_VERBS_ALT = sorted({v for v in _NVL_VERBS} | {v.upper() for v in _NVL_VERBS},
                        key=len, reverse=True)


def _looks_like_name(cand):
    """Does `cand` look like 1-4 capitalised name tokens?

    NON_ASCII_GUEST_NAME_V1_2026_08_09 — this replaced
        r"^[A-Z][a-zA-Z\\.'\\-]+(?:\\s+[A-Z][a-zA-Z\\.'\\-]+){0,3}$"
    whose character classes were ASCII-only, so ANY guest with an accented or
    non-English letter failed the colon branch and fell through to
    FALLTHROUGH_NO_MATCH. "Professor Hasan Ünal: ..." and "Gabor Maté: ..." were
    both dropped as unparseable (the Ünal episode never reached videos.json at
    all, and the GU week showed n=0 with the episode live and on 5.6k views),
    while the identical ASCII title "Hasan Unal: ..." parsed fine. Every other
    pattern in this module already allows À-ÿ; only this one did not.

    str.isupper()/isalpha() are Unicode-aware, so this covers Turkish, Spanish,
    Nordic and every other alphabet without enumerating codepoint ranges the way
    a character class would.
    """
    parts = cand.split()
    if not 1 <= len(parts) <= 4:
        return False
    for i, p in enumerate(parts):
        core = p.strip(".'-")
        if not core:
            return False
        if not core[0].isupper():
            # NAME_PARTICLE_V1_2026_09_28 — every token had to start uppercase, so any name
            # carrying a lowercase nobiliary/patronymic particle was rejected outright:
            # _looks_like_name("Lula da Silva") was False, and Brazil's president therefore
            # fell through the colon and name_on paths on every episode he appeared in.
            # Allowed only from a CLOSED list and only in a MIDDLE position, so a particle
            # can never be the whole surname or open the name — which is what keeps this
            # from admitting ordinary lowercase prose.
            if not (0 < i < len(parts) - 1 and core.lower() in _NAME_PARTICLES):
                return False
        if not all(ch.isalpha() or ch in ".'-" for ch in p):
            return False
    return True


def extract_guest(title, source="unknown"):
    """Public entry point. Delegates, then sanitises what comes back.

    MULTILINE_TITLE_V1_2026_09_29 — several feeds carry a title that is really a whole
    post: "NEW EPISODE OF NEW ORDER \\n\\nBRICS 'UP & RUNNING': ... — C. Uday Bhaskar\\n\\nHow is
    India navigating..." The dash path matched the tail and returned the name WITH the
    paragraph after it glued on. Found by differencing this module against the private copy
    in auto_update.py over the 92-title corpus — the copy got it right and this module did
    not, which is the whole reason that comparison is run before any consolidation.

    A guest name never spans a line break, so cut at the first one. Done HERE, once, rather
    than at each of the dozen return points inside the matcher, so no future path can skip it.
    """
    g = _extract_guest_impl(title, source=source)
    if not isinstance(g, str):
        return g
    g = g.split("\n")[0].split("\r")[0].strip(" \t\u00a0\u2014\u2013-,;:")
    # Cut at the line break ONLY. Re-validating with _looks_like_name() here was too
    # strict and dropped "James (Jim) Webb", which the matcher had returned correctly for
    # months and which the corpus asserts with tolerant_match — the paths that need
    # validation already do it themselves.
    return g or None


def _extract_guest_impl(title, source="unknown"):
    """Extract guest name from a YouTube/RSS episode title.

    Returns the guest name (str) or None when no pattern matches. On None,
    emits a structured rejection record to parser_rejections.jsonl.
    `source` is a tag like 'fetch_and_push' / 'episode_cluster' / 'auto_update'
    so rejections can be grouped by caller.
    """
    title = title.strip()
    title = title.replace('\u2018', "'").replace('\u2019', "'").replace('\u201c', '"').replace('\u201d', '"')
    title = re.sub(r'^[\W_]+', '', title).strip()
    paths_tried = []

    # A. Ex-{Nationality} {Role} {Name} {ALL-CAPS-VERB or particle}...
    paths_tried.append("ex_role")
    ex_role_match = re.match(
        r'^(?:Ex|Former|Fmr)[\s\-]+'
        r'(?:(?:Israeli|US|UK|British|American|EU|French|German|Russian|Chinese|Iranian|'
        r'Saudi|Indian|Pakistani|Turkish|Egyptian|Iraqi|Syrian|Palestinian|Lebanese|'
        r'Jordanian|Greek|Italian|Spanish|Dutch|Brazilian|Mexican|Canadian|Australian|'
        r'Japanese|Korean|Thai|Filipino|Indonesian|Vietnamese|African|European)\s+)?'
        r'(?:President|PM|Prime\s+Minister|Minister|Officer|Ambassador|Amb|Director|Head|'
        r'Chief|Senator|Congressman|Congresswoman|MP|General|Admiral|Secretary|Advisor|'
        r'Adviser|Analyst|Spokesperson|Editor|Professor|Commander|Colonel|Captain|Major|'
        r'Sgt\.?|Lt\.?\s*Col\.?|Dr\.?|Prof\.?|VP|Vice\s+President|Deputy|CEO|CFO|'
        r'Mayor|Governor|Judge|Justice)\s+'
        r'([A-Z][a-zA-ZÀ-ÿ\-]+(?:\s+[A-Z][a-zA-ZÀ-ÿ\-]+){1,2}?)'
        r'(?=\s+(?:[A-Z]{2,}|on|in|of|at|for|with|to|from|by|and|or|but|'
        r'Explains|Says|Argues|Discusses|Talks|Reveals|Warns|Why|How|What|When|Where|'
        r'Who|That|Which|will|could|would|should|is|are|was|were|has|have|had|tells|'
        r'told|shares|gives)\b|[\'\":,.\-——–]|\s*$)',
        title
    )
    if ex_role_match:
        return ex_role_match.group(1).strip()

    # Honorific {Name} {verb-or-terminator} — possessive ' supported in lookahead.
    # RANK_STRIP_V1_2026_07_20 — added enlisted ranks to the leading honorific list
    # so "CMSGT. Dennis Fritz: ..." captures "Dennis Fritz" instead of falling
    # through to the colon branch (which used to keep "CMSGT. Dennis Fritz").
    paths_tried.append("honorific")
    honorific_match = re.match(
        r'^(?:Prof|Dr|Mr|Mrs|Ms|Sir|Lady|Sen|Rep|Ambassador|Amb|Col|Gen|Lt|Capt|Maj|'
        r'Hon|Rabbi|Imam|Rev|Sgt|Baroness|Lord|'
        r'CMSGT|MSGT|SFC|SSG|SGM|CSM|SMA|CPL|PFC|PVT|SPC|'
        r'MSgt|GySgt|SSgt|TSgt|SrA|A1C|'
        r'PO1|PO2|PO3|CPO|SCPO|MCPO|ENS|LTJG|CDR|LCDR|RADM|VADM|'
        r'CWO|CW2|CW3|CW4|CW5|WO1|WO2|WO3|WO4|WO5|'
        r'Ret|Retired)\.?\s+'
        # RANK_STRIP_V1_2026_07_20 — first token may itself be a nested rank
        # like "Gen." / "Col." (Ret. Gen. Wesley Clark). Allow trailing dot
        # inside the token, then _strip_role peels it.
        r'([A-Z][a-zA-ZÀ-ÿ\-]+\.?(?:\s+[A-Z][a-zA-ZÀ-ÿ\-]+){1,2}?)'
        r'(?=\s+(?:on|in|of|at|for|with|to|from|by|and|or|but|Explains|Says|Argues|'
        r'Discusses|Talks|Reveals|Warns|Why|How|What|When|Where|Who|That|Which|will|'
        r'could|would|should|is|are|was|were|has|have|had|tells|told|shares|gives)\b'
        r"|[\'\":,.\-——–]|\s*$)",
        title
    )
    if honorific_match:
        # RANK_STRIP_V1_2026_07_20 — post-strip so double-honorifics
        # like "Ret. Col." peel down to just the name.
        return _strip_role(honorific_match.group(1).strip()) or honorific_match.group(1).strip()

    paths_tried.append("paren")
    paren = re.search(r'\(([^)]+)\)\s*$', title)
    if paren:
        guest = _strip_role(paren.group(1).strip())
        if guest and len(guest) > 3:
            return guest
        return paren.group(1).strip()

    paths_tried.append("name_on")
    name_on = re.match(r'^(?:\S+\'s\s+)?([A-Z][a-z]+(?:\s+[A-Z][a-z-]+)+)\s+on\s+', title)
    if name_on:
        return name_on.group(1)

    paths_tried.append("dash_terminal")
    dash_match = re.split(r'\s*[–—]\s*|\s+-\s+|-\s+(?=[A-Z](?:[a-z]|x-|ormer))', title)
    if len(dash_match) >= 2:
        guest = _strip_role(dash_match[-1].strip())
        if guest and len(guest) > 3:
            return guest
        return dash_match[-1].strip()

    paths_tried.append("colon")
    colon_match = re.match(r'^([^:]{2,40}):\s+(.*)', title)
    if colon_match:
        cand = _strip_role(colon_match.group(1).strip())
        rest = colon_match.group(2).strip()
        is_all_caps = cand == cand.upper() and len(cand) > 2
        looks_like_name = _looks_like_name(cand)
        if looks_like_name and not is_all_caps and 3 < len(cand) <= 40:
            return cand
        # 4b "topic: Honorific Name on rest"
        paths_tried.append("colon_name_on_after")
        m = re.match(
            r'^(?:(?:Prof|Dr|Lt\.?\s*Col|Sgt|Mr|Mrs|Ms|Sir|Amb)\.?\s+)?'
            r'([A-Z][a-z]+(?:\s+[A-Z][a-zA-Z\-]+){1,3})\s+on\s+', rest)
        if m: return m.group(1).strip()
        # 4c "topic: Honorific Name <verb>"
        paths_tried.append("colon_name_verb_after")
        m = re.match(
            r'^(?:(?:Prof|Dr|Mr|Mrs|Ms|Sir|Lady|Sen|Rep|Ambassador|Amb|Col|Gen|Lt|Capt|'
            r'Maj|Hon|Rabbi|Imam|Rev|Sgt)\.?\s+)?'
            r'([A-Z][a-zA-ZÀ-ÿ\-]+(?:\s+[A-Z][a-zA-ZÀ-ÿ\-]+){1,2}?)'
            r'(?=\s+(?:explains|says|argues|discusses|talks|reveals|warns|tells|told|'
            r'shares|gives|will|could|would|should|is|are|was|were|has|have|had)\b)',
            rest, flags=re.IGNORECASE)
        if m: return m.group(1).strip()
        # 4d (PATCH D): TOPIC: {1-3 descriptor words} {Name} {ALL-CAPS-VERB}
        paths_tried.append("colon_generic_pre_name_caps_verb")
        m = re.match(
            r'^(?:[A-Z][a-zA-Z]+\s+){1,3}'
            r'([A-Z][a-zA-ZÀ-ÿ\-]+(?:\s+[A-Z][a-zA-ZÀ-ÿ\-]+){1,2}?)'
            r'(?=\s+(?:[A-Z]{2,}|explains|says|argues|reveals|warns|tells|on)\b)', rest)
        if m: return m.group(1).strip()
        # 4e (PATCH E 2026-06-20): "TOPIC: Ex-{Nationality}? {Role} {Name} {particle/verb}..."
        paths_tried.append("colon_ex_role_name_verb")
        m = re.match(
            r'^(?:Ex|Former|Fmr)[\s\-]+'
            r'(?:(?:Israeli|US|UK|British|American|EU|French|German|Russian|Chinese|Iranian|'
            r'Saudi|Indian|Pakistani|Turkish|Egyptian|Iraqi|Syrian|Palestinian|Lebanese|'
            r'Jordanian|Greek|Italian|Spanish|Dutch|Brazilian|Mexican|Canadian|Australian)\s+)?'
            r'(?:President|PM|Prime\s+Minister|Minister|Officer|Ambassador|Amb|Director|Head|'
            r'Chief|Senator|Congressman|MP|General|Admiral|Secretary|Advisor|Adviser|Analyst|'
            r'Spokesperson|Editor|Professor|Commander|Colonel|Captain|Major|Dr\.?|Prof\.?)\s+'
            r'([A-Z][a-zA-ZÀ-ÿ\-]+(?:\s+[A-Z][a-zA-ZÀ-ÿ\-]+){1,2}?)'
            r'(?=\s+(?:on|in|of|at|for|with|to|and|or|but|explains|says|argues|reveals|'
            r'warns|tells|told|shares|gives|will|could|would|should|is|are|was|were|has|'
            r'have|had|[A-Z]{2,})\b|[\'\":,.\-——–]|\s*$)',
            rest, flags=re.IGNORECASE)
        if m: return m.group(1).strip()

    # EXTRACT_GUEST_NAME_VERB_V1_2026_07_04 — "<Guest Full Name> <Verb> ...", e.g.
    # "Max Blumenthal Reveals Why ...", "Peter Schiff Explains ...". Ported here on
    # 2026-08-09 from the duplicate copy of this parser that had grown inside
    # fetch_and_push.py: that copy alone could read these two live episodes, so this
    # module had to gain the pattern before production could be pointed at it.
    paths_tried.append("name_verb_leading")
    name_verb_leading = re.match(
        r"^([A-ZÀ-Þ][a-zà-ÿ]+(?:\s+[A-ZÀ-Þ][a-zA-ZÀ-ÿ\-']+){1,3}?)\s+"
        # CAPS_VERB_V1_2026_09_28 -- this alternation was Title-case only, but GU shouts its
        # verbs: "Dennis Kucinich SLAMS Integration of US & Israeli Military" matched nothing
        # because the pattern held "Slams" and the title said "SLAMS". Two live Kucinich
        # episodes fell through to FALLTHROUGH_NO_MATCH on exactly this. Both cases are
        # listed explicitly rather than by re.IGNORECASE, because IGNORECASE would also
        # loosen the NAME group ahead of it and let lowercase prose ("If the dollar...")
        # start looking like a guest.
        + r"(?:" + "|".join(_NVL_VERBS_ALT) + r")\b",
        title
    )
    if name_verb_leading:
        cand = name_verb_leading.group(1).strip()
        if cand.lower() not in ("afshin rattansi", "afshin"):
            return cand

    # INTERVIEWER_ANCHORED_V1_2026_09_28 — the house style changed and the parser did not.
    #
    # Current GU titles are "<long clickbait headline>: Afshin Rattansi <verb> <billing>
    # <Name>", e.g. "Americans Pay $2 BILLION A DAY for the Iran War: Afshin Rattansi
    # Challenges Ex-Deputy CENTCOM Commander Vice Admiral Robert Harward". Every existing
    # path missed it, and the `colon` family never even ran: its prefix pattern is
    # `^([^:]{2,40}):` and that headline is 49 characters, so sub-paths 4b-4e were
    # unreachable. The observable consequence is on the live Substack — EP 1449 (27 Jun,
    # article 203827430) published with the intro "Afshin Rattansi speaks with 'The US
    # LACKS THE POWER TO DEFEAT IRAN'", i.e. the HEADLINE where the guest belongs, and a
    # hardcoded override in totals_pusher.py papered over the same episode for the
    # LaMetric/Tidbyt path only — one caller fixed, the other left wrong.
    #
    # The interviewer's own name is the most reliable anchor in the title: it is fixed, and
    # `name_verb_leading` above already encodes that Afshin is never the guest. Anchoring on
    # it makes the headline length irrelevant, which is why this needs no change to the
    # colon cap.
    #
    # Deliberately placed LAST, as a fallthrough rescue: every title the parser already
    # resolves is resolved identically, so this can only convert a rejection into a match.
    # It is also allowed to find nothing — the truncated variant of this same episode
    # ("...Afshin Rattansi Challenges Ex-Deputy CENTCOM") has no name in it at all, and
    # inventing one from "CENTCOM" is exactly the junk-surname laundering that put 4.4M
    # fabricated views into a "measured" feed. No name, no guess.
    paths_tried.append("interviewer_anchored")
    _iv = re.search(
        r'\bAfshin\s+Rattansi\s+'
        r'(?:Challenges?|Confronts?|Questions?|Grills?|Interviews?|Asks?|Meets?|'
        r'Speaks?\s+(?:to|with)|Talks?\s+(?:to|with)|Debates?|Presses?)\s+'
        r'(.+)$', title, flags=re.I)
    if _iv:
        _tail = _iv.group(1).strip().strip('\'"“”‘’').strip()
        _guest = _trailing_person_name(_tail)
        if _guest and _guest.lower() not in ("afshin rattansi", "afshin"):
            return _guest

    # All patterns failed — emit structured rejection and return None.
    paths_tried.append("FALLTHROUGH_NO_MATCH")
    _emit_rejection(title, paths_tried, source)
    return None


def extract_surname(guest_name):
    """Return just the surname, with role tokens, cache suffixes and generational suffixes
    stripped and hyphenated surnames kept whole.

    SURNAME_SINGLE_SOURCE_V1_2026_09_29 — this was the WEAKEST of three copies, so folding
    the other two onto it would have been a regression. Both capabilities are ported in
    first, each for a reason already paid for:

      * generational suffixes (Jr./Sr./II/III/IV), from fetch_and_push. "Paulo Nogueira
        Batista Jr." yielded the surname "Jr.", which the validity filter rejects, so the
        13 Sep New Order episode was dropped at ingest as SKIP(unparseable) and never
        reached videos_neworder.json (GU_SURNAME_GENERATIONAL_SUFFIX_V1_20260917).
      * hyphenated surnames ("Ben-Menashe"), from auto_update, which is the only copy that
        handled a surname split across a trailing hyphen.
      * the BROADER cache-key strip `_R[A-Za-z0-9]{2,10}$` (fetch_and_push) in place of the
        narrow `_R\\d{1,2}[A-Z][a-z]{2}` this copy carried, which missed e.g. "_R8May".
    """
    if not guest_name:
        return None
    name = (guest_name.replace('(Jim) ', '').replace('Lt. Col. ', '')
                      .replace('Dr. ', '').replace('Prof. ', '').replace('Sgt. ', ''))
    name = re.sub(r'_R[A-Za-z0-9]{2,10}$', '', str(name)).strip()
    parts = name.split()
    # A generational suffix is never the surname; drop it and take the token before it.
    while len(parts) >= 2 and parts[-1].rstrip('.,').lower() in ('jr', 'sr', 'ii', 'iii', 'iv'):
        parts = parts[:-1]
    if not parts:
        return None
    last = parts[-1]
    # "Ben- Menashe" -> "Ben-Menashe": the surname was split across a trailing hyphen.
    if len(parts) >= 2 and parts[-2].endswith('-'):
        return parts[-2] + last
    return last


def cluster_id_for(title, pub_iso, source="unknown"):
    """Convenience: return 'surname_YYYY-MM-DD' or None on parse failure."""
    g = extract_guest(title, source=source)
    if not g: return None
    sn = extract_surname(g)
    if not sn: return None
    return f"{sn.lower()}_{(pub_iso or '')[:10]}"
