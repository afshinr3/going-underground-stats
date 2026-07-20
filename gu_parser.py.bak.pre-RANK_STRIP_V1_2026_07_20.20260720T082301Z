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
    """Strip leading role/honorific tokens from a candidate name."""
    name = re.sub(
        r'^(?:(?:Ex|Former|Fmr|Acting|Deputy|Senior|Chief|Head)[\s.-]*)*'
        r'(?:Israeli\s+|US\s+|UK\s+|British\s+|American\s+)?'
        r'(?:Intel\s+|Intelligence\s+)?(?:Acting\s+)?'
        r'(?:President|PM|Prime\s+Minister|Minister|Officer|Ambassador|Amb|MP|'
        r'Director|Head|Chief|Senator|Congressman|General|Admiral|Secretary|'
        r'Advisor|Analyst|Spokesperson|Editor|Professor|Commander|Colonel|Captain|'
        r'Major|Sgt\.?|Lt\.?\s*Col\.?|Dr\.?|Prof\.?)\.?\s+',
        '', name, flags=re.I
    ).strip()
    return name


def extract_guest(title, source="unknown"):
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
    paths_tried.append("honorific")
    honorific_match = re.match(
        r'^(?:Prof|Dr|Mr|Mrs|Ms|Sir|Lady|Sen|Rep|Ambassador|Amb|Col|Gen|Lt|Capt|Maj|'
        r'Hon|Rabbi|Imam|Rev|Sgt|Baroness|Lord)\.?\s+'
        r'([A-Z][a-zA-ZÀ-ÿ\-]+(?:\s+[A-Z][a-zA-ZÀ-ÿ\-]+){1,2}?)'
        r'(?=\s+(?:on|in|of|at|for|with|to|from|by|and|or|but|Explains|Says|Argues|'
        r'Discusses|Talks|Reveals|Warns|Why|How|What|When|Where|Who|That|Which|will|'
        r'could|would|should|is|are|was|were|has|have|had|tells|told|shares|gives)\b'
        r"|[\'\":,.\-——–]|\s*$)",
        title
    )
    if honorific_match:
        return honorific_match.group(1).strip()

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
        looks_like_name = bool(re.match(r"^[A-Z][a-zA-Z\.'\-]+(?:\s+[A-Z][a-zA-Z\.'\-]+){0,3}$", cand))
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

    # All patterns failed — emit structured rejection and return None.
    paths_tried.append("FALLTHROUGH_NO_MATCH")
    _emit_rejection(title, paths_tried, source)
    return None


def extract_surname(guest_name):
    """Return just the surname (last whitespace-separated token, with role tokens stripped)."""
    if not guest_name:
        return None
    name = (guest_name.replace('(Jim) ', '').replace('Lt. Col. ', '')
                      .replace('Dr. ', '').replace('Prof. ', '').replace('Sgt. ', ''))
    name = re.sub(r'_R\d{1,2}[A-Z][a-z]{2}.*$', '', name).strip()
    parts = name.strip().split()
    if not parts:
        return None
    return parts[-1]


def cluster_id_for(title, pub_iso, source="unknown"):
    """Convenience: return 'surname_YYYY-MM-DD' or None on parse failure."""
    g = extract_guest(title, source=source)
    if not g: return None
    sn = extract_surname(g)
    if not sn: return None
    return f"{sn.lower()}_{(pub_iso or '')[:10]}"
