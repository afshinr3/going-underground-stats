"""Caption cleaning and destination CTA routing.

Pipeline (see `build_caption`):
  1. unescape HTML entities X returns (&amp; -> &)
  2. expand t.co links to their real URL; drop links that only point at the attached
     media or at the quoted post (those mean nothing off X)
  3. strip the trailing "Watch the full interview in the quoted post below. 👇" CTA
     (and close variants, incl. "latest episode" phrasings the source account now uses)
  4. append the show-specific Rumble CTA
  5. fit to each platform's caption limit, truncating the body — never the CTA
"""
from __future__ import annotations

import html
import re

from config.settings import CAPTION_LIMITS, REDNOTE_TITLE_LIMIT, source_for

# SHOW_SPECIFIC_CTA_V1_2026_10_16 — the headline is per-show DATA on SourceAccount now
# (settings.SourceAccount.cta), not one global string. This module's own docstring already
# promised "the show-specific Rumble CTA" while only the URL was ever show-specific.
# Kept as the legacy constant ONLY so the stripper below documents what it removes.
LEGACY_CTA_HEADLINE = "Watch the full interview \U0001F447"

_ARROWS = "👇⬇️⬇🔽👉➡️⤵️"
_ARROW_CLASS = "[" + re.escape(_ARROWS) + "️]"

# A CTA sentence pointing at the quoted post / "below". Matched only at the end of the text.
_TRAILING_CTA = re.compile(
    r"""
    [\s\-–—:|]*                                   # separator before it
    (?:""" + _ARROW_CLASS + r"""\s*)*             # optional leading arrow
    (?:watch|see|catch|view|stream)\s+
    (?:the\s+|our\s+)?
    (?:full|whole|entire|complete|latest|newest)\s+
    (?:interview|episode|show|conversation|video|programme|program|clip|discussion)
    [^\n]*?                                       # "in the quoted post below", "here", ...
    (?:quoted?|below|here|link|thread|replies|👇|⬇)
    [^\n]*?                                       # trailing punctuation
    \s*(?:""" + _ARROW_CLASS + r"""\s*)*          # trailing arrows
    \Z
    """,
    re.IGNORECASE | re.VERBOSE,
)
_TRAILING_ARROWS = re.compile(r"[\s" + re.escape(_ARROWS) + r"️]+\Z")
_TRAILING_TCO = re.compile(r"\s*https?://t\.co/\w+\s*\Z")
_HASHTAG_LINE = re.compile(r"^(?:\s*[#$]\w+)+\s*$")


# --------------------------------------------------------------------------- helpers
def utf16_len(s: str) -> int:
    """Length in UTF-16 code units — what Telegram/Meta count, so emoji cost 2."""
    return len(s.encode("utf-16-le")) // 2


def expand_links(text: str, urls: list[dict] | None, quoted_ids: set[str] | None = None) -> str:
    """Replace t.co links with expanded URLs; drop media and quoted-post links."""
    quoted_ids = quoted_ids or set()
    for u in urls or []:
        short = u.get("url")
        if not short:
            continue
        expanded = u.get("unwound_url") or u.get("expanded_url") or short
        is_media = bool(u.get("media_key")) or re.search(r"/(?:video|photo)/\d+/?$", expanded or "")
        is_quote = any(f"/status/{qid}" in (expanded or "") for qid in quoted_ids)
        text = text.replace(short, "" if (is_media or is_quote) else expanded)
    return text


def strip_trailing_cta(text: str) -> str:
    """Remove the "watch the full interview in the quoted post below 👇" tail (repeatedly)."""
    prev = None
    text = text.rstrip()
    while prev != text:
        prev = text
        text = _TRAILING_TCO.sub("", text).rstrip()
        # Hashtags after the CTA must survive: peel them off, strip, then re-attach.
        lines = text.split("\n")
        tail: list[str] = []
        while lines and _HASHTAG_LINE.match(lines[-1]) and len(lines) > 1:
            tail.insert(0, lines.pop())
        core = "\n".join(lines).rstrip()
        core = _TRAILING_CTA.sub("", core).rstrip()
        core = _TRAILING_ARROWS.sub("", core).rstrip()  # a lone 👇 means nothing once the quote is gone
        text = "\n".join([core, *tail]).strip() if tail else core
    return text


# SCHEDULED_EPISODE_LINK_V1_20261016: "Don't miss it, follow us on X and follow our Rumble channel, link below in
# the replies 👇" means nothing on Facebook/Instagram/Telegram -- there are no X replies there. The line is dropped
# and the CTA carries the real link instead.
_REPLIES_LINK_LINE = re.compile(r"^[^\n]*\blinks?\s+(?:is\s+)?(?:below\s+)?in\s+(?:the\s+)?(?:replies|reply|comments|thread)\b[^\n]*$",
                                re.IGNORECASE | re.MULTILINE)


def mentions_reply_link(raw: str) -> bool:
    return bool(_REPLIES_LINK_LINE.search(html.unescape(raw or "")))


def clean_text(raw: str, urls: list[dict] | None = None, quoted_ids: set[str] | None = None) -> str:
    text = html.unescape(raw or "")
    text = _REPLIES_LINK_LINE.sub("", text)
    text = expand_links(text, urls, quoted_ids)
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return strip_trailing_cta(text).strip()


def cta_for(handle: str, episode_url: str | None = None) -> str:
    """Show-specific CTA + the scheduled episode's own Rumble link when known, else that show's channel."""
    src = source_for(handle)
    return f"{src.cta}\n{episode_url or src.rumble_url}"


def _truncate(body: str, budget: int) -> str:
    """Trim `body` to <= budget UTF-16 units on a word boundary, ending with an ellipsis."""
    if utf16_len(body) <= budget:
        return body
    if budget <= 1:
        return ""
    cut = body
    while cut and utf16_len(cut) > budget - 1:
        cut = cut[:-1]
    space = max(cut.rfind(" "), cut.rfind("\n"))
    if space > len(cut) * 0.6:
        cut = cut[:space]
    return cut.rstrip(" ,;:-–—\n") + "…"


def fit_caption(body: str, cta: str, limit: int, sep: str = "\n\n") -> str:
    full = f"{body}{sep}{cta}" if body else cta
    if utf16_len(full) <= limit:
        return full
    budget = limit - utf16_len(cta) - utf16_len(sep)
    body = _truncate(body, budget)
    return f"{body}{sep}{cta}" if body else cta[:limit]


# --------------------------------------------------------------------------- public API
def build_caption(post: dict, platform: str | None = None) -> str:
    """Final caption for `platform` (or the untruncated canonical caption if None)."""
    body = clean_text(post.get("text", ""), post.get("urls"), set(post.get("quoted_ids") or []))
    cta = cta_for(post["handle"], post.get("episode_url"))
    if platform is None:
        return f"{body}\n\n{cta}" if body else cta
    return fit_caption(body, cta, CAPTION_LIMITS[platform])


def build_title(post: dict, limit: int = REDNOTE_TITLE_LIMIT) -> str:
    """Short title (Rednote): first line/sentence of the cleaned body, hashtags removed."""
    body = clean_text(post.get("text", ""), post.get("urls"), set(post.get("quoted_ids") or []))
    first = re.split(r"(?<=[.!?。！？])\s|\n", body, maxsplit=1)[0]
    first = re.sub(r"[#$]\w+", "", first).strip() or source_for(post["handle"]).label
    if len(first) <= limit:
        return first
    return first[: limit - 1].rstrip() + "…"
