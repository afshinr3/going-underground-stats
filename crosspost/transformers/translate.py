"""Simplified-Chinese captions for Rednote (operator request 2026-10-12).

Uses the Claude Code CLI installed on the Mac (`claude -p`), so no extra API key. Returns None on
any failure so the caller falls back to the English text: a post in English beats no post.
"""
from __future__ import annotations

import logging
import shutil
import subprocess

log = logging.getLogger("crosspost.translate")

CLAUDE = shutil.which("claude") or "/opt/homebrew/bin/claude"
MODEL = "claude-sonnet-5"

CAPTION_PROMPT = (
    "Translate this social-media caption into natural Simplified Chinese for a Xiaohongshu (Rednote) post. "
    "Keep every URL, @handle and emoji exactly as they are. Give people's names in standard Chinese "
    "transliteration followed by the English name in brackets the first time they appear. Keep the meaning "
    "and tone; do not add or remove claims. Output only the translation, nothing else.\n\n"
)
TITLE_PROMPT = (
    "Write a Simplified Chinese title for a Xiaohongshu (Rednote) video post about the caption below. "
    "At most {limit} Chinese characters, faithful to the content, no quotation marks, no emoji. "
    "Output only the title.\n\n"
)


def _run(prompt: str, timeout: int = 180) -> str | None:
    try:
        r = subprocess.run([CLAUDE, "-p", "--model", MODEL], input=prompt, capture_output=True,
                           text=True, timeout=timeout)
    except Exception as e:  # noqa: BLE001
        log.warning("translation unavailable", extra={"error": repr(e)[:200]})
        return None
    out = (r.stdout or "").strip()
    if r.returncode != 0 or not out:
        log.warning("translation failed", extra={"rc": r.returncode, "stderr": (r.stderr or "")[-200:]})
        return None
    return out


def caption_zh(caption: str, limit: int) -> str | None:
    out = _run(CAPTION_PROMPT + caption)
    if out and len(out) > limit:
        out = out[: limit - 1] + "…"
    return out


def title_zh(caption: str, limit: int) -> str | None:
    out = _run(TITLE_PROMPT.format(limit=limit) + caption)
    if not out:
        return None
    out = out.strip().strip("\"'“”「」").splitlines()[0].strip()
    return out[:limit] or None
