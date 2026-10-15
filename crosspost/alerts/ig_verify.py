"""IG_POST_VERIFY_V1_20261015 -- an Instagram "published" is not a post until it is still there later.

2026-10-15: the operator saw nothing on Instagram while the log said "published" with a reel link. Every one of
the 5 reels from 14 Oct 22:39 to 15 Oct 16:09 (+4 Dubai) had been accepted, given a permalink, then removed by
Instagram (GET media -> "does not exist"); 2 of 25 earlier ones had gone the same way. The pipeline only checked
at publish time, so removals were invisible. Each cycle this re-reads recent Instagram deliveries once they are
VERIFY_AFTER_S old: still there -> verified; gone -> the delivery becomes 'removed' and a Telegram alert fires.
Checks repeat until KEEP_CHECKING_S, because a takedown can come hours later. Nothing is re-posted
automatically (re-uploading what Instagram removed can worsen the account's standing).
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

import requests

log = logging.getLogger("alerts.ig_verify")

VERIFY_AFTER_S = 600
KEEP_CHECKING_S = 24 * 3600
RECHECK_EVERY_S = 3 * 3600


def _age_s(iso: str) -> float:
    try:
        t = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        return time.time() - t.timestamp()
    except Exception:  # noqa: BLE001
        return -1


def exists(s, media_id: str) -> bool | None:
    """True / False, or None when Instagram could not be asked (never treated as removed)."""
    try:
        r = requests.get(f"https://{s.instagram_graph_host}/{s.meta_graph_version}/{media_id}",
                         params={"fields": "id", "access_token": s.instagram_access_token}, timeout=20)
        body = r.json()
    except Exception:  # noqa: BLE001
        return None
    if "id" in body:
        return True
    err = body.get("error") or {}
    if err.get("code") == 100 and err.get("error_subcode") == 33:
        return False
    return None


def verify(state, s, notifier) -> dict:
    out = {"checked": 0, "removed": [], "unknown": 0}
    rows = state._conn.execute(
        "SELECT tweet_id, remote_id, updated_at FROM deliveries WHERE platform='instagram' AND status='success'"
        " AND remote_id IS NOT NULL").fetchall()
    for tweet_id, media_id, updated in rows:
        age = _age_s(updated)
        if age < VERIFY_AFTER_S or age > KEEP_CHECKING_S + RECHECK_EVERY_S:
            continue
        k = f"igverify:{tweet_id}"
        last = float(state.get(k, "0") or 0)
        if last and time.time() - last < RECHECK_EVERY_S:
            continue
        ok = exists(s, media_id)
        out["checked"] += 1
        if ok is None:
            out["unknown"] += 1
            continue
        state.set(k, str(time.time()))
        if not ok:
            with state._tx() as c:
                c.execute("UPDATE deliveries SET status='removed', last_error=? WHERE tweet_id=? AND platform='instagram'",
                          ("removed by Instagram after publish (media %s no longer exists)" % media_id, tweet_id))
            out["removed"].append(tweet_id)
            log.warning("instagram post removed after publish", extra={"tweet_id": tweet_id, "remote_id": media_id})
    if out["removed"]:
        notifier.alert("instagram_removed",
                       "Instagram REMOVED %d reel(s) after accepting them (tweets %s). They are not on the account. "
                       "Check Instagram > Settings > Account status for a copyright or content restriction."
                       % (len(out["removed"]), ", ".join(out["removed"][:5])))
    return out
