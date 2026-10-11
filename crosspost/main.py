#!/usr/bin/env python3
"""X → Telegram / Instagram / LinkedIn / Rednote video cross-poster.

    python main.py              # daemon: poll every POLL_INTERVAL_SECONDS
    python main.py --once       # single cycle (cron / systemd timer)
    python main.py --dry-run    # discover + print captions; publish nothing, persist nothing
    python main.py --check-credentials   # renew Instagram, print every token's expiry
"""
from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import time

from config.settings import Settings, load_settings
from db.state import StateStore
from alerts.notifier import Notifier
from alerts.tokens import apply_overrides as apply_token_overrides
from alerts.tokens import maintain as maintain_tokens
from monitors.x_monitor import XAuthError, build_monitor, download_video, handles
from publishers import REGISTRY, PublishError, Publisher
from transformers.caption import build_caption

log = logging.getLogger("crosspost")
_stop = False

_STD_ATTRS = set(vars(logging.LogRecord("", 0, "", 0, "", None, None))) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        out = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        out.update({k: v for k, v in vars(record).items() if k not in _STD_ATTRS})
        if record.exc_info:
            out["exc"] = self.formatException(record.exc_info)
        return json.dumps(out, ensure_ascii=False, default=str)


def setup_logging(s: Settings) -> None:
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(JsonFormatter() if s.log_json else logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    logging.basicConfig(level=s.log_level.upper(), handlers=[h], force=True)


def build_publishers(s: Settings, only: list[str] | None, notifier: Notifier | None = None) -> dict[str, Publisher]:
    pubs = {}
    for name in s.enabled_platforms():
        if only and name not in only:
            continue
        p = REGISTRY[name](s)
        try:
            p.validate()
        except PublishError as e:
            log.error("publisher disabled: misconfigured", extra={"platform": name, "error": str(e)})
            if notifier:
                notifier.alert(f"config:{name}", f"{name} is enabled but not configured, so it is skipped: {e}")
            continue
        if notifier:
            notifier.resolve(f"config:{name}", f"{name} is configured and active.")
        pubs[name] = p
    return pubs


def discover(monitor, state: StateStore, only_handle: str | None, notifier: Notifier | None = None) -> int:
    new = 0
    for handle in handles():
        if only_handle and handle.lower() != only_handle.lstrip("@").lower():
            continue
        fails_key = f"discover_fails:{handle.lower()}"
        try:
            posts = monitor.fetch_new(handle)
        except Exception as e:
            log.exception("discovery failed", extra={"handle": handle})
            fails = int(state.get(fails_key, "0") or 0) + 1
            state.set(fails_key, str(fails))
            # Login problems alert at once; anything else only once it keeps happening.
            if notifier and (isinstance(e, XAuthError) or fails >= notifier.s.discovery_fail_threshold):
                notifier.alert(f"discover:{handle.lower()}",
                               f"can't read @{handle} on X ({fails} failed check(s) in a row): {e}")
            continue
        state.set(fails_key, "0")
        if notifier:
            notifier.resolve(f"discover:{handle.lower()}", f"reading @{handle} on X works again.")
        for post in posts:
            if state.record_post(post):
                new += 1
                log.info("new video post", extra={"handle": handle, "tweet_id": post["tweet_id"], "url": post["permalink"]})
    return new


def process(post: dict, pubs: dict[str, Publisher], state: StateStore, s: Settings, notifier: Notifier) -> None:
    tid = post["tweet_id"]
    platforms = list(pubs)
    remaining = state.platforms_remaining(tid, platforms, s.max_attempts)
    if not remaining:
        _finish(post, pubs, state, s, notifier)
        return
    try:
        video = download_video(post, s)
    except Exception as e:
        log.exception("download failed", extra={"tweet_id": tid})
        for p in remaining:
            state.mark_delivery(tid, p, ok=False, error=f"download: {e}")
        notifier.alert("download", f"couldn't download the video for {post['permalink']}: {e}")
        _finish(post, pubs, state, s, notifier)
        return
    notifier.resolve("download", "video downloads work again.")

    for name in remaining:
        caption = build_caption(post, name)
        t0 = time.time()
        try:
            res = pubs[name].publish(video, caption, post)
            state.mark_delivery(tid, name, ok=True, remote_id=res.remote_id)
            log.info("published", extra={"platform": name, "tweet_id": tid, "remote_id": res.remote_id,
                                         "url": res.url, "seconds": round(time.time() - t0, 1)})
            notifier.resolve(f"publish:{name}", f"{name} posting works again.")
        except Exception as e:
            state.mark_delivery(tid, name, ok=False, error=str(e))
            log.error("publish failed", exc_info=not isinstance(e, PublishError),
                      extra={"platform": name, "tweet_id": tid, "error": str(e)})
            notifier.alert(f"publish:{name}", f"{name} publish failed for {post['permalink']}: {e}")
    _finish(post, pubs, state, s, notifier)


def _finish(post: dict, pubs: dict[str, Publisher], state: StateStore, s: Settings, notifier: Notifier) -> None:
    """Mark done/failed once nothing is left to retry, and delete the downloaded video."""
    tid = post["tweet_id"]
    platforms = list(pubs)
    if state.platforms_remaining(tid, platforms, s.max_attempts):
        return  # keep video on disk for the next retry
    exhausted = state.platforms_exhausted(tid, platforms, s.max_attempts)
    state.set_post_status(tid, "failed" if exhausted else "done")
    if exhausted:
        log.error("gave up on platforms", extra={"tweet_id": tid, "platforms": exhausted})
        errs = "; ".join(f"{p}: {(state.delivery(tid, p) or {}).get('last_error', '')[:200]}" for p in exhausted)
        notifier.alert(f"gaveup:{tid}", f"gave up on {', '.join(exhausted)} after {s.max_attempts} tries "
                                        f"for {post['permalink']}. Post it manually. {errs}")
    for f in s.temp_dir.glob(f"{tid}.*"):
        f.unlink(missing_ok=True)
    log.info("post complete", extra={"tweet_id": tid, "status": "failed" if exhausted else "done"})


def _check_gap(state: StateStore, s: Settings, notifier: Notifier) -> None:
    """Notice when cycles stopped for a while (Mac asleep, logged out, agent dead)."""
    now = time.time()
    last = float(state.get("cycle:last", "0") or 0)
    state.set("cycle:last", str(now))
    if last and now - last > s.stale_gap_hours * 3600:
        hrs = (now - last) / 3600
        notifier.send(f"ℹ️ crosspost: back after {hrs:.1f} h without checking X (Mac asleep or logged out?). "
                      "Catching up now; posts from that window still go out.")


def cycle(monitor, pubs, state, s, notifier: Notifier, only_handle=None) -> None:
    _check_gap(state, s, notifier)
    try:
        maintain_tokens(s, state, notifier)
    except Exception:
        log.exception("credential check crashed")
    discover(monitor, state, only_handle, notifier)
    if not pubs:
        log.warning("no publishers configured; posts stay pending")
        return
    for post in state.pending_posts():
        if _stop:
            break
        process(post, pubs, state, s, notifier)


def dry_run(s: Settings, only_handle: str | None) -> None:
    state = StateStore(":memory:")  # nothing persisted, since_id untouched
    monitor = build_monitor(s, state)
    discover(monitor, state, only_handle)
    for post in state.pending_posts():
        print("=" * 72)
        print(f"{post['permalink']}  video={post.get('video_url')}")
        for p in ("telegram", "instagram", "linkedin", "rednote"):
            print(f"--- {p} ---\n{build_caption(post, p)}\n")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--once", action="store_true", help="run one cycle and exit")
    ap.add_argument("--dry-run", action="store_true", help="discover and print captions only")
    ap.add_argument("--handle", help="restrict to one source handle")
    ap.add_argument("--platforms", help="comma list subset, e.g. telegram,linkedin")
    ap.add_argument("--check-credentials", action="store_true", help="renew/check all tokens now and exit")
    args = ap.parse_args()

    s = load_settings()
    setup_logging(s)
    if args.dry_run:
        dry_run(s, args.handle)
        return 0

    state = StateStore(s.db_path)
    notifier = Notifier(s, state)
    if args.check_credentials:
        for k, v in maintain_tokens(s, state, notifier, force=True).items():
            print(f"{k:10s} {'never expires / unknown' if not v else time.strftime('%Y-%m-%d', time.gmtime(v))}")
        return 0

    def _sig(*_):
        global _stop
        _stop = True
        log.info("shutdown requested")

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)

    apply_token_overrides(s, state)  # pick up an auto-renewed Instagram token before validating
    monitor = build_monitor(s, state)
    pubs = build_publishers(s, args.platforms.split(",") if args.platforms else None, notifier)
    log.info("starting", extra={"platforms": list(pubs), "handles": handles(), "once": args.once,
                                "alerts": notifier.enabled})

    while not _stop:
        ok = True
        try:
            cycle(monitor, pubs, state, s, notifier, args.handle)
        except Exception as e:
            ok = False
            log.exception("cycle crashed")
            notifier.alert("cycle", f"a check cycle crashed: {e}")
        notifier.heartbeat(ok)
        if args.once:
            break
        for _ in range(s.poll_interval_seconds):
            if _stop:
                break
            time.sleep(1)
    state.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
