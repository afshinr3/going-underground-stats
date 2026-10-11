#!/usr/bin/env python3
"""X → Telegram / Instagram / LinkedIn / Rednote video cross-poster.

    python main.py              # daemon: poll every POLL_INTERVAL_SECONDS
    python main.py --once       # single cycle (cron / systemd timer)
    python main.py --dry-run    # discover + print captions; publish nothing, persist nothing
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
from monitors.x_monitor import build_monitor, download_video, handles
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


def build_publishers(s: Settings, only: list[str] | None) -> dict[str, Publisher]:
    pubs = {}
    for name in s.enabled_platforms():
        if only and name not in only:
            continue
        p = REGISTRY[name](s)
        try:
            p.validate()
        except PublishError as e:
            log.error("publisher disabled: misconfigured", extra={"platform": name, "error": str(e)})
            continue
        pubs[name] = p
    return pubs


def discover(monitor, state: StateStore, only_handle: str | None) -> int:
    new = 0
    for handle in handles():
        if only_handle and handle.lower() != only_handle.lstrip("@").lower():
            continue
        try:
            posts = monitor.fetch_new(handle)
        except Exception:
            log.exception("discovery failed", extra={"handle": handle})
            continue
        for post in posts:
            if state.record_post(post):
                new += 1
                log.info("new video post", extra={"handle": handle, "tweet_id": post["tweet_id"], "url": post["permalink"]})
    return new


def process(post: dict, pubs: dict[str, Publisher], state: StateStore, s: Settings) -> None:
    tid = post["tweet_id"]
    platforms = list(pubs)
    remaining = state.platforms_remaining(tid, platforms, s.max_attempts)
    if not remaining:
        _finish(post, pubs, state, s)
        return
    try:
        video = download_video(post, s)
    except Exception as e:
        log.exception("download failed", extra={"tweet_id": tid})
        for p in remaining:
            state.mark_delivery(tid, p, ok=False, error=f"download: {e}")
        _finish(post, pubs, state, s)
        return

    for name in remaining:
        caption = build_caption(post, name)
        t0 = time.time()
        try:
            res = pubs[name].publish(video, caption, post)
            state.mark_delivery(tid, name, ok=True, remote_id=res.remote_id)
            log.info("published", extra={"platform": name, "tweet_id": tid, "remote_id": res.remote_id,
                                         "url": res.url, "seconds": round(time.time() - t0, 1)})
        except Exception as e:
            state.mark_delivery(tid, name, ok=False, error=str(e))
            log.error("publish failed", exc_info=not isinstance(e, PublishError),
                      extra={"platform": name, "tweet_id": tid, "error": str(e)})
    _finish(post, pubs, state, s)


def _finish(post: dict, pubs: dict[str, Publisher], state: StateStore, s: Settings) -> None:
    """Mark done/failed once nothing is left to retry, and delete the downloaded video."""
    tid = post["tweet_id"]
    platforms = list(pubs)
    if state.platforms_remaining(tid, platforms, s.max_attempts):
        return  # keep video on disk for the next retry
    exhausted = state.platforms_exhausted(tid, platforms, s.max_attempts)
    state.set_post_status(tid, "failed" if exhausted else "done")
    if exhausted:
        log.error("gave up on platforms", extra={"tweet_id": tid, "platforms": exhausted})
    for f in s.temp_dir.glob(f"{tid}.*"):
        f.unlink(missing_ok=True)
    log.info("post complete", extra={"tweet_id": tid, "status": "failed" if exhausted else "done"})


def cycle(monitor, pubs, state, s, only_handle=None) -> None:
    discover(monitor, state, only_handle)
    if not pubs:
        log.warning("no publishers configured; posts stay pending")
        return
    for post in state.pending_posts():
        if _stop:
            break
        process(post, pubs, state, s)


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
    args = ap.parse_args()

    s = load_settings()
    setup_logging(s)
    if args.dry_run:
        dry_run(s, args.handle)
        return 0

    def _sig(*_):
        global _stop
        _stop = True
        log.info("shutdown requested")

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)

    state = StateStore(s.db_path)
    monitor = build_monitor(s, state)
    pubs = build_publishers(s, args.platforms.split(",") if args.platforms else None)
    log.info("starting", extra={"platforms": list(pubs), "handles": handles(), "once": args.once})

    while not _stop:
        try:
            cycle(monitor, pubs, state, s, args.handle)
        except Exception:
            log.exception("cycle crashed")
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
