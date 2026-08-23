#!/usr/bin/env python3
"""DERIVED_FEED_CONSISTENCY_V1_2026_08_23 — do the derived feeds still follow the source?

WHY THIS EXISTS
---------------
On 2026-08-23 the 22 Aug episode was repaired in videos.json by an out-of-band
commit (c8744db2, author timezone +0400 — the M2, not a UTC runner). That commit
rewrote the canonical feed and left the derived one alone: stats_1week_gu.json
still carried generated_at=11:33:19 and n=0 while videos.json already held the
restored row. The leaderboard read empty for one cycle and nothing anywhere said
the two files disagreed. The pipeline itself is correctly ordered —
_generate_weekly_stats() runs after update_show() saves — so no run was at
fault; the gap only opens when something writes the canonical feed WITHOUT
running the generator. Nothing detected that, which is the actual defect.

THE INVARIANT
-------------
For every (canonical source -> derived feed) pair:

    derived_feed_on_disk == _generate_weekly_stats()(canonical_source_on_disk)

after normalising the fields that are explicitly non-semantic. Equality is
checked over: every top-level payload field, the SET of entry records, their
MULTIPLICITY (no duplicates), their ORDER, and every field within each record.

Only `generated_at` is normalised away. It is a wall-clock stamp that moves on
every regeneration and carries no information about the feed's contents — the
bridge already discards it for the same reason when deciding whether a weekly
file is worth committing. Nothing else is normalised. In particular ENTRY ORDER
IS SEMANTIC: _generate_weekly_stats() emits entries in canonical-source order
with no sort of its own, so a feed whose entries are in a different order was
not produced by the current generator from the current source, and that is
exactly the divergence this check exists to catch.

THE 45-DAY RETENTION RULE IS POLICY, NOT DRIFT
----------------------------------------------
_url_bind_cleanup_and_backfill() intentionally drops canonical rows that match
no RSS entry and are older than STALE_DAYS_DROP_UNBOUND days. That constant is
read out of fetch_and_push.py at runtime rather than copied, so this checker
cannot drift from the policy it is honouring; if it cannot be read, the check
reports itself unverifiable rather than guessing.

The production predicate is `_age_days > STALE_DAYS_DROP_UNBOUND`, so the exact
boundary is: at 45 days a record is RETAINED, at 46 days it is dropped. A record
still present in the derived feed but gone from the canonical source is
therefore an intentional retirement only when its age is strictly greater than
45 days. At exactly 45 it is a real mismatch, because policy says the canonical
row should still have been there.

SAFETY
------
The check NEVER writes to the live tree. Canonical sources are snapshotted into
a private temp directory, the generator is pointed at that directory, and the
live feeds' bytes are re-read afterwards to prove they were untouched. Every
write this program makes is atomic (write to .tmp, then os.replace) and lands
only inside the temp directory, which is removed on exit. To stay correct while
a concurrent run is committing, the canonical bytes are read twice and the check
restarts if they moved; if they will not settle, the result is "unverifiable"
(nonzero) rather than a fabricated verdict.

OUTPUT
------
    [DERIVED_FEED_CONSISTENT]
    [DERIVED_FEED_MISMATCH] type=<t> id=<feed:record> field=<f>

Exit 0 only when every pair is consistent; 1 on mismatch; 2 when the check
cannot be performed at all.
"""

import contextlib
import datetime as _dt
import io
import json
import os
import re
import shutil
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))

# (canonical source, derived feed, show code) — mirrors _generate_weekly_stats().
# Each derived payload records its own `source_feed`, which is asserted below so
# this table cannot silently disagree with the generator.
FEED_PAIRS = (
    ("videos.json", "stats_1week_gu.json", "GU"),
    ("videos_neworder.json", "stats_1week_no.json", "NO"),
)

# The only field whose value carries no information about feed CONTENT.
NON_SEMANTIC_TOP_LEVEL = ("generated_at",)

EXIT_CONSISTENT = 0
EXIT_MISMATCH = 1
EXIT_UNVERIFIABLE = 2

MAX_MISMATCH_LINES = 60


class Unverifiable(Exception):
    """The check could not be performed; never report a verdict in this case."""


# --------------------------------------------------------------------------
# policy


def stale_days(source_path=None):
    """Read STALE_DAYS_DROP_UNBOUND from fetch_and_push.py — never hardcode it.

    It is a local inside _url_bind_cleanup_and_backfill(), so it cannot be
    imported. Reading it keeps one source of truth: if the retention policy
    changes, this checker follows it instead of quietly enforcing the old one.
    """
    path = source_path or os.path.join(HERE, "fetch_and_push.py")
    try:
        with open(path) as f:
            src = f.read()
    except Exception as e:
        raise Unverifiable(f"cannot read {os.path.basename(path)}: {e}")
    m = re.search(r"^\s*STALE_DAYS_DROP_UNBOUND\s*=\s*(\d+)", src, re.M)
    if not m:
        raise Unverifiable("STALE_DAYS_DROP_UNBOUND not found in fetch_and_push.py")
    return int(m.group(1))


_MONS = {"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
         "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12}


def parse_feed_date(dstr, today):
    """Parse the feed's "%d %b" date the same way the generator does.

    Year is inferred: a date that would fall in the future belongs to last year.
    """
    m = re.match(r"(\d+)\s+([A-Za-z]+)", dstr or "")
    if not m:
        return None
    mon = _MONS.get(m.group(2)[:3].lower())
    if not mon:
        return None
    try:
        d = _dt.date(today.year, mon, int(m.group(1)))
    except ValueError:
        return None
    if d > today:
        try:
            d = _dt.date(today.year - 1, mon, int(m.group(1)))
        except ValueError:
            return None
    return d


def age_days(entry, today):
    d = parse_feed_date(entry.get("date"), today)
    return None if d is None else (today - d).days


def is_intentional_retirement(entry, canonical_ids, today, stale):
    """True when this record's absence from the canonical source is POLICY.

    Mirrors `_age_days > STALE_DAYS_DROP_UNBOUND` exactly: 45 days is retained,
    46 is dropped. Only applies to a record the canonical source no longer has.
    """
    if record_id(entry) in canonical_ids:
        return False
    age = age_days(entry, today)
    return age is not None and age > stale


# --------------------------------------------------------------------------
# records


def record_id(entry):
    """Stable identity for one episode record, most durable key first."""
    for key in ("canonical_video_id", "canonical_episode_id_v2", "canonical_episode_id"):
        val = entry.get(key)
        if val:
            return str(val)
    return "{}|{}".format(entry.get("surname") or "", entry.get("date") or "")


def _finding(kind, feed, rec="-", field="-"):
    return {"type": kind, "id": f"{feed}:{rec}" if rec != "-" else feed, "field": field}


def compare_feed(feed, expected, live, canonical_ids, today, stale):
    """Compare one derived feed against what the generator would produce.

    Returns (findings, retirements). `retirements` are records whose absence
    from the canonical source is the documented 45-day policy; they are reported
    for visibility but are NOT mismatches.
    """
    findings, retirements = [], []

    if not isinstance(live, dict):
        return [_finding("unreadable", feed, "-", "payload")], []

    declared = live.get("source_feed")
    expected_src = expected.get("source_feed")
    if declared is not None and declared != expected_src:
        findings.append(_finding("field", feed, "-", "source_feed"))

    # ---- top-level payload fields (entries handled separately)
    for key in sorted(set(expected) | set(live)):
        if key == "entries" or key in NON_SEMANTIC_TOP_LEVEL:
            continue
        if expected.get(key) != live.get(key):
            findings.append(_finding("field", feed, "-", key))

    exp_entries = expected.get("entries") or []
    live_entries = live.get("entries") or []

    exp_ids = [record_id(e) for e in exp_entries]
    live_ids = [record_id(e) for e in live_entries]

    # ---- duplicates
    seen = {}
    for rid in live_ids:
        seen[rid] = seen.get(rid, 0) + 1
    for rid, n in seen.items():
        if n > 1:
            findings.append(_finding("duplicate", feed, rid, "count"))

    exp_by_id = {}
    for e in exp_entries:
        exp_by_id.setdefault(record_id(e), e)
    live_by_id = {}
    for e in live_entries:
        live_by_id.setdefault(record_id(e), e)

    # ---- missing / extra
    for rid in exp_ids:
        if rid not in live_by_id:
            findings.append(_finding("missing", feed, rid))
    for rid in live_ids:
        if rid in exp_by_id:
            continue
        entry = live_by_id[rid]
        if is_intentional_retirement(entry, canonical_ids, today, stale):
            retirements.append(rid)
        else:
            findings.append(_finding("extra", feed, rid))

    # ---- per-record field divergence
    for rid in exp_ids:
        live_entry = live_by_id.get(rid)
        if live_entry is None:
            continue
        exp_entry = exp_by_id[rid]
        for key in sorted(set(exp_entry) | set(live_entry)):
            if exp_entry.get(key) != live_entry.get(key):
                findings.append(_finding("field", feed, rid, key))

    # ---- order (only meaningful once membership matches)
    if sorted(exp_ids) == sorted(live_ids) and exp_ids != live_ids:
        findings.append(_finding("order", feed, "-", "entry_order"))

    return findings, retirements


# --------------------------------------------------------------------------
# regeneration


def _read_bytes(path):
    try:
        with open(path, "rb") as f:
            return f.read()
    except FileNotFoundError:
        return None
    except Exception as e:
        raise Unverifiable(f"cannot read {os.path.basename(path)}: {e}")


def _atomic_write(path, data):
    tmp = path + ".tmp"
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _snapshot(root, names, attempts=3):
    """Read files twice; retry while a concurrent writer is moving them."""
    for _ in range(attempts):
        first = {n: _read_bytes(os.path.join(root, n)) for n in names}
        second = {n: _read_bytes(os.path.join(root, n)) for n in names}
        if first == second:
            return first
    raise Unverifiable("canonical sources kept changing under a concurrent run")


def regenerate_expected(root, tmpdir):
    """Run the real generator against a snapshot, in a temp dir. Never writes live."""
    sys.path.insert(0, root)
    try:
        import fetch_and_push as FP
    except Exception as e:
        raise Unverifiable(f"cannot import fetch_and_push: {e}")

    canonical_names = [src for src, _o, _c in FEED_PAIRS]
    derived_names = [out for _s, out, _c in FEED_PAIRS]

    snap = _snapshot(root, canonical_names)
    live_before = {n: _read_bytes(os.path.join(root, n)) for n in derived_names}

    for name, data in snap.items():
        if data is None:
            raise Unverifiable(f"canonical source missing: {name}")
        _atomic_write(os.path.join(tmpdir, name), data)

    original_root = FP.ROOT
    try:
        FP.ROOT = tmpdir
        # The generator narrates to stdout; keep this program's output strictly
        # machine-readable by swallowing it.
        with contextlib.redirect_stdout(io.StringIO()):
            FP._generate_weekly_stats()
    except Exception as e:
        raise Unverifiable(f"generator failed: {e}")
    finally:
        FP.ROOT = original_root

    # Prove the live tree was untouched by the regeneration.
    live_after = {n: _read_bytes(os.path.join(root, n)) for n in derived_names}
    if live_after != live_before:
        raise Unverifiable("live derived feed changed during the check — aborting")

    expected = {}
    for _src, out, _code in FEED_PAIRS:
        raw = _read_bytes(os.path.join(tmpdir, out))
        if raw is None:
            raise Unverifiable(f"generator produced no {out}")
        expected[out] = json.loads(raw)
    live = {}
    for name, raw in live_before.items():
        if raw is None:
            raise Unverifiable(f"derived feed missing: {name}")
        try:
            live[name] = json.loads(raw)
        except Exception as e:
            raise Unverifiable(f"derived feed unparseable: {name}: {e}")
    canonical = {}
    for name, raw in snap.items():
        canonical[name] = json.loads(raw)
    return expected, live, canonical


def main(argv=None):
    root = HERE
    today = _dt.date.today()
    tmpdir = tempfile.mkdtemp(prefix="derived_feed_check_")
    try:
        stale = stale_days()
        expected, live, canonical = regenerate_expected(root, tmpdir)
    except Unverifiable as e:
        print(f"[DERIVED_FEED_MISMATCH] type=unverifiable id=- field=-", flush=True)
        print(f"  reason: {e}", file=sys.stderr)
        return EXIT_UNVERIFIABLE
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    all_findings, all_retired = [], []
    for src, out, _code in FEED_PAIRS:
        canonical_ids = {record_id(r) for r in (canonical.get(src) or [])}
        findings, retired = compare_feed(
            out, expected[out], live[out], canonical_ids, today, stale)
        all_findings.extend(findings)
        all_retired.extend(f"{out}:{r}" for r in retired)

    for rid in all_retired:
        print(f"  [DERIVED_FEED_RETENTION] id={rid} intentional_drop_older_than_{stale}d")

    if not all_findings:
        print("[DERIVED_FEED_CONSISTENT]")
        return EXIT_CONSISTENT

    for f in all_findings[:MAX_MISMATCH_LINES]:
        print(f"[DERIVED_FEED_MISMATCH] type={f['type']} id={f['id']} field={f['field']}")
    if len(all_findings) > MAX_MISMATCH_LINES:
        print(f"[DERIVED_FEED_MISMATCH] type=truncated id=- "
              f"field={len(all_findings) - MAX_MISMATCH_LINES}_more")
    return EXIT_MISMATCH


if __name__ == "__main__":
    sys.exit(main())
