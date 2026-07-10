#!/usr/bin/env python3
# CANONICAL_PUBLISH_V1_2026_07_10 — one-shot backfill for the current
# videos.json / videos_neworder.json so the live raw endpoint shows
# canonical full names immediately (before the next scheduled fetch).
import os, sys, json
sys.path.insert(0, "/Users/afshin/going-underground-stats")
import fetch_and_push as fp

ROOT = "/Users/afshin/going-underground-stats"
TARGETS = ["videos.json", "videos_neworder.json"]

def backfill(path):
    p = os.path.join(ROOT, path)
    if not os.path.exists(p):
        print(f"SKIP {path}: missing"); return
    with open(p) as f: cache = json.load(f)
    resolved = unchanged = 0
    for v in cache:
        try:
            title = v.get("title") or ""
            cur = v.get("guest") or ""
            res = fp.resolve_guest_identity(title, source="canonical_backfill_v1")
            canon = res.get("guest") if isinstance(res, dict) else None
            if not canon and title:
                try:
                    kdb = fp._load_known_guests()
                    for shortname, rec in (kdb or {}).items():
                        if not isinstance(rec, dict): continue
                        cn = rec.get("canonical")
                        if not cn or not isinstance(cn, str): continue
                        sn = cn.split()[-1] if cn.split() else ""
                        if len(sn) >= 5 and sn in title:
                            canon = cn; break
                except Exception: pass
            if canon:
                v["canonical_guest_full_name"] = canon
                bad = False
                if not cur: bad = True
                elif cur.endswith(" "): bad = True
                elif cur.startswith(("Ex-", "Former ", "Fmr ", "SLAMS ", "BLASTS ", "REVEALS ", "EXPOSES ", "WARNS ")): bad = True
                else:
                    last = cur.split()[-1] if cur.split() else ""
                    if last[:1].isupper() and last.endswith(('rat','ing','tio','ion','ent','ies','nes')) and len(last) < 12:
                        bad = True
                if bad or cur != canon:
                    v["guest"] = canon
                resolved += 1
            else:
                v.setdefault("canonical_guest_full_name", None)
                unchanged += 1
        except Exception as e:
            v.setdefault("canonical_guest_full_name", None)
            unchanged += 1
    with open(p, "w") as f: json.dump(cache, f, indent=2)
    print(f"BACKFILLED {path}: resolved={resolved} unchanged={unchanged} n_total={len(cache)}")
    for i, x in enumerate(cache[:16]):
        print(f"  #{i} guest={x.get('guest')!r} canonical={x.get('canonical_guest_full_name')!r}")

if __name__ == "__main__":
    for t in TARGETS: backfill(t)
