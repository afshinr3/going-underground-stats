#!/usr/bin/env python3
# CANONICAL_PUBLISH_V1_2026_07_10 v2 — self-contained one-shot backfill.
import os, sys, json

ROOT = "/Users/afshin/going-underground-stats"
TARGETS = ["videos.json", "videos_neworder.json"]

CANON_MAP = {
    "ellwood":     "Tobias Ellwood",
    "wilkerson":   "Lawrence Wilkerson",
    "kucinich":    "Dennis Kucinich",
    "pyne":        "David Pyne",
    "kortunov":    "Andrey Kortunov",
    "trenin":      "Dmitri Trenin",
    "blumenthal":  "Max Blumenthal",
    "mearsheimer": "John Mearsheimer",
    "shlaim":      "Avi Shlaim",
    "sibal":       "Kanwal Sibal",
    "bhaskar":     "C. Uday Bhaskar",
    "sood":        "Vikram Sood",
    "sachs":       "Jeffrey Sachs",
    "wolff":       "Richard Wolff",
    "bolton":      "John Bolton",
    "hanke":       "Steve Hanke",
    "keen":        "Steve Keen",
    "olmert":      "Ehud Olmert",
    "postol":      "Theodore Postol",
    "roberts":     "Paul Craig Roberts",
    "weihua":      "Chen Weihua",
    "weiwei":      "Zhang Weiwei",
    "ben-menashe": "Ari Ben-Menashe",
    "menashe":     "Ari Ben-Menashe",
    "bryant":      "Wes Bryant",
}
BAD_PREFIXES = ("Ex-", "Former ", "Fmr ", "SLAMS ", "BLASTS ", "REVEALS ", "EXPOSES ", "WARNS ", "'", "\u2018", "\u2019")

def canonical_for(title, cur_guest):
    tl = (title or "").lower()
    for sn, cn in CANON_MAP.items():
        if sn in tl:
            return cn
    cg_low = (cur_guest or "").lower()
    for sn, cn in CANON_MAP.items():
        if sn in cg_low:
            return cn
    if cur_guest and " " in cur_guest and not cur_guest.endswith(" "):
        if not any(cur_guest.startswith(p) for p in BAD_PREFIXES):
            last = cur_guest.split()[-1]
            if not (last[:1].isupper() and last.endswith(("rat","ing","tio","ion","ent","ies","nes")) and len(last) < 12):
                return cur_guest
    return None

def backfill(path):
    p = os.path.join(ROOT, path)
    if not os.path.exists(p):
        print(f"SKIP {path}: missing"); return
    with open(p) as f: cache = json.load(f)
    resolved = unchanged = rewritten = 0
    for v in cache:
        title = v.get("title") or ""
        cur = v.get("guest") or ""
        canon = canonical_for(title, cur)
        if canon:
            v["canonical_guest_full_name"] = canon
            bad = False
            if not cur: bad = True
            elif cur.endswith(" "): bad = True
            elif any(cur.startswith(pref) for pref in BAD_PREFIXES): bad = True
            else:
                last = cur.split()[-1] if cur.split() else ""
                if last[:1].isupper() and last.endswith(("rat","ing","tio","ion","ent","ies","nes")) and len(last) < 12:
                    bad = True
            if bad or cur != canon:
                v["guest"] = canon
                rewritten += 1
            resolved += 1
        else:
            v.setdefault("canonical_guest_full_name", None)
            unchanged += 1
    with open(p, "w") as f: json.dump(cache, f, indent=2)
    print(f"BACKFILLED {path}: resolved={resolved} unchanged={unchanged} rewritten={rewritten} n_total={len(cache)}")
    for i, x in enumerate(cache[:16]):
        print(f"  #{i} guest={x.get('guest')!r} canonical={x.get('canonical_guest_full_name')!r}")

if __name__ == "__main__":
    for t in TARGETS: backfill(t)
