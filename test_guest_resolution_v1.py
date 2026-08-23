#!/usr/bin/env python3
"""TEST — guest resolution guards.

Covers PUB_ISO_FALLBACK_V1_20260823, CANON_AT_INGEST_V1_20260823 and
CANON_LULA_V1_20260823. Read-only: touches no feed file, fetches nothing.

Run: python3 test_guest_resolution_v1.py
"""

import importlib.util
import json
import os
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)

_spec = importlib.util.spec_from_file_location("fp", os.path.join(ROOT, "fetch_and_push.py"))
m = importlib.util.module_from_spec(_spec)
try:
    _spec.loader.exec_module(m)
except SystemExit:
    pass

FAILED, PASSED = [], []


def check(name, cond, detail=""):
    (PASSED if cond else FAILED).append(name)
    print(f"  {'PASS' if cond else 'FAIL'}  {name}" + (f"  --  {detail}" if not cond else ""))


print("\n[1] PUB_ISO_FALLBACK — the repair pass can date a row that has no pub_iso")
check("'22 Aug' -> ISO midnight that day",
      m._short_to_iso("22 Aug", __import__("datetime").datetime(2026, 8, 23)) ==
      "2026-08-22T00:00:00Z", m._short_to_iso("22 Aug"))
check("pub_iso WINS over the short date when present",
      m._row_pub_iso({"pub_iso": "2026-08-22T11:55:34Z", "date": "1 Jan"})
      == "2026-08-22T11:55:34Z")
check("short date is used only as a fallback",
      m._row_pub_iso({"date": "22 Aug"}) == "2026-08-22T00:00:00Z")
for junk in ({}, {"date": ""}, {"date": "nonsense"}, {"date": None}):
    check(f"unusable date {junk!r} yields '' (never a guess)", m._row_pub_iso(junk) == "")
check("a future day/month does not resolve to a future year",
      m._short_to_iso("31 Dec", __import__("datetime").datetime(2026, 8, 23))
      == "2025-12-31T00:00:00Z", m._short_to_iso("31 Dec"))

print("\n[2] CANON_LULA — the name resolves from every title shape it appears in")
for t in ["Brazil’s President Lula da Silva on Trump, BRICS and the Dollar",
          "Lula da Silva: Brazil Will NOT Be Subordinated to the Dollar",
          "Lula on Why BRICS Needs Its Own Currency"]:
    cfn, csu, _ = m._canonical_from_title(t, "", "")
    check(f"resolves: {t[:44]}...", cfn == "Luiz Inácio Lula da Silva" and csu == "SILVA",
          f"{cfn!r}/{csu!r}")
check("both surname spellings are mapped",
      m.CANON_MAP.get("silva") == m.CANON_MAP.get("lula") == "Luiz Inácio Lula da Silva")

print("\n[3] CANON_AT_INGEST — an empty guest yields ONLY a real CANON_MAP hit")
# This is the property the ingest-time call depends on. If _canonical_from_title
# ever starts inventing names from arbitrary titles, the ingest gate becomes a
# source of role-fragments exactly like the "Ex-" defect it was added to prevent.
for t in ["Former Pentagon Official on the Coming War",
          "Afshin Rattansi CHALLENGES Ex-CIA Advisor on the Legacy of America’s Wars",
          "Iran War: Trump’s Oil Market Manipulation & The Breakdown of US Dollar Hegemony",
          "Why the Global South Is Turning Away From Washington"]:
    cfn, csu, _ = m._canonical_from_title(t, "", "")
    check(f"refuses to name: {t[:44]}...", cfn is None and csu is None, f"{cfn!r}/{csu!r}")

print("\n[4] role fragments are still rejected as surnames")
for bad in ["Ex-", "ex", "Former", "Chief", "Lead", "Vice"]:
    check(f"{bad!r} is not a valid surname",
          bad.rstrip('-').lower() in m._ROLE_PREFIXES or not m._looks_valid_surname(bad))
check("'Silva' IS a valid surname", m._looks_valid_surname("Silva"))
check("'O’Hanlon' IS a valid surname", m._looks_valid_surname("O’Hanlon"))

print("\n[5] the live feed carries no role-fragment surname")
for fn in ("videos.json", "videos_neworder.json"):
    p = os.path.join(ROOT, fn)
    if not os.path.exists(p):
        continue
    rows = json.load(open(p))
    bad = [r for r in rows
           if (r.get("surname") or "").rstrip('-').lower() in m._ROLE_PREFIXES
           or (r.get("surname") or "").endswith('-')]
    check(f"{fn}: no role-prefix surname",
          not bad, ", ".join(f"{r.get('date')}:{r.get('surname')!r}" for r in bad))
    # A guest whose name opens the title is NORMAL — "Gabor Maté: Netanyahu is...",
    # "Peter Schiff on...". The defect is a title-prefix guest whose SURNAME is not
    # a name: that is the title[:30] truncation artefact ("Ex-", "DEF", "Israel’s").
    # Testing the prefix alone flagged six correct rows and would have trained the
    # eye to ignore this guard.
    frag = [r for r in rows
            if r.get("guest") and r.get("title", "").startswith(r["guest"])
            and len(r.get("title", "")) > len(r["guest"]) + 10
            and not m._looks_valid_surname(r.get("surname") or "")]
    check(f"{fn}: no truncated title-prefix guest",
          not frag, ", ".join(f"{r.get('date')}:{r.get('surname')!r}" for r in frag))

print(f"\n{'All guest-resolution tests passed.' if not FAILED else str(len(FAILED)) + ' FAILURES'}"
      f"  ({len(PASSED)} passed)")
sys.exit(0 if not FAILED else 1)
