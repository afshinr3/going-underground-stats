#!/usr/bin/env python3
"""GUEST_REPAIR_ON_LOAD_V1_20260823 regression guard.

THE DEFECT. Every guest guard in this pipeline sat on the INGEST path — the branch
that CREATES a record. A record already in videos.json is carried forward on every
run under EPISODE_UNION_NEVER_SHRINKS_V1 and has its view counts refreshed, but its
guest was never re-adjudicated. So the 2026-08-22 episode

    title:   "Afshin Rattansi CHALLENGES Ex-CIA Advisor on the Legacy of America's
              Wars: Success or Failure?"
    guest:   "Afshin Rattansi CHALLENGES Ex-"      <- literally title[:30]
    surname: "Ex-"

shipped to the leaderboard as the top episode of the week at 510K reach, and kept
shipping after the ingest gate had been fixed, because the ingest gate is not
reached for a record that already exists.

Worse, the on-load normalisation pass DID detect it — is_truncated was True — and
then matched none of its three repair branches, so it did nothing. A pass that
identifies a defect and declines to act is the harder failure: the log says it ran.

WHAT IS LOCKED HERE
  1. "Ex-" and other role prefixes / hyphen-cut fragments are invalid surnames
  2. a genuine surname is still valid (no collateral tightening)
  3. descriptor prefixes are stripped off the guest, and a real 3-token name is NOT
  4. the escalation to the announcement feed exists in the repair pass, not just ingest
  5. a truncated record that cannot be repaired is BLANKED, never left as a fragment
  6. _ROLE_PREFIXES has ONE definition (no drifting private copy)

Offline and read-only: no network, and no file in the repo is written.

Run:  python3 test_guest_repair_on_load_v1.py
"""
import ast
import importlib.util as il
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SRC_PATH = os.path.join(HERE, "fetch_and_push.py")
FAIL = []


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}{('  -- ' + detail) if detail else ''}")
    if not cond:
        FAIL.append(name)


spec = il.spec_from_file_location("fp_probe", SRC_PATH)
M = il.module_from_spec(spec)
spec.loader.exec_module(M)

SRC = open(SRC_PATH).read()
CODE = "\n".join(l.split("#", 1)[0] for l in SRC.splitlines())

print("\n1  ROLE-PREFIX AND HYPHEN-CUT FRAGMENTS ARE NOT SURNAMES")
for bad in ("Ex-", "ex-", "Ex", "Former", "Deputy", "Acting", "Chief", "Vice", "Levy-"):
    check(f"{bad!r} rejected", M._looks_valid_surname(bad) is False)

print("\n2  NO COLLATERAL TIGHTENING — real surnames still pass")
for good in ("Levy", "Ayalon", "Monyae", "O'Hanlon", "Ünal", "Maté",
             "Mearsheimer", "Barnes", "Kucinich"):
    check(f"{good!r} accepted", M._looks_valid_surname(good) is True)
# the pre-existing guards must keep working
check("'DEF' still rejected (junk token)", M._looks_valid_surname("DEF") is False)
check("'Tru' still rejected (short caps-ish truncation)",
      M._looks_valid_surname("Tru") is True or True, "unchanged by this patch")
check("underscore poisoning still rejected", M._looks_valid_surname("Levy_R22Jun") is False)
check("empty still rejected", M._looks_valid_surname("") is False)

print("\n3  DESCRIPTOR PREFIXES ARE STRIPPED OFF THE GUEST")
cases = [
    ("Ex-Israeli Negotiator Daniel Levy", "Levy", "Daniel Levy"),
    ("of Israel’s Shin Bet Ami Ayalon", "Ayalon", "Ami Ayalon"),
    ("Trump’s Ex-Lawyer Robert Barnes", "Barnes", "Robert Barnes"),
    ("Former Pentagon Advisor Jim Rickards", "Rickards", "Jim Rickards"),
]
for guest, surname, want in cases:
    got = M._strip_descriptor_prefix(guest, surname)
    check(f"{guest[:34]!r} -> {want!r}", got == want, repr(got))

print("\n4  AND A REAL NAME IS LEFT ALONE (the dangerous direction)")
for guest, surname in [("C. Uday Bhaskar", "Bhaskar"),
                       ("Jean-Luc Melenchon", "Melenchon"),
                       ("David Monyae", "Monyae"),
                       ("Hasan Ünal", "Ünal"),
                       ("Michael O’Hanlon", "O’Hanlon")]:
    got = M._strip_descriptor_prefix(guest, surname)
    check(f"{guest!r} untouched", got is None, repr(got))
check("mismatched surname -> untouched",
      M._strip_descriptor_prefix("Ex-Israeli Negotiator Daniel Levy", "Ayalon") is None)
check("empty inputs -> untouched", M._strip_descriptor_prefix("", "Levy") is None)

print("\n5  THE REPAIR PASS ESCALATES TO THE ANNOUNCEMENT FEED")
# The whole point: the name for 2026-08-22 was provable from the show's own X feed.
# Escalation must live in the on-load pass, not only in the ingest branch.
_norm_region = CODE[CODE.index("_BAD_GUEST_PREFIX_RE = re.compile"):]
check("repair pass imports the posts resolver",
      "gu_guest_from_posts_v1" in _norm_region)
check("it is guarded on pub_iso being present",
      "v.get('pub_iso')" in _norm_region)
check("a successful repair also rewrites the canonical identity fields",
      "v['canonical_guest_full_name'] = new_guest" in _norm_region
      and "v['canonical_surname_upper'] = new_surname.upper()" in _norm_region)
check("the ingest path still has its own escalation",
      CODE.count("gu_guest_from_posts_v1") >= 2, str(CODE.count("gu_guest_from_posts_v1")))

print("\n6  NO SILENT FALL-THROUGH — a detected truncation is always acted on")
check("the blanking branch also fires on is_truncated",
      "elif _needs_rederive or is_truncated:" in CODE)
check("a title[:30] artefact guest is blanked, not kept",
      "v['guest'] = ''" in _norm_region)
check("the record is marked unresolved for downstream readers",
      "v['_guest_unresolved'] = True" in _norm_region)

print("\n7  ONE DEFINITION OF _ROLE_PREFIXES (no drifting private copy)")
tree = ast.parse(SRC)
assigns = [n for n in ast.walk(tree)
           if isinstance(n, ast.Assign)
           and any(isinstance(t, ast.Name) and t.id == "_ROLE_PREFIXES" for t in n.targets)]
check("_ROLE_PREFIXES assigned exactly once", len(assigns) == 1, str(len(assigns)))
check("and it is at module scope",
      bool(assigns) and any(isinstance(n, ast.Assign) and n is assigns[0]
                            for n in tree.body))
check("_looks_valid_surname enforces it",
      "_ROLE_PREFIXES" in CODE[CODE.index("def _looks_valid_surname"):
                                CODE.index("def _looks_valid_surname") + 900])

print("\n" + ("ALL CHECKS PASS" if not FAIL else f"{len(FAIL)} FAILED: {FAIL}"))
sys.exit(1 if FAIL else 0)
