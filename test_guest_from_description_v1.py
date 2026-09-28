#!/usr/bin/env python3
"""GU_GUEST_FROM_DESCRIPTION_V1_20260928 regression guard.

THE DEFECT THIS LOCKS OUT. YouTube caps a title at 100 characters and this show writes
the guest LAST, so the cap amputates exactly the part the parser needs. On 2026-09-28 the
episode arrived as

    "Americans Pay $2 BILLION A DAY for Iran War: Afshin Rattansi Challenges Ex-Deputy
     CENTCOM Commander"                                                      (99 chars)

with "Vice Admiral Robert Harward" cut off. gu_parser rejected it on every pipeline cycle
from 07:06Z (30+ rows in parser_rejections.jsonl) and the episode never reached
videos.json while it was live on YouTube (226 views) and Rumble (1.42K).

THE INVARIANT: the right name or NOTHING. Every refusal check below is a candidate the
back-test produced that is name-SHAPED but not the guest.

Offline -- synthetic descriptions, no network, no file writes.

Run:  python3 test_guest_from_description_v1.py
"""
import sys
import gu_guest_from_description_v1 as R

FAIL = []
def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}{('  -- ' + detail) if detail else ''}")
    if not cond: FAIL.append(name)

def got(desc):
    return R.guest_from_description(desc)[0]

OPEN_GU = "On this episode of Going Underground, we speak to "
OPEN_NO = "On this episode of New Order, we are joined by "

print("A. the 2026-09-28 episode this module exists for")
check("rank-prefixed guest resolves",
      got(OPEN_GU + "Vice Admiral Robert Harward, former Deputy Commander of US CENTCOM "
                    "and Executive Vice President of Shield AI.") == "Robert Harward")
check("the truncated TITLE alone still yields nothing (this is not a title parser)",
      got("Americans Pay $2 BILLION A DAY for Iran War: Afshin Rattansi Challenges "
          "Ex-Deputy CENTCOM Commander") is None)

print("B. every opener shape seen live in the 2026 feeds")
check("honorific-first", got(OPEN_GU + "Dr. Michael O’Hanlon, former Pentagon Defense "
                                       "Policy Board member") == "Michael O’Hanlon")
check("no honorific", got(OPEN_GU + "Joe Kent, Trump’s former Counterterror Chief")
      == "Joe Kent")
check("role clause then rank then name",
      got(OPEN_GU + "former Director of Shin Bet Admiral Ami Ayalon. He discusses")
      == "Ami Ayalon")
check("name then 'of the University'",
      got(OPEN_GU + "Prof. John Mearsheimer of the University of Chicago. He discusses")
      == "John Mearsheimer")
check("'joined by' on the other show", got(OPEN_NO + "Prof. David Monyae, Director of "
                                                     "the Centre for Africa-China Studies")
      == "David Monyae")
check("diacritics preserved", got(OPEN_GU + "Gabor Maté, the Holocaust survivor and "
                                            "trauma specialist") == "Gabor Maté")

print("C. refusals -- name-shaped candidates that are NOT the guest")
check("floating run after a preposition is refused",
      got(OPEN_GU + "former Prime Minister of Israel Ehud Olmert. He discusses") is None,
      "'Israel Ehud Olmert' is name-shaped; the name must abut the opener, a rank or a role")
check("no opener phrase -> refuse",
      got("Afshin Rattansi challenges the former Lockheed Martin executive on the war.")
      is None)
check("empty description -> refuse", got("") is None)
check("None description -> refuse", got(None) is None)
check("host is never the guest",
      got(OPEN_GU + "Afshin Rattansi. He discusses") is None)
check("a role title is not a person",
      got(OPEN_GU + "the former Lead Economist at the World Bank") is None)
check("an organisation is not a person",
      got(OPEN_GU + "the Quincy Institute for Responsible Statecraft") is None)

print("D. the role-anchor cannot reach past a preposition")
check("'Minister of Israel X' still refuses",
      got(OPEN_GU + "former Foreign Minister of Israel Ehud Olmert") is None)

print("E. reads only the clause, never the interview summary")
check("names discussed later in the description are not read",
      got(OPEN_GU + "Prof. Steve Hanke, the applied economist. He discusses Donald Trump "
                    "and Scott Bessent and Benjamin Netanyahu.") == "Steve Hanke")

print("F. surname helper agrees with the posts module")
check("surname_of", R.surname_of("Robert Harward") == "Harward")

print()
if FAIL:
    print(f"FAILED {len(FAIL)}: {FAIL}")
    sys.exit(1)
print("ALL PASS")
