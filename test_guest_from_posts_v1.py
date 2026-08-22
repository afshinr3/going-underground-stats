#!/usr/bin/env python3
"""GU_GUEST_FROM_POSTS_V1_20260822 regression guard.

Locks the behaviour that took four back-test iterations to get right. Each check below
corresponds to a real false positive the back-test caught BEFORE this shipped:

  "Shidore" for a GU episode   -- the GU feed announces BOTH shows; time proximity alone
                                  does not bind a post to an episode (10 of 12 wrong)
  "at", "of", "the"            -- re.I spanning the name group defeated capitalisation
  "Trump’s"/"World Bank’s ..." -- possessive tokens
  "US-Iran MoU"                -- internal capitals; not a person
  "Lead Economist"             -- correctly shaped, but a role title
  refusing every "Dr. X"       -- splitting the clause on "." truncated "Dr."

THE INVARIANT: it returns the right name or NOTHING. A refusal is a valid answer; inventing
one is the defect this module exists to end.

Offline — synthetic posts, no network, no file writes.

Run:  python3 test_guest_from_posts_v1.py
"""
import sys
from datetime import datetime, timedelta, timezone
import gu_guest_from_posts_v1 as R

FAIL = []
def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}{('  -- ' + detail) if detail else ''}")
    if not cond: FAIL.append(name)

PUB = "2026-08-22T11:55:34Z"
base = datetime.fromisoformat(PUB.replace("Z", "+00:00"))
def post(txt, hours_before): return (txt, base - timedelta(hours=hours_before))

print("\n1  The three real announcement shapes all resolve")
cases = [
    ("name first",   "SATURDAY’S GOING UNDERGROUND:\n\nWe’re joined by Dr. Michael O’Hanlon, former member of the CIA External Advisory Board.", "Michael O’Hanlon"),
    ("name last",    "SATURDAY’S GOING UNDERGROUND:\n\nWe’re joined by Donald Trump’s former lawyer Robert Barnes. \n\nWhat is behind...", "Robert Barnes"),
    ("honorific late","MONDAY’S GOING UNDERGROUND:\n\nWe’ll be joined by Holocaust survivor and world-renowned trauma specialist Dr. Gabor Maté\n\nHow has...", "Gabor Maté"),
]
for label, text, want in cases:
    got, _ = R.resolve_guest(PUB, "GU", posts=[post(text, 21)])
    check(f"{label} -> {want}", got == want, repr(got))

print("\n2  The other show's announcement must NOT claim this episode")
no_ann = post("NEW EPISODE OF NEW ORDER\n\nWe’re joined by Sarang Shidore, director of the Global South Program.", 20)
gu_ann = post("SATURDAY’S GOING UNDERGROUND:\n\nWe’re joined by Dr. Michael O’Hanlon, former member of the CIA board.", 21)
got, _ = R.resolve_guest(PUB, "GU", posts=[no_ann, gu_ann])
check("GU episode resolves the GU guest", got == "Michael O’Hanlon", repr(got))
got, _ = R.resolve_guest(PUB, "GU", posts=[no_ann])
check("a NO-only window refuses for GU", got is None, repr(got))

print("\n3  Non-persons are refused (every one a real back-test false positive)")
for label, clause in [
    ("possessive", "We’re joined by the World Bank’s Research team"),
    ("role title", "We’re joined by the former Lead Economist"),
    ("acronym",    "We’re joined by the US-Iran MoU"),
    ("lowercase",  "We’re joined by at the studio today"),
]:
    got, _ = R.resolve_guest(PUB, "GU", posts=[post("GOING UNDERGROUND:\n\n" + clause, 20)])
    check(f"{label} refused", got is None, repr(got))

print("\n4  Evidence must belong to THIS episode (time-bound)")
ann = "SATURDAY’S GOING UNDERGROUND:\n\nWe’re joined by Dr. Michael O’Hanlon, former CIA board member."
got, _ = R.resolve_guest(PUB, "GU", posts=[post(ann, 21)])
check("21h before -> resolves", got == "Michael O’Hanlon")
got, _ = R.resolve_guest(PUB, "GU", posts=[post(ann, 24 * 7)])
check("a week before -> refused", got is None, repr(got))
near, far = post(ann, 20), post("GOING UNDERGROUND:\n\nWe’re joined by Philip Pilkington, economist.", 39)
got, _ = R.resolve_guest(PUB, "GU", posts=[far, near])
check("nearest announcement wins", got == "Michael O’Hanlon", repr(got))

print("\n5  Refusal, never invention")
check("no posts -> None", R.resolve_guest(PUB, "GU", posts=[])[0] is None)
check("no announcement -> None",
      R.resolve_guest(PUB, "GU", posts=[post("GOING UNDERGROUND: great episode today", 5)])[0] is None)
check("bad pub_iso -> None", R.resolve_guest("not-a-date", "GU", posts=[post(ann, 1)])[0] is None)

print("\n6  surname_of keeps compound and apostrophe names intact")
for full, want in [("Michael O’Hanlon", "O’Hanlon"), ("Habib Al Mulla", "Al Mulla"),
                   ("Gabor Maté", "Maté"), ("Hasan Ünal", "Ünal")]:
    check(f"{full} -> {want}", R.surname_of(full) == want, repr(R.surname_of(full)))

print("\n" + ("ALL CHECKS PASS" if not FAIL else f"{len(FAIL)} FAILED: {FAIL}"))
sys.exit(1 if FAIL else 0)
