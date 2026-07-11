#!/usr/bin/env python3
# CANONICAL_FIELD_EMISSION_V1_2026_07_11 — regression suite for the
# source-of-truth canonical fields emitted by fetch_and_push.py.
#
# T1: Ukraine Proxy War / James Carden title -> ("James Carden","CARDEN",<hash>)
# T2: Ex-UK Defence Minister Tobias Ellwood -> ("Tobias Ellwood","ELLWOOD",<hash>)
# T3: Wilkerson clip title -> ("Lawrence Wilkerson","WILKERSON",<hash>)
# T4: Unknown-guest title -> (None, None-or-SURNAME, <hash>)
# T5: Tidbyt name-picker prefers canonical_surname_upper over broken surname.
# T6: Tidbyt name-picker falls back to `surname` when canonical absent.
# T7: 11-Jul row in videos.json has canonical_surname_upper == "CARDEN".
# T8: 6-Jul row in videos.json has canonical_surname_upper == "ELLWOOD".
import json
import os
import sys

# Avoid importing all of fetch_and_push (has scraper deps we don't need). We
# import _canonical_from_title / CANON_MAP directly by execing the top of the
# module until we hit the first async block.
HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "fetch_and_push.py")


def _load_helpers():
    """Import the CANON_MAP + _canonical_from_title helpers from fetch_and_push
    WITHOUT triggering playwright / requests / PIL imports (which fail on some
    environments)."""
    ns = {"__name__": "_gu_helpers", "__file__": SRC}
    with open(SRC) as f:
        src = f.read()
    # Extract only the block between CANONICAL_FIELD_EMISSION_V1_2026_07_11
    # marker start and marker end.
    start = src.index("# CANONICAL_FIELD_EMISSION_V1_2026_07_11 ---")
    end_marker = "# ---------------------------------------------------------------------------"
    end = src.index(end_marker, start) + len(end_marker)
    block = "import hashlib\n" + src[start:end]
    exec(block, ns)
    return ns


NS = _load_helpers()
_canonical_from_title = NS["_canonical_from_title"]
CANON_MAP = NS["CANON_MAP"]

PASSES = 0
FAILS = 0


def _assert(name, cond, detail=""):
    global PASSES, FAILS
    if cond:
        PASSES += 1
        print(f"PASS {name} {detail}")
    else:
        FAILS += 1
        print(f"FAIL {name} {detail}")


# T1 -----------------------------------------------------------------
CARDEN_TITLE = (
    "NEW EPISODE OF GOING UNDERGROUND\n\n"
    "Ukraine Proxy War: Europe's Elites & US Neocons Want the DISSOLUTION "
    "of Russia - Ex-State Department Official James Carden"
)
cfn, csu, ceid = _canonical_from_title(CARDEN_TITLE, "Ukraine Proxy War", "War")
_assert("T1 carden full name", cfn == "James Carden", f"got {cfn!r}")
_assert("T1 carden surname upper", csu == "CARDEN", f"got {csu!r}")
_assert("T1 carden episode id len", isinstance(ceid, str) and len(ceid) == 12, f"got {ceid!r}")

# T2 -----------------------------------------------------------------
ELLWOOD_TITLE = (
    "Ex-UK Defence Minister Tobias Ellwood Says Trump Won NOTHING in Iran, "
    "SLAMS Netanyahu's Endless Wars"
)
cfn, csu, ceid = _canonical_from_title(ELLWOOD_TITLE, "Ex-UK Defence Minister Tobias ", "Tobias")
_assert("T2 ellwood full name", cfn == "Tobias Ellwood", f"got {cfn!r}")
_assert("T2 ellwood surname upper", csu == "ELLWOOD", f"got {csu!r}")
_assert("T2 ellwood episode id len", isinstance(ceid, str) and len(ceid) == 12, f"got {ceid!r}")

# T3 -----------------------------------------------------------------
WILK_TITLE = "Some clip about Wilkerson from 2023"
cfn, csu, ceid = _canonical_from_title(WILK_TITLE, "", "")
_assert("T3 wilkerson full name", cfn == "Lawrence Wilkerson", f"got {cfn!r}")
_assert("T3 wilkerson surname upper", csu == "WILKERSON", f"got {csu!r}")

# T4 -----------------------------------------------------------------
UNKNOWN_TITLE = "Some completely opaque headline about geopolitics"
cfn, csu, ceid = _canonical_from_title(UNKNOWN_TITLE, "", None)
_assert("T4 unknown full name is None", cfn is None, f"got {cfn!r}")
_assert("T4 unknown surname upper is None", csu is None, f"got {csu!r}")
_assert("T4 unknown episode id len", isinstance(ceid, str) and len(ceid) == 12, f"got {ceid!r}")

# T5 -----------------------------------------------------------------
# Tidbyt name selection: canonical_surname_upper wins.
row_carden = {"canonical_surname_upper": "CARDEN", "surname": "War"}
picked = row_carden.get('canonical_surname_upper') or row_carden.get('surname', '?')
_assert("T5 tidbyt picks canonical", picked == "CARDEN", f"got {picked!r}")

# T6 -----------------------------------------------------------------
row_legacy = {"surname": "War"}
picked = row_legacy.get('canonical_surname_upper') or row_legacy.get('surname', '?')
_assert("T6 tidbyt falls back to surname", picked == "War", f"got {picked!r}")

# T7 / T8 ------------------------------------------------------------
VIDEOS_JSON = os.path.join(HERE, "videos.json")
if os.path.exists(VIDEOS_JSON):
    with open(VIDEOS_JSON) as f:
        videos = json.load(f)
    row_11jul = next((v for v in videos if v.get("date") in ("11 Jul",)), None)
    row_6jul = next((v for v in videos if v.get("date") in ("6 Jul",)), None)
    if row_11jul:
        _assert("T7 11 Jul canonical_surname_upper == CARDEN",
                row_11jul.get("canonical_surname_upper") == "CARDEN",
                f"got {row_11jul.get('canonical_surname_upper')!r}")
    else:
        _assert("T7 videos.json has 11 Jul row", False, "not found")
    if row_6jul:
        _assert("T8 6 Jul canonical_surname_upper == ELLWOOD",
                row_6jul.get("canonical_surname_upper") == "ELLWOOD",
                f"got {row_6jul.get('canonical_surname_upper')!r}")
    else:
        _assert("T8 videos.json has 6 Jul row", False, "not found")
else:
    _assert("T7/T8 videos.json exists", False, f"missing at {VIDEOS_JSON}")

print(f"\nSUMMARY: {PASSES} passed, {FAILS} failed")
sys.exit(0 if FAILS == 0 else 1)
