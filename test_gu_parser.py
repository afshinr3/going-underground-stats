#!/usr/bin/env python3
"""Permanent regression test runner for gu_parser.extract_guest.

Reads regression_tests_gu_titles.json (alongside this file) and asserts every
test case. Exits non-zero on any failure. Designed to run from:
  - local dev (manual)
  - pre-commit hook
  - GitHub Actions before publish/update
  - nightly M2/M3 launchd

Usage:
  python3 test_gu_parser.py
  python3 test_gu_parser.py --module gu_parser
"""
import json, sys, argparse, importlib.util
from pathlib import Path

HERE = Path(__file__).resolve().parent
DATA = HERE / "regression_tests_gu_titles.json"


def load_module(name):
    spec = importlib.util.spec_from_file_location(name, HERE / f"{name}.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--module", default="gu_parser",
                    help="Python module containing extract_guest (default: gu_parser)")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    mod = load_module(args.module)
    data = json.loads(DATA.read_text())
    cases = data.get("test_cases") or []

    n_pass, n_fail = 0, 0
    failures = []
    by_category = {}
    for c in cases:
        title = c["title"]
        expected = c.get("expected_guest")
        tolerant = c.get("tolerant_match", False)
        got = mod.extract_guest(title, source="regression_test") if "source" in mod.extract_guest.__code__.co_varnames else mod.extract_guest(title)

        ok = (got == expected)
        if not ok and tolerant and got and expected:
            # tolerant_match: ignore parenthetical nicknames etc.
            norm_got = got.replace("(Jim) ", "").strip()
            norm_exp = expected.replace("(Jim) ", "").strip()
            ok = (norm_got == norm_exp)

        cat = c.get("category", "uncategorised")
        by_category.setdefault(cat, {"pass": 0, "fail": 0})
        if ok:
            n_pass += 1
            by_category[cat]["pass"] += 1
        else:
            n_fail += 1
            by_category[cat]["fail"] += 1
            failures.append({
                "title": title[:100],
                "expected": expected,
                "got": got,
                "category": cat,
                "added_reason": c.get("added_reason"),
            })

    summary = {
        "module": args.module,
        "data_file": str(DATA),
        "n_total": len(cases),
        "n_pass": n_pass,
        "n_fail": n_fail,
        "by_category": by_category,
    }
    if failures:
        summary["failures"] = failures
    print(json.dumps(summary, indent=2, ensure_ascii=False, default=str))
    return 0 if n_fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
