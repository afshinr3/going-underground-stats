#!/usr/bin/env python3
"""TEST — MERGE_MEASURED_FIELDS_V1: a cloud push never nulls a bridge-measured Rumble/IG value."""
import sys
import merge_measured_fields_v1 as M

F = []
def check(n, c):
    print(f"  {'PASS' if c else 'FAIL'}  {n}")
    if not c: F.append(n)

cloud = [{"surname": "Carlson", "date": "11 Sep", "canonical_video_id": "JUHl-tQRN7g",
          "rumble_views": None, "ig_likes": "?", "x_views": "700K"},
         {"surname": "Silva", "date": "25 Aug", "rumble_views": "9.9K", "ig_likes": None},
         {"surname": "New", "date": "16 Sep", "rumble_views": None, "ig_likes": None}]
origin = [{"surname": "Carlson", "date": "11 Sep", "canonical_video_id": "JUHl-tQRN7g",
           "rumble_views": "4.4K", "ig_likes": "1.2K", "x_views": "684K"},
          {"surname": "Silva", "date": "25 Aug", "rumble_views": "3.6K", "ig_likes": "?"}]
out, n = M.merge_rows(cloud, origin)
check("null rumble carried forward from origin (the 00:32Z revert)", out[0]["rumble_views"] == "4.4K")
check("'?' ig carried forward from origin", out[0]["ig_likes"] == "1.2K")
check("a field the cloud measured is never overwritten", out[0]["x_views"] == "700K" and out[1]["rumble_views"] == "9.9K")
check("unmeasured origin does not replace unmeasured cloud", out[1]["ig_likes"] is None)
check("rows with no origin match are left alone, none added", out[2]["rumble_views"] is None and len(out) == 3)
check("count of carried fields", n == 2)
print("All merge tests passed." if not F else f"{len(F)} FAILURES")
sys.exit(1 if F else 0)
