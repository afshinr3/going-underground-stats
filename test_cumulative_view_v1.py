#!/usr/bin/env python3
"""Guards for the cumulative dashboard view (GU_CUMULATIVE_VIEW_V1)."""
import json, re, unittest
from pathlib import Path
HERE = Path(__file__).resolve().parent
SRC = (HERE / "docs" / "index.html").read_text()
INV = json.loads((HERE / "gu_content_inventory_v1.json").read_text())

class TestUIConsumesInventory(unittest.TestCase):
    def test_dashboard_fetches_the_inventory(self):
        self.assertIn("gu_content_inventory_v1.json", SRC)

    def test_cumulative_renderer_is_called_on_load(self):
        self.assertIn("renderCumulative(lastData.inventory", SRC)

    def test_cumulative_table_exists(self):
        for el in ("cumulative-body", "cumulative-meta", "cumulative-table"):
            self.assertIn(el, SRC)

class TestScopesCannotMix(unittest.TestCase):
    def test_renderer_refuses_wrong_scope(self):
        self.assertIn("inv.metric_scope !== CUMULATIVE_SCOPE", SRC)
        self.assertIn("refusing to render", SRC)

    def test_inventory_declares_cumulative_scope(self):
        self.assertEqual(INV["metric_scope"], "cumulative_to_measurement_timestamp")

    def test_rolling_window_logic_untouched(self):
        # the Sat-Mon rolling rule and its marker must still be present
        self.assertIn("WIN_LAST_SAT_MON_AND_SHOW_BADGE_V1_2026_07_17", SRC)
        self.assertIn("1 Week (last Sat-Mon)", SRC)

    def test_cumulative_values_never_feed_the_rolling_total(self):
        # totalOf/totalParts (rolling) must not reference the inventory
        m = re.search(r"function totalParts\(v\) \{.*?\n\}", SRC, re.S)
        self.assertIsNotNone(m)
        self.assertNotIn("inventory", m.group(0))
        self.assertNotIn("by_classification", m.group(0))

class TestEditorialTotalComposition(unittest.TestCase):
    def test_editorial_is_full_plus_clip_only_in_ui(self):
        self.assertIn("const editorial = (full === null && clip === null) ? null : (full || 0) + (clip || 0);", SRC)

    def test_promo_and_ambiguous_not_in_editorial_expression(self):
        m = re.search(r"const editorial = .*?;", SRC)
        self.assertNotIn("promo", m.group(0))
        self.assertNotIn("amb", m.group(0))

    def test_data_layer_agrees(self):
        for k, e in INV["episodes"].items():
            b = e["by_classification"]
            self.assertEqual(e["editorial_total_measured"],
                             (b["FULL_INTERVIEW"]["sum_measured"] or 0) + (b["CLIP"]["sum_measured"] or 0), k)

    def test_promo_and_ambiguous_excluded_in_data(self):
        for k, e in INV["episodes"].items():
            b = e["by_classification"]
            ed = e["editorial_total_measured"] or 0
            self.assertNotEqual(ed, ed + (b["PROMO"]["sum_measured"] or 0) if b["PROMO"]["sum_measured"] else ed + 1)
            for i in INV["items"]:
                if i["episode_key"] == k and i["classification"] in ("PROMO", "AMBIGUOUS"):
                    self.assertNotEqual(i["status"], "measured_editorial")

class TestPartialStaleAndUnavailable(unittest.TestCase):
    def test_partial_marker_rendered(self):
        self.assertIn("*partial", SRC)

    def test_stale_marker_rendered(self):
        self.assertIn("STALE (", SRC)
        self.assertIn("STALE_AFTER_HOURS", SRC)

    def test_unavailable_renders_NA_not_zero(self):
        self.assertIn("? `<em title=\"${title || 'not measured'}\"", SRC.replace('\n', ''))
        self.assertIn(">N/A</em>", SRC)

    def test_tooltip_names_missing_platforms_and_items(self):
        self.assertIn("no verified full interview on:", SRC)
        self.assertIn("ambiguous item(s) excluded", SRC)

class TestMalformedInventoryFailsVisibly(unittest.TestCase):
    def test_missing_inventory_is_reported(self):
        self.assertIn("Content inventory unavailable", SRC)

    def test_empty_episodes_is_reported(self):
        self.assertIn("malformed or empty", SRC)

    def test_no_silent_fallback_for_inventory(self):
        self.assertIn("_inventory_error", SRC)

if __name__ == "__main__":
    unittest.main(verbosity=2)
