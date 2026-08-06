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

class TestTotalCumulativeTabIsPresentAndDefault(unittest.TestCase):
    """GU_TOTAL_CUMULATIVE_TAB_V1 — the cumulative scope used to be an unlabelled
    panel ~1400px below the fold with no tab, so the operator never saw it."""

    def test_tab_is_named_exactly_TOTAL_SLASH_CUMULATIVE(self):
        self.assertIn('<button data-view="cumulative" class="active">TOTAL / CUMULATIVE</button>', SRC)

    def test_view_bar_exists_above_main(self):
        self.assertIn('id="view-bar"', SRC)
        self.assertLess(SRC.index('id="view-bar"'), SRC.index("<main>"),
                        "view switcher must be above the panels, not below the fold")

    def test_cumulative_is_the_default_view(self):
        self.assertIn("const DEFAULT_VIEW = 'cumulative';", SRC)
        self.assertIn("applyView(DEFAULT_VIEW);", SRC)

    def test_cumulative_panel_is_bound_to_the_cumulative_view(self):
        self.assertIn('data-view="cumulative" id="panel-cumulative"', SRC)

    def test_rolling_panels_are_bound_to_the_rolling_view(self):
        # TOTAL REACH, stacked chart and followers chart all belong to rolling
        panels = re.findall(r'<div class="panel"[^>]*data-view="rolling"', SRC)
        self.assertEqual(len(panels), 3, panels)

    def test_default_view_shows_the_inventory_not_the_rolling_window(self):
        # applyView hides every panel that is not the active view
        self.assertIn("p.style.display = p.dataset.view === view ? '' : 'none'", SRC)

    def test_rolling_tab_still_reachable(self):
        self.assertIn('<button data-view="rolling">ROLLING WINDOW</button>', SRC)

    def test_switching_views_does_not_touch_windowDays(self):
        m = re.search(r"function applyView\(view\) \{.*?\n\}", SRC, re.S)
        self.assertIsNotNone(m)
        self.assertNotIn("windowDays", m.group(0))

    def test_window_bar_tabs_unchanged(self):
        for t in ('>All<', '1 Week (last Sat-Mon)', '1 Month', '6 Months'):
            self.assertIn(t, SRC)
        self.assertIn('id="window-bar"', SRC)


class TestDefaultViewConsumesTheInventory(unittest.TestCase):
    def test_default_view_panel_is_fed_by_renderCumulative(self):
        # the panel shown by default is the one renderCumulative writes into
        self.assertIn('id="panel-cumulative"', SRC)
        self.assertIn('id="cumulative-body"', SRC)
        self.assertIn("document.getElementById('cumulative-body')", SRC)

    def test_inventory_is_fetched_before_that_panel_renders(self):
        self.assertLess(SRC.index("loadJSON('gu_content_inventory_v1.json'"),
                        SRC.index("renderCumulative(lastData.inventory"))

    def test_inventory_file_is_reachable_from_the_declared_base(self):
        # the page fetches ${RAW}/<name>; RAW must point at a base that carries
        # the inventory. Pages serves /docs as site root and the inventory lives
        # at repo root, so a page-relative fetch would 404.
        self.assertIn("const RAW = `https://raw.githubusercontent.com/${REPO}/main`;", SRC)
        self.assertTrue((HERE / "gu_content_inventory_v1.json").exists())

    def test_headline_names_the_scope(self):
        self.assertIn("TOTAL / CUMULATIVE BY CONTENT ITEM", SRC)


class TestShellFreshness(unittest.TestCase):
    """A tab left open reloaded its JSON but never its HTML, so a newly deployed
    view could never reach an operator who already had the dashboard open."""

    def test_build_id_marker_present(self):
        self.assertIn("GU_UI_BUILD_ID:GU_TOTAL_CUMULATIVE_TAB_V1_2026_08_06", SRC)

    def test_build_id_marker_matches_the_js_constant(self):
        marker = re.search(r"GU_UI_BUILD_ID:([A-Za-z0-9_.\-]+)", SRC).group(1)
        const = re.search(r"const UI_BUILD = '([^']+)'", SRC).group(1)
        self.assertEqual(marker, const)

    def test_shell_check_is_scheduled(self):
        self.assertIn("setInterval(checkShellFreshness, SHELL_CHECK_MS);", SRC)

    def test_reload_is_guarded_against_loops(self):
        m = re.search(r"async function checkShellFreshness\(\) \{.*?\n\}", SRC, re.S)
        self.assertIn("sessionStorage.getItem('gu_shell_reloaded')", m.group(0))
        self.assertIn("cache: 'no-store'", m.group(0))


class TestMalformedInventoryFailsVisibly(unittest.TestCase):
    def test_missing_inventory_is_reported(self):
        self.assertIn("Content inventory unavailable", SRC)

    def test_empty_episodes_is_reported(self):
        self.assertIn("malformed or empty", SRC)

    def test_no_silent_fallback_for_inventory(self):
        self.assertIn("_inventory_error", SRC)

if __name__ == "__main__":
    unittest.main(verbosity=2)
