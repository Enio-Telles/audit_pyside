from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_frontend_loads_datagrid_between_table_utils_and_tabs():
    html = (ROOT / "audit_pyside_frontend.html").read_text(encoding="utf-8")

    table_utils_pos = html.index("audit_react/table-utils.js")
    datagrid_pos = html.index("audit_react/data-grid.jsx")
    tabs_pos = html.index("audit_react/tabs-other.jsx")

    assert table_utils_pos < datagrid_pos < tabs_pos


def test_datagrid_component_uses_shared_filter_profile_helpers():
    source = (ROOT / "audit_react" / "data-grid.jsx").read_text(encoding="utf-8")

    assert "function DataGrid" in source
    assert "rowMatchesFlexibleQuery" in source
    assert "applyColumnFilters" in source
    assert "applyColumnProfile" in source
    assert "audit.tableProfile." in source
    assert "window.DataGrid = DataGrid" in source


def test_manual_aggregation_uses_datagrid_with_profiles_and_flexible_search():
    source = (ROOT / "audit_react" / "tabs-other.jsx").read_text(encoding="utf-8")

    assert "const agregacaoColumns" in source
    assert "const agregacaoProfiles" in source
    assert "<DataGrid" in source
    assert 'tableId="agregacao-manual"' in source
    assert "rowMatchesFlexibleQuery" not in source
