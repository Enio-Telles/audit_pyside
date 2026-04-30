from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_similarity_tab_uses_shared_filter_and_profile_helpers():
    source = (ROOT / "audit_react" / "tab-similaridade.jsx").read_text(
        encoding="utf-8"
    )

    assert "rowMatchesFlexibleQuery" in source
    assert "applyColumnFilters" in source
    assert "applyColumnProfile" in source
    assert "columnFilters" in source
    assert "similaridadeProfiles" in source


def test_frontend_loads_table_utils_before_similarity_tab():
    html = (ROOT / "audit_pyside_frontend.html").read_text(encoding="utf-8")

    table_utils_pos = html.index("audit_react/table-utils.js")
    similarity_pos = html.index("audit_react/tab-similaridade.jsx")

    assert table_utils_pos < similarity_pos
