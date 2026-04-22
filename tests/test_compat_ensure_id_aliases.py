from __future__ import annotations

import polars as pl

from src.utilitarios.compat import ensure_id_aliases


def _dtype(df: pl.DataFrame, col: str):
    return df.select(pl.col(col)).dtypes[0]


def test_only_id_agrupado_creates_alias_and_casts():
    df = pl.DataFrame({"id_agrupado": ["a", "b"], "x": [1, 2]})
    out = ensure_id_aliases(df)
    assert "id_agrupado" in out.columns
    assert "id_agregado" in out.columns
    assert out["id_agrupado"].to_list() == out["id_agregado"].to_list()
    assert _dtype(out, "id_agrupado") == pl.Utf8
    assert _dtype(out, "id_agregado") == pl.Utf8


def test_only_id_agregado_creates_canonical_and_casts():
    df = pl.DataFrame({"id_agregado": ["p1", "p2"], "v": [10, 20]})
    out = ensure_id_aliases(df)
    assert "id_agrupado" in out.columns
    assert "id_agregado" in out.columns
    assert out["id_agregado"].to_list() == out["id_agrupado"].to_list()
    assert _dtype(out, "id_agregado") == pl.Utf8
    assert _dtype(out, "id_agrupado") == pl.Utf8


def test_both_present_and_equal_are_preserved_and_cast():
    df = pl.DataFrame({"id_agrupado": ["x", "y"], "id_agregado": ["x", "y"]})
    out = ensure_id_aliases(df.lazy())
    assert "id_agrupado" in out.columns and "id_agregado" in out.columns
    assert out["id_agrupado"].to_list() == ["x", "y"]
    assert out["id_agregado"].to_list() == ["x", "y"]
    assert _dtype(out, "id_agrupado") == pl.Utf8
    assert _dtype(out, "id_agregado") == pl.Utf8


def test_both_present_and_divergent_are_preserved_and_cast():
    df = pl.DataFrame({"id_agrupado": ["a", "b"], "id_agregado": ["A", "B"]})
    out = ensure_id_aliases(df)
    assert out["id_agrupado"].to_list() == ["a", "b"]
    assert out["id_agregado"].to_list() == ["A", "B"]
    assert _dtype(out, "id_agrupado") == pl.Utf8
    assert _dtype(out, "id_agregado") == pl.Utf8


def test_neither_present_leaves_frame_unchanged():
    df = pl.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    out = ensure_id_aliases(df)
    assert "id_agrupado" not in out.columns
    assert "id_agregado" not in out.columns
