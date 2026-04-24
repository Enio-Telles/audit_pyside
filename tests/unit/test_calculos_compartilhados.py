from __future__ import annotations

from datetime import date

import polars as pl

from utilitarios.calculos_compartilhados import boolish_expr, format_st_periodos


def test_boolish_expr_true_values() -> None:
    df = pl.DataFrame({"flag": ["true", "1", "s", "sim", "y", "yes"]})
    result = df.select(boolish_expr("flag")).to_series().to_list()
    assert all(result)


def test_boolish_expr_false_values() -> None:
    df = pl.DataFrame({"flag": ["false", "0", "n", "nao", "no", ""]})
    result = df.select(boolish_expr("flag")).to_series().to_list()
    assert not any(result)


def test_boolish_expr_null() -> None:
    df = pl.DataFrame({"flag": [None]})
    result = df.select(boolish_expr("flag")).to_series().to_list()
    assert result == [False]


def test_format_st_periodos_empty() -> None:
    assert format_st_periodos([]) == ""
    assert format_st_periodos(None) == ""


def test_format_st_periodos_with_records() -> None:
    records = [
        {"it_in_st": "S", "vig_ini": date(2023, 1, 1), "vig_fim": date(2023, 12, 31)},
        {"it_in_st": "N", "vig_ini": date(2024, 1, 1), "vig_fim": date(2024, 6, 30)},
    ]
    result = format_st_periodos(records)
    assert "S" in result
    assert "N" in result
    assert "01/01/2023" in result


def test_format_st_periodos_skips_incomplete() -> None:
    records = [{"it_in_st": "S", "vig_ini": None, "vig_fim": date(2023, 12, 31)}]
    assert format_st_periodos(records) == ""


def test_format_st_periodos_polars_series() -> None:
    s = pl.Series([None])
    result = format_st_periodos(s)
    assert result == ""


def test_format_st_periodos_sorted() -> None:
    records = [
        {"it_in_st": "N", "vig_ini": date(2024, 1, 1), "vig_fim": date(2024, 6, 30)},
        {"it_in_st": "S", "vig_ini": date(2022, 1, 1), "vig_fim": date(2022, 12, 31)},
    ]
    result = format_st_periodos(records)
    idx_2022 = result.index("2022")
    idx_2024 = result.index("2024")
    assert idx_2022 < idx_2024
