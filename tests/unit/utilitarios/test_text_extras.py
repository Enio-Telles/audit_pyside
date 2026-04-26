from __future__ import annotations

from decimal import Decimal

from utilitarios.text import display_cell, normalize_text, remove_accents


def test_remove_accents_none_returns_none() -> None:
    assert remove_accents(None) is None


def test_normalize_text_none_returns_empty() -> None:
    assert normalize_text(None) == ""


def test_display_cell_none_returns_empty() -> None:
    assert display_cell(None) == ""


def test_display_cell_invalid_date_month_triggers_value_error() -> None:
    # "2024-13-01" matches the ISO date pattern but month=13 raises ValueError
    # _parse_data_iso returns None → display_cell returns the original string
    assert display_cell("2024-13-01") == "2024-13-01"


def test_display_cell_invalid_date_day_triggers_value_error() -> None:
    assert display_cell("2024-02-30") == "2024-02-30"


def test_display_cell_to_list_success() -> None:
    class FakeList:
        def to_list(self):
            return [1, 2, 3]

    result = display_cell(FakeList())
    assert result == "1, 2, 3"


def test_display_cell_to_list_exception_falls_through() -> None:
    class BrokenList:
        def __str__(self) -> str:
            return "<broken>"

        def to_list(self):
            raise RuntimeError("broken")

    instance = BrokenList()
    result = display_cell(instance)
    assert result == str(instance)


def test_display_cell_decimal_nan_returns_empty() -> None:
    assert display_cell(Decimal("NaN")) == ""


def test_display_cell_decimal_inf_returns_empty() -> None:
    assert display_cell(Decimal("Infinity")) == ""


def test_display_cell_unknown_type_uses_str() -> None:
    class Weird:
        def __str__(self) -> str:
            return "weird_value"

    assert display_cell(Weird()) == "weird_value"
