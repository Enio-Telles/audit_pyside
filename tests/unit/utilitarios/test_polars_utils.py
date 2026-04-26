from __future__ import annotations

import math

import polars as pl
import pytest

from utilitarios.polars_utils import (
    boolish_expr,
    clean_digits_expr,
    norm_text_expr,
    safe_value,
    sanitize_cnpj,
    to_float_expr,
    to_int_expr,
)


def test_norm_text_expr_removes_accents_and_uppercases() -> None:
    df = pl.DataFrame({"x": ["café  ", "AÇÚCAR", "ñoño"]})
    result = df.with_columns(norm_text_expr("x"))
    assert result["x"][0] == "CAFE"
    assert result["x"][1] == "ACUCAR"
    assert result["x"][2] == "NONO"


def test_norm_text_expr_handles_null() -> None:
    df = pl.DataFrame({"x": [None]})
    result = df.with_columns(norm_text_expr("x"))
    assert result["x"][0] == ""


def test_norm_text_expr_alias() -> None:
    df = pl.DataFrame({"x": ["abc"]})
    result = df.with_columns(norm_text_expr("x", alias="y"))
    assert "y" in result.columns
    assert "x" in result.columns


def test_clean_digits_expr_removes_non_digits() -> None:
    df = pl.DataFrame({"cpf": ["123.456.789-00", "abc", None]})
    result = df.with_columns(clean_digits_expr("cpf"))
    assert result["cpf"][0] == "12345678900"
    assert result["cpf"][1] == ""
    assert result["cpf"][2] == ""


def test_clean_digits_expr_alias() -> None:
    df = pl.DataFrame({"cpf": ["00.000.000/0001-00"]})
    result = df.with_columns(clean_digits_expr("cpf", alias="digits"))
    assert "digits" in result.columns


def test_to_float_expr_converts() -> None:
    df = pl.DataFrame({"v": ["1.5", "abc", None]})
    result = df.with_columns(to_float_expr("v"))
    assert result["v"][0] == pytest.approx(1.5)
    assert result["v"][1] is None
    assert result["v"][2] is None


def test_to_int_expr_converts() -> None:
    df = pl.DataFrame({"v": ["3", "abc", None]})
    result = df.with_columns(to_int_expr("v"))
    assert result["v"][0] == 3
    assert result["v"][1] is None


def test_boolish_expr_true_values() -> None:
    df = pl.DataFrame({"flag": ["S", "1", "TRUE", "sim"]})
    result = df.with_columns(boolish_expr("flag"))
    assert all(result["flag"].to_list())


def test_boolish_expr_false_values() -> None:
    df = pl.DataFrame({"flag": ["N", "0", "false", "nao", ""]})
    result = df.with_columns(boolish_expr("flag"))
    assert not any(result["flag"].to_list())


def test_safe_value_none_returns_none() -> None:
    assert safe_value(None) is None


def test_safe_value_nan_returns_none() -> None:
    assert safe_value(float("nan")) is None


def test_safe_value_inf_returns_none() -> None:
    assert safe_value(float("inf")) is None
    assert safe_value(float("-inf")) is None


def test_safe_value_normal_passthrough() -> None:
    assert safe_value(42) == 42
    assert safe_value("hello") == "hello"
    assert safe_value(3.14) == pytest.approx(3.14)


def test_sanitize_cnpj_none_returns_empty() -> None:
    assert sanitize_cnpj(None) == ""


def test_sanitize_cnpj_strips_formatting() -> None:
    assert sanitize_cnpj("12.345.678/0001-90") == "12345678000190"


def test_sanitize_cnpj_already_clean() -> None:
    assert sanitize_cnpj("12345678000190") == "12345678000190"
