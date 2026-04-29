from __future__ import annotations

import math
import re
from typing import Any

import polars as pl


def norm_text_expr(col: str, alias: str | None = None) -> pl.Expr:
    """Cria uma expressão Polars para normalização de texto em colunas.

    Substitui acentos, remove espaços extras e converte para maiúsculas de forma vetorizada.
    """
    expr = (
        pl.col(col)
        .cast(pl.Utf8, strict=False)
        .fill_null("")
        .str.to_uppercase()
        .str.replace_all(r"[ÁÀÂÃÄ]", "A")
        .str.replace_all(r"[ÉÈÊË]", "E")
        .str.replace_all(r"[ÍÌÎÏ]", "I")
        .str.replace_all(r"[ÓÒÔÕÖ]", "O")
        .str.replace_all(r"[ÚÙÛÜ]", "U")
        .str.replace_all(r"Ç", "C")
        .str.replace_all(r"Ñ", "N")
        .str.strip_chars()
        .str.replace_all(r"\s+", " ")
    )
    return expr.alias(alias or col)


def clean_digits_expr(col: str, alias: str | None = None) -> pl.Expr:
    """Mantém apenas dígitos em uma coluna de forma vetorizada."""
    expr = (
        pl.col(col)
        .cast(pl.Utf8, strict=False)
        .fill_null("")
        .str.replace_all(r"\D", "")
        .str.strip_chars()
    )
    return expr.alias(alias or col)


def to_float_expr(col: str, alias: str | None = None) -> pl.Expr:
    """Converte coluna para Float64 com tratamento de erros."""
    return pl.col(col).cast(pl.Float64, strict=False).alias(alias or col)


def to_int_expr(col: str, alias: str | None = None) -> pl.Expr:
    """Converte coluna para Int64 com tratamento de erros."""
    return pl.col(col).cast(pl.Int64, strict=False).alias(alias or col)


def boolish_expr(col: str, alias: str | None = None) -> pl.Expr:
    """Converte valores 'S'/'N', '1'/'0', true/false para Boolean."""
    c = pl.col(col).cast(pl.Utf8).str.to_uppercase().str.strip_chars()
    expr = pl.when(c.is_in(["S", "1", "TRUE", "SIM"])).then(True).otherwise(False)
    return expr.alias(alias or col)


def safe_value(val: Any) -> Any:
    """Trata valores NaN ou Inf para retorno JSON seguro."""
    if val is None:
        return None
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return None
    return val


def sanitize_cnpj(cnpj: str | None) -> str:
    """Remove caracteres não numéricos do CNPJ."""
    if cnpj is None:
        return ""
    limpo = re.sub(r"\D", "", str(cnpj))
    return limpo
