from __future__ import annotations

import re

import polars as pl


def _limpar_parte(valor: str | None) -> str:
    texto = str(valor or "").strip()
    texto = re.sub(r"\s+", " ", texto)
    return texto


def gerar_codigo_fonte(cnpj: str | None, codigo: str | None) -> str | None:
    cnpj_limpo = re.sub(r"\D", "", str(cnpj or ""))
    codigo_limpo = _limpar_parte(codigo)
    if not codigo_limpo:
        return None
    if cnpj_limpo:
        return f"{cnpj_limpo}|{codigo_limpo}"
    return codigo_limpo


def normalizar_codigo_fonte(valor: str | None) -> str | None:
    texto = _limpar_parte(valor)
    if not texto:
        return None
    if "|" not in texto:
        return texto
    esquerda, direita = texto.split("|", 1)
    esquerda = re.sub(r"\D", "", esquerda)
    direita = _limpar_parte(direita)
    if esquerda and direita:
        return f"{esquerda}|{direita}"
    if direita:
        return direita
    return None


def expr_normalizar_codigo_fonte(col: str, alias: str = "codigo_fonte") -> pl.Expr:
    c = pl.col(col).cast(pl.Utf8, strict=False)

    # Clean the input text
    texto = c.fill_null("").str.strip_chars().str.replace_all(r"\s+", " ")

    # Condition: no pipe
    no_pipe = ~texto.str.contains(r"\|")

    # Split
    split = texto.str.split_exact("|", 1)
    esquerda = split.struct.field("field_0").str.replace_all(r"\D", "")
    direita = split.struct.field("field_1").str.strip_chars().str.replace_all(r"\s+", " ")

    return (
        pl.when(texto == "").then(pl.lit(None, dtype=pl.Utf8))
        .when(no_pipe).then(texto)
        .when((esquerda != "") & (direita != "")).then(pl.concat_str([esquerda, pl.lit("|"), direita]))
        .when(direita != "").then(direita)
        .otherwise(pl.lit(None, dtype=pl.Utf8))
        .alias(alias)
    )


def expr_gerar_codigo_fonte(col_cnpj: str, col_codigo: str, alias: str = "codigo_fonte") -> pl.Expr:
    cnpj_expr = pl.col(col_cnpj).cast(pl.Utf8).fill_null("").str.replace_all(r"\D", "")
    cod_expr = pl.col(col_codigo).cast(pl.Utf8).fill_null("").str.strip_chars().str.replace_all(r"\s+", " ")

    return (
        pl.when(cod_expr == "")
        .then(pl.lit(None))
        .when(cnpj_expr != "")
        .then(pl.concat_str([cnpj_expr, pl.lit("|"), cod_expr]))
        .otherwise(cod_expr)
        .alias(alias)
    )
