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
    return (
        pl.col(col)
        .cast(pl.Utf8, strict=False)
        .map_elements(normalizar_codigo_fonte, return_dtype=pl.Utf8)
        .alias(alias)
    )


def expr_gerar_codigo_fonte(col_cnpj: str, col_codigo: str, alias: str = "codigo_fonte") -> pl.Expr:
    return (
        pl.struct([pl.col(col_cnpj), pl.col(col_codigo)])
        .map_elements(
            lambda row: gerar_codigo_fonte(row.get(col_cnpj), row.get(col_codigo)),
            return_dtype=pl.Utf8,
        )
        .alias(alias)
    )
