"""Snapshot da geracao de id_agrupado_base antes de map_batches.

Origem: `git show origin/main:src/transformacao/rastreabilidade_produtos/03_descricao_produtos.py`,
antes da PR #193.
"""

import hashlib

import polars as pl


def _gerar_id_agrupado_automatico(texto_normalizado: str | None) -> str:
    texto = (texto_normalizado or "").strip()
    digest = hashlib.sha1(texto.encode("utf-8")).hexdigest()[:12]
    return f"id_agrupado_auto_{digest}"


def gerar_id_agrupado_automatico_expr(col: str = "descricao_normalizada") -> pl.Expr:
    return (
        pl.col(col)
        .cast(pl.Utf8, strict=False)
        .fill_null("")
        .map_elements(_gerar_id_agrupado_automatico, return_dtype=pl.Utf8)
        .alias("id_agrupado_base")
    )