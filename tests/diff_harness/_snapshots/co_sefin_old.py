"""Snapshot de inferencia de co_sefin antes do projection pushdown.

Origem: `git show origin/main:src/transformacao/movimentacao_estoque_pkg/co_sefin.py`,
antes da PR #191.
"""

from pathlib import Path

import polars as pl


def _limpar_expr(coluna: str) -> pl.Expr:
    return pl.col(coluna).cast(pl.String, strict=False).str.replace_all(r"\.", "").str.strip_chars()


def _resolver_ref(nome_arquivo: str) -> Path:
    raise RuntimeError("_resolver_ref deve ser sobrescrito pelo teste diferencial")


def inferir_co_sefin_dataframe(
    df: pl.DataFrame,
    col_ncm: str = "ncm",
    col_cest: str = "cest",
    output_col: str = "co_sefin_inferido",
) -> pl.DataFrame:
    if df.is_empty():
        return df.with_columns(pl.lit(None, dtype=pl.String).alias(output_col))

    ref_cest_ncm_path = _resolver_ref("sitafe_cest_ncm.parquet")
    ref_cest_path = _resolver_ref("sitafe_cest.parquet")
    ref_ncm_path = _resolver_ref("sitafe_ncm.parquet")

    for caminho in [ref_cest_ncm_path, ref_cest_path, ref_ncm_path]:
        if not caminho.exists():
            raise FileNotFoundError(f"Tabela de referencia nao encontrada: {caminho}")

    df_base = df
    if col_ncm not in df_base.columns:
        df_base = df_base.with_columns(pl.lit(None, dtype=pl.String).alias(col_ncm))
    if col_cest not in df_base.columns:
        df_base = df_base.with_columns(pl.lit(None, dtype=pl.String).alias(col_cest))

    ref_cn = (
        pl.read_parquet(ref_cest_ncm_path)
        .select(["it_nu_cest", "it_nu_ncm", "it_co_sefin"])
        .with_columns(
            [
                _limpar_expr("it_nu_cest").alias("ref_cest"),
                _limpar_expr("it_nu_ncm").alias("ref_ncm"),
                pl.col("it_co_sefin").cast(pl.String, strict=False),
            ]
        )
        .drop(["it_nu_cest", "it_nu_ncm"])
    )

    ref_c = (
        pl.read_parquet(ref_cest_path)
        .select(["cest", "co-sefin"])
        .with_columns(_limpar_expr("cest").alias("ref_cest"))
        .drop("cest")
        .rename({"co-sefin": "co_sefin_cest"})
    )

    ref_n = (
        pl.read_parquet(ref_ncm_path)
        .select(["ncm", "co-sefin"])
        .with_columns(_limpar_expr("ncm").alias("ref_ncm"))
        .drop("ncm")
        .rename({"co-sefin": "co_sefin_ncm"})
    )

    df_join = df_base.with_columns(
        [
            _limpar_expr(col_ncm).alias("_ncm_join"),
            _limpar_expr(col_cest).alias("_cest_join"),
        ]
    )
    df_join = df_join.join(
        ref_cn,
        left_on=["_cest_join", "_ncm_join"],
        right_on=["ref_cest", "ref_ncm"],
        how="left",
    )
    df_join = df_join.join(ref_c, left_on="_cest_join", right_on="ref_cest", how="left")
    df_join = df_join.join(ref_n, left_on="_ncm_join", right_on="ref_ncm", how="left")

    return df_join.with_columns(
        pl.coalesce(["it_co_sefin", "co_sefin_cest", "co_sefin_ncm"])
        .cast(pl.String, strict=False)
        .alias(output_col)
    ).drop(
        ["_ncm_join", "_cest_join", "it_co_sefin", "co_sefin_cest", "co_sefin_ncm"],
        strict=False,
    )
