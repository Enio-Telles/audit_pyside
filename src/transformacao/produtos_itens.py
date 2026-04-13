"""
produtos_itens.py

Gera tabela de itens vinculados ao cadastro de produtos.

Saida:
- produtos_itens_<cnpj>.parquet em analises/produtos

Campos principais:
- chave_item
- chave_produto (compatibilidade)
- descricao
- codigo
- descr_compl
- tipo_item
- ncm
- cest
- gtin
- lista_unid
"""

from __future__ import annotations

import re
from pathlib import Path
from utilitarios.project_paths import PROJECT_ROOT

import polars as pl

ROOT_DIR = PROJECT_ROOT
DADOS_DIR = ROOT_DIR / "dados"
CNPJ_ROOT = DADOS_DIR / "CNPJ"


def _normalizar_descricao_expr(col: str) -> pl.Expr:
    return (
        pl.col(col)
        .cast(pl.Utf8, strict=False)
        .fill_null("")
        .str.normalize("NFKD")
        .str.replace_all(r"[\u0300-\u036f]", "")
        .str.to_uppercase()
        .str.strip_chars()
        .str.replace_all(r"\s+", " ")
        .alias("__descricao_normalizada__")
    )


def gerar_produtos_itens(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    cnpj = re.sub(r"\D", "", cnpj or "")
    if len(cnpj) not in {11, 14}:
        raise ValueError("CPF/CNPJ invalido.")

    if pasta_cnpj is None:
        pasta_cnpj = CNPJ_ROOT / cnpj

    pasta_analises = pasta_cnpj / "analises" / "produtos"
    arq_unid = pasta_analises / f"produtos_unidades_{cnpj}.parquet"
    arq_prod = pasta_analises / f"produtos_{cnpj}.parquet"
    arq_saida = pasta_analises / f"produtos_itens_{cnpj}.parquet"

    if not arq_unid.exists() or not arq_prod.exists():
        return False

    df_unid = pl.read_parquet(arq_unid)
    df_prod = pl.read_parquet(arq_prod)
    if "chave_item" not in df_prod.columns and "chave_produto" in df_prod.columns:
        df_prod = df_prod.with_columns(pl.col("chave_produto").alias("chave_item"))
    if "chave_produto" not in df_prod.columns and "chave_item" in df_prod.columns:
        df_prod = df_prod.with_columns(pl.col("chave_item").alias("chave_produto"))
    df_prod = df_prod.select(["chave_item", "chave_produto", "descricao_normalizada"])

    required_cols = ["descricao", "codigo", "descr_compl", "tipo_item", "ncm", "cest", "gtin", "unid"]
    for col in required_cols:
        if col not in df_unid.columns:
            df_unid = df_unid.with_columns(pl.lit(None, pl.Utf8).alias(col))

    df_items = (
        df_unid
        .with_columns(_normalizar_descricao_expr("descricao"))
        .join(
            df_prod.rename({"descricao_normalizada": "__descricao_normalizada__"}),
            on="__descricao_normalizada__",
            how="left",
        )
        .drop("__descricao_normalizada__")
    )

    df_items = (
        df_items
        .with_columns(
            [
                pl.col("descricao").cast(pl.Utf8, strict=False).alias("descricao"),
                pl.col("codigo").cast(pl.Utf8, strict=False).alias("codigo"),
                pl.col("descr_compl").cast(pl.Utf8, strict=False).alias("descr_compl"),
                pl.col("tipo_item").cast(pl.Utf8, strict=False).alias("tipo_item"),
                pl.col("ncm").cast(pl.Utf8, strict=False).alias("ncm"),
                pl.col("cest").cast(pl.Utf8, strict=False).alias("cest"),
                pl.col("gtin").cast(pl.Utf8, strict=False).alias("gtin"),
                pl.col("unid").cast(pl.Utf8, strict=False).alias("unid"),
            ]
        )
        .group_by(
            [
                "chave_item",
                "chave_produto",
                "descricao",
                "codigo",
                "descr_compl",
                "tipo_item",
                "ncm",
                "cest",
                "gtin",
            ]
        )
        .agg(
            pl.col("unid")
            .drop_nulls()
            .filter(pl.col("unid").str.strip_chars() != "")
            .unique()
            .sort()
            .alias("lista_unid")
        )
        .sort(["chave_item", "descricao"], nulls_last=True)
    )

    arq_saida.parent.mkdir(parents=True, exist_ok=True)
    df_items.write_parquet(arq_saida, compression="snappy")
    return True



