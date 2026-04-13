from __future__ import annotations

from pathlib import Path

import polars as pl

from transformacao.ressarcimento_st_pkg.base import (
    alinhar_schema,
    caminho_analise,
    caminho_produtos,
    expr_mes_ref,
    ler_parquet_opcional,
    salvar_df,
)


SCHEMA_CREDITO_ITEM = {
    "chave_saida": pl.Utf8,
    "num_item_saida": pl.Int64,
    "cod_item_saida": pl.Utf8,
    "chave_nfe_ultima_entrada": pl.Utf8,
    "prod_nitem_entrada": pl.Int64,
    "mes_ref": pl.Date,
    "vl_unit_icms_proprio_unid_ref": pl.Float64,
    "qtd_saida_unid_ref": pl.Float64,
    "vl_credito_icms_total": pl.Float64,
}


def gerar_credito_icms_item(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    cnpj_limpo = "".join(filter(str.isdigit, cnpj))
    caminho_c176 = caminho_produtos(cnpj_limpo, f"c176_xml_{cnpj_limpo}.parquet", pasta_cnpj)
    caminho_saida = caminho_analise(cnpj_limpo, f"credito_icms_item_{cnpj_limpo}.parquet", pasta_cnpj)

    df_c176 = ler_parquet_opcional(caminho_c176)
    if df_c176.is_empty():
        return salvar_df(alinhar_schema(pl.DataFrame(), SCHEMA_CREDITO_ITEM), caminho_saida)

    df = (
        df_c176
        .with_columns(
            expr_mes_ref("periodo_efd"),
            pl.col("num_item_saida").cast(pl.Int64, strict=False),
            pl.col("cod_item_ref_saida").cast(pl.Utf8, strict=False).alias("cod_item_saida"),
            pl.col("chave_nfe_ultima_entrada").cast(pl.Utf8, strict=False),
            pl.col("prod_nitem").cast(pl.Int64, strict=False).alias("prod_nitem_entrada"),
            pl.col("vl_unit_icms_proprio_entrada_unid_ref").cast(pl.Float64, strict=False).alias("vl_unit_icms_proprio_unid_ref"),
            pl.col("qtd_saida_unid_ref").cast(pl.Float64, strict=False),
        )
        .select(
            "chave_saida",
            "num_item_saida",
            "cod_item_saida",
            "chave_nfe_ultima_entrada",
            "prod_nitem_entrada",
            "mes_ref",
            "vl_unit_icms_proprio_unid_ref",
            "qtd_saida_unid_ref",
        )
        .with_columns(
            (
                pl.coalesce([pl.col("vl_unit_icms_proprio_unid_ref"), pl.lit(0.0)])
                * pl.coalesce([pl.col("qtd_saida_unid_ref"), pl.lit(0.0)])
            ).alias("vl_credito_icms_total")
        )
    )

    return salvar_df(alinhar_schema(df, SCHEMA_CREDITO_ITEM), caminho_saida)
