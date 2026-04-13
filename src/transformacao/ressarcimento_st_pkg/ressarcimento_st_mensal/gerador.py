from __future__ import annotations

from pathlib import Path

import polars as pl

from transformacao.ressarcimento_st_pkg.base import alinhar_schema, caminho_analise, caminho_bruto, ler_parquet_opcional, salvar_df


SCHEMA_MENSAL = {
    "mes_ref": pl.Date,
    "qtd_itens_c176": pl.Int64,
    "vl_st_decl_total_mes": pl.Float64,
    "vl_st_calc_total_mes": pl.Float64,
    "vl_st_fronteira_total_mes": pl.Float64,
    "vl_e111_st_mes": pl.Float64,
    "vl_e111_st_extemporaneo_mes": pl.Float64,
    "dif_c176_st_x_e111": pl.Float64,
    "dif_calc_st_x_c176": pl.Float64,
    "dif_fronteira_st_x_c176": pl.Float64,
    "dif_fronteira_st_x_calc": pl.Float64,
}


def _resumo_e111(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return alinhar_schema(pl.DataFrame(), {"mes_ref": pl.Date, "vl_e111_st_mes": pl.Float64, "vl_e111_st_extemporaneo_mes": pl.Float64})

    base = df.with_columns(
        (
            pl.col("periodo_efd")
            .cast(pl.Utf8, strict=False)
            .str.replace("/", "-")
            + pl.lit("-01")
        )
        .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
        .alias("mes_ref"),
        pl.col("codigo_ajuste").cast(pl.Utf8, strict=False),
        pl.col("valor_ajuste").cast(pl.Float64, strict=False),
    )

    return base.group_by("mes_ref").agg(
        pl.sum(
            pl.when(
                ((pl.col("mes_ref") < pl.date(2025, 1, 1)) & (pl.col("codigo_ajuste") == "RO020022"))
                | ((pl.col("mes_ref") >= pl.date(2025, 1, 1)) & (pl.col("codigo_ajuste") == "RO020047"))
            )
            .then(pl.col("valor_ajuste"))
            .otherwise(0.0)
        ).alias("vl_e111_st_mes"),
        pl.sum(
            pl.when(pl.col("codigo_ajuste") == "RO020048")
            .then(pl.col("valor_ajuste"))
            .otherwise(0.0)
        ).alias("vl_e111_st_extemporaneo_mes"),
    )


def gerar_ressarcimento_st_mensal(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    cnpj_limpo = "".join(filter(str.isdigit, cnpj))
    caminho_item = caminho_analise(cnpj_limpo, f"ressarcimento_st_item_{cnpj_limpo}.parquet", pasta_cnpj)
    caminho_e111 = caminho_bruto(cnpj_limpo, f"e111_{cnpj_limpo}.parquet", pasta_cnpj)
    if not caminho_e111.exists():
        caminho_e111 = caminho_bruto(cnpj_limpo, f"E111_{cnpj_limpo}.parquet", pasta_cnpj)
    caminho_saida = caminho_analise(cnpj_limpo, f"ressarcimento_st_mensal_{cnpj_limpo}.parquet", pasta_cnpj)

    df_item = ler_parquet_opcional(caminho_item)
    if df_item.is_empty():
        return salvar_df(alinhar_schema(pl.DataFrame(), SCHEMA_MENSAL), caminho_saida)

    resumo_item = df_item.group_by("mes_ref").agg(
        pl.len().alias("qtd_itens_c176"),
        pl.sum(pl.coalesce([pl.col("vl_st_decl_total_considerado"), pl.lit(0.0)])).alias("vl_st_decl_total_mes"),
        pl.sum(pl.coalesce([pl.col("vl_st_calc_total_considerado"), pl.lit(0.0)])).alias("vl_st_calc_total_mes"),
        pl.sum(pl.coalesce([pl.col("vl_st_fronteira_total_considerado"), pl.lit(0.0)])).alias("vl_st_fronteira_total_mes"),
    )
    resumo_e111 = _resumo_e111(ler_parquet_opcional(caminho_e111))

    df_saida = (
        resumo_item
        .join(resumo_e111, on="mes_ref", how="left")
        .with_columns(
            pl.coalesce([pl.col("vl_e111_st_mes"), pl.lit(0.0)]).alias("vl_e111_st_mes"),
            pl.coalesce([pl.col("vl_e111_st_extemporaneo_mes"), pl.lit(0.0)]).alias("vl_e111_st_extemporaneo_mes"),
        )
        .with_columns(
            (pl.col("vl_st_decl_total_mes") - pl.col("vl_e111_st_mes")).alias("dif_c176_st_x_e111"),
            (pl.col("vl_st_calc_total_mes") - pl.col("vl_st_decl_total_mes")).alias("dif_calc_st_x_c176"),
            (pl.col("vl_st_fronteira_total_mes") - pl.col("vl_st_decl_total_mes")).alias("dif_fronteira_st_x_c176"),
            (pl.col("vl_st_fronteira_total_mes") - pl.col("vl_st_calc_total_mes")).alias("dif_fronteira_st_x_calc"),
        )
        .sort("mes_ref")
    )

    return salvar_df(alinhar_schema(df_saida, SCHEMA_MENSAL), caminho_saida)
