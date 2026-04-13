from __future__ import annotations

from pathlib import Path

import polars as pl

from transformacao.ressarcimento_st_pkg.base import alinhar_schema, caminho_analise, ler_parquet_opcional, salvar_df


SCHEMA_CONCILIACAO = {
    "mes_ref": pl.Date,
    "qtd_itens_c176": pl.Int64,
    "qtd_pendencias_conversao": pl.Int64,
    "qtd_parcial_pos_2022": pl.Int64,
    "qtd_itens_com_st_calc": pl.Int64,
    "qtd_itens_com_fronteira": pl.Int64,
    "vl_st_decl_total_mes": pl.Float64,
    "vl_st_calc_total_mes": pl.Float64,
    "vl_st_fronteira_total_mes": pl.Float64,
    "vl_e111_st_mes": pl.Float64,
    "vl_e111_st_extemporaneo_mes": pl.Float64,
    "dif_c176_st_x_e111": pl.Float64,
    "dif_calc_st_x_c176": pl.Float64,
    "dif_fronteira_st_x_c176": pl.Float64,
    "dif_fronteira_st_x_calc": pl.Float64,
    "cobertura_st_calc_pct": pl.Float64,
    "cobertura_fronteira_pct": pl.Float64,
}


def gerar_ressarcimento_st_conciliacao(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    cnpj_limpo = "".join(filter(str.isdigit, cnpj))
    caminho_item = caminho_analise(cnpj_limpo, f"ressarcimento_st_item_{cnpj_limpo}.parquet", pasta_cnpj)
    caminho_mensal = caminho_analise(cnpj_limpo, f"ressarcimento_st_mensal_{cnpj_limpo}.parquet", pasta_cnpj)
    caminho_saida = caminho_analise(cnpj_limpo, f"ressarcimento_st_conciliacao_{cnpj_limpo}.parquet", pasta_cnpj)

    df_item = ler_parquet_opcional(caminho_item)
    df_mensal = ler_parquet_opcional(caminho_mensal)

    if df_item.is_empty() and df_mensal.is_empty():
        return salvar_df(alinhar_schema(pl.DataFrame(), SCHEMA_CONCILIACAO), caminho_saida)

    contagens = (
        df_item.group_by("mes_ref").agg(
            pl.len().alias("qtd_itens_c176"),
            pl.sum(pl.when(pl.col("status_calculo") == "pendente_conversao").then(1).otherwise(0)).alias("qtd_pendencias_conversao"),
            pl.sum(pl.when(pl.col("status_calculo") == "parcial_pos_2022").then(1).otherwise(0)).alias("qtd_parcial_pos_2022"),
            pl.sum(pl.when(pl.col("possui_st_calc_ate_2022")).then(1).otherwise(0)).alias("qtd_itens_com_st_calc"),
            pl.sum(pl.when(pl.col("possui_fronteira")).then(1).otherwise(0)).alias("qtd_itens_com_fronteira"),
        )
        .with_columns(
            pl.when(pl.col("qtd_itens_c176") > 0)
            .then((pl.col("qtd_itens_com_st_calc") * 100.0) / pl.col("qtd_itens_c176"))
            .otherwise(0.0)
            .alias("cobertura_st_calc_pct"),
            pl.when(pl.col("qtd_itens_c176") > 0)
            .then((pl.col("qtd_itens_com_fronteira") * 100.0) / pl.col("qtd_itens_c176"))
            .otherwise(0.0)
            .alias("cobertura_fronteira_pct"),
        )
    )

    df_saida = df_mensal.join(contagens, on=["mes_ref", "qtd_itens_c176"], how="left")
    return salvar_df(alinhar_schema(df_saida, SCHEMA_CONCILIACAO), caminho_saida)
