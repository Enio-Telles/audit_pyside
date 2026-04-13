from __future__ import annotations

from pathlib import Path

import polars as pl

from transformacao.ressarcimento_st_pkg.base import (
    STATUS_OK,
    STATUS_PARCIAL_POS_2022,
    STATUS_PENDENTE_CONVERSAO,
    alinhar_schema,
    caminho_analise,
    caminho_oracle,
    caminho_produtos,
    expr_ano_ref,
    expr_mes_ref,
    ler_parquet_opcional,
    salvar_df,
)


SCHEMA_ITEM = {
    "cnpj": pl.Utf8,
    "mes_ref": pl.Date,
    "ano_ref": pl.Int32,
    "periodo_efd": pl.Utf8,
    "chave_saida": pl.Utf8,
    "num_item_saida": pl.Int64,
    "cod_item_saida": pl.Utf8,
    "id_agrupado": pl.Utf8,
    "descr_padrao": pl.Utf8,
    "unid_ref": pl.Utf8,
    "fator_saida": pl.Float64,
    "fator_entrada": pl.Float64,
    "qtd_saida_unid_ref": pl.Float64,
    "qtd_entrada_xml": pl.Float64,
    "qtd_entrada_xml_unid_ref": pl.Float64,
    "chave_nfe_ultima_entrada": pl.Utf8,
    "prod_nitem_entrada": pl.Int64,
    "vl_unit_st_decl_unid_ref": pl.Float64,
    "vl_unit_icms_proprio_unid_ref": pl.Float64,
    "vl_credito_icms_total": pl.Float64,
    "calc_st_total_item": pl.Float64,
    "vl_unit_st_calc_unid_ref": pl.Float64,
    "fronteira_valor_icms_total_item": pl.Float64,
    "vl_unit_st_fronteira_unid_ref": pl.Float64,
    "vl_st_decl_total_considerado": pl.Float64,
    "vl_st_calc_total_considerado": pl.Float64,
    "vl_st_fronteira_total_considerado": pl.Float64,
    "possui_st_calc_ate_2022": pl.Boolean,
    "possui_fronteira": pl.Boolean,
    "possui_entrada_vinculada": pl.Boolean,
    "status_calculo": pl.Utf8,
    "motivo_validacao": pl.Utf8,
    "dif_calc_st_x_c176": pl.Float64,
    "dif_fronteira_st_x_c176": pl.Float64,
    "dif_fronteira_st_x_calc": pl.Float64,
}


def _normalizar_base_c176(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return alinhar_schema(pl.DataFrame(), SCHEMA_ITEM)

    return df.with_columns(
        expr_mes_ref("periodo_efd"),
        expr_ano_ref("periodo_efd"),
        pl.col("cnpj").cast(pl.Utf8, strict=False),
        pl.col("num_item_saida").cast(pl.Int64, strict=False),
        pl.col("cod_item_ref_saida").cast(pl.Utf8, strict=False).alias("cod_item_saida"),
        pl.col("id_agrupado").cast(pl.Utf8, strict=False),
        pl.col("descr_padrao").cast(pl.Utf8, strict=False),
        pl.col("unid_ref").cast(pl.Utf8, strict=False),
        pl.col("fator_saida").cast(pl.Float64, strict=False),
        pl.col("fator_entrada_xml").cast(pl.Float64, strict=False).alias("fator_entrada"),
        pl.col("qtd_saida_unid_ref").cast(pl.Float64, strict=False),
        pl.col("qtd_entrada_xml").cast(pl.Float64, strict=False),
        pl.col("qtd_entrada_xml_unid_ref").cast(pl.Float64, strict=False),
        pl.col("chave_nfe_ultima_entrada").cast(pl.Utf8, strict=False),
        pl.col("prod_nitem").cast(pl.Int64, strict=False).alias("prod_nitem_entrada"),
        pl.col("vl_unit_ressarcimento_st_unid_ref").cast(pl.Float64, strict=False).alias("vl_unit_st_decl_unid_ref"),
        pl.col("vl_unit_icms_proprio_entrada_unid_ref").cast(pl.Float64, strict=False).alias("vl_unit_icms_proprio_unid_ref"),
    ).select(
        "cnpj",
        "mes_ref",
        "ano_ref",
        "periodo_efd",
        "chave_saida",
        "num_item_saida",
        "cod_item_saida",
        "id_agrupado",
        "descr_padrao",
        "unid_ref",
        "fator_saida",
        "fator_entrada",
        "qtd_saida_unid_ref",
        "qtd_entrada_xml",
        "qtd_entrada_xml_unid_ref",
        "chave_nfe_ultima_entrada",
        "prod_nitem_entrada",
        "vl_unit_st_decl_unid_ref",
        "vl_unit_icms_proprio_unid_ref",
    )


def gerar_ressarcimento_st_item(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    cnpj_limpo = "".join(filter(str.isdigit, cnpj))
    caminho_c176 = caminho_produtos(cnpj_limpo, f"c176_xml_{cnpj_limpo}.parquet", pasta_cnpj)
    caminho_credito = caminho_analise(cnpj_limpo, f"credito_icms_item_{cnpj_limpo}.parquet", pasta_cnpj)
    caminho_st = caminho_oracle(cnpj_limpo, f"10_st_calc_ate_2022_{cnpj_limpo}.parquet", pasta_cnpj)
    caminho_fronteira_simples = caminho_oracle(cnpj_limpo, f"11_fronteira_item_simples_{cnpj_limpo}.parquet", pasta_cnpj)
    caminho_saida = caminho_analise(cnpj_limpo, f"ressarcimento_st_item_{cnpj_limpo}.parquet", pasta_cnpj)
    caminho_validacoes = caminho_analise(cnpj_limpo, f"ressarcimento_st_validacoes_{cnpj_limpo}.parquet", pasta_cnpj)

    df_base = _normalizar_base_c176(ler_parquet_opcional(caminho_c176))
    if df_base.is_empty():
        ok_item = salvar_df(alinhar_schema(pl.DataFrame(), SCHEMA_ITEM), caminho_saida)
        ok_val = salvar_df(alinhar_schema(pl.DataFrame(), SCHEMA_ITEM), caminho_validacoes)
        return ok_item and ok_val

    chaves_join = ["chave_nfe_ultima_entrada", "prod_nitem_entrada"]
    chaves_credito = [
        "chave_saida",
        "num_item_saida",
        "cod_item_saida",
        "chave_nfe_ultima_entrada",
        "prod_nitem_entrada",
    ]

    df_credito = ler_parquet_opcional(caminho_credito)
    df_st = ler_parquet_opcional(caminho_st)
    df_fronteira_simples = ler_parquet_opcional(caminho_fronteira_simples)

    df = (
        df_base
        .join(df_credito.select(chaves_credito + ["vl_credito_icms_total"]), on=chaves_credito, how="left")
        .join(df_st.select(chaves_join + ["calc_st_total_item"]), on=chaves_join, how="left")
        .join(
            df_fronteira_simples.select(
                chaves_join + ["fronteira_valor_icms_total_item"]
            ),
            on=chaves_join,
            how="left",
        )
        .with_columns(
            pl.col("calc_st_total_item").cast(pl.Float64, strict=False),
            pl.col("fronteira_valor_icms_total_item").cast(pl.Float64, strict=False),
            pl.col("vl_credito_icms_total").cast(pl.Float64, strict=False),
            pl.when(pl.col("qtd_entrada_xml_unid_ref") > 0)
            .then(pl.col("calc_st_total_item") / pl.col("qtd_entrada_xml_unid_ref"))
            .otherwise(None)
            .alias("vl_unit_st_calc_unid_ref"),
            pl.when(pl.col("qtd_entrada_xml_unid_ref") > 0)
            .then(pl.col("fronteira_valor_icms_total_item") / pl.col("qtd_entrada_xml_unid_ref"))
            .otherwise(None)
            .alias("vl_unit_st_fronteira_unid_ref"),
            (pl.coalesce([pl.col("vl_unit_st_decl_unid_ref"), pl.lit(0.0)]) * pl.coalesce([pl.col("qtd_saida_unid_ref"), pl.lit(0.0)])).alias("vl_st_decl_total_considerado"),
            pl.when(pl.col("ano_ref") <= 2022)
            .then(pl.coalesce([pl.col("vl_unit_st_calc_unid_ref"), pl.lit(0.0)]) * pl.coalesce([pl.col("qtd_saida_unid_ref"), pl.lit(0.0)]))
            .otherwise(None)
            .alias("vl_st_calc_total_considerado"),
            (pl.coalesce([pl.col("vl_unit_st_fronteira_unid_ref"), pl.lit(0.0)]) * pl.coalesce([pl.col("qtd_saida_unid_ref"), pl.lit(0.0)])).alias("vl_st_fronteira_total_considerado"),
            pl.col("calc_st_total_item").is_not_null().alias("possui_st_calc_ate_2022"),
            pl.col("fronteira_valor_icms_total_item").is_not_null().alias("possui_fronteira"),
            (pl.col("chave_nfe_ultima_entrada").is_not_null() & (pl.col("chave_nfe_ultima_entrada") != "") & pl.col("prod_nitem_entrada").is_not_null()).alias("possui_entrada_vinculada"),
        )
        .with_columns(
            pl.when(
                pl.col("id_agrupado").is_null()
                | (pl.col("id_agrupado") == "")
                | pl.col("unid_ref").is_null()
                | (pl.col("unid_ref") == "")
                | pl.col("fator_saida").is_null()
                | (pl.col("fator_saida") <= 0)
            )
            .then(pl.lit(STATUS_PENDENTE_CONVERSAO))
            .when(pl.col("ano_ref") > 2022)
            .then(pl.lit(STATUS_PARCIAL_POS_2022))
            .otherwise(pl.lit(STATUS_OK))
            .alias("status_calculo")
        )
        .with_columns(
            pl.when(pl.col("status_calculo") == STATUS_PENDENTE_CONVERSAO)
            .then(pl.lit("Conversao de unidade pendente ou incompleta"))
            .when(pl.col("status_calculo") == STATUS_PARCIAL_POS_2022)
            .then(pl.lit("Calculo oficial limitado a operacoes ate 2022"))
            .when(~pl.col("possui_entrada_vinculada"))
            .then(pl.lit("C176 sem NF de entrada vinculada"))
            .when((pl.col("ano_ref") <= 2022) & ~pl.col("possui_st_calc_ate_2022"))
            .then(pl.lit("Sem base ST material ate 2022"))
            .otherwise(pl.lit(""))
            .alias("motivo_validacao"),
            (pl.coalesce([pl.col("vl_st_calc_total_considerado"), pl.lit(0.0)]) - pl.coalesce([pl.col("vl_st_decl_total_considerado"), pl.lit(0.0)])).alias("dif_calc_st_x_c176"),
            (pl.coalesce([pl.col("vl_st_fronteira_total_considerado"), pl.lit(0.0)]) - pl.coalesce([pl.col("vl_st_decl_total_considerado"), pl.lit(0.0)])).alias("dif_fronteira_st_x_c176"),
            (pl.coalesce([pl.col("vl_st_fronteira_total_considerado"), pl.lit(0.0)]) - pl.coalesce([pl.col("vl_st_calc_total_considerado"), pl.lit(0.0)])).alias("dif_fronteira_st_x_calc"),
        )
    )

    df_item = alinhar_schema(df, SCHEMA_ITEM)
    df_validacoes = df_item.filter(
        (pl.col("status_calculo") != STATUS_OK)
        | ((pl.col("ano_ref") <= 2022) & ~pl.col("possui_st_calc_ate_2022"))
        | ~pl.col("possui_entrada_vinculada")
    )

    ok_item = salvar_df(df_item, caminho_saida)
    ok_validacoes = salvar_df(df_validacoes, caminho_validacoes)
    return ok_item and ok_validacoes
