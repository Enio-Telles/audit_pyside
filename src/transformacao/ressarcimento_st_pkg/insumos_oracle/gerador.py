from __future__ import annotations

from pathlib import Path

import polars as pl

from transformacao.ressarcimento_st_pkg.base import (
    alinhar_schema,
    caminho_bruto,
    caminho_oracle,
    ler_parquet_opcional,
    salvar_df,
)


SCHEMA_VIGENCIA = {
    "chave_nfe_ultima_entrada": pl.Utf8,
    "prod_nitem_entrada": pl.Int64,
    "it_co_sefin_efetivo": pl.Utf8,
    "it_pc_interna": pl.Float64,
    "it_in_st": pl.Utf8,
    "it_pc_mva": pl.Float64,
    "it_in_mva_ajustado": pl.Utf8,
}

SCHEMA_RATEIO = {
    "chave_nfe_ultima_entrada": pl.Utf8,
    "prod_nitem_entrada": pl.Int64,
    "rateio_frete_nf_item": pl.Float64,
    "rateio_icms_frete_nf_item": pl.Float64,
}

SCHEMA_ST_MATERIAL = {
    "chave_nfe_ultima_entrada": pl.Utf8,
    "prod_nitem_entrada": pl.Int64,
    "calc_st_total_item": pl.Float64,
    "icms_vbcst": pl.Float64,
    "icms_vicmsst": pl.Float64,
    "icms_vicmsstret": pl.Float64,
}

SCHEMA_FRONTEIRA_SIMPLES = {
    "chave_nfe_ultima_entrada": pl.Utf8,
    "prod_nitem_entrada": pl.Int64,
    "fronteira_tipo_operacao": pl.Utf8,
    "fronteira_chave_acesso": pl.Utf8,
    "fronteira_num_item": pl.Int64,
    "fronteira_cod_item": pl.Utf8,
    "fronteira_desc_item": pl.Utf8,
    "fronteira_ncm": pl.Utf8,
    "fronteira_cest": pl.Utf8,
    "fronteira_qtd_comercial": pl.Float64,
    "fronteira_valor_produto": pl.Float64,
    "fronteira_bc_icms_st_destacado": pl.Float64,
    "fronteira_icms_st_destacado": pl.Float64,
    "fronteira_co_sefin_calc": pl.Utf8,
    "fronteira_cod_rotina_calculo": pl.Utf8,
    "fronteira_valor_icms_total_item": pl.Float64,
}

SCHEMA_FRONTEIRA_COMPLETO = {
    "chave_nfe_ultima_entrada": pl.Utf8,
    "prod_nitem_entrada": pl.Int64,
    "fronteira_dt_entrada": pl.Date,
    "fronteira_comando": pl.Utf8,
    "fronteira_qtd_guias": pl.Int64,
    "fronteira_guia_exemplo": pl.Utf8,
    "fronteira_receita": pl.Utf8,
    "fronteira_valor_devido": pl.Float64,
    "fronteira_valor_pago": pl.Float64,
    "fronteira_situacao": pl.Utf8,
    "fronteira_co_sefin_lanc": pl.Utf8,
    "fronteira_nome_co_sefin": pl.Utf8,
    "fronteira_vl_merc_item": pl.Float64,
    "fronteira_vl_bc_merc_item": pl.Float64,
    "fronteira_aliq_item": pl.Float64,
    "fronteira_vl_tot_debito_item": pl.Float64,
    "fronteira_vl_credito_rateio": pl.Float64,
    "fronteira_vl_icms_recolher": pl.Float64,
    "fronteira_ind_produto_st": pl.Utf8,
    "fronteira_ind_cest_st": pl.Utf8,
    "fronteira_pc_interna_merc": pl.Float64,
}


def _normalizar_vigencia(df: pl.DataFrame) -> pl.DataFrame:
    if "chave_nfe_ultima_entrada" not in df.columns:
        return alinhar_schema(pl.DataFrame(), SCHEMA_VIGENCIA)
    return alinhar_schema(df, SCHEMA_VIGENCIA)


def _normalizar_rateio(df: pl.DataFrame) -> pl.DataFrame:
    if "rateio_frete_nf_item" not in df.columns:
        return alinhar_schema(pl.DataFrame(), SCHEMA_RATEIO)
    return alinhar_schema(df, SCHEMA_RATEIO)


def _normalizar_st_material(df: pl.DataFrame) -> pl.DataFrame:
    if "calc_st_total_item" in df.columns:
        return alinhar_schema(df, SCHEMA_ST_MATERIAL)

    if "chave_acesso" not in df.columns:
        return alinhar_schema(pl.DataFrame(), SCHEMA_ST_MATERIAL)

    convertido = df.select(
        pl.col("chave_acesso").cast(pl.Utf8, strict=False).alias("chave_nfe_ultima_entrada"),
        pl.col("prod_nitem").cast(pl.Int64, strict=False).alias("prod_nitem_entrada"),
        pl.coalesce(
            [
                pl.col("icms_vicmsstret").cast(pl.Float64, strict=False),
                pl.col("icms_vicmsst").cast(pl.Float64, strict=False),
                pl.lit(0.0),
            ]
        ).alias("calc_st_total_item"),
        pl.col("icms_vbcst").cast(pl.Float64, strict=False).alias("icms_vbcst"),
        pl.col("icms_vicmsst").cast(pl.Float64, strict=False).alias("icms_vicmsst"),
        pl.col("icms_vicmsstret").cast(pl.Float64, strict=False).alias("icms_vicmsstret"),
    )
    return alinhar_schema(convertido, SCHEMA_ST_MATERIAL)


def _normalizar_fronteira_simples(df: pl.DataFrame) -> pl.DataFrame:
    if "fronteira_chave_acesso" in df.columns:
        return alinhar_schema(df, SCHEMA_FRONTEIRA_SIMPLES)

    if "chave_acesso" not in df.columns:
        return alinhar_schema(pl.DataFrame(), SCHEMA_FRONTEIRA_SIMPLES)

    convertido = df.select(
        pl.col("chave_acesso").cast(pl.Utf8, strict=False).alias("chave_nfe_ultima_entrada"),
        pl.col("num_item").cast(pl.Int64, strict=False).alias("prod_nitem_entrada"),
        pl.col("tipo_operacao").cast(pl.Utf8, strict=False).alias("fronteira_tipo_operacao"),
        pl.col("chave_acesso").cast(pl.Utf8, strict=False).alias("fronteira_chave_acesso"),
        pl.col("num_item").cast(pl.Int64, strict=False).alias("fronteira_num_item"),
        pl.col("cod_item").cast(pl.Utf8, strict=False).alias("fronteira_cod_item"),
        pl.col("desc_item").cast(pl.Utf8, strict=False).alias("fronteira_desc_item"),
        pl.col("ncm").cast(pl.Utf8, strict=False).alias("fronteira_ncm"),
        pl.col("cest").cast(pl.Utf8, strict=False).alias("fronteira_cest"),
        pl.col("qtd_comercial").cast(pl.Float64, strict=False).alias("fronteira_qtd_comercial"),
        pl.col("valor_produto").cast(pl.Float64, strict=False).alias("fronteira_valor_produto"),
        pl.col("bc_icms_st_destacado").cast(pl.Float64, strict=False).alias("fronteira_bc_icms_st_destacado"),
        pl.col("icms_st_destacado").cast(pl.Float64, strict=False).alias("fronteira_icms_st_destacado"),
        pl.col("co_sefin").cast(pl.Utf8, strict=False).alias("fronteira_co_sefin_calc"),
        pl.col("cod_rotina_calculo").cast(pl.Utf8, strict=False).alias("fronteira_cod_rotina_calculo"),
        pl.col("valor_icms_fronteira").cast(pl.Float64, strict=False).alias("fronteira_valor_icms_total_item"),
    )
    return alinhar_schema(convertido, SCHEMA_FRONTEIRA_SIMPLES)


def _normalizar_fronteira_completo(df: pl.DataFrame) -> pl.DataFrame:
    if "chave_nfe_ultima_entrada" in df.columns:
        return alinhar_schema(df, SCHEMA_FRONTEIRA_COMPLETO)

    if "chave" not in df.columns:
        return alinhar_schema(pl.DataFrame(), SCHEMA_FRONTEIRA_COMPLETO)

    convertido = df.select(
        pl.col("chave").cast(pl.Utf8, strict=False).alias("chave_nfe_ultima_entrada"),
        pl.col("prod_nitem").cast(pl.Int64, strict=False).alias("prod_nitem_entrada"),
        pl.col("entrada").cast(pl.Date, strict=False).alias("fronteira_dt_entrada"),
        pl.col("comando").cast(pl.Utf8, strict=False).alias("fronteira_comando"),
        pl.lit(None).cast(pl.Int64).alias("fronteira_qtd_guias"),
        pl.col("guia").cast(pl.Utf8, strict=False).alias("fronteira_guia_exemplo"),
        pl.col("receita").cast(pl.Utf8, strict=False).alias("fronteira_receita"),
        pl.col("valor_devido").cast(pl.Float64, strict=False).alias("fronteira_valor_devido"),
        pl.col("valor_pago").cast(pl.Float64, strict=False).alias("fronteira_valor_pago"),
        pl.col("situação").cast(pl.Utf8, strict=False).alias("fronteira_situacao"),
        pl.col("co_sefin").cast(pl.Utf8, strict=False).alias("fronteira_co_sefin_lanc"),
        pl.col("nome_co_sefin").cast(pl.Utf8, strict=False).alias("fronteira_nome_co_sefin"),
        pl.col("vl_merc").cast(pl.Float64, strict=False).alias("fronteira_vl_merc_item"),
        pl.col("vl_bc_merc").cast(pl.Float64, strict=False).alias("fronteira_vl_bc_merc_item"),
        pl.col("aliq").cast(pl.Float64, strict=False).alias("fronteira_aliq_item"),
        pl.col("vl_tot_deb").cast(pl.Float64, strict=False).alias("fronteira_vl_tot_debito_item"),
        pl.col("vl_tot_cred").cast(pl.Float64, strict=False).alias("fronteira_vl_credito_rateio"),
        pl.col("vl_icms").cast(pl.Float64, strict=False).alias("fronteira_vl_icms_recolher"),
        pl.col("it_in_produto_st").cast(pl.Utf8, strict=False).alias("fronteira_ind_produto_st"),
        pl.col("it_in_cest_st").cast(pl.Utf8, strict=False).alias("fronteira_ind_cest_st"),
        pl.col("it_pc_interna").cast(pl.Float64, strict=False).alias("fronteira_pc_interna_merc"),
    )
    return alinhar_schema(convertido, SCHEMA_FRONTEIRA_COMPLETO)


def gerar_vigencia_sefin(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    cnpj_limpo = "".join(filter(str.isdigit, cnpj))
    caminho_saida = caminho_oracle(cnpj_limpo, f"07_sefin_vigencia_{cnpj_limpo}.parquet", pasta_cnpj)
    df = _normalizar_vigencia(ler_parquet_opcional(caminho_saida, SCHEMA_VIGENCIA))
    return salvar_df(df, caminho_saida)


def gerar_rateio_frete_cte(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    cnpj_limpo = "".join(filter(str.isdigit, cnpj))
    caminho_saida = caminho_oracle(cnpj_limpo, f"08_rateio_frete_cte_{cnpj_limpo}.parquet", pasta_cnpj)
    df = _normalizar_rateio(ler_parquet_opcional(caminho_saida, SCHEMA_RATEIO))
    return salvar_df(df, caminho_saida)


def gerar_st_material_ate_2022(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    cnpj_limpo = "".join(filter(str.isdigit, cnpj))
    caminho_saida = caminho_oracle(cnpj_limpo, f"10_st_calc_ate_2022_{cnpj_limpo}.parquet", pasta_cnpj)
    legado = caminho_bruto(cnpj_limpo, f"nfe_dados_st_{cnpj_limpo}.parquet", pasta_cnpj)
    if not legado.exists():
        legado = caminho_bruto(cnpj_limpo, f"Nfe_dados_ST_{cnpj_limpo}.parquet", pasta_cnpj)

    df_fonte = ler_parquet_opcional(caminho_saida if caminho_saida.exists() else legado, SCHEMA_ST_MATERIAL)
    df = _normalizar_st_material(df_fonte)
    return salvar_df(df, caminho_saida)


def gerar_fronteira_item(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    cnpj_limpo = "".join(filter(str.isdigit, cnpj))
    caminho_simples = caminho_oracle(cnpj_limpo, f"11_fronteira_item_simples_{cnpj_limpo}.parquet", pasta_cnpj)
    caminho_completo = caminho_oracle(cnpj_limpo, f"12_fronteira_item_completo_{cnpj_limpo}.parquet", pasta_cnpj)

    legado_simples = caminho_bruto(cnpj_limpo, f"fronteira_{cnpj_limpo}.parquet", pasta_cnpj)
    legado_completo = caminho_bruto(cnpj_limpo, f"fronteira_completo_{cnpj_limpo}.parquet", pasta_cnpj)

    df_simples = _normalizar_fronteira_simples(
        ler_parquet_opcional(caminho_simples if caminho_simples.exists() else legado_simples, SCHEMA_FRONTEIRA_SIMPLES)
    )
    df_completo = _normalizar_fronteira_completo(
        ler_parquet_opcional(caminho_completo if caminho_completo.exists() else legado_completo, SCHEMA_FRONTEIRA_COMPLETO)
    )

    ok_simples = salvar_df(df_simples, caminho_simples)
    ok_completo = salvar_df(df_completo, caminho_completo)
    return ok_simples and ok_completo


def gerar_insumos_oracle_ressarcimento(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    ok_vigencia = gerar_vigencia_sefin(cnpj, pasta_cnpj)
    ok_rateio = gerar_rateio_frete_cte(cnpj, pasta_cnpj)
    ok_st = gerar_st_material_ate_2022(cnpj, pasta_cnpj)
    ok_fronteira = gerar_fronteira_item(cnpj, pasta_cnpj)
    return ok_vigencia and ok_rateio and ok_st and ok_fronteira
