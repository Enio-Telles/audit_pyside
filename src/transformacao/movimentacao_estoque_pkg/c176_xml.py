"""
c176_xml.py

Objetivo:
- Reinterpretar o C176 usando o produto da saida como ancora principal.
- Obter o `id_agrupado` pela saida (`chave_saida` + `num_item_saida` + `cod_item`)
  em `c170_agr`.
- Buscar a entrada XML correspondente em `nfe_agr` usando
  (`chave_nfe_ultima_entrada` + item declarado no C176 + id_agrupado da saida).
- Converter quantidades e valores unitarios para a `unid_ref` do produto com
  base em `fatores_conversao`.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from utilitarios.project_paths import PROJECT_ROOT

import polars as pl
from rich import print as rprint

ROOT_DIR = PROJECT_ROOT
SRC_DIR = ROOT_DIR / "src"
DADOS_DIR = ROOT_DIR / "dados"
CNPJ_ROOT = DADOS_DIR / "CNPJ"


try:
    from utilitarios.salvar_para_parquet import salvar_para_parquet
    from utilitarios.validacao_schema import (
        SchemaValidacaoError,
        validar_parquet_essencial,
    )
except ImportError as e:
    rprint(f"[red]Erro ao importar modulos utilitarios:[/red] {e}")
    sys.exit(1)


def _norm_unid_expr(col: str) -> pl.Expr:
    return (
        pl.col(col)
        .cast(pl.Utf8, strict=False)
        .fill_null("")
        .str.strip_chars()
        .str.to_uppercase()
    )


def _to_float_expr(col: str) -> pl.Expr:
    return pl.col(col).cast(pl.Float64, strict=False)


def _norm_text_expr(col: str) -> pl.Expr:
    return (
        pl.col(col)
        .cast(pl.Utf8, strict=False)
        .fill_null("")
        .str.strip_chars()
        .str.to_uppercase()
    )


def gerar_c176_xml(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    cnpj = re.sub(r"\D", "", cnpj or "")
    if len(cnpj) not in {11, 14}:
        raise ValueError("CPF/CNPJ invalido.")

    if pasta_cnpj is None:
        pasta_cnpj = CNPJ_ROOT / cnpj

    pasta_brutos = pasta_cnpj / "arquivos_parquet"
    pasta_analises = pasta_cnpj / "analises" / "produtos"

    arq_c176 = pasta_brutos / f"c176_{cnpj}.parquet"
    arq_c170_agr = pasta_brutos / f"c170_agr_{cnpj}.parquet"
    arq_nfe_agr = pasta_brutos / f"nfe_agr_{cnpj}.parquet"
    arq_fatores = pasta_analises / f"fatores_conversao_{cnpj}.parquet"

    for arq in (arq_c176, arq_c170_agr, arq_nfe_agr, arq_fatores):
        if not arq.exists():
            rprint(f"[red]Arquivo necessario nao encontrado:[/red] {arq}")
            return False

    rprint(f"[bold cyan]Gerando c176_xml para CNPJ: {cnpj}[/bold cyan]")

    try:
        validar_parquet_essencial(
            arq_c176,
            ["chave_saida", "num_item_saida", "cod_item", "chave_nfe_ultima_entrada"],
            contexto="c176_xml/c176",
        )
        validar_parquet_essencial(
            arq_c170_agr,
            ["chv_nfe", "num_item", "cod_item", "id_agrupado", "descr_padrao", "cod_ncm", "cest", "unid", "qtd"],
            contexto="c176_xml/c170_agr",
        )
        validar_parquet_essencial(
            arq_nfe_agr,
            ["chave_acesso", "prod_nitem", "id_agrupado"],
            contexto="c176_xml/nfe_agr",
        )
        validar_parquet_essencial(
            arq_fatores,
            ["id_agrupado", "unid", "unid_ref", "fator"],
            contexto="c176_xml/fatores_conversao",
        )
    except SchemaValidacaoError as exc:
        rprint(f"[red]{exc}[/red]")
        return False

    df_c176 = pl.read_parquet(arq_c176)
    df_saida = pl.read_parquet(arq_c170_agr)
    df_nfe = pl.read_parquet(arq_nfe_agr)
    df_fatores = pl.read_parquet(arq_fatores)

    if df_c176.is_empty():
        rprint("[yellow]Arquivo c176 esta vazio.[/yellow]")
        return salvar_para_parquet(pl.DataFrame(), pasta_analises, f"c176_xml_{cnpj}.parquet")

    df_saida_ref = (
        df_saida
        .select(
            [
                "chv_nfe",
                "num_item",
                "cod_item",
                "id_agrupado",
                "descr_padrao",
                "cod_ncm",
                "cest",
                "unid",
                "qtd",
            ]
        )
        .with_columns(
            [
                pl.col("num_item").cast(pl.Int64, strict=False).alias("num_item"),
                pl.col("cod_item").cast(pl.Utf8, strict=False).str.strip_chars().alias("cod_item"),
                pl.col("cod_ncm").cast(pl.Utf8, strict=False).str.strip_chars().alias("cod_ncm_saida_agr"),
                pl.col("cest").cast(pl.Utf8, strict=False).str.strip_chars().alias("cest_saida_agr"),
                _norm_unid_expr("unid").alias("unid_saida_agr"),
                _to_float_expr("qtd").alias("qtd_saida_agr"),
            ]
        )
        .unique(subset=["chv_nfe", "num_item", "cod_item"])
    )

    df_c176_base = (
        df_c176
        .with_columns(
            [
                pl.col("num_item_saida").cast(pl.Int64, strict=False).alias("num_item_saida"),
                pl.col("cod_item").cast(pl.Utf8, strict=False).str.strip_chars().alias("cod_item"),
                pl.col("c176_num_item_ult_e_declarado").cast(pl.Int64, strict=False).alias("__item_entrada_xml"),
                _norm_unid_expr("unid_saida").alias("unid_saida_norm"),
                _to_float_expr("qtd_item_saida").alias("qtd_item_saida"),
            ]
        )
        .join(
            df_saida_ref,
            left_on=["chave_saida", "num_item_saida", "cod_item"],
            right_on=["chv_nfe", "num_item", "cod_item"],
            how="left",
        )
        .with_columns(
            [
                pl.col("id_agrupado").is_not_null().alias("match_saida_id_agrupado"),
                pl.col("cod_item").alias("cod_item_ref_saida"),
            ]
        )
    )

    df_fatores_ref = (
        df_fatores
        .select(["id_agrupado", "unid", "unid_ref", "fator"])
        .with_columns(
            [
                _norm_unid_expr("unid").alias("__unid_norm"),
                _norm_unid_expr("unid_ref").alias("unid_ref"),
                _to_float_expr("fator").fill_null(1.0).alias("fator"),
            ]
        )
        .unique(subset=["id_agrupado", "__unid_norm"])
    )

    df_saida_conv = (
        df_c176_base
        .join(
            df_fatores_ref.select(
                [
                    "id_agrupado",
                    "__unid_norm",
                    pl.col("unid_ref").alias("unid_ref_saida"),
                    pl.col("fator").alias("fator_saida"),
                ]
            ),
            left_on=["id_agrupado", "unid_saida_norm"],
            right_on=["id_agrupado", "__unid_norm"],
            how="left",
        )
        .with_columns(
            [
                pl.coalesce([pl.col("unid_ref_saida"), pl.col("unid_saida_norm")]).alias("unid_ref"),
                pl.col("fator_saida").fill_null(1.0).alias("fator_saida"),
            ]
        )
        .with_columns(
            [
                (pl.col("qtd_item_saida") * pl.col("fator_saida")).alias("qtd_saida_unid_ref"),
                pl.when(pl.col("fator_saida") > 0)
                .then(_to_float_expr("vl_unit_bc_st_entrada") / pl.col("fator_saida"))
                .otherwise(_to_float_expr("vl_unit_bc_st_entrada"))
                .alias("vl_unit_bc_st_entrada_unid_ref"),
                pl.when(pl.col("fator_saida") > 0)
                .then(_to_float_expr("vl_unit_icms_proprio_entrada") / pl.col("fator_saida"))
                .otherwise(_to_float_expr("vl_unit_icms_proprio_entrada"))
                .alias("vl_unit_icms_proprio_entrada_unid_ref"),
                pl.when(pl.col("fator_saida") > 0)
                .then(_to_float_expr("vl_unit_ressarcimento_st") / pl.col("fator_saida"))
                .otherwise(_to_float_expr("vl_unit_ressarcimento_st"))
                .alias("vl_unit_ressarcimento_st_unid_ref"),
            ]
        )
    )

    df_entrada_xml = (
        df_nfe
        .select(
            [
                "chave_acesso",
                "prod_nitem",
                "prod_cprod",
                "prod_xprod",
                "prod_ncm",
                "prod_cest",
                "id_agrupado",
                "prod_ucom",
                "prod_qcom",
                "prod_vprod",
                "prod_vfrete",
                "prod_vseg",
                "prod_voutro",
                "prod_vdesc",
                "prod_vuncom",
                "prod_utrib",
                "prod_qtrib",
                "prod_vuntrib",
            ]
        )
        .with_columns(
            [
                pl.col("prod_nitem").cast(pl.Int64, strict=False).alias("prod_nitem"),
                pl.col("prod_cprod").cast(pl.Utf8, strict=False).str.strip_chars().alias("prod_cprod"),
                pl.col("prod_ncm").cast(pl.Utf8, strict=False).str.strip_chars().alias("prod_ncm"),
                pl.col("prod_cest").cast(pl.Utf8, strict=False).str.strip_chars().alias("prod_cest"),
                _norm_unid_expr("prod_ucom").alias("unid_entrada_xml"),
                _norm_text_expr("prod_xprod").alias("prod_xprod_norm"),
                _to_float_expr("prod_qcom").alias("qtd_entrada_xml"),
                (
                    _to_float_expr("prod_vprod")
                    + _to_float_expr("prod_vfrete")
                    + _to_float_expr("prod_vseg")
                    + _to_float_expr("prod_voutro")
                    - _to_float_expr("prod_vdesc")
                ).alias("vl_total_entrada_xml"),
                _to_float_expr("prod_vuncom").alias("vl_unitario_entrada_xml"),
                _norm_unid_expr("prod_utrib").alias("unid_tributavel_xml"),
                _to_float_expr("prod_qtrib").alias("qtd_tributavel_xml"),
                _to_float_expr("prod_vuntrib").alias("vl_unitario_tributavel_xml"),
            ]
        )
        .rename({"id_agrupado": "id_agrupado_entrada_xml"})
        .unique(subset=["chave_acesso", "prod_nitem", "id_agrupado_entrada_xml"])
    )

    chaves_particao = ["chave_saida", "num_item_saida", "cod_item_ref_saida", "chave_nfe_ultima_entrada"]

    df_result = (
        df_saida_conv
        .with_columns(
            [
                _norm_text_expr("descricao_item").alias("descricao_item_norm"),
                _norm_text_expr("descr_padrao").alias("descr_padrao_norm"),
                pl.col("cod_item_ref_saida").cast(pl.Utf8, strict=False).str.strip_chars().alias("cod_item_ref_saida"),
            ]
        )
        .join(
            df_entrada_xml,
            left_on=["chave_nfe_ultima_entrada"],
            right_on=["chave_acesso"],
            how="left",
        )
        .with_columns(
            [
                pl.col("prod_nitem").is_not_null().alias("tem_item_xml"),
                (pl.col("prod_nitem") == pl.col("__item_entrada_xml")).fill_null(False).alias("ind_match_item_declarado"),
                (pl.col("id_agrupado_entrada_xml") == pl.col("id_agrupado")).fill_null(False).alias("ind_match_id_agrupado"),
                (pl.col("prod_cprod") == pl.col("cod_item_ref_saida")).fill_null(False).alias("ind_match_cod_item"),
                (
                    pl.col("prod_ncm").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()
                    == pl.col("cod_ncm_saida_agr").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()
                ).fill_null(False).alias("ind_match_ncm"),
                (
                    pl.col("prod_cest").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()
                    == pl.col("cest_saida_agr").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()
                ).fill_null(False).alias("ind_match_cest"),
                (pl.col("prod_xprod_norm") == pl.col("descricao_item_norm")).fill_null(False).alias("ind_match_desc_saida"),
                (pl.col("prod_xprod_norm") == pl.col("descr_padrao_norm")).fill_null(False).alias("ind_match_desc_padrao"),
                (pl.col("qtd_entrada_xml") - pl.col("qtd_item_saida")).abs().alias("diff_qtd_vinculo"),
            ]
        )
        .with_columns(
            [
                pl.col("ind_match_id_agrupado").max().over(chaves_particao).alias("existe_match_id_agrupado"),
                pl.col("ind_match_cod_item").max().over(chaves_particao).alias("existe_match_cod_item"),
                (
                    pl.when(pl.col("ind_match_item_declarado")).then(pl.lit(1000)).otherwise(pl.lit(0))
                    + pl.when(pl.col("ind_match_ncm")).then(pl.lit(300)).otherwise(pl.lit(0))
                    + pl.when(pl.col("ind_match_cest")).then(pl.lit(250)).otherwise(pl.lit(0))
                    + pl.when(pl.col("ind_match_desc_saida")).then(pl.lit(150)).otherwise(pl.lit(0))
                    + pl.when(pl.col("ind_match_desc_padrao")).then(pl.lit(100)).otherwise(pl.lit(0))
                    + pl.when(pl.col("qtd_entrada_xml") == pl.col("qtd_item_saida")).then(pl.lit(80))
                    .when((pl.col("qtd_entrada_xml") - pl.col("qtd_item_saida")).abs() <= 1).then(pl.lit(30))
                    .otherwise(pl.lit(0))
                ).alias("score_vinculo_entrada"),
            ]
        )
        .with_columns(
            [
                pl.when(pl.col("existe_match_id_agrupado"))
                .then(pl.col("ind_match_id_agrupado").cast(pl.Int8))
                .when(pl.col("existe_match_cod_item"))
                .then(pl.col("ind_match_cod_item").cast(pl.Int8))
                .otherwise(pl.lit(1))
                .alias("__ordem_match")
            ]
        )
        .sort(
            chaves_particao + ["__ordem_match", "score_vinculo_entrada", "diff_qtd_vinculo", "prod_nitem"],
            descending=[False, False, False, False, True, True, False, False],
            nulls_last=True,
        )
        .with_columns(
            pl.int_range(0, pl.len()).over(chaves_particao).alias("__rank_vinculo")
        )
        .filter(pl.col("__rank_vinculo") == 0)
        .join(
            df_fatores_ref.select(
                [
                    "id_agrupado",
                    "__unid_norm",
                    pl.col("unid_ref").alias("unid_ref_entrada"),
                    pl.col("fator").alias("fator_entrada_xml"),
                ]
            ),
            left_on=["id_agrupado", "unid_entrada_xml"],
            right_on=["id_agrupado", "__unid_norm"],
            how="left",
        )
        .with_columns(
            [
                pl.col("prod_nitem").is_not_null().alias("match_entrada_xml"),
                pl.col("fator_entrada_xml").fill_null(1.0).alias("fator_entrada_xml"),
                pl.coalesce([pl.col("unid_ref"), pl.col("unid_ref_entrada"), pl.col("unid_entrada_xml")]).alias("unid_ref"),
            ]
        )
        .with_columns(
            [
                pl.when(pl.col("match_entrada_xml") & pl.col("ind_match_id_agrupado"))
                .then(pl.lit("VINCULO POR ID_AGRUPADO"))
                .when(pl.col("match_entrada_xml") & pl.col("ind_match_cod_item"))
                .then(pl.lit("VINCULO POR COD_ITEM_XML"))
                .when(pl.col("match_entrada_xml"))
                .then(pl.lit("VINCULO POR SCORE"))
                .otherwise(pl.lit("SEM VINCULO XML"))
                .alias("regra_vinculo_entrada"),
            ]
        )
        .with_columns(
            [
                (pl.col("qtd_entrada_xml") * pl.col("fator_entrada_xml")).alias("qtd_entrada_xml_unid_ref"),
                pl.when((pl.col("qtd_entrada_xml") * pl.col("fator_entrada_xml")) > 0)
                .then(pl.col("vl_total_entrada_xml") / (pl.col("qtd_entrada_xml") * pl.col("fator_entrada_xml")))
                .otherwise(None)
                .alias("vl_unitario_entrada_xml_unid_ref"),
            ]
        )
        .select(
            [
                pl.lit(cnpj).alias("cnpj"),
                "periodo_efd",
                "data_entrega_efd_periodo",
                "cod_fin_efd",
                "finalidade_efd",
                "chave_saida",
                "num_nf_saida",
                "dt_doc_saida",
                "dt_e_s_saida",
                "cod_item_ref_saida",
                "descricao_item",
                "num_item_saida",
                "cfop_saida",
                "id_agrupado",
                "descr_padrao",
                "unid_saida",
                "fator_saida",
                "unid_ref",
                "qtd_item_saida",
                "qtd_saida_unid_ref",
                "cod_mot_res",
                "descricao_motivo_ressarcimento",
                "chave_nfe_ultima_entrada",
                "c176_num_item_ult_e_declarado",
                "dt_ultima_entrada",
                "prod_nitem",
                "unid_entrada_xml",
                "fator_entrada_xml",
                "qtd_entrada_xml",
                "qtd_entrada_xml_unid_ref",
                "vl_total_entrada_xml",
                "vl_unitario_entrada_xml",
                "vl_unitario_entrada_xml_unid_ref",
                "vl_unit_bc_st_entrada",
                "vl_unit_bc_st_entrada_unid_ref",
                "vl_unit_icms_proprio_entrada",
                "vl_unit_icms_proprio_entrada_unid_ref",
                "vl_unit_ressarcimento_st",
                "vl_unit_ressarcimento_st_unid_ref",
                "vl_ressarc_credito_proprio",
                "vl_ressarc_st_retido",
                "vr_total_ressarcimento",
                "score_vinculo_entrada",
                "diff_qtd_vinculo",
                "regra_vinculo_entrada",
                "match_saida_id_agrupado",
                "match_entrada_xml",
            ]
        )
        .sort(["periodo_efd", "chave_saida", "num_item_saida", "chave_nfe_ultima_entrada"], nulls_last=True)
    )

    return salvar_para_parquet(df_result, pasta_analises, f"c176_xml_{cnpj}.parquet")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        gerar_c176_xml(sys.argv[1])
    else:
        gerar_c176_xml(input("CNPJ: "))


