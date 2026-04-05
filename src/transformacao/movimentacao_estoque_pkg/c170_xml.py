"""
c170_xml.py

Objetivo:
- Gerar uma tabela padronizada no formato de `Tabela_estoques.xlsx`
  usando o C170 como base.
- Quando existir XML correspondente (NFe/NFCe), usar seus valores.
- Quando o XML nao tiver o valor, preencher com o que existir apenas no C170.
- Identificar o item XML mais aderente por score, priorizando id_agrupado e,
  em seguida, descricao, NCM, CEST, GTIN, quantidade e preco.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import polars as pl
from rich import print as rprint

ROOT_DIR = Path(r"c:\funcoes - Copia")
SRC_DIR = ROOT_DIR / "src"
DADOS_DIR = ROOT_DIR / "dados"
CNPJ_ROOT = DADOS_DIR / "CNPJ"


try:
    from utilitarios.salvar_para_parquet import salvar_para_parquet
    from utilitarios.validacao_schema import (
        SchemaValidacaoError,
        validar_parquet_essencial,
    )
    from transformacao.co_sefin_class import enriquecer_co_sefin_class
    from utilitarios.text import remove_accents
except ImportError as e:
    rprint(f"[red]Erro ao importar modulos utilitarios:[/red] {e}")
    sys.exit(1)


def _norm_text(value: str | None) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", (remove_accents(value) or "").upper().strip())


def _norm_text_expr(col: str, alias: str | None = None) -> pl.Expr:
    # Optimization: Replace .map_elements with native Polars string operations to preserve vectorization
    expr = (
        pl.col(col)
        .cast(pl.Utf8, strict=False)
        .fill_null("")
        .str.to_uppercase()
        .str.replace_all(r"[ÁÀÂÃÄ]", "A")
        .str.replace_all(r"[ÉÈÊË]", "E")
        .str.replace_all(r"[ÍÌÎÏ]", "I")
        .str.replace_all(r"[ÓÒÔÕÖ]", "O")
        .str.replace_all(r"[ÚÙÛÜ]", "U")
        .str.replace_all(r"Ç", "C")
        .str.replace_all(r"Ñ", "N")
        .str.strip_chars()
        .str.replace_all(r"\s+", " ")
    )
    return expr.alias(alias or col)


def _clean_digits_expr(col: str, alias: str | None = None) -> pl.Expr:
    expr = (
        pl.col(col)
        .cast(pl.Utf8, strict=False)
        .fill_null("")
        .str.replace_all(r"\D", "")
        .str.strip_chars()
    )
    return expr.alias(alias or col)


def _to_float_expr(col: str, alias: str | None = None) -> pl.Expr:
    expr = pl.col(col).cast(pl.Float64, strict=False)
    return expr.alias(alias or col)


def _to_int_expr(col: str, alias: str | None = None) -> pl.Expr:
    expr = pl.col(col).cast(pl.Int64, strict=False)
    return expr.alias(alias or col)


def _desc_similarity(payload: dict) -> float:
    a = _norm_text(payload.get("a"))
    b = _norm_text(payload.get("b"))
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    ta = {t for t in a.split(" ") if t}
    tb = {t for t in b.split(" ") if t}
    if not ta or not tb:
        return 0.0
    inter = len(ta & tb)
    denom = max(len(ta), len(tb), 1)
    return inter / denom


def _build_cst_xml(df: pl.DataFrame) -> pl.Expr:
    has_cst = "icms_cst" in df.columns
    has_csosn = "icms_csosn" in df.columns
    if has_cst and has_csosn:
        return (
            pl.when(pl.col("icms_cst").cast(pl.Utf8, strict=False).is_not_null())
            .then(
                pl.concat_str(
                    [
                        pl.col("icms_orig").cast(pl.Utf8, strict=False).fill_null(""),
                        pl.col("icms_cst").cast(pl.Utf8, strict=False).fill_null(""),
                    ],
                    separator="",
                )
            )
            .otherwise(
                pl.concat_str(
                    [
                        pl.col("icms_orig").cast(pl.Utf8, strict=False).fill_null(""),
                        pl.col("icms_csosn").cast(pl.Utf8, strict=False).fill_null(""),
                    ],
                    separator="",
                )
            )
            .alias("cst_xml")
        )
    if has_cst:
        return pl.concat_str(
            [
                pl.col("icms_orig").cast(pl.Utf8, strict=False).fill_null(""),
                pl.col("icms_cst").cast(pl.Utf8, strict=False).fill_null(""),
            ],
            separator="",
        ).alias("cst_xml")
    if has_csosn:
        return pl.concat_str(
            [
                pl.col("icms_orig").cast(pl.Utf8, strict=False).fill_null(""),
                pl.col("icms_csosn").cast(pl.Utf8, strict=False).fill_null(""),
            ],
            separator="",
        ).alias("cst_xml")
    return pl.lit(None, pl.Utf8).alias("cst_xml")


def _prepare_xml_candidates(df: pl.DataFrame, fonte: str) -> pl.DataFrame:
    optional = {c: c in df.columns for c in df.columns}

    def col_or_null(name: str, dtype=pl.Utf8):
        if name in df.columns:
            return pl.col(name).cast(dtype, strict=False)
        return pl.lit(None, dtype=dtype)

    exprs = [
        pl.lit(fonte).alias("fonte_xml"),
        col_or_null("nsu", pl.Int64).alias("nsu_xml"),
        col_or_null("chave_acesso").alias("Chv_nfe"),
        _to_int_expr("prod_nitem", "Num_item_xml") if "prod_nitem" in df.columns else pl.lit(None, pl.Int64).alias("Num_item_xml"),
        col_or_null("prod_cprod").alias("Cod_item_xml"),
        pl.coalesce(
            [
                col_or_null("prod_ceantrib"),
                col_or_null("prod_cean"),
            ]
        ).alias("Cod_barra_xml"),
        _clean_digits_expr("prod_ncm", "Ncm_xml") if "prod_ncm" in df.columns else pl.lit("", pl.Utf8).alias("Ncm_xml"),
        _clean_digits_expr("prod_cest", "Cest_xml") if "prod_cest" in df.columns else pl.lit("", pl.Utf8).alias("Cest_xml"),
        col_or_null("prod_xprod").alias("Descr_item_xml"),
        col_or_null("co_cfop").alias("Cfop_xml"),
        col_or_null("prod_ucom").alias("Unid_xml"),
        _to_float_expr("prod_qcom", "Qtd_xml") if "prod_qcom" in df.columns else pl.lit(None, pl.Float64).alias("Qtd_xml"),
        (
            col_or_null("prod_vprod", pl.Float64).fill_null(0)
            + col_or_null("prod_vfrete", pl.Float64).fill_null(0)
            + col_or_null("prod_vseg", pl.Float64).fill_null(0)
            + col_or_null("prod_voutro", pl.Float64).fill_null(0)
            - col_or_null("prod_vdesc", pl.Float64).fill_null(0)
        ).alias("Vl_item_xml"),
        col_or_null("icms_picms", pl.Float64).alias("Aliq_icms_xml"),
        col_or_null("icms_vbc", pl.Float64).alias("Vl_bc_icms_xml"),
        col_or_null("icms_vicms", pl.Float64).alias("Vl_icms_xml"),
        # Mantem o mesmo layout da referencia existente.
        col_or_null("icms_vbcst", pl.Float64).alias("vl_icms_st_xml"),
        col_or_null("icms_vicmsst", pl.Float64).alias("vl_bc_icms_st_xml"),
        col_or_null("icms_picmsst", pl.Float64).alias("aliq_st_xml"),
        col_or_null("tipo_operacao").alias("Tipo_operacao_xml"),
        col_or_null("ide_co_mod").alias("mod_xml"),
        col_or_null("ide_serie").alias("Ser_xml"),
        col_or_null("nnf").alias("num_nfe_xml"),
        col_or_null("dhemi").alias("Dt_doc_xml"),
        pl.coalesce([col_or_null("dhsaient"), col_or_null("dhemi")]).alias("Dt_e_s_xml"),
        col_or_null("co_uf_emit").alias("co_uf_emit_xml"),
        col_or_null("co_uf_dest").alias("co_uf_dest_xml"),
        col_or_null("co_finnfe").alias("finnfe_xml"),
        _build_cst_xml(df),
        col_or_null("id_agrupado").alias("id_agrupado_xml"),
        col_or_null("co_sefin_agr").alias("co_sefin_agr_xml"),
    ]

    return df.select(exprs)


def gerar_c170_xml(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    cnpj = re.sub(r"\D", "", cnpj or "")
    if len(cnpj) not in {11, 14}:
        raise ValueError("CPF/CNPJ invalido.")

    if pasta_cnpj is None:
        pasta_cnpj = CNPJ_ROOT / cnpj

    pasta_brutos = pasta_cnpj / "arquivos_parquet"

    arq_c170 = pasta_brutos / f"c170_{cnpj}.parquet"
    arq_c170_agr = pasta_brutos / f"c170_agr_{cnpj}.parquet"
    arq_nfe_agr = pasta_brutos / f"nfe_agr_{cnpj}.parquet"
    arq_nfce_agr = pasta_brutos / f"nfce_agr_{cnpj}.parquet"

    for arq in (arq_c170, arq_c170_agr, arq_nfe_agr, arq_nfce_agr):
        if not arq.exists():
            rprint(f"[red]Arquivo necessario nao encontrado:[/red] {arq}")
            return False

    rprint(f"[bold cyan]Gerando c170_xml para CNPJ: {cnpj}[/bold cyan]")

    try:
        validar_parquet_essencial(
            arq_c170,
            ["chv_nfe", "num_item", "cod_item", "cod_ncm", "descr_item"],
            contexto="c170_xml/c170",
        )
        validar_parquet_essencial(
            arq_c170_agr,
            ["chv_nfe", "num_item", "cod_item", "id_agrupado", "co_sefin_agr"],
            contexto="c170_xml/c170_agr",
        )
        validar_parquet_essencial(
            arq_nfe_agr,
            ["chave_acesso", "id_agrupado"],
            contexto="c170_xml/nfe_agr",
        )
        validar_parquet_essencial(
            arq_nfce_agr,
            ["chave_acesso", "id_agrupado"],
            contexto="c170_xml/nfce_agr",
        )
    except SchemaValidacaoError as exc:
        rprint(f"[red]{exc}[/red]")
        return False

    df_c170 = pl.read_parquet(arq_c170)
    df_c170_agr = pl.read_parquet(arq_c170_agr)
    df_nfe_agr = pl.read_parquet(arq_nfe_agr)
    df_nfce_agr = pl.read_parquet(arq_nfce_agr)

    if df_c170.is_empty():
        return salvar_para_parquet(pl.DataFrame(), pasta_brutos, f"c170_xml_{cnpj}.parquet")

    df_c170_base = (
        df_c170
        .with_row_index("__rowid__")
        .with_columns(
            [
                _to_int_expr("num_item", "num_item"),
                pl.col("cod_item").cast(pl.Utf8, strict=False).str.strip_chars().alias("cod_item"),
            ]
        )
        .join(
            df_c170_agr
            .with_columns(
                [
                    _to_int_expr("num_item", "num_item"),
                    pl.col("cod_item").cast(pl.Utf8, strict=False).str.strip_chars().alias("cod_item"),
                ]
            )
            .select(
                [
                    "chv_nfe",
                    "num_item",
                    "cod_item",
                    "id_agrupado",
                    "co_sefin_agr",
                ]
            ),
            on=["chv_nfe", "num_item", "cod_item"],
            how="left",
        )
        .with_columns(
            [
                pl.col("ind_oper").cast(pl.Utf8, strict=False).alias("Tipo_operacao_c170"),
                pl.lit(None, pl.Int64).alias("nsu_c170"),
                pl.col("chv_nfe").cast(pl.Utf8, strict=False).alias("Chv_nfe"),
                pl.col("chv_nfe").cast(pl.Utf8, strict=False).str.slice(20, 2).alias("mod_c170"),
                pl.col("ser").cast(pl.Utf8, strict=False).alias("Ser_c170"),
                pl.col("num_doc").cast(pl.Utf8, strict=False).alias("num_nfe_c170"),
                pl.col("dt_doc").alias("Dt_doc_c170"),
                pl.col("dt_e_s").alias("Dt_e_s_c170"),
                pl.col("num_item").alias("Num_item_c170"),
                pl.col("cod_item").alias("Cod_item_c170"),
                _clean_digits_expr("cod_barra", "Cod_barra_c170"),
                _clean_digits_expr("cod_ncm", "Ncm_c170"),
                _clean_digits_expr("cest", "Cest_c170"),
                pl.col("tipo_item").cast(pl.Utf8, strict=False).alias("Tipo_item_c170"),
                pl.col("descr_item").cast(pl.Utf8, strict=False).alias("Descr_item_c170"),
                pl.col("descr_compl").cast(pl.Utf8, strict=False).alias("Descr_compl_c170"),
                pl.col("cfop").cast(pl.Utf8, strict=False).alias("Cfop_c170"),
                pl.col("cst_icms").cast(pl.Utf8, strict=False).alias("Cst_c170"),
                _to_float_expr("qtd", "Qtd_c170"),
                pl.col("unid").cast(pl.Utf8, strict=False).alias("Unid_c170"),
                _to_float_expr("vl_item", "Vl_item_c170"),
                _to_float_expr("vl_desc", "vl_desc_c170"),
                _to_float_expr("aliq_icms", "Aliq_icms_c170"),
                _to_float_expr("vl_bc_icms", "Vl_bc_icms_c170"),
                _to_float_expr("vl_icms", "Vl_icms_c170"),
                _to_float_expr("vl_icms_st", "vl_icms_st_c170"),
                _to_float_expr("vl_bc_icms_st", "vl_bc_icms_st_c170"),
                _to_float_expr("aliq_st", "aliq_st_c170"),
                _norm_text_expr("descr_item", "Descr_item_c170_norm"),
                _norm_text_expr("descr_compl", "Descr_compl_c170_norm"),
            ]
        )
        .drop(["num_item", "cod_item"])
    )

    df_xml_candidatos = pl.concat(
        [
            _prepare_xml_candidates(df_nfe_agr, "nfe"),
            _prepare_xml_candidates(df_nfce_agr, "nfce"),
        ],
        how="vertical_relaxed",
    ).with_columns(
        [
            _norm_text_expr("Descr_item_xml", "Descr_item_xml_norm"),
            _clean_digits_expr("Cod_barra_xml", "Cod_barra_xml"),
        ]
    )

    chaves_particao = ["__rowid__"]

    df_match = (
        df_c170_base
        .join(df_xml_candidatos, on="Chv_nfe", how="left")
        .with_columns(
            [
                (pl.col("id_agrupado") == pl.col("id_agrupado_xml")).fill_null(False).alias("ind_match_id_agrupado"),
                (
                    (pl.col("Ncm_c170") != "")
                    & (pl.col("Ncm_xml") != "")
                    & (pl.col("Ncm_c170") == pl.col("Ncm_xml"))
                ).fill_null(False).alias("ind_match_ncm"),
                (
                    (pl.col("Cest_c170") != "")
                    & (pl.col("Cest_xml") != "")
                    & (pl.col("Cest_c170") == pl.col("Cest_xml"))
                ).fill_null(False).alias("ind_match_cest"),
                (
                    (pl.col("Cod_barra_c170") != "")
                    & (pl.col("Cod_barra_c170") != "SEMGTIN")
                    & (pl.col("Cod_barra_xml") != "")
                    & (pl.col("Cod_barra_c170") == pl.col("Cod_barra_xml"))
                ).fill_null(False).alias("ind_match_gtin"),
                (
                    pl.struct(
                        [
                            pl.col("Descr_item_c170_norm").alias("a"),
                            pl.col("Descr_item_xml_norm").alias("b"),
                        ]
                    )
                    .map_elements(_desc_similarity, return_dtype=pl.Float64)
                    .fill_null(0.0)
                ).alias("desc_similarity"),
                (pl.col("Qtd_c170") - pl.col("Qtd_xml")).abs().alias("diff_qtd"),
                (pl.col("Vl_item_c170") - pl.col("Vl_item_xml")).abs().alias("diff_vl_total"),
                pl.when((pl.col("Qtd_c170") > 0) & (pl.col("Qtd_xml") > 0))
                .then(((pl.col("Vl_item_c170") / pl.col("Qtd_c170")) - (pl.col("Vl_item_xml") / pl.col("Qtd_xml"))).abs())
                .otherwise(None)
                .alias("diff_vl_unit"),
                (pl.col("Num_item_c170") == pl.col("Num_item_xml")).fill_null(False).alias("ind_match_num_item"),
            ]
        )
        .with_columns(
            [
                pl.col("ind_match_id_agrupado").max().over(chaves_particao).alias("existe_match_id_agrupado"),
                (
                    pl.when(pl.col("ind_match_ncm")).then(pl.lit(300)).otherwise(pl.lit(0))
                    + pl.when(pl.col("ind_match_cest")).then(pl.lit(250)).otherwise(pl.lit(0))
                    + pl.when(pl.col("ind_match_gtin")).then(pl.lit(220)).otherwise(pl.lit(0))
                    + pl.when(pl.col("desc_similarity") >= 0.98).then(pl.lit(200))
                    .when(pl.col("desc_similarity") >= 0.75).then(pl.lit(140))
                    .when(pl.col("desc_similarity") >= 0.50).then(pl.lit(80))
                    .otherwise(pl.lit(0))
                    + pl.when(pl.col("diff_qtd") <= 0.000001).then(pl.lit(150))
                    .when(pl.col("diff_qtd") <= 1).then(pl.lit(50))
                    .otherwise(pl.lit(0))
                    + pl.when(pl.col("diff_vl_total") <= 0.01).then(pl.lit(150))
                    .when(pl.col("diff_vl_unit") <= 0.01).then(pl.lit(100))
                    .otherwise(pl.lit(0))
                    + pl.when(pl.col("ind_match_num_item")).then(pl.lit(80)).otherwise(pl.lit(0))
                ).alias("score_vinculo_xml")
            ]
        )
        .with_columns(
            [
                pl.when(pl.col("existe_match_id_agrupado"))
                .then(pl.col("ind_match_id_agrupado").cast(pl.Int8))
                .otherwise(pl.lit(1))
                .alias("__ordem_match")
            ]
        )
        .sort(
            chaves_particao + ["__ordem_match", "score_vinculo_xml", "desc_similarity", "diff_qtd", "diff_vl_total", "Num_item_xml"],
            descending=[False, True, True, True, False, False, False],
            nulls_last=True,
        )
        .with_columns(pl.int_range(0, pl.len()).over(chaves_particao).alias("__rank_match"))
        .filter(pl.col("__rank_match") == 0)
        .with_columns(
            [
                pl.col("Num_item_xml").is_not_null().alias("match_xml"),
            ]
        )
        .with_columns(
            [
                pl.when(pl.col("match_xml") & pl.col("ind_match_id_agrupado"))
                .then(pl.lit("VINCULO POR ID_AGRUPADO"))
                .when(pl.col("match_xml"))
                .then(pl.lit("VINCULO POR SCORE"))
                .otherwise(pl.lit("SEM VINCULO XML"))
                .alias("regra_vinculo_xml"),
            ]
        )
    )

    df_final = (
        df_match
        .select(
            [
                pl.coalesce([pl.col("Tipo_operacao_xml"), pl.col("Tipo_operacao_c170")]).alias("Tipo_operacao"),
                pl.col("nsu_xml").alias("nsu"),
                pl.col("Chv_nfe").alias("Chv_nfe"),
                pl.coalesce([pl.col("mod_xml"), pl.col("mod_c170")]).alias("mod"),
                pl.coalesce([pl.col("Ser_xml"), pl.col("Ser_c170")]).alias("Ser"),
                pl.coalesce([pl.col("num_nfe_xml"), pl.col("num_nfe_c170")]).alias("num_nfe"),
                pl.coalesce([pl.col("Dt_doc_xml"), pl.col("Dt_doc_c170")]).alias("Dt_doc"),
                pl.coalesce([pl.col("Dt_e_s_xml"), pl.col("Dt_e_s_c170")]).alias("Dt_e_s"),
                pl.coalesce([pl.col("Num_item_xml"), pl.col("Num_item_c170")]).alias("Num_item"),
                pl.coalesce([pl.col("finnfe_xml"), pl.lit(None, pl.Utf8)]).alias("finnfe"),
                pl.coalesce([pl.col("co_uf_emit_xml"), pl.col("Chv_nfe").str.slice(0, 2)]).alias("co_uf_emit"),
                pl.col("co_uf_dest_xml").alias("co_uf_dest"),
                pl.coalesce([pl.col("Cod_item_xml"), pl.col("Cod_item_c170")]).alias("Cod_item"),
                pl.coalesce([pl.col("Cod_barra_xml"), pl.col("Cod_barra_c170")]).alias("Cod_barra"),
                pl.coalesce([pl.col("Ncm_xml"), pl.col("Ncm_c170")]).alias("Ncm"),
                pl.coalesce([pl.col("Cest_xml"), pl.col("Cest_c170")]).alias("Cest"),
                pl.col("Tipo_item_c170").alias("Tipo_item"),
                pl.coalesce([pl.col("Descr_item_xml"), pl.col("Descr_item_c170")]).alias("Descr_item"),
                pl.col("Descr_compl_c170").alias("Descr_compl"),
                pl.coalesce([pl.col("Cfop_xml"), pl.col("Cfop_c170")]).alias("Cfop"),
                pl.coalesce([pl.col("cst_xml"), pl.col("Cst_c170")]).alias("Cst"),
                pl.coalesce([pl.col("Qtd_xml"), pl.col("Qtd_c170")]).alias("Qtd"),
                pl.coalesce([pl.col("Unid_xml"), pl.col("Unid_c170")]).alias("Unid"),
                pl.coalesce([pl.col("Vl_item_xml"), pl.col("Vl_item_c170")]).alias("Vl_item"),
                pl.col("vl_desc_c170").alias("vl_desc"),
                pl.coalesce([pl.col("Aliq_icms_xml"), pl.col("Aliq_icms_c170")]).alias("Aliq_icms"),
                pl.coalesce([pl.col("Vl_bc_icms_xml"), pl.col("Vl_bc_icms_c170")]).alias("Vl_bc_icms"),
                pl.coalesce([pl.col("Vl_icms_xml"), pl.col("Vl_icms_c170")]).alias("Vl_icms"),
                pl.coalesce([pl.col("vl_icms_st_xml"), pl.col("vl_icms_st_c170")]).alias("vl_icms_st"),
                pl.coalesce([pl.col("vl_bc_icms_st_xml"), pl.col("vl_bc_icms_st_c170")]).alias("vl_bc_icms_st"),
                pl.coalesce([pl.col("aliq_st_xml"), pl.col("aliq_st_c170")]).alias("aliq_st"),
                pl.col("id_agrupado"),
                pl.coalesce([pl.col("co_sefin_agr_xml"), pl.col("co_sefin_agr")]).alias("co_sefin_agr"),
                pl.col("score_vinculo_xml"),
                pl.col("desc_similarity"),
                pl.col("diff_qtd"),
                pl.col("diff_vl_total"),
                pl.col("diff_vl_unit"),
                pl.col("regra_vinculo_xml"),
                pl.col("fonte_xml"),
                pl.col("match_xml"),
                pl.col("Cod_item_c170").alias("cod_item_c170_ref"),
                pl.col("Cod_item_xml").alias("cod_item_xml_escolhido"),
                pl.col("Num_item_c170").alias("num_item_c170_ref"),
                pl.col("Num_item_xml").alias("num_item_xml_escolhido"),
            ]
        )
        .with_columns(
            [
                pl.col("Ncm").alias("ncm_padrao"),
                pl.col("Cest").alias("cest_padrao"),
            ]
        )
    )

    df_final = enriquecer_co_sefin_class(df_final, cnpj)
    df_final = (
        df_final
        .drop(["ncm_padrao", "cest_padrao"], strict=False)
        .sort(["Dt_doc", "Chv_nfe", "Num_item"], nulls_last=True)
    )

    return salvar_para_parquet(df_final, pasta_brutos, f"c170_xml_{cnpj}.parquet")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        gerar_c170_xml(sys.argv[1])
    else:
        gerar_c170_xml(input("CNPJ: "))
