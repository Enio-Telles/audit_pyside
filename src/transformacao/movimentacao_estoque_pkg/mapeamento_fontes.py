"""
mapeamento_fontes.py

Funcoes de mapeamento de dados brutos para o schema padrao de mov_estoque:
- Parse de expressoes do map_estoque.json
- Deteccao de colunas de descricao/unidade por fonte
- Normalizacao de descricao
- Carregamento de flags CFOP

Extraido de movimentacao_estoque.py para melhorar modularidade.
"""

from __future__ import annotations

import re
from pathlib import Path

import polars as pl
from utilitarios.text import remove_accents

ROOT_DIR = Path(r"c:\funcoes - Copia")
DADOS_DIR = ROOT_DIR / "dados"


def normalizar_descricao_expr(col: str) -> pl.Expr:
    # Optimization: Replace .map_elements with native Polars string operations to preserve vectorization
    return (
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
        .alias("__descricao_normalizada__")
    )


def detectar_coluna_descricao(df: pl.DataFrame, fonte: str) -> str | None:
    candidatos = {
        "c170": ["descr_item", "descricao", "prod_xprod"],
        "bloco_h": ["descricao_produto", "descr_item", "descricao", "prod_xprod"],
        "nfe": ["prod_xprod", "descricao", "descr_item"],
        "nfce": ["prod_xprod", "descricao", "descr_item"],
    }
    for col in candidatos.get(fonte, []):
        if col in df.columns:
            return col
    return None


def detectar_coluna_unidade(df: pl.DataFrame, fonte: str) -> str | None:
    candidatos = {
        "c170": ["unid"],
        "bloco_h": ["unidade_medida", "unidade_media", "unid", "unidade"],
        "nfe": ["prod_ucom"],
        "nfce": ["prod_ucom"],
    }
    for col in candidatos.get(fonte, []):
        if col in df.columns:
            return col
    return None


def parse_expression(expr_str: str, col_alias: str) -> pl.Expr:
    """Traduz as strings do map_estoque em expressoes do Polars."""
    if not expr_str or str(expr_str).strip() == "" or expr_str == "(vazio)":
        return pl.lit(None).alias(col_alias)

    expr_str = str(expr_str).strip()

    # Literal string
    if expr_str.startswith('"') and expr_str.endswith('"'):
        return pl.lit(expr_str.strip('"')).alias(col_alias)

    # NCM/CEST cleanup
    if expr_str in ["cod_ncm", "prod_ncm", "cest", "prod_cest"]:
        return pl.col(expr_str).cast(pl.String).str.replace_all(r"\D", "").alias(col_alias)

    # Complex: Cst
    if expr_str == "icms_orig & icms_cst ou icms_csosn":
        return pl.when(pl.col("icms_cst").is_not_null())\
                 .then(pl.concat_str([pl.col("icms_orig"), pl.col("icms_cst")], separator=""))\
                 .otherwise(pl.concat_str([pl.col("icms_orig"), pl.col("icms_csosn")], separator=""))\
                 .alias(col_alias)

    # Complex: Cod_barra
    if expr_str == "prod_ceantrib ou caso for nulo -> prod_cean":
        return pl.coalesce(["prod_ceantrib", "prod_cean"]).alias(col_alias)

    # Complex: Valores matemáticos (Vl_item em C170 ou Nfe)
    if expr_str == "vl_item-vl_desc":
        return (pl.col("vl_item").cast(pl.Float64) - pl.col("vl_desc").cast(pl.Float64).fill_null(0)).alias(col_alias)

    if expr_str == "prod_vprod+prod_vfrete+prod_vseg+prod_voutro-prod_vdesc":
        return (
            pl.col("prod_vprod").cast(pl.Float64).fill_null(0) +
            pl.col("prod_vfrete").cast(pl.Float64).fill_null(0) +
            pl.col("prod_vseg").cast(pl.Float64).fill_null(0) +
            pl.col("prod_voutro").cast(pl.Float64).fill_null(0) -
            pl.col("prod_vdesc").cast(pl.Float64).fill_null(0)
        ).alias(col_alias)

    # Extração via Chave
    if expr_str == "correspondência com chave NF":
        if col_alias == "mod":
            return pl.col("chv_nfe").str.slice(20, 2).alias(col_alias)
        elif col_alias == "co_uf_emit":
            return pl.col("chv_nfe").str.slice(0, 2).alias(col_alias)
        elif col_alias == "co_uf_dest":
            return pl.lit(None).alias(col_alias)
        return pl.lit(None).alias(col_alias)

    if expr_str == '"gerado" ou "registro" (se está no bloco_h)':
        return pl.lit("registro").alias(col_alias)

    # Fallback to column name
    return pl.col(expr_str).alias(col_alias)


def carregar_flags_cfop() -> pl.DataFrame:
    """Carrega e combina as tabelas de referencia CFOP para flags de exclusao/devolucao."""
    arq_cfop = DADOS_DIR / "referencias" / "referencias" / "cfop" / "cfop.parquet"
    arq_cfop_bi = DADOS_DIR / "referencias" / "referencias" / "cfop" / "cfop_bi.parquet"

    df_cfop = pl.DataFrame(schema={"Cfop": pl.Utf8})
    if arq_cfop.exists():
        df_cfop_raw = pl.read_parquet(arq_cfop)
        exprs = [
            pl.col("co_cfop").cast(pl.Utf8, strict=False).str.replace_all(r"\D", "").alias("Cfop"),
        ]
        for col in ["excluir_estoque", "dev_simples"]:
            if col in df_cfop_raw.columns:
                exprs.append(pl.col(col).alias(col))
            else:
                exprs.append(pl.lit(None).alias(col))
        df_cfop = (
            df_cfop_raw
            .select(exprs)
            .filter(pl.col("Cfop").is_not_null() & (pl.col("Cfop") != ""))
            .unique(subset=["Cfop"], keep="first")
        )

    df_cfop_bi = pl.DataFrame(schema={"Cfop": pl.Utf8})
    if arq_cfop_bi.exists():
        df_cfop_bi_raw = pl.read_parquet(arq_cfop_bi)
        exprs = [
            pl.col("co_cfop").cast(pl.Utf8, strict=False).str.replace_all(r"\D", "").alias("Cfop"),
        ]
        for col in ["dev_venda", "dev_compra", "dev_ent_simples"]:
            if col in df_cfop_bi_raw.columns:
                exprs.append(pl.col(col).alias(col))
            else:
                exprs.append(pl.lit(None).alias(col))
        df_cfop_bi = (
            df_cfop_bi_raw
            .select(exprs)
            .filter(pl.col("Cfop").is_not_null() & (pl.col("Cfop") != ""))
            .unique(subset=["Cfop"], keep="first")
        )

    if df_cfop.is_empty() and df_cfop_bi.is_empty():
        return pl.DataFrame(
            schema={
                "Cfop": pl.Utf8,
                "excluir_estoque": pl.Boolean,
                "dev_simples": pl.Boolean,
                "dev_venda": pl.Utf8,
                "dev_compra": pl.Utf8,
                "dev_ent_simples": pl.Utf8,
            }
        )

    if df_cfop.is_empty():
        return df_cfop_bi
    if df_cfop_bi.is_empty():
        return df_cfop
    return df_cfop.join(df_cfop_bi, on="Cfop", how="full", coalesce=True)
