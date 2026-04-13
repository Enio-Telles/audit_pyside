"""
04_produtos_final.py

Objetivo: inicializar a camada de agrupamento manual e gerar a tabela final
de produtos recalculavel a partir de descricao_produtos.

Saidas:
- produtos_agrupados_<cnpj>.parquet
- map_produto_agrupado_<cnpj>.parquet
- produtos_final_<cnpj>.parquet
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
    from transformacao.descricao_produtos import descricao_produtos
    from transformacao.id_agrupados import gerar_id_agrupados
except ImportError as e:
    rprint(f"[red]Erro ao importar modulos:[/red] {e}")
    sys.exit(1)


def _gerar_id_agrupado(seq: int) -> str:
    return f"id_agrupado_{seq}"


def _serie_limpa_lista(values: list | None) -> list[str]:
    if not values:
        return []
    out: list[str] = []
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            out.append(text)
    return sorted(set(out))



def get_mode_expr(col_name: str) -> pl.Expr:
    return (
        pl.col(col_name)
        .cast(pl.Utf8, strict=False)
        .str.strip_chars()
        .filter(pl.col(col_name).cast(pl.Utf8, strict=False).str.strip_chars() != "")
        .mode()
        .first()
    )

def _clean_list_expr(col_name: str) -> pl.Expr:
    return (
        pl.col(col_name)
        .list.eval(
            pl.element()
            .cast(pl.Utf8, strict=False)
            .str.strip_chars()
            .filter(pl.element().is_not_null() & (pl.element().str.strip_chars() != ""))
        )
        .list.unique()
        .list.sort()
    )

def produtos_agrupados(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    cnpj = re.sub(r"\D", "", cnpj or "")
    if len(cnpj) not in {11, 14}:
        raise ValueError("CPF/CNPJ invalido.")

    if pasta_cnpj is None:
        pasta_cnpj = CNPJ_ROOT / cnpj

    pasta_analises = pasta_cnpj / "analises" / "produtos"
    arq_descricoes = pasta_analises / f"descricao_produtos_{cnpj}.parquet"
    arq_item_unid = pasta_analises / f"item_unidades_{cnpj}.parquet"

    if not arq_descricoes.exists():
        rprint("[yellow]descricao_produtos nao encontrado. Gerando base...[/yellow]")
        if not descricao_produtos(cnpj, pasta_cnpj):
            return False

    if not arq_descricoes.exists() or not arq_item_unid.exists():
        rprint("[red]Arquivos base para agrupamento final nao encontrados.[/red]")
        return False

    rprint(f"[bold cyan]Gerando produtos_agrupados/final para CNPJ: {cnpj}[/bold cyan]")

    try:
        schema_descricoes = validar_parquet_essencial(
            arq_descricoes,
            ["id_descricao", "descricao_normalizada", "descricao"],
            contexto="produtos_final/descricao_produtos",
        )
        validar_parquet_essencial(
            arq_item_unid,
            ["descricao", "ncm", "cest", "gtin", "co_sefin_item"],
            contexto="produtos_final/item_unidades",
        )
    except SchemaValidacaoError as exc:
        rprint(f"[red]{exc}[/red]")
        return False

    df_descricoes = pl.scan_parquet(arq_descricoes).select(schema_descricoes).collect()
    df_item_unid = (
        pl.scan_parquet(arq_item_unid)
        .select(["descricao", "ncm", "cest", "gtin", "co_sefin_item"])
        .collect()
    )

    if df_descricoes.is_empty():
        rprint("[yellow]descricao_produtos esta vazio.[/yellow]")
        return False

    for col in ["lista_unid", "fontes", "lista_co_sefin", "lista_id_item_unid", "lista_id_item",
                  "lista_ncm", "lista_cest", "lista_gtin", "lista_desc_compl"]:
        if col not in df_descricoes.columns:
            df_descricoes = df_descricoes.with_columns(pl.lit([]).cast(pl.List(pl.String)).alias(col))

    # Cast any List(Null) columns to List(String) — happens when all source values were null
    _list_str_cols = ["lista_ncm", "lista_cest", "lista_gtin", "lista_unid", "lista_co_sefin", "lista_desc_compl"]
    df_descricoes = df_descricoes.with_columns([
        pl.col(c).cast(pl.List(pl.String), strict=False)
        for c in _list_str_cols
        if c in df_descricoes.columns
    ])

    df_item_unid_norm = (
        df_item_unid
        .filter(pl.col("descricao").is_not_null())
        .with_columns(
            pl.col("descricao")
            .cast(pl.Utf8, strict=False)
            .str.to_uppercase()
            .str.replace_all(r"\s+", " ")
            .alias("__descricao_upper")
        )
    )


    df_item_unid_parts = df_item_unid_norm.partition_by("__descricao_upper", as_dict=True)
    df_item_unid_empty = df_item_unid_norm.filter(pl.lit(False)).drop("__descricao_upper", strict=False)

    # ⚡ Bolt: Vectorized calculation of standard attributes by description
    # This replaces the O(N) loop with O(1) vectorized operations
    df_padrao = (
        df_item_unid_norm
        .group_by("__descricao_upper")
        .agg([
            pl.col("descricao").first().alias("descr_padrao"),
            get_mode_expr("ncm").alias("ncm_padrao"),
            get_mode_expr("cest").alias("cest_padrao"),
            get_mode_expr("gtin").alias("gtin_padrao"),
            get_mode_expr("co_sefin_item").alias("co_sefin_padrao")
        ])
    )

    # Note: registros_mestra and registros_ponte are now superseded by vectorized joins below
    # but we keep the structure if needed for specific logic.

    # 2. Join the pre-calculated attributes back to df_descricoes
    df_descricoes = df_descricoes.with_row_index("seq", offset=1)
    df_descricoes = df_descricoes.with_columns([
        pl.format("id_agrupado_{}", pl.col("seq")).alias("id_agrupado")
    ])

    df_mestra_base = df_descricoes.join(
        df_padrao, left_on="descricao_normalizada", right_on="__descricao_upper", how="left"
    )

    # 3. Handle missing null schema resolution by explicitly handling null lists
    # Since we can't reliably map empty arrays if schema goes null, we avoid list.eval with string ops
    # if it's potentially returning null schema. Actually, our cast earlier fixed most.

    df_mestra = df_mestra_base.with_columns([
        pl.when(pl.col("id_descricao").is_not_null())
          .then(pl.col("id_descricao").cast(pl.List(pl.Utf8)))
          .otherwise(pl.lit([]).cast(pl.List(pl.Utf8)))
          .alias("lista_chave_produto"),
        pl.coalesce([pl.col("descr_padrao"), pl.col("descricao")]).alias("descr_padrao"),
        _clean_list_expr("lista_ncm").alias("lista_ncm"),
        _clean_list_expr("lista_cest").alias("lista_cest"),
        _clean_list_expr("lista_gtin").alias("lista_gtin"),
        _clean_list_expr("lista_co_sefin").alias("lista_co_sefin"),
        _clean_list_expr("lista_unid").alias("lista_unidades"),
        _clean_list_expr("fontes").alias("fontes"),

        pl.col("descricao").cast(pl.Utf8, strict=False).str.strip_chars()
          .filter(pl.col("descricao").cast(pl.Utf8, strict=False).str.strip_chars() != "")
          .map_elements(lambda x: [x] if x else [], return_dtype=pl.List(pl.Utf8)).alias("lista_descricoes"),

        _clean_list_expr("lista_desc_compl").alias("lista_desc_compl"),
    ]).with_columns([
        (pl.col("lista_co_sefin").list.len() > 1).alias("co_sefin_divergentes")
    ])

    df_mestra = df_mestra.select([
        pl.col("id_agrupado").cast(pl.String),
        pl.col("lista_chave_produto").cast(pl.List(pl.String)),
        pl.col("descr_padrao").cast(pl.String),
        pl.col("ncm_padrao").cast(pl.String),
        pl.col("cest_padrao").cast(pl.String),
        pl.col("gtin_padrao").cast(pl.String),
        pl.col("lista_ncm").cast(pl.List(pl.String)),
        pl.col("lista_cest").cast(pl.List(pl.String)),
        pl.col("lista_gtin").cast(pl.List(pl.String)),
        pl.col("lista_descricoes").cast(pl.List(pl.String)),
        pl.col("lista_desc_compl").cast(pl.List(pl.String)),
        pl.col("lista_co_sefin").cast(pl.List(pl.String)),
        pl.col("co_sefin_padrao").cast(pl.String),
        pl.col("lista_unidades").cast(pl.List(pl.String)),
        pl.col("co_sefin_divergentes").cast(pl.Boolean),
        pl.col("fontes").cast(pl.List(pl.String)),
    ])

    df_ponte = df_descricoes.filter(pl.col("id_descricao").is_not_null()).select([
        pl.col("id_descricao").alias("chave_produto"),
        "id_agrupado"
    ])

    ok_mestra = salvar_para_parquet(df_mestra, pasta_analises, f"produtos_agrupados_{cnpj}.parquet")
    ok_ponte = salvar_para_parquet(df_ponte, pasta_analises, f"map_produto_agrupado_{cnpj}.parquet")
    if not (ok_mestra and ok_ponte):
        return False

    df_map = (
        df_mestra
        .select(
            [
                "id_agrupado",
                "lista_chave_produto",
                "descr_padrao",
                "ncm_padrao",
                "cest_padrao",
                "gtin_padrao",
                pl.col("lista_co_sefin").alias("lista_co_sefin_agr"),
                "co_sefin_padrao",
                pl.col("lista_unidades").alias("lista_unidades_agr"),
                "co_sefin_divergentes",
                pl.col("fontes").alias("fontes_agr"),
            ]
        )
        .explode("lista_chave_produto")
        .rename({"lista_chave_produto": "id_descricao"})
    )

    df_final = (
        df_descricoes
        .join(df_map, on="id_descricao", how="left")
        .with_columns(
            [
                pl.coalesce([pl.col("descr_padrao"), pl.col("descricao")]).alias("descricao_final"),
                pl.coalesce([pl.col("ncm_padrao"), pl.col("lista_ncm").list.first()]).alias("ncm_final"),
                pl.coalesce([pl.col("cest_padrao"), pl.col("lista_cest").list.first()]).alias("cest_final"),
                pl.coalesce([pl.col("gtin_padrao"), pl.col("lista_gtin").list.first()]).alias("gtin_final"),
                pl.coalesce(
                    [
                        pl.col("co_sefin_padrao"),
                        pl.col("lista_co_sefin_agr").list.first(),
                        pl.col("lista_co_sefin").list.first(),
                    ]
                ).alias("co_sefin_final"),
                pl.coalesce([pl.col("lista_unidades_agr").list.first(), pl.col("lista_unid").list.first()]).alias("unid_ref_sugerida"),
            ]
        )
        .sort(["id_agrupado", "id_descricao"], nulls_last=True)
    )

    ok_final = salvar_para_parquet(df_final, pasta_analises, f"produtos_final_{cnpj}.parquet")
    if not ok_final:
        return False
    return gerar_id_agrupados(cnpj, pasta_cnpj)

def gerar_produtos_final(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    return produtos_agrupados(cnpj, pasta_cnpj)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        produtos_agrupados(sys.argv[1])
    else:
        produtos_agrupados(input("CNPJ: "))
