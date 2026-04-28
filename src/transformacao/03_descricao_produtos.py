"""
03_descricao_produtos.py

Objetivo: Gerar a tabela consolidada de descricoes normalizadas e unicas.

Saida:
- descricao_produtos_<cnpj>.parquet em analises/produtos
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
UTILITARIOS_DIR = SRC_DIR / "utilitarios"
DADOS_DIR = ROOT_DIR / "dados"
CNPJ_ROOT = DADOS_DIR / "CNPJ"

for _dir in (SRC_DIR, UTILITARIOS_DIR):
    dir_str = str(_dir)
    if dir_str not in sys.path:
        sys.path.insert(0, dir_str)

try:
    from salvar_para_parquet import salvar_para_parquet
    from text import expr_normalizar_descricao
    from item_unidades import item_unidades
    from itens import itens
except ImportError as e:
    rprint(f"[red]Erro ao importar modulos:[/red] {e}")
    sys.exit(1)


def _normalizar_descricao_expr(col: str) -> pl.Expr:
    """Retorna expressão Polars que normaliza a coluna de descrição para 'descricao_normalizada'."""
    return expr_normalizar_descricao(col).alias("descricao_normalizada")


def _agg_list(col: str, alias: str) -> pl.Expr:
    """Agrega valores únicos e ordenados de *col* em uma List, expondo-os sob *alias*."""
    return (
        pl.col(col)
        .cast(pl.String, strict=False)
        .str.strip_chars()
        .replace("", None)
        .drop_nulls()
        .unique()
        .sort()
        .alias(alias)
    )


def descricao_produtos(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    """Gera a tabela consolidada de descrições normalizadas e únicas.

    Lê ``item_unidades_{cnpj}.parquet`` e ``itens_{cnpj}.parquet``, agrega por
    ``descricao_normalizada`` e produz ``descricao_produtos_{cnpj}.parquet`` em
    ``analises/produtos``.

    Args:
        cnpj: CPF (11 dígitos) ou CNPJ (14 dígitos) do contribuinte.
        pasta_cnpj: Pasta raiz do CNPJ; usa o padrão global quando None.

    Returns:
        True em caso de sucesso; False se a geração falhar ou inputs estiverem ausentes.
    """
    cnpj = re.sub(r"\D", "", cnpj or "")
    if len(cnpj) not in {11, 14}:
        raise ValueError("CPF/CNPJ invalido.")

    if pasta_cnpj is None:
        pasta_cnpj = CNPJ_ROOT / cnpj

    pasta_analises = pasta_cnpj / "analises" / "produtos"
    arq_item_unid = pasta_analises / f"item_unidades_{cnpj}.parquet"
    arq_itens = pasta_analises / f"itens_{cnpj}.parquet"

    if not arq_item_unid.exists():
        rprint("[yellow]item_unidades nao encontrado. Gerando base...[/yellow]")
        if not item_unidades(cnpj, pasta_cnpj):
            return False

    if not arq_itens.exists():
        rprint("[yellow]itens nao encontrado. Gerando base...[/yellow]")
        if not itens(cnpj, pasta_cnpj):
            return False

    if not arq_item_unid.exists() or not arq_itens.exists():
        rprint("[red]Arquivos base para descricao_produtos nao foram encontrados.[/red]")
        return False

    rprint(f"[bold cyan]Gerando descricao_produtos para CNPJ: {cnpj}[/bold cyan]")

    lf_item_unid = pl.scan_parquet(arq_item_unid)
    lf_itens = pl.scan_parquet(arq_itens)

    # Garante colunas obrigatórias
    required_item_cols = [
        "id_item_unid",
        "codigo",
        "descricao",
        "descr_compl",
        "tipo_item",
        "ncm",
        "cest",
        "co_sefin_item",
        "gtin",
        "unid",
        "fontes",
    ]
    for col in required_item_cols:
        if col not in lf_item_unid.schema:
            if col == "fontes":
                lf_item_unid = lf_item_unid.with_columns(
                    pl.lit([]).cast(pl.List(pl.String)).alias(col)
                )
            else:
                lf_item_unid = lf_item_unid.with_columns(pl.lit(None, pl.String).alias(col))

    lf_item_unid = lf_item_unid.with_columns(_normalizar_descricao_expr("descricao"))

    lf_lista_ids = (
        lf_itens.select(["descricao_normalizada", "id_item"])
        .group_by("descricao_normalizada")
        .agg(_agg_list("id_item", "lista_id_item"))
    )

    lf_descricoes = lf_item_unid.group_by("descricao_normalizada").agg(
        [
            pl.col("descricao").drop_nulls().first().alias("descricao"),
            _agg_list("descr_compl", "lista_desc_compl"),
            _agg_list("codigo", "lista_codigos"),
            _agg_list("tipo_item", "lista_tipo_item"),
            _agg_list("ncm", "lista_ncm"),
            _agg_list("cest", "lista_cest"),
            _agg_list("co_sefin_item", "lista_co_sefin"),
            _agg_list("gtin", "lista_gtin"),
            _agg_list("unid", "lista_unid"),
            pl.col("fontes").explode().drop_nulls().unique().sort().alias("fontes"),
            _agg_list("id_item_unid", "lista_id_item_unid"),
        ]
    )
    # Join LazyFrames
    lf_descricoes = lf_descricoes.join(lf_lista_ids, on="descricao_normalizada", how="left")
    lf_descricoes = lf_descricoes.sort(["descricao_normalizada", "descricao"], nulls_last=True)
    lf_descricoes = lf_descricoes.with_row_count("seq", offset=1)
    lf_descricoes = lf_descricoes.with_columns(
        pl.format("id_descricao_{}", pl.col("seq")).alias("id_descricao")
    )
    lf_descricoes = lf_descricoes.drop("seq")
    lf_descricoes = lf_descricoes.select(
        [
            "id_descricao",
            "descricao_normalizada",
            "descricao",
            "lista_desc_compl",
            "lista_codigos",
            "lista_tipo_item",
            "lista_ncm",
            "lista_cest",
            "lista_co_sefin",
            "lista_gtin",
            "lista_unid",
            "fontes",
            "lista_id_item_unid",
            "lista_id_item",
        ]
    )

    df_descricoes = lf_descricoes.collect()
    if df_descricoes.is_empty():
        rprint("[yellow]Arquivo descricao_produtos resultou vazio.[/yellow]")
        return False
    return salvar_para_parquet(df_descricoes, pasta_analises, f"descricao_produtos_{cnpj}.parquet")


def gerar_descricao_produtos(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    """Alias público para ``descricao_produtos``; ponto de entrada usado pelo orquestrador."""
    return descricao_produtos(cnpj, pasta_cnpj)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        descricao_produtos(sys.argv[1])
    else:
        descricao_produtos(input("CNPJ: "))
