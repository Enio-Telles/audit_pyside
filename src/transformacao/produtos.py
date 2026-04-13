"""
produtos.py

Objetivo: Gerar a tabela de produtos unicos reportados pelas fontes.
Identificador canonico: chave_produto (derivado de codigo_fonte).
Agrupamento: Por codigo_fonte (uma linha exata por origem de produto).
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import polars as pl
from rich.console import Console

from src.utilitarios.salvar_para_parquet import salvar_para_parquet
from src.utilitarios.text import remove_accents

CURRENT_FILE = Path(__file__).resolve()
ROOT_DIR = CURRENT_FILE.parent.parent.parent
DADOS_DIR = ROOT_DIR / "dados"
CNPJ_ROOT = DADOS_DIR / "CNPJ"

ERR_CONSOLE = Console(stderr=True)
OUT_CONSOLE = Console(stderr=False)


def _remover_acentos(text: str | None) -> str | None:
    return remove_accents(text) if text else None


def gerar_produtos(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    cnpj = re.sub(r"\D", "", cnpj)
    if pasta_cnpj is None:
        pasta_cnpj = CNPJ_ROOT / cnpj

    pasta_analises = pasta_cnpj / "analises" / "produtos"
    arq_input = pasta_analises / f"produtos_unidades_{cnpj}.parquet"

    if not arq_input.exists():
        ERR_CONSOLE.print(f"[red]Arquivo de entrada nao encontrado:[/red] {arq_input}")
        return False

    OUT_CONSOLE.print(f"[bold cyan]Gerando produtos para CNPJ: {cnpj}[/bold cyan]")

    try:
        df = pl.read_parquet(arq_input)

        # Optimization: Use native Polars string operations to preserve vectorization
        df = df.with_columns(
            pl.col("descricao")
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
            .alias("descricao_normalizada")
        )

        def _agg_list(col: str, suffix: str | None = None) -> pl.Expr:
            target_alias = f"lista_{col}" if suffix is None else f"lista_{suffix}"
            return (
                pl.col(col)
                .cast(pl.String)
                .str.strip_chars()
                .replace("", None)
                .drop_nulls()
                .unique()
                .sort()
                .alias(target_alias)
            )

        df_grouped = (
            df.group_by("codigo_fonte")
            .agg(
                [
                    pl.col("descricao_normalizada").first().alias("descricao_normalizada"),
                    pl.col("descricao").first().alias("descricao"),
                    _agg_list("descr_compl"),
                    _agg_list("codigo"),
                    _agg_list("tipo_item"),
                    _agg_list("ncm"),
                    _agg_list("cest"),
                    _agg_list("gtin"),
                    _agg_list("co_sefin_item", suffix="co_sefin"),
                    _agg_list("unid", suffix="unid"),
                    pl.col("compras").sum().alias("total_compras"),
                    pl.col("vendas").sum().alias("total_vendas"),
                ]
            )
        )

        df_grouped = df_grouped.with_columns(
            pl.col("codigo_fonte").alias("chave_produto"),
            pl.col("codigo_fonte").alias("chave_item") # Mantido para compatibilidade temporaria
        )

        list_cols = [c for c in df_grouped.columns if c.startswith("lista_")]
        cols = [
            "codigo_fonte",
            "chave_produto",
            "chave_item",
            "descricao_normalizada",
            "descricao",
            *list_cols,
            "total_compras",
            "total_vendas",
        ]
        df_grouped = df_grouped.select(cols)

        arquivo_final = f"produtos_{cnpj}.parquet"
        OUT_CONSOLE.print(f"[dim]Salvando {arquivo_final} em {pasta_analises}[/dim]")
        ok = salvar_para_parquet(df_grouped, pasta_analises, arquivo_final)
        if not ok:
            raise RuntimeError(f"Falha ao salvar {arquivo_final}.")
        return True

    except Exception as exc:
        ERR_CONSOLE.print(f"[red]Erro fatal no processamento de produtos para {cnpj}:[/red] {exc}")
        raise


if __name__ == "__main__":
    if len(sys.argv) > 1:
        gerar_produtos(sys.argv[1])
    else:
        c = input("CNPJ: ")
        gerar_produtos(c)
