"""
produtos_final.py

Objetivo: Integrar produtos + produtos_agrupados em uma visao final recalculavel.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import polars as pl
from rich.console import Console

from src.utilitarios.salvar_para_parquet import salvar_para_parquet

CURRENT_FILE = Path(__file__).resolve()
ROOT_DIR = CURRENT_FILE.parent.parent.parent
DADOS_DIR = ROOT_DIR / "dados"
CNPJ_ROOT = DADOS_DIR / "CNPJ"

ERR_CONSOLE = Console(stderr=True)
OUT_CONSOLE = Console(stderr=False)


def gerar_produtos_final(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    cnpj = re.sub(r"\D", "", cnpj)
    if pasta_cnpj is None:
        pasta_cnpj = CNPJ_ROOT / cnpj

    pasta_analises = pasta_cnpj / "analises" / "produtos"
    arq_produtos = pasta_analises / f"produtos_{cnpj}.parquet"
    arq_agrupados = pasta_analises / f"produtos_agrupados_{cnpj}.parquet"

    if not arq_produtos.exists():
        ERR_CONSOLE.print(f"[red]Arquivo nao encontrado:[/red] {arq_produtos}")
        return False
    if not arq_agrupados.exists():
        ERR_CONSOLE.print(f"[red]Arquivo nao encontrado:[/red] {arq_agrupados}")
        return False

    OUT_CONSOLE.print(f"[bold cyan]Gerando produtos_final para CNPJ: {cnpj}[/bold cyan]")

    try:
        df_produtos = pl.read_parquet(arq_produtos)
        if "chave_item" not in df_produtos.columns and "chave_produto" in df_produtos.columns:
            df_produtos = df_produtos.with_columns(pl.col("chave_produto").alias("chave_item"))
        if "chave_produto" not in df_produtos.columns and "chave_item" in df_produtos.columns:
            df_produtos = df_produtos.with_columns(pl.col("chave_item").alias("chave_produto"))
        df_agrupados = pl.read_parquet(arq_agrupados)
        arq_pont = pasta_analises / f"map_produto_agrupado_{cnpj}.parquet"
        if arq_pont.exists() and "lista_chave_produto" not in df_agrupados.columns:
            df_pont = pl.read_parquet(arq_pont)
            df_list = df_pont.group_by("id_agrupado").agg(pl.col("chave_produto").alias("lista_chave_produto"))
            df_agrupados = df_agrupados.join(df_list, on="id_agrupado", how="left")

        if "co_sefin_agr" not in df_agrupados.columns:
            df_agrupados = df_agrupados.with_columns(
                pl.col("lista_co_sefin")
                .list.eval(pl.element().cast(pl.Utf8))
                .list.join(", ")
                .alias("co_sefin_agr")
            )

        cols_agrup_base = [
            "id_agrupado",
            "lista_chave_produto",
            "descr_padrao",
            "ncm_padrao",
            "cest_padrao",
            "gtin_padrao",
            pl.col("lista_co_sefin").alias("lista_co_sefin_agr"),
            "co_sefin_padrao",
            "co_sefin_agr",
            pl.col("lista_unidades").alias("lista_unidades_agr"),
            "co_sefin_divergentes",
        ]
        cols_agrup_opt = []
        cols_agrup_opt_names = []
        if "total_compras" in df_agrupados.columns:
            cols_agrup_opt.append(pl.col("total_compras").alias("total_compras_agr"))
            cols_agrup_opt_names.append("total_compras_agr")
        if "total_vendas" in df_agrupados.columns:
            cols_agrup_opt.append(pl.col("total_vendas").alias("total_vendas_agr"))
            cols_agrup_opt_names.append("total_vendas_agr")

        df_map = (
            df_agrupados
            .select([*cols_agrup_base, *cols_agrup_opt])
            .explode("lista_chave_produto")
            .rename({"lista_chave_produto": "chave_item"})
        )

        df_final = df_produtos.join(df_map, on="chave_item", how="left")

        df_final = df_final.with_columns(
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
                pl.coalesce([pl.col("lista_unidades_agr").list.first(), pl.col("lista_unid").list.first()]).alias(
                    "unid_ref_sugerida"
                ),
            ]
        )

        colunas_base = [c for c in df_produtos.columns]
        colunas_agrup = [
            "id_agrupado",
            "descr_padrao",
            "ncm_padrao",
            "cest_padrao",
            "gtin_padrao",
            "lista_co_sefin_agr",
            "co_sefin_padrao",
            "lista_unidades_agr",
            "co_sefin_divergentes",
            *cols_agrup_opt_names,
        ]
        colunas_finais = [
            "descricao_final",
            "ncm_final",
            "cest_final",
            "gtin_final",
            "co_sefin_final",
            "unid_ref_sugerida",
        ]

        df_final = (
            df_final
            .select([*colunas_base, *colunas_agrup, *colunas_finais])
            .sort(["id_agrupado", "chave_item"], nulls_last=True)
        )

        arquivo_final = f"produtos_final_{cnpj}.parquet"
        OUT_CONSOLE.print(f"[dim]Salvando {arquivo_final} em {pasta_analises}[/dim]")
        ok = salvar_para_parquet(df_final, pasta_analises, arquivo_final)
        if not ok:
            raise RuntimeError(f"Falha ao salvar {arquivo_final}.")
        return True
    except Exception as exc:
        ERR_CONSOLE.print(f"[red]Erro fatal ao gerar produtos_final para {cnpj}:[/red] {exc}")
        raise


if __name__ == "__main__":
    if len(sys.argv) > 1:
        gerar_produtos_final(sys.argv[1])
    else:
        c = input("CNPJ: ")
        gerar_produtos_final(c)
