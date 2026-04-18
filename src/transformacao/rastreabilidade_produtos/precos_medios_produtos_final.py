"""
precos_medios_produtos_final.py

Objetivo: Calcular preco medio por produto agrupado/unidade a partir da base final.
Tambem gera logs de itens sem preco medio de compra.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from utilitarios.project_paths import PROJECT_ROOT

import polars as pl
from rich import print as rprint

from transformacao.movimentacao_estoque_pkg.mapeamento_fontes import (
    normalizar_descricao_expr,
)

ROOT_DIR = PROJECT_ROOT
SRC_DIR = ROOT_DIR / "src"
DADOS_DIR = ROOT_DIR / "dados"
CNPJ_ROOT = DADOS_DIR / "CNPJ"


try:
    from utilitarios.salvar_para_parquet import salvar_para_parquet
except ImportError as e:
    rprint(f"[red]Erro ao importar modulos utilitarios:[/red] {e}")
    sys.exit(1)


def calcular_precos_medios_produtos_final(
    cnpj: str,
    pasta_cnpj: Path | None = None,
    salvar_logs: bool = True,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    cnpj = re.sub(r"\D", "", cnpj)
    if pasta_cnpj is None:
        pasta_cnpj = CNPJ_ROOT / cnpj

    pasta_analises = pasta_cnpj / "analises" / "produtos"
    arq_unid = pasta_analises / f"item_unidades_{cnpj}.parquet"
    arq_agrup = pasta_analises / f"produtos_agrupados_{cnpj}.parquet"
    arq_final = pasta_analises / f"produtos_final_{cnpj}.parquet"
    arq_map = pasta_analises / f"map_produto_agrupado_{cnpj}.parquet"

    for arq in (arq_unid, arq_agrup):
        if not arq.exists():
            raise FileNotFoundError(f"Arquivo necessario nao encontrado: {arq}")

    df_unid = pl.read_parquet(arq_unid)
    df_agrup = pl.read_parquet(arq_agrup).select(["id_agrupado", "descr_padrao"])

    if "codigo_fonte" in df_unid.columns and arq_map.exists():
        df_map = pl.read_parquet(arq_map).rename({"chave_produto": "codigo_fonte"})
        df_link = df_unid.join(df_map, on="codigo_fonte", how="left").join(
            df_agrup, on="id_agrupado", how="left"
        )
    else:
        if not arq_final.exists():
            raise FileNotFoundError(f"Arquivo necessario nao encontrado: {arq_final}")
        df_final = (
            pl.read_parquet(arq_final)
            .select(["descricao_normalizada", "id_agrupado", "descr_padrao"])
            .with_columns(
                [
                    pl.col("descricao_normalizada")
                    .cast(pl.Utf8, strict=False)
                    .fill_null(""),
                    pl.col("id_agrupado").cast(pl.Utf8, strict=False),
                    pl.col("descr_padrao").cast(pl.Utf8, strict=False),
                ]
            )
            .filter(pl.col("descricao_normalizada") != "")
            .unique(subset=["descricao_normalizada", "id_agrupado"])
        )
        df_link = (
            df_unid.with_columns(
                normalizar_descricao_expr("descricao").alias("descricao_normalizada")
            )
            .join(df_final, on="descricao_normalizada", how="left")
            .drop("descricao_normalizada")
            .join(df_agrup, on=["id_agrupado", "descr_padrao"], how="left")
        )

    df_precos = (
        df_link.filter(
            pl.col("id_agrupado").is_not_null() & pl.col("unid").is_not_null()
        )
        .group_by(["id_agrupado", "descr_padrao", "unid"])
        .agg(
            [
                pl.col("compras").sum().alias("compras_total"),
                pl.col("qtd_compras").sum().alias("qtd_compras_total"),
                pl.col("vendas").sum().alias("vendas_total"),
                pl.col("qtd_vendas").sum().alias("qtd_vendas_total"),
                (pl.col("qtd_compras").sum() + pl.col("qtd_vendas").sum()).alias(
                    "qtd_mov_total"
                ),
            ]
        )
        .with_columns(
            [
                pl.when(pl.col("qtd_compras_total") > 0)
                .then(pl.col("compras_total") / pl.col("qtd_compras_total"))
                .otherwise(None)
                .alias("preco_medio_compra"),
                pl.when(pl.col("qtd_vendas_total") > 0)
                .then(pl.col("vendas_total") / pl.col("qtd_vendas_total"))
                .otherwise(None)
                .alias("preco_medio_venda"),
            ]
        )
        .with_columns(
            [
                pl.coalesce(
                    [pl.col("preco_medio_compra"), pl.col("preco_medio_venda")]
                ).alias("preco_medio_base"),
                pl.when(pl.col("preco_medio_compra").is_not_null())
                .then(pl.lit("COMPRA"))
                .when(pl.col("preco_medio_venda").is_not_null())
                .then(pl.lit("VENDA"))
                .otherwise(pl.lit("SEM_PRECO"))
                .alias("origem_preco"),
            ]
        )
        .sort(["id_agrupado", "unid"])
    )

    df_sem_compra = (
        df_precos.filter(pl.col("preco_medio_compra").is_null())
        .with_columns(
            [
                pl.col("preco_medio_venda").is_not_null().alias("tem_preco_venda"),
                pl.when(pl.col("preco_medio_venda").is_not_null())
                .then(pl.lit("Sem compra; usado preco medio de venda como fallback."))
                .otherwise(pl.lit("Sem preco medio de compra e sem fallback de venda."))
                .alias("motivo"),
            ]
        )
        .select(
            [
                "id_agrupado",
                "descr_padrao",
                "unid",
                "qtd_compras_total",
                "qtd_vendas_total",
                "preco_medio_compra",
                "preco_medio_venda",
                "tem_preco_venda",
                "motivo",
            ]
        )
    )

    if salvar_logs:
        salvar_para_parquet(
            df_sem_compra, pasta_analises, f"log_sem_preco_medio_compra_{cnpj}.parquet"
        )
        resumo = {
            "cnpj": cnpj,
            "qtd_itens_sem_preco_compra": int(df_sem_compra.height),
            "qtd_itens_com_fallback_venda": int(
                df_sem_compra.filter(pl.col("tem_preco_venda")).height
            ),
            "qtd_itens_sem_preco_algum": int(
                df_sem_compra.filter(~pl.col("tem_preco_venda")).height
            ),
        }
        with open(
            pasta_analises / f"log_sem_preco_medio_compra_{cnpj}.json",
            "w",
            encoding="utf-8",
        ) as f:
            json.dump(resumo, f, ensure_ascii=False, indent=2)

    return df_precos, df_sem_compra


if __name__ == "__main__":
    if len(sys.argv) > 1:
        d1, d2 = calcular_precos_medios_produtos_final(sys.argv[1])
        rprint(f"[green]OK[/green] precos_medios={d1.height}, sem_compra={d2.height}")
    else:
        c = input("CNPJ: ")
        d1, d2 = calcular_precos_medios_produtos_final(c)
        rprint(f"[green]OK[/green] precos_medios={d1.height}, sem_compra={d2.height}")
