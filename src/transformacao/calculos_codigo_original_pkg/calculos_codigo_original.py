"""
calculos_codigo_original.py

Calcula saldos de estoque por Cod_item (codigo original do contribuinte),
sem agregacao por id_agrupado. Cada Cod_item/fonte e tratado de forma
independente, recalculando saldo e custo medio proprios.
"""

import re
import sys
from pathlib import Path
from time import perf_counter

import polars as pl
from rich import print as rprint

from utilitarios.project_paths import PROJECT_ROOT
from utilitarios.salvar_para_parquet import salvar_para_parquet
from utilitarios.perf_monitor import registrar_evento_performance
from utilitarios.calculos_compartilhados import boolish_expr
from transformacao.movimentacao_estoque_pkg.calculo_saldos import _calc_saldos_loop

ROOT_DIR = PROJECT_ROOT
CNPJ_ROOT = ROOT_DIR / "dados" / "CNPJ"


def _finnfe_4_expr() -> pl.Expr:
    return pl.col("finnfe").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars() == "4"


def calcular_aba_codigo_original_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    """
    Recalcula saldos de estoque agrupando por (Cod_item, fonte, __ano__) em vez
    de id_agrupado. Retorna agregacao mensal por (Cod_item, Descr_item, fonte, ano, mes).
    """
    if df.is_empty():
        return pl.DataFrame(
            schema={
                "Cod_item": pl.Utf8,
                "Descr_item": pl.Utf8,
                "fonte": pl.Utf8,
                "id_agrupado": pl.Utf8,
                "descr_padrao": pl.Utf8,
                "ano": pl.Int32,
                "mes": pl.Int32,
                "valor_entradas": pl.Float64,
                "qtd_entradas": pl.Float64,
                "pme_mes": pl.Float64,
                "valor_saidas": pl.Float64,
                "qtd_saidas": pl.Float64,
                "pms_mes": pl.Float64,
                "saldo_mes": pl.Float64,
                "custo_medio_mes": pl.Float64,
                "valor_estoque": pl.Float64,
            }
        )

    preco_col = (
        "preco_item"
        if "preco_item" in df.columns
        else ("Vl_item" if "Vl_item" in df.columns else None)
    )
    if preco_col is None:
        df = df.with_columns(pl.lit(0.0).alias("preco_item"))
        preco_col = "preco_item"

    for col in ["q_conv", "__q_conv_sinal__", "ordem_operacoes", "periodo_inventario"]:
        if col not in df.columns:
            if col == "ordem_operacoes":
                df = df.with_row_index("ordem_operacoes", offset=1)
            elif col == "periodo_inventario":
                df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))
            else:
                df = df.with_columns(pl.lit(0.0).alias(col))

    for col in ["dev_simples", "dev_venda", "dev_compra", "dev_ent_simples", "finnfe"]:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))

    for col in ["Cod_item", "Descr_item", "fonte", "id_agrupado", "descr_padrao"]:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))

    data_efetiva = pl.coalesce(
        [
            pl.col("Dt_e_s").cast(pl.Date, strict=False),
            pl.col("Dt_doc").cast(pl.Date, strict=False),
        ]
    )

    is_entrada = pl.col("Tipo_operacao").cast(pl.Utf8, strict=False).str.starts_with("1 - ENTRADA")
    is_saida = pl.col("Tipo_operacao").cast(pl.Utf8, strict=False).str.starts_with("2 - SAIDA")
    is_devolucao = (
        boolish_expr("dev_simples")
        | boolish_expr("dev_venda")
        | boolish_expr("dev_compra")
        | boolish_expr("dev_ent_simples")
        | _finnfe_4_expr()
    ).fill_null(False)
    is_excluida = (
        boolish_expr("excluir_estoque").fill_null(False)
        if "excluir_estoque" in df.columns
        else pl.lit(False)
    )
    is_q_conv_positiva = pl.col("q_conv").cast(pl.Float64, strict=False).fill_null(0.0) > 0
    is_valida_media = ~is_devolucao & ~is_excluida & is_q_conv_positiva

    df_base = df.with_columns(
        [
            data_efetiva.alias("__data_efetiva__"),
            data_efetiva.dt.year().cast(pl.Int32).alias("ano"),
            data_efetiva.dt.month().cast(pl.Int32).alias("mes"),
            data_efetiva.dt.year().cast(pl.Int32).alias("__ano__"),
            pl.col(preco_col).cast(pl.Float64, strict=False).fill_null(0.0).alias("__preco_calc__"),
            pl.col("q_conv").cast(pl.Float64, strict=False).fill_null(0.0).alias("__q_conv_calc__"),
            is_entrada.alias("__is_entrada__"),
            is_saida.alias("__is_saida__"),
            is_devolucao.alias("__is_devolucao__"),
            is_excluida.alias("__is_excluida__"),
            is_valida_media.alias("__is_valida_media__"),
        ]
    ).filter(
        pl.col("__data_efetiva__").is_not_null()
        & pl.col("Cod_item").is_not_null()
        & (pl.col("Cod_item") != "")
    )

    if df_base.is_empty():
        return pl.DataFrame(
            schema={
                "Cod_item": pl.Utf8,
                "Descr_item": pl.Utf8,
                "fonte": pl.Utf8,
                "id_agrupado": pl.Utf8,
                "descr_padrao": pl.Utf8,
                "ano": pl.Int32,
                "mes": pl.Int32,
                "valor_entradas": pl.Float64,
                "qtd_entradas": pl.Float64,
                "pme_mes": pl.Float64,
                "valor_saidas": pl.Float64,
                "qtd_saidas": pl.Float64,
                "pms_mes": pl.Float64,
                "saldo_mes": pl.Float64,
                "custo_medio_mes": pl.Float64,
                "valor_estoque": pl.Float64,
            }
        )

    rprint("[cyan]Calculando saldos por Cod_item (anual)...[/cyan]")
    df_com_saldos_anual = (
        df_base.sort(["Cod_item", "fonte", "__ano__", "ordem_operacoes"], nulls_last=True)
        .group_by(["Cod_item", "fonte", "__ano__"], maintain_order=True)
        .map_groups(lambda g: _calc_saldos_loop(g, "anual"))
    )

    rprint("[cyan]Calculando saldos por Cod_item (periodo)...[/cyan]")
    df_com_saldos = (
        df_com_saldos_anual.sort(
            ["Cod_item", "fonte", "periodo_inventario", "ordem_operacoes"], nulls_last=True
        )
        .group_by(["Cod_item", "fonte", "periodo_inventario"], maintain_order=True)
        .map_groups(lambda g: _calc_saldos_loop(g, "periodo"))
    )

    rprint("[cyan]Agregando mensalmente por Cod_item...[/cyan]")
    agrupado = (
        df_com_saldos.sort(["Cod_item", "fonte", "ano", "mes", "ordem_operacoes"], nulls_last=True)
        .group_by(["Cod_item", "fonte", "ano", "mes"])
        .agg(
            [
                pl.col("Descr_item").drop_nulls().last().alias("Descr_item"),
                pl.col("id_agrupado").drop_nulls().last().alias("id_agrupado"),
                pl.col("descr_padrao").drop_nulls().last().alias("descr_padrao"),
                pl.when(pl.col("__is_entrada__"))
                .then(pl.col("__preco_calc__"))
                .otherwise(0.0)
                .sum()
                .alias("valor_entradas"),
                pl.when(pl.col("__is_entrada__"))
                .then(pl.col("__q_conv_calc__"))
                .otherwise(0.0)
                .sum()
                .alias("qtd_entradas"),
                pl.when(pl.col("__is_saida__"))
                .then(pl.col("__preco_calc__").abs())
                .otherwise(0.0)
                .sum()
                .alias("valor_saidas"),
                pl.when(pl.col("__is_saida__"))
                .then(pl.col("__q_conv_calc__").abs())
                .otherwise(0.0)
                .sum()
                .alias("qtd_saidas"),
                pl.when(pl.col("__is_entrada__") & pl.col("__is_valida_media__"))
                .then(pl.col("__preco_calc__"))
                .otherwise(0.0)
                .sum()
                .alias("__soma_valor_ent_val__"),
                pl.when(pl.col("__is_entrada__") & pl.col("__is_valida_media__"))
                .then(pl.col("__q_conv_calc__"))
                .otherwise(0.0)
                .sum()
                .alias("__soma_qtd_ent_val__"),
                pl.when(pl.col("__is_saida__") & pl.col("__is_valida_media__"))
                .then(pl.col("__preco_calc__").abs())
                .otherwise(0.0)
                .sum()
                .alias("__soma_valor_sai_val__"),
                pl.when(pl.col("__is_saida__") & pl.col("__is_valida_media__"))
                .then(pl.col("__q_conv_calc__").abs())
                .otherwise(0.0)
                .sum()
                .alias("__soma_qtd_sai_val__"),
                pl.col("saldo_estoque_anual").last().alias("saldo_mes"),
                pl.col("custo_medio_anual").last().alias("custo_medio_mes"),
                pl.col("saldo_estoque_periodo").last().alias("saldo_mes_periodo"),
                pl.col("custo_medio_periodo").last().alias("custo_medio_mes_periodo"),
            ]
        )
        .with_columns(
            [
                pl.when(pl.col("__soma_qtd_ent_val__") > 0)
                .then(pl.col("__soma_valor_ent_val__") / pl.col("__soma_qtd_ent_val__"))
                .otherwise(0.0)
                .alias("pme_mes"),
                pl.when(pl.col("__soma_qtd_sai_val__") > 0)
                .then(pl.col("__soma_valor_sai_val__") / pl.col("__soma_qtd_sai_val__"))
                .otherwise(0.0)
                .alias("pms_mes"),
            ]
        )
        .with_columns(
            [
                (
                    pl.col("saldo_mes").cast(pl.Float64, strict=False).fill_null(0.0)
                    * pl.col("custo_medio_mes").cast(pl.Float64, strict=False).fill_null(0.0)
                ).alias("valor_estoque"),
                (
                    pl.col("saldo_mes_periodo").cast(pl.Float64, strict=False).fill_null(0.0)
                    * pl.col("custo_medio_mes_periodo")
                    .cast(pl.Float64, strict=False)
                    .fill_null(0.0)
                ).alias("valor_estoque_periodo"),
                pl.col("valor_entradas").round(2),
                pl.col("valor_saidas").round(2),
                pl.col("qtd_entradas").round(4),
                pl.col("qtd_saidas").round(4),
                pl.col("pme_mes").round(4),
                pl.col("pms_mes").round(4),
                pl.col("saldo_mes").round(4),
                pl.col("custo_medio_mes").round(4),
                pl.col("saldo_mes_periodo").round(4),
                pl.col("custo_medio_mes_periodo").round(4),
            ]
        )
        .with_columns(
            [
                pl.col("valor_estoque").round(2),
                pl.col("valor_estoque_periodo").round(2),
            ]
        )
        .select(
            [
                "Cod_item",
                "Descr_item",
                "fonte",
                "id_agrupado",
                "descr_padrao",
                "ano",
                "mes",
                "valor_entradas",
                "qtd_entradas",
                "pme_mes",
                "valor_saidas",
                "qtd_saidas",
                "pms_mes",
                "saldo_mes",
                "custo_medio_mes",
                "valor_estoque",
                "saldo_mes_periodo",
                "custo_medio_mes_periodo",
                "valor_estoque_periodo",
            ]
        )
        .sort(["ano", "mes", "Cod_item", "fonte"])
    )

    return agrupado


def gerar_calculos_codigo_original(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    inicio_total = perf_counter()
    cnpj = re.sub(r"\D", "", cnpj)
    if pasta_cnpj is None:
        pasta_cnpj = CNPJ_ROOT / cnpj

    pasta_analises = pasta_cnpj / "analises" / "produtos"
    if not pasta_analises.exists():
        rprint(f"[red]Pasta de analises nao encontrada para o CNPJ: {cnpj}[/red]")
        return False

    arq_mov_estoque = pasta_analises / f"mov_estoque_{cnpj}.parquet"
    if not arq_mov_estoque.exists():
        rprint(f"[red]Arquivo necessario nao encontrado:[/red] {arq_mov_estoque}")
        return False

    rprint(f"\n[bold cyan]Gerando calculos_codigo_original para CNPJ: {cnpj}[/bold cyan]")
    df = pl.read_parquet(arq_mov_estoque)

    inicio_calculo = perf_counter()
    df_result = calcular_aba_codigo_original_dataframe(df)
    registrar_evento_performance(
        "calculos_codigo_original.calcular_dataframe",
        perf_counter() - inicio_calculo,
        {
            "cnpj": cnpj,
            "linhas_saida": df_result.height,
            "colunas_saida": df_result.width,
        },
    )

    saida = pasta_analises / f"aba_codigo_original_{cnpj}.parquet"
    inicio_gravacao = perf_counter()
    ok = salvar_para_parquet(df_result, pasta_analises, saida.name)
    registrar_evento_performance(
        "calculos_codigo_original.gravar_parquet",
        perf_counter() - inicio_gravacao,
        {"cnpj": cnpj, "arquivo_saida": str(saida), "sucesso": bool(ok)},
        status="ok" if ok else "error",
    )
    if ok:
        rprint(
            f"[green]Sucesso! {df_result.height} registros salvos em aba_codigo_original.[/green]"
        )
    registrar_evento_performance(
        "calculos_codigo_original.total",
        perf_counter() - inicio_total,
        {"cnpj": cnpj, "linhas_saida": df_result.height, "sucesso": bool(ok)},
        status="ok" if ok else "error",
    )
    return ok


if __name__ == "__main__":
    try:
        if len(sys.argv) > 1:
            gerar_calculos_codigo_original(sys.argv[1])
        else:
            c = input("CNPJ: ")
            gerar_calculos_codigo_original(c)
    except Exception as e:
        import structlog

        structlog.get_logger(__name__).error(
            "Erro na geracao de calculos_codigo_original", exc_info=e
        )
        raise
