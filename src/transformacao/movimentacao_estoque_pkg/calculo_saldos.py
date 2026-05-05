"""
calculo_saldos.py

Logica de geração de eventos de estoque (ESTOQUE INICIAL/FINAL) e
calculo sequencial de saldo por grupo (id_agrupado, ano).

Extraido de movimentacao_estoque.py para melhorar modularidade.
"""

from __future__ import annotations

import numpy as np
import polars as pl
from numba import njit
from rich import print as rprint


def _padronizar_tipo_operacao_expr(col: str = "Tipo_operacao") -> pl.Expr:
    """Normaliza valores de Tipo_operacao para os 4 tipos canonicos."""
    valor = (
        pl.col(col).cast(pl.Utf8, strict=False).fill_null("").str.strip_chars().str.to_uppercase()
    )
    return (
        pl.when(valor == "INVENTARIO")
        .then(pl.lit("inventario"))
        .when(
            (valor == "0")
            | (valor == "0 - ENTRADA")
            | (valor == "ENTRADA")
            | valor.str.contains("ENTRADA", literal=True)
        )
        .then(pl.lit("1 - ENTRADA"))
        .when(
            (valor == "1")
            | (valor == "1 - SAIDA")
            | (valor == "2 - SAIDAS")
            | (valor == "SAIDA")
            | (valor == "SAIDAS")
            | valor.str.contains("SAIDA", literal=True)
        )
        .then(pl.lit("2 - SAIDAS"))
        .otherwise(pl.col(col).cast(pl.Utf8, strict=False))
        .alias(col)
    )


def _boolish_expr(coluna: str) -> pl.Expr:
    return (
        pl.col(coluna)
        .cast(pl.Utf8, strict=False)
        .fill_null("")
        .str.strip_chars()
        .str.to_uppercase()
        .is_in(["1", "TRUE", "T", "S", "SIM", "Y", "YES", "X"])
    )


def gerar_eventos_estoque(df_mov: pl.DataFrame) -> pl.DataFrame:
    """Gera linhas de ESTOQUE INICIAL e ESTOQUE FINAL para cada id_agrupado/ano."""
    is_lazy = isinstance(df_mov, pl.LazyFrame)
    if (is_lazy and df_mov.collect_schema().names() == []) or (not is_lazy and df_mov.is_empty()):
        return df_mov

    columns = df_mov.collect_schema().names() if is_lazy else df_mov.columns
    if "id_agrupado" not in columns:
        return df_mov

    dt_doc_dtype = df_mov.schema.get("Dt_doc", pl.Datetime)
    dt_es_dtype = df_mov.schema.get("Dt_e_s", pl.Datetime)

    df_base = df_mov.with_columns(
        [
            pl.coalesce(
                [
                    pl.col("Dt_e_s").cast(pl.Date, strict=False),
                    pl.col("Dt_doc").cast(pl.Date, strict=False),
                ]
            ).alias("__data_ref__"),
            pl.col("Tipo_operacao").cast(pl.Utf8, strict=False).fill_null("").alias("__tipo_op__"),
            (
                pl.col("origem_evento_estoque").cast(pl.Utf8, strict=False)
                if "origem_evento_estoque" in columns
                else pl.lit("registro")
            )
            .fill_null("registro")
            .alias("origem_evento_estoque"),
            (
                pl.col("evento_sintetico").cast(pl.Boolean, strict=False)
                if "evento_sintetico" in columns
                else pl.lit(False)
            )
            .fill_null(False)
            .alias("evento_sintetico"),
        ]
    )

    df_exist_final = df_base.filter(pl.col("__tipo_op__") == "inventario").with_columns(
        [
            pl.lit("3 - ESTOQUE FINAL").alias("Tipo_operacao"),
            pl.col("__data_ref__").cast(dt_doc_dtype, strict=False).alias("Dt_doc"),
            pl.col("__data_ref__").cast(dt_es_dtype, strict=False).alias("Dt_e_s"),
            pl.lit("inventario_bloco_h").alias("origem_evento_estoque"),
            pl.lit(False).alias("evento_sintetico"),
        ]
    )

    rprint("[cyan]Ajustando eventos de estoque...[/cyan]")

    produtos_unicos = (
        df_base.filter(pl.col("id_agrupado").is_not_null())
        .select(
            [
                "id_agrupado",
                "ncm_padrao",
                "cest_padrao",
                "descr_padrao",
                "Cod_item",
                "Cod_barra",
                "Ncm",
                "Cest",
                "Tipo_item",
                "Descr_item",
                "Cfop",
                "co_sefin_agr",
                "unid_ref",
                "fator",
            ]
        )
        .unique(subset=["id_agrupado"])
    )

    anos_ativos = (
        df_base.filter(
            pl.col("id_agrupado").is_not_null()
            & pl.col("__data_ref__").is_not_null()
            & (pl.col("__tipo_op__") != "inventario")
        )
        .with_columns(pl.col("__data_ref__").dt.year().alias("__ano__"))
        .select(["id_agrupado", "__ano__"])
        .unique()
    )

    movimentos_31_12 = (
        df_base.filter(
            pl.col("id_agrupado").is_not_null()
            & pl.col("__data_ref__").is_not_null()
            & (pl.col("__tipo_op__") == "inventario")
        )
        .with_columns(
            [
                pl.col("__data_ref__").dt.year().alias("__ano__"),
                pl.col("__data_ref__").dt.month().alias("__mes__"),
                pl.col("__data_ref__").dt.day().alias("__dia__"),
            ]
        )
        .filter((pl.col("__mes__") == 12) & (pl.col("__dia__") == 31))
        .select(["id_agrupado", "__ano__"])
        .unique()
    )

    pares_sem_31_12 = anos_ativos.join(movimentos_31_12, on=["id_agrupado", "__ano__"], how="anti")

    df_gerado_final = pl.DataFrame()
    if isinstance(pares_sem_31_12, pl.LazyFrame):
        tem_pares = pares_sem_31_12.select(pl.len()).collect().item() > 0
    else:
        tem_pares = not pares_sem_31_12.is_empty()
    if tem_pares:
        df_gerado_final = (
            pares_sem_31_12.join(produtos_unicos, on="id_agrupado", how="left")
            .with_columns(
                [
                    pl.concat_str(
                        [
                            pl.col("__ano__").cast(pl.Utf8),
                            pl.lit("-12-31"),
                        ]
                    )
                    .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
                    .alias("__data_final__"),
                    pl.lit("3 - ESTOQUE FINAL gerado").alias("Tipo_operacao"),
                    pl.lit(0).cast(pl.Float64).alias("Qtd"),
                    pl.lit(0).cast(pl.Float64).alias("Vl_item"),
                    pl.lit(None).alias("Unid"),
                    pl.lit("gerado").alias("Ser"),
                    pl.lit("gerado").alias("fonte"),
                    pl.lit("estoque_final_gerado").alias("origem_evento_estoque"),
                    pl.lit(True).alias("evento_sintetico"),
                ]
            )
            .with_columns(
                [
                    pl.col("__data_final__").cast(dt_doc_dtype, strict=False).alias("Dt_doc"),
                    pl.col("__data_final__").cast(dt_es_dtype, strict=False).alias("Dt_e_s"),
                    pl.col("__data_final__").alias("__data_ref__"),
                    pl.lit("inventario").alias("__tipo_op__"),
                ]
            )
        )

        for c in df_base.columns:
            if c not in df_gerado_final.columns:
                df_gerado_final = df_gerado_final.with_columns(pl.lit(None).alias(c))
        df_gerado_final = df_gerado_final.select(df_base.columns)

    df_finais = pl.concat(
        [
            df_exist_final.select(df_base.columns),
            df_gerado_final.select(df_base.columns)
            if not df_gerado_final.is_empty()
            else pl.DataFrame(schema=df_base.schema),
        ],
        how="vertical_relaxed",
    )

    df_iniciais = pl.DataFrame(schema=df_base.schema)
    if not df_finais.is_empty():
        df_iniciais = df_finais.with_columns(
            [
                (pl.col("__data_ref__").cast(pl.Date, strict=False) + pl.duration(days=1)).alias(
                    "__data_inicial__"
                ),
            ]
        ).with_columns(
            [
                pl.when(pl.col("Tipo_operacao") == "3 - ESTOQUE FINAL")
                .then(pl.lit("0 - ESTOQUE INICIAL"))
                .otherwise(pl.lit("0 - ESTOQUE INICIAL gerado"))
                .alias("Tipo_operacao"),
                pl.col("__data_inicial__").cast(dt_doc_dtype, strict=False).alias("Dt_doc"),
                pl.col("__data_inicial__").cast(dt_es_dtype, strict=False).alias("Dt_e_s"),
                pl.lit("gerado").alias("fonte"),
                pl.when(pl.col("origem_evento_estoque") == "inventario_bloco_h")
                .then(pl.lit("estoque_inicial_derivado"))
                .otherwise(pl.lit("estoque_inicial_gerado"))
                .alias("origem_evento_estoque"),
                pl.lit(True).alias("evento_sintetico"),
            ]
        )

    iniciais_deriv_01_01 = (
        df_iniciais.with_columns(
            [
                pl.col("Dt_e_s").dt.year().alias("__ano__"),
                pl.col("Dt_e_s").dt.month().alias("__mes__"),
                pl.col("Dt_e_s").dt.day().alias("__dia__"),
            ]
        )
        .filter((pl.col("__mes__") == 1) & (pl.col("__dia__") == 1))
        .select(["id_agrupado", "__ano__"])
        .unique()
    )

    inv_01_01_base = (
        df_base.filter(
            pl.col("id_agrupado").is_not_null()
            & pl.col("__data_ref__").is_not_null()
            & (pl.col("__tipo_op__") == "inventario")
        )
        .with_columns(
            [
                pl.col("__data_ref__").dt.year().alias("__ano__"),
                pl.col("__data_ref__").dt.month().alias("__mes__"),
                pl.col("__data_ref__").dt.day().alias("__dia__"),
            ]
        )
        .filter((pl.col("__mes__") == 1) & (pl.col("__dia__") == 1))
        .select(["id_agrupado", "__ano__"])
        .unique()
    )

    tem_01_01 = pl.concat([iniciais_deriv_01_01, inv_01_01_base]).unique()
    pares_sem_01_01 = anos_ativos.join(tem_01_01, on=["id_agrupado", "__ano__"], how="anti")

    if isinstance(pares_sem_01_01, pl.LazyFrame):
        tem_pares_01 = pares_sem_01_01.select(pl.len()).collect().item() > 0
    else:
        tem_pares_01 = not pares_sem_01_01.is_empty()
    if tem_pares_01:
        df_gerado_inicial = (
            pares_sem_01_01.join(produtos_unicos, on="id_agrupado", how="left")
            .with_columns(
                [
                    pl.concat_str([pl.col("__ano__").cast(pl.Utf8), pl.lit("-01-01")])
                    .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
                    .alias("__data_inicial__"),
                    pl.lit("0 - ESTOQUE INICIAL gerado").alias("Tipo_operacao"),
                    pl.lit(0).cast(pl.Float64).alias("Qtd"),
                    pl.lit(0).cast(pl.Float64).alias("Vl_item"),
                    pl.lit(None).alias("Unid"),
                    pl.lit("gerado").alias("Ser"),
                    pl.lit("gerado").alias("fonte"),
                    pl.lit("estoque_inicial_gerado").alias("origem_evento_estoque"),
                    pl.lit(True).alias("evento_sintetico"),
                ]
            )
            .with_columns(
                [
                    pl.col("__data_inicial__").cast(dt_doc_dtype, strict=False).alias("Dt_doc"),
                    pl.col("__data_inicial__").cast(dt_es_dtype, strict=False).alias("Dt_e_s"),
                    pl.col("__data_inicial__").alias("__data_ref__"),
                    pl.lit("inventario").alias("__tipo_op__"),
                ]
            )
        )
        for c in df_base.columns:
            if c not in df_gerado_inicial.columns:
                df_gerado_inicial = df_gerado_inicial.with_columns(pl.lit(None).alias(c))
        df_iniciais = pl.concat(
            [df_iniciais.select(df_base.columns), df_gerado_inicial.select(df_base.columns)],
            how="vertical_relaxed",
        )

    df_sem_inventario = df_base.filter(pl.col("__tipo_op__") != "inventario").with_columns(
        [
            pl.coalesce([pl.col("origem_evento_estoque"), pl.lit("registro")]).alias(
                "origem_evento_estoque"
            ),
            pl.coalesce([pl.col("evento_sintetico"), pl.lit(False)]).alias("evento_sintetico"),
        ]
    )
    df_result = pl.concat(
        [
            df_sem_inventario.select(df_base.columns),
            df_finais.select(df_base.columns)
            if not df_finais.is_empty()
            else pl.DataFrame(schema=df_base.schema),
            df_iniciais.select(df_base.columns)
            if not df_iniciais.is_empty()
            else pl.DataFrame(schema=df_base.schema),
        ],
        how="vertical_relaxed",
    )

    return df_result.drop(["__data_ref__", "__tipo_op__"], strict=False)


@njit
def _numba_calc_saldos_core(
    tipos_int: np.ndarray,
    q_conv_arr: np.ndarray,
    q_sinal_arr: np.ndarray,
    preco_arr: np.ndarray,
    is_devolucao_arr: np.ndarray,
):
    n = len(tipos_int)
    saldos = np.zeros(n, dtype=np.float64)
    entradas_desacob = np.zeros(n, dtype=np.float64)
    custos = np.zeros(n, dtype=np.float64)

    saldo_qtd = 0.0
    saldo_valor = 0.0
    custo_medio = 0.0

    for i in range(n):
        tipo_int = tipos_int[i]
        q_conv = q_conv_arr[i]
        q_sinal = q_sinal_arr[i]
        preco_item = preco_arr[i]
        is_devolucao = is_devolucao_arr[i]

        entr_desac = 0.0

        if tipo_int == 0:
            if q_sinal > 0:
                saldo_qtd += q_sinal
                if is_devolucao:
                    saldo_valor += q_sinal * custo_medio
                else:
                    saldo_valor += preco_item
                custo_medio = (saldo_valor / saldo_qtd) if saldo_qtd > 0 else 0.0

        elif tipo_int == 1:
            if q_conv > 0:
                saldo_qtd += q_sinal
                if saldo_qtd < 0:
                    entr_desac = -saldo_qtd
                    saldo_qtd = 0.0
                    saldo_valor = 0.0
                    custo_medio = 0.0
                else:
                    saldo_valor -= q_conv * custo_medio
                    if saldo_qtd <= 0:
                        saldo_qtd = 0.0
                        saldo_valor = 0.0
                        custo_medio = 0.0
                    else:
                        if saldo_valor < 0.0:
                            saldo_valor = 0.0
                        custo_medio = saldo_valor / saldo_qtd

        elif tipo_int == 2:
            pass

        saldos[i] = round(saldo_qtd, 6)
        entradas_desacob[i] = round(entr_desac, 6)
        custos[i] = round(custo_medio, 6)

    return saldos, entradas_desacob, custos


def _calc_saldos_loop(df: pl.DataFrame, sufixo: str) -> pl.DataFrame:
    if df.is_empty():
        return df

    df_prep = df.with_columns(
        [
            pl.col("Tipo_operacao").cast(pl.Utf8, strict=False).fill_null("").alias("__tipo_str__"),
            pl.col("finnfe").cast(pl.Utf8, strict=False).fill_null("").alias("__finnfe_str__")
            if "finnfe" in df.columns
            else pl.lit("").alias("__finnfe_str__"),
        ]
    ).with_columns(
        [
            pl.when(
                pl.col("__tipo_str__").str.starts_with("0")
                | (pl.col("__tipo_str__") == "1 - ENTRADA")
            )
            .then(0)
            .when(pl.col("__tipo_str__") == "2 - SAIDAS")
            .then(1)
            .when(pl.col("__tipo_str__").str.starts_with("3"))
            .then(2)
            .otherwise(3)
            .alias("__tipo_int__"),
            (
                (pl.col("__finnfe_str__").str.strip_chars() == "4")
                | _boolish_expr("dev_simples")
                | _boolish_expr("dev_venda")
                | _boolish_expr("dev_compra")
                | _boolish_expr("dev_ent_simples")
            ).alias("__is_devolucao__"),
        ]
    )

    tipos_int = df_prep["__tipo_int__"].to_numpy().astype(np.int8)
    q_conv_arr = df_prep["q_conv"].cast(pl.Float64, strict=False).fill_null(0.0).to_numpy()
    q_sinal_arr = (
        df_prep["__q_conv_sinal__"].cast(pl.Float64, strict=False).fill_null(0.0).to_numpy()
    )
    preco_arr = df_prep["preco_item"].cast(pl.Float64, strict=False).fill_null(0.0).to_numpy()
    is_devolucao_arr = df_prep["__is_devolucao__"].to_numpy().astype(np.bool_)

    saldos, entradas_desacob, custos = _numba_calc_saldos_core(
        tipos_int, q_conv_arr, q_sinal_arr, preco_arr, is_devolucao_arr
    )

    return df.with_columns(
        [
            pl.Series(f"saldo_estoque_{sufixo}", saldos),
            pl.Series(f"entr_desac_{sufixo}", entradas_desacob),
            pl.Series(f"custo_medio_{sufixo}", custos),
        ]
    )


def calcular_saldo_estoque_anual(df: pl.DataFrame) -> pl.DataFrame:
    """Calcula o saldo de estoque acumulado no eixo anual para cada linha.

    Executa o loop sequencial de saldo usando o sufixo ``anual``, adicionando
    ao DataFrame as colunas ``saldo_estoque_anual``, ``entr_desac_anual`` e
    ``custo_medio_anual``.

    Args:
        df: DataFrame com movimentacoes ordenadas cronologicamente por produto,
            contendo pelo menos as colunas ``Tipo_operacao``, ``q_conv`` e
            ``preco_item``.

    Returns:
        DataFrame de entrada acrescido das tres colunas de saldo anual.
    """
    return _calc_saldos_loop(df, "anual")


def calcular_saldo_estoque_periodo(df: pl.DataFrame) -> pl.DataFrame:
    """Calcula o saldo de estoque acumulado no eixo de periodo (mes/EFD) para cada linha.

    Executa o loop sequencial de saldo usando o sufixo ``periodo``, adicionando
    ao DataFrame as colunas ``saldo_estoque_periodo``, ``entr_desac_periodo`` e
    ``custo_medio_periodo``.

    Args:
        df: DataFrame com movimentacoes ordenadas cronologicamente por produto
            dentro de cada periodo, contendo pelo menos as colunas
            ``Tipo_operacao``, ``q_conv`` e ``preco_item``.

    Returns:
        DataFrame de entrada acrescido das tres colunas de saldo por periodo.
    """
    return _calc_saldos_loop(df, "periodo")
