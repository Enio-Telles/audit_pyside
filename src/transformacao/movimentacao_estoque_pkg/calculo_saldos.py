"""
calculo_saldos.py

Logica de geração de eventos de estoque (ESTOQUE INICIAL/FINAL) e
calculo sequencial de saldo por grupo (id_agrupado, ano).

Extraido de movimentacao_estoque.py para melhorar modularidade.
"""

from __future__ import annotations

import numpy as np
import polars as pl
from rich import print as rprint


def _padronizar_tipo_operacao_expr(col: str = "Tipo_operacao") -> pl.Expr:
    """Normaliza valores de Tipo_operacao para os 4 tipos canonicos."""
    valor = (
        pl.col(col)
        .cast(pl.Utf8, strict=False)
        .fill_null("")
        .str.strip_chars()
        .str.to_uppercase()
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


def _valor_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    texto = str(value).strip().upper()
    return texto in {"1", "TRUE", "T", "S", "SIM", "Y", "YES"}


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
    """Gera linhas de ESTOQUE INICIAL e ESTOQUE FINAL para cada id_agrupado/ano.

    - Inventarios existentes viram ESTOQUE FINAL.
    - Anos sem inventario 31/12 recebem ESTOQUE FINAL gerado (qtd=0).
    - Cada ESTOQUE FINAL gera um ESTOQUE INICIAL no dia seguinte.
    """
    if df_mov.is_empty() or "id_agrupado" not in df_mov.columns:
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
        ]
    )

    # Estoque vindo do bloco H vira sempre ESTOQUE FINAL existente.
    df_exist_final = (
        df_base
        .filter(pl.col("__tipo_op__") == "inventario")
        .with_columns(
            [
                pl.lit("3 - ESTOQUE FINAL").alias("Tipo_operacao"),
                pl.col("__data_ref__").cast(dt_doc_dtype, strict=False).alias("Dt_doc"),
                pl.col("__data_ref__").cast(dt_es_dtype, strict=False).alias("Dt_e_s"),
            ]
        )
    )

    rprint("[cyan]Ajustando eventos de estoque...[/cyan]")

    produtos_unicos = (
        df_base
        .filter(pl.col("id_agrupado").is_not_null())
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
        df_base
        .filter(
            pl.col("id_agrupado").is_not_null()
            & pl.col("__data_ref__").is_not_null()
            & (pl.col("__tipo_op__") != "inventario")
        )
        .with_columns(pl.col("__data_ref__").dt.year().alias("__ano__"))
        .select(["id_agrupado", "__ano__"])
        .unique()
    )

    movimentos_31_12 = (
        df_base
        .filter(
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
    if pares_sem_31_12.height > 0:
        df_gerado_final = (
            pares_sem_31_12
            .join(produtos_unicos, on="id_agrupado", how="left")
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
            df_gerado_final.select(df_base.columns) if not df_gerado_final.is_empty() else pl.DataFrame(schema=df_base.schema),
        ],
        how="vertical_relaxed",
    )

    df_iniciais = pl.DataFrame(schema=df_base.schema)
    if not df_finais.is_empty():
        df_iniciais = (
            df_finais
            .with_columns(
                [
                    (
                        pl.col("__data_ref__").cast(pl.Date, strict=False) + pl.duration(days=1)
                    ).alias("__data_inicial__"),
                ]
            )
            .with_columns(
                [
                    pl.when(pl.col("Tipo_operacao") == "3 - ESTOQUE FINAL")
                    .then(pl.lit("0 - ESTOQUE INICIAL"))
                    .otherwise(pl.lit("0 - ESTOQUE INICIAL gerado"))
                    .alias("Tipo_operacao"),
                    pl.col("__data_inicial__").cast(dt_doc_dtype, strict=False).alias("Dt_doc"),
                    pl.col("__data_inicial__").cast(dt_es_dtype, strict=False).alias("Dt_e_s"),
                    pl.lit("gerado").alias("fonte"),
                ]
            )
        )

    iniciais_deriv_01_01 = (
        df_iniciais
        .with_columns(
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
        df_base
        .filter(
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

    if pares_sem_01_01.height > 0:
        df_gerado_inicial = (
            pares_sem_01_01
            .join(produtos_unicos, on="id_agrupado", how="left")
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
        df_iniciais = pl.concat([df_iniciais.select(df_base.columns), df_gerado_inicial.select(df_base.columns)], how="vertical_relaxed")

    df_sem_inventario = df_base.filter(pl.col("__tipo_op__") != "inventario")
    df_result = pl.concat(
        [
            df_sem_inventario.select(df_base.columns),
            df_finais.select(df_base.columns) if not df_finais.is_empty() else pl.DataFrame(schema=df_base.schema),
            df_iniciais.select(df_base.columns) if not df_iniciais.is_empty() else pl.DataFrame(schema=df_base.schema),
        ],
        how="vertical_relaxed",
    )

    return df_result.drop(["__data_ref__", "__tipo_op__"], strict=False)


def calcular_saldo_estoque_anual(df: pl.DataFrame) -> pl.DataFrame:
    """Calcula saldo de estoque sequencial por grupo (id_agrupado, ano).

    Usa arrays NumPy em vez de to_dicts() para ~3-5x de speedup.
    A logica sequencial (saldo depende da linha anterior) impede window functions
    puras, mas o acesso direto a arrays evita overhead de dicts Python.
    """
    if df.is_empty():
        return df

    n = df.height

    # Extrair colunas como arrays (evita to_dicts overhead)
    tipos = df["Tipo_operacao"].cast(pl.Utf8, strict=False).fill_null("").to_list()
    q_conv_arr = df["q_conv"].cast(pl.Float64, strict=False).fill_null(0.0).to_numpy()
    q_sinal_arr = df["__q_conv_sinal__"].cast(pl.Float64, strict=False).fill_null(0.0).to_numpy()
    preco_arr = df["preco_item"].cast(pl.Float64, strict=False).fill_null(0.0).to_numpy()
    qtd_decl_arr = df["__qtd_decl_final_audit__"].cast(pl.Float64, strict=False).fill_null(0.0).to_numpy()
    finnfe_arr = (
        df["finnfe"].cast(pl.Utf8, strict=False).fill_null("").to_list()
        if "finnfe" in df.columns
        else [""] * n
    )
    dev_simples_arr = (
        df["dev_simples"].to_list()
        if "dev_simples" in df.columns
        else [False] * n
    )
    dev_venda_arr = (
        df["dev_venda"].to_list()
        if "dev_venda" in df.columns
        else [False] * n
    )
    dev_compra_arr = (
        df["dev_compra"].to_list()
        if "dev_compra" in df.columns
        else [False] * n
    )
    dev_ent_simples_arr = (
        df["dev_ent_simples"].to_list()
        if "dev_ent_simples" in df.columns
        else [False] * n
    )

    saldos = np.empty(n, dtype=np.float64)
    entradas_desacob = np.empty(n, dtype=np.float64)
    custos = np.empty(n, dtype=np.float64)

    saldo_qtd = 0.0
    saldo_valor = 0.0
    custo_medio = 0.0

    for i in range(n):
        tipo = tipos[i]
        q_conv = q_conv_arr[i]
        q_sinal = q_sinal_arr[i]
        preco_item = preco_arr[i]
        qtd_decl_final = qtd_decl_arr[i]
        finnfe = str(finnfe_arr[i]).strip()
        is_devolucao = (
            finnfe == "4"
            or _valor_bool(dev_simples_arr[i])
            or _valor_bool(dev_venda_arr[i])
            or _valor_bool(dev_compra_arr[i])
            or _valor_bool(dev_ent_simples_arr[i])
        )

        entr_desac = 0.0

        if tipo[:1] == "0" or tipo == "1 - ENTRADA":
            # ESTOQUE INICIAL ou ENTRADA
            if q_sinal > 0:
                saldo_qtd += q_sinal
                if tipo == "1 - ENTRADA" and is_devolucao:
                    # Devolucoes retornam quantidade sem alterar o custo medio vigente.
                    saldo_valor += q_sinal * custo_medio
                else:
                    saldo_valor += preco_item
                custo_medio = (saldo_valor / saldo_qtd) if saldo_qtd > 0 else 0.0

        elif tipo == "2 - SAIDAS":
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

        elif tipo[:1] == "3":
            # ESTOQUE FINAL apenas audita a quantidade declarada em
            # __qtd_decl_final_audit__. A regra de negocio vigente exige
            # que essa linha permaneça neutra para entradas desacobertadas,
            # saldo fisico e custo medio.
            pass

        saldos[i] = round(saldo_qtd, 6)
        entradas_desacob[i] = round(entr_desac, 6)
        custos[i] = round(custo_medio, 6)

    return df.with_columns(
        [
            pl.Series("saldo_estoque_anual", saldos),
            pl.Series("entr_desac_anual", entradas_desacob),
            pl.Series("custo_medio_anual", custos),
        ]
    )
