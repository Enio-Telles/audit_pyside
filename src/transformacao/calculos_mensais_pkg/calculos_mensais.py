import sys
import re
from datetime import date
from pathlib import Path
from utilitarios.project_paths import PROJECT_ROOT, TRACEBACK_PATH
from time import perf_counter

import polars as pl
from rich import print as rprint

ROOT_DIR = PROJECT_ROOT
SRC_DIR = ROOT_DIR / "src"
DADOS_DIR = ROOT_DIR / "dados"
CNPJ_ROOT = DADOS_DIR / "CNPJ"


try:
    from utilitarios.salvar_para_parquet import salvar_para_parquet
    from utilitarios.perf_monitor import registrar_evento_performance
except ImportError as e:
    rprint(f"[red]Erro ao importar modulos:[/red] {e}")
    sys.exit(1)


def _boolish_expr(col_name: str) -> pl.Expr:
    texto = (
        pl.col(col_name)
        .cast(pl.Utf8, strict=False)
        .fill_null("")
        .str.strip_chars()
        .str.to_lowercase()
    )
    return texto.is_in(["true", "1", "s", "sim", "y", "yes"])


def _finnfe_4_expr() -> pl.Expr:
    return (
        pl.col("finnfe")
        .cast(pl.Utf8, strict=False)
        .fill_null("")
        .str.strip_chars()
        == "4"
    )


def _resolver_ref(nome_arquivo: str) -> Path | None:
    candidatos = [
        DADOS_DIR / "referencias" / "referencias" / "CO_SEFIN" / nome_arquivo,
        DADOS_DIR / "referencias" / "CO_SEFIN" / nome_arquivo,
        DADOS_DIR / "referencias" / nome_arquivo,
    ]
    for caminho in candidatos:
        if caminho.exists():
            return caminho
    return None


def _format_st_periodos_mensais(registros) -> str:
    if registros is None:
        return ""
    if isinstance(registros, pl.Series):
        registros = registros.to_list()
    if not registros:
        return ""

    periodos = []
    for registro in registros:
        if not registro:
            continue
        status = str(registro.get("it_in_st") or "").strip()
        dt_ini = registro.get("vig_ini")
        dt_fim = registro.get("vig_fim")
        if not status or dt_ini is None or dt_fim is None:
            continue
        periodos.append((status, dt_ini, dt_fim))

    if not periodos:
        return ""

    periodos.sort(key=lambda item: (item[1], item[2], item[0]))
    return ";".join(
        f"['{status}' de {dt_ini.strftime('%d/%m/%Y')} ate {dt_fim.strftime('%d/%m/%Y')}]"
        for status, dt_ini, dt_fim in periodos
    )


def _carregar_referencia_st_mensal(df_base: pl.DataFrame, df_aux_st: pl.DataFrame | None = None) -> pl.DataFrame:
    df_chaves = (
        df_base
        .select(["id_agrupado", "co_sefin_agr", "ano", "mes"])
        .drop_nulls("co_sefin_agr")
        .unique()
        .with_columns(
            [
                pl.col("co_sefin_agr").cast(pl.Utf8, strict=False),
                pl.col("ano").cast(pl.Int32, strict=False),
                pl.col("mes").cast(pl.Int32, strict=False),
            ]
        )
        .with_columns(
            [
                pl.struct(["ano", "mes"]).map_elements(
                    lambda registro: date(int(registro["ano"]), int(registro["mes"]), 1),
                    return_dtype=pl.Date,
                ).alias("__mes_ini__"),
                pl.struct(["ano", "mes"]).map_elements(
                    lambda registro: (
                        date(int(registro["ano"]) + 1, 1, 1)
                        if int(registro["mes"]) == 12
                        else date(int(registro["ano"]), int(registro["mes"]) + 1, 1)
                    ),
                    return_dtype=pl.Date,
                ).alias("__prox_mes_ini__"),
            ]
        )
        .with_columns(
            (pl.col("__prox_mes_ini__") - pl.duration(days=1)).alias("__mes_fim__")
        )
    )
    if df_chaves.is_empty():
        return pl.DataFrame(
            schema={
                "id_agrupado": pl.Utf8,
                "ano": pl.Int32,
                "mes": pl.Int32,
                "ST": pl.Utf8,
                "__tem_st_mes__": pl.Boolean,
            }
        )

    if df_aux_st is None:
        caminho_aux = _resolver_ref("sitafe_produto_sefin_aux.parquet")
        if caminho_aux is None:
            return pl.DataFrame(
                schema={
                    "id_agrupado": pl.Utf8,
                    "ano": pl.Int32,
                    "mes": pl.Int32,
                    "ST": pl.Utf8,
                    "__tem_st_mes__": pl.Boolean,
                }
            )
        df_aux_st = pl.read_parquet(caminho_aux)

    df_aux = (
        df_aux_st
        .select(["it_co_sefin", "it_da_inicio", "it_da_final", "it_in_st"])
        .with_columns(
            [
                pl.col("it_co_sefin").cast(pl.Utf8, strict=False).alias("it_co_sefin"),
                pl.col("it_da_inicio").cast(pl.Utf8, strict=False).str.strptime(pl.Date, "%Y%m%d", strict=False).alias("da_inicio"),
                pl.when(
                    pl.col("it_da_final").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars() == ""
                )
                .then(pl.lit(None, dtype=pl.Date))
                .otherwise(
                    pl.col("it_da_final").cast(pl.Utf8, strict=False).str.strptime(pl.Date, "%Y%m%d", strict=False)
                )
                .alias("da_final"),
                pl.col("it_in_st").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars().str.to_uppercase().alias("it_in_st"),
            ]
        )
    )

    return (
        df_chaves
        .join(df_aux, left_on="co_sefin_agr", right_on="it_co_sefin", how="left")
        .filter(
            (pl.col("da_inicio").is_null() | (pl.col("da_inicio") <= pl.col("__mes_fim__")))
            & (pl.col("da_final").is_null() | (pl.col("da_final") >= pl.col("__mes_ini__")))
        )
        .with_columns(
            [
                pl.max_horizontal([pl.col("da_inicio"), pl.col("__mes_ini__")]).alias("vig_ini"),
                pl.min_horizontal(
                    [
                        pl.coalesce([pl.col("da_final"), pl.col("__mes_fim__")]),
                        pl.col("__mes_fim__"),
                    ]
                ).alias("vig_fim"),
                pl.col("da_inicio").is_not_null().alias("__tem_inicio__"),
                pl.col("da_final").is_not_null().alias("__tem_final__"),
            ]
        )
        .sort(
            ["id_agrupado", "ano", "mes", "__tem_inicio__", "vig_ini", "__tem_final__", "vig_fim"],
            descending=[False, False, False, True, True, True, True],
            nulls_last=True,
        )
        .group_by(["id_agrupado", "ano", "mes"])
        .agg(
            [
                pl.struct(["it_in_st", "vig_ini", "vig_fim"]).alias("__st_registros__"),
                (pl.col("it_in_st") == "S").any().alias("__tem_st_mes__"),
            ]
        )
        .with_columns(
            pl.col("__st_registros__").map_elements(_format_st_periodos_mensais, return_dtype=pl.Utf8).alias("ST")
        )
        .select(["id_agrupado", "ano", "mes", "ST", "__tem_st_mes__"])
    )


def _calc_mva_expr(
    col_mva_pct: str = "__mva_pct_mes__",
    col_mva_ajustada: str = "__mva_ajustado__",
    col_aliq_inter: str = "__aliq_inter_mes__",
    col_aliq_interna: str = "__aliq_mes__",
) -> pl.Expr:
    mva_pct = pl.col(col_mva_pct).cast(pl.Float64, strict=False).fill_null(0.0) / 100.0
    aliq_inter = pl.col(col_aliq_inter).cast(pl.Float64, strict=False).fill_null(0.0) / 100.0
    aliq_interna = pl.col(col_aliq_interna).cast(pl.Float64, strict=False).fill_null(0.0) / 100.0
    mva_ajustada = (
        (((1.0 + mva_pct) * (1.0 - aliq_inter)) / (1.0 - aliq_interna)) - 1.0
    )

    return (
        pl.when(pl.col(col_mva_ajustada).cast(pl.Utf8, strict=False).fill_null("").str.strip_chars().str.to_uppercase() == "S")
        .then(mva_ajustada)
        .otherwise(mva_pct)
    )


def calcular_aba_mensal_dataframe(df: pl.DataFrame, df_aux_st: pl.DataFrame | None = None) -> pl.DataFrame:
    if df.is_empty():
        return pl.DataFrame(
            schema={
                "ano": pl.Int32,
                "mes": pl.Int32,
                "id_agregado": pl.Utf8,
                "descr_padrao": pl.Utf8,
                "unids_mes": pl.List(pl.Utf8),
                "unids_ref_mes": pl.List(pl.Utf8),
                "ST": pl.Utf8,
                "it_in_st": pl.Utf8,
                "valor_entradas": pl.Float64,
                "qtd_entradas": pl.Float64,
                "pme_mes": pl.Float64,
                "valor_saidas": pl.Float64,
                "qtd_saidas": pl.Float64,
                "pms_mes": pl.Float64,
                "MVA": pl.Float64,
                "MVA_ajustado": pl.Float64,
                "entradas_desacob": pl.Float64,
                "ICMS_entr_desacob": pl.Float64,
                "saldo_mes": pl.Float64,
                "custo_medio_mes": pl.Float64,
                "valor_estoque": pl.Float64,
            }
        )

    preco_col = "preco_item" if "preco_item" in df.columns else ("Vl_item" if "Vl_item" in df.columns else None)
    if preco_col is None:
        df = df.with_columns(pl.lit(0.0).alias("preco_item"))
        preco_col = "preco_item"

    expected_cols = [
        "q_conv",
        "entr_desac_anual",
        "saldo_estoque_anual",
        "custo_medio_anual",
        "Aliq_icms",
        "it_pc_interna",
        "it_pc_mva",
        "it_in_st",
        "it_in_mva_ajustado",
        "excluir_estoque",
        "dev_simples",
        "dev_venda",
        "dev_compra",
        "dev_ent_simples",
        "Unid",
        "unid_ref",
        "ordem_operacoes",
        "co_sefin_agr",
    ]
    for col in expected_cols:
        if col not in df.columns:
            if col in {"it_in_st", "Unid", "unid_ref"}:
                df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))
            elif col == "ordem_operacoes":
                df = df.with_row_index("ordem_operacoes", offset=1)
            else:
                df = df.with_columns(pl.lit(0.0).alias(col))

    data_efetiva = pl.coalesce(
        [
            pl.col("Dt_e_s").cast(pl.Date, strict=False),
            pl.col("Dt_doc").cast(pl.Date, strict=False),
        ]
    )

    is_entrada = pl.col("Tipo_operacao").cast(pl.Utf8, strict=False).str.starts_with("1 - ENTRADA")
    is_saida = pl.col("Tipo_operacao").cast(pl.Utf8, strict=False).str.starts_with("2 - SAIDA")
    is_devolucao = (
        _boolish_expr("dev_simples")
        | _boolish_expr("dev_venda")
        | _boolish_expr("dev_compra")
        | _boolish_expr("dev_ent_simples")
        | _finnfe_4_expr()
    ).fill_null(False)
    is_excluida = _boolish_expr("excluir_estoque").fill_null(False)
    is_q_conv_positiva = pl.col("q_conv").cast(pl.Float64, strict=False).fill_null(0.0) > 0
    is_valida_media = ~is_devolucao & ~is_excluida & is_q_conv_positiva

    df_base = (
        df.with_columns(
            [
                data_efetiva.alias("__data_efetiva__"),
                data_efetiva.dt.year().alias("ano"),
                data_efetiva.dt.month().alias("mes"),
                pl.col(preco_col).cast(pl.Float64, strict=False).fill_null(0.0).alias("__preco_calc__"),
                pl.col("q_conv").cast(pl.Float64, strict=False).fill_null(0.0).alias("__q_conv_calc__"),
                pl.col("entr_desac_anual").cast(pl.Float64, strict=False).fill_null(0.0).alias("__entr_desac_calc__"),
                pl.col("saldo_estoque_anual").cast(pl.Float64, strict=False).fill_null(0.0).alias("__saldo_calc__"),
                pl.col("custo_medio_anual").cast(pl.Float64, strict=False).fill_null(0.0).alias("__custo_calc__"),
                pl.col("Aliq_icms").cast(pl.Float64, strict=False).fill_null(0.0).alias("__aliq_inter_calc__"),
                pl.col("it_pc_interna").cast(pl.Float64, strict=False).fill_null(0.0).alias("__aliq_mes_calc__"),
                pl.col("it_pc_mva").cast(pl.Float64, strict=False).fill_null(0.0).alias("__mva_pct_calc__"),
                is_entrada.alias("__is_entrada__"),
                is_saida.alias("__is_saida__"),
                is_devolucao.alias("__is_devolucao__"),
                is_excluida.alias("__is_excluida__"),
                is_valida_media.alias("__is_valida_media__"),
            ]
        )
        .filter(pl.col("__data_efetiva__").is_not_null())
        .sort(["id_agrupado", "ano", "mes", "ordem_operacoes"], nulls_last=True)
    )

    df_st_mensal = _carregar_referencia_st_mensal(df_base, df_aux_st=df_aux_st)

    agrupado = (
        df_base.group_by(["id_agrupado", "ano", "mes"])
        .agg(
            [
                pl.col("descr_padrao").drop_nulls().last().alias("descr_padrao"),
                pl.col("Unid").cast(pl.Utf8, strict=False).drop_nulls().unique().sort().alias("unids_mes"),
                pl.col("unid_ref").cast(pl.Utf8, strict=False).drop_nulls().unique().sort().alias("unids_ref_mes"),
                pl.col("it_in_st").cast(pl.Utf8, strict=False).drop_nulls().last().alias("it_in_st"),
                pl.when(pl.col("__is_entrada__")).then(pl.col("__preco_calc__")).otherwise(0.0).sum().alias("valor_entradas"),
                pl.when(pl.col("__is_entrada__")).then(pl.col("__q_conv_calc__")).otherwise(0.0).sum().alias("qtd_entradas"),
                pl.when(pl.col("__is_saida__")).then(pl.col("__preco_calc__").abs()).otherwise(0.0).sum().alias("valor_saidas"),
                pl.when(pl.col("__is_saida__")).then(pl.col("__q_conv_calc__").abs()).otherwise(0.0).sum().alias("qtd_saidas"),
                pl.when(pl.col("__is_entrada__") & pl.col("__is_valida_media__")).then(pl.col("__preco_calc__")).otherwise(0.0).sum().alias("__soma_valor_entradas_validas__"),
                pl.when(pl.col("__is_entrada__") & pl.col("__is_valida_media__")).then(pl.col("__q_conv_calc__")).otherwise(0.0).sum().alias("__soma_qtd_entradas_validas__"),
                pl.when(pl.col("__is_saida__") & pl.col("__is_valida_media__")).then(pl.col("__preco_calc__").abs()).otherwise(0.0).sum().alias("__soma_valor_saidas_validas__"),
                pl.when(pl.col("__is_saida__") & pl.col("__is_valida_media__")).then(pl.col("__q_conv_calc__").abs()).otherwise(0.0).sum().alias("__soma_qtd_saidas_validas__"),
                pl.col("__entr_desac_calc__").sum().alias("entradas_desacob"),
                pl.col("__saldo_calc__").last().alias("saldo_mes"),
                pl.col("__custo_calc__").last().alias("custo_medio_mes"),
                pl.col("__aliq_mes_calc__").drop_nulls().last().alias("__aliq_mes__"),
                pl.col("__aliq_inter_calc__").drop_nulls().last().alias("__aliq_inter_mes__"),
                pl.col("__mva_pct_calc__").drop_nulls().last().alias("__mva_pct_mes__"),
                pl.col("it_in_mva_ajustado").cast(pl.Utf8, strict=False).drop_nulls().last().alias("__mva_ajustado__"),
            ]
        )
        .join(df_st_mensal, on=["id_agrupado", "ano", "mes"], how="left")
        .with_columns(
            [
                pl.when(pl.col("__soma_qtd_entradas_validas__") > 0)
                .then(pl.col("__soma_valor_entradas_validas__") / pl.col("__soma_qtd_entradas_validas__"))
                .otherwise(0.0)
                .alias("pme_mes"),
                pl.when(pl.col("__soma_qtd_saidas_validas__") > 0)
                .then(pl.col("__soma_valor_saidas_validas__") / pl.col("__soma_qtd_saidas_validas__"))
                .otherwise(0.0)
                .alias("pms_mes"),
                pl.col("ST").fill_null("").alias("ST"),
                pl.col("__tem_st_mes__").fill_null(False).alias("__tem_st_mes__"),
                _calc_mva_expr().alias("__mva_mes__"),
            ]
        )
        .with_columns(
            [
                pl.when(pl.col("__tem_st_mes__"))
                .then(pl.col("__mva_pct_mes__").cast(pl.Float64, strict=False).fill_null(0.0))
                .otherwise(pl.lit(None, dtype=pl.Float64))
                .alias("MVA"),
                # As medias exibidas na tabela mensal devem ser as mesmas usadas na base do ICMS,
                # evitando diferenca entre o valor mostrado e o valor efetivamente multiplicado.
                pl.col("pme_mes").round(2).alias("__pme_mes_icms__"),
                pl.col("pms_mes").round(2).alias("__pms_mes_icms__"),
                pl.when(
                    pl.col("__tem_st_mes__")
                    & (
                        pl.col("__mva_ajustado__")
                        .cast(pl.Utf8, strict=False)
                        .fill_null("")
                        .str.strip_chars()
                        .str.to_uppercase()
                        == "S"
                    )
                )
                .then(pl.col("__mva_mes__"))
                .otherwise(pl.lit(None, dtype=pl.Float64))
                .alias("MVA_ajustado"),
            ]
        )
        .with_columns(
            [
                (
                    pl.col("saldo_mes").cast(pl.Float64, strict=False).fill_null(0.0)
                    * pl.col("custo_medio_mes").cast(pl.Float64, strict=False).fill_null(0.0)
                ).alias("valor_estoque"),
                pl.when(
                    pl.col("__tem_st_mes__")
                    & (pl.col("entradas_desacob") > 0)
                )
                .then(
                    pl.when(pl.col("__pms_mes_icms__") > 0)
                    .then(
                        pl.col("__pms_mes_icms__")
                        * pl.col("entradas_desacob")
                        * (pl.col("__aliq_mes__") / 100.0)
                    )
                    .otherwise(
                        pl.col("__pme_mes_icms__")
                        * pl.col("entradas_desacob")
                        * (pl.col("__aliq_mes__") / 100.0)
                        * pl.col("__mva_mes__")
                    )
                )
                .otherwise(0.0)
                .alias("ICMS_entr_desacob")
            ]
        )
        .with_columns(
            [
                pl.col("valor_entradas").round(2),
                pl.col("valor_saidas").round(2),
                pl.col("pme_mes").round(4),
                pl.col("pms_mes").round(4),
                pl.col("MVA").round(4),
                pl.col("MVA_ajustado").round(6),
                pl.col("entradas_desacob").round(4),
                pl.col("saldo_mes").round(4),
                pl.col("custo_medio_mes").round(4),
                pl.col("valor_estoque").round(2),
                pl.col("ICMS_entr_desacob").round(2),
                pl.col("qtd_entradas").round(4),
                pl.col("qtd_saidas").round(4),
            ]
        )
        .select(
            [
                "ano",
                "mes",
                pl.col("id_agrupado").alias("id_agregado"),
                "descr_padrao",
                "unids_mes",
                "unids_ref_mes",
                "ST",
                "it_in_st",
                "valor_entradas",
                "qtd_entradas",
                "pme_mes",
                "valor_saidas",
                "qtd_saidas",
                "pms_mes",
                "MVA",
                "MVA_ajustado",
                "entradas_desacob",
                "ICMS_entr_desacob",
                "saldo_mes",
                "custo_medio_mes",
                "valor_estoque",
            ]
        )
        .sort(["ano", "mes", "id_agregado"])
    )

    return agrupado


def gerar_calculos_mensais(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
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

    rprint(f"\n[bold cyan]Gerando calculos_mensais (Aba Mensal) para CNPJ: {cnpj}[/bold cyan]")
    inicio_leitura = perf_counter()
    df = pl.read_parquet(arq_mov_estoque)
    registrar_evento_performance(
        "calculos_mensais.read_mov_estoque",
        perf_counter() - inicio_leitura,
        {"cnpj": cnpj, "linhas": df.height, "colunas": df.width},
    )

    inicio_calculo = perf_counter()
    df_result = calcular_aba_mensal_dataframe(df)
    registrar_evento_performance(
        "calculos_mensais.calcular_dataframe",
        perf_counter() - inicio_calculo,
        {"cnpj": cnpj, "linhas_saida": df_result.height, "colunas_saida": df_result.width},
    )

    saida = pasta_analises / f"aba_mensal_{cnpj}.parquet"
    inicio_gravacao = perf_counter()
    ok = salvar_para_parquet(df_result, pasta_analises, saida.name)
    registrar_evento_performance(
        "calculos_mensais.gravar_parquet",
        perf_counter() - inicio_gravacao,
        {"cnpj": cnpj, "arquivo_saida": saida, "sucesso": bool(ok)},
        status="ok" if ok else "error",
    )
    if ok:
        rprint(f"[green]Sucesso! {df_result.height} registros salvos na aba mensal.[/green]")
    registrar_evento_performance(
        "calculos_mensais.total",
        perf_counter() - inicio_total,
        {"cnpj": cnpj, "linhas_saida": df_result.height, "sucesso": bool(ok)},
        status="ok" if ok else "error",
    )
    return ok


if __name__ == "__main__":
    try:
        if len(sys.argv) > 1:
            gerar_calculos_mensais(sys.argv[1])
        else:
            c = input("CNPJ: ")
            gerar_calculos_mensais(c)
    except Exception:
        import traceback

        with open(TRACEBACK_PATH, "w", encoding="utf-8") as f:
            traceback.print_exc(file=f)
        raise


