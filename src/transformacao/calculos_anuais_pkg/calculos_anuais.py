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


def _boolish_expr(col_name: str) -> pl.Expr:
    col = pl.col(col_name).cast(pl.Utf8, strict=False).fill_null("").str.strip_chars().str.to_uppercase()
    return (
        pl.when(col.is_in(["1", "TRUE", "T", "S", "SIM", "Y", "YES"]))
        .then(pl.lit(True))
        .when(col.is_in(["0", "FALSE", "F", "N", "NAO", "NÃƒO", "NO", ""]))
        .then(pl.lit(False))
        .otherwise(pl.col(col_name).cast(pl.Int64, strict=False).fill_null(0) != 0)
    )


def _format_st_periodos_anuais(registros) -> str:
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
        f"['{status}' de {dt_ini.strftime('%d/%m/%Y')} atÃ© {dt_fim.strftime('%d/%m/%Y')}]"
        for status, dt_ini, dt_fim in periodos
    )


def _carregar_referencia_st_anual(df_anual: pl.DataFrame) -> pl.DataFrame:
    caminho_aux = _resolver_ref("sitafe_produto_sefin_aux.parquet")
    if caminho_aux is None:
        return pl.DataFrame(
            schema={
                "co_sefin_agr": pl.Utf8,
                "ano": pl.Int32,
                "ST": pl.Utf8,
                "__tem_st_ano__": pl.Boolean,
                "__aliq_ref__": pl.Float64,
            }
        )

    df_chaves = (
        df_anual
        .select(["co_sefin_agr", "ano"])
        .drop_nulls("co_sefin_agr")
        .unique()
        .with_columns(
            [
                pl.col("co_sefin_agr").cast(pl.Utf8, strict=False),
                pl.col("ano").cast(pl.Int32, strict=False),
            ]
        )
        .with_columns(
            [
                pl.col("ano").map_elements(lambda ano: date(int(ano), 1, 1), return_dtype=pl.Date).alias("__ano_ini__"),
                pl.col("ano").map_elements(lambda ano: date(int(ano), 12, 31), return_dtype=pl.Date).alias("__ano_fim__"),
            ]
        )
    )
    if df_chaves.is_empty():
        return pl.DataFrame(
            schema={
                "co_sefin_agr": pl.Utf8,
                "ano": pl.Int32,
                "ST": pl.Utf8,
                "__tem_st_ano__": pl.Boolean,
                "__aliq_ref__": pl.Float64,
            }
        )

    df_aux = (
        pl.read_parquet(caminho_aux)
        .select(["it_co_sefin", "it_da_inicio", "it_da_final", "it_pc_interna", "it_in_st"])
        .with_columns(
            [
                pl.col("it_co_sefin").cast(pl.Utf8, strict=False).alias("it_co_sefin"),
                pl.col("it_da_inicio").cast(pl.Utf8, strict=False).str.strptime(pl.Date, "%Y%m%d", strict=False).alias("da_inicio"),
                pl.col("it_da_final").cast(pl.Utf8, strict=False).str.strptime(pl.Date, "%Y%m%d", strict=False).alias("da_final"),
                pl.col("it_pc_interna").cast(pl.Float64, strict=False).alias("it_pc_interna"),
                pl.col("it_in_st").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars().str.to_uppercase().alias("it_in_st"),
            ]
        )
    )

    df_ref = (
        df_chaves
        .join(df_aux, left_on="co_sefin_agr", right_on="it_co_sefin", how="left")
        .filter(
            (pl.col("da_inicio").is_null() | (pl.col("da_inicio") <= pl.col("__ano_fim__")))
            & (pl.col("da_final").is_null() | (pl.col("da_final") >= pl.col("__ano_ini__")))
        )
        .with_columns(
            [
                pl.max_horizontal([pl.col("da_inicio"), pl.col("__ano_ini__")]).alias("vig_ini"),
                pl.min_horizontal(
                    [
                        pl.coalesce([pl.col("da_final"), pl.col("__ano_fim__")]),
                        pl.col("__ano_fim__"),
                    ]
                ).alias("vig_fim"),
                pl.col("da_inicio").is_not_null().alias("__tem_inicio__"),
                pl.col("da_final").is_not_null().alias("__tem_final__"),
            ]
        )
        .sort(
            ["co_sefin_agr", "ano", "__tem_inicio__", "vig_ini", "__tem_final__", "vig_fim"],
            descending=[False, False, True, True, True, True],
            nulls_last=True,
        )
        .group_by(["co_sefin_agr", "ano"])
        .agg(
            [
                pl.struct(["it_in_st", "vig_ini", "vig_fim"]).alias("__st_registros__"),
                (pl.col("it_in_st") == "S").any().alias("__tem_st_ano__"),
                pl.col("it_pc_interna").drop_nulls().first().alias("__aliq_ref__"),
            ]
        )
        .with_columns(
            pl.col("__st_registros__").map_elements(_format_st_periodos_anuais, return_dtype=pl.Utf8).alias("ST")
        )
        .select(["co_sefin_agr", "ano", "ST", "__tem_st_ano__", "__aliq_ref__"])
    )

    return df_ref


def calcular_aba_anual_dataframe(df: pl.DataFrame, df_aux_st: pl.DataFrame | None = None) -> pl.DataFrame:
    if df.is_empty():
        return pl.DataFrame()

    df = df.with_columns(
        pl.coalesce([pl.col("Dt_e_s"), pl.col("Dt_doc")]).cast(pl.Date, strict=False).alias("__data_efetiva__")
    ).with_columns(
        pl.col("__data_efetiva__").dt.year().alias("ano")
    )

    expected_cols = [
        "dev_simples",
        "excluir_estoque",
        "it_pc_interna",
        "Vl_item",
        "preco_item",
        "entr_desac_anual",
        "q_conv",
        "__qtd_decl_final_audit__",
        "co_sefin_agr",
        "saldo_estoque_anual",
        "ordem_operacoes",
        "Tipo_operacao",
        "descr_padrao",
        "unid_ref",
    ]
    for c in expected_cols:
        if c not in df.columns:
            if c in {"dev_simples", "excluir_estoque"}:
                df = df.with_columns(pl.lit(False).alias(c))
            elif c in {"Vl_item", "preco_item", "entr_desac_anual", "q_conv", "__qtd_decl_final_audit__", "it_pc_interna", "saldo_estoque_anual"}:
                df = df.with_columns(pl.lit(0.0).alias(c))
            else:
                df = df.with_columns(pl.lit(None).alias(c))

    valor_item_expr = pl.coalesce(
        [
            pl.col("preco_item").cast(pl.Float64, strict=False),
            pl.col("Vl_item").cast(pl.Float64, strict=False),
            pl.lit(0.0),
        ]
    )
    q_conv_positiva_expr = pl.col("q_conv").cast(pl.Float64, strict=False).fill_null(0.0) > 0
    entrada_valida_media_expr = (
        pl.col("Tipo_operacao").str.starts_with("1 - ENTRADA")
        & ~_boolish_expr("dev_simples").fill_null(False)
        & ~_boolish_expr("excluir_estoque").fill_null(False)
        & q_conv_positiva_expr
    )
    saida_valida_media_expr = (
        pl.col("Tipo_operacao").str.starts_with("2 - SAIDA")
        & ~_boolish_expr("dev_simples").fill_null(False)
        & ~_boolish_expr("excluir_estoque").fill_null(False)
        & q_conv_positiva_expr
    )

    df_anual = (
        df.group_by(["id_agrupado", "ano"])
        .agg(
            [
                pl.col("descr_padrao").drop_nulls().last().alias("descr_padrao"),
                pl.col("unid_ref").cast(pl.Utf8, strict=False).drop_nulls().last().alias("unid_ref"),
                pl.col("co_sefin_agr").cast(pl.Utf8, strict=False).drop_nulls().last().alias("co_sefin_agr"),
                pl.when(pl.col("Tipo_operacao").str.starts_with("0 - ESTOQUE INICIAL")).then(pl.col("q_conv")).otherwise(0.0).sum().alias("estoque_inicial"),
                pl.when(pl.col("Tipo_operacao").str.starts_with("1 - ENTRADA")).then(pl.col("q_conv")).otherwise(0.0).sum().alias("entradas"),
                pl.when(pl.col("Tipo_operacao").str.starts_with("2 - SAIDA")).then(pl.col("q_conv")).otherwise(0.0).sum().alias("saidas"),
                pl.when(pl.col("Tipo_operacao").str.starts_with("3 - ESTOQUE FINAL"))
                .then(pl.col("__qtd_decl_final_audit__").cast(pl.Float64, strict=False).fill_null(0.0))
                .otherwise(0.0)
                .sum()
                .alias("estoque_final"),
                pl.when(entrada_valida_media_expr).then(valor_item_expr).otherwise(0.0).sum().alias("soma_valor_entradas"),
                pl.when(entrada_valida_media_expr).then(pl.col("q_conv")).otherwise(0.0).sum().alias("soma_qtd_entradas"),
                pl.when(saida_valida_media_expr).then(valor_item_expr).otherwise(0.0).sum().alias("soma_valor_saidas"),
                pl.when(saida_valida_media_expr).then(pl.col("q_conv")).otherwise(0.0).sum().alias("soma_qtd_saidas"),
                pl.col("entr_desac_anual").sum().alias("entradas_desacob"),
                pl.col("saldo_estoque_anual").sort_by("ordem_operacoes").last().alias("saldo_final"),
                pl.col("it_pc_interna").cast(pl.Float64, strict=False).drop_nulls().last().alias("aliq_interna_mov"),
            ]
        )
        .with_columns(
            [
                (pl.col("estoque_inicial") + pl.col("entradas") + pl.col("entradas_desacob") - pl.col("estoque_final")).alias("saidas_calculadas"),
                # Os cenarios sao opostos por construcao:
                # estoque declarado acima do saldo sistemico indica saida desacobertada;
                # saldo sistemico acima do estoque declarado indica estoque final desacoberto.
                pl.when(pl.col("estoque_final") > pl.col("saldo_final"))
                .then(pl.col("estoque_final") - pl.col("saldo_final"))
                .otherwise(0.0)
                .alias("saidas_desacob"),
                pl.when(pl.col("saldo_final") > pl.col("estoque_final"))
                .then(pl.col("saldo_final") - pl.col("estoque_final"))
                .otherwise(0.0)
                .alias("estoque_final_desacob"),
                pl.when(pl.col("soma_qtd_entradas") > 0)
                .then(pl.col("soma_valor_entradas") / pl.col("soma_qtd_entradas"))
                .otherwise(0.0)
                .alias("pme"),
                pl.when(pl.col("soma_qtd_saidas") > 0)
                .then(pl.col("soma_valor_saidas") / pl.col("soma_qtd_saidas"))
                .otherwise(0.0)
                .alias("pms"),
            ]
        )
    )

    if df_aux_st is None:
        df_st_anual = _carregar_referencia_st_anual(df_anual)
    else:
        df_st_anual = df_aux_st

    if not df_st_anual.is_empty():
        df_anual = df_anual.join(df_st_anual, on=["co_sefin_agr", "ano"], how="left")
    else:
        df_anual = df_anual.with_columns(
            [
                pl.lit("").alias("ST"),
                pl.lit(False).alias("__tem_st_ano__"),
                pl.lit(None, dtype=pl.Float64).alias("__aliq_ref__"),
            ]
        )

    df_anual = df_anual.with_columns(
        [
            pl.coalesce([pl.col("__aliq_ref__"), pl.col("aliq_interna_mov"), pl.lit(0.0)]).alias("aliq_interna"),
            pl.col("__tem_st_ano__").fill_null(False).alias("__tem_st_ano__"),
            pl.col("ST").fill_null("").alias("ST"),
            # O PMS anual exibido na tabela e o mesmo PMS usado no calculo do ICMS.
            # Isso evita divergencia entre o valor mostrado ao usuario e a base monetaria aplicada.
            pl.col("pms").round(2).alias("__pms_icms__"),
        ]
    )

    aliq_factor = pl.col("aliq_interna").cast(pl.Float64, strict=False).fill_null(0.0) / 100.0
    base_saida = (
        pl.when(pl.col("__pms_icms__") > 0)
        .then(pl.col("saidas_desacob") * pl.col("__pms_icms__"))
        .otherwise(pl.col("saidas_desacob") * pl.col("pme") * pl.lit(1.30))
    )
    base_estoque = (
        pl.when(pl.col("__pms_icms__") > 0)
        .then(pl.col("estoque_final_desacob") * pl.col("__pms_icms__"))
        .otherwise(pl.col("estoque_final_desacob") * pl.col("pme") * pl.lit(1.30))
    )

    df_anual = df_anual.with_columns(
        [
            pl.when(pl.col("__tem_st_ano__"))
            .then(0.0)
            .otherwise(base_saida * aliq_factor)
            .alias("ICMS_saidas_desac"),
            (base_estoque * aliq_factor).alias("ICMS_estoque_desac"),
        ]
    )

    cols_qtd = [
        "estoque_inicial",
        "entradas",
        "saidas",
        "estoque_final",
        "saidas_calculadas",
        "saldo_final",
        "entradas_desacob",
        "saidas_desacob",
        "estoque_final_desacob",
    ]
    cols_valor = ["pme", "pms", "ICMS_saidas_desac", "ICMS_estoque_desac", "aliq_interna"]
    exprs_arredondamento = [pl.col(c).round(4) for c in cols_qtd if c in df_anual.columns]
    exprs_arredondamento += [pl.col(c).round(2) for c in cols_valor if c in df_anual.columns]
    df_anual = df_anual.with_columns(exprs_arredondamento)

    return (
        df_anual.select(
            [
                "ano",
                pl.col("id_agrupado").alias("id_agregado"),
                "descr_padrao",
                "unid_ref",
                "ST",
                "estoque_inicial",
                "entradas",
                "saidas",
                "estoque_final",
                "saidas_calculadas",
                "saldo_final",
                "entradas_desacob",
                "saidas_desacob",
                "estoque_final_desacob",
                "pme",
                "pms",
                "aliq_interna",
                "ICMS_saidas_desac",
                "ICMS_estoque_desac",
            ]
        )
        .sort(["ano", "id_agregado"])
    )


def gerar_calculos_anuais(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
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

    rprint(f"\n[bold cyan]Gerando calculos_anuais (Aba Anual) para CNPJ: {cnpj}[/bold cyan]")

    inicio_leitura = perf_counter()
    df = pl.read_parquet(arq_mov_estoque)
    registrar_evento_performance(
        "calculos_anuais.read_mov_estoque",
        perf_counter() - inicio_leitura,
        {"cnpj": cnpj, "linhas": df.height, "colunas": df.width},
    )

    inicio_calculo = perf_counter()
    df_result = calcular_aba_anual_dataframe(df)
    registrar_evento_performance(
        "calculos_anuais.calcular_dataframe",
        perf_counter() - inicio_calculo,
        {"cnpj": cnpj, "linhas_saida": df_result.height, "colunas_saida": df_result.width},
    )

    saida = pasta_analises / f"aba_anual_{cnpj}.parquet"
    inicio_gravacao = perf_counter()
    ok = salvar_para_parquet(df_result, pasta_analises, saida.name)
    registrar_evento_performance(
        "calculos_anuais.gravar_parquet",
        perf_counter() - inicio_gravacao,
        {"cnpj": cnpj, "arquivo_saida": saida, "sucesso": bool(ok)},
        status="ok" if ok else "error",
    )
    if ok:
        rprint(f"[green]Sucesso! {df_result.height} registros salvos na aba anual.[/green]")
    registrar_evento_performance(
        "calculos_anuais.total",
        perf_counter() - inicio_total,
        {"cnpj": cnpj, "linhas_saida": df_result.height, "sucesso": bool(ok)},
        status="ok" if ok else "error",
    )

    return ok


if __name__ == "__main__":
    try:
        if len(sys.argv) > 1:
            gerar_calculos_anuais(sys.argv[1])
        else:
            c = input("CNPJ: ")
            gerar_calculos_anuais(c)
    except Exception as e:
        import traceback
        with open(TRACEBACK_PATH, "w", encoding="utf-8") as f:
            traceback.print_exc(file=f)
        raise


