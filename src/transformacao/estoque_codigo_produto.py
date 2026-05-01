from __future__ import annotations

import re
from pathlib import Path
from time import perf_counter

import polars as pl
from rich import print as rprint

from transformacao.calculos_anuais import calcular_aba_anual_dataframe
from transformacao.calculos_mensais import calcular_aba_mensal_dataframe
from transformacao.calculos_periodo_pkg import calcular_aba_periodos_dataframe
from transformacao.resumo_global import gerar_resumo_global_dataframe
from utilitarios.perf_monitor import registrar_evento_performance
from utilitarios.project_paths import PROJECT_ROOT
from utilitarios.salvar_para_parquet import salvar_para_parquet


CNPJ_ROOT = PROJECT_ROOT / "dados" / "CNPJ"
ARTEFATOS_ESTOQUE_CODIGO = {
    "mov_estoque": "mov_estoque_codigo_produto_{cnpj}.parquet",
    "aba_mensal": "aba_mensal_codigo_produto_{cnpj}.parquet",
    "aba_anual": "aba_anual_codigo_produto_{cnpj}.parquet",
    "aba_periodos": "aba_periodos_codigo_produto_{cnpj}.parquet",
    "resumo_global": "aba_resumo_global_codigo_produto_{cnpj}.parquet",
}


def _codigo_produto_expr() -> pl.Expr:
    return (
        pl.col("Cod_item")
        .cast(pl.Utf8, strict=False)
        .fill_null("")
        .str.strip_chars()
        .alias("codigo_produto_estoque")
    )


def preparar_mov_estoque_por_codigo(mov_estoque: pl.DataFrame) -> pl.DataFrame:
    """Rechaveia o movimento para cálculo por código do produto já convertido.

    A quantidade usada continua sendo q_conv/q_conv_fisica materializada em mov_estoque,
    preservando a aplicação prévia dos fatores de conversão. A chave operacional passa a
    ser exclusivamente Cod_item, que representa cod_item no C170 e prod_cprod em NFe/NFCe.
    """
    if mov_estoque.is_empty() or "Cod_item" not in mov_estoque.columns:
        return pl.DataFrame()

    df = mov_estoque.with_columns(_codigo_produto_expr()).filter(
        pl.col("codigo_produto_estoque") != ""
    )
    if df.is_empty():
        return pl.DataFrame()

    exprs: list[pl.Expr] = [
        pl.col("codigo_produto_estoque").alias("id_agrupado"),
        pl.col("codigo_produto_estoque").alias("id_agregado"),
        pl.lit("cod_item/prod_cprod").alias("criterio_calculo_estoque"),
    ]
    if "id_agrupado" in df.columns:
        exprs.append(
            pl.col("id_agrupado").cast(pl.Utf8, strict=False).alias("id_agrupado_original")
        )
    else:
        exprs.append(pl.lit(None, dtype=pl.Utf8).alias("id_agrupado_original"))
    if "id_agregado" in df.columns:
        exprs.append(
            pl.col("id_agregado").cast(pl.Utf8, strict=False).alias("id_agregado_original")
        )
    else:
        exprs.append(pl.lit(None, dtype=pl.Utf8).alias("id_agregado_original"))

    return df.with_columns(exprs).sort(
        ["id_agrupado", "Dt_e_s", "Dt_doc", "ordem_operacoes"],
        nulls_last=True,
    )


def calcular_estoque_codigo_produto_dataframe(
    mov_estoque: pl.DataFrame,
) -> dict[str, pl.DataFrame]:
    mov_codigo = preparar_mov_estoque_por_codigo(mov_estoque)
    if mov_codigo.is_empty():
        vazio = pl.DataFrame()
        return {
            "mov_estoque": vazio,
            "aba_mensal": vazio,
            "aba_anual": vazio,
            "aba_periodos": vazio,
            "resumo_global": vazio,
        }

    mensal = calcular_aba_mensal_dataframe(mov_codigo)
    anual = calcular_aba_anual_dataframe(mov_codigo)
    periodos = calcular_aba_periodos_dataframe(mov_codigo)
    resumo = gerar_resumo_global_dataframe(mensal, anual, periodos)
    return {
        "mov_estoque": mov_codigo,
        "aba_mensal": mensal,
        "aba_anual": anual,
        "aba_periodos": periodos,
        "resumo_global": resumo,
    }


def gerar_estoque_codigo_produto(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    inicio = perf_counter()
    cnpj = re.sub(r"\D", "", cnpj)
    if pasta_cnpj is None:
        pasta_cnpj = CNPJ_ROOT / cnpj

    pasta_analises = pasta_cnpj / "analises" / "produtos"
    arq_mov = pasta_analises / f"mov_estoque_{cnpj}.parquet"
    if not arq_mov.exists():
        rprint(f"[red]Arquivo necessario nao encontrado:[/red] {arq_mov}")
        return False

    rprint(f"[bold cyan]Gerando estoque por codigo do produto para CNPJ: {cnpj}[/bold cyan]")
    artefatos = calcular_estoque_codigo_produto_dataframe(pl.read_parquet(arq_mov))
    ok = True
    for nome, df in artefatos.items():
        ok = (
            salvar_para_parquet(
                df,
                pasta_analises,
                ARTEFATOS_ESTOQUE_CODIGO[nome].format(cnpj=cnpj),
            )
            and ok
        )

    registrar_evento_performance(
        "estoque_codigo_produto.gerar",
        perf_counter() - inicio,
        {
            "cnpj": cnpj,
            "sucesso": ok,
            "linhas_mov_estoque": artefatos["mov_estoque"].height,
            "linhas_mensal": artefatos["aba_mensal"].height,
            "linhas_anual": artefatos["aba_anual"].height,
            "linhas_periodos": artefatos["aba_periodos"].height,
        },
    )
    return ok
