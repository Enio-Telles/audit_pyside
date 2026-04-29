import re
from pathlib import Path
from time import perf_counter

import polars as pl
from rich import print as rprint

from utilitarios.perf_monitor import registrar_evento_performance
from utilitarios.project_paths import PROJECT_ROOT
from utilitarios.salvar_para_parquet import salvar_para_parquet

CNPJ_ROOT = PROJECT_ROOT / "dados" / "CNPJ"

RESUMO_GLOBAL_SCHEMA = {
    "Ano/Mes": pl.Utf8,
    "ICMS_entr_desacob": pl.Float64,
    "ICMS_saidas_desac": pl.Float64,
    "ICMS_estoque_desac": pl.Float64,
    "Total": pl.Float64,
    "ICMS_entr_desacob_periodo": pl.Float64,
    "ICMS_saidas_desac_periodo": pl.Float64,
    "ICMS_estoque_desac_periodo": pl.Float64,
    "Total_periodo": pl.Float64,
}
RESUMO_GLOBAL_COLUMNS = list(RESUMO_GLOBAL_SCHEMA)


def _empty_resumo_global() -> pl.DataFrame:
    return pl.DataFrame(schema=RESUMO_GLOBAL_SCHEMA)


def _ano_mes_from_periodo_col(coluna: str) -> pl.Expr:
    periodo = pl.col(coluna).cast(pl.Int64, strict=False)
    ano = (periodo // 100).cast(pl.Utf8)
    mes = (periodo % 100).cast(pl.Utf8).str.zfill(2)
    return pl.concat_str([ano, pl.lit("-"), mes]).alias("Ano/Mes")


def _anos_periodos(periodos: pl.DataFrame) -> list[int]:
    for coluna in ("cod_per", "periodo_inventario"):
        if not periodos.is_empty() and coluna in periodos.columns:
            periodos_validos = (
                periodos.get_column(coluna).cast(pl.Int64, strict=False).drop_nulls()
            )
            return (
                (periodos_validos // 100)
                .cast(pl.Int32, strict=False)
                .unique()
                .sort()
                .to_list()
            )
    return []


def _coluna_valor(df: pl.DataFrame, coluna: str) -> pl.Expr:
    if coluna in df.columns:
        return pl.col(coluna).cast(pl.Float64, strict=False).fill_null(0.0).alias(coluna)
    return pl.lit(0.0).alias(coluna)


def gerar_resumo_global_dataframe(
    mensal: pl.DataFrame,
    anual: pl.DataFrame,
    periodos: pl.DataFrame | None = None,
    anos_base: list[int] | None = None,
    *,
    manter_competencias_zeradas: bool = False,
) -> pl.DataFrame:
    """Consolida totais de ICMS por competencia anual, mensal e por periodo."""
    periodos = periodos if periodos is not None else pl.DataFrame()
    if mensal.is_empty() and anual.is_empty() and periodos.is_empty():
        return _empty_resumo_global()

    if anos_base is None:
        anos = set()
        for df in (mensal, anual):
            if not df.is_empty() and "ano" in df.columns:
                anos.update(
                    df.get_column("ano")
                    .cast(pl.Int32, strict=False)
                    .drop_nulls()
                    .unique()
                    .to_list()
                )
        anos.update(_anos_periodos(periodos))
        anos_base = sorted(anos)

    if not anos_base:
        return _empty_resumo_global()

    competencias = [f"{ano:04d}-{mes:02d}" for ano in anos_base for mes in range(1, 13)]
    base_df = pl.DataFrame({"Ano/Mes": competencias})

    if not mensal.is_empty() and {"ano", "mes"}.issubset(set(mensal.columns)):
        m_base = (
            mensal.select(
                [
                    pl.concat_str(
                        [
                            pl.col("ano").cast(pl.Utf8),
                            pl.lit("-"),
                            pl.col("mes").cast(pl.Utf8).str.zfill(2),
                        ]
                    ).alias("Ano/Mes"),
                    _coluna_valor(mensal, "ICMS_entr_desacob"),
                    _coluna_valor(mensal, "ICMS_entr_desacob_periodo"),
                ]
            )
            .group_by("Ano/Mes")
            .agg(
                [
                    pl.col("ICMS_entr_desacob").sum(),
                    pl.col("ICMS_entr_desacob_periodo").sum(),
                ]
            )
        )
    else:
        m_base = pl.DataFrame(
            schema={
                "Ano/Mes": pl.Utf8,
                "ICMS_entr_desacob": pl.Float64,
                "ICMS_entr_desacob_periodo": pl.Float64,
            }
        )

    if not anual.is_empty() and {"ano", "ICMS_saidas_desac", "ICMS_estoque_desac"}.issubset(
        set(anual.columns)
    ):
        a_base = (
            anual.select(
                [
                    pl.concat_str([pl.col("ano").cast(pl.Utf8), pl.lit("-12")]).alias(
                        "Ano/Mes"
                    ),
                    pl.col("ICMS_saidas_desac").cast(pl.Float64, strict=False).fill_null(0.0),
                    pl.col("ICMS_estoque_desac").cast(pl.Float64, strict=False).fill_null(0.0),
                ]
            )
            .group_by("Ano/Mes")
            .agg([pl.col("ICMS_saidas_desac").sum(), pl.col("ICMS_estoque_desac").sum()])
        )
    else:
        a_base = pl.DataFrame(
            schema={
                "Ano/Mes": pl.Utf8,
                "ICMS_saidas_desac": pl.Float64,
                "ICMS_estoque_desac": pl.Float64,
            }
        )

    col_periodo = next(
        (c for c in ("cod_per", "periodo_inventario") if c in periodos.columns),
        None,
    )
    if (
        not periodos.is_empty()
        and col_periodo is not None
        and {
            "ICMS_saidas_desac_periodo",
            "ICMS_estoque_desac_periodo",
        }.issubset(set(periodos.columns))
    ):
        p_base = (
            periodos.select(
                [
                    _ano_mes_from_periodo_col(col_periodo),
                    pl.col("ICMS_saidas_desac_periodo")
                    .cast(pl.Float64, strict=False)
                    .fill_null(0.0),
                    pl.col("ICMS_estoque_desac_periodo")
                    .cast(pl.Float64, strict=False)
                    .fill_null(0.0),
                ]
            )
            .group_by("Ano/Mes")
            .agg(
                [
                    pl.col("ICMS_saidas_desac_periodo").sum(),
                    pl.col("ICMS_estoque_desac_periodo").sum(),
                ]
            )
        )
    else:
        p_base = pl.DataFrame(
            schema={
                "Ano/Mes": pl.Utf8,
                "ICMS_saidas_desac_periodo": pl.Float64,
                "ICMS_estoque_desac_periodo": pl.Float64,
            }
        )

    resumo = (
        base_df.join(m_base, on="Ano/Mes", how="left")
        .join(a_base, on="Ano/Mes", how="left")
        .join(p_base, on="Ano/Mes", how="left")
        .fill_null(0.0)
        .with_columns(
            [
                (
                    pl.col("ICMS_entr_desacob")
                    + pl.col("ICMS_saidas_desac")
                    + pl.col("ICMS_estoque_desac")
                ).alias("Total"),
                (
                    pl.col("ICMS_entr_desacob_periodo")
                    + pl.col("ICMS_saidas_desac_periodo")
                    + pl.col("ICMS_estoque_desac_periodo")
                ).alias("Total_periodo"),
            ]
        )
        .with_columns([pl.col(c).round(2) for c in RESUMO_GLOBAL_COLUMNS if c != "Ano/Mes"])
        .sort("Ano/Mes")
        .select(RESUMO_GLOBAL_COLUMNS)
    )

    if manter_competencias_zeradas:
        return resumo
    return resumo.filter((pl.col("Total") != 0) | (pl.col("Total_periodo") != 0))


def gerar_aba_resumo_global(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    inicio = perf_counter()
    cnpj = re.sub(r"\D", "", cnpj)
    if pasta_cnpj is None:
        pasta_cnpj = CNPJ_ROOT / cnpj

    pasta_analises = pasta_cnpj / "analises" / "produtos"
    arq_mensal = pasta_analises / f"aba_mensal_{cnpj}.parquet"
    arq_anual = pasta_analises / f"aba_anual_{cnpj}.parquet"
    arq_periodos = pasta_analises / f"aba_periodos_{cnpj}.parquet"

    if not arq_mensal.exists() or not arq_anual.exists():
        rprint(
            f"[yellow]Avisos: Arquivos mensal/anual ausentes para resumo global em {cnpj}[/yellow]"
        )
        return False

    rprint(f"[bold cyan]Gerando aba_resumo_global para CNPJ: {cnpj}[/bold cyan]")
    df_m = pl.read_parquet(arq_mensal)
    df_a = pl.read_parquet(arq_anual)
    df_p = pl.read_parquet(arq_periodos) if arq_periodos.exists() else pl.DataFrame()

    df_resumo = gerar_resumo_global_dataframe(df_m, df_a, df_p)

    saida = pasta_analises / f"aba_resumo_global_{cnpj}.parquet"
    ok = salvar_para_parquet(df_resumo, pasta_analises, saida.name)

    registrar_evento_performance(
        "resumo_global.gerar",
        perf_counter() - inicio,
        {"cnpj": cnpj, "linhas": df_resumo.height},
    )
    return ok


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        gerar_aba_resumo_global(sys.argv[1])
