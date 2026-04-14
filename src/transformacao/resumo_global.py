import re
import polars as pl
from pathlib import Path
from time import perf_counter
from rich import print as rprint

from utilitarios.project_paths import PROJECT_ROOT
from utilitarios.salvar_para_parquet import salvar_para_parquet
from utilitarios.perf_monitor import registrar_evento_performance

CNPJ_ROOT = PROJECT_ROOT / "dados" / "CNPJ"

def gerar_resumo_global_dataframe(mensal: pl.DataFrame, anual: pl.DataFrame) -> pl.DataFrame:
    """Consolida os totais de ICMS por competência a partir das abas mensal e anual."""
    if mensal.is_empty() and anual.is_empty():
        return pl.DataFrame(schema={
            "Ano/Mes": pl.Utf8,
            "ICMS_entr_desacob": pl.Float64,
            "ICMS_saidas_desac": pl.Float64,
            "ICMS_estoque_desac": pl.Float64,
            "Total": pl.Float64,
        })

    anos = set()
    for df in (mensal, anual):
        if not df.is_empty() and "ano" in df.columns:
            anos.update(df.get_column("ano").cast(pl.Int32, strict=False).drop_nulls().unique().to_list())
    
    if not anos:
        return pl.DataFrame(schema={
            "Ano/Mes": pl.Utf8,
            "ICMS_entr_desacob": pl.Float64,
            "ICMS_saidas_desac": pl.Float64,
            "ICMS_estoque_desac": pl.Float64,
            "Total": pl.Float64,
        })

    anos_base = sorted(list(anos))
    competencias = [f"{ano:04d}-{mes:02d}" for ano in anos_base for mes in range(1, 13)]
    base_df = pl.DataFrame({"Ano/Mes": competencias})

    # Processar Mensal
    if not mensal.is_empty() and "ICMS_entr_desacob" in mensal.columns:
        m_base = (
            mensal
            .select([
                pl.concat_str([
                    pl.col("ano").cast(pl.Utf8),
                    pl.lit("-"),
                    pl.col("mes").cast(pl.Utf8).str.zfill(2)
                ]).alias("Ano/Mes"),
                pl.col("ICMS_entr_desacob").fill_null(0.0)
            ])
            .group_by("Ano/Mes")
            .agg(pl.col("ICMS_entr_desacob").sum())
        )
    else:
        m_base = pl.DataFrame(schema={"Ano/Mes": pl.Utf8, "ICMS_entr_desacob": pl.Float64})

    # Processar Anual (atribui ao mês 12)
    if not anual.is_empty() and {"ICMS_saidas_desac", "ICMS_estoque_desac"}.issubset(set(anual.columns)):
        a_base = (
            anual
            .select([
                pl.concat_str([
                    pl.col("ano").cast(pl.Utf8),
                    pl.lit("-12")
                ]).alias("Ano/Mes"),
                pl.col("ICMS_saidas_desac").fill_null(0.0),
                pl.col("ICMS_estoque_desac").fill_null(0.0)
            ])
            .group_by("Ano/Mes")
            .agg([
                pl.col("ICMS_saidas_desac").sum(),
                pl.col("ICMS_estoque_desac").sum()
            ])
        )
    else:
        a_base = pl.DataFrame(schema={"Ano/Mes": pl.Utf8, "ICMS_saidas_desac": pl.Float64, "ICMS_estoque_desac": pl.Float64})

    # Join e finalização
    resumo = (
        base_df
        .join(m_base, on="Ano/Mes", how="left")
        .join(a_base, on="Ano/Mes", how="left")
        .fill_null(0.0)
        .with_columns([
            (pl.col("ICMS_entr_desacob") + pl.col("ICMS_saidas_desac") + pl.col("ICMS_estoque_desac")).alias("Total")
        ])
        .with_columns([
            pl.col(c).round(2) for c in ["ICMS_entr_desacob", "ICMS_saidas_desac", "ICMS_estoque_desac", "Total"]
        ])
        .sort("Ano/Mes")
    )

    # Filtrar competências sem valores para não poluir
    return resumo.filter(pl.col("Total") != 0)

def gerar_aba_resumo_global(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    inicio = perf_counter()
    cnpj = re.sub(r"\D", "", cnpj)
    if pasta_cnpj is None:
        pasta_cnpj = CNPJ_ROOT / cnpj
    
    pasta_analises = pasta_cnpj / "analises" / "produtos"
    arq_mensal = pasta_analises / f"aba_mensal_{cnpj}.parquet"
    arq_anual = pasta_analises / f"aba_anual_{cnpj}.parquet"

    if not arq_mensal.exists() or not arq_anual.exists():
        rprint(f"[yellow]Avisos: Arquivos mensal/anual ausentes para resumo global em {cnpj}[/yellow]")
        return False

    rprint(f"[bold cyan]Gerando aba_resumo_global para CNPJ: {cnpj}[/bold cyan]")
    df_m = pl.read_parquet(arq_mensal)
    df_a = pl.read_parquet(arq_anual)

    df_resumo = gerar_resumo_global_dataframe(df_m, df_a)
    
    saida = pasta_analises / f"aba_resumo_global_{cnpj}.parquet"
    ok = salvar_para_parquet(df_resumo, pasta_analises, saida.name)
    
    registrar_evento_performance(
        "resumo_global.gerar",
        perf_counter() - inicio,
        {"cnpj": cnpj, "linhas": df_resumo.height}
    )
    return ok

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        gerar_aba_resumo_global(sys.argv[1])
