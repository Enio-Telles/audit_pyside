import re
import polars as pl
from pathlib import Path
from time import perf_counter
from rich import print as rprint

from utilitarios.project_paths import PROJECT_ROOT
from utilitarios.salvar_para_parquet import salvar_para_parquet
from utilitarios.perf_monitor import registrar_evento_performance

CNPJ_ROOT = PROJECT_ROOT / "dados" / "CNPJ"

def gerar_produtos_selecionados_dataframe(mensal: pl.DataFrame, anual: pl.DataFrame, mov_estoque: pl.DataFrame) -> pl.DataFrame:
    """Cria uma base mestre de todos os produtos agregados com resumos de cada visão."""
    
    # Base de nomes e IDs
    bases_id = []
    if not mensal.is_empty():
        bases_id.append(mensal.select(["id_agregado", "descr_padrao"]).unique())
    if not anual.is_empty():
        bases_id.append(anual.select(["id_agregado", "descr_padrao"]).unique())
    if not mov_estoque.is_empty():
        col_id = "id_agregado" if "id_agregado" in mov_estoque.columns else ("id_agrupado" if "id_agrupado" in mov_estoque.columns else None)
        if col_id:
            bases_id.append(
                mov_estoque.select([
                    pl.col(col_id).cast(pl.Utf8).alias("id_agregado"),
                    pl.col("descr_padrao").cast(pl.Utf8)
                ]).unique()
            )
    
    if not bases_id:
        return pl.DataFrame(schema={"id_agregado": pl.Utf8, "descr_padrao": pl.Utf8, "em_mensal": pl.Boolean, "em_anual": pl.Boolean, "em_estoque": pl.Boolean})

    master = pl.concat(bases_id, how="vertical_relaxed").unique(subset=["id_agregado"]).sort("id_agregado")

    # Flags de presença
    ids_mensal = mensal.get_column("id_agregado").unique() if not mensal.is_empty() else pl.Series(dtype=pl.Utf8)
    ids_anual = anual.get_column("id_agregado").unique() if not anual.is_empty() else pl.Series(dtype=pl.Utf8)
    
    col_id_mov = "id_agregado" if "id_agregado" in mov_estoque.columns else ("id_agrupado" if "id_agrupado" in mov_estoque.columns else None)
    ids_mov = mov_estoque.get_column(col_id_mov).unique() if (not mov_estoque.is_empty() and col_id_mov) else pl.Series(dtype=pl.Utf8)

    master = master.with_columns([
        pl.col("id_agregado").is_in(ids_mensal).alias("em_mensal"),
        pl.col("id_agregado").is_in(ids_anual).alias("em_anual"),
        pl.col("id_agregado").is_in(ids_mov).alias("em_estoque"),
    ])

    return master

def gerar_aba_produtos_selecionados(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    inicio = perf_counter()
    cnpj = re.sub(r"\D", "", cnpj)
    if pasta_cnpj is None:
        pasta_cnpj = CNPJ_ROOT / cnpj
    
    pasta_analises = pasta_cnpj / "analises" / "produtos"
    arq_mensal = pasta_analises / f"aba_mensal_{cnpj}.parquet"
    arq_anual = pasta_analises / f"aba_anual_{cnpj}.parquet"
    arq_mov = pasta_analises / f"mov_estoque_{cnpj}.parquet"

    # Carregar o que houver
    df_m = pl.read_parquet(arq_mensal) if arq_mensal.exists() else pl.DataFrame()
    df_a = pl.read_parquet(arq_anual) if arq_anual.exists() else pl.DataFrame()
    df_mov = pl.read_parquet(arq_mov) if arq_mov.exists() else pl.DataFrame()

    if df_m.is_empty() and df_a.is_empty() and df_mov.is_empty():
        return False

    rprint(f"[bold cyan]Gerando aba_produtos_selecionados para CNPJ: {cnpj}[/bold cyan]")
    df_master = gerar_produtos_selecionados_dataframe(df_m, df_a, df_mov)
    
    saida = pasta_analises / f"aba_produtos_selecionados_{cnpj}.parquet"
    ok = salvar_para_parquet(df_master, pasta_analises, saida.name)
    
    registrar_evento_performance(
        "produtos_selecionados.gerar",
        perf_counter() - inicio,
        {"cnpj": cnpj, "linhas": df_master.height}
    )
    return ok

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        gerar_aba_produtos_selecionados(sys.argv[1])
