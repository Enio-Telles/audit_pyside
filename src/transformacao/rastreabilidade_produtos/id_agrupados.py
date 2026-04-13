from __future__ import annotations

import re
import sys
from pathlib import Path
from utilitarios.project_paths import PROJECT_ROOT

import polars as pl
from rich import print as rprint

ROOT_DIR = PROJECT_ROOT
SRC_DIR = ROOT_DIR / "src"
DADOS_DIR = ROOT_DIR / "dados"
CNPJ_ROOT = DADOS_DIR / "CNPJ"


try:
    from utilitarios.salvar_para_parquet import salvar_para_parquet
except ImportError as e:
    rprint(f"[red]Erro ao importar modulos:[/red] {e}")
    sys.exit(1)


def gerar_id_agrupados(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    cnpj = re.sub(r"\D", "", cnpj or "")
    if len(cnpj) not in {11, 14}:
        raise ValueError("CPF/CNPJ invalido.")

    if pasta_cnpj is None:
        pasta_cnpj = CNPJ_ROOT / cnpj

    pasta_analises = pasta_cnpj / "analises" / "produtos"
    arq_final = pasta_analises / f"produtos_final_{cnpj}.parquet"
    if not arq_final.exists():
        rprint(f"[red]Arquivo necessario nao encontrado:[/red] {arq_final}")
        return False

    df_final = pl.read_parquet(arq_final)
    if df_final.is_empty() or "id_agrupado" not in df_final.columns:
        rprint("[yellow]produtos_final vazio ou sem id_agrupado.[/yellow]")
        return False

    # Intermediate columns used only during aggregation; dropped from the final result.
    _TMP_AGG_COLS = [
        "_tmp_desc_padrao", "_tmp_desc_final", "_tmp_desc", "_tmp_desc_compl",
        "_tmp_codigos", "_tmp_unid", "_tmp_unid_agr", "_tmp_unid_ref",
    ]

    df_id_agrupados = (
        df_final
        .with_columns(
            [
                pl.col("id_agrupado").cast(pl.Utf8, strict=False),
                pl.col("descr_padrao").cast(pl.Utf8, strict=False),
                pl.col("descricao_final").cast(pl.Utf8, strict=False),
                pl.col("descricao").cast(pl.Utf8, strict=False),
                pl.col("lista_desc_compl").cast(pl.List(pl.Utf8), strict=False),
                pl.col("lista_codigos").cast(pl.List(pl.Utf8), strict=False),
                pl.col("lista_unid").cast(pl.List(pl.Utf8), strict=False),
                pl.col("lista_unidades_agr").cast(pl.List(pl.Utf8), strict=False),
                pl.col("unid_ref_sugerida").cast(pl.Utf8, strict=False),
            ]
        )
        .group_by("id_agrupado")
        .agg([
            # descr_padrao: first non-null, non-empty value in the group
            pl.col("descr_padrao")
                .filter(pl.col("descr_padrao").is_not_null() & (pl.col("descr_padrao").str.strip_chars() != ""))
                .first()
                .alias("descr_padrao"),
            # Collect non-null values from scalar description columns for later union
            pl.col("descr_padrao").drop_nulls().alias("_tmp_desc_padrao"),
            pl.col("descricao_final").drop_nulls().alias("_tmp_desc_final"),
            pl.col("descricao").drop_nulls().alias("_tmp_desc"),
            # Flatten list columns — lista_desc_compl kept as a separate output column
            pl.col("lista_desc_compl").explode().drop_nulls().alias("_tmp_desc_compl"),
            pl.col("lista_codigos").explode().drop_nulls().alias("_tmp_codigos"),
            pl.col("lista_unid").explode().drop_nulls().alias("_tmp_unid"),
            pl.col("lista_unidades_agr").explode().drop_nulls().alias("_tmp_unid_agr"),
            pl.col("unid_ref_sugerida").drop_nulls().alias("_tmp_unid_ref"),
        ])
        .with_columns([
            # lista_descricoes: union of descr_padrao, descricao_final, descricao (stripped, deduped, sorted)
            pl.concat_list(["_tmp_desc_padrao", "_tmp_desc_final", "_tmp_desc"])
                .list.eval(pl.element().str.strip_chars())
                .list.eval(pl.element().filter(pl.element() != ""))
                .list.unique().list.sort()
                .alias("lista_descricoes"),
            # lista_desc_compl: flattened from all group rows (stripped, deduped, sorted)
            pl.col("_tmp_desc_compl")
                .list.eval(pl.element().str.strip_chars())
                .list.eval(pl.element().filter(pl.element() != ""))
                .list.unique().list.sort()
                .alias("lista_desc_compl"),
            # lista_codigos: flattened from all group rows (stripped, deduped, sorted)
            pl.col("_tmp_codigos")
                .list.eval(pl.element().str.strip_chars())
                .list.eval(pl.element().filter(pl.element() != ""))
                .list.unique().list.sort()
                .alias("lista_codigos"),
            # lista_unidades: union of lista_unid, lista_unidades_agr, unid_ref_sugerida
            pl.concat_list(["_tmp_unid", "_tmp_unid_agr", "_tmp_unid_ref"])
                .list.eval(pl.element().str.strip_chars())
                .list.eval(pl.element().filter(pl.element() != ""))
                .list.unique().list.sort()
                .alias("lista_unidades"),
        ])
        .drop(_TMP_AGG_COLS)
        .sort("id_agrupado")
    )

    return salvar_para_parquet(df_id_agrupados, pasta_analises, f"id_agrupados_{cnpj}.parquet")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        gerar_id_agrupados(sys.argv[1])
    else:
        gerar_id_agrupados(input("CNPJ: "))


