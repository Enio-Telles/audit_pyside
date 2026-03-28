from __future__ import annotations

import re
import sys
from pathlib import Path

import polars as pl
from rich import print as rprint

ROOT_DIR = Path(r"c:\funcoes - Copia")
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
            pl.col("descr_padrao").filter(pl.col("descr_padrao").str.strip_chars() != "").drop_nulls().first().alias("descr_padrao"),
            pl.concat_list([
                pl.col("descr_padrao").drop_nulls().implode(),
                pl.col("descricao_final").drop_nulls().implode(),
                pl.col("descricao").drop_nulls().implode(),
                pl.col("lista_desc_compl").explode().drop_nulls().implode()
            ]).explode().drop_nulls().str.strip_chars().unique().sort().alias("lista_descricoes"),
            pl.col("lista_codigos").explode().drop_nulls().str.strip_chars().unique().sort().alias("lista_codigos"),
            pl.concat_list([
                pl.col("lista_unid").explode().drop_nulls().implode(),
                pl.col("lista_unidades_agr").explode().drop_nulls().implode(),
                pl.col("unid_ref_sugerida").drop_nulls().implode()
            ]).explode().drop_nulls().str.strip_chars().unique().sort().alias("lista_unidades")
        ])
        .with_columns([
            pl.col("lista_descricoes").list.eval(pl.element().filter(pl.element() != "")),
            pl.col("lista_codigos").list.eval(pl.element().filter(pl.element() != "")),
            pl.col("lista_unidades").list.eval(pl.element().filter(pl.element() != ""))
        ])
        .sort("id_agrupado")
    )

    return salvar_para_parquet(df_id_agrupados, pasta_analises, f"id_agrupados_{cnpj}.parquet")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        gerar_id_agrupados(sys.argv[1])
    else:
        gerar_id_agrupados(input("CNPJ: "))
