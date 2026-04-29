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


def _adicionar_lista_vazia(df: pl.DataFrame, nome: str) -> pl.DataFrame:
    if nome not in df.columns:
        return df.with_columns(pl.lit([]).cast(pl.List(pl.Utf8)).alias(nome))
    return df


def _carregar_codigos_da_ponte(pasta_analises: Path, cnpj: str) -> pl.DataFrame:
    arq_mapa = pasta_analises / f"map_produto_agrupado_{cnpj}.parquet"
    if not arq_mapa.exists():
        return pl.DataFrame(schema={"id_agrupado": pl.Utf8, "lista_codigos": pl.List(pl.Utf8)})

    df_mapa = pl.read_parquet(arq_mapa)
    if (
        df_mapa.is_empty()
        or "id_agrupado" not in df_mapa.columns
        or "codigo_fonte" not in df_mapa.columns
    ):
        return pl.DataFrame(schema={"id_agrupado": pl.Utf8, "lista_codigos": pl.List(pl.Utf8)})

    return (
        df_mapa.select(
            [
                pl.col("id_agrupado").cast(pl.Utf8, strict=False),
                pl.col("codigo_fonte").cast(pl.Utf8, strict=False),
            ]
        )
        .drop_nulls(["id_agrupado", "codigo_fonte"])
        .filter(pl.col("codigo_fonte").str.strip_chars() != "")
        .group_by("id_agrupado")
        .agg(
            pl.col("codigo_fonte")
            .drop_nulls()
            .str.strip_chars()
            .unique()
            .sort()
            .alias("lista_codigos")
        )
    )


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

    for col in ["descr_padrao", "descricao_final", "descricao", "unid_ref_sugerida"]:
        if col not in df_final.columns:
            df_final = df_final.with_columns(pl.lit(None, dtype=pl.Utf8).alias(col))

    for col in [
        "lista_desc_compl",
        "lista_codigos",
        "lista_unid",
        "lista_unidades_agr",
        "lista_descricoes",
    ]:
        df_final = _adicionar_lista_vazia(df_final, col)

    df_codigos_ponte = _carregar_codigos_da_ponte(pasta_analises, cnpj)
    if not df_codigos_ponte.is_empty():
        df_final = (
            df_final.join(df_codigos_ponte, on="id_agrupado", how="left", suffix="_ponte")
            .with_columns(
                pl.when(pl.col("lista_codigos").list.len() > 0)
                .then(pl.col("lista_codigos"))
                .otherwise(pl.col("lista_codigos_ponte"))
                .alias("lista_codigos")
            )
            .drop("lista_codigos_ponte", strict=False)
        )
    # Intermediate columns used only during aggregation; dropped from the final result.
    _TMP_AGG_COLS = [
        "_tmp_desc_padrao",
        "_tmp_desc_final",
        "_tmp_desc",
        "_tmp_desc_compl",
        "_tmp_codigos",
        "_tmp_unid",
        "_tmp_unid_agr",
        "_tmp_unid_ref",
    ]

    df_id_agrupados = (
        df_final.with_columns(
            [
                pl.col("id_agrupado").cast(pl.Utf8, strict=False),
                pl.col("descr_padrao").cast(pl.Utf8, strict=False),
                pl.col("descricao_final").cast(pl.Utf8, strict=False),
                pl.col("descricao").cast(pl.Utf8, strict=False),
                pl.col("lista_desc_compl").cast(pl.List(pl.Utf8), strict=False),
                pl.col("lista_codigos").cast(pl.List(pl.Utf8), strict=False),
                pl.col("lista_unid").cast(pl.List(pl.Utf8), strict=False),
                pl.col("lista_unidades_agr").cast(pl.List(pl.Utf8), strict=False),
                pl.col("lista_descricoes").cast(pl.List(pl.Utf8), strict=False),
                pl.col("unid_ref_sugerida").cast(pl.Utf8, strict=False),
            ]
        )
        .group_by("id_agrupado")
        .agg(
            [
                pl.col("descr_padrao")
                .filter(pl.col("descr_padrao").str.strip_chars() != "")
                .drop_nulls()
                .first()
                .alias("descr_padrao"),
                pl.concat_list(
                    [
                        pl.col("descr_padrao").drop_nulls().implode(),
                        pl.col("descricao_final").drop_nulls().implode(),
                        pl.col("descricao").drop_nulls().implode(),
                        pl.col("lista_descricoes").explode().drop_nulls().implode(),
                    ]
                )
                .explode()
                .drop_nulls()
                .str.strip_chars()
                .unique()
                .sort()
                .alias("lista_descricoes"),
                pl.col("lista_codigos")
                .explode()
                .drop_nulls()
                .str.strip_chars()
                .unique()
                .sort()
                .alias("lista_codigos"),
                pl.col("lista_desc_compl")
                .explode()
                .drop_nulls()
                .str.strip_chars()
                .unique()
                .sort()
                .alias("lista_desc_compl"),
                pl.concat_list(
                    [
                        pl.col("lista_unid").explode().drop_nulls().implode(),
                        pl.col("lista_unidades_agr").explode().drop_nulls().implode(),
                        pl.col("unid_ref_sugerida").drop_nulls().implode(),
                    ]
                )
                .explode()
                .drop_nulls()
                .str.strip_chars()
                .unique()
                .sort()
                .alias("lista_unidades"),
            ]
        )
        .with_columns(
            [
                pl.col("lista_descricoes").list.eval(pl.element().filter(pl.element() != "")),
                pl.col("lista_codigos").list.eval(pl.element().filter(pl.element() != "")),
                pl.col("lista_desc_compl").list.eval(pl.element().filter(pl.element() != "")),
                pl.col("lista_unidades").list.eval(pl.element().filter(pl.element() != "")),
            ]
        )
        .with_columns(
            pl.col("lista_descricoes")
            .list.len()
            .cast(pl.Int64)
            .alias("qtd_descricoes")
        )
        .select(
            [
                "id_agrupado",
                "descr_padrao",
                "lista_descricoes",
                "qtd_descricoes",
                "lista_codigos",
                "lista_desc_compl",
                "lista_unidades",
            ]
        )
        .sort("id_agrupado")
    )

    return salvar_para_parquet(df_id_agrupados, pasta_analises, f"id_agrupados_{cnpj}.parquet")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        gerar_id_agrupados(sys.argv[1])
    else:
        gerar_id_agrupados(input("CNPJ: "))
