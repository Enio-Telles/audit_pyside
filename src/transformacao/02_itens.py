"""
02_itens.py

Objetivo: Gerar a tabela consolidada de itens a partir de item_unidades.

Saida:
- itens_<cnpj>.parquet em analises/produtos
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import polars as pl
from rich import print as rprint

ROOT_DIR = Path(r"c:\funcoes - Copia")
SRC_DIR = ROOT_DIR / "src"
UTILITARIOS_DIR = SRC_DIR / "utilitarios"
DADOS_DIR = ROOT_DIR / "dados"
CNPJ_ROOT = DADOS_DIR / "CNPJ"

for _dir in (SRC_DIR, UTILITARIOS_DIR):
    dir_str = str(_dir)
    if dir_str not in sys.path:
        sys.path.insert(0, dir_str)

try:
    from salvar_para_parquet import salvar_para_parquet
    from text import remove_accents
    from item_unidades import item_unidades
except ImportError as e:
    rprint(f"[red]Erro ao importar modulos:[/red] {e}")
    sys.exit(1)


def _normalizar_descricao_expr(col: str) -> pl.Expr:
    return (
        pl.col(col)
        .cast(pl.Utf8, strict=False)
        .fill_null("")
        .str.to_uppercase()
        .str.replace_all(r"[ÁÀÂÃÄ]", "A")
        .str.replace_all(r"[ÉÈÊË]", "E")
        .str.replace_all(r"[ÍÌÎÏ]", "I")
        .str.replace_all(r"[ÓÒÔÕÖ]", "O")
        .str.replace_all(r"[ÚÙÛÜ]", "U")
        .str.replace_all(r"Ç", "C")
        .str.replace_all(r"Ñ", "N")
        .str.strip_chars()
        .str.replace_all(r"\s+", " ")
        .alias("descricao_normalizada")
    )


def _agg_list(col: str, alias: str) -> pl.Expr:
    return (
        pl.col(col)
        .cast(pl.String, strict=False)
        .str.strip_chars()
        .replace("", None)
        .drop_nulls()
        .unique()
        .sort()
        .alias(alias)
    )


def itens(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    cnpj = re.sub(r"\D", "", cnpj or "")
    if len(cnpj) not in {11, 14}:
        raise ValueError("CPF/CNPJ invalido.")

    if pasta_cnpj is None:
        pasta_cnpj = CNPJ_ROOT / cnpj

    pasta_analises = pasta_cnpj / "analises" / "produtos"
    arq_item_unid = pasta_analises / f"item_unidades_{cnpj}.parquet"

    if not arq_item_unid.exists():
        rprint("[yellow]item_unidades nao encontrado. Gerando base antes de consolidar itens...[/yellow]")
        if not item_unidades(cnpj, pasta_cnpj):
            return False

    if not arq_item_unid.exists():
        rprint(f"[red]Arquivo de entrada nao encontrado: {arq_item_unid}[/red]")
        return False

    rprint(f"[bold cyan]Gerando itens para CNPJ: {cnpj}[/bold cyan]")

    df = pl.read_parquet(arq_item_unid)
    if df.is_empty():
        rprint("[yellow]Arquivo item_unidades esta vazio.[/yellow]")
        return False

    required_cols = [
        "id_item_unid",
        "codigo",
        "descricao",
        "descr_compl",
        "tipo_item",
        "ncm",
        "cest",
        "co_sefin_item",
        "gtin",
        "unid",
        "fontes",
    ]
    for col in required_cols:
        if col not in df.columns:
            if col == "fontes":
                df = df.with_columns(pl.lit([]).cast(pl.List(pl.String)).alias(col))
            else:
                df = df.with_columns(pl.lit(None, pl.String).alias(col))

    df = df.with_columns(_normalizar_descricao_expr("descricao"))

    # Ordena primeiro os registros mais "ricos" para que os campos canonicos usem
    # uma linha representativa com mais informacao preenchida.
    df = (
        df.with_columns(
            (
                pl.when(pl.col("codigo").is_not_null() & (pl.col("codigo").cast(pl.String).str.strip_chars() != "")).then(1).otherwise(0)
                + pl.when(pl.col("descr_compl").is_not_null() & (pl.col("descr_compl").cast(pl.String).str.strip_chars() != "")).then(1).otherwise(0)
                + pl.when(pl.col("tipo_item").is_not_null() & (pl.col("tipo_item").cast(pl.String).str.strip_chars() != "")).then(1).otherwise(0)
                + pl.when(pl.col("ncm").is_not_null() & (pl.col("ncm").cast(pl.String).str.strip_chars() != "")).then(1).otherwise(0)
                + pl.when(pl.col("cest").is_not_null() & (pl.col("cest").cast(pl.String).str.strip_chars() != "")).then(1).otherwise(0)
                + pl.when(pl.col("co_sefin_item").is_not_null() & (pl.col("co_sefin_item").cast(pl.String).str.strip_chars() != "")).then(1).otherwise(0)
                + pl.when(pl.col("gtin").is_not_null() & (pl.col("gtin").cast(pl.String).str.strip_chars() != "")).then(1).otherwise(0)
            ).alias("__score")
        )
        .sort(["descricao_normalizada", "__score", "descricao", "codigo"], descending=[False, True, False, False], nulls_last=True)
    )

    df_itens = (
        df.group_by("descricao_normalizada")
        .agg(
            [
                pl.col("descricao").drop_nulls().first().alias("descricao"),
                pl.col("codigo").drop_nulls().first().alias("codigo"),
                pl.col("descr_compl").drop_nulls().first().alias("descr_compl"),
                pl.col("tipo_item").drop_nulls().first().alias("tipo_item"),
                pl.col("ncm").drop_nulls().first().alias("ncm"),
                pl.col("cest").drop_nulls().first().alias("cest"),
                pl.col("co_sefin_item").drop_nulls().first().alias("co_sefin_item"),
                pl.col("gtin").drop_nulls().first().alias("gtin"),
                _agg_list("unid", "lista_unid"),
                pl.col("fontes").explode().drop_nulls().unique().sort().alias("fontes"),
                _agg_list("id_item_unid", "lista_id_item_unid"),
            ]
        )
        .sort(["descricao_normalizada", "descricao"], nulls_last=True)
        .with_row_count("seq", offset=1)
        .with_columns(pl.format("id_item_{}", pl.col("seq")).alias("id_item"))
        .drop("seq", "__score", strict=False)
        .select(
            [
                "id_item",
                "codigo",
                "descricao_normalizada",
                "descricao",
                "descr_compl",
                "tipo_item",
                "ncm",
                "cest",
                "co_sefin_item",
                "gtin",
                "lista_unid",
                "fontes",
                "lista_id_item_unid",
            ]
        )
    )

    return salvar_para_parquet(df_itens, pasta_analises, f"itens_{cnpj}.parquet")


def gerar_itens(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    return itens(cnpj, pasta_cnpj)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        itens(sys.argv[1])
    else:
        itens(input("CNPJ: "))
