from __future__ import annotations

from pathlib import Path
from typing import Iterable

import polars as pl

BASE = Path("data")


def scan_domain(domain: str) -> pl.LazyFrame:
    return pl.scan_parquet(str(BASE / "silver" / domain / "**" / "*.parquet"))


def normalizar_cnpj(expr: pl.Expr) -> pl.Expr:
    return expr.cast(pl.Utf8).str.replace_all(r"\D+", "").str.zfill(14)


def carregar_resumo_documental() -> pl.LazyFrame:
    docs = scan_domain("documentos").filter(pl.col("co_emitente").is_not_null() | pl.col("co_destinatario").is_not_null())

    return (
        docs.with_columns(
            [
                normalizar_cnpj(pl.col("co_emitente")).alias("co_emitente"),
                normalizar_cnpj(pl.col("co_destinatario")).alias("co_destinatario"),
                pl.col("dhemi").dt.year().alias("ano"),
            ]
        )
        .group_by("co_emitente", "ano")
        .agg(
            [
                pl.n_unique("chave_acesso").alias("qtd_documentos"),
                pl.sum("valor_item").alias("valor_total_documental"),
            ]
        )
        .rename({"co_emitente": "co_cnpj_cpf"})
    )


def carregar_base_externa_parquet(path_glob: str) -> pl.LazyFrame:
    return pl.scan_parquet(path_glob).with_columns(
        normalizar_cnpj(pl.col("co_cnpj_cpf")).alias("co_cnpj_cpf")
    )


def gerar_mart_resumo(path_externo: str) -> pl.DataFrame:
    contribuinte = scan_domain("contribuinte").with_columns(
        normalizar_cnpj(pl.col("co_cnpj_cpf")).alias("co_cnpj_cpf")
    )

    cadastro = scan_domain("cadastro").with_columns(
        normalizar_cnpj(pl.col("co_cnpj_cpf")).alias("co_cnpj_cpf")
    )

    documental = carregar_resumo_documental()

    externo = carregar_base_externa_parquet(path_externo)

    resumo = (
        contribuinte
        .join(cadastro, on="co_cnpj_cpf", how="left")
        .join(documental.group_by("co_cnpj_cpf").agg(pl.sum("valor_total_documental").alias("valor_total_documental")), on="co_cnpj_cpf", how="left")
        .join(externo, on="co_cnpj_cpf", how="left")
    )

    return resumo.collect()


if __name__ == "__main__":
    df = gerar_mart_resumo("data/bronze/parquet_externo/**/*.parquet")
    print(df.head())
