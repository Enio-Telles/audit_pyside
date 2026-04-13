from __future__ import annotations

import re
from pathlib import Path

import polars as pl

from utilitarios.project_paths import CNPJ_ROOT
from utilitarios.salvar_para_parquet import salvar_para_parquet


STATUS_OK = "ok"
STATUS_PENDENTE_CONVERSAO = "pendente_conversao"
STATUS_PARCIAL_POS_2022 = "parcial_pos_2022"


def sanitizar_cnpj(cnpj: str) -> str:
    return re.sub(r"\D", "", cnpj or "")


def resolver_pasta_cnpj(cnpj: str, pasta_cnpj: Path | None = None) -> Path:
    if pasta_cnpj is not None:
        return Path(pasta_cnpj)
    return CNPJ_ROOT / sanitizar_cnpj(cnpj)


def pasta_brutos(cnpj: str, pasta_cnpj: Path | None = None) -> Path:
    caminho = resolver_pasta_cnpj(cnpj, pasta_cnpj) / "arquivos_parquet"
    caminho.mkdir(parents=True, exist_ok=True)
    return caminho


def pasta_produtos(cnpj: str, pasta_cnpj: Path | None = None) -> Path:
    caminho = resolver_pasta_cnpj(cnpj, pasta_cnpj) / "analises" / "produtos"
    caminho.mkdir(parents=True, exist_ok=True)
    return caminho


def pasta_oracle_ressarcimento(cnpj: str, pasta_cnpj: Path | None = None) -> Path:
    caminho = pasta_brutos(cnpj, pasta_cnpj) / "ressarcimento_st"
    caminho.mkdir(parents=True, exist_ok=True)
    return caminho


def pasta_analises_ressarcimento(cnpj: str, pasta_cnpj: Path | None = None) -> Path:
    caminho = resolver_pasta_cnpj(cnpj, pasta_cnpj) / "analises" / "ressarcimento_st"
    caminho.mkdir(parents=True, exist_ok=True)
    return caminho


def caminho_produtos(cnpj: str, nome_arquivo: str, pasta_cnpj: Path | None = None) -> Path:
    return pasta_produtos(cnpj, pasta_cnpj) / nome_arquivo


def caminho_bruto(cnpj: str, nome_arquivo: str, pasta_cnpj: Path | None = None) -> Path:
    return pasta_brutos(cnpj, pasta_cnpj) / nome_arquivo


def caminho_oracle(cnpj: str, nome_arquivo: str, pasta_cnpj: Path | None = None) -> Path:
    return pasta_oracle_ressarcimento(cnpj, pasta_cnpj) / nome_arquivo


def caminho_analise(cnpj: str, nome_arquivo: str, pasta_cnpj: Path | None = None) -> Path:
    return pasta_analises_ressarcimento(cnpj, pasta_cnpj) / nome_arquivo


def dataframe_vazio(schema: dict[str, pl.DataType]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            nome: pl.Series(name=nome, values=[], dtype=tipo)
            for nome, tipo in schema.items()
        }
    )


def ler_parquet_opcional(
    caminho: Path,
    schema: dict[str, pl.DataType] | None = None,
) -> pl.DataFrame:
    if caminho.exists():
        return pl.read_parquet(caminho)
    if schema is None:
        return pl.DataFrame()
    return dataframe_vazio(schema)


def alinhar_schema(df: pl.DataFrame, schema: dict[str, pl.DataType]) -> pl.DataFrame:
    if df.is_empty() and not df.columns:
        return dataframe_vazio(schema)

    resultado = df
    for nome, tipo in schema.items():
        if nome not in resultado.columns:
            resultado = resultado.with_columns(pl.lit(None).cast(tipo).alias(nome))

    return resultado.select(
        [pl.col(nome).cast(tipo, strict=False).alias(nome) for nome, tipo in schema.items()]
    )


def salvar_df(df: pl.DataFrame, caminho: Path) -> bool:
    return salvar_para_parquet(df, caminho)


def expr_mes_ref(coluna_periodo: str = "periodo_efd") -> pl.Expr:
    return (
        (
            pl.col(coluna_periodo)
            .cast(pl.Utf8, strict=False)
            .str.replace("/", "-")
            + pl.lit("-01")
        )
        .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
        .alias("mes_ref")
    )


def expr_ano_ref(coluna_periodo: str = "periodo_efd") -> pl.Expr:
    return (
        pl.col(coluna_periodo)
        .cast(pl.Utf8, strict=False)
        .str.slice(0, 4)
        .cast(pl.Int32, strict=False)
        .alias("ano_ref")
    )
