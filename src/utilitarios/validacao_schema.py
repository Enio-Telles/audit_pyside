from __future__ import annotations

from pathlib import Path

import polars as pl


class SchemaValidacaoError(ValueError):
    """Erro de contrato de schema/colunas em artefatos do pipeline."""


def _prefixo_contexto(contexto: str | None = None) -> str:
    return f"{contexto}: " if contexto else ""


def _formatar_tipo(dtype: pl.DataType | None) -> str:
    return "desconhecido" if dtype is None else str(dtype)


def garantir_colunas_obrigatorias(
    df: pl.DataFrame,
    colunas: list[str] | tuple[str, ...],
    contexto: str | None = None,
) -> pl.DataFrame:
    colunas_ausentes = [col for col in colunas if col not in df.columns]
    if colunas_ausentes:
        raise SchemaValidacaoError(
            f"{_prefixo_contexto(contexto)}colunas obrigatorias ausentes: {', '.join(colunas_ausentes)}"
        )
    return df


def validar_parquet_essencial(
    path: Path,
    colunas: list[str] | tuple[str, ...],
    contexto: str | None = None,
) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"{_prefixo_contexto(contexto)}arquivo nao encontrado: {path}")

    schema_cols = list(pl.read_parquet_schema(path).names())
    colunas_ausentes = [col for col in colunas if col not in schema_cols]
    if colunas_ausentes:
        raise SchemaValidacaoError(
            f"{_prefixo_contexto(contexto)}arquivo {path.name} sem colunas obrigatorias: {', '.join(colunas_ausentes)}"
        )
    return schema_cols


def garantir_tipos_compativeis(
    df: pl.DataFrame,
    schema_esperado: dict[str, pl.DataType],
    contexto: str | None = None,
) -> pl.DataFrame:
    garantir_colunas_obrigatorias(df, list(schema_esperado.keys()), contexto=contexto)

    incompatibilidades: list[str] = []
    for coluna, dtype_esperado in schema_esperado.items():
        dtype_atual = df.schema.get(coluna)
        if dtype_atual != dtype_esperado:
            incompatibilidades.append(
                f"{coluna} (atual={_formatar_tipo(dtype_atual)}, esperado={_formatar_tipo(dtype_esperado)})"
            )

    if incompatibilidades:
        raise SchemaValidacaoError(
            f"{_prefixo_contexto(contexto)}tipos incompativeis: {'; '.join(incompatibilidades)}"
        )
    return df
