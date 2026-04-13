from pathlib import Path
import sys

import polars as pl
import pytest

sys.path.insert(0, str(Path("src").resolve()))

from utilitarios.validacao_schema import (  # noqa: E402
    SchemaValidacaoError,
    garantir_colunas_obrigatorias,
    garantir_tipos_compativeis,
    validar_parquet_essencial,
)


def test_garantir_colunas_obrigatorias_retorna_dataframe_quando_ok():
    df = pl.DataFrame({"a": [1], "b": ["x"]})
    resultado = garantir_colunas_obrigatorias(df, ["a", "b"], contexto="teste")
    assert resultado is df


def test_garantir_colunas_obrigatorias_falha_com_contexto():
    df = pl.DataFrame({"a": [1]})
    with pytest.raises(SchemaValidacaoError, match="teste: colunas obrigatorias ausentes: b"):
        garantir_colunas_obrigatorias(df, ["a", "b"], contexto="teste")


def test_validar_parquet_essencial_falha_quando_coluna_ausente(tmp_path: Path):
    path = tmp_path / "amostra.parquet"
    pl.DataFrame({"a": [1], "b": [2]}).write_parquet(path)

    with pytest.raises(
        SchemaValidacaoError,
        match=r"arquivo amostra\.parquet sem colunas obrigatorias: c",
    ):
        validar_parquet_essencial(path, ["a", "c"], contexto="pipeline")


def test_validar_parquet_essencial_retorna_schema_quando_ok(tmp_path: Path):
    path = tmp_path / "ok.parquet"
    pl.DataFrame({"a": [1], "b": [2]}).write_parquet(path)

    schema_cols = validar_parquet_essencial(path, ["a", "b"], contexto="pipeline")

    assert schema_cols == ["a", "b"]


def test_validar_parquet_essencial_falha_quando_arquivo_nao_existe(tmp_path: Path):
    path = tmp_path / "inexistente.parquet"

    with pytest.raises(FileNotFoundError, match="arquivo nao encontrado"):
        validar_parquet_essencial(path, ["a"], contexto="pipeline")


def test_garantir_tipos_compativeis_falha_em_tipo_diferente():
    df = pl.DataFrame({"a": [1], "b": ["x"]})
    with pytest.raises(SchemaValidacaoError, match="tipos incompativeis"):
        garantir_tipos_compativeis(
            df,
            {"a": pl.Int64, "b": pl.Int64},
            contexto="tipagem",
        )
