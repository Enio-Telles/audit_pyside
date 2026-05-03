"""Fixtures Polars deterministicas para os testes do differential harness."""
import pytest
import polars as pl


@pytest.fixture()
def df_baseline_nfe() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "chave_acesso": [f"NFE_{i:04d}" for i in range(10)],
            "prod_nitem": [1] * 10,
            "id_agrupado": [f"AGR_{i % 3}" for i in range(10)],
        }
    )


@pytest.fixture()
def df_novo_identico(df_baseline_nfe: pl.DataFrame) -> pl.DataFrame:
    return df_baseline_nfe.clone()


@pytest.fixture()
def df_novo_com_divergencia(df_baseline_nfe: pl.DataFrame) -> pl.DataFrame:
    return df_baseline_nfe.with_columns(
        pl.when(pl.col("chave_acesso") == "NFE_0000")
        .then(pl.lit("AGR_ERRADO"))
        .otherwise(pl.col("id_agrupado"))
        .alias("id_agrupado")
    )


@pytest.fixture()
def df_vazio() -> pl.DataFrame:
    return pl.DataFrame(
        {"chave_acesso": pl.Series([], dtype=pl.Utf8), "prod_nitem": pl.Series([], dtype=pl.Int64)}
    )
