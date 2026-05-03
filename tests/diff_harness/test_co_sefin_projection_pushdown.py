"""Teste diferencial para projection pushdown nas referencias de co_sefin."""

from pathlib import Path
import importlib

import polars as pl
import pytest

from tests.diff_harness._snapshots import co_sefin_old
from tests.diff_harness.golden_dataset import INVARIANTS, load_golden
from tests.diff_harness.run_harness import run_harness


pytestmark = pytest.mark.diff_harness


def _criar_referencias(base_dir: Path) -> Path:
    refs_dir = base_dir / "CO_SEFIN"
    refs_dir.mkdir(parents=True, exist_ok=True)

    pl.DataFrame(
        {
            "it_nu_cest": ["1234567", "7654321"],
            "it_nu_ncm": ["11111111", "22222222"],
            "it_co_sefin": ["001", "002"],
        }
    ).write_parquet(refs_dir / "sitafe_cest_ncm.parquet")

    pl.DataFrame({"cest": ["7654321", "8888888"], "co-sefin": ["003", "005"]}).write_parquet(
        refs_dir / "sitafe_cest.parquet"
    )

    pl.DataFrame({"ncm": ["33333333", "44444444"], "co-sefin": ["004", "006"]}).write_parquet(
        refs_dir / "sitafe_ncm.parquet"
    )

    return refs_dir


@pytest.fixture(scope="module")
def refs_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return _criar_referencias(tmp_path_factory.mktemp("co_sefin_refs"))


def _dataset_com_bordas() -> pl.DataFrame:
    dataset = load_golden(seed=42, n_rows=100_000)
    return (
        dataset.with_row_index("__row_nr")
        .with_columns(
            pl.when((pl.col("__row_nr") % 5) == 0)
            .then(pl.lit("1234567"))
            .when((pl.col("__row_nr") % 5) == 1)
            .then(pl.lit("7654321"))
            .when((pl.col("__row_nr") % 5) == 2)
            .then(pl.lit("0000000"))
            .when((pl.col("__row_nr") % 5) == 3)
            .then(pl.lit(None))
            .otherwise(pl.lit("12.345.67"))
            .alias("cest"),
            pl.when((pl.col("__row_nr") % 5) == 0)
            .then(pl.lit("11111111"))
            .when((pl.col("__row_nr") % 5) == 1)
            .then(pl.lit("99999999"))
            .when((pl.col("__row_nr") % 5) == 2)
            .then(pl.lit("33333333"))
            .when((pl.col("__row_nr") % 5) == 3)
            .then(pl.lit(None))
            .otherwise(pl.lit("11.111.111"))
            .alias("ncm"),
        )
        .drop("__row_nr")
    )


def _materializar_invariantes(df: pl.DataFrame) -> pl.DataFrame:
    marcador = pl.coalesce([pl.col("co_sefin_inferido"), pl.lit("sem_co_sefin")])
    return (
        df.with_columns(
            pl.format("{}|{}", pl.col("id_agrupado"), marcador).alias("id_agrupado"),
            pl.format("{}|{}", pl.col("id_agregado"), marcador).alias("id_agregado"),
        )
        .select(INVARIANTS)
    )


def _impl_old(df: pl.DataFrame, refs_dir: Path) -> pl.DataFrame:
    co_sefin_old._resolver_ref = lambda nome_arquivo: refs_dir / nome_arquivo
    return _materializar_invariantes(co_sefin_old.inferir_co_sefin_dataframe(df))


def _impl_new(df: pl.DataFrame, refs_dir: Path) -> pl.DataFrame:
    modulo = importlib.import_module("transformacao.movimentacao_estoque_pkg.co_sefin")
    modulo._resolver_ref = lambda nome_arquivo: refs_dir / nome_arquivo
    return _materializar_invariantes(modulo.inferir_co_sefin_dataframe(df))


def test_projection_pushdown_preserva_co_sefin_byte_a_byte(refs_dir: Path) -> None:
    dataset = _dataset_com_bordas()
    report = run_harness(
        lambda df: _impl_old(df, refs_dir),
        lambda df: _impl_new(df, refs_dir),
        dataset=dataset,
    )

    assert report.total_rows == 100_000
    assert not report.tem_divergencia, report.resumo()
    for chave in INVARIANTS:
        assert report.divergentes[chave] == 0, (
            f"Divergencia em {chave}: {report.amostras[chave]}"
        )