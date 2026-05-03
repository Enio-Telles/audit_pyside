"""Teste diferencial para a troca de map_elements por map_batches em id_agrupado."""

import importlib

import polars as pl
import pytest

from tests.diff_harness._snapshots.descricao_produtos_old import (
    gerar_id_agrupado_automatico_expr as expr_old,
)
from tests.diff_harness.golden_dataset import INVARIANTS, load_golden
from tests.diff_harness.run_harness import run_harness


pytestmark = pytest.mark.diff_harness


_EDGE_DESCRICOES = [
    None,
    "",
    "   ",
    "CAFE SOLUVEL GRANULADO 200G *",
    "  CAFE SOLUVEL GRANULADO 200G *  ",
    "linha premium #2 @ oferta",
    "produto {kit}[ref]\\serie/2026;",
    "123",
]


def _dataset_com_bordas() -> pl.DataFrame:
    dataset = load_golden(seed=42, n_rows=100_000)
    overrides = pl.DataFrame(
        {
            "__row_nr": list(range(len(_EDGE_DESCRICOES))),
            "descricao_override": _EDGE_DESCRICOES,
        }
    )

    return (
        dataset.with_row_index("__row_nr")
        .join(overrides, on="__row_nr", how="left")
        .with_columns(
            pl.coalesce([pl.col("descricao_override"), pl.col("descricao")]).alias(
                "descricao_normalizada"
            )
        )
        .drop(["__row_nr", "descricao_override"])
    )


def _aplica_expr(df: pl.DataFrame, expr: pl.Expr) -> pl.DataFrame:
    return (
        df.with_columns(expr)
        .with_columns(
            pl.col("id_agrupado_base").alias("id_agrupado"),
            pl.col("id_agrupado_base").alias("id_agregado"),
        )
        .select(INVARIANTS)
    )


def _impl_old(df: pl.DataFrame) -> pl.DataFrame:
    return _aplica_expr(df, expr_old())


def _expr_new() -> pl.Expr:
    modulo = importlib.import_module(
        "transformacao.rastreabilidade_produtos.03_descricao_produtos"
    )
    return modulo._gerar_id_agrupado_automatico_expr()


def _impl_new(df: pl.DataFrame) -> pl.DataFrame:
    return _aplica_expr(df, _expr_new())


def test_map_batches_preserva_ids_agrupados_byte_a_byte() -> None:
    report = run_harness(
        _impl_old,
        _impl_new,
        dataset=_dataset_com_bordas(),
    )

    assert report.total_rows == 100_000
    assert not report.tem_divergencia, report.resumo()
    for chave in INVARIANTS:
        assert report.divergentes[chave] == 0, (
            f"Divergencia em {chave}: {report.amostras[chave]}"
        )