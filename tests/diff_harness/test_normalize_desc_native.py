"""Teste diferencial da normalizacao nativa de descricao fiscal."""
import polars as pl
import pytest

from src.utilitarios.text import expr_normalizar_descricao as expr_new
from tests.diff_harness._snapshots.text_old import expr_normalizar_descricao as expr_old
from tests.diff_harness.golden_dataset import INVARIANTS, load_golden
from tests.diff_harness.run_harness import run_harness


pytestmark = pytest.mark.diff_harness


_EDGE_DESCRIPTIONS = [
    None,
    "",
    "acucar cristal branco-500g",
    "cafe soluvel/instantaneo 200g",
    "oleo de soja extra-virgem 900ml",
    "sabao_em_po 1kg",
    "queijo mussarela kg - fatiado",
    "agua mineral s/gas 1,5l",
    "PRESUNTO/QUEIJO - combo",
    "macarrao espaguete n.8",
    "jalapeno nino 250g",
    "linha premium #2 @ oferta",
    "produto {kit}[ref]\\serie/2026;",
]


def _dataset_com_bordas() -> pl.DataFrame:
    dataset = load_golden(seed=42, n_rows=100_000)
    overrides = pl.DataFrame(
        {
            "__row_nr": list(range(len(_EDGE_DESCRIPTIONS))),
            "descricao_override": _EDGE_DESCRIPTIONS,
        }
    )

    return (
        dataset.with_row_index("__row_nr")
        .join(overrides, on="__row_nr", how="left")
        .with_columns(
            pl.coalesce([pl.col("descricao_override"), pl.col("descricao")]).alias(
                "descricao"
            )
        )
        .drop(["__row_nr", "descricao_override"])
    )


def _aplica_normalizacao(expr: pl.Expr) -> pl.Expr:
    return expr.alias("id_agrupado")


def _impl_old(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(_aplica_normalizacao(expr_old("descricao")))


def _impl_new(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(_aplica_normalizacao(expr_new("descricao")))


def test_normalizacao_descricao_nativa_equivale_snapshot_antigo() -> None:
    """A vetorizacao nativa deve preservar a saida byte-a-byte da expressao antiga."""
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
