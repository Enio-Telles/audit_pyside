"""
Benchmarks para o pipeline de calculo de saldos de estoque.

Mede gerar_eventos_estoque e calcular_saldo_estoque_anual
sobre dataset sintetico de 50k linhas / 1k itens / 12 meses.
"""

import polars as pl
import pytest

from transformacao.movimentacao_estoque_pkg.calculo_saldos import (
    calcular_saldo_estoque_anual,
    gerar_eventos_estoque,
)


pytestmark = pytest.mark.bench


@pytest.mark.benchmark(group="calculo_saldos_50k")
def test_gerar_eventos_estoque(
    benchmark: object, bench_movimentacao_estoque_synthetic: pl.DataFrame
) -> None:
    """Benchmark: geracao de eventos ESTOQUE INICIAL / FINAL (50k linhas)."""
    df = bench_movimentacao_estoque_synthetic

    def run() -> pl.DataFrame:
        return gerar_eventos_estoque(df)

    benchmark(run)  # type: ignore[operator]


@pytest.mark.benchmark(group="calculo_saldos_50k")
def test_calcular_saldo_estoque_anual(
    benchmark: object, bench_movimentacao_estoque_synthetic: pl.DataFrame
) -> None:
    """Benchmark: calculo de saldo anual sobre eventos gerados (50k linhas)."""
    df_com_eventos = gerar_eventos_estoque(bench_movimentacao_estoque_synthetic)

    def run() -> pl.DataFrame:
        return calcular_saldo_estoque_anual(df_com_eventos)

    benchmark(run)  # type: ignore[operator]
