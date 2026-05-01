"""
test_baseline_self.py

Contrato: run_harness(impl, impl) deve retornar zero divergencias
em todas as 5 chaves invariantes.

Este teste garante que o harness em si esta correto — se falhar,
o problema e no harness, nao na implementacao auditada.
"""
from __future__ import annotations

import polars as pl
import pytest

from tests.diff_harness.golden_dataset import INVARIANTS, load_golden
from tests.diff_harness.run_harness import run_harness


pytestmark = pytest.mark.diff_harness


def _impl_identidade(df: pl.DataFrame) -> pl.DataFrame:
    """Implementacao de referencia: retorna o dataset inalterado."""
    return df


def test_self_check_zero_divergencias_todas_chaves() -> None:
    """run_harness(f, f) deve ter zero divergencias em todas as 5 chaves invariantes."""
    dataset = load_golden(seed=42, n_rows=10_000)
    report = run_harness(_impl_identidade, _impl_identidade, dataset=dataset)

    assert not report.tem_divergencia, report.resumo()
    assert report.total_rows == 10_000

    for chave in INVARIANTS:
        assert report.divergentes.get(chave, 0) == 0, (
            f"Divergencia inesperada em {chave}: {report.divergentes[chave]}\n"
            f"Amostras: {report.amostras.get(chave, [])}"
        )


def test_self_check_detecta_divergencia_artificial() -> None:
    """Verifica que o harness detecta divergencia quando q_conv e modificada."""
    dataset = load_golden(seed=42, n_rows=1_000)

    def _impl_modifica_q_conv(df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns((pl.col("q_conv") * 1.01).alias("q_conv"))

    report = run_harness(_impl_identidade, _impl_modifica_q_conv, dataset=dataset)

    assert report.divergentes["q_conv"] == 1_000, (
        "Esperado 1000 divergencias em q_conv, mas harness nao detectou"
    )
    assert report.divergentes["id_agrupado"] == 0
    assert report.divergentes["__qtd_decl_final_audit__"] == 0


def test_self_check_relatorio_tem_amostras() -> None:
    """Amostras de divergencia devem conter input, old e new."""
    dataset = load_golden(seed=42, n_rows=500)

    def _impl_muda_id_agrupado(df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(
            pl.lit("id_agrupado_auto_MODIFIED").alias("id_agrupado")
        )

    report = run_harness(_impl_identidade, _impl_muda_id_agrupado, dataset=dataset)

    assert report.divergentes["id_agrupado"] == 500
    assert len(report.amostras["id_agrupado"]) == 10

    primeira = report.amostras["id_agrupado"][0]
    assert "linha" in primeira
    assert "input" in primeira
    assert "old" in primeira
    assert "new" in primeira
    assert primeira["new"] == "id_agrupado_auto_MODIFIED"
