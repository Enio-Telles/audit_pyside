"""Fixture obrigatoria: reproduz a regressao NFC-e detectada em #196.

Cenario:
- Baseline tinha 42 linhas em nfce_agr e 20 em nfce_agr_sem_id.
- Novo tem 0 em nfce_agr e 62 em nfce_agr_fora_escopo.
- Nivel 1 (divergencias) NAO detecta porque a intersecao por chave e vazia.
- Nivel 2 (conservacao) APROVA porque 42+20 == 0+0+62.
- Nivel 3 (colapso) REPROVA porque baseline_principal>0 e novo_principal==0.

Este teste garante que o gate atual pegaria a regressao.
"""
import polars as pl
import pytest

from tests.diff_harness.nivel_1_divergencias import assert_zero_divergencias
from tests.diff_harness.nivel_2_conservacao import assert_conservacao_de_massa
from tests.diff_harness.nivel_3_colapso_tripwire import assert_nao_colapsou

_CHAVE = ["chave_acesso", "prod_nitem"]


def _baseline_principal() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "chave_acesso": [f"NFCE_{i:03d}" for i in range(42)],
            "prod_nitem": [1] * 42,
            "id_agrupado": [f"AGR_{i % 5}" for i in range(42)],
        }
    )


def _baseline_sem_id() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "chave_acesso": [f"NFCE_{i:03d}" for i in range(42, 62)],
            "prod_nitem": [1] * 20,
        }
    )


def _novo_principal_vazio() -> pl.DataFrame:
    return pl.DataFrame(
        schema={"chave_acesso": pl.Utf8, "prod_nitem": pl.Int64, "id_agrupado": pl.Utf8}
    )


def _novo_sem_id_vazio() -> pl.DataFrame:
    return pl.DataFrame(schema={"chave_acesso": pl.Utf8, "prod_nitem": pl.Int64})


def _novo_fora_escopo() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "chave_acesso": [f"NFCE_{i:03d}" for i in range(62)],
            "prod_nitem": [1] * 62,
            "motivo_fora_escopo_canonico": ["fora_escopo_canonico"] * 62,
        }
    )


def test_nivel_1_nao_detecta_regressao() -> None:
    """Nivel 1 passa trivialmente — intersecao vazia = 0 divergencias (falso-verde)."""
    assert_zero_divergencias(
        _baseline_principal(),
        _novo_principal_vazio(),
        chave=_CHAVE,
        colunas_invariantes=["id_agrupado"],
        etapa="nfce_agr",
    )


def test_nivel_2_nao_detecta_regressao() -> None:
    """Nivel 2 passa — 42+20 == 0+0+62 (conservacao de massa OK)."""
    assert_conservacao_de_massa(
        baseline_principal=_baseline_principal(),
        baseline_sem_id=_baseline_sem_id(),
        novo_principal=_novo_principal_vazio(),
        novo_sem_id=_novo_sem_id_vazio(),
        novo_fora_escopo=_novo_fora_escopo(),
        fonte="nfce",
    )


def test_nivel_3_detecta_regressao() -> None:
    """Nivel 3 PEGA O BUG — colapso: baseline>0 e novo==0."""
    with pytest.raises(AssertionError, match="colapso"):
        assert_nao_colapsou(
            baseline_principal=_baseline_principal(),
            novo_principal=_novo_principal_vazio(),
            fonte="nfce",
        )
