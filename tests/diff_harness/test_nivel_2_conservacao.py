import polars as pl
import pytest

from tests.diff_harness.nivel_2_conservacao import assert_conservacao_de_massa


def _df(n: int) -> pl.DataFrame:
    return pl.DataFrame({"chave_acesso": [f"K{i}" for i in range(n)]})


def test_conservacao_exata_sem_fora_escopo() -> None:
    assert_conservacao_de_massa(
        baseline_principal=_df(100),
        baseline_sem_id=_df(10),
        novo_principal=_df(90),
        novo_sem_id=_df(20),
        novo_fora_escopo=None,
        fonte="nfe",
    )


def test_conservacao_com_fora_escopo() -> None:
    assert_conservacao_de_massa(
        baseline_principal=_df(100),
        baseline_sem_id=_df(10),
        novo_principal=_df(70),
        novo_sem_id=_df(5),
        novo_fora_escopo=_df(35),
        fonte="nfe",
    )


def test_falha_quando_soma_diferente() -> None:
    with pytest.raises(AssertionError, match="colapso de massa"):
        assert_conservacao_de_massa(
            baseline_principal=_df(100),
            baseline_sem_id=_df(10),
            novo_principal=_df(50),
            novo_sem_id=_df(10),
            novo_fora_escopo=_df(5),
            fonte="nfe",
        )


def test_zero_em_tudo_passa() -> None:
    assert_conservacao_de_massa(
        baseline_principal=_df(0),
        baseline_sem_id=_df(0),
        novo_principal=_df(0),
        novo_sem_id=_df(0),
        novo_fora_escopo=_df(0),
        fonte="nfce",
    )
