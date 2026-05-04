import polars as pl
import pytest

from tests.diff_harness.nivel_3_colapso_tripwire import (
    assert_nao_colapsou,
    assert_tripwire_mov_estoque,
)


def _df(n: int) -> pl.DataFrame:
    return pl.DataFrame({"chave_acesso": [f"K{i}" for i in range(n)]})


def test_sem_colapso_passa() -> None:
    assert_nao_colapsou(_df(42), _df(30), fonte="nfce")


def test_colapso_levanta() -> None:
    with pytest.raises(AssertionError, match="colapso"):
        assert_nao_colapsou(_df(42), _df(0), fonte="nfce")


def test_baseline_vazio_nao_levanta() -> None:
    assert_nao_colapsou(_df(0), _df(0), fonte="nfce")


def test_tripwire_dentro_tolerancia() -> None:
    assert_tripwire_mov_estoque(_df(1000), _df(995))


def test_tripwire_excede_levanta() -> None:
    with pytest.raises(AssertionError, match="tripwire"):
        assert_tripwire_mov_estoque(_df(1000), _df(980))


def test_tripwire_tolerancia_customizada() -> None:
    assert_tripwire_mov_estoque(_df(1000), _df(950), tolerancia=0.05)


def test_tripwire_baseline_vazio_passa() -> None:
    assert_tripwire_mov_estoque(_df(0), _df(0))
