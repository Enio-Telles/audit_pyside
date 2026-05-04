import polars as pl
import pytest

from tests.diff_harness.nivel_1_divergencias import assert_zero_divergencias


def _df(chaves: list[str], ids: list[str]) -> pl.DataFrame:
    return pl.DataFrame({"chave_acesso": chaves, "prod_nitem": [1] * len(chaves), "id_agrupado": ids})


def test_sem_divergencias_passa() -> None:
    b = _df(["A", "B", "C"], ["G1", "G2", "G3"])
    n = _df(["A", "B", "C"], ["G1", "G2", "G3"])
    assert_zero_divergencias(b, n, chave=["chave_acesso", "prod_nitem"], etapa="nfe_agr")


def test_com_divergencia_levanta() -> None:
    b = _df(["A", "B"], ["G1", "G2"])
    n = _df(["A", "B"], ["G1", "G_ERRADO"])
    with pytest.raises(AssertionError, match="divergencias"):
        assert_zero_divergencias(b, n, chave=["chave_acesso", "prod_nitem"], etapa="nfe_agr")


def test_intersecao_vazia_passa() -> None:
    b = _df(["A", "B"], ["G1", "G2"])
    n = _df(["C", "D"], ["G3", "G4"])
    assert_zero_divergencias(b, n, chave=["chave_acesso", "prod_nitem"], etapa="nfe_agr")


def test_coluna_ausente_em_um_lado_ignorada() -> None:
    b = pl.DataFrame({"chave_acesso": ["A"], "prod_nitem": [1]})
    n = pl.DataFrame({"chave_acesso": ["A"], "prod_nitem": [1], "id_agrupado": ["G1"]})
    assert_zero_divergencias(b, n, chave=["chave_acesso", "prod_nitem"], etapa="nfe_agr")


def test_nulos_iguais_passa() -> None:
    b = pl.DataFrame({"chave_acesso": ["A"], "prod_nitem": [1], "id_agrupado": [None]})
    n = pl.DataFrame({"chave_acesso": ["A"], "prod_nitem": [1], "id_agrupado": [None]})
    assert_zero_divergencias(b, n, chave=["chave_acesso", "prod_nitem"], etapa="nfe_agr")


def test_nulo_vs_valor_levanta() -> None:
    b = pl.DataFrame({"chave_acesso": ["A"], "prod_nitem": [1], "id_agrupado": [None]})
    n = pl.DataFrame({"chave_acesso": ["A"], "prod_nitem": [1], "id_agrupado": ["G1"]})
    with pytest.raises(AssertionError):
        assert_zero_divergencias(b, n, chave=["chave_acesso", "prod_nitem"], etapa="nfe_agr")
